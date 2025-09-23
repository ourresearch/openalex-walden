# Databricks notebook source
%pip install elasticsearch==8.19.0

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.authors.authors_api",
    "index_name": "authors-v16"
}

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )
    
    def generate_actions(op_type = "index"):
        for row in partition:
            yield {
                "_op_type": op_type,
                "_index": CONFIG["index_name"],
                "_id": row.id,
                "_source": row._source.asDict(True)
            }
    
    try:
        count = 0
        for success, info in helpers.parallel_bulk(
            client, 
            generate_actions(), 
            chunk_size=500,
            thread_count=4
        ):
            count += 1
            if not success:
                print(f"FAILED TO INDEX: {info}")
                raise Exception(f"Failed to index document: {info}")
            # we can't see these messages unless we collect them per partition like in Works
            # elif count % 1000 == 0:
            #     msg = f"Indexed {count} documents to {index_name}..."
            #     log.warning(msg)
            #     print(msg)
        
        print(f"Successfully indexed {count} total documents to {index_name}")
        
    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Sync

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = spark.sql(f"SELECT id, STRUCT(*) as _source FROM {CONFIG['table_name']}").repartition(1024).cache()
    print(f"Total records to process: {df.count()}")
    
    def send_partition_wrapper(partition):
        return send_partition_to_elastic(
            partition,
            CONFIG['index_name']
        )
    
    df.foreachPartition(send_partition_wrapper)
    
    print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")
    
except Exception as e:
    print(f"Failed to process {CONFIG['table_name']}: {e}")
    log.error(f"Failed to process {CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nIndexing operation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use only for testing

# COMMAND ----------

import pprint # Import the pretty-print library for clean output

def generate_prepared_actions(partition, parsing_errors, op_type = "index"):
    for row in partition:
        try:
            yield {
                "_op_type": op_type,
                "_index": CONFIG["index_name"],
                "_id": row.id,
                "_source": row._source.asDict(True)
            }
        except Exception as e:
            parsing_errors.append({"row_id": row.id, "error": str(e)})

TEST = False

if (TEST):
    indexed_count = 0
    # --- Your existing setup code remains the same ---
    df_transformed_rows = spark.sql(f"SELECT id, STRUCT(*) as _source FROM {CONFIG['table_name']} LIMIT 10000").collect()
    print(f"Total records to process: {len(df_transformed_rows)}")

    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        request_timeout=180,
        max_retries=5,
        retry_on_timeout=True,
        http_compress=True,
    )

    # --- Refined bulk indexing with detailed error reporting ---
    indexed_count = 0
    failed_docs = []
    parsing_errors = []

    # Use streaming_bulk with a small chunk_size
    for success, info in helpers.streaming_bulk(
        client,
        generate_prepared_actions(df_transformed_rows, parsing_errors), # Assuming the generator is simplified
        chunk_size=1000 # Process in batches of 10
    ):
        if success:
            indexed_count += 1
        else:
            # This block now correctly parses the error info
            action, result = info.popitem()
            doc_id = result.get("_id", "[unknown_id]")
            error_details = result.get("error", {})
            
            # Print a clear error message
            print("---" * 15)
            print(f"💥 FAILED to index document ID: {doc_id}")
            pprint.pprint(error_details)
            print("---" * 15)
            
            # Keep track of failed documents
            failed_docs.append(doc_id)
        if (indexed_count % 100 == 0):
            print(f"Indexed {indexed_count} documents so far...")

    client.close()
    print(parsing_errors)

# COMMAND ----------


