# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync `openalex.institutions.affiliation_strings_lookup` to Elasticsearch
# MAGIC
# MAGIC This notebook syncs raw affiliation strings with their resolved institution IDs to Elasticsearch
# MAGIC for fast text search. We hash the raw_affiliation_string to create a stable document ID.

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.institutions.affiliation_strings_lookup",
    "index_name": "raw-affiliation-strings-v1"
}

def create_index_if_not_exists(client, index_name):
    """Create index with accent-folding analyzer if it doesn't exist."""
    if client.indices.exists(index=index_name):
        print(f"Index {index_name} already exists, skipping creation")
        return
    
    client.indices.create(
        index=index_name,
        body={
            "settings": {
                "analysis": {
                    "analyzer": {
                        "affiliation_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "raw_affiliation_string": {
                        "type": "text",
                        "analyzer": "affiliation_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "institution_ids": {"type": "keyword"},
                    "institution_ids_override": {"type": "keyword"},
                    "countries": {"type": "keyword"}
                }
            }
        }
    )
    print(f"Created index {index_name} with accent-folding analyzer")


def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )
    
    def generate_actions(op_type="index"):
        for row in partition:
            yield {
                "_op_type": op_type,
                "_index": index_name,
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
        
        print(f"Successfully indexed {count} total documents to {index_name}")
        
    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

# COMMAND ----------

# Create the index with custom mappings (if it doesn't exist)
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=3,
    request_timeout=180
)
create_index_if_not_exists(client, CONFIG['index_name'])

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(CONFIG['table_name'])
        # Create a hashed ID from raw_affiliation_string
        .withColumn("id", F.sha2(F.col("raw_affiliation_string"), 256))
        # Select only the columns we need
        .select(
            "id",
            F.struct(
                F.col("raw_affiliation_string"),
                F.col("institution_ids"),
                F.col("institution_ids_override"),
                F.col("countries")
            ).alias("_source")
        )
        # Filter out any rows where the affiliation string is null
        .filter(F.col("id").isNotNull())
    )
    
    df = df.repartition(8)
    record_count = df.count()
    print(f"Total records to process: {record_count}")
    
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

# Refresh the index to make documents searchable immediately
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=3,
    request_timeout=180
)

client.indices.refresh(index=CONFIG['index_name'])
print(f"Index {CONFIG['index_name']} refreshed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the index (optional)

# COMMAND ----------

# Quick verification - count docs and sample search
result = client.count(index=CONFIG['index_name'])
print(f"Total documents in index: {result['count']}")

def test_search(query_string):
    """Test a search using simple_query_string syntax."""
    result = client.search(
        index=CONFIG['index_name'],
        body={
            "query": {
                "simple_query_string": {
                    "query": query_string,
                    "fields": ["raw_affiliation_string"],
                    "default_operator": "AND"
                }
            },
            "size": 5
        }
    )
    print(f"\nSearch: {query_string}")
    print(f"  Hits: {result['hits']['total']['value']}")
    for hit in result['hits']['hits']:
        print(f"  - {hit['_source']['raw_affiliation_string'][:100]}...")
    return result

# Example searches demonstrating different operators:
# Basic term search
test_search("university")

# Phrase search (words must appear consecutively in order)
test_search('"University of Florida"')

# AND search (both terms required) - this is the default
test_search("Harvard Boston")

# OR search (either term)
test_search("MIT | Stanford")

# Exclusion (NOT)
test_search("university -hospital")

# Prefix wildcard
test_search("univers*")

# Combined: phrase + required term + exclusion
test_search('"medical center" + California -UCLA')
