# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PREPARE source_types_api table
# MAGIC CREATE OR REPLACE TABLE openalex.common.source_types_api AS
# MAGIC WITH source_type_counts AS (
# MAGIC     SELECT 
# MAGIC         source_type,
# MAGIC         count(*) as works_count,
# MAGIC         sum(cited_by_count) as total_citations
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             w.id,
# MAGIC             w.cited_by_count,
# MAGIC             location.source.type as source_type
# MAGIC         FROM openalex.works.openalex_works w
# MAGIC         LATERAL VIEW explode(w.locations) as location
# MAGIC         WHERE location.source.type IS NOT NULL
# MAGIC     ) distinct_records
# MAGIC     GROUP BY source_type
# MAGIC )
# MAGIC SELECT 
# MAGIC     st.id as original_id,
# MAGIC     CONCAT('https://openalex.org/source-types/', st.id) as id,
# MAGIC     st.display_name,
# MAGIC     CONCAT('https://api.openalex.org/works?data-version=2&filter=locations.source.type:', st.id) as works_api_url,
# MAGIC     stc.works_count,
# MAGIC     stc.total_citations as cited_by_count,
# MAGIC     st.created_date,
# MAGIC     st.updated_date
# MAGIC FROM openalex.common.source_types st
# MAGIC JOIN source_type_counts stc on st.id = stc.source_type

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.common.source_types_api",
    "index_name": "source-types-v1"
}

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
    )
    
    def generate_actions():
        for row in partition:
            doc = row.asDict()
            
            for key, value in doc.items():
                if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                    try:
                        parsed_value = json.loads(value)
                        if isinstance(parsed_value, list):
                            doc[key] = parsed_value
                    except (json.JSONDecodeError, ValueError):
                        pass
                elif hasattr(value, 'asDict'):
                    doc[key] = value.asDict()
                elif isinstance(value, list) and len(value) > 0 and hasattr(value[0], 'asDict'):
                    doc[key] = [item.asDict() if hasattr(item, 'asDict') else item for item in value]
            
            entity_id = doc['id']
            
            yield {
                "_index": index_name,
                "_id": entity_id,
                "_source": doc
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
            elif count % 100 == 0:
                msg = f"Indexed {count} documents to {index_name}..."
                log.info(msg)
                print(msg)
        
        print(f"Successfully indexed {count} total documents to {index_name}")
        
    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = spark.read.table(CONFIG['table_name'])
    
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
