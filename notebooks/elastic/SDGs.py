# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PREPARE sdgs_api table
# MAGIC CREATE OR REPLACE TABLE openalex.common.sdgs_api AS
# MAGIC WITH sdg_counts AS (
# MAGIC     SELECT 
# MAGIC       count(*) as works_count,
# MAGIC       sum(cited_by_count) as cited_by_count, 
# MAGIC       sdg_id
# MAGIC     FROM openalex.works.openalex_works
# MAGIC     LATERAL VIEW explode(sustainable_development_goals.id) AS sdg_id
# MAGIC     GROUP BY sdg_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     CONCAT('https://openalex.org/sdgs/', s.id) as id,
# MAGIC     struct(
# MAGIC       CONCAT('https://openalex.org/sdgs/', s.id) as openalex,
# MAGIC       CONCAT('https://metadata.un.org/sdg/', s.id) as un,
# MAGIC       CONCAT('https://www.wikidata.org/wiki/', s.wikidata_id) as wikidata
# MAGIC     ) AS ids,
# MAGIC     s.display_name,
# MAGIC     s.description,
# MAGIC     sc.works_count,
# MAGIC     sc.cited_by_count,
# MAGIC     s.image_url,
# MAGIC     s.image_thumbnail_url,
# MAGIC     CONCAT('https://api.openalex.org/works?data-version=2&filter=sustainable_development_goals.id:https://metadata.un.org/sdg/', s.id) as works_api_url,
# MAGIC     s.created_date,
# MAGIC     s.updated_date
# MAGIC FROM openalex.common.sdgs s
# MAGIC JOIN sdg_counts sc ON CONCAT('https://metadata.un.org/sdg/', s.id) = sc.sdg_id

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
    "table_name": "openalex.common.sdgs_api",
    "index_name": "sdgs-v2"
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
