# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PREPARE continents_api table
# MAGIC CREATE OR REPLACE TABLE openalex.common.continents_api AS
# MAGIC SELECT 
# MAGIC     c.id as original_id,
# MAGIC     CONCAT('https://openalex.org/continents/', c.wikidata_id) as id,
# MAGIC     c.display_name,
# MAGIC     c.wikidata_id,
# MAGIC     c.wikidata_url,
# MAGIC     c.wikipedia_url,
# MAGIC     c.display_name_alternatives,
# MAGIC     c.description,
# MAGIC     c.created_date,
# MAGIC     c.updated_date,
# MAGIC     COALESCE(countries_agg.countries, ARRAY()) AS countries
# MAGIC FROM openalex.common.continents c
# MAGIC LEFT JOIN (
# MAGIC     SELECT 
# MAGIC         continent_id,
# MAGIC         SORT_ARRAY(
# MAGIC             COLLECT_LIST(
# MAGIC                 NAMED_STRUCT(
# MAGIC                     'id', CONCAT('https://openalex.org/countries/', id),
# MAGIC                     'display_name', display_name
# MAGIC                 )
# MAGIC             )
# MAGIC         ) AS countries
# MAGIC     FROM openalex.common.countries
# MAGIC     GROUP BY continent_id
# MAGIC ) countries_agg ON c.id = countries_agg.continent_id

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

# Continents-specific configuration
CONTINENTS_CONFIG = {
    "table_name": "openalex.common.continents_api",
    "index_name": "continents-v1",
    "id_prefix": "continents"
}

def send_partition_to_elastic(partition, index_name, id_prefix):
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
            
            entity_id = doc["id"]
            
            doc["ids"] = {
                "openalex": entity_id,
                "wikidata": doc.get("wikidata_id", None),
            }
            
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
                raise Exception(f"Failed to index document: {info}")
            elif count % 100 == 0:
                msg = f"Indexed {count} documents to {index_name}..."
                log.info(msg)
                print(msg)
                
        print(f"Successfully indexed {count} total documents to {index_name}")
        
    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

print(f"\n=== Processing {CONTINENTS_CONFIG['table_name']} ===")

try:
    df = spark.read.table(CONTINENTS_CONFIG['table_name'])
    
    def send_partition_wrapper(partition):
        return send_partition_to_elastic(
            partition, 
            CONTINENTS_CONFIG['index_name'], 
            CONTINENTS_CONFIG['id_prefix']
        )
    
    df.foreachPartition(send_partition_wrapper)
    
    print(f"Completed indexing {CONTINENTS_CONFIG['table_name']} to {CONTINENTS_CONFIG['index_name']}")
    
except Exception as e:
    print(f"Failed to process {CONTINENTS_CONFIG['table_name']}: {e}")
    log.error(f"Failed to process {CONTINENTS_CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nContinents indexing operation completed!")
