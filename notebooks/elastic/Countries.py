# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PREPARE countries_api table
# MAGIC CREATE OR REPLACE TABLE openalex.common.countries_api AS
# MAGIC WITH country_counts AS (
# MAGIC     SELECT 
# MAGIC         country_code,
# MAGIC         count(*) as works_count,
# MAGIC         sum(cited_by_count) as total_citations
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             id,
# MAGIC             cited_by_count,
# MAGIC             institution.country_code as country_code
# MAGIC         FROM openalex.works.openalex_works
# MAGIC         LATERAL VIEW explode(authorships) AS authorship
# MAGIC         LATERAL VIEW explode(authorship.institutions) as institution
# MAGIC         WHERE institution.country_code IS NOT NULL
# MAGIC     ) distinct_records
# MAGIC     GROUP BY country_code
# MAGIC )
# MAGIC SELECT 
# MAGIC     co.id as original_id,
# MAGIC     CONCAT('https://openalex.org/countries/', co.id) as id,
# MAGIC     co.id as country_code,
# MAGIC     co.display_name,
# MAGIC     co.continent_id,
# MAGIC     co.is_global_south,
# MAGIC     co.wikidata_url,
# MAGIC     co.wikipedia_url,
# MAGIC     co.display_name_alternatives,
# MAGIC     co.description,
# MAGIC     co.alpha_3,
# MAGIC     co.numeric,
# MAGIC     co.full_name,
# MAGIC     cc.works_count,
# MAGIC     cc.total_citations as cited_by_count,
# MAGIC     CONCAT('https://api.openalex.org/authors?data-version=2&filter=last_known_institutions.country_code:', co.id) as authors_api_url,
# MAGIC     CONCAT('https://api.openalex.org/institutions?data-version=2&filter=country_code:', co.id) as institutions_api_url,
# MAGIC     CONCAT('https://api.openalex.org/works?data-version=2&filter=authorships.countries:', co.id) as works_api_url,
# MAGIC     co.created_date,
# MAGIC     co.updated_date,
# MAGIC     NAMED_STRUCT(
# MAGIC         'id', CONCAT('https://openalex.org/continents/', c.wikidata_id),
# MAGIC         'display_name', c.display_name
# MAGIC     ) AS continent
# MAGIC FROM openalex.common.countries co
# MAGIC LEFT JOIN openalex.common.continents c ON co.continent_id = c.id
# MAGIC LEFT JOIN country_counts cc ON co.id = cc.country_code

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

COUNTRIES_CONFIG = {
    "table_name": "openalex.common.countries_api",
    "index_name": "countries-v2",
    "id_prefix": "countries"
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
            
            entity_id = doc['id']
            
            doc["ids"] = {
                "openalex": entity_id,
                "iso": f"https://www.iso.org/obp/ui/#iso:code:3166:{doc['original_id']}",
                "wikidata": doc.get("wikidata_url", None),
                "wikipedia": doc.get("wikipedia_url", None)
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

print(f"\n=== Processing {COUNTRIES_CONFIG['table_name']} ===")

try:
    df = spark.read.table(COUNTRIES_CONFIG['table_name'])
    
    def send_partition_wrapper(partition):
        return send_partition_to_elastic(
            partition, 
            COUNTRIES_CONFIG['index_name'], 
            COUNTRIES_CONFIG['id_prefix']
        )
    
    df.foreachPartition(send_partition_wrapper)
    
    print(f"Completed indexing {COUNTRIES_CONFIG['table_name']} to {COUNTRIES_CONFIG['index_name']}")
    
except Exception as e:
    print(f"Failed to process {COUNTRIES_CONFIG['table_name']}: {e}")
    log.error(f"Failed to process {COUNTRIES_CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nCountries indexing operation completed!")
