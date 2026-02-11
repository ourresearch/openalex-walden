# Databricks notebook source
# NOTE: Unlike larger entities (sources, publishers, etc.) which separate API table creation
# (SQL warehouse) from ES sync (compute), these small entities (10-100 rows) combine both
# steps in a single notebook for simplicity.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists("openalex.common.source_types_api"):
    spark.sql("""
        CREATE OR REPLACE TABLE openalex.common.source_types_api_hash AS
        SELECT id, updated_date,
            xxhash64(CONCAT_WS('|',
                COALESCE(display_name, ''),
                COALESCE(CAST(works_count AS STRING), ''),
                COALESCE(CAST(cited_by_count AS STRING), '')
            )) AS content_hash
        FROM openalex.common.source_types_api
    """)
else:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS openalex.common.source_types_api_hash (
            id STRING, updated_date TIMESTAMP, content_hash BIGINT
        )
    """)
print("Hash snapshot complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rebuild source_types_api table
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
# MAGIC     DATE_TRUNC('SECOND', st.created_date) AS created_date,
# MAGIC     CAST(NULL AS TIMESTAMP) AS updated_date
# MAGIC FROM openalex.common.source_types st
# MAGIC JOIN source_type_counts stc on st.id = stc.source_type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set updated_date based on content hash comparison
# MAGIC WITH new_hashes AS (
# MAGIC     SELECT id,
# MAGIC         xxhash64(CONCAT_WS('|',
# MAGIC             COALESCE(display_name, ''),
# MAGIC             COALESCE(CAST(works_count AS STRING), ''),
# MAGIC             COALESCE(CAST(cited_by_count AS STRING), '')
# MAGIC         )) AS content_hash
# MAGIC     FROM openalex.common.source_types_api
# MAGIC )
# MAGIC MERGE INTO openalex.common.source_types_api AS target
# MAGIC USING (
# MAGIC     SELECT n.id,
# MAGIC         CASE
# MAGIC             WHEN p.id IS NULL THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             WHEN n.content_hash <> p.content_hash THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             ELSE p.updated_date
# MAGIC         END AS new_updated_date
# MAGIC     FROM new_hashes n
# MAGIC     LEFT JOIN openalex.common.source_types_api_hash p ON n.id = p.id
# MAGIC ) AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET target.updated_date = source.new_updated_date

# COMMAND ----------

from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
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
        request_timeout=180
    )

    def generate_actions():
        for row in partition:
            yield {
                "_op_type": "index",
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

        print(f"Successfully indexed {count} documents to {index_name}")

    except Exception as e:
        log.error(f"Error indexing to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing to {index_name}: {e}")

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(CONFIG['table_name'])
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    df = df.repartition(1)
    print(f"Total records: {df.count()}")

    def send_partition_wrapper(partition):
        return send_partition_to_elastic(partition, CONFIG['index_name'])

    df.foreachPartition(send_partition_wrapper)

    print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")

except Exception as e:
    print(f"Failed to process {CONFIG['table_name']}: {e}")
    log.error(f"Failed: {CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nIndexing operation completed!")
