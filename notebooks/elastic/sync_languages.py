# Databricks notebook source
# NOTE: Unlike larger entities (sources, publishers, etc.) which separate API table creation
# (SQL warehouse) from ES sync (compute), these small entities (10-100 rows) combine both
# steps in a single notebook for simplicity.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists("openalex.common.languages_api"):
    spark.sql("""
        CREATE OR REPLACE TABLE openalex.common.languages_api_hash AS
        SELECT id, updated_date,
            xxhash64(CONCAT_WS('|',
                COALESCE(display_name, ''),
                COALESCE(CAST(works_count AS STRING), ''),
                COALESCE(CAST(cited_by_count AS STRING), '')
            )) AS content_hash
        FROM openalex.common.languages_api
    """)
else:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS openalex.common.languages_api_hash (
            id STRING, updated_date TIMESTAMP, content_hash BIGINT
        )
    """)
print("Hash snapshot complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rebuild languages_api table
# MAGIC CREATE OR REPLACE TABLE openalex.common.languages_api AS
# MAGIC WITH language_counts as (
# MAGIC     SELECT
# MAGIC       count(*) as works_count,
# MAGIC       sum(cited_by_count) as cited_by_count,
# MAGIC       language
# MAGIC     FROM openalex.works.openalex_works
# MAGIC     WHERE language IS NOT NULL
# MAGIC     GROUP by language
# MAGIC )
# MAGIC SELECT
# MAGIC     l.id as original_id,
# MAGIC     CONCAT('https://openalex.org/languages/', l.id) as id,
# MAGIC     CONCAT('https://api.openalex.org/works?data-version=2&filter=language:', l.id) as works_api_url,
# MAGIC     l.display_name,
# MAGIC     lc.works_count,
# MAGIC     lc.cited_by_count,
# MAGIC     DATE_TRUNC('SECOND', l.created_date) AS created_date,
# MAGIC     CAST(NULL AS TIMESTAMP) AS updated_date
# MAGIC FROM openalex.common.languages l
# MAGIC JOIN language_counts lc ON l.id = lc.language

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
# MAGIC     FROM openalex.common.languages_api
# MAGIC )
# MAGIC MERGE INTO openalex.common.languages_api AS target
# MAGIC USING (
# MAGIC     SELECT n.id,
# MAGIC         CASE
# MAGIC             WHEN p.id IS NULL THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             WHEN n.content_hash <> p.content_hash THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             ELSE p.updated_date
# MAGIC         END AS new_updated_date
# MAGIC     FROM new_hashes n
# MAGIC     LEFT JOIN openalex.common.languages_api_hash p ON n.id = p.id
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
    "table_name": "openalex.common.languages_api",
    "index_name": "languages-v3"
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
