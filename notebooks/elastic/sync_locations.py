# Databricks notebook source
%pip install elasticsearch==8.19.0

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.works.locations_mapped",
    "index_name": "locations-v1"
}

dbutils.widgets.text("is_full_sync", "false")
IS_FULL_SYNC = dbutils.widgets.get("is_full_sync").lower() == "true"
print(f"IS_FULL_SYNC: {IS_FULL_SYNC}")

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=5,
        retry_on_timeout=True,
        request_timeout=180,
        http_compress=True,
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
        raise
    finally:
        client.close()

# COMMAND ----------

if IS_FULL_SYNC:
    try:
        client = Elasticsearch(
            hosts=[ELASTIC_URL],
            request_timeout=180,
            max_retries=5,
            retry_on_timeout=True
        )
        if client.indices.exists(index=CONFIG["index_name"]):
            client.indices.put_settings(index=CONFIG["index_name"], body={
                "index": {"number_of_replicas": 0}
            })
            print(f"Set replicas to 0 on {CONFIG['index_name']} for full sync")
        else:
            print(f"Index {CONFIG['index_name']} does not exist yet - will create with default settings")
    finally:
        client.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Sync

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

doc_columns = [
    "work_id", "native_id", "native_id_namespace", "provenance", "merge_key",
    "title", "type", "version", "license", "language",
    "published_date", "created_date", "updated_date",
    "issue", "volume", "first_page", "last_page",
    "is_retracted", "is_oa", "is_oa_source",
    "abstract", "authors", "ids", "urls", "references",
    "source_name", "publisher", "source_id",
    "pdf_url", "landing_page_url", "pdf_s3_id", "grobid_s3_id",
    "openalex_created_dt", "openalex_updated_dt",
]

df = spark.table(CONFIG["table_name"])

if not IS_FULL_SYNC:
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    df = df.filter(F.col("openalex_updated_dt") >= two_days_ago)

df = (df
    .filter(F.col("native_id").isNotNull() & F.col("native_id_namespace").isNotNull())
    .withColumn("id", F.concat(F.col("native_id_namespace"), F.lit(":"), F.col("native_id")))
    .withColumn("work_id", F.concat(F.lit("https://openalex.org/W"), F.coalesce(F.col("work_id"), F.lit(-1)).cast("string")))
    .withColumn("source_id", F.when(F.col("source_id").isNotNull(),
        F.concat(F.lit("https://openalex.org/S"), F.col("source_id").cast("string"))))
    .select("id", F.struct(F.col("id"), *[F.col(c) for c in doc_columns]).alias("_source"))
)
df = df.repartition(2048)
print(f"Total records to process: {df.count()}")

def send_partition_wrapper(partition):
    return send_partition_to_elastic(partition, CONFIG["index_name"])

df.foreachPartition(send_partition_wrapper)

print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")

# COMMAND ----------

client = Elasticsearch(
    hosts=[ELASTIC_URL],
    request_timeout=180,
    max_retries=5,
    retry_on_timeout=True
)

client.indices.refresh(index=CONFIG["index_name"], request_timeout=600)
print(f"Refreshed index {CONFIG['index_name']}")
print(f"{client.count(index=CONFIG['index_name'])['count']} documents in {CONFIG['index_name']}")

if IS_FULL_SYNC:
    client.indices.put_settings(index=CONFIG["index_name"], body={
        "index": {"number_of_replicas": 1}
    })
    print(f"Restored replicas to 1 on {CONFIG['index_name']}")

client.close()
