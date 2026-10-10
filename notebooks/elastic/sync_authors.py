# Databricks notebook source
%pip install elasticsearch==8.19.0

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

# index_name is a widget so a rebuild can load a fresh index beside the live one (oxjob #1531: authors-v20; #1617: authors-v21); the job yaml passes it.
dbutils.widgets.text("index_name", "authors-v21")
CONFIG = {
    "table_name": "openalex.authors.openalex_authors",
    "index_name": dbutils.widgets.get("index_name")
}

# oxjob #1617: the keys OQL's `at [X] since Y` and `in [country] since Y` read (one SQL
# expression each, so the same text runs on a SQL warehouse to check it)
INSTITUTION_YEARS_SQL = """
filter(array_distinct(flatten(filter(transform(affiliations, a -> flatten(filter(transform(
    CASE WHEN size(a.institution.lineage) > 0 THEN a.institution.lineage
         ELSE array(a.institution.id) END,
    inst -> transform(a.years, y -> concat(regexp_extract(inst, '([A-Za-z][0-9]+)$', 1), ':',
                                           cast(y AS string)))),
    x -> x IS NOT NULL))), x -> x IS NOT NULL))),
  k -> k IS NOT NULL AND NOT startswith(k, ':'))
"""
COUNTRY_YEARS_SQL = """
filter(array_distinct(flatten(filter(transform(affiliations, a -> transform(a.years,
    y -> concat(upper(a.institution.country_code), ':', cast(y AS string)))),
    x -> x IS NOT NULL))),
  k -> k IS NOT NULL)
"""

dbutils.widgets.text("is_full_sync", "false")
IS_FULL_SYNC = dbutils.widgets.get("is_full_sync").lower() == "true"
print(f"IS_FULL_SYNC: {IS_FULL_SYNC}")

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

        print(f"Successfully indexed {count} total documents to {index_name}")

    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

# COMMAND ----------

# Set replicas to 0 for faster bulk indexing during full sync
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

try:
    df = spark.table(f"{CONFIG['table_name']}")

    if not IS_FULL_SYNC:
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        df = df.filter(F.col("updated_date") >= two_days_ago)

    df = (df
        .withColumn("id", F.concat(F.lit("https://openalex.org/A"), F.col("id").cast("string")))
        .withColumn("topics", F.slice(F.col("topics"), 1, 5))
        .withColumn("topic_share", F.slice(F.col("topic_share"), 1, 5))
        .withColumn("display_name_alternatives", F.col("raw_author_names"))
        # Institution-year and country-year keys (oxjob #1617), so OQL's `at [UBC](I141945490)
        # since 2022` is one terms query: "I141945490:2022" for the institution and every
        # institution in its lineage (a department's years count for its university), and
        # "CA:2022" for the institution's country, in each year the record lists it.
        .withColumn("institution_years", F.expr(INSTITUTION_YEARS_SQL))
        .withColumn("country_years", F.expr(COUNTRY_YEARS_SQL))
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    df = df.repartition(1024)
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

# refresh and restore replicas
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    request_timeout=180,
    max_retries=5,
    retry_on_timeout=True
)

client.indices.refresh(index=CONFIG["index_name"])
print(f"Refreshed index {CONFIG['index_name']}")

if IS_FULL_SYNC:
    client.indices.put_settings(index=CONFIG["index_name"], body={
        "index": {"number_of_replicas": 1}
    })
    print(f"Restored replicas to 1 on {CONFIG['index_name']}")

client.close()
