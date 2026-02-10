# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync `openalex.common.concepts_api` to Elasticsearch

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
    "table_name": "openalex.common.concepts_api",
    "index_name": "concepts-v8"
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
                "_id": row._id,
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

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(f"{CONFIG['table_name']}")
        # Keep original BIGINT id as _id for ES document identity
        .withColumn("_id", F.col("id").cast("string"))
        # Transform id to full URL for _source
        .withColumn("id", F.concat(F.lit("https://openalex.org/C"), F.col("id")))
        # First cast to timestamp
        .withColumn("created_date", F.to_timestamp("created_date"))
        .withColumn("updated_date", F.to_timestamp("updated_date"))
        # Apply range checks using BETWEEN
        .withColumn(
            "created_date",
            F.when(
                F.col("created_date").between(F.lit("1000-01-01"), F.lit("9999-12-31")),
                F.col("created_date")
            ).otherwise(F.lit(None).cast("timestamp"))
        )
        .withColumn(
            "updated_date",
            F.when(
                F.col("updated_date").between(F.lit("1000-01-01"), F.lit("9999-12-31")),
                F.col("updated_date")
            ).otherwise(F.lit(None).cast("timestamp"))
        )
        .select("_id", F.struct(
            F.col("id"), F.col("display_name"), F.col("level"), F.col("description"),
            F.col("wikidata"), F.col("image_url"), F.col("image_thumbnail_url"),
            F.col("works_count"), F.col("cited_by_count"), F.col("ids"),
            F.col("works_api_url"), F.col("summary_stats"), F.col("international"),
            F.col("ancestors"), F.col("related_concepts"), F.col("counts_by_year"),
            F.col("created_date"), F.col("updated_date")
        ).alias("_source"))
    )
    df = df.repartition(8)
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

client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )

client.indices.refresh(index=CONFIG['index_name'])
