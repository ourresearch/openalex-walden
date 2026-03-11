# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync topic hierarchy to Elasticsearch
# MAGIC Syncs all 4 entity types: topics, subfields, fields, domains

# COMMAND ----------

from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

ENTITIES = [
    {
        "table_name": "openalex.common.topics_api",
        "index_name": "topics-v4",
        "id_prefix": "https://openalex.org/T",
    },
    {
        "table_name": "openalex.common.subfields_api",
        "index_name": "subfields-v2",
        "id_prefix": "https://openalex.org/subfields/",
    },
    {
        "table_name": "openalex.common.fields_api",
        "index_name": "fields-v2",
        "id_prefix": "https://openalex.org/fields/",
    },
    {
        "table_name": "openalex.common.domains_api",
        "index_name": "domains-v2",
        "id_prefix": "https://openalex.org/domains/",
    },
]

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

for entity in ENTITIES:
    table_name = entity["table_name"]
    index_name = entity["index_name"]
    id_prefix = entity["id_prefix"]

    print(f"\n=== Processing {table_name} -> {index_name} ===")

    try:
        df = (spark.sql(f"SELECT * FROM {table_name}")
            .withColumn("id", F.concat(F.lit(id_prefix), F.col("id")))
            .select("id", F.struct(F.col("*")).alias("_source"))
        )
        # Small tables — 1 partition each is sufficient
        df = df.repartition(1)
        print(f"Total records: {df.count()}")

        def make_sender(idx_name):
            def send(partition):
                return send_partition_to_elastic(partition, idx_name)
            return send

        df.foreachPartition(make_sender(index_name))

        print(f"Completed indexing {table_name} to {index_name}")

    except Exception as e:
        print(f"Failed to process {table_name}: {e}")
        log.error(f"Failed: {table_name}: {e}", stack_info=True, exc_info=True)

print("\nAll indexing operations completed!")

# COMMAND ----------

client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=5,
    request_timeout=180
)
for entity in ENTITIES:
    client.indices.refresh(index=entity["index_name"])
    count = client.count(index=entity["index_name"])["count"]
    print(f"{entity['index_name']}: {count} documents")

# COMMAND ----------


