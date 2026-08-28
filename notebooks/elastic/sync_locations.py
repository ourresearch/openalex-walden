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
    "index_name": "locations-v2"
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
        errors = []
        for success, info in helpers.parallel_bulk(
            client,
            generate_actions(),
            chunk_size=500,
            thread_count=1,
            raise_on_error=False,
            raise_on_exception=False
        ):
            if success:
                count += 1
            else:
                errors.append(info)

        if errors:
            print(f"PARTITION HAD {len(errors)} FAILED DOCS (indexed {count}). First 5 reasons:")
            for e in errors[:5]:
                print(str(e)[:500])
            if len(errors) > 1000:
                raise Exception(f"{len(errors)} failed docs in one partition - aborting")
        else:
            print(f"Successfully indexed {count} total documents to {index_name}")

    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")
        raise
    finally:
        client.close()

# COMMAND ----------

# Explicit mapping: locations-v1's live mapping, minus the urls.conent_type typo field,
# plus endpoint_id, with updated_date as a real date (was keyword in v1).
INDEX_MAPPING = {
    "dynamic": "strict",
    "properties": {
        "abstract": {"type": "keyword", "ignore_above": 8191},
        "authors": {
            "properties": {
                "affiliations": {
                    "properties": {
                        "department": {"type": "keyword"},
                        "name": {"type": "keyword"},
                        "ror_id": {"type": "keyword"},
                    }
                },
                "author_key": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "family": {"type": "keyword"},
                "given": {"type": "keyword"},
                "is_corresponding": {"type": "boolean"},
                "name": {"type": "keyword"},
                "orcid": {"type": "keyword"},
            }
        },
        "created_date": {"type": "keyword"},
        "endpoint_id": {"type": "keyword"},
        "first_page": {"type": "keyword"},
        "grobid_s3_id": {"type": "keyword"},
        "id": {"type": "keyword"},
        "ids": {
            "properties": {
                "id": {"type": "keyword"},
                "namespace": {"type": "keyword"},
                "relationship": {"type": "keyword"},
            }
        },
        "ingested_at": {"type": "keyword"},
        "is_oa": {"type": "boolean"},
        "is_oa_source": {"type": "boolean"},
        "is_retracted": {"type": "boolean"},
        "issue": {"type": "keyword"},
        "landing_page_url": {"type": "keyword", "ignore_above": 8191},
        "language": {"type": "keyword"},
        "last_page": {"type": "keyword"},
        "license": {"type": "keyword"},
        "merge_key": {
            "properties": {
                "arxiv": {"type": "keyword"},
                "doi": {"type": "keyword"},
                "pmid": {"type": "keyword"},
                "title_author": {"type": "keyword"},
            }
        },
        "native_id": {"type": "keyword", "ignore_above": 8191},
        "native_id_namespace": {"type": "keyword"},
        "openalex_created_dt": {"type": "keyword"},
        "openalex_updated_dt": {"type": "keyword"},
        "pdf_s3_id": {"type": "keyword"},
        "pdf_url": {"type": "keyword", "ignore_above": 8191},
        "provenance": {"type": "keyword"},
        "published_date": {"type": "keyword"},
        "publisher": {"type": "keyword", "ignore_above": 8191},
        "raw_type": {"type": "keyword"},
        "references": {
            "properties": {
                "arxiv": {"type": "keyword"},
                "authors": {"type": "keyword"},
                "doi": {"type": "keyword"},
                "pmid": {"type": "keyword"},
                "raw": {"type": "keyword"},
                "title": {"type": "keyword"},
                "year": {"type": "keyword"},
            }
        },
        "source_id": {"type": "keyword"},
        "source_name": {"type": "keyword", "ignore_above": 8191},
        "title": {"type": "keyword", "ignore_above": 8191},
        "type": {"type": "keyword"},
        "updated_date": {"type": "date", "format": "yyyy-MM-dd"},
        "urls": {
            "properties": {
                "content_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "url": {"type": "keyword"},
            }
        },
        "version": {"type": "keyword"},
        "volume": {"type": "keyword"},
        "work_id": {"type": "keyword"},
    },
}

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
                "index": {"number_of_replicas": 0, "refresh_interval": "-1"}
            })
            print(f"Set replicas to 0 and refresh_interval to -1 on {CONFIG['index_name']} for full sync")
        else:
            client.indices.create(
                index=CONFIG["index_name"],
                body={
                    "settings": {
                        "index": {
                            "number_of_shards": 12,
                            "number_of_replicas": 0,
                            "refresh_interval": "-1",
                        }
                    },
                    "mappings": INDEX_MAPPING,
                },
            )
            print(f"Created index {CONFIG['index_name']} with explicit mapping, replicas=0, refresh_interval=-1")
    finally:
        client.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Sync

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

doc_columns = [
    "work_id", "native_id", "native_id_namespace", "provenance", "merge_key",
    "title", "type", "raw_type", "version", "license", "language",
    "published_date", "created_date", "updated_date",
    "issue", "volume", "first_page", "last_page",
    "is_retracted", "is_oa", "is_oa_source",
    "abstract", "authors", "ids", "urls", "references",
    "source_name", "publisher", "source_id",
    "pdf_url", "landing_page_url", "pdf_s3_id", "grobid_s3_id",
    "endpoint_id", "ingested_at",
    "openalex_created_dt", "openalex_updated_dt",
]

df = spark.table(CONFIG["table_name"])

if not IS_FULL_SYNC:
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    df = df.filter(F.col("openalex_updated_dt") >= two_days_ago)

for c in ["published_date", "created_date", "updated_date", "ingested_at", "openalex_created_dt", "openalex_updated_dt"]:
    df = df.withColumn(c, F.col(c).cast("string"))

df = (df
    .filter(F.col("native_id").isNotNull() & F.col("native_id_namespace").isNotNull())
    .filter(F.col("work_id").isNotNull() & (F.col("work_id") > 0))
    .withColumn("id", F.concat(F.col("native_id_namespace"), F.lit(":"), F.col("native_id")))
    .withColumn("work_id", F.concat(F.lit("https://openalex.org/W"), F.col("work_id").cast("string")))
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
        "index": {"number_of_replicas": 1, "refresh_interval": "30s"}
    })
    print(f"Restored replicas to 1 and refresh_interval to 30s on {CONFIG['index_name']}")

client.close()
