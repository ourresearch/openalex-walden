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
    "index_name": "locations-v3"
}

dbutils.widgets.text("is_full_sync", "false")
IS_FULL_SYNC = dbutils.widgets.get("is_full_sync").lower() == "true"
print(f"IS_FULL_SYNC: {IS_FULL_SYNC}")

# Watermark-incremental (oxjob #850): same pattern and table as sync_works, keyed by
# index_name. A failed night no longer ages out of a fixed window — the watermark stays
# put and the next run re-covers the gap.
SYNC_STATE_TABLE = "openalex.works.elastic_sync_state"
FALLBACK_LOOKBACK_DAYS = 2     # incremental fallback when no watermark row exists yet
WATERMARK_BUFFER_SECONDS = 60  # clock skew only. The nightly stamp wave (CreateLocationsMapped)
# lands hours before this task in the same run, so any longer buffer just re-syncs docs
# already sent (sync_works 08-18: a 6h buffer re-sent 42.4M).

# Captured BEFORE any read; becomes the next watermark only if this run finishes clean.
sync_started_at = datetime.utcnow()

def read_watermark():
    if not spark.catalog.tableExists(SYNC_STATE_TABLE):
        return None
    rows = spark.sql(
        f"SELECT last_sync_started_at FROM {SYNC_STATE_TABLE} WHERE index_name = '{CONFIG['index_name']}'"
    ).collect()
    return rows[0].last_sync_started_at if rows else None

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

    count = 0
    errors = []
    try:
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
    finally:
        client.close()

    yield (count, len(errors))

# COMMAND ----------

# Explicit mapping: locations-v1's live mapping, minus the urls.conent_type typo field,
# plus endpoint_id, with updated_date as a real date (was keyword in v1).
# v3 (oxjobs #915/#850/#851, 2026-08-30): date-type ingested_at, published_date,
# created_date, openalex_created_dt, openalex_updated_dt (were keyword in v2 — range
# filters silently matched nothing), and add an analyzed `title.text` subfield so
# /locations search can work (#850). Values arrive as Spark string-cast timestamps
# ("2026-08-29 11:00:05.521", 0-6 fractional digits, space separator) or bare dates,
# hence the explicit format list; ignore_malformed guards feed garbage.
# NOTE (fire-time): delete the stale composable template `locations` (index_patterns
# ["locations-*"], v1-era mapping) BEFORE the create, or it merges into v3:
#   DELETE $ES/_index_template/locations
TS_FORMATS = (
    "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||"
    "yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.S||"
    "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time"
)
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
        "created_date": {"type": "date", "format": TS_FORMATS, "ignore_malformed": True},
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
        "ingested_at": {"type": "date", "format": TS_FORMATS, "ignore_malformed": True},
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
        "openalex_created_dt": {"type": "date", "format": TS_FORMATS, "ignore_malformed": True},
        "openalex_updated_dt": {"type": "date", "format": TS_FORMATS, "ignore_malformed": True},
        "pdf_s3_id": {"type": "keyword"},
        "pdf_url": {"type": "keyword", "ignore_above": 8191},
        "provenance": {"type": "keyword"},
        "published_date": {"type": "date", "format": TS_FORMATS, "ignore_malformed": True},
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
        "title": {"type": "keyword", "ignore_above": 8191, "fields": {"text": {"type": "text"}}},
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
    watermark = read_watermark()
    if watermark is not None:
        sync_since = watermark - timedelta(seconds=WATERMARK_BUFFER_SECONDS)
        print(f"Watermark {watermark} minus {WATERMARK_BUFFER_SECONDS}s buffer -> syncing from {sync_since}")
    else:
        sync_since = sync_started_at - timedelta(days=FALLBACK_LOOKBACK_DAYS)
        print(f"No watermark for {CONFIG['index_name']}; falling back to {FALLBACK_LOOKBACK_DAYS}-day lookback from {sync_since}")
    df = df.filter(F.col("openalex_updated_dt") >= F.lit(sync_since.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))

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
record_count = df.count()
print(f"Total records to process: {record_count:,}")

# Same replica strategy as sync_works: replicas -> 0 for large syncs so bulk indexing
# isn't paying for replica writes. A full sync already dropped them at index setup.
LARGE_SYNC_THRESHOLD = 10_000_000
DROP_REPLICAS = IS_FULL_SYNC or record_count >= LARGE_SYNC_THRESHOLD
if DROP_REPLICAS and not IS_FULL_SYNC:
    client = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True)
    client.indices.put_settings(index=CONFIG["index_name"], body={
        "index": {"number_of_replicas": 0}
    })
    client.close()
    print(f"Large incremental ({record_count:,} docs): set replicas to 0 on {CONFIG['index_name']}")

df = df.repartition(2048)

results_rdd = df.rdd.mapPartitions(lambda p: send_partition_to_elastic(p, CONFIG["index_name"]))
# ONE action only: a second action would recompute the RDD and re-send every partition.
totals = spark.createDataFrame(results_rdd, "indexed_count LONG, error_count LONG").agg(
    F.count("*").alias("partitions"),
    F.sum("indexed_count").alias("indexed"),
    F.sum("error_count").alias("errors"),
).collect()[0]
print(f"Processed {totals.partitions} partitions; indexed {totals.indexed or 0:,} docs, "
      f"{totals.errors or 0} doc errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advance Watermark

# COMMAND ----------

# Advance after a clean-enough run: a failed or partial sync leaves the watermark in
# place, so the next incremental run re-covers the gap (no fixed-window ageout).
# Doc-level failures below ERROR_TOLERANCE must not freeze the watermark and regrow
# the window (sync_works 08-18: 1 error out of 43.9M held back a 44M re-sync);
# tolerated docs are retried only if their rows get re-stamped upstream.
ERROR_TOLERANCE = 500
total_errors = int(totals.errors or 0)

if total_errors >= ERROR_TOLERANCE:
    print(f"Watermark NOT advanced: {total_errors} doc errors (tolerance {ERROR_TOLERANCE}). "
          "The incremental window regrows until a clean run.")
else:
    if total_errors:
        print(f"Tolerating {total_errors} doc errors (< {ERROR_TOLERANCE})")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE} (
  index_name STRING,
  last_sync_started_at TIMESTAMP,
  updated_at TIMESTAMP
)""")
    stamp = sync_started_at.strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""MERGE INTO {SYNC_STATE_TABLE} AS t
USING (SELECT '{CONFIG['index_name']}' AS index_name,
              TIMESTAMP'{stamp}' AS last_sync_started_at,
              CURRENT_TIMESTAMP() AS updated_at) AS s
ON t.index_name = s.index_name
WHEN MATCHED THEN UPDATE SET t.last_sync_started_at = s.last_sync_started_at,
                             t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (index_name, last_sync_started_at, updated_at)
VALUES (s.index_name, s.last_sync_started_at, s.updated_at)
""")
    print(f"Watermark for {CONFIG['index_name']} advanced to {stamp} (UTC)")

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
elif DROP_REPLICAS:
    client.indices.put_settings(index=CONFIG["index_name"], body={
        "index": {"number_of_replicas": 1}
    })
    print(f"Restored replicas to 1 on {CONFIG['index_name']}")

client.close()
