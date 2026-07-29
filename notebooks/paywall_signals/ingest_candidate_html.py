# Databricks notebook source
# Ingest PDF-candidate landing-page HTML (oxjob #695)
# R2 landing-page HTML -> full decompressed body -> Delta table, for paywall/free
# classification of the #682 never-attempted PDF-candidate pot. Clone of
# notebooks/meta_tags/harvest_meta_tags.py conventions: append-only, resume by
# anti-join on file_key, bucketed runs. Cohort = Source-3-replica filters; rebuild
# is gated (rebuild_cohort) so parallel bucket runs share one snapshot.

from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# COMMAND ----------

dbutils.widgets.dropdown("rebuild_cohort", "false", ["false", "true"])
dbutils.widgets.text("cohort_table", "openalex.parseland.pdf_candidate_cohort")
dbutils.widgets.text("target_table", "openalex.landing_page.pdf_candidate_html")
dbutils.widgets.text("num_buckets", "1")       # split the run: hash(file_key) % num_buckets
dbutils.widgets.text("bucket", "0")            # which bucket THIS run processes
dbutils.widgets.text("file_limit", "0")        # 0 = no cap; small value = pilot
dbutils.widgets.text("max_threads", "24")      # downloads in flight per partition

rebuild_cohort = dbutils.widgets.get("rebuild_cohort") == "true"
cohort_table = dbutils.widgets.get("cohort_table")
target_table = dbutils.widgets.get("target_table")
num_buckets = int(dbutils.widgets.get("num_buckets"))
bucket = int(dbutils.widgets.get("bucket"))
file_limit = int(dbutils.widgets.get("file_limit"))
max_threads = int(dbutils.widgets.get("max_threads"))

INGEST_VERSION = "fullhtml-2026-07-29"
HTML_CAP = 2_000_000   # chars; truncated flag records the cut

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "200")  # full-HTML rows are wide

# COMMAND ----------

R2_ENDPOINT = dbutils.secrets.get("meta-tags-r2", "r2-endpoint")
if not R2_ENDPOINT.startswith("http"):
    R2_ENDPOINT = "https://" + R2_ENDPOINT
R2_ACCESS_KEY = dbutils.secrets.get("meta-tags-r2", "r2-access-key-id")
R2_SECRET_KEY = dbutils.secrets.get("meta-tags-r2", "r2-secret-access-key")
R2_BUCKET = "openalex-html"


class R2ClientSingleton:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            import boto3
            from botocore.config import Config
            cls._client = boto3.client(
                "s3",
                endpoint_url=R2_ENDPOINT,
                aws_access_key_id=R2_ACCESS_KEY,
                aws_secret_access_key=R2_SECRET_KEY,
                config=Config(retries={"max_attempts": 3}, connect_timeout=10,
                              read_timeout=60, max_pool_connections=max_threads * 2),
            )
        return cls._client

# COMMAND ----------

# Cohort snapshot (Source-3 replica, validated vs #682's 2026-07-29 measurement):
# campaign-fresh landing_page_works exposing a content_type='pdf' URL, re-keyed
# pmh > doi > native, minus (a) works with a taxicab 200/pdf/s3_path harvest,
# (b) works holding a pdf_s3_id/grobid_s3_id in locations_mapped, (c) pdf URLs
# ever attempted in taxicab_results.
DOI_PREFIX = r"^(doi:|https?://(dx\.)?doi\.org/)"
DOI_PREFIX_SQL = DOI_PREFIX.replace("\\", "\\\\")

if rebuild_cohort:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {cohort_table} AS
    WITH fresh AS (
      SELECT
        coalesce(concat(get(filter(l.ids, x -> x.namespace = 'html.gz'), 0).id, '.html.gz'),
                 get(filter(l.ids, x -> x.namespace = 'docs.html'), 0).id)          AS file_key,
        coalesce(get(filter(l.ids, x -> x.namespace = 'pmh'), 0).id,
                 regexp_replace(lower(get(filter(l.ids, x -> x.namespace = 'doi'), 0).id),
                                '{DOI_PREFIX_SQL}', ''),
                 l.native_id)                                                        AS work_key,
        CASE WHEN size(filter(l.ids, x -> x.namespace = 'pmh')) > 0 THEN 'pmh'
             WHEN size(filter(l.ids, x -> x.namespace = 'doi')) > 0 THEN 'doi'
             ELSE 'native' END                                                       AS work_key_ns,
        l.native_id, l.native_id_namespace,
        get(filter(l.urls, u -> u.content_type = 'pdf'), 0).url                      AS pdf_url,
        l.publisher,
        row_number() OVER (PARTITION BY
          coalesce(get(filter(l.ids, x -> x.namespace = 'pmh'), 0).id,
                   regexp_replace(lower(get(filter(l.ids, x -> x.namespace = 'doi'), 0).id),
                                  '{DOI_PREFIX_SQL}', ''),
                   l.native_id)
          ORDER BY l.ingested_at DESC)                                               AS rn
      FROM openalex.landing_page.landing_page_works l
      WHERE l.created_date >= '2026-07-27'
        AND size(filter(l.urls, u -> u.content_type = 'pdf')) > 0
    ),
    cand AS (SELECT * FROM fresh WHERE rn = 1 AND file_key IS NOT NULL AND pdf_url IS NOT NULL),
    harvested AS (
      SELECT DISTINCT
        CASE WHEN native_id_namespace = 'doi'
             THEN regexp_replace(lower(native_id), '{DOI_PREFIX_SQL}', '')
             ELSE native_id END AS work_key
      FROM openalex.taxicab.taxicab_results
      WHERE status_code = 200 AND content_type LIKE '%pdf%' AND s3_path IS NOT NULL
    ),
    held_works AS (
      SELECT DISTINCT work_id
      FROM openalex.works.locations_mapped
      WHERE work_id IS NOT NULL
        AND (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
    ),
    cand_work AS (
      SELECT c.work_key, lm.work_id
      FROM cand c
      JOIN openalex.works.locations_mapped lm
        ON lm.native_id = c.native_id AND lm.native_id_namespace = c.native_id_namespace
      WHERE lm.work_id IS NOT NULL
    ),
    attempted_urls AS (SELECT DISTINCT url FROM openalex.taxicab.taxicab_results)
    SELECT
      c.file_key, c.work_key, c.work_key_ns, c.native_id, c.native_id_namespace,
      c.pdf_url, lower(parse_url(c.pdf_url, 'HOST')) AS url_host, c.publisher,
      current_timestamp() AS snapshot_at
    FROM cand c
    LEFT ANTI JOIN harvested h ON c.work_key = h.work_key
    LEFT ANTI JOIN (SELECT DISTINCT cw.work_key
                    FROM cand_work cw JOIN held_works hw ON cw.work_id = hw.work_id) held
      ON c.work_key = held.work_key
    LEFT ANTI JOIN attempted_urls a ON c.pdf_url = a.url
    """)
    print(f"cohort rebuilt: {spark.read.table(cohort_table).count():,} candidates")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  file_key             STRING NOT NULL,
  work_key             STRING,
  work_key_ns          STRING,
  native_id            STRING,
  native_id_namespace  STRING,
  pdf_url              STRING,
  url_host             STRING,
  publisher            STRING,
  html                 STRING,
  truncated            BOOLEAN,
  nbytes               INT,
  status               STRING,
  error                STRING,
  ingest_version       STRING,
  fetched_at           TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)""")

# COMMAND ----------

response_schema = StructType([
    StructField("html", StringType(), True),
    StructField("truncated", BooleanType(), True),
    StructField("nbytes", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True),
])


def fetch_html_single(file_key):
    """Fetch one blob from R2; return full decompressed body. Errors become data."""
    try:
        raw = R2ClientSingleton.get_client().get_object(Bucket=R2_BUCKET, Key=file_key)["Body"].read()
        try:  # content_type lies (octet-stream on gzip bytes) — always try gunzip first
            html = gzip.decompress(raw).decode("utf-8", errors="replace")
        except OSError:
            html = raw.decode("utf-8", errors="replace")
        truncated = len(html) > HTML_CAP
        return {"html": html[:HTML_CAP], "truncated": truncated, "nbytes": len(raw),
                "status": "ok", "error": None}
    except Exception as e:
        return {"html": None, "truncated": False, "nbytes": 0,
                "status": "error", "error": str(e)[:300]}


def process_batch_with_threadpool(file_keys, max_workers):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(fetch_html_single, k): i
                         for i, k in enumerate(file_keys)}
        for future in as_completed(future_to_key):
            results[future_to_key[future]] = future.result()
    return [results[i] for i in range(len(file_keys))]


@F.pandas_udf(response_schema)
def fetch_html_udf(file_keys: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(process_batch_with_threadpool(file_keys.tolist(), max_threads))

# COMMAND ----------

keys = spark.read.table(cohort_table).select(
    "file_key", "work_key", "work_key_ns", "native_id", "native_id_namespace",
    "pdf_url", "url_host", "publisher").dropDuplicates(["file_key"])

if num_buckets > 1:
    keys = keys.filter((F.abs(F.hash("file_key")) % num_buckets) == bucket)

existing = [r.ingest_version for r in
            spark.read.table(target_table).filter("status = 'ok'")
            .select("ingest_version").distinct().collect()]
if existing and set(existing) != {INGEST_VERSION}:
    raise Exception(f"Table has ok-rows from other ingest version(s) {existing} vs "
                    f"current '{INGEST_VERSION}'. Re-ingestion must be deliberate — "
                    "write to a new table or delete old-version rows first.")

# resume/retry: skip only files that already SUCCEEDED — error rows retry on later runs
done = spark.read.table(target_table).filter("status = 'ok'").select("file_key")
keys = keys.join(done, "file_key", "left_anti")
if file_limit > 0:
    keys = keys.limit(file_limit)

keys = keys.cache()
print(f"bucket={bucket}/{num_buckets} | files to ingest: {keys.count():,}")

# COMMAND ----------

run_start = spark.sql("SELECT current_timestamp() AS t").collect()[0].t

result_df = (
    keys.repartition(max(8, sc.defaultParallelism * 2))
    .withColumn("r", fetch_html_udf(F.col("file_key")))
    .select(
        "file_key", "work_key", "work_key_ns", "native_id", "native_id_namespace",
        "pdf_url", "url_host", "publisher",
        F.col("r.html").alias("html"),
        F.col("r.truncated").alias("truncated"),
        F.col("r.nbytes").alias("nbytes"),
        F.col("r.status").alias("status"),
        F.col("r.error").alias("error"),
        F.lit(INGEST_VERSION).alias("ingest_version"),
        F.current_timestamp().alias("fetched_at"),
    ))

result_df.write.mode("append").saveAsTable(target_table)

# COMMAND ----------

t = spark.read.table(target_table)
this_run = t.filter(F.col("fetched_at") >= F.lit(run_start))

print("=== THIS RUN ===")
this_run.groupBy("status").count().show()
this_run.filter(F.col("status") == "error").groupBy(
    F.substring("error", 1, 80).alias("error_prefix")
).count().orderBy(F.desc("count")).show(10, truncate=False)
this_run.filter(F.col("status") == "ok").select(
    F.avg(F.length("html")).alias("avg_html_chars"),
    F.avg(F.col("truncated").cast("int")).alias("truncated_frac"),
    F.avg("nbytes").alias("avg_gz_bytes"),
).show()

print("=== TABLE CUMULATIVE ===")
t.groupBy("status").count().show()
