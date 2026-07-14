# Databricks notebook source
# Harvest Meta Tags (wave-2, work-type classifier)
# R2 landing-page HTML -> ALL raw <meta> tag lines + <title> -> Delta table.
# Casey 2026-07-13: ingest ALL meta tags (no family filter — refetching R2 is the expensive
# part; re-parsing a stored tag is a minutes-long job). The classifier's 8-family selection
# happens in a DERIVED layer over this table (byte-compat contract lives there).
# Append-only, resume by anti-join on file_key (taxicab_results conventions).

from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import re

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# COMMAND ----------

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "historical"])
dbutils.widgets.text("target_table", "openalex_dev.rohan_lab.landing_page_meta_tags_dev")
dbutils.widgets.text("num_buckets", "1")       # split the historical run: hash(file_key) % num_buckets
dbutils.widgets.text("bucket", "0")            # which bucket THIS run processes
dbutils.widgets.text("file_limit", "0")        # 0 = no cap; 1000000 = pilot
dbutils.widgets.text("lookback_hours", "48")   # incremental mode window
dbutils.widgets.text("max_threads", "24")      # downloads in flight per partition

mode = dbutils.widgets.get("mode")
target_table = dbutils.widgets.get("target_table")
num_buckets = int(dbutils.widgets.get("num_buckets"))
bucket = int(dbutils.widgets.get("bucket"))
file_limit = int(dbutils.widgets.get("file_limit"))
lookback_hours = int(dbutils.widgets.get("lookback_hours"))
max_threads = int(dbutils.widgets.get("max_threads"))

EXTRACTOR_VERSION = "allmeta-2026-07-13"   # bumped: ALL tags kept (was w2ref-2026-07-11, 8 families)

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

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  file_key             STRING NOT NULL,
  native_id            STRING,
  native_id_namespace  STRING,
  meta_tags            ARRAY<STRING>,
  page_title           STRING,
  nbytes               INT,
  status               STRING,
  error                STRING,
  extractor_version    STRING,
  fetched_at           TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)""")

# COMMAND ----------

response_schema = StructType([
    StructField("meta_tags", ArrayType(StringType()), True),
    StructField("page_title", StringType(), True),
    StructField("nbytes", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True),
])

META_TAG = re.compile(r"<meta\b[^>]*>", re.I)     # ALL meta tags, whole raw line each
TITLE = re.compile(r"<title\b[^>]*>(.*?)</title>", re.I | re.S)

# COMMAND ----------

def fetch_meta_tags_single(file_key):
    """Fetch one blob from R2; keep every raw <meta> line + first <title>. Errors become data."""
    try:
        raw = R2ClientSingleton.get_client().get_object(Bucket=R2_BUCKET, Key=file_key)["Body"].read()
        try:  # content_type lies (octet-stream on gzip bytes) — always try gunzip first
            html = gzip.decompress(raw).decode("utf-8", errors="replace")
        except OSError:
            html = raw.decode("utf-8", errors="replace")
        metas = [m.group(0) for m in META_TAG.finditer(html)]
        t = TITLE.search(html)
        title = re.sub(r"\s+", " ", t.group(1)).strip()[:160] if t else None
        return {"meta_tags": metas, "page_title": title, "nbytes": len(raw),
                "status": "ok", "error": None}
    except Exception as e:
        return {"meta_tags": [], "page_title": None, "nbytes": 0,
                "status": "error", "error": str(e)[:300]}


def process_batch_with_threadpool(file_keys, max_workers):
    """Process a batch of file keys using ThreadPool; preserves input order."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(fetch_meta_tags_single, k): i
                         for i, k in enumerate(file_keys)}
        for future in as_completed(future_to_key):
            results[future_to_key[future]] = future.result()
    return [results[i] for i in range(len(file_keys))]


@F.pandas_udf(response_schema)
def meta_tags_udf_vectorized(file_keys: pd.Series) -> pd.DataFrame:
    """Vectorized pandas UDF: parallel R2 fetch + extraction per partition."""
    return pd.DataFrame(process_batch_with_threadpool(file_keys.tolist(), max_threads))

# COMMAND ----------

# key list: which files does THIS run process = (candidates) - (already succeeded).
# native_id / native_id_namespace follow the openalex convention (works.locations_mapped /
# repo.repo_works): namespace='doi' -> bare lowercase DOI (never 'doi:'- or URL-prefixed);
# namespace='pmh' -> the oai:... handle.
DOI_PREFIX = r"^(doi:|https?://(dx\.)?doi\.org/)"
DOI_PREFIX_SQL = DOI_PREFIX.replace("\\", "\\\\")  # SQL literals eat lone backslashes; keep \. escaped

if mode == "historical":
    # landing_page_works ids — the only index covering all stored files (incl. pre-taxicab).
    # Timestamps here are unreliable (reparses rewrite them); fine for a one-time pass.
    keys = spark.sql(f"""
      SELECT file_key, native_id, native_id_namespace FROM (
        SELECT
          coalesce(concat(get(filter(l.ids, x -> x.namespace = 'html.gz'), 0).id, '.html.gz'),
                   get(filter(l.ids, x -> x.namespace = 'docs.html'), 0).id)       AS file_key,
          coalesce(
            regexp_replace(lower(get(filter(l.ids, x -> x.namespace = 'doi'), 0).id),
                           '{DOI_PREFIX_SQL}', ''),
            get(filter(l.ids, x -> x.namespace = 'pmh'), 0).id,
            l.native_id)                                                            AS native_id,
          CASE WHEN size(filter(l.ids, x -> x.namespace = 'doi')) > 0 THEN 'doi'
               WHEN size(filter(l.ids, x -> x.namespace = 'pmh')) > 0 THEN 'pmh'
               ELSE 'url' END                                                       AS native_id_namespace,
          row_number() OVER (PARTITION BY
            coalesce(concat(get(filter(l.ids, x -> x.namespace = 'html.gz'), 0).id, '.html.gz'),
                     get(filter(l.ids, x -> x.namespace = 'docs.html'), 0).id)
            ORDER BY l.ingested_at DESC)                                            AS rn
        FROM openalex.landing_page.landing_page_works l
      ) WHERE file_key IS NOT NULL AND rn = 1
    """)
else:
    # taxicab_results — append-only feed of newly fetched pages (trustworthy timestamps).
    keys = (
        spark.read.table("openalex.taxicab.taxicab_results")
        .filter(F.col("processed_date") >= F.expr(f"now() - INTERVAL {lookback_hours} HOURS"))
        .filter(F.col("s3_path").isNotNull())
        .filter(F.lower(F.col("content_type")).rlike("html|octet|gz"))
        .select(
            F.regexp_extract("s3_path", r"[^/]+$", 0).alias("file_key"),
            F.when(F.col("native_id_namespace") == "doi",
                   F.regexp_replace(F.lower("native_id"), DOI_PREFIX, ""))
             .otherwise(F.col("native_id")).alias("native_id"),
            F.col("native_id_namespace"),
        )
        .filter(F.col("file_key") != "")
        .dropDuplicates(["file_key"]))

if num_buckets > 1:
    keys = keys.filter((F.abs(F.hash("file_key")) % num_buckets) == bucket)

# extractor_version is provenance ONLY — it never drives resume. If it ever changes, a full
# R2 re-extraction (~304M files, days) must be a deliberate act: new table or explicit
# delete of old-version rows. This tripwire prevents both silent-skip and silent-refetch.
existing = [r.extractor_version for r in
            spark.read.table(target_table).filter("status = 'ok'")
            .select("extractor_version").distinct().collect()]
if existing and set(existing) != {EXTRACTOR_VERSION}:
    raise Exception(f"Table has ok-rows from other extractor version(s) {existing} vs "
                    f"current '{EXTRACTOR_VERSION}'. Re-extraction must be deliberate — "
                    "write to a new table or delete old-version rows first.")

# resume/retry: skip only files that already SUCCEEDED — error rows (e.g. NoSuchKey on very
# fresh scrapes) retry on later runs WHILE their taxicab row is inside the lookback window
# (daily run + 48h window = one retry day); older misses are caught by historical passes
done = spark.read.table(target_table).filter("status = 'ok'").select("file_key")
keys = keys.join(done, "file_key", "left_anti")
if file_limit > 0:
    keys = keys.limit(file_limit)

# cache: count() + write would otherwise each rebuild the full key plan (scan + window + anti-join).
# Bucketed runs cache ~9.5M rows ≈ ~1GB — trivial.
# snapshot semantics: the list is frozen before the write; files scraped mid-run belong to the next run (desired).
keys = keys.cache()

print(f"mode={mode} bucket={bucket}/{num_buckets} | files to parse: {keys.count():,}")

# COMMAND ----------

# run_start bounds THIS run's rows for the stats cell (fetched_at = current_timestamp() is
# fixed at write-query start, so every appended row lands at >= run_start)
run_start = spark.sql("SELECT current_timestamp() AS t").collect()[0].t

result_df = (
    keys.repartition(max(8, sc.defaultParallelism * 2))
    .withColumn("r", meta_tags_udf_vectorized(F.col("file_key")))
    .select(
        "file_key", "native_id", "native_id_namespace",
        F.col("r.meta_tags").alias("meta_tags"),
        F.col("r.page_title").alias("page_title"),
        F.col("r.nbytes").alias("nbytes"),
        F.col("r.status").alias("status"),
        F.col("r.error").alias("error"),
        F.lit(EXTRACTOR_VERSION).alias("extractor_version"),
        F.current_timestamp().alias("fetched_at"),
    ))

result_df.write.mode("append").saveAsTable(target_table)

# COMMAND ----------

t = spark.read.table(target_table)
this_run = t.filter(F.col("fetched_at") >= F.lit(run_start))

print("=== THIS RUN ===")
this_run.groupBy("status").count().show()
# rate-limit visibility: boto3 puts the error class in the message (SlowDown / TooManyRequests
# / 429) — when R2 starts throttling during scale-up it shows here, grouped
this_run.filter(F.col("status") == "error").groupBy(
    F.substring("error", 1, 80).alias("error_prefix")
).count().orderBy(F.desc("count")).show(10, truncate=False)
this_run.filter(F.col("status") == "ok").select(
    F.avg(F.size("meta_tags")).alias("avg_tags"),
    F.avg((F.size("meta_tags") > 0).cast("int")).alias("any_tag_frac"),
    F.avg(F.col("page_title").isNotNull().cast("int")).alias("title_frac"),
    F.avg("nbytes").alias("avg_bytes"),
).show()

print("=== TABLE CUMULATIVE ===")
t.groupBy("status").count().show()
t.groupBy("native_id_namespace").count().show()
