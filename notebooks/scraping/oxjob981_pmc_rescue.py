# Databricks notebook source
# MAGIC %md
# MAGIC # #981 PMC rescue: one-off copy of walden `notebooks/scraping/pmc_pdf_sweep.py` (main, 17 Nov 2026)
# MAGIC
# MAGIC Only change: the works it serves. The daily sweep takes works that show no PDF; this run takes the works #981's
# MAGIC scan found serving a damaged PDF (read from `works_file`, one `work_id` column), which still show one until the
# MAGIC strip. Everything else (bucket, md5 + %PDF- checks, `store_pdf`, the `taxicab_results` row) is the sweep's.
# MAGIC Revert: rows it wrote carry a `resolved_url` in the bucket and processed_date of this run.

# COMMAND ----------

import datetime
import hashlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import boto3
import requests
from botocore.config import Config
from pyspark.sql import functions as F
from pyspark.sql import types as T
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

dbutils.widgets.text("max_records", "50000", "Max records per run")
dbutils.widgets.text("min_age_hours", "48", "Only records ingested at least this long ago (TaxiCab goes first)")
dbutils.widgets.text("retry_days", "7", "Retry a record whose last sweep attempt failed after this many days")
dbutils.widgets.text("dry_run", "false", "true = select and report only")
dbutils.widgets.text("works_file", "", "parquet with a work_id column: the works to rescue")

MAX_RECORDS = int(dbutils.widgets.get("max_records"))
MIN_AGE_HOURS = int(dbutils.widgets.get("min_age_hours"))
RETRY_DAYS = int(dbutils.widgets.get("retry_days"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

BUCKET = "https://pmc-oa-opendata.s3.amazonaws.com/"
USER_AGENT = "OpenAlex PMC PDF sweep (https://openalex.org; team@ourresearch.org)"
PDF_BUCKET = "openalex-pdfs"
PMH_PREFIX = "oai:pubmedcentral.nih.gov:"

# COMMAND ----------

# MAGIC %md ## Records to fetch

# COMMAND ----------

pmc_tx = spark.table("openalex.taxicab.taxicab_results").filter(F.col("native_id").startswith(PMH_PREFIX))
stored = (
    pmc_tx.filter(F.col("content_type").contains("pdf") & (F.col("status_code") == 200) & F.col("s3_path").isNotNull())
    .select("native_id").distinct()
)
recent_misses = (
    pmc_tx.filter(F.col("error").startswith("pmc_sweep")
                  & (F.col("processed_date") >= F.expr(f"current_timestamp() - INTERVAL {RETRY_DAYS} DAYS")))
    .select("native_id").distinct()
)
# latest version of each PMC article in the bucket; pdf_url looks like s3://pmc-oa-opendata/PMC123.1/PMC123.1.pdf?md5=...
bucket = (
    spark.table("openalex.pubmed_central.pmc_oa_articles")
    .filter(F.col("error").isNull())
    .groupBy(F.regexp_extract("pmcid", r"(\d+)", 1).alias("d"))
    .agg(F.max_by(F.struct("is_pmc_openaccess", "is_manuscript", "pdf_url"), "version").alias("s"))
    .filter(F.col("s.is_pmc_openaccess") & ~F.col("s.is_manuscript") & F.col("s.pdf_url").isNotNull())
    .select("d",
            F.regexp_extract("s.pdf_url", r"^s3://pmc-oa-opendata/([^?]+)", 1).alias("bucket_key"),
            F.regexp_extract("s.pdf_url", r"md5=([0-9a-f]{32})", 1).alias("md5"))
    .filter(F.col("bucket_key") != "")
)
# the works #981 found serving a damaged PDF (they still show has_content.pdf true, so the daily sweep skips them)
rescue_works = spark.read.parquet(dbutils.widgets.get("works_file")).select("work_id").distinct()
no_pdf_works = (
    spark.table("openalex.works.openalex_works")
    .join(rescue_works, F.col("id") == F.col("work_id"))
    .filter(F.col("ids")["pmcid"].isNotNull())
    .select(F.regexp_extract(F.col("ids")["pmcid"], r"(\d+)", 1).alias("d"))
    .distinct()
)
candidates = (
    spark.table("openalex.repo.repo_works")
    .filter(F.col("native_id").startswith(PMH_PREFIX)
            & (F.col("ingested_at") <= F.expr(f"current_timestamp() - INTERVAL {MIN_AGE_HOURS} HOURS")))
    .select("native_id").distinct()
    .withColumn("d", F.expr(f"substring(native_id, {len(PMH_PREFIX) + 1})"))
    .join(no_pdf_works, "d")
    .join(bucket, "d")
    .join(stored, "native_id", "left_anti")
    .join(recent_misses, "native_id", "left_anti")
    .orderBy(F.col("d").cast("long").desc())  # newest first
    .limit(MAX_RECORDS)
)
todo = [(r.native_id, r.d, r.bucket_key, r.md5) for r in candidates.collect()]
print(f"{len(todo):,} PMC records with no PDF to fetch (cap {MAX_RECORDS:,}); "
      f"first {[t[1] for t in todo[:5]]}, last {[t[1] for t in todo[-5:]]}")
if DRY_RUN or not todo:
    dbutils.notebook.exit(f"dry_run={DRY_RUN}, candidates={len(todo)}, "
                          f"first={[t[1] for t in todo[:5]]}, last={[t[1] for t in todo[-5:]]}")

# COMMAND ----------

# MAGIC %md ## Fetch, check, store

# COMMAND ----------

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT
session.mount("https://", HTTPAdapter(pool_maxsize=32, max_retries=Retry(
    total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])))
s3 = boto3.client(
    "s3",
    endpoint_url=dbutils.secrets.get("cloudflare", "r2-endpoint"),
    aws_access_key_id=dbutils.secrets.get("cloudflare", "r2-access-key-id"),
    aws_secret_access_key=dbutils.secrets.get("cloudflare", "r2-secret-access-key"),
    region_name="auto",
    config=Config(max_pool_connections=32, retries={"max_attempts": 6}),
)
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.pdf_store import md5_table, store_pdf  # noqa: E402

pdf_md5 = md5_table(dbutils)
reused = []


def pdf_url(digits):
    # PMC's own address for the article's PDF: a browser is redirected to the file
    return f"https://pmc.ncbi.nlm.nih.gov/articles/PMC{digits}/pdf/"


def miss(native_id, digits, why, status=None):
    now = datetime.datetime.utcnow()
    return (None, pdf_url(digits), None, status, None, native_id, "pmh", None, False, now, now, f"pmc_sweep: {why}"[:500])


def fetch(item):
    native_id, digits, key, md5 = item
    try:
        r = session.get(BUCKET + key, timeout=300)
        if r.status_code != 200:
            return miss(native_id, digits, f"bucket {r.status_code}", r.status_code)
        body = r.content
        if md5 and hashlib.md5(body).hexdigest() != md5:
            return miss(native_id, digits, "bucket md5 mismatch")
        if len(body) < 100 or not body[:1024].lstrip().startswith(b"%PDF-"):
            return miss(native_id, digits, "bucket object is not a PDF")
        now = datetime.datetime.utcnow()
        pdf_uuid, was_reused = store_pdf(s3, pdf_md5, body, {
            "url": quote(pdf_url(digits)), "resolved_url": quote(BUCKET + key), "created_date": now.isoformat(),
            "content_type": "pdf", "native_id": native_id, "native_id_namespace": "pmh", "source": "pmc_pdf_sweep"})
        if was_reused:
            reused.append(pdf_uuid)
        return (pdf_uuid, pdf_url(digits), BUCKET + key, 200, "pdf", native_id, "pmh",
                f"s3://{PDF_BUCKET}/{pdf_uuid}.pdf", False, now, now, None)
    except Exception as e:
        return miss(native_id, digits, f"{type(e).__name__}: {e}")


t0 = time.time()
with ThreadPoolExecutor(max_workers=16) as ex:
    rows = list(ex.map(fetch, todo))
n_stored = sum(1 for r in rows if r[7])
print(f"bucket: {n_stored:,} stored ({len(reused):,} of them a file we already held), {len(rows) - n_stored:,} misses "
      f"({time.time() - t0:.0f}s)")

# COMMAND ----------

# MAGIC %md ## Write `taxicab_results` rows (stored PDFs and misses)

# COMMAND ----------

schema = T.StructType([
    T.StructField("taxicab_id", T.StringType()), T.StructField("url", T.StringType()),
    T.StructField("resolved_url", T.StringType()), T.StructField("status_code", T.IntegerType()),
    T.StructField("content_type", T.StringType()), T.StructField("native_id", T.StringType()),
    T.StructField("native_id_namespace", T.StringType()), T.StructField("s3_path", T.StringType()),
    T.StructField("is_soft_block", T.BooleanType()), T.StructField("created_date", T.TimestampType()),
    T.StructField("processed_date", T.TimestampType()), T.StructField("error", T.StringType()),
])
spark.createDataFrame(rows, schema).write.mode("append").format("delta").saveAsTable("openalex.taxicab.taxicab_results")
print(f"Wrote {len(rows):,} rows: {n_stored:,} PDFs stored, {len(rows) - n_stored:,} misses ({time.time() - t0:.0f}s)")
