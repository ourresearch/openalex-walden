# Databricks notebook source
# MAGIC %md
# MAGIC # arXiv PDF sweep (oxjob #1987)
# MAGIC
# MAGIC Every arXiv record should have its PDF. TaxiCab fetches a repository record only while its `ingested_at` is inside
# MAGIC the lookback window and never re-queues a miss, so on 3 Nov 2026 ~1.84M of 3.23M arXiv records had never been
# MAGIC fetched, and ~72K PDF fetches had stored an HTML page. This sweep has no time window: each run takes arXiv records
# MAGIC (`oai:arXiv.org:*` in `repo_works`) that are at least `min_age_hours` old and have **no stored PDF** in
# MAGIC `taxicab_results`, and fetches the paper's latest version
# MAGIC
# MAGIC 1. from arXiv's public bulk mirror on Google Cloud (`gs://arxiv-dataset`, listed on arXiv's bulk-data page; no
# MAGIC    load on arxiv.org), or, when the mirror does not have it yet (it lags ~1-2 weeks),
# MAGIC 2. from `https://arxiv.org/pdf/<id>`, one request every `arxiv_pace_seconds` (arXiv's guidance for automated
# MAGIC    access), at most `arxiv_limit` per run.
# MAGIC
# MAGIC A PDF is stored exactly as TaxiCab stores one (R2 `openalex-pdfs/<uuid4>.pdf`, same object metadata) and gets a
# MAGIC `taxicab_results` row (`content_type='pdf'`, `url='https://arxiv.org/pdf/<id>'`, `resolved_url` = where the bytes
# MAGIC came from), so the 00:30 UTC Parse PDFs run parses it and the PDF reaches the work through the usual path.
# MAGIC A miss is written as a row too (no `s3_path`, `error` set), and a record is retried after `retry_days`.
# MAGIC
# MAGIC Revert: pause the job. Rows it wrote carry `resolved_url` from the mirror or `error LIKE 'arxiv_sweep%'`.

# COMMAND ----------

import base64
import datetime
import hashlib
import os
import re
import time
import uuid
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
dbutils.widgets.text("arxiv_limit", "1500", "Max fetches from arxiv.org per run (mirror misses)")
dbutils.widgets.text("arxiv_pace_seconds", "3", "Seconds between arxiv.org fetches")
dbutils.widgets.text("dry_run", "false", "true = select and report only")

MAX_RECORDS = int(dbutils.widgets.get("max_records"))
MIN_AGE_HOURS = int(dbutils.widgets.get("min_age_hours"))
RETRY_DAYS = int(dbutils.widgets.get("retry_days"))
ARXIV_LIMIT = int(dbutils.widgets.get("arxiv_limit"))
ARXIV_PACE = float(dbutils.widgets.get("arxiv_pace_seconds"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

MIRROR = "https://storage.googleapis.com/arxiv-dataset/"
LIST_API = "https://storage.googleapis.com/storage/v1/b/arxiv-dataset/o"
USER_AGENT = "OpenAlex arXiv PDF sweep (https://openalex.org; team@ourresearch.org)"
PDF_BUCKET = "openalex-pdfs"

# COMMAND ----------

# MAGIC %md ## Records to fetch

# COMMAND ----------

stored = (
    spark.table("openalex.taxicab.taxicab_results")
    .filter(F.col("native_id").startswith("oai:arXiv.org:")
            & F.col("content_type").contains("pdf") & (F.col("status_code") == 200) & F.col("s3_path").isNotNull())
    .select("native_id").distinct()
)
recent_misses = (
    spark.table("openalex.taxicab.taxicab_results")
    .filter(F.col("native_id").startswith("oai:arXiv.org:") & F.col("error").startswith("arxiv_sweep")
            & (F.col("processed_date") >= F.expr(f"current_timestamp() - INTERVAL {RETRY_DAYS} DAYS")))
    .select("native_id").distinct()
)
candidates = (
    spark.table("openalex.repo.repo_works")
    .filter(F.col("native_id").startswith("oai:arXiv.org:")
            & (F.col("ingested_at") <= F.expr(f"current_timestamp() - INTERVAL {MIN_AGE_HOURS} HOURS")))
    .select("native_id").distinct()
    .join(stored, "native_id", "left_anti")
    .join(recent_misses, "native_id", "left_anti")
    .withColumn("arxiv_id", F.expr("substring(native_id, 15)"))
    .orderBy(F.desc("arxiv_id"))  # newest first: they matter most and the old backlog was backfilled (#1987)
    .limit(MAX_RECORDS)
)
todo = [(r.native_id, r.arxiv_id) for r in candidates.collect()]
print(f"{len(todo):,} arXiv records with no stored PDF (cap {MAX_RECORDS:,})")
if DRY_RUN or not todo:
    dbutils.notebook.exit(f"dry_run={DRY_RUN}, candidates={len(todo)}")

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


def mirror_object(arxiv_id):
    """(object name, md5) of the paper's highest version in the mirror, or None."""
    if "/" in arxiv_id:  # old style: hep-th/9901001 -> arxiv/hep-th/pdf/9901/9901001v<N>.pdf
        archive, num = arxiv_id.split("/", 1)
        prefix = f"arxiv/{archive}/pdf/{num[:4]}/{num}v"
    else:                # 2503.09516 -> arxiv/arxiv/pdf/2503/2503.09516v<N>.pdf
        prefix = f"arxiv/arxiv/pdf/{arxiv_id[:4]}/{arxiv_id}v"
    r = session.get(LIST_API, params={"prefix": prefix, "fields": "items(name,md5Hash)"}, timeout=60)
    r.raise_for_status()
    best = None
    for it in r.json().get("items", []):
        m = re.search(r"v(\d+)\.pdf$", it["name"])
        if m and (best is None or int(m.group(1)) > best[0]):
            best = (int(m.group(1)), it["name"], it.get("md5Hash"))
    return (best[1], best[2]) if best else None


def is_pdf(body):
    return len(body) >= 100 and body[:1024].lstrip().startswith(b"%PDF-")


def store(native_id, arxiv_id, body, source_url):
    pdf_uuid = str(uuid.uuid4())
    url = f"https://arxiv.org/pdf/{arxiv_id}"
    now = datetime.datetime.utcnow()
    s3.put_object(Bucket=PDF_BUCKET, Key=f"{pdf_uuid}.pdf", Body=body, Metadata={
        "url": quote(url), "resolved_url": quote(source_url), "created_date": now.isoformat(),
        "content_type": "pdf", "id": pdf_uuid, "native_id": native_id, "native_id_namespace": "pmh",
        "source": "arxiv_pdf_sweep"})
    return (pdf_uuid, url, source_url, 200, "pdf", native_id, "pmh", f"s3://{PDF_BUCKET}/{pdf_uuid}.pdf", False, now, now, None)


def miss(native_id, arxiv_id, why, status=None):
    now = datetime.datetime.utcnow()
    return (None, f"https://arxiv.org/pdf/{arxiv_id}", None, status, None, native_id, "pmh", None, False, now, now,
            f"arxiv_sweep: {why}"[:500])


def from_mirror(item):
    native_id, arxiv_id = item
    try:
        hit = mirror_object(arxiv_id)
        if not hit:
            return ("not_in_mirror", item)
        name, md5 = hit
        r = session.get(MIRROR + name, timeout=300)
        r.raise_for_status()
        body = r.content
        if md5 and base64.b64encode(hashlib.md5(body).digest()).decode() != md5:
            return ("row", miss(native_id, arxiv_id, "mirror md5 mismatch"))
        if not is_pdf(body):
            return ("row", miss(native_id, arxiv_id, "mirror object is not a PDF"))
        return ("row", store(native_id, arxiv_id, body, MIRROR + name))
    except Exception as e:
        return ("row", miss(native_id, arxiv_id, f"mirror {type(e).__name__}: {e}"))


t0 = time.time()
rows, mirror_misses = [], []
with ThreadPoolExecutor(max_workers=16) as ex:
    for kind, val in ex.map(from_mirror, todo):
        (mirror_misses if kind == "not_in_mirror" else rows).append(val)
print(f"mirror: {sum(1 for r in rows if r[7]):,} stored, {sum(1 for r in rows if not r[7]):,} failed, "
      f"{len(mirror_misses):,} not in the mirror ({time.time() - t0:.0f}s)")

# arxiv.org, paced, newest first (the mirror lags; these are mostly the last couple of weeks)
for native_id, arxiv_id in mirror_misses[:ARXIV_LIMIT]:
    started = time.time()
    try:
        r = session.get(f"https://arxiv.org/pdf/{arxiv_id}", timeout=120)
        if r.status_code == 200 and is_pdf(r.content):
            rows.append(store(native_id, arxiv_id, r.content, r.url))
        else:
            rows.append(miss(native_id, arxiv_id, f"arxiv.org {r.status_code} {r.headers.get('content-type', '')}",
                             r.status_code))
    except Exception as e:
        rows.append(miss(native_id, arxiv_id, f"arxiv.org {type(e).__name__}: {e}"))
    time.sleep(max(0.0, ARXIV_PACE - (time.time() - started)))
print(f"arxiv.org: tried {min(len(mirror_misses), ARXIV_LIMIT):,} (cap {ARXIV_LIMIT:,}); "
      f"{max(0, len(mirror_misses) - ARXIV_LIMIT):,} left for the next run")

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
if rows:
    spark.createDataFrame(rows, schema).write.mode("append").format("delta").saveAsTable("openalex.taxicab.taxicab_results")
n_stored = sum(1 for r in rows if r[7])
print(f"Wrote {len(rows):,} rows: {n_stored:,} PDFs stored, {len(rows) - n_stored:,} misses ({time.time() - t0:.0f}s)")
