# Databricks notebook source
# MAGIC %md
# MAGIC # Scan PDF Bytes in R2
# MAGIC
# MAGIC Classifies the first 1024 bytes of every `pdf_s3_id` in `locations_mapped`
# MAGIC to identify invalid PDFs (HTML, gzipped landing pages, error text, etc.)
# MAGIC stored in the `openalex-pdfs` R2 bucket.
# MAGIC
# MAGIC **Source**: `openalex.works.locations_mapped` (one row per work_id)
# MAGIC **Target**: `openalex.works.pdf_byte_classification{suffix}` (Delta)
# MAGIC
# MAGIC Classifier mirrors `make_validation_set.py` (used to label the 10K
# MAGIC validation set under #185) so labels are directly comparable.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `sample_limit`: 0 = full scan (~64.9M). >0 = random sample (for pilot).
# MAGIC   - `target_suffix`: appended to the output table name (e.g. `_pilot`).

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# R2 / bucket config — see notebooks/elastic/export_content_manifest_to_r2.py
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "openalex-pdfs"

# 1024 bytes matches make_validation_set.py / the 10K validation set
PROBE_BYTES = 1024
THREADS_PER_TASK = 32

R2_ACCESS_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-access-key-id")
R2_SECRET_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-secret-access-key")

dbutils.widgets.text("sample_limit", "100000", "Sample limit (0 = full scan)")
dbutils.widgets.text("target_suffix", "_pilot", "Target table suffix (e.g. '_pilot' or '')")
dbutils.widgets.text("num_partitions", "128", "Spark partitions for the probe")

SAMPLE_LIMIT = int(dbutils.widgets.get("sample_limit"))
TARGET_SUFFIX = dbutils.widgets.get("target_suffix").strip()
NUM_PARTITIONS = int(dbutils.widgets.get("num_partitions"))

TARGET_TABLE = f"openalex.works.pdf_byte_classification{TARGET_SUFFIX}"
print(f"sample_limit={SAMPLE_LIMIT:,}  target={TARGET_TABLE}  partitions={NUM_PARTITIONS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the work list

# COMMAND ----------

# For a pilot, TABLESAMPLE pushes the sample into the file scan so we don't
# pay for the GROUP BY shuffle over the full ~71M-row matching set. A
# given pdf_s3_id may map to multiple work_ids; for byte-validation that
# doesn't matter — probing the same UUID twice is harmless and rare at low
# sample rates. The full run does GROUP BY since we want one row per work.
if SAMPLE_LIMIT > 0:
    sample_pct = min(100.0, max(SAMPLE_LIMIT / 70_000_000 * 100 * 3, 0.01))
    df = spark.sql(f"""
        SELECT work_id, pdf_s3_id
        FROM openalex.works.locations_mapped TABLESAMPLE ({sample_pct} PERCENT)
        WHERE pdf_s3_id IS NOT NULL
          AND work_id  IS NOT NULL
        LIMIT {SAMPLE_LIMIT}
    """)
else:
    df = spark.sql("""
        SELECT
          work_id,
          MIN(pdf_s3_id) AS pdf_s3_id
        FROM openalex.works.locations_mapped
        WHERE pdf_s3_id IS NOT NULL
          AND work_id  IS NOT NULL
        GROUP BY work_id
    """)

df = df.repartition(NUM_PARTITIONS)
total = df.count()
print(f"Scanning {total:,} PDFs across {NUM_PARTITIONS} partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe + classify (mapPartitions)

# COMMAND ----------

# Capture into module-scope locals for closure serialization.
_R2_ENDPOINT = R2_ENDPOINT
_R2_BUCKET = R2_BUCKET
_R2_ACCESS_KEY = R2_ACCESS_KEY
_R2_SECRET_KEY = R2_SECRET_KEY
_PROBE_BYTES = PROBE_BYTES
_THREADS_PER_TASK = THREADS_PER_TASK


def scan_partition(rows):
    """Probe first PROBE_BYTES bytes of each pdf in R2, classify by signature.

    Classifier mirrors evidence/make_validation_set.py exactly so labels are
    directly comparable to the 10K validation set.
    """
    import gzip
    from concurrent.futures import ThreadPoolExecutor

    import boto3
    from botocore.client import Config as BotoConfig
    from botocore.exceptions import ClientError

    HTML_TAG_HINTS = (
        b"<!doctype", b"<html", b"<head", b"<body", b"<title",
        b"<br>", b"<br/>", b"<br />", b"<p>", b"<div", b"<meta", b"<script",
    )

    def classify(buf: bytes) -> str:
        if not buf:
            return "EMPTY"
        if buf[:5] == b"%PDF-":
            return "VALID_PDF"
        if buf[:2] == b"\x1f\x8b":
            try:
                inner = gzip.decompress(buf)[: _PROBE_BYTES]
            except (OSError, EOFError):
                inner = b""
            if not inner:
                return "GZIPPED_TRUNCATED"
            if inner[:5] == b"%PDF-":
                return "GZIPPED_PDF"
            head_window = inner[:512].lower()
            if any(tag in head_window for tag in HTML_TAG_HINTS):
                return "GZIPPED_HTML"
            return "GZIPPED_OTHER"
        head_window = buf[:512].lower()
        if any(tag in head_window for tag in HTML_TAG_HINTS):
            return "HTML"
        stripped = head_window.lstrip()
        if stripped.startswith(b"{") and b'"error"' in buf[:500].lower():
            return "JSON_ERROR"
        if all(32 <= b < 127 or b in (9, 10, 13) for b in buf[:128]) and b"404" in buf[:128]:
            return "TEXT_ERROR"
        return "OTHER"

    s3 = boto3.client(
        "s3",
        endpoint_url=_R2_ENDPOINT,
        aws_access_key_id=_R2_ACCESS_KEY,
        aws_secret_access_key=_R2_SECRET_KEY,
        config=BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=_THREADS_PER_TASK,
            connect_timeout=10,
            read_timeout=30,
        ),
    )

    def probe(row):
        work_id = row.work_id
        pdf_s3_id = row.pdf_s3_id
        key = pdf_s3_id  # already includes ".pdf"
        try:
            obj = s3.get_object(Bucket=_R2_BUCKET, Key=key, Range=f"bytes=0-{_PROBE_BYTES - 1}")
            buf = obj["Body"].read()
            return (
                int(work_id),
                pdf_s3_id,
                classify(buf),
                buf[:16].hex(),
                len(buf),
                "OK",
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return (int(work_id), pdf_s3_id, "R2_MISSING", "", 0, "MISSING")
            return (int(work_id), pdf_s3_id, "R2_ERROR", "", 0, f"ERR_{code}"[:32])
        except Exception as e:
            return (int(work_id), pdf_s3_id, "R2_ERROR", "", 0, f"EXC_{type(e).__name__}"[:32])

    buf_rows = list(rows)
    if not buf_rows:
        return iter([])
    with ThreadPoolExecutor(max_workers=_THREADS_PER_TASK) as ex:
        return iter(list(ex.map(probe, buf_rows)))


result_schema = StructType([
    StructField("work_id", LongType(), False),
    StructField("pdf_s3_id", StringType(), False),
    StructField("label", StringType(), False),
    StructField("head_hex", StringType(), True),
    StructField("bytes_read", IntegerType(), True),
    StructField("status", StringType(), True),
])

print("Starting mapPartitions scan…")
t0 = time.time()
result_rdd = df.rdd.mapPartitions(scan_partition)
result_df = (
    spark.createDataFrame(result_rdd, schema=result_schema)
    .withColumn("scanned_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta table

# COMMAND ----------

(
    result_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)
elapsed = time.time() - t0
print(f"Wrote {TARGET_TABLE} in {elapsed:.0f}s ({total / elapsed:.0f} rows/s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by label

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
      label,
      COUNT(*) AS n,
      ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {TARGET_TABLE}
    GROUP BY label
    ORDER BY n DESC
""")
summary.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bad-PDF cohort (everything not VALID_PDF or GZIPPED_PDF)

# COMMAND ----------

bad_count = spark.sql(f"""
    SELECT COUNT(*) AS bad_pdfs
    FROM {TARGET_TABLE}
    WHERE label NOT IN ('VALID_PDF', 'GZIPPED_PDF')
""").collect()[0]["bad_pdfs"]

total_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE}").collect()[0]["n"]
print(f"Bad PDFs: {bad_count:,} / {total_count:,} ({100 * bad_count / total_count:.2f}%)")
