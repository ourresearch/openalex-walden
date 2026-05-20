# Databricks notebook source
# MAGIC %md
# MAGIC # Delete invalid R2 objects
# MAGIC
# MAGIC Purges bad-cohort objects from R2 — both the PDF bytes in `openalex-pdfs`
# MAGIC and (optionally) the cascaded GROBID XMLs in `openalex-grobid-xml`. Pairs
# MAGIC with the in-place pdf_works strip (notebooks/scraping/strip_invalid_pdfs_from_pdf_works.py)
# MAGIC and the D1 deletion (scripts/run_d1_delete_invalid_pdfs.py). Oxjob #185.
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - **PDF keys**: `openalex.pdf.invalid_pdfs.source_pdf_id` → key `<uuid>.pdf` in `openalex-pdfs`.
# MAGIC - **XML keys**: pdf_works @ time-travel version (pre-strip), extract docs.parsed-pdf
# MAGIC   entries from rows whose docs.pdf was in cohort → key `<uuid>.xml.gz` in
# MAGIC   `openalex-grobid-xml`. We need time travel because the strip MERGE removed
# MAGIC   those entries; current pdf_works no longer has the mapping. The Delta default
# MAGIC   file retention is 7 days — verify your version is still available.
# MAGIC
# MAGIC ## Approach
# MAGIC
# MAGIC Spark `mapPartitions` over the key list, batched `delete_objects` (S3/R2
# MAGIC supports up to 1000 keys per request). 64-worker cluster mirrors the
# MAGIC scan_pdf_bytes tuning that proved comfortable on R2.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC DELETE on a non-existent key is a no-op (R2 returns 204). Safe to re-run.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `target`: `pdfs` / `xmls` / `both` (default `pdfs` — XMLs require pdf_works time travel)
# MAGIC   - `dry_run`: `true` / `false` (default `true` — counts only, no deletes)
# MAGIC   - `sample_limit`: 0 = full cohort, >0 = pilot mode
# MAGIC   - `pdf_works_version`: Delta version of pdf_works to read for XML key lookup (pre-strip; typically v4224)
# MAGIC   - `num_partitions`: Spark partitions for the delete (default 256)
# MAGIC   - `threads_per_task`: concurrent delete_objects calls per Spark task (default 16)

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_PDF_BUCKET = "openalex-pdfs"
R2_XML_BUCKET = "openalex-grobid-xml"

# R2's DeleteObjects supports up to 1000 keys per request. Use the max.
DELETE_BATCH_KEYS = 1000

R2_ACCESS_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-access-key-id")
R2_SECRET_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-secret-access-key")

dbutils.widgets.dropdown("target", "pdfs", ["pdfs", "xmls", "both"], "Target (pdfs / xmls / both)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no deletes)")
dbutils.widgets.text("sample_limit", "0", "Sample limit (0 = full)")
dbutils.widgets.text("pdf_works_version", "4224", "pdf_works Delta version for XML key lookup (pre-strip)")
dbutils.widgets.text("num_partitions", "256", "Spark partitions")
dbutils.widgets.text("threads_per_task", "16", "Threads per Spark task")

TARGET = dbutils.widgets.get("target")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
SAMPLE_LIMIT = int(dbutils.widgets.get("sample_limit"))
PDF_WORKS_VERSION = int(dbutils.widgets.get("pdf_works_version"))
NUM_PARTITIONS = int(dbutils.widgets.get("num_partitions"))
THREADS_PER_TASK = int(dbutils.widgets.get("threads_per_task"))

print(f"target={TARGET}  dry_run={DRY_RUN}  sample_limit={SAMPLE_LIMIT:,}  "
      f"pdf_works_version={PDF_WORKS_VERSION}  partitions={NUM_PARTITIONS}  threads={THREADS_PER_TASK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the key list

# COMMAND ----------

def build_pdf_keys():
    """One row per (bucket, key) where key = '<source_pdf_id>.pdf'."""
    limit = f"LIMIT {SAMPLE_LIMIT}" if SAMPLE_LIMIT > 0 else ""
    return spark.sql(f"""
      SELECT
        '{R2_PDF_BUCKET}' AS bucket,
        CONCAT(source_pdf_id, '.pdf') AS key
      FROM openalex.pdf.invalid_pdfs
      {limit}
    """)


def build_xml_keys():
    """One row per (bucket, key) where key = '<grobid_uuid>.xml.gz'.

    Source: pdf_works @ time-travel version (pre-strip). For each pdf_works
    row whose docs.pdf id is in invalid_pdfs, take the docs.parsed-pdf id
    on the same row (the cascaded XML).
    """
    limit = f"LIMIT {SAMPLE_LIMIT}" if SAMPLE_LIMIT > 0 else ""
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW _pdf_works_pre AS
      SELECT native_id, ids
      FROM openalex.pdf.pdf_works VERSION AS OF {PDF_WORKS_VERSION}
    """)
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW _pre_exploded AS
      SELECT native_id, EXPLODE(ids) AS i
      FROM _pdf_works_pre
    """)
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW _victim_native_ids AS
      SELECT DISTINCT e.native_id
      FROM _pre_exploded e
      JOIN openalex.pdf.invalid_pdfs c
        ON e.i.namespace = 'docs.pdf'
       AND e.i.id        = CONCAT(c.source_pdf_id, '.pdf')
    """)
    return spark.sql(f"""
      SELECT
        '{R2_XML_BUCKET}' AS bucket,
        e.i.id AS key  -- already '<uuid>.xml.gz'
      FROM _pre_exploded e
      JOIN _victim_native_ids v ON e.native_id = v.native_id
      WHERE e.i.namespace = 'docs.parsed-pdf'
        -- docs.parsed-pdf id = CONCAT(grobid_processing_results.id, '.xml.gz')
        -- which is NULL when GROBID never ran for that PDF (PDF exists, XML
        -- doesn't). Skip — there's no R2 object to delete.
        AND e.i.id IS NOT NULL
      {limit}
    """)


if TARGET == "pdfs":
    df = build_pdf_keys()
elif TARGET == "xmls":
    df = build_xml_keys()
else:  # both
    df = build_pdf_keys().unionByName(build_xml_keys())

df = df.repartition(NUM_PARTITIONS)
n = df.count()
print(f"Keys to delete: {n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-bucket count preview

# COMMAND ----------

df.groupBy("bucket").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE (Spark mapPartitions + boto3 batched delete_objects)

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — skipping deletes. Set dry_run=false to apply.")
else:
    _R2_ENDPOINT = R2_ENDPOINT
    _R2_ACCESS_KEY = R2_ACCESS_KEY
    _R2_SECRET_KEY = R2_SECRET_KEY
    _DELETE_BATCH_KEYS = DELETE_BATCH_KEYS
    _THREADS_PER_TASK = THREADS_PER_TASK

    def delete_partition(rows):
        from collections import defaultdict
        from concurrent.futures import ThreadPoolExecutor

        import boto3
        from botocore.client import Config as BotoConfig
        from botocore.exceptions import ClientError

        s3 = boto3.client(
            "s3",
            endpoint_url=_R2_ENDPOINT,
            aws_access_key_id=_R2_ACCESS_KEY,
            aws_secret_access_key=_R2_SECRET_KEY,
            config=BotoConfig(
                retries={"max_attempts": 5, "mode": "standard"},
                max_pool_connections=_THREADS_PER_TASK * 4,
                connect_timeout=10,
                read_timeout=30,
            ),
        )

        # Group keys by bucket (each delete_objects call is single-bucket).
        per_bucket = defaultdict(list)
        for r in rows:
            per_bucket[r.bucket].append(r.key)

        def delete_chunk(bucket, keys):
            # Returns (bucket, deleted, errored, exception, error_sample, sample_key)
            # - errored: per-key errors returned by delete_objects (response body)
            # - exception: whole-batch failures (ClientError OR any other Exception)
            # - error_sample: first error message seen, for surfacing to caller
            # - sample_key: one of the keys in the failed batch, for repro
            try:
                resp = s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
                )
                errors = resp.get("Errors", []) or []
                err_sample = None
                if errors:
                    first = errors[0]
                    err_sample = f"{first.get('Code', '?')}: {first.get('Message', '?')[:200]}"
                sample_key = (errors[0].get("Key") if errors else None) or (keys[0] if keys else None)
                return bucket, len(keys) - len(errors), len(errors), 0, err_sample, sample_key
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "?")
                msg = e.response.get("Error", {}).get("Message", "?")[:200]
                return bucket, 0, 0, len(keys), f"ClientError {code}: {msg}", keys[0] if keys else None
            except Exception as e:
                return bucket, 0, 0, len(keys), f"{type(e).__name__}: {str(e)[:200]}", keys[0] if keys else None

        tasks = []
        for bucket, keys in per_bucket.items():
            for i in range(0, len(keys), _DELETE_BATCH_KEYS):
                tasks.append((bucket, keys[i : i + _DELETE_BATCH_KEYS]))

        if not tasks:
            return iter([])

        with ThreadPoolExecutor(max_workers=_THREADS_PER_TASK) as ex:
            results = list(ex.map(lambda t: delete_chunk(*t), tasks))

        # Roll up per bucket: counts + first error_sample + first sample_key
        agg: dict = {}
        for bucket, d, e, x, err_sample, sample_key in results:
            row = agg.setdefault(bucket, {"d": 0, "e": 0, "x": 0, "err": None, "key": None})
            row["d"] += d
            row["e"] += e
            row["x"] += x
            if row["err"] is None and err_sample:
                row["err"] = err_sample
                row["key"] = sample_key
        return iter([
            (bucket, v["d"], v["e"], v["x"], v["err"], v["key"])
            for bucket, v in agg.items()
        ])

    result_schema = StructType([
        StructField("bucket", StringType(), False),
        StructField("deleted", IntegerType(), False),
        StructField("errored", IntegerType(), False),
        StructField("exception", IntegerType(), False),
        StructField("error_sample", StringType(), True),
        StructField("sample_key", StringType(), True),
    ])

    print("Starting mapPartitions delete…")
    t0 = time.time()
    result_rdd = df.rdd.mapPartitions(delete_partition)
    result_df = (
        spark.createDataFrame(result_rdd, schema=result_schema)
        .groupBy("bucket")
        .agg(
            F.sum("deleted").alias("deleted"),
            F.sum("errored").alias("errored"),
            F.sum("exception").alias("exception"),
            # First non-null error across partitions for that bucket
            F.first("error_sample", ignorenulls=True).alias("error_sample"),
            F.first("sample_key", ignorenulls=True).alias("sample_key"),
        )
    )
    result_df.show(truncate=False)
    elapsed = time.time() - t0
    print(f"DELETE complete in {elapsed:.0f}s")
