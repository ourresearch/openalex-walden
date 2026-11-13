# Databricks notebook source
# MAGIC %md
# MAGIC # Export Content Manifest to R2
# MAGIC
# MAGIC Exports a Parquet manifest mapping OpenAlex work IDs to PDF/Grobid UUIDs in the
# MAGIC `openalex-pdfs` R2 bucket, plus the source native_id/native_id_namespace of the
# MAGIC location that supplied the file. Partners doing bulk sync use this to locate files.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes
# MAGIC **Source**: `openalex.works.locations_mapped`
# MAGIC **Target**: `s3://openalex-pdfs/_manifest/content_index/` (R2 via S3 API)
# MAGIC
# MAGIC **Switch** (oxjob #1958): from the 2026-11-23 run `content_index/` holds one row per work location that has
# MAGIC a file (work ids repeat), the same six columns plus `version` and `is_primary` (rule in `utils/content_files.py`).
# MAGIC `_manifest/content_index_preview/` has held that layout since 2026-11-02 and is written until the 2026-11-29 run.
# MAGIC Each folder holds one file, `manifest.parquet`, replaced in full by each run (one upload, so never empty or partial).
# MAGIC
# MAGIC Output is one Parquet file:
# MAGIC ```python
# MAGIC import duckdb
# MAGIC duckdb.sql("SELECT * FROM 'content_index/manifest.parquet' WHERE openalex_id = 'W2741809807'")
# MAGIC ```
# MAGIC
# MAGIC The export is a full replacement each run (not incremental).

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import boto3
import os
import sys
from datetime import date, datetime

import pyarrow.parquet as pq

# R2 configuration (S3-compatible API)
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "openalex-pdfs"
R2_PREFIX = "_manifest/content_index"
R2_PREVIEW_PREFIX = "_manifest/content_index_preview"
SWITCH_DATE = date(2026, 11, 23)  # content_index/ takes the per-location layout (oxjob #1958)
PREVIEW_END = date(2026, 11, 30)  # content_index_preview/ is no longer written from this run

R2_ACCESS_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-access-key-id")
R2_SECRET_KEY = dbutils.secrets.get(scope="cloudflare", key="r2-secret-access-key")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Content Index

# COMMAND ----------

# Use GROUP BY instead of ROW_NUMBER window function — much faster on single node.
# No ORDER BY needed — Parquet handles random access fine.
# Skip separate count() — get it from the write stats instead.
# native_id comes from the same location row as the reported pdf_uuid
# (grobid row as fallback) — ~7.5% of works have multiple native_ids.
df = spark.sql("""
    SELECT
      openalex_id,
      pdf_uuid,
      grobid_xml_id,
      src.native_id,
      src.native_id_namespace,
      updated_date
    FROM (
      SELECT
        CONCAT('W', work_id) AS openalex_id,
        REPLACE(MIN(pdf_s3_id), '.pdf', '') AS pdf_uuid,
        REPLACE(MIN(grobid_s3_id), '.xml.gz', '') AS grobid_xml_id,
        COALESCE(
          MIN_BY(STRUCT(native_id, native_id_namespace), pdf_s3_id),
          MIN_BY(STRUCT(native_id, native_id_namespace), grobid_s3_id)
        ) AS src,
        MAX(updated_date) AS updated_date
      FROM openalex.works.locations_mapped
      WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
        AND work_id IS NOT NULL
      GROUP BY work_id
    )
""")

# COMMAND ----------

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "content_files.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "content_files.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils.content_files import content_files_sql  # noqa: E402

# New columns go last so readers of the six current columns keep working.
preview_df = spark.sql(f"""
    SELECT
      CONCAT('W', work_id) AS openalex_id,
      pdf_uuid,
      grobid_xml_id,
      native_id,
      native_id_namespace,
      updated_date,
      version,
      is_primary
    FROM ({content_files_sql()})
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parquet Files and Upload to R2

# COMMAND ----------

# Spark writes ~10 parts (repartition(10); coalesce(1) is too slow), then the driver copies their row groups into one
# file (oxjob #1958): clients copy one name, and one upload swaps it whole, so readers never see a mix of two days.
def export_manifest(manifest_df, r2_prefix):
    name = r2_prefix.split("/")[-1]
    dbfs_path = f"/tmp/{name}"
    dbutils.fs.rm(dbfs_path, recurse=True)
    manifest_df.repartition(10).write.parquet(f"dbfs:{dbfs_path}")
    parts = sorted(f.path.replace("dbfs:", "/dbfs") for f in dbutils.fs.ls(dbfs_path) if f.path.endswith(".parquet"))

    local_file = f"/local_disk0/tmp/{name}_manifest.parquet"
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    rows = 0
    with pq.ParquetWriter(local_file, pq.read_schema(parts[0]), compression="snappy") as writer:
        for part in parts:
            pf = pq.ParquetFile(part)
            for i in range(pf.num_row_groups):
                group = pf.read_row_group(i)
                writer.write_table(group)
                rows += group.num_rows
    size_mb = os.path.getsize(local_file) / (1024 * 1024)
    print(f"Merged {len(parts)} parts into one file: {rows:,} rows, {size_mb:.1f} MB")

    key = f"{r2_prefix}/manifest.parquet"
    s3.upload_file(local_file, R2_BUCKET, key)
    paginator = s3.get_paginator("list_objects_v2")
    old_keys = [obj["Key"]
                for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{r2_prefix}/")
                for obj in page.get("Contents", []) if obj["Key"] != key]
    if old_keys:
        s3.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": [{"Key": k} for k in old_keys]})
    print(f"Deleted {len(old_keys)} old files")

    os.remove(local_file)
    dbutils.fs.rm(dbfs_path, recurse=True)
    print(f"\nDone. r2://{R2_BUCKET}/{key} ({rows:,} rows, {size_mb:.1f} MB)")


# COMMAND ----------

run_date = datetime.utcnow().date()
export_manifest(preview_df if run_date >= SWITCH_DATE else df, R2_PREFIX)

# COMMAND ----------

if run_date < PREVIEW_END:
    export_manifest(preview_df, R2_PREVIEW_PREFIX)
