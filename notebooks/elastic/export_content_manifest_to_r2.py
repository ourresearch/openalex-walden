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
# MAGIC **Switch** (oxjob #1958): from the 2026-11-16 run `content_index/` holds one row per work location that has
# MAGIC a file (work ids repeat), the same six columns plus `version` and `is_primary` (rule in `utils/content_files.py`).
# MAGIC `_manifest/content_index_preview/` has held that layout since 2026-11-02 and is written until the 2026-11-22 run.
# MAGIC Files are named `part_000.parquet` … and uploaded before old ones are deleted, so the manifest is never empty.
# MAGIC
# MAGIC Output is a directory of Parquet files, queryable as one dataset:
# MAGIC ```python
# MAGIC import duckdb
# MAGIC duckdb.sql("SELECT * FROM 'content_index/*.parquet' WHERE openalex_id = 'W2741809807'")
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

# R2 configuration (S3-compatible API)
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "openalex-pdfs"
R2_PREFIX = "_manifest/content_index"
R2_PREVIEW_PREFIX = "_manifest/content_index_preview"
SWITCH_DATE = date(2026, 11, 16)  # content_index/ takes the per-location layout (oxjob #1958)
PREVIEW_END = date(2026, 11, 23)  # content_index_preview/ is no longer written from this run

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

# Write ~10 Parquet files (~200MB each). Default partitioning produces 2800+
# files which is slow to write and upload. coalesce(1) is too slow (single
# partition shuffle). repartition(10) is the sweet spot.
# Fixed names, all uploaded before any old key is deleted: readers never find the prefix empty, and clients that
# copy it without --delete don't pile up old names (oxjob #1958). s3.upload_file() does multipart internally;
# the per-file work is bandwidth-bound, so 10 concurrent files saturate R2 ingest without contention.
def export_manifest(manifest_df, r2_prefix):
    dbfs_path = f"/tmp/{r2_prefix.split('/')[-1]}"
    dbutils.fs.rm(dbfs_path, recurse=True)
    manifest_df.repartition(10).write.parquet(f"dbfs:{dbfs_path}")

    parquet_files = sorted((f for f in dbutils.fs.ls(dbfs_path) if f.path.endswith(".parquet")), key=lambda f: f.path)
    total_size_mb = sum(f.size for f in parquet_files) / (1024 * 1024)
    print(f"Wrote {len(parquet_files)} Parquet files, {total_size_mb:.1f} MB total")
    new_keys = {f"{r2_prefix}/part_{i:03d}.parquet": f for i, f in enumerate(parquet_files)}

    def _upload_file(item):
        key, f = item
        s3.upload_file(f.path.replace("dbfs:", "/dbfs"), R2_BUCKET, key)
        return key

    print(f"Uploading {len(new_keys)} files to r2://{R2_BUCKET}/{r2_prefix}/ ...")
    done = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        for fut in as_completed([ex.submit(_upload_file, item) for item in new_keys.items()]):
            fut.result()  # propagate any exception
            done += 1
            if done % 5 == 0 or done == len(new_keys):
                print(f"  Uploaded {done}/{len(new_keys)}")

    paginator = s3.get_paginator("list_objects_v2")
    old_keys = [obj["Key"]
                for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{r2_prefix}/")
                for obj in page.get("Contents", []) if obj["Key"] not in new_keys]
    # Delete in batches of 1000 (S3 API limit)
    for i in range(0, len(old_keys), 1000):
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": [{"Key": k} for k in old_keys[i:i+1000]]}
        )
    print(f"Deleted {len(old_keys)} old files")

    dbutils.fs.rm(dbfs_path, recurse=True)
    print(f"\nDone. {len(new_keys)} files ({total_size_mb:.1f} MB) → r2://{R2_BUCKET}/{r2_prefix}/")


# COMMAND ----------

run_date = datetime.utcnow().date()
export_manifest(preview_df if run_date >= SWITCH_DATE else df, R2_PREFIX)

# COMMAND ----------

if run_date < PREVIEW_END:
    export_manifest(preview_df, R2_PREVIEW_PREFIX)
