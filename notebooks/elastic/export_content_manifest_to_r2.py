# Databricks notebook source
# MAGIC %md
# MAGIC # Export Content Manifest to R2
# MAGIC
# MAGIC Exports a Parquet manifest mapping OpenAlex work IDs to PDF/Grobid UUIDs in the
# MAGIC `openalex-pdfs` R2 bucket. Partners doing bulk sync use this to locate files.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes
# MAGIC **Source**: `openalex.works.locations_mapped`
# MAGIC **Target**: `s3://openalex-pdfs/_manifest/content_index/` (R2 via S3 API)
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
from datetime import datetime

# R2 configuration (S3-compatible API)
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "openalex-pdfs"
R2_PREFIX = "_manifest/content_index"

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
df = spark.sql("""
    SELECT
      CONCAT('W', work_id) AS openalex_id,
      REPLACE(MIN(pdf_s3_id), '.pdf', '') AS pdf_uuid,
      REPLACE(MIN(grobid_s3_id), '.xml.gz', '') AS grobid_xml_id
    FROM openalex.works.locations_mapped
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL
    GROUP BY work_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parquet Files and Upload to R2

# COMMAND ----------

# Write Parquet files to DBFS (Spark default partitioning, no coalesce)
dbfs_path = "/tmp/content_manifest"
dbutils.fs.rm(dbfs_path, recurse=True)

df.write.parquet(f"dbfs:{dbfs_path}")

parquet_files = [f for f in dbutils.fs.ls(dbfs_path) if f.path.endswith(".parquet")]
total_size_mb = sum(f.size for f in parquet_files) / (1024 * 1024)
print(f"Wrote {len(parquet_files)} Parquet files, {total_size_mb:.1f} MB total")

# COMMAND ----------

# Delete old manifest files from R2
print(f"Clearing old manifest at r2://{R2_BUCKET}/{R2_PREFIX}/ ...")
paginator = s3.get_paginator("list_objects_v2")
old_keys = []
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{R2_PREFIX}/"):
    for obj in page.get("Contents", []):
        old_keys.append(obj["Key"])

if old_keys:
    # Delete in batches of 1000 (S3 API limit)
    for i in range(0, len(old_keys), 1000):
        batch = old_keys[i:i+1000]
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch]}
        )
    print(f"Deleted {len(old_keys)} old files")
else:
    print("No old files to delete")

# COMMAND ----------

# Upload new Parquet files to R2
print(f"Uploading {len(parquet_files)} files to r2://{R2_BUCKET}/{R2_PREFIX}/ ...")
for i, f in enumerate(parquet_files):
    filename = f.path.split("/")[-1]
    local_path = f.path.replace("dbfs:", "/dbfs")
    r2_key = f"{R2_PREFIX}/{filename}"
    s3.upload_file(local_path, R2_BUCKET, r2_key)
    if (i + 1) % 10 == 0 or (i + 1) == len(parquet_files):
        print(f"  Uploaded {i + 1}/{len(parquet_files)}")

print("Upload complete")

# COMMAND ----------

# Clean up temp files
dbutils.fs.rm(dbfs_path, recurse=True)

print(f"\nDone. {len(parquet_files)} files ({total_size_mb:.1f} MB) → r2://{R2_BUCKET}/{R2_PREFIX}/")
