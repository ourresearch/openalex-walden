# Databricks notebook source
# MAGIC %md
# MAGIC # Export Content Manifest to R2
# MAGIC
# MAGIC Exports a Parquet manifest mapping OpenAlex work IDs to PDF/Grobid UUIDs in the
# MAGIC `openalex-pdfs` R2 bucket. Partners doing bulk sync use this to locate files.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes
# MAGIC **Source**: `openalex.works.locations_mapped`
# MAGIC **Target**: `s3://openalex-pdfs/_manifest/content_index.parquet` (R2 via S3 API)
# MAGIC
# MAGIC The export is a full replacement each run (not incremental).

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import boto3
import os
import tempfile
from datetime import datetime

# R2 configuration (S3-compatible API)
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
R2_ENDPOINT = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "openalex-pdfs"
R2_KEY = "_manifest/content_index.parquet"

R2_ACCESS_KEY = dbutils.secrets.get(scope="cloudflare", key="r2_access_key_id")
R2_SECRET_KEY = dbutils.secrets.get(scope="cloudflare", key="r2_secret_access_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Content Index

# COMMAND ----------

df = spark.sql("""
    WITH ranked AS (
      SELECT
        CONCAT('W', work_id) AS openalex_id,
        REPLACE(pdf_s3_id, '.pdf', '') AS pdf_uuid,
        REPLACE(grobid_s3_id, '.xml.gz', '') AS grobid_xml_id,
        ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY pdf_s3_id) AS rn
      FROM openalex.works.locations_mapped
      WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
        AND work_id IS NOT NULL
    )
    SELECT openalex_id, pdf_uuid, grobid_xml_id
    FROM ranked
    WHERE rn = 1
    ORDER BY openalex_id
""")

total = df.count()
print(f"Content index: {total:,} works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parquet and Upload to R2

# COMMAND ----------

# Write single Parquet file to temp location on DBFS
dbfs_path = "/tmp/content_manifest"
dbutils.fs.rm(dbfs_path, recurse=True)

df.coalesce(1).write.parquet(f"dbfs:{dbfs_path}")

# Find the actual .parquet file (Spark names it part-00000-*.parquet)
parquet_files = [f.path for f in dbutils.fs.ls(dbfs_path) if f.path.endswith(".parquet")]
if not parquet_files:
    raise RuntimeError("No parquet file written")

spark_parquet = parquet_files[0]
local_path = spark_parquet.replace("dbfs:", "/dbfs")
file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
print(f"Parquet file: {file_size_mb:.1f} MB")

# COMMAND ----------

# Upload to R2
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

print(f"Uploading to r2://{R2_BUCKET}/{R2_KEY} ...")
s3.upload_file(local_path, R2_BUCKET, R2_KEY)
print("Upload complete")

# Verify
head = s3.head_object(Bucket=R2_BUCKET, Key=R2_KEY)
r2_size_mb = head["ContentLength"] / (1024 * 1024)
print(f"R2 object size: {r2_size_mb:.1f} MB")
print(f"Last modified: {head['LastModified']}")

# COMMAND ----------

# Clean up temp files
dbutils.fs.rm(dbfs_path, recurse=True)

print(f"\nDone. {total:,} works → {file_size_mb:.1f} MB Parquet → r2://{R2_BUCKET}/{R2_KEY}")
