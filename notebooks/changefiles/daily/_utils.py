# Databricks notebook source
# Shared utilities for daily changefiles export.
# %run'd by all daily export notebooks.

import json
import math
import os
import time
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType

S3_BUCKET = "openalex-snapshots"
S3_BASE = f"s3://{S3_BUCKET}/daily"
WATERMARK_TABLE = "openalex.snapshot.daily_watermark"

# ---------------------------------------------------------------------------
# Preflight: verify boto3 can access the S3 bucket (fail fast, not after
# 20 min of data processing)
# ---------------------------------------------------------------------------
try:
    import boto3 as _boto3
    _s3 = _boto3.client(
        "s3",
        aws_access_key_id=dbutils.secrets.get("webscraper", "aws_access_key_id"),
        aws_secret_access_key=dbutils.secrets.get("webscraper", "aws_secret_access_key"),
    )
    _s3.head_bucket(Bucket=S3_BUCKET)
    print(f"S3 preflight OK: s3://{S3_BUCKET} accessible")
    del _s3, _boto3
except Exception as _e:
    raise RuntimeError(f"S3 preflight FAILED — cannot access s3://{S3_BUCKET}: {_e}")

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def get_snapshot_date():
    """Return today's date as YYYY-MM-DD (the day whose updates we export)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


_watermark_cutoff = None

def get_watermark_cutoff(spark):
    """Read the cutoff timestamp from the watermark table.

    If the snapshot already ran today, re-uses the same cutoff so that re-runs
    produce the same complete output (not just the delta since the last run).
    Otherwise, returns the completed_ts from the most recent run.
    Falls back to 24 hours ago if the table is empty or doesn't exist.
    Result is cached for the lifetime of this notebook execution.
    """
    global _watermark_cutoff
    if _watermark_cutoff is not None:
        return _watermark_cutoff

    today = get_snapshot_date()

    try:
        # If we already ran today, re-use the same cutoff (idempotent re-runs)
        today_row = (spark.read.table(WATERMARK_TABLE)
                     .where(F.col("snapshot_date") == F.lit(today))
                     .orderBy(F.asc("completed_ts"))
                     .limit(1)
                     .first())
        if today_row and today_row.cutoff_ts:
            _watermark_cutoff = today_row.cutoff_ts.strftime("%Y-%m-%d %H:%M:%S")
            print(f"Watermark cutoff: {_watermark_cutoff} (re-run — reusing today's cutoff)")
            return _watermark_cutoff

        # Normal case: use the latest completed_ts as the new cutoff
        row = (spark.read.table(WATERMARK_TABLE)
               .orderBy(F.desc("completed_ts"))
               .limit(1)
               .first())
        if row and row.completed_ts:
            _watermark_cutoff = row.completed_ts.strftime("%Y-%m-%d %H:%M:%S")
            print(f"Watermark cutoff: {_watermark_cutoff} (from previous snapshot)")
            return _watermark_cutoff
    except Exception:
        pass

    _watermark_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Watermark cutoff: {_watermark_cutoff} (fallback — no previous watermark found)")
    return _watermark_cutoff


def get_daily_df(spark, table: str, date_str: str) -> DataFrame:
    """Read *table* and filter to rows updated since the last snapshot.

    Uses a high-water mark from the daily_watermark table to ensure no records
    are missed between consecutive snapshot runs. The *date_str* parameter is
    used for file naming only, not filtering.
    """
    cutoff = get_watermark_cutoff(spark)
    return (
        spark.read.table(table)
        .where(F.col("updated_date") >= F.lit(cutoff))
    )

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _get_s3_client(dbutils):
    """Return a boto3 S3 client using credentials from Databricks secrets."""
    import boto3
    return boto3.client(
        "s3",
        aws_access_key_id=dbutils.secrets.get("webscraper", "aws_access_key_id"),
        aws_secret_access_key=dbutils.secrets.get("webscraper", "aws_secret_access_key"),
    )


def _s3_multipart_merge(s3_client, bucket, source_prefix, dest_key, extension):
    """Merge all part files under *source_prefix* into a single S3 object.

    Uses S3 upload_part_copy for server-side merging (no data downloaded).
    Each part must be >= 5 MB (S3 multipart minimum) except the last part.
    If only one source file exists, uses simple copy_object instead.

    Returns the total content_length of the merged object.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=source_prefix)

    parts = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(f".{extension}"):
                parts.append({"key": obj["Key"], "size": obj["Size"]})

    if not parts:
        raise RuntimeError(f"No .{extension} files found under s3://{bucket}/{source_prefix}")

    parts.sort(key=lambda p: p["key"])

    # Validate 5 MB minimum (except last part)
    MIN_PART_SIZE = 5 * 1024 * 1024
    for part in parts[:-1]:
        if part["size"] < MIN_PART_SIZE:
            raise RuntimeError(
                f"Part {part['key']} is {part['size']:,} bytes — below 5 MB minimum for "
                f"S3 multipart copy. Increase records_per_file to produce larger part files."
            )

    # Single file — simple copy
    if len(parts) == 1:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": parts[0]["key"]},
            Key=dest_key,
        )
        total_size = parts[0]["size"]
        print(f"    Copied single part to {dest_key} ({total_size / (1024**2):.1f} MB)")
        return total_size

    # Multiple files — multipart merge
    mp = s3_client.create_multipart_upload(Bucket=bucket, Key=dest_key)
    upload_id = mp["UploadId"]

    try:
        def _copy_part(part_key, part_num):
            resp = s3_client.upload_part_copy(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": part_key},
                Key=dest_key,
                PartNumber=part_num,
                UploadId=upload_id,
            )
            return {"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": part_num}

        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [
                pool.submit(_copy_part, p["key"], i + 1)
                for i, p in enumerate(parts)
            ]
            completed = [f.result() for f in as_completed(futures)]

        completed.sort(key=lambda x: x["PartNumber"])
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": completed},
        )
        total_size = sum(p["size"] for p in parts)
        print(f"    Merged {len(parts)} parts into {dest_key} ({total_size / (1024**2):.1f} MB)")
        return total_size

    except Exception:
        s3_client.abort_multipart_upload(Bucket=bucket, Key=dest_key, UploadId=upload_id)
        raise


def _parquet_merge(s3_client, bucket, source_prefix, dest_key):
    """Merge multiple Parquet part files into a single Parquet file via PyArrow.

    Unlike JSONL/gzip, Parquet files have headers and footers that prevent
    simple byte-level concatenation. This function downloads each part file,
    reads it with PyArrow, and appends it as a row group in a single output
    file using ParquetWriter.

    Local temp files are written to /local_disk0 (Databricks instance storage)
    and cleaned up after each part to bound disk usage.

    Returns the content_length of the uploaded merged file.
    """
    import pyarrow.parquet as pq

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=source_prefix)

    parts = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".snappy.parquet"):
                parts.append(obj["Key"])

    if not parts:
        raise RuntimeError(f"No .snappy.parquet files found under s3://{bucket}/{source_prefix}")

    parts.sort()

    local_tmp_dir = tempfile.mkdtemp(dir="/local_disk0", prefix="parquet_merge_")
    merged_path = os.path.join(local_tmp_dir, "merged.parquet")
    writer = None

    try:
        for i, part_key in enumerate(parts):
            part_path = os.path.join(local_tmp_dir, f"part_{i}.parquet")
            s3_client.download_file(bucket, part_key, part_path)

            table = pq.read_table(part_path)
            if writer is None:
                writer = pq.ParquetWriter(merged_path, table.schema, compression="snappy")
            writer.write_table(table)

            # Free memory and disk for this part
            del table
            os.remove(part_path)

        writer.close()
        writer = None

        s3_client.upload_file(merged_path, bucket, dest_key)
        content_length = os.path.getsize(merged_path)
        print(f"    Merged {len(parts)} Parquet parts into {dest_key} ({content_length / (1024**2):.1f} MB)")
        return content_length

    finally:
        if writer is not None:
            writer.close()
        # Clean up local temp directory
        for f in os.listdir(local_tmp_dir):
            try:
                os.remove(os.path.join(local_tmp_dir, f))
            except OSError:
                pass
        try:
            os.rmdir(local_tmp_dir)
        except OSError:
            pass


def _rename_single_file(dbutils, output_path, extension, target_path):
    """Rename the single Spark part file in *output_path* to *target_path*."""
    files = dbutils.fs.ls(output_path)
    data_files = [f for f in files if f.name.endswith(f".{extension}")]
    if len(data_files) != 1:
        raise RuntimeError(f"Expected 1 .{extension} file in {output_path}, found {len(data_files)}")
    dbutils.fs.mv(data_files[0].path, target_path)
    return data_files[0].size


def _cleanup_spark_metadata(dbutils, output_path):
    """Remove Spark metadata files (_SUCCESS, _committed, etc.) from *output_path*."""
    try:
        files = dbutils.fs.ls(output_path)
    except Exception:
        return
    for f in files:
        if f.name.startswith("_"):
            try:
                dbutils.fs.rm(f.path, recurse=True)
            except Exception:
                pass


def _write_entity_meta(dbutils, date_str, fmt, entity, filename, record_count, content_length):
    """Write per-entity metadata JSON to _meta/ prefix for update_meta to aggregate."""
    meta = {
        "entity": entity,
        "filename": filename,
        "record_count": record_count,
        "content_length": content_length,
    }
    meta_path = f"{S3_BASE}/{date_str}/_meta/{fmt}/{entity}.json"
    dbutils.fs.put(meta_path, json.dumps(meta), overwrite=True)

# ---------------------------------------------------------------------------
# JSONL export
# ---------------------------------------------------------------------------

def export_jsonl(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                 records_per_file: int = 500_000, record_count: int = None):
    """Write *df* as gzip-compressed JSON lines to a single file per entity.

    Small entities (single partition): coalesce(1) to temp dir, rename.
    Large entities (multiple partitions): parallel write to temp dir, then
    S3 multipart merge into a single file.
    """
    target_name = f"{entity}_{date_str}.jsonl.gz"
    target_path = f"{S3_BASE}/{date_str}/jsonl/{target_name}"

    if record_count is None:
        record_count = df.count()
    num_partitions = max(1, math.ceil(record_count / records_per_file))

    print(f"  JSONL: {record_count:,} records -> {num_partitions} partition(s)")

    temp_path = f"{S3_BASE}/{date_str}/_temp/jsonl/{entity}"

    (df.coalesce(num_partitions)
       .write
       .mode("overwrite")
       .option("compression", "gzip")
       .option("ignoreNullFields", "false")
       .json(temp_path))

    _cleanup_spark_metadata(dbutils, temp_path)

    if num_partitions == 1:
        content_length = _rename_single_file(dbutils, temp_path, "gz", target_path)
    else:
        s3_client = _get_s3_client(dbutils)
        source_prefix = f"daily/{date_str}/_temp/jsonl/{entity}"
        dest_key = f"daily/{date_str}/jsonl/{target_name}"
        content_length = _s3_multipart_merge(s3_client, S3_BUCKET, source_prefix, dest_key, "gz")

    # Clean up temp directory
    try:
        dbutils.fs.rm(temp_path, recurse=True)
    except Exception:
        pass

    _write_entity_meta(dbutils, date_str, "jsonl", entity, target_name, record_count, content_length)
    return target_path, record_count

# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------

def export_parquet(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                   records_per_file: int = 500_000, record_count: int = None):
    """Write *df* as snappy-compressed Parquet to a single file per entity.

    Small entities (single partition): coalesce(1) to temp dir, rename.
    Large entities (multiple partitions): parallel write to temp dir, then
    PyArrow merge into a single file (Parquet has headers/footers that
    prevent byte-level concatenation).
    """
    target_name = f"{entity}_{date_str}.parquet"
    target_path = f"{S3_BASE}/{date_str}/parquet/{target_name}"

    if record_count is None:
        record_count = df.count()
    num_partitions = max(1, math.ceil(record_count / records_per_file))

    print(f"  Parquet: {record_count:,} records -> {num_partitions} partition(s)")

    temp_path = f"{S3_BASE}/{date_str}/_temp/parquet/{entity}"

    (df.coalesce(num_partitions)
       .write
       .mode("overwrite")
       .option("compression", "snappy")
       .parquet(temp_path))

    _cleanup_spark_metadata(dbutils, temp_path)

    if num_partitions == 1:
        content_length = _rename_single_file(dbutils, temp_path, "snappy.parquet", target_path)
    else:
        s3_client = _get_s3_client(dbutils)
        source_prefix = f"daily/{date_str}/_temp/parquet/{entity}"
        dest_key = f"daily/{date_str}/parquet/{target_name}"
        content_length = _parquet_merge(s3_client, S3_BUCKET, source_prefix, dest_key)

    # Clean up temp directory
    try:
        dbutils.fs.rm(temp_path, recurse=True)
    except Exception:
        pass

    _write_entity_meta(dbutils, date_str, "parquet", entity, target_name, record_count, content_length)
    return target_path, record_count

# ---------------------------------------------------------------------------
# Multi-format export
# ---------------------------------------------------------------------------

def export_all_formats(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                       jsonl_records_per_file: int = 500_000,
                       columnar_records_per_file: int = 500_000):
    """Cache *df*, export to JSONL and Parquet, write per-entity metadata, unpersist."""
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")
    df.cache()
    record_count = df.count()

    if record_count == 0:
        print(f"  {entity}: 0 records — writing empty metadata")
        df.unpersist()
        for fmt in ("jsonl", "parquet"):
            _write_entity_meta(dbutils, date_str, fmt, entity, None, 0, 0)
        return

    print(f"\n{'='*60}")
    print(f"Exporting {entity}: {record_count:,} records")
    print(f"{'='*60}")

    # For JSONL, parse abstract_inverted_index from string to map if present
    df_jsonl = df
    if "abstract_inverted_index" in df.columns:
        df_jsonl = df.withColumn(
            "abstract_inverted_index",
            F.from_json(
                F.col("abstract_inverted_index"),
                MapType(StringType(), ArrayType(IntegerType()))
            )
        )

    # Export both formats in parallel (data is cached, so concurrent writes are safe)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(export_jsonl, spark, dbutils, df_jsonl, date_str, entity, jsonl_records_per_file, record_count),
            pool.submit(export_parquet, spark, dbutils, df, date_str, entity, columnar_records_per_file, record_count),
        ]
        for future in as_completed(futures):
            future.result()  # raise if any export failed

    df.unpersist()
    print(f"  {entity} export complete")
