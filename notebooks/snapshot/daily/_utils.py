# Databricks notebook source
# Shared utilities for daily snapshot export.
# %run'd by all daily export notebooks.

import json
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType

S3_BUCKET = "openalex-snapshots"
S3_BASE = f"s3://{S3_BUCKET}/daily"

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def get_snapshot_date():
    """Return yesterday's date as YYYY-MM-DD (the day whose updates we export)."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def get_daily_df(spark, table: str, date_str: str) -> DataFrame:
    """Read *table* and filter to rows where updated_date falls on *date_str*."""
    return (
        spark.read.table(table)
        .where(F.to_date("updated_date") == F.lit(date_str))
    )

# ---------------------------------------------------------------------------
# File-naming / cleanup
# ---------------------------------------------------------------------------

def rename_files_and_cleanup(dbutils, output_path: str, extension: str = "gz"):
    """Rename Spark-generated part files to sequential part_NNNN.{ext} and
    remove Spark metadata files (_SUCCESS, _committed, etc.)."""
    try:
        all_files = dbutils.fs.ls(output_path)
    except Exception:
        return  # nothing written

    data_files = sorted(
        [f for f in all_files if f.name.endswith(f".{extension}")],
        key=lambda x: x.name,
    )
    metadata_files = [f for f in all_files if f.name.startswith("_")]

    for idx, file_info in enumerate(data_files):
        new_name = f"part_{str(idx).zfill(4)}.{extension}"
        new_path = f"{output_path}/{new_name}"
        if file_info.name != new_name:
            dbutils.fs.mv(file_info.path, new_path)

    for f in metadata_files:
        try:
            dbutils.fs.rm(f.path, recurse=True)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# JSONL export
# ---------------------------------------------------------------------------

def export_jsonl(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                 records_per_file: int = 400_000):
    """Write *df* as gzip-compressed JSON lines to the daily JSONL path."""
    output_path = f"{S3_BASE}/{date_str}/jsonl/{entity}"
    record_count = df.count()
    num_partitions = max(1, math.ceil(record_count / records_per_file))

    print(f"  JSONL: {record_count:,} records -> {num_partitions} file(s)")

    (df.coalesce(num_partitions)
       .write
       .mode("overwrite")
       .option("compression", "gzip")
       .json(output_path))

    rename_files_and_cleanup(dbutils, output_path, "gz")
    return output_path, record_count

# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------

def export_parquet(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                   records_per_file: int = 500_000):
    """Write *df* as snappy-compressed Parquet to the daily Parquet path."""
    output_path = f"{S3_BASE}/{date_str}/parquet/{entity}"
    record_count = df.count()
    num_partitions = max(1, math.ceil(record_count / records_per_file))

    print(f"  Parquet: {record_count:,} records -> {num_partitions} file(s)")

    (df.coalesce(num_partitions)
       .write
       .mode("overwrite")
       .option("compression", "snappy")
       .parquet(output_path))

    rename_files_and_cleanup(dbutils, output_path, "snappy.parquet")
    return output_path, record_count

# ---------------------------------------------------------------------------
# Avro export
# ---------------------------------------------------------------------------

def _sanitize_avro_schema(df: DataFrame) -> DataFrame:
    """Rename fields that start with a digit (invalid in Avro) by prefixing with underscore.
    Handles both top-level columns and nested struct fields recursively."""
    from pyspark.sql.types import StructType, StructField, ArrayType as SparkArrayType

    def fix_name(name):
        return f"_{name}" if name[0].isdigit() else name

    def fix_struct(schema):
        new_fields = []
        for field in schema.fields:
            new_type = fix_type(field.dataType)
            new_fields.append(StructField(fix_name(field.name), new_type, field.nullable, field.metadata))
        return StructType(new_fields)

    def fix_type(dtype):
        if isinstance(dtype, StructType):
            return fix_struct(dtype)
        if isinstance(dtype, SparkArrayType):
            return SparkArrayType(fix_type(dtype.elementType), dtype.containsNull)
        return dtype

    new_schema = fix_struct(df.schema)
    if new_schema == df.schema:
        return df
    return df.sparkSession.createDataFrame(df.rdd, new_schema)


def export_avro(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                records_per_file: int = 500_000):
    """Write *df* as Avro to the daily Avro path."""
    output_path = f"{S3_BASE}/{date_str}/avro/{entity}"
    df = _sanitize_avro_schema(df)
    record_count = df.count()
    num_partitions = max(1, math.ceil(record_count / records_per_file))

    print(f"  Avro: {record_count:,} records -> {num_partitions} file(s)")

    (df.coalesce(num_partitions)
       .write
       .mode("overwrite")
       .format("avro")
       .save(output_path))

    rename_files_and_cleanup(dbutils, output_path, "avro")
    return output_path, record_count

# ---------------------------------------------------------------------------
# Multi-format export
# ---------------------------------------------------------------------------

def export_all_formats(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                       jsonl_records_per_file: int = 400_000,
                       columnar_records_per_file: int = 500_000):
    """Cache *df*, export to all 3 formats, create manifests, unpersist."""
    df.cache()
    record_count = df.count()

    if record_count == 0:
        print(f"  {entity}: 0 records — writing empty manifests")
        df.unpersist()
        for fmt in ("jsonl", "parquet", "avro"):
            create_empty_manifest(dbutils, date_str, fmt, entity)
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

    jsonl_path, _ = export_jsonl(spark, dbutils, df_jsonl, date_str, entity, jsonl_records_per_file)
    create_manifest(dbutils, jsonl_path, entity, "jsonl", date_str)

    parquet_path, _ = export_parquet(spark, dbutils, df, date_str, entity, columnar_records_per_file)
    create_manifest(dbutils, parquet_path, entity, "parquet", date_str)

    avro_path, _ = export_avro(spark, dbutils, df, date_str, entity, columnar_records_per_file)
    create_manifest(dbutils, avro_path, entity, "avro", date_str)

    df.unpersist()
    print(f"  {entity} export complete")

# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _s3_url(dbfs_path: str, fmt: str, entity: str, date_str: str) -> str:
    """Convert a dbfs:/ path to the public S3 URL."""
    # Build canonical URL: s3://openalex-snapshots/daily/{date}/{fmt}/{entity}/filename
    filename = dbfs_path.rstrip("/").split("/")[-1]
    return f"s3://{S3_BUCKET}/daily/{date_str}/{fmt}/{entity}/{filename}"


def create_manifest(dbutils, output_path: str, entity: str, fmt: str, date_str: str):
    """Create a manifest file listing every data file with size and record count."""
    try:
        all_files = dbutils.fs.ls(output_path)
    except Exception:
        create_empty_manifest(dbutils, date_str, fmt, entity)
        return

    # Determine extension by format
    ext_map = {"jsonl": ".gz", "parquet": ".parquet", "avro": ".avro"}
    ext = ext_map.get(fmt, "")

    data_files = sorted(
        [f for f in all_files if f.name.endswith(ext) and not f.name.startswith("_")],
        key=lambda x: x.name,
    )

    entries = []
    total_size = 0
    total_records = 0

    for file_info in data_files:
        s3_url = _s3_url(file_info.path, fmt, entity, date_str)

        # For JSONL we can count lines; for columnar formats, use Spark
        if fmt == "jsonl":
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                rc = spark.read.text(file_info.path).count()
            except Exception:
                rc = 0
        elif fmt == "parquet":
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                rc = spark.read.parquet(file_info.path).count()
            except Exception:
                rc = 0
        elif fmt == "avro":
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                rc = spark.read.format("avro").load(file_info.path).count()
            except Exception:
                rc = 0
        else:
            rc = 0

        entries.append({
            "url": s3_url,
            "meta": {
                "content_length": file_info.size,
                "record_count": rc,
            }
        })
        total_size += file_info.size
        total_records += rc

    manifest = {
        "entries": entries,
        "meta": {
            "content_length": total_size,
            "record_count": total_records,
        }
    }

    manifest_path = f"{output_path}/manifest"
    dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2), overwrite=True)
    print(f"  Manifest ({fmt}): {len(entries)} files, {total_records:,} records, {total_size / (1024**2):.2f} MB")


def create_empty_manifest(dbutils, date_str: str, fmt: str, entity: str):
    """Write an empty manifest for an entity with zero updates on this day."""
    manifest = {
        "entries": [],
        "meta": {
            "content_length": 0,
            "record_count": 0,
        }
    }
    output_path = f"{S3_BASE}/{date_str}/{fmt}/{entity}"
    manifest_path = f"{output_path}/manifest"
    dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2), overwrite=True)
    print(f"  Empty manifest ({fmt}): {entity}")
