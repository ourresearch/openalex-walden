# Databricks notebook source
# Shared utilities for the monthly full-snapshot export.
# %run'd by all notebooks/snapshot/export_*.ipynb.
#
# Output layout (mirrors notebooks/changefiles/daily/_utils.py shape):
#   s3://openalex-snapshots/full/{date}/jsonl/{entity}/updated_date=*/part_NNNN.gz
#   s3://openalex-snapshots/full/{date}/parquet/{entity}/updated_date=*/part_NNNN.snappy.parquet
#   s3://openalex-snapshots/full/{date}/_meta/{format}/{entity}.json   # consumed by update_meta
#
# Snapshot scale (works ~263M, authors ~110M) means we keep updated_date partitioning
# and produce many per-partition part files per entity per format — unlike the daily
# changefile which collapses to one merged file per entity.

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType

S3_BUCKET = "openalex-snapshots"
S3_BASE = f"s3://{S3_BUCKET}/full"

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def get_snapshot_date():
    """Return today's date as YYYY-MM-DD."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Salting (large-entity hash distribution, copied verbatim from the original
# export_works/export_authors flow so file sizes stay balanced across partitions
# even when one updated_date holds 100M+ records).
# ---------------------------------------------------------------------------

def _apply_salting(df_with_count: DataFrame) -> DataFrame:
    """Add a `salt` column whose modulus scales with the partition's record count."""
    return df_with_count.withColumn(
        "salt",
        F.when(F.col("date_count") > 100_000_000, F.abs(F.hash("id")) % 1400)
        .when(F.col("date_count") > 40_000_000, F.abs(F.hash("id")) % 160)
        .when(F.col("date_count") > 10_000_000, F.abs(F.hash("id")) % 50)
        .when(F.col("date_count") > 5_000_000, F.abs(F.hash("id")) % 25)
        .when(F.col("date_count") > 2_000_000, F.abs(F.hash("id")) % 10)
        .when(F.col("date_count") > 800_000, F.abs(F.hash("id")) % 3)
        .otherwise(0)
    ).drop("date_count")


# ---------------------------------------------------------------------------
# Filename rename + spark-metadata cleanup
# ---------------------------------------------------------------------------

def _rename_partitions(dbutils, output_path: str, file_extension: str, max_workers: int = 30):
    """Rename Spark's `_partition_date=` directories to `updated_date=` and rename
    each part file to `part_NNNN.{file_extension}`.

    file_extension is "gz" for jsonl.gz output or "snappy.parquet" for parquet.
    """
    partitions = dbutils.fs.ls(output_path)
    partitions_to_process = [p for p in partitions if p.name.startswith("_partition_date=")]

    print(f"  Renaming {len(partitions_to_process)} partition(s) (extension=.{file_extension})")

    def _process(partition):
        date_value = partition.name.replace("_partition_date=", "").rstrip("/")
        new_partition_path = f"{output_path}/updated_date={date_value}/"

        files = dbutils.fs.ls(partition.path)
        data_files = [f for f in files if f.name.endswith(f".{file_extension}")]
        meta_files = [f for f in files if not f.name.endswith(f".{file_extension}")]
        data_files.sort(key=lambda x: x.name)

        if not data_files:
            for f in meta_files:
                try:
                    dbutils.fs.rm(f.path)
                except Exception:
                    pass
            try:
                dbutils.fs.rm(partition.path, recurse=True)
            except Exception:
                pass
            return partition.name, 0, "empty partition cleaned"

        # Parallel inner rename for partitions with many files (works case)
        if len(data_files) > 100:
            counter_lock = threading.Lock()
            counter = {"moved": 0}

            def _move(file_info, idx):
                new_name = f"part_{str(idx).zfill(4)}.{file_extension}"
                try:
                    dbutils.fs.mv(file_info.path, f"{new_partition_path}{new_name}")
                    with counter_lock:
                        counter["moved"] += 1
                    return True
                except Exception:
                    return False

            with ThreadPoolExecutor(max_workers=50) as inner:
                inner_futs = [inner.submit(_move, f, i) for i, f in enumerate(data_files)]
                for fut in as_completed(inner_futs):
                    fut.result()
            moved = counter["moved"]
        else:
            moved = 0
            for idx, f in enumerate(data_files):
                new_name = f"part_{str(idx).zfill(4)}.{file_extension}"
                try:
                    dbutils.fs.mv(f.path, f"{new_partition_path}{new_name}")
                    moved += 1
                except Exception as e:
                    print(f"    rename failed for {f.path}: {e}")

        for f in meta_files:
            try:
                dbutils.fs.rm(f.path)
            except Exception:
                pass
        try:
            dbutils.fs.rm(partition.path, recurse=True)
        except Exception:
            pass

        return partition.name, moved, f"{moved} file(s) moved"

    start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_process, p) for p in partitions_to_process]
        completed = 0
        for fut in as_completed(futures):
            name, moved, msg = fut.result()
            completed += 1
            elapsed = time.time() - start
            print(f"  [{completed}/{len(partitions_to_process)}] {name}: {msg} ({elapsed:.1f}s)")

    # Clean up root-level Spark metadata files (_SUCCESS, etc.)
    try:
        for f in dbutils.fs.ls(output_path):
            if f.name.startswith("_"):
                dbutils.fs.rm(f.path, recurse=True)
    except Exception as e:
        print(f"  warning: root cleanup failed: {e}")


# ---------------------------------------------------------------------------
# Per-entity meta JSON (consumed by update_meta to build per-format manifests)
# ---------------------------------------------------------------------------

def _enumerate_files(dbutils, output_path: str, file_extension: str):
    """Walk `output_path` and return a list of (relative_path, size, partition_name, full_path)
    tuples for files matching `.{file_extension}`."""
    out = []
    try:
        partitions = dbutils.fs.ls(output_path)
    except Exception:
        return out
    for p in partitions:
        if not p.name.startswith("updated_date="):
            continue
        for f in dbutils.fs.ls(p.path):
            if not f.name.endswith(f".{file_extension}"):
                continue
            # relative path: "updated_date=YYYY-MM-DD/part_NNNN.ext"
            rel = f"{p.name.rstrip('/')}/{f.name}"
            out.append((rel, f.size, p.name.rstrip("/"), f.path))
    return out


def _count_records_jsonl(spark, files):
    """Return list of record counts (one per file) for gzipped JSONL files."""
    counts = []
    lock = threading.Lock()

    def _count_one(idx, full_path):
        n = spark.read.text(full_path).count()
        with lock:
            counts.append((idx, n))

    with ThreadPoolExecutor(max_workers=50) as pool:
        futs = [pool.submit(_count_one, i, t[3]) for i, t in enumerate(files)]
        for fut in as_completed(futs):
            fut.result()
    counts.sort(key=lambda x: x[0])
    return [c for _, c in counts]


def _count_records_parquet(spark, files):
    """Return list of record counts (one per file) for parquet files.

    Uses spark.read.parquet(...).count() which reads only the parquet footer
    (cheap — no row scan).
    """
    counts = [0] * len(files)
    lock = threading.Lock()

    def _count_one(idx, full_path):
        n = spark.read.parquet(full_path).count()
        with lock:
            counts[idx] = n

    with ThreadPoolExecutor(max_workers=20) as pool:
        futs = [pool.submit(_count_one, i, t[3]) for i, t in enumerate(files)]
        for fut in as_completed(futs):
            fut.result()
    return counts


def _write_entity_meta(dbutils, date_str: str, fmt: str, entity: str,
                       record_count: int, content_length: int,
                       file_entries: list):
    """Write `_meta/{fmt}/{entity}.json` with full per-file manifest entries.

    file_entries is a list of dicts:
      {"url": "...", "meta": {"content_length": N, "record_count": N}}
    """
    payload = {
        "entity": entity,
        "format": fmt,
        "record_count": record_count,
        "content_length": content_length,
        "files": file_entries,
    }
    meta_path = f"{S3_BASE}/{date_str}/_meta/{fmt}/{entity}.json"
    dbutils.fs.put(meta_path, json.dumps(payload), overwrite=True)


# ---------------------------------------------------------------------------
# JSONL.gz export — partitioned, with optional salting for large entities
# ---------------------------------------------------------------------------

def export_partitioned_jsonl(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                              salt: bool = False,
                              records_per_file: int = 400_000):
    """Write `df` as gzip-compressed JSON lines partitioned by updated_date.

    salt=True turns on hash-based salting for entities whose largest single
    updated_date partition exceeds ~1M records (works, authors). Otherwise the
    output is `coalesce(1)` per partition (small entities).

    Returns the per-entity total (record_count, content_length) and writes
    `_meta/jsonl/{entity}.json`.
    """
    output_path = f"{S3_BASE}/{date_str}/jsonl/{entity}"

    try:
        dbutils.fs.rm(output_path, recurse=True)
    except Exception:
        pass

    df_p = df.withColumn(
        "_partition_date",
        F.coalesce(F.to_date("updated_date"), F.to_date("created_date"), F.current_date()),
    )

    if salt:
        date_counts = (df_p.groupBy("_partition_date").count()
                          .withColumnRenamed("count", "date_count"))
        df_with_count = df_p.join(F.broadcast(date_counts), on="_partition_date")

        date_stats = (df_with_count.select("_partition_date", "date_count")
                                    .distinct().orderBy(F.desc("date_count")).collect())
        print(f"  jsonl: top date partitions for {entity}:")
        for row in date_stats[:5]:
            expected = (row["date_count"] + records_per_file - 1) // records_per_file
            print(f"    {row['_partition_date']}: {row['date_count']:,} recs → ~{expected} file(s)")

        df_salted = _apply_salting(df_with_count)
        df_out = df_salted.repartition(F.col("_partition_date"), F.col("salt")).drop("salt")

        (df_out.write
            .mode("overwrite")
            .option("compression", "gzip")
            .option("maxRecordsPerFile", records_per_file)
            .partitionBy("_partition_date")
            .option("ignoreNullFields", "false")
            .json(output_path))
    else:
        (df_p.coalesce(1).write
            .mode("overwrite")
            .option("compression", "gzip")
            .partitionBy("_partition_date")
            .option("ignoreNullFields", "false")
            .json(output_path))

    _rename_partitions(dbutils, output_path, "gz")

    # Build per-entity meta with per-file record counts.
    files = _enumerate_files(dbutils, output_path, "gz")
    if not files:
        _write_entity_meta(dbutils, date_str, "jsonl", entity, 0, 0, [])
        return 0, 0

    counts = _count_records_jsonl(spark, files)
    file_entries = []
    total_size = 0
    total_count = 0
    for (rel, size, _part, full_path), n in zip(files, counts):
        url = f"{S3_BASE}/{date_str}/jsonl/{entity}/{rel}"
        file_entries.append({"url": url, "meta": {"content_length": size, "record_count": n}})
        total_size += size
        total_count += n

    file_entries.sort(key=lambda e: e["url"])
    _write_entity_meta(dbutils, date_str, "jsonl", entity, total_count, total_size, file_entries)

    print(f"  jsonl: {entity} → {len(file_entries)} file(s), "
          f"{total_count:,} records, {total_size / (1024**3):.2f} GB")
    return total_count, total_size


# ---------------------------------------------------------------------------
# Parquet export — same partitioning shape as JSONL
# ---------------------------------------------------------------------------

def export_partitioned_parquet(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                                salt: bool = False,
                                records_per_file: int = 400_000):
    """Write `df` as snappy-compressed parquet partitioned by updated_date.

    Same salting logic as JSONL for large entities. Output:
      {S3_BASE}/{date}/parquet/{entity}/updated_date=*/part_NNNN.snappy.parquet
    """
    output_path = f"{S3_BASE}/{date_str}/parquet/{entity}"

    try:
        dbutils.fs.rm(output_path, recurse=True)
    except Exception:
        pass

    df_p = df.withColumn(
        "_partition_date",
        F.coalesce(F.to_date("updated_date"), F.to_date("created_date"), F.current_date()),
    )

    if salt:
        date_counts = (df_p.groupBy("_partition_date").count()
                          .withColumnRenamed("count", "date_count"))
        df_with_count = df_p.join(F.broadcast(date_counts), on="_partition_date")
        df_salted = _apply_salting(df_with_count)
        df_out = df_salted.repartition(F.col("_partition_date"), F.col("salt")).drop("salt")

        (df_out.write
            .mode("overwrite")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", records_per_file)
            .partitionBy("_partition_date")
            .parquet(output_path))
    else:
        (df_p.coalesce(1).write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("_partition_date")
            .parquet(output_path))

    _rename_partitions(dbutils, output_path, "snappy.parquet")

    files = _enumerate_files(dbutils, output_path, "snappy.parquet")
    if not files:
        _write_entity_meta(dbutils, date_str, "parquet", entity, 0, 0, [])
        return 0, 0

    counts = _count_records_parquet(spark, files)
    file_entries = []
    total_size = 0
    total_count = 0
    for (rel, size, _part, full_path), n in zip(files, counts):
        url = f"{S3_BASE}/{date_str}/parquet/{entity}/{rel}"
        file_entries.append({"url": url, "meta": {"content_length": size, "record_count": n}})
        total_size += size
        total_count += n

    file_entries.sort(key=lambda e: e["url"])
    _write_entity_meta(dbutils, date_str, "parquet", entity, total_count, total_size, file_entries)

    print(f"  parquet: {entity} → {len(file_entries)} file(s), "
          f"{total_count:,} records, {total_size / (1024**3):.2f} GB")
    return total_count, total_size


# ---------------------------------------------------------------------------
# Public entry point: write JSONL + parquet for an entity in parallel
# ---------------------------------------------------------------------------

def export_partitioned_all_formats(spark, dbutils, df: DataFrame, date_str: str, entity: str,
                                    salt: bool = False,
                                    records_per_file: int = 400_000):
    """Cache `df`, export to JSONL and parquet in parallel, write per-entity meta.

    Use salt=True only for large entities (works, authors). For other entities the
    coalesce(1) path produces a single part file per updated_date partition.

    `abstract_inverted_index`, when present and stored as a JSON string, is parsed
    to a MapType for the JSONL writer (matching the daily-changefile behavior).
    Parquet keeps the raw schema unchanged.
    """
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")

    df.cache()
    record_count = df.count()

    print(f"\n{'=' * 60}")
    print(f"Snapshot export — entity={entity} records={record_count:,} salt={salt}")
    print(f"{'=' * 60}")

    if record_count == 0:
        df.unpersist()
        for fmt in ("jsonl", "parquet"):
            _write_entity_meta(dbutils, date_str, fmt, entity, 0, 0, [])
        return

    # JSONL view: parse abstract_inverted_index to map for proper JSON output
    df_jsonl = df
    if "abstract_inverted_index" in df.columns:
        df_jsonl = df.withColumn(
            "abstract_inverted_index",
            F.from_json(
                F.col("abstract_inverted_index"),
                MapType(StringType(), ArrayType(IntegerType())),
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(export_partitioned_jsonl, spark, dbutils, df_jsonl,
                        date_str, entity, salt, records_per_file),
            pool.submit(export_partitioned_parquet, spark, dbutils, df,
                        date_str, entity, salt, records_per_file),
        ]
        for fut in as_completed(futs):
            fut.result()

    df.unpersist()
    print(f"  {entity}: export complete")
