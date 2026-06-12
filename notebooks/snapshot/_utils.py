# Databricks notebook source
# Shared utilities for the full-snapshot export (runs daily from end2end;
# quarterly releases additionally sync to the public bucket).
# %run'd by all notebooks/snapshot/export_*.ipynb.
#
# Output layout (mirrors notebooks/changefiles/daily/_utils.py shape):
#   s3://openalex-snapshots/full/{date}/jsonl/{entity}/updated_date=*/part_NNNN.gz
#   s3://openalex-snapshots/full/{date}/parquet/{entity}/updated_date=*/part_NNNN.parquet
#     (snappy-compressed, but named `.parquet` to match daily-changefiles convention)
#   s3://openalex-snapshots/full/{date}/_meta/{format}/{entity}.json   # consumed by update_meta
#
# Snapshot scale (works ~500M, authors ~110M) means we keep updated_date partitioning
# and produce many per-partition part files per entity per format — unlike the daily
# changefile which collapses to one merged file per entity.

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import unquote

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


def _partition_date_col():
    # Built lazily (not at module scope) so %run on serverless never constructs
    # Spark expressions at import time.
    return F.coalesce(
        F.to_date("updated_date"), F.to_date("created_date"), F.current_date()
    )


def _collect_date_counts(df: DataFrame):
    """Return [(partition_date, count), ...] for the salting broadcast join.

    Computed once per entity in export_partitioned_all_formats and shared by both
    format writers, so the 500M-row aggregation runs once instead of twice.
    """
    rows = (df.groupBy(_partition_date_col().alias("_partition_date"))
              .count().collect())
    return [(r["_partition_date"], r["count"]) for r in rows]


def _date_counts_df(spark, date_counts):
    return spark.createDataFrame(date_counts, "_partition_date date, date_count long")


# ---------------------------------------------------------------------------
# Filename rename + spark-metadata cleanup
# ---------------------------------------------------------------------------

def _s3_split(path: str):
    """'s3://bucket/key...' -> (bucket, key)."""
    p = path.replace("s3a://", "").replace("s3://", "").replace("dbfs:", "")
    bucket, key = p.split("/", 1)
    return bucket, key


def _get_s3_client(path: str):
    """Return a boto3 S3 client if ambient credentials can reach the bucket, else None.

    The rename fast path uses server-side CopyObject directly; dbutils.fs.mv pays
    ~1-15s of per-call overhead through the driver command channel.
    """
    try:
        import boto3
        bucket, _ = _s3_split(path + "/x")
        client = boto3.client("s3")
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return client
    except Exception as e:
        print(f"  boto3 fast path unavailable ({type(e).__name__}: {e}); "
              f"falling back to dbutils.fs.mv")
        return None


def _rename_partitions(dbutils, output_path: str, match_extension: str,
                       target_extension: str = None, max_workers: int = 64):
    """Rename Spark's `_partition_date=` directories to `updated_date=` and rename
    each part file from Spark's auto-generated name to `part_NNNN.{target_extension}`.

    `match_extension` is the suffix Spark writes (e.g. "gz", "snappy.parquet").
    `target_extension` is the suffix we want on disk (defaults to match_extension).
    For parquet we pass match="snappy.parquet" / target="parquet" so files end up
    named `part_NNNN.parquet` even though they're snappy-compressed — matching the
    daily-changefiles convention.

    All moves run on one flat thread pool (no per-partition serialization), using
    boto3 server-side copies when credentials allow. Any failed move raises so a
    half-renamed export can't be silently published.
    """
    if target_extension is None:
        target_extension = match_extension

    partitions = dbutils.fs.ls(output_path)
    partitions_to_process = [p for p in partitions if p.name.startswith("_partition_date=")]

    print(f"  Renaming {len(partitions_to_process)} partition(s) "
          f"(match=.{match_extension} → target=.{target_extension})")

    # Phase 1: list all partition dirs in parallel, build one flat move list.
    moves = []  # (src_path, dst_path, size)
    lock = threading.Lock()

    def _collect(partition):
        date_value = partition.name.replace("_partition_date=", "").rstrip("/")
        new_partition_path = f"{output_path}/updated_date={date_value}/"
        files = dbutils.fs.ls(partition.path)
        data_files = sorted(
            [f for f in files if f.name.endswith(f".{match_extension}")],
            key=lambda x: x.name,
        )
        local = [
            (f.path, f"{new_partition_path}part_{str(idx).zfill(4)}.{target_extension}", f.size)
            for idx, f in enumerate(data_files)
        ]
        with lock:
            moves.extend(local)

    with ThreadPoolExecutor(max_workers=32) as pool:
        for fut in as_completed([pool.submit(_collect, p) for p in partitions_to_process]):
            fut.result()

    print(f"  {len(moves)} file move(s) queued")

    # Phase 2: copy every file to its new name.
    start = time.time()
    done = {"n": 0}

    def _progress():
        with lock:
            done["n"] += 1
            n = done["n"]
        if n % 250 == 0 or n == len(moves):
            print(f"  [{n}/{len(moves)}] moved ({time.time() - start:.1f}s)")

    s3 = _get_s3_client(output_path) if moves else None

    if s3 is not None:
        from boto3.s3.transfer import TransferConfig
        big_cfg = TransferConfig(multipart_threshold=4 * 1024**3, max_concurrency=4)

        def _move(src, dst, size):
            src_b, src_k = _s3_split(src)
            dst_b, dst_k = _s3_split(dst)
            if size < 4 * 1024**3:
                s3.copy_object(CopySource={"Bucket": src_b, "Key": src_k},
                               Bucket=dst_b, Key=dst_k)
            else:  # CopyObject caps at 5GB; managed copy does multipart
                s3.copy({"Bucket": src_b, "Key": src_k}, dst_b, dst_k, Config=big_cfg)
            _progress()
    else:
        def _move(src, dst, size):
            dbutils.fs.mv(src, dst)
            _progress()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for fut in as_completed([pool.submit(_move, *m) for m in moves]):
            fut.result()  # propagate any failure — don't publish a partial rename

    # Phase 3: remove old `_partition_date=` dirs (covers the boto3-path originals
    # plus Spark's per-partition metadata files) and root-level _SUCCESS etc.
    def _cleanup(partition):
        dbutils.fs.rm(partition.path, recurse=True)

    with ThreadPoolExecutor(max_workers=32) as pool:
        for fut in as_completed([pool.submit(_cleanup, p) for p in partitions_to_process]):
            fut.result()

    try:
        for f in dbutils.fs.ls(output_path):
            if f.name.startswith("_"):
                dbutils.fs.rm(f.path, recurse=True)
    except Exception as e:
        print(f"  warning: root cleanup failed: {e}")

    print(f"  rename complete: {len(moves)} file(s) in {time.time() - start:.1f}s")


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


def _count_records_jsonl(spark, files, output_path: str):
    """Return list of record counts (one per file) for gzipped JSONL files.

    One distributed job over the whole export (grouped by input_file_name) instead
    of one driver-scheduled job per file — the data still has to be gunzipped once,
    but with full cluster parallelism.
    """
    rows = (spark.read.text(f"{output_path}/updated_date=*")
                 .groupBy(F.input_file_name().alias("path"))
                 .count().collect())
    by_rel = {}
    for r in rows:
        path = unquote(r["path"])
        rel = "/".join(path.split("/")[-2:])  # updated_date=YYYY-MM-DD/part_NNNN.gz
        by_rel[rel] = r["count"]
    return [by_rel.get(rel, 0) for rel, _size, _part, _full in files]


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
                              records_per_file: int = 400_000,
                              date_counts: list = None):
    """Write `df` as gzip-compressed JSON lines partitioned by updated_date.

    salt=True turns on hash-based salting for entities whose largest single
    updated_date partition exceeds ~1M records (works, authors). Otherwise the
    output is `coalesce(1)` per partition (small entities).

    `date_counts` (optional, [(date, count), ...]) lets the caller share one
    precomputed per-date aggregation across both format writers.

    Returns the per-entity total (record_count, content_length) and writes
    `_meta/jsonl/{entity}.json`.
    """
    output_path = f"{S3_BASE}/{date_str}/jsonl/{entity}"

    try:
        dbutils.fs.rm(output_path, recurse=True)
    except Exception:
        pass

    df_p = df.withColumn("_partition_date", _partition_date_col())

    if salt:
        if date_counts is None:
            date_counts = _collect_date_counts(df)
        df_with_count = df_p.join(F.broadcast(_date_counts_df(spark, date_counts)),
                                  on="_partition_date")
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

    counts = _count_records_jsonl(spark, files, output_path)
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
                                records_per_file: int = 400_000,
                                date_counts: list = None):
    """Write `df` as snappy-compressed parquet partitioned by updated_date.

    Same salting logic as JSONL for large entities. Output:
      {S3_BASE}/{date}/parquet/{entity}/updated_date=*/part_NNNN.parquet
    (snappy-compressed; the `.snappy.` suffix is dropped to match daily changefiles)
    """
    # Force INT64 microsecond encoding. Spark's default (INT96) is deprecated,
    # and `F.to_timestamp(...)` on Spark 4 / DBR 16.x can produce INT64 nanoseconds
    # that downstream readers (polars, the Databricks SQL warehouse, older
    # pyarrow) cannot decode. Set here (not at module scope) so update_meta /
    # smoke_tests can %run _utils on serverless without hitting CONFIG_NOT_AVAILABLE.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    output_path = f"{S3_BASE}/{date_str}/parquet/{entity}"

    try:
        dbutils.fs.rm(output_path, recurse=True)
    except Exception:
        pass

    df_p = df.withColumn("_partition_date", _partition_date_col())

    if salt:
        if date_counts is None:
            date_counts = _collect_date_counts(df)
        df_with_count = df_p.join(F.broadcast(_date_counts_df(spark, date_counts)),
                                  on="_partition_date")
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

    _rename_partitions(dbutils, output_path, match_extension="snappy.parquet",
                       target_extension="parquet")

    files = _enumerate_files(dbutils, output_path, "parquet")
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
    """Export `df` to JSONL and parquet in parallel, write per-entity meta.

    Use salt=True only for large entities (works, authors). For other entities the
    coalesce(1) path produces a single part file per updated_date partition.

    No caching: every caller passes either a materialized snapshot table or a cheap
    transform of one, so re-reading Delta in each format writer is faster than
    forcing a full memory/disk materialization up front.

    `abstract_inverted_index`, when present and stored as a JSON string, is parsed
    to a MapType for the JSONL writer (matching the daily-changefile behavior).
    Parquet keeps the raw schema unchanged.
    """
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")

    print(f"\n{'=' * 60}")
    print(f"Snapshot export — entity={entity} salt={salt}")
    print(f"{'=' * 60}")

    if df.isEmpty():
        for fmt in ("jsonl", "parquet"):
            _write_entity_meta(dbutils, date_str, fmt, entity, 0, 0, [])
        return

    # One per-date aggregation shared by both format writers (salting input).
    date_counts = None
    if salt:
        date_counts = _collect_date_counts(df)
        print(f"  top date partitions for {entity}:")
        for date_value, n in sorted(date_counts, key=lambda t: -t[1])[:5]:
            expected = (n + records_per_file - 1) // records_per_file
            print(f"    {date_value}: {n:,} recs → ~{expected} file(s)")

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
                        date_str, entity, salt, records_per_file, date_counts),
            pool.submit(export_partitioned_parquet, spark, dbutils, df,
                        date_str, entity, salt, records_per_file, date_counts),
        ]
        for fut in as_completed(futs):
            fut.result()

    print(f"  {entity}: export complete")
