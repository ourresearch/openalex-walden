# Databricks notebook source
# Sync full snapshot from staging (s3://openalex-snapshots/full/{date}/)
# to the public bucket (s3://openalex/data/).
#
# Runs only on quarterly releases. Monthly snapshots stay in staging only and
# are served to enterprise customers via the API-key gateway. Public free-tier
# consumers continue to receive a quarterly drop here.
#
# Layout in staging (set by notebooks/snapshot/_utils.py):
#   {staging_base}/jsonl/{entity}/updated_date=*/part_NNNN.gz
#   {staging_base}/parquet/{entity}/updated_date=*/part_NNNN.snappy.parquet
#   {staging_base}/{format}/{entity}/manifest.json   # per-entity manifest
#   {staging_base}/{format}/manifest.json            # per-format combined manifest
#
# Transfer model:
#   The two buckets live in different AWS accounts with no single principal that
#   can read staging AND write public, so a server-side S3->S3 copy is not
#   available. Instead each file is streamed staging->public THROUGH the cluster:
#   the read side uses the "webscraper" keys (which can read staging), the write
#   side uses the "openalex-open-data" keys (which can write public). boto3's
#   default credential chain has no access to either bucket on these clusters.
#
#   The copy is distributed across Spark executors (mapPartitions) and streamed
#   object-to-object (no local disk round-trip), so aggregate throughput scales
#   with worker count instead of being pinned to a single driver NIC.
#
#   Ordering is copy-first, delete-stale-last: old public objects are overwritten
#   in place and any objects no longer in the snapshot are deleted only AFTER the
#   copy + verification succeed. A failed/aborted run therefore never leaves the
#   public prefix with fewer files than it started with.

import json
import os
import random
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Quarterly-release gate
# ---------------------------------------------------------------------------

dbutils.widgets.text("is_quarterly_release", "false")
_quarterly_param = dbutils.widgets.get("is_quarterly_release").strip().lower()
IS_QUARTERLY = _quarterly_param in ("true", "1", "yes")

if not IS_QUARTERLY:
    print(f"is_quarterly_release={_quarterly_param!r} — skipping public sync.")
    print("Snapshot remains in staging only (s3://openalex-snapshots/full/...).")
    dbutils.notebook.exit("skipped: monthly run, no public sync")

print(f"is_quarterly_release={_quarterly_param!r} — proceeding with public sync.")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGING_BUCKET = "openalex-snapshots"
PUBLIC_BUCKET = "openalex"
PUBLIC_PREFIX = "data"
FORMATS = ("jsonl", "parquet")

# Shared snapshot date (default {{job.start_time.iso_date}}, UTC, fixed at job launch)
# so the public sync reads the same staging folder the exports wrote to, even across
# a UTC-midnight straddle. Falls back to now(UTC) for ad-hoc runs.
dbutils.widgets.text("snapshot_date", "")
date_str = dbutils.widgets.get("snapshot_date").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
staging_base = f"s3://{STAGING_BUCKET}/full/{date_str}"
staging_key_prefix = f"full/{date_str}"  # S3 key prefix (no scheme/bucket) for boto3
local_scratch = "/local_disk0/s3_transfer"

# Manifests are written against the staging layout
# (s3://openalex-snapshots/full/{date}/{fmt}/{entity}/...). On the public bucket
# the same files live at s3://openalex/data/{fmt}/{entity}/..., so every URL inside
# a manifest must be repointed from the staging prefix to the public prefix before
# it is published — otherwise public consumers are sent to an inaccessible bucket.
STAGING_URL_PREFIX = f"{staging_base}/"
PUBLIC_URL_PREFIX = f"s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/"

MAX_RETRIES = 3

# Per-file multipart streaming config (applied on each executor).
MULTIPART_THRESHOLD = 64 * 1024 * 1024
MULTIPART_CHUNKSIZE = 64 * 1024 * 1024
MULTIPART_CONCURRENCY = 8

# Number of Spark partitions to slice each entity's (shuffled) file list into.
# Fixed rather than tied to current defaultParallelism so an autoscaling cluster
# still gets enough pending tasks to scale up during the big entities (works).
TARGET_SLICES = 256

# S3 batch-delete caps at 1000 keys per request.
DELETE_BATCH = 1000

ENTITIES = [
    "works",
    "authors",
    "institutions",
    "sources",
    "publishers",
    "funders",
    "awards",
    "keywords",
    "concepts",
    "topics",
    "subfields",
    "fields",
    "domains",
    "continents",
    "countries",
    "institution-types",
    "languages",
    "licenses",
    "sdgs",
    "source-types",
    "work-types",
]

print(f"Snapshot date: {date_str}")
print(f"Staging base:  {staging_base}")
print(f"Public:        s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/")
print(f"Formats:       {FORMATS}")
print(f"Entities:      {len(ENTITIES)}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Preflight: staging readable (driver AND executors), public creds valid
# ---------------------------------------------------------------------------

import boto3

# Verify staging is accessible via dbutils — both format subtrees must exist
try:
    staging_entries = dbutils.fs.ls(staging_base)
    top_dirs = {e.name.rstrip("/") for e in staging_entries if not e.name.startswith("_")}
    missing = [fmt for fmt in FORMATS if fmt not in top_dirs]
    if missing:
        raise RuntimeError(f"missing format subdirectories in staging: {missing}")
    print(f"Staging preflight OK: {sorted(top_dirs)} found at {staging_base}")
except Exception as e:
    raise RuntimeError(f"Staging preflight FAILED — cannot access {staging_base}: {e}")

# Staging-read credentials. boto3's default chain has NO access to the staging
# bucket on these clusters — dbutils/S3A read it via Databricks-managed creds that
# boto3 does not share, and no EC2 instance profile is attached. Read staging with
# the explicit "webscraper" keys, the same scope the daily changefiles job uses for
# boto3 access to s3://openalex-snapshots.
staging_access_key = dbutils.secrets.get("webscraper", "aws_access_key_id")
staging_secret_key = dbutils.secrets.get("webscraper", "aws_secret_access_key")
staging_client = boto3.client(
    "s3",
    aws_access_key_id=staging_access_key,
    aws_secret_access_key=staging_secret_key,
)
staging_client.head_bucket(Bucket=STAGING_BUCKET)
print(f"Driver staging-read OK: s3://{STAGING_BUCKET} reachable via boto3 (webscraper creds)")

# Public-bucket write credentials
public_access_key = dbutils.secrets.get("openalex-open-data", "aws_access_key_id")
public_secret_key = dbutils.secrets.get("openalex-open-data", "aws_secret_access_key")

_preflight_public = boto3.client(
    "s3",
    aws_access_key_id=public_access_key,
    aws_secret_access_key=public_secret_key,
)
_preflight_public.head_bucket(Bucket=PUBLIC_BUCKET)
print(f"Public-bucket preflight OK: s3://{PUBLIC_BUCKET} accessible")
del _preflight_public

# Broadcast both credential pairs to executors: staging keys for the read side,
# open-data keys for the write side.
sc = spark.sparkContext
_bc_staging_keys = sc.broadcast((staging_access_key, staging_secret_key))
_bc_public_keys = sc.broadcast((public_access_key, public_secret_key))
print(f"Cluster defaultParallelism: {sc.defaultParallelism}")


def executor_staging_read_check():
    """Confirm EXECUTORS (not just the driver) can read staging via boto3.

    The distributed copy reads staging from worker nodes with the broadcast
    webscraper keys. Probe that from executors here so a credential gap fails
    fast, before any public-bucket writes.
    """
    n = min(max(sc.defaultParallelism, 1), 8)
    staging_bucket = STAGING_BUCKET
    prefix = staging_key_prefix
    bc_keys = _bc_staging_keys

    def _probe(_rows):
        import boto3 as _b
        ak, sk = bc_keys.value
        client = _b.client("s3", aws_access_key_id=ak, aws_secret_access_key=sk)
        client.list_objects_v2(Bucket=staging_bucket, Prefix=prefix, MaxKeys=1)
        yield 1

    try:
        got = sc.parallelize(range(n), numSlices=n).mapPartitions(_probe).collect()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "Executors cannot read the staging bucket via boto3 "
            f"({type(e).__name__}: {e}). The distributed copy needs the webscraper "
            "keys usable on workers — aborting before any public-bucket writes."
        )
    print(f"Executor staging-read preflight OK ({len(got)} partitions probed)")


executor_staging_read_check()

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_public_client = None


def _get_public_client():
    """Return a cached driver-side boto3 client for the public bucket.

    Driver-side calls (list, delete, manifest upload) are single-threaded, so one
    cached client is sufficient. The executor copy path builds its own clients.
    """
    global _public_client
    if _public_client is None:
        _public_client = boto3.client(
            "s3",
            aws_access_key_id=public_access_key,
            aws_secret_access_key=public_secret_key,
        )
    return _public_client


def list_staging_keys(fmt, entity):
    """List all data files for an entity+format in staging via boto3.

    Returns a list of (staging_key, public_key, size):
      staging_key: full S3 key in the staging bucket
      public_key:  destination key in the public bucket ("data/{fmt}/{entity}/...")
      size:        object size in bytes

    The per-entity manifest.json is skipped here — it is rewritten + uploaded
    separately (its URLs change, and so does its size).
    """
    prefix = f"{staging_key_prefix}/{fmt}/{entity}/"
    cut = len(f"{staging_key_prefix}/")  # strip "full/{date}/" -> "{fmt}/{entity}/..."
    files = []
    paginator = staging_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=STAGING_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.rsplit("/", 1)[-1] == "manifest.json":
                continue
            relative = key[cut:]  # "{fmt}/{entity}/updated_date=.../part_XXXX.{ext}"
            public_key = f"{PUBLIC_PREFIX}/{relative}"
            files.append((key, public_key, obj["Size"]))
    return files


def list_public_prefix(prefix):
    """Return {key: size} for every object under a public-bucket prefix."""
    client = _get_public_client()
    paginator = client.get_paginator("list_objects_v2")
    out = {}
    for page in paginator.paginate(Bucket=PUBLIC_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            out[obj["Key"]] = obj["Size"]
    return out


def delete_keys(keys):
    """Batch-delete explicit public-bucket keys. Returns the count deleted."""
    client = _get_public_client()
    deleted = 0
    for i in range(0, len(keys), DELETE_BATCH):
        batch = keys[i:i + DELETE_BATCH]
        client.delete_objects(
            Bucket=PUBLIC_BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        deleted += len(batch)
    return deleted


def _make_copy_fn(staging_bucket, public_bucket, bc_staging_keys, bc_public_keys,
                  max_retries, mp_threshold, mp_chunksize, mp_concurrency):
    """Build the mapPartitions function that streams staging->public on executors.

    Read side uses the broadcast webscraper keys; write side uses the broadcast
    open-data keys. Each object is streamed straight from the GetObject body into
    a multipart upload — no local disk.
    """
    def _copy_partition(rows):
        import time as _time
        import boto3 as _boto3
        from boto3.s3.transfer import TransferConfig

        sak, ssk = bc_staging_keys.value
        src = _boto3.client("s3", aws_access_key_id=sak, aws_secret_access_key=ssk)
        pak, psk = bc_public_keys.value
        dst = _boto3.client("s3", aws_access_key_id=pak, aws_secret_access_key=psk)
        cfg = TransferConfig(
            multipart_threshold=mp_threshold,
            multipart_chunksize=mp_chunksize,
            max_concurrency=mp_concurrency,
            use_threads=True,
        )

        for staging_key, public_key, size in rows:
            last_err = None
            for attempt in range(max_retries):
                try:
                    # Re-GET each attempt: a failed upload consumes the stream body.
                    body = src.get_object(Bucket=staging_bucket, Key=staging_key)["Body"]
                    dst.upload_fileobj(body, public_bucket, public_key, Config=cfg)
                    head = dst.head_object(Bucket=public_bucket, Key=public_key)
                    public_size = head["ContentLength"]
                    if public_size != size:
                        raise RuntimeError(
                            f"size mismatch for {public_key}: expected {size}, got {public_size}"
                        )
                    yield (public_key, public_size)
                    break
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    if attempt < max_retries - 1:
                        _time.sleep(2 ** attempt)
            else:
                raise RuntimeError(
                    f"Failed after {max_retries} attempts for {public_key}: {last_err}"
                )

    return _copy_partition


copy_partition_fn = _make_copy_fn(
    STAGING_BUCKET, PUBLIC_BUCKET, _bc_staging_keys, _bc_public_keys, MAX_RETRIES,
    MULTIPART_THRESHOLD, MULTIPART_CHUNKSIZE, MULTIPART_CONCURRENCY,
)


def _rewrite_manifest_urls(obj):
    """Recursively repoint staging S3 URLs to their public-bucket equivalents.

    Staging layout:  s3://openalex-snapshots/full/{date}/{fmt}/{entity}/...
    Public layout:   s3://openalex/data/{fmt}/{entity}/...

    Any string starting with the staging prefix has that prefix swapped for the
    public prefix; everything else is returned unchanged.
    """
    if isinstance(obj, dict):
        return {k: _rewrite_manifest_urls(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_rewrite_manifest_urls(v) for v in obj]
    if isinstance(obj, str) and obj.startswith(STAGING_URL_PREFIX):
        return PUBLIC_URL_PREFIX + obj[len(STAGING_URL_PREFIX):]
    return obj


def _publish_manifest(manifest_dbfs, public_key, local_rel):
    """Copy a manifest from staging, rewrite its URLs to the public layout, upload it.

    Returns the uploaded byte size. Raises if any staging reference survives the
    rewrite (a guard against the public manifest pointing back at the private bucket).
    """
    local_path = os.path.join(local_scratch, local_rel)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    dbutils.fs.cp(manifest_dbfs, f"file:{local_path}")
    with open(local_path) as f:
        manifest = json.load(f)

    rewritten = _rewrite_manifest_urls(manifest)
    serialized = json.dumps(rewritten, indent=2)

    # Guard: no staging-bucket reference may leak into a published manifest.
    if f"s3://{STAGING_BUCKET}/" in serialized:
        raise RuntimeError(f"staging URL survived rewrite for {public_key}")

    with open(local_path, "w") as f:
        f.write(serialized)
    local_size = os.path.getsize(local_path)

    client = _get_public_client()
    client.upload_file(local_path, PUBLIC_BUCKET, public_key)

    head = client.head_object(Bucket=PUBLIC_BUCKET, Key=public_key)
    if head["ContentLength"] != local_size:
        raise RuntimeError(
            f"Manifest size mismatch for {public_key}: "
            f"expected {local_size}, got {head['ContentLength']}"
        )

    try:
        os.remove(local_path)
    except OSError:
        pass

    return local_size


def upload_entity_manifest(fmt, entity):
    """Rewrite + upload the per-entity manifest (lives inside the entity folder).

    Source: {staging_base}/{fmt}/{entity}/manifest.json
    Target: s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/{fmt}/{entity}/manifest.json
    """
    return _publish_manifest(
        f"{staging_base}/{fmt}/{entity}/manifest.json",
        f"{PUBLIC_PREFIX}/{fmt}/{entity}/manifest.json",
        os.path.join(fmt, entity, "manifest.json"),
    )


def upload_manifest(fmt):
    """Rewrite + upload the per-format combined manifest (last, as completion marker).

    Source: {staging_base}/{fmt}/manifest.json
    Target: s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/{fmt}/manifest.json
    """
    return _publish_manifest(
        f"{staging_base}/{fmt}/manifest.json",
        f"{PUBLIC_PREFIX}/{fmt}/manifest.json",
        os.path.join(fmt, "manifest.json"),
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------

os.makedirs(local_scratch, exist_ok=True)

overall_start = time.time()
entity_results = []

for fmt in FORMATS:
    for entity in ENTITIES:
        print(f"\n{'=' * 60}")
        print(f"Syncing: {fmt}/{entity}")
        print(f"{'=' * 60}")
        entity_start = time.time()
        prefix = f"{PUBLIC_PREFIX}/{fmt}/{entity}/"

        # 1. List staging data files (boto3, server-side pagination).
        files = list_staging_keys(fmt, entity)
        staging_total_size = sum(f[2] for f in files)
        print(f"  Staging: {len(files)} files, {staging_total_size / (1024**3):.2f} GB")

        if not files:
            print(f"  WARNING: No files found for {fmt}/{entity} — skipping")
            entity_results.append({
                "format": fmt, "entity": entity, "status": "skipped",
                "files": 0, "size_gb": 0,
            })
            continue

        # 2. Copy-first: stream data files staging->public, distributed across
        #    executors, OVERWRITING in place. Old public objects stay until
        #    overwritten, so a failed run never shrinks the public prefix.
        #    Shuffle so each contiguous Spark slice gets a mix of sizes (the
        #    staging list is date-ordered, i.e. tiny-old -> huge-recent).
        shuffled = list(files)
        random.shuffle(shuffled)
        n_slices = max(1, min(len(shuffled), TARGET_SLICES))
        results = sc.parallelize(shuffled, numSlices=n_slices).mapPartitions(copy_partition_fn).collect()

        uploaded_count = len(results)
        uploaded_size = sum(s for _, s in results)
        print(f"  Copied {uploaded_count} files ({uploaded_size / (1024**3):.2f} GB) "
              f"across {n_slices} partitions")
        if uploaded_count != len(files):
            raise RuntimeError(
                f"Copy count mismatch for {fmt}/{entity}: "
                f"staged {len(files)}, copied {uploaded_count}"
            )

        # 3. Verify the expected new set landed (count + total size) by listing
        #    the public prefix once.
        expected = {pk for _, pk, _ in files}
        present = list_public_prefix(prefix)
        missing = expected - present.keys()
        if missing:
            raise RuntimeError(
                f"{len(missing)} expected file(s) missing from public bucket for "
                f"{fmt}/{entity} (e.g. {sorted(missing)[:3]})"
            )
        public_total_size = sum(present[k] for k in expected)
        if public_total_size != staging_total_size:
            raise RuntimeError(
                f"Total size mismatch for {fmt}/{entity}: "
                f"staged {staging_total_size}, public bucket has {public_total_size}"
            )

        # 4. Delete-stale-last: remove public objects no longer in the snapshot,
        #    only after the copy + verification succeeded. Exclude the per-entity
        #    manifest.json (rewritten + re-uploaded in step 5).
        stale = [
            k for k in present
            if k not in expected and not k.endswith("/manifest.json")
        ]
        if stale:
            delete_keys(stale)
            print(f"  Deleted {len(stale)} stale public-bucket files")

        # 5. Rewrite + upload the per-entity manifest (URLs repointed to public).
        entity_manifest_size = upload_entity_manifest(fmt, entity)
        print(f"  Entity manifest uploaded ({entity_manifest_size / 1024:.1f} KB)")

        elapsed = time.time() - entity_start
        rate = (uploaded_size / (1024**3)) / elapsed if elapsed > 0 else 0
        print(f"  Done: {len(files)} files, {uploaded_size / (1024**3):.2f} GB, "
              f"{elapsed:.0f}s ({rate:.2f} GB/s)")

        entity_results.append({
            "format": fmt, "entity": entity, "status": "ok",
            "files": len(files), "size_gb": round(uploaded_size / (1024**3), 2),
            "elapsed_s": round(elapsed),
        })

    # After all entities for this format are uploaded, copy the per-format manifest
    # (acts as the completion marker for that format).
    manifest_size = upload_manifest(fmt)
    print(f"\n  {fmt} manifest uploaded ({manifest_size / 1024:.1f} KB)")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

total_elapsed = time.time() - overall_start

print(f"\n{'=' * 60}")
print(f"SYNC COMPLETE")
print(f"{'=' * 60}")
print(f"Date:     {date_str}")
print(f"Duration: {total_elapsed / 60:.1f} minutes")
print(f"")

ok = [r for r in entity_results if r["status"] == "ok"]
skipped = [r for r in entity_results if r["status"] == "skipped"]

total_files = sum(r["files"] for r in ok)
total_gb = sum(r["size_gb"] for r in ok)
total_units = len(FORMATS) * len(ENTITIES)

print(f"Format/entity pairs synced: {len(ok)}/{total_units}")
print(f"Total files:                {total_files:,}")
print(f"Total size:                 {total_gb:.2f} GB")
if total_elapsed > 0:
    print(f"Avg throughput:             {total_gb / total_elapsed:.2f} GB/s")

if skipped:
    print(f"\nSkipped: {[(r['format'], r['entity']) for r in skipped]}")

print(f"\nPer-(format,entity) breakdown:")
for r in entity_results:
    label = f"{r['format']}/{r['entity']}"
    if r["status"] == "ok":
        print(f"  {label:30s}  {r['files']:6,} files  {r['size_gb']:8.2f} GB  {r['elapsed_s']:5d}s")
    else:
        print(f"  {label:30s}  SKIPPED")

print(f"\nPublic snapshot ready at: s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Upload RELEASE_NOTES.txt to public bucket root
# ---------------------------------------------------------------------------

import requests as _requests

RELEASE_NOTES_URL = (
    "https://raw.githubusercontent.com/ourresearch/openalex-walden"
    "/main/notebooks/snapshot/RELEASE_NOTES.txt"
)

print("Uploading RELEASE_NOTES.txt to s3://openalex/ ...")

resp = _requests.get(RELEASE_NOTES_URL, timeout=30)
resp.raise_for_status()

os.makedirs(local_scratch, exist_ok=True)
local_rn = os.path.join(local_scratch, "RELEASE_NOTES.txt")
with open(local_rn, "w") as f:
    f.write(resp.text)

client = _get_public_client()
client.upload_file(local_rn, PUBLIC_BUCKET, "RELEASE_NOTES.txt")

head = client.head_object(Bucket=PUBLIC_BUCKET, Key="RELEASE_NOTES.txt")
print(f"  Uploaded RELEASE_NOTES.txt ({head['ContentLength']} bytes)")

try:
    os.remove(local_rn)
except OSError:
    pass
