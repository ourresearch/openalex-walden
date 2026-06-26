# Databricks notebook source
# Sync full snapshot from staging (s3://openalex-snapshots/full/{date}/)
# to the public bucket (s3://openalex/data/) using boto3 for the public bucket.
#
# Runs only on quarterly releases. Monthly snapshots stay in staging only and
# are served to enterprise customers via the API-key gateway. Public free-tier
# consumers continue to receive a quarterly drop here.
#
# Layout in staging (set by notebooks/snapshot/_utils.py):
#   {staging_base}/jsonl/{entity}/updated_date=*/part_NNNN.gz
#   {staging_base}/parquet/{entity}/updated_date=*/part_NNNN.snappy.parquet
#   {staging_base}/{format}/manifest.json   # per-format combined manifest
#
# Staging bucket is accessible natively via Databricks (instance profile).
# The public bucket requires separate AWS credentials from the
# "openalex-open-data" secret scope.

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
local_scratch = "/local_disk0/s3_transfer"

# Manifests are written against the staging layout
# (s3://openalex-snapshots/full/{date}/{fmt}/{entity}/...). On the public bucket
# the same files live at s3://openalex/data/{fmt}/{entity}/..., so every URL inside
# a manifest must be repointed from the staging prefix to the public prefix before
# it is published — otherwise public consumers are sent to an inaccessible bucket.
STAGING_URL_PREFIX = f"{staging_base}/"
PUBLIC_URL_PREFIX = f"s3://{PUBLIC_BUCKET}/{PUBLIC_PREFIX}/"

MAX_WORKERS = 30
MAX_RETRIES = 3

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
# Preflight: verify staging snapshot exists and public-bucket credentials work
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

# Verify public-bucket credentials
public_access_key = dbutils.secrets.get("openalex-open-data", "aws_access_key_id")
public_secret_key = dbutils.secrets.get("openalex-open-data", "aws_secret_access_key")

public_client = boto3.client(
    "s3",
    aws_access_key_id=public_access_key,
    aws_secret_access_key=public_secret_key,
)
public_client.head_bucket(Bucket=PUBLIC_BUCKET)
print(f"Public-bucket preflight OK: s3://{PUBLIC_BUCKET} accessible")
del public_client

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _get_public_client():
    """Return a thread-local boto3 S3 client for the public bucket."""
    if not hasattr(_thread_local, "s3_client"):
        _thread_local.s3_client = boto3.client(
            "s3",
            aws_access_key_id=public_access_key,
            aws_secret_access_key=public_secret_key,
        )
    return _thread_local.s3_client


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


def list_staging_files(fmt, entity):
    """List all files for an entity+format in staging.

    Returns a list of (dbfs_path, relative_key, size). The relative_key starts with
    `{fmt}/{entity}/...` so it can be appended directly to PUBLIC_PREFIX.
    """
    entity_path = f"{staging_base}/{fmt}/{entity}"
    files = []
    marker = f"/{fmt}/{entity}/"

    def _walk(path):
        try:
            entries = dbutils.fs.ls(path)
        except Exception:
            return
        for entry in entries:
            if entry.isDir():
                _walk(entry.path)
            else:
                # The per-entity manifest.json lives at the entity root. Skip it here:
                # its URLs need staging→public rewriting (which changes its size), so
                # it's published separately via upload_entity_manifest().
                if entry.name == "manifest.json":
                    continue
                # Build the relative key: {fmt}/{entity}/updated_date=.../part_XXXX.{ext}
                raw = entry.path.replace("dbfs:", "").replace("s3:", "")
                idx = raw.find(marker)
                relative = raw[idx + 1:]  # e.g. "jsonl/works/updated_date=.../part_0000.gz"
                files.append((entry.path, relative, entry.size))

    _walk(entity_path)
    return files


def upload_file_to_public(dbfs_path, relative_key, expected_size):
    """Download a file from staging to local disk, then upload to the public bucket.

    Returns (relative_key, public_size) on success.
    """
    public_key = f"{PUBLIC_PREFIX}/{relative_key}"
    local_dir = os.path.join(local_scratch, os.path.dirname(relative_key))
    local_path = os.path.join(local_scratch, relative_key)

    os.makedirs(local_dir, exist_ok=True)

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            # Copy from staging (DBFS/S3) to local NVMe
            dbutils.fs.cp(dbfs_path, f"file:{local_path}")

            # Verify local size matches staging
            local_size = os.path.getsize(local_path)
            if local_size != expected_size:
                raise RuntimeError(
                    f"Local size mismatch: expected {expected_size}, got {local_size}"
                )

            # Upload to public bucket
            client = _get_public_client()
            client.upload_file(local_path, PUBLIC_BUCKET, public_key)

            # Verify public-bucket size
            head = client.head_object(Bucket=PUBLIC_BUCKET, Key=public_key)
            public_size = head["ContentLength"]
            if public_size != expected_size:
                raise RuntimeError(
                    f"Public size mismatch: expected {expected_size}, got {public_size}"
                )

            return relative_key, public_size

        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
        finally:
            # Clean up local file
            try:
                os.remove(local_path)
            except OSError:
                pass

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts for {relative_key}: {last_err}")


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

        # 1. List staging files
        files = list_staging_files(fmt, entity)
        staging_total_size = sum(f[2] for f in files)
        print(f"  Staging: {len(files)} files, {staging_total_size / (1024**3):.2f} GB")

        if not files:
            print(f"  WARNING: No files found for {fmt}/{entity} — skipping")
            entity_results.append({
                "format": fmt,
                "entity": entity,
                "status": "skipped",
                "files": 0,
                "size_gb": 0,
            })
            continue

        # 2. Delete old public-bucket files for this format+entity
        prefix = f"{PUBLIC_PREFIX}/{fmt}/{entity}/"
        del_client = _get_public_client()
        del_paginator = del_client.get_paginator("list_objects_v2")
        deleted = 0
        for page in del_paginator.paginate(Bucket=PUBLIC_BUCKET, Prefix=prefix):
            objects = page.get("Contents", [])
            if not objects:
                continue
            del_client.delete_objects(
                Bucket=PUBLIC_BUCKET,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects], "Quiet": True},
            )
            deleted += len(objects)
        print(f"  Deleted {deleted} old public-bucket files")

        # 3. Upload data files in parallel
        uploaded_size = 0
        failed = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(upload_file_to_public, dbfs_path, rel_key, size): rel_key
                for dbfs_path, rel_key, size in files
            }

            done_count = 0
            for future in as_completed(futures):
                rel_key = futures[future]
                try:
                    _, size = future.result()
                    uploaded_size += size
                    done_count += 1
                    if done_count % 50 == 0 or done_count == len(files):
                        print(f"  Progress: {done_count}/{len(files)} files ({uploaded_size / (1024**3):.2f} GB)")
                except Exception as e:
                    failed.append((rel_key, str(e)))
                    print(f"  FAILED: {rel_key}: {e}")

        if failed:
            raise RuntimeError(
                f"Failed to upload {len(failed)} files for {fmt}/{entity}: "
                + "; ".join(f[0] for f in failed[:5])
            )

        # 4. Verify file count in public bucket for this entity prefix
        client = _get_public_client()
        paginator = client.get_paginator("list_objects_v2")
        public_count = 0
        public_total_size = 0
        for page in paginator.paginate(Bucket=PUBLIC_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                public_count += 1
                public_total_size += obj["Size"]

        if public_count != len(files):
            raise RuntimeError(
                f"File count mismatch for {fmt}/{entity}: "
                f"staged {len(files)}, public bucket has {public_count}"
            )

        if public_total_size != staging_total_size:
            raise RuntimeError(
                f"Total size mismatch for {fmt}/{entity}: "
                f"staged {staging_total_size}, public bucket has {public_total_size}"
            )

        # 5. Rewrite + upload the per-entity manifest (URLs repointed to public).
        #    Uploaded after the count/size verification above so it doesn't perturb
        #    the data-file checks.
        entity_manifest_size = upload_entity_manifest(fmt, entity)
        print(f"  Entity manifest uploaded ({entity_manifest_size / 1024:.1f} KB)")

        elapsed = time.time() - entity_start
        print(f"  Done: {len(files)} files, {uploaded_size / (1024**3):.2f} GB, {elapsed:.0f}s")

        entity_results.append({
            "format": fmt,
            "entity": entity,
            "status": "ok",
            "files": len(files),
            "size_gb": round(uploaded_size / (1024**3), 2),
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
