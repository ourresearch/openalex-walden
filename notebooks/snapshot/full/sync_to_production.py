# Databricks notebook source
# Sync full snapshot from staging (s3://openalex-snapshots/full/{date}/)
# to production (s3://openalex/data/) using boto3 for the production bucket.
#
# Staging bucket is accessible natively via Databricks (instance profile).
# Production bucket requires separate AWS credentials from the
# "openalex-open-data" secret scope.

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGING_BUCKET = "openalex-snapshots"
PROD_BUCKET = "openalex"
PROD_PREFIX = "data"

date_str = datetime.now().strftime("%Y-%m-%d")
staging_base = f"s3://{STAGING_BUCKET}/full/{date_str}"
local_scratch = "/local_disk0/s3_transfer"

MAX_WORKERS = 10
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
print(f"Production:    s3://{PROD_BUCKET}/{PROD_PREFIX}/")
print(f"Entities:      {len(ENTITIES)}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Preflight: verify staging snapshot exists and prod credentials work
# ---------------------------------------------------------------------------

import boto3

# Verify staging is accessible via dbutils
try:
    staging_entries = dbutils.fs.ls(staging_base)
    staging_dirs = [e.name.rstrip("/") for e in staging_entries if not e.name.startswith("_")]
    print(f"Staging preflight OK: {len(staging_dirs)} entities found at {staging_base}")
except Exception as e:
    raise RuntimeError(f"Staging preflight FAILED — cannot access {staging_base}: {e}")

# Verify production credentials
prod_access_key = dbutils.secrets.get("openalex-open-data", "aws_access_key_id")
prod_secret_key = dbutils.secrets.get("openalex-open-data", "aws_secret_access_key")

prod_client = boto3.client(
    "s3",
    aws_access_key_id=prod_access_key,
    aws_secret_access_key=prod_secret_key,
)
prod_client.head_bucket(Bucket=PROD_BUCKET)
print(f"Production preflight OK: s3://{PROD_BUCKET} accessible")
del prod_client

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _get_prod_client():
    """Return a thread-local boto3 S3 client for the production bucket."""
    if not hasattr(_thread_local, "s3_client"):
        _thread_local.s3_client = boto3.client(
            "s3",
            aws_access_key_id=prod_access_key,
            aws_secret_access_key=prod_secret_key,
        )
    return _thread_local.s3_client


def delete_prod_prefix(entity):
    """Delete all objects under s3://{PROD_BUCKET}/{PROD_PREFIX}/{entity}/."""
    client = _get_prod_client()
    prefix = f"{PROD_PREFIX}/{entity}/"
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0

    for page in paginator.paginate(Bucket=PROD_BUCKET, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_keys = [{"Key": obj["Key"]} for obj in objects]
        client.delete_objects(
            Bucket=PROD_BUCKET,
            Delete={"Objects": delete_keys, "Quiet": True},
        )
        deleted += len(delete_keys)

    return deleted


def list_staging_files(entity):
    """List all files for an entity in staging, returning (dbfs_path, relative_key) tuples.

    Skips the manifest file — it's copied last as a completion marker.
    """
    entity_path = f"{staging_base}/{entity}"
    files = []

    def _walk(path):
        try:
            entries = dbutils.fs.ls(path)
        except Exception:
            return
        for entry in entries:
            if entry.isDir():
                _walk(entry.path)
            elif entry.name == "manifest":
                pass  # skip manifest — copied last
            else:
                # Build the relative key: {entity}/updated_date=.../part_XXXX.gz
                raw = entry.path.replace("dbfs:", "").replace("s3:", "")
                marker = f"/{entity}/"
                idx = raw.find(marker)
                relative = raw[idx + 1:]  # e.g. "entity/updated_date=.../part_0000.gz"
                files.append((entry.path, relative, entry.size))

    _walk(entity_path)
    return files


def upload_file_to_prod(dbfs_path, relative_key, expected_size):
    """Download a file from staging to local disk, then upload to production.

    Returns (relative_key, prod_size) on success.
    """
    prod_key = f"{PROD_PREFIX}/{relative_key}"
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

            # Upload to production
            client = _get_prod_client()
            client.upload_file(local_path, PROD_BUCKET, prod_key)

            # Verify production size
            head = client.head_object(Bucket=PROD_BUCKET, Key=prod_key)
            prod_size = head["ContentLength"]
            if prod_size != expected_size:
                raise RuntimeError(
                    f"Prod size mismatch: expected {expected_size}, got {prod_size}"
                )

            return relative_key, prod_size

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


def upload_manifest(entity):
    """Copy the manifest file for an entity to production (last, as completion marker)."""
    manifest_dbfs = f"{staging_base}/{entity}/manifest"
    local_path = os.path.join(local_scratch, entity, "manifest")
    prod_key = f"{PROD_PREFIX}/{entity}/manifest"

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    dbutils.fs.cp(manifest_dbfs, f"file:{local_path}")
    local_size = os.path.getsize(local_path)

    client = _get_prod_client()
    client.upload_file(local_path, PROD_BUCKET, prod_key)

    head = client.head_object(Bucket=PROD_BUCKET, Key=prod_key)
    if head["ContentLength"] != local_size:
        raise RuntimeError(
            f"Manifest size mismatch for {entity}: "
            f"expected {local_size}, got {head['ContentLength']}"
        )

    try:
        os.remove(local_path)
    except OSError:
        pass

    return local_size

# COMMAND ----------

# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------

os.makedirs(local_scratch, exist_ok=True)

overall_start = time.time()
entity_results = []

for entity in ENTITIES:
    print(f"\n{'=' * 60}")
    print(f"Syncing: {entity}")
    print(f"{'=' * 60}")
    entity_start = time.time()

    # 1. List staging files
    files = list_staging_files(entity)
    staging_total_size = sum(f[2] for f in files)
    print(f"  Staging: {len(files)} files, {staging_total_size / (1024**3):.2f} GB")

    if not files:
        print(f"  WARNING: No files found for {entity} — skipping")
        entity_results.append({
            "entity": entity,
            "status": "skipped",
            "files": 0,
            "size_gb": 0,
        })
        continue

    # 2. Delete old production files
    deleted = delete_prod_prefix(entity)
    print(f"  Deleted {deleted} old production files")

    # 3. Upload data files in parallel
    uploaded_size = 0
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(upload_file_to_prod, dbfs_path, rel_key, size): rel_key
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
            f"Failed to upload {len(failed)} files for {entity}: "
            + "; ".join(f[0] for f in failed[:5])
        )

    # 4. Verify file count in production
    client = _get_prod_client()
    prod_prefix = f"{PROD_PREFIX}/{entity}/"
    paginator = client.get_paginator("list_objects_v2")
    prod_count = 0
    prod_total_size = 0
    for page in paginator.paginate(Bucket=PROD_BUCKET, Prefix=prod_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/manifest"):
                prod_count += 1
                prod_total_size += obj["Size"]

    if prod_count != len(files):
        raise RuntimeError(
            f"File count mismatch for {entity}: "
            f"staged {len(files)}, production has {prod_count}"
        )

    if prod_total_size != staging_total_size:
        raise RuntimeError(
            f"Total size mismatch for {entity}: "
            f"staged {staging_total_size}, production has {prod_total_size}"
        )

    # 5. Copy manifest last (completion marker)
    manifest_size = upload_manifest(entity)

    elapsed = time.time() - entity_start
    print(f"  Done: {len(files)} files, {uploaded_size / (1024**3):.2f} GB, "
          f"manifest {manifest_size / 1024:.1f} KB, {elapsed:.0f}s")

    entity_results.append({
        "entity": entity,
        "status": "ok",
        "files": len(files),
        "size_gb": round(uploaded_size / (1024**3), 2),
        "elapsed_s": round(elapsed),
    })

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

print(f"Entities synced:  {len(ok)}/{len(ENTITIES)}")
print(f"Total files:      {total_files:,}")
print(f"Total size:       {total_gb:.2f} GB")

if skipped:
    print(f"\nSkipped entities: {[r['entity'] for r in skipped]}")

print(f"\nPer-entity breakdown:")
for r in entity_results:
    if r["status"] == "ok":
        print(f"  {r['entity']:20s}  {r['files']:6,} files  {r['size_gb']:8.2f} GB  {r['elapsed_s']:5d}s")
    else:
        print(f"  {r['entity']:20s}  SKIPPED")

print(f"\nProduction snapshot ready at: s3://{PROD_BUCKET}/{PROD_PREFIX}/")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Upload RELEASE_NOTES.txt to production bucket root
# ---------------------------------------------------------------------------

import requests as _requests

RELEASE_NOTES_URL = (
    "https://raw.githubusercontent.com/ourresearch/openalex-walden"
    "/main/notebooks/snapshot/full/RELEASE_NOTES.txt"
)

print("Uploading RELEASE_NOTES.txt to s3://openalex/ ...")

resp = _requests.get(RELEASE_NOTES_URL, timeout=30)
resp.raise_for_status()

local_rn = os.path.join(local_scratch, "RELEASE_NOTES.txt")
with open(local_rn, "w") as f:
    f.write(resp.text)

client = _get_prod_client()
client.upload_file(local_rn, PROD_BUCKET, "RELEASE_NOTES.txt")

head = client.head_object(Bucket=PROD_BUCKET, Key="RELEASE_NOTES.txt")
print(f"  Uploaded RELEASE_NOTES.txt ({head['ContentLength']} bytes)")

try:
    os.remove(local_rn)
except OSError:
    pass
