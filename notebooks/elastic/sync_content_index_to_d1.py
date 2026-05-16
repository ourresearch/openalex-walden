# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Content Index to Cloudflare D1
# MAGIC
# MAGIC Syncs work_id → pdf_uuid/grobid_uuid mappings from `locations_mapped` to the
# MAGIC `openalex-content-index` D1 database that powers `api.openalex.org/content/*`.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes (via separate job or as task in end2end)
# MAGIC **Source**: `openalex.works.locations_mapped`
# MAGIC **Target**: Cloudflare D1 `openalex-content-index` database
# MAGIC
# MAGIC The sync is incremental: only works updated since the last sync are processed.

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %restart_python

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

# Cloudflare API configuration
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")

# Sync configuration
BATCH_SIZE = 10000  # Rows per D1 API call (D1 limit is 10MB per request)
SYNC_LOOKBACK_DAYS = 2  # How far back to look for updates (overlap for safety)

# Widgets for backfill / catch-up mode. Leave blank for normal nightly run.
dbutils.widgets.text("sync_from_override", "", "sync_from override (UTC ISO, blank=read from D1)")
dbutils.widgets.text("sync_to_override", "", "sync_to override (UTC ISO, blank=now)")
dbutils.widgets.text("chunk_days", "30", "Days per chunk (large windows split into chunks)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (count only, no D1 writes)")

SYNC_FROM_OVERRIDE = dbutils.widgets.get("sync_from_override").strip()
SYNC_TO_OVERRIDE = dbutils.widgets.get("sync_to_override").strip()
CHUNK_DAYS = max(1, int(dbutils.widgets.get("chunk_days") or "30"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def d1_execute(sql: str, params: list = None) -> dict:
    """Execute SQL against Cloudflare D1 via REST API."""
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"sql": sql}
    if params:
        payload["params"] = params

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


def d1_batch_insert(rows: list) -> dict:
    """Insert/update rows in D1 using INSERT OR REPLACE."""
    if not rows:
        return {"success": True, "inserted": 0}

    # Build INSERT OR REPLACE statement with VALUES
    values_list = []
    for row in rows:
        work_id = row["work_id"]
        pdf_uuid = f"'{row['pdf_uuid']}'" if row.get("pdf_uuid") else "NULL"
        grobid_uuid = f"'{row['grobid_uuid']}'" if row.get("grobid_uuid") else "NULL"
        values_list.append(f"({work_id}, {pdf_uuid}, {grobid_uuid})")

    sql = f"""
        INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid)
        VALUES {','.join(values_list)}
    """

    result = d1_execute(sql)
    return {"success": result.get("success", False), "inserted": len(rows)}


def update_content_stats():
    """Update the content_stats table with current counts."""
    stats_sql = """
        INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at)
        SELECT 'count_all', COUNT(*), datetime('now') FROM content_index;
    """
    d1_execute(stats_sql)

    pdf_sql = """
        INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at)
        SELECT 'count_pdf', COUNT(*), datetime('now') FROM content_index WHERE pdf_uuid IS NOT NULL;
    """
    d1_execute(pdf_sql)

    grobid_sql = """
        INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at)
        SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index WHERE grobid_uuid IS NOT NULL;
    """
    d1_execute(grobid_sql)

    print("Updated content_stats table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Last Sync Timestamp

# COMMAND ----------

if SYNC_FROM_OVERRIDE:
    last_sync = _parse_iso(SYNC_FROM_OVERRIDE)
    print(f"sync_from override: {last_sync}")
else:
    try:
        result = d1_execute("SELECT stat_value FROM content_stats WHERE stat_key = 'last_sync_timestamp'")
        if result.get("result") and result["result"][0].get("results"):
            last_sync_str = result["result"][0]["results"][0]["stat_value"]
            last_sync = _parse_iso(last_sync_str)
            print(f"Last sync: {last_sync}")
        else:
            last_sync = datetime.utcnow() - timedelta(days=SYNC_LOOKBACK_DAYS)
            print(f"No previous sync found, using lookback: {last_sync}")
    except Exception as e:
        print(f"Error getting last sync time: {e}")
        last_sync = datetime.utcnow() - timedelta(days=SYNC_LOOKBACK_DAYS)
        print(f"Using lookback: {last_sync}")

# Add safety buffer
sync_from = last_sync - timedelta(hours=2)
sync_to = _parse_iso(SYNC_TO_OVERRIDE) if SYNC_TO_OVERRIDE else datetime.utcnow()
print(f"Syncing changes: {sync_from} → {sync_to}")
print(f"Chunk size: {CHUNK_DAYS} days; dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync in Chunks (streamed, with failed-batch logging)

# COMMAND ----------

def sync_chunk(chunk_from: datetime, chunk_to: datetime):
    """Query locations_mapped for one [chunk_from, chunk_to) window, stream rows to D1
    in BATCH_SIZE-row upserts. Returns (count, synced, failed_batches)."""
    query = f"""
    WITH ranked AS (
      SELECT
        work_id,
        REPLACE(pdf_s3_id, '.pdf', '') as pdf_uuid,
        REPLACE(grobid_s3_id, '.xml.gz', '') as grobid_uuid,
        ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY pdf_s3_id) as rn
      FROM openalex.works.locations_mapped
      WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
        AND work_id IS NOT NULL
        AND openalex_updated_dt >= '{chunk_from.strftime("%Y-%m-%d %H:%M:%S")}'
        AND openalex_updated_dt <  '{chunk_to.strftime("%Y-%m-%d %H:%M:%S")}'
    )
    SELECT work_id, pdf_uuid, grobid_uuid
    FROM ranked
    WHERE rn = 1
    """
    df = spark.sql(query)
    count = df.count()
    print(f"  rows in window: {count:,}")
    if count == 0 or DRY_RUN:
        return count, 0, []

    failed = []
    synced = 0
    buf = []

    def flush():
        nonlocal synced
        if not buf:
            return
        try:
            d1_batch_insert(buf)
            synced += len(buf)
        except Exception as e:
            failed.append({
                "chunk_from": chunk_from.isoformat(),
                "chunk_to": chunk_to.isoformat(),
                "first_work_id": buf[0]["work_id"],
                "last_work_id": buf[-1]["work_id"],
                "size": len(buf),
                "error": str(e),
            })
        buf.clear()

    for row in df.toLocalIterator(prefetchPartitions=True):
        buf.append({
            "work_id": row.work_id,
            "pdf_uuid": row.pdf_uuid,
            "grobid_uuid": row.grobid_uuid,
        })
        if len(buf) >= BATCH_SIZE:
            flush()
            if synced and (synced // BATCH_SIZE) % 10 == 0:
                print(f"  progress: {synced:,} / {count:,} ({100*synced/count:.1f}%)")
    flush()
    return count, synced, failed


# Build chunk windows
chunks = []
t = sync_from
while t < sync_to:
    nxt = min(t + timedelta(days=CHUNK_DAYS), sync_to)
    chunks.append((t, nxt))
    t = nxt

print(f"Processing {len(chunks)} chunk(s) of up to {CHUNK_DAYS} day(s)")

grand_count = 0
grand_synced = 0
all_failed = []
for i, (a, b) in enumerate(chunks, 1):
    print(f"\n[{i}/{len(chunks)}] {a} → {b}")
    c, s, f = sync_chunk(a, b)
    grand_count += c
    grand_synced += s
    all_failed.extend(f)

print(f"\nSync complete: {grand_synced:,} / {grand_count:,} rows synced to D1 (dry_run={DRY_RUN})")
if all_failed:
    print(f"\nWARNING: {len(all_failed)} failed batch(es). First 20:")
    for f in all_failed[:20]:
        print(f"  {f}")
    # Surface to job status so failures aren't silent.
    raise RuntimeError(f"{len(all_failed)} D1 batches failed; see logs above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Stats and Last Sync Timestamp

# COMMAND ----------

if DRY_RUN:
    print("Dry run — skipping content_stats and last_sync_timestamp writes")
else:
    update_content_stats()

    # Advance last_sync_timestamp to the end of the window we just covered,
    # not "now" — so the next run picks up cleanly from sync_to.
    sync_timestamp = sync_to.isoformat() + "Z"
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) VALUES (?, ?, datetime('now'))",
        ["last_sync_timestamp", sync_timestamp]
    )
    print(f"Updated last_sync_timestamp to {sync_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sync

# COMMAND ----------

# Check D1 counts
result = d1_execute("SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
if result.get("result") and result["result"][0].get("results"):
    print("\nD1 Content Stats:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")

# Compare with Databricks source
source_count = spark.sql("""
    SELECT COUNT(DISTINCT work_id) as count
    FROM openalex.works.locations_mapped
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL
""").collect()[0]["count"]

print(f"\nDatabricks source: {source_count:,} unique works with content")
