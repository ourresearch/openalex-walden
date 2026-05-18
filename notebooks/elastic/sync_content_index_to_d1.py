# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Content Index to Cloudflare D1
# MAGIC
# MAGIC Syncs work_id → pdf_uuid/grobid_uuid mappings from `locations_mapped` to the
# MAGIC `openalex-content-index` D1 database that powers `api.openalex.org/content/*`.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes.
# MAGIC **Source**: `openalex.works.locations_mapped` (via `spark.sql()`)
# MAGIC **Target**: Cloudflare D1 `openalex-content-index` database
# MAGIC
# MAGIC The sync is incremental: filters `locations_mapped` on `openalex_updated_dt`
# MAGIC so that in-place edits (e.g. a PDF arriving on an older work) are picked up,
# MAGIC not just newly-created rows. A separate antijoin reconciliation pass exists
# MAGIC as a backstop.

# COMMAND ----------

import time
from datetime import datetime, timedelta, timezone

import requests

# Cloudflare API configuration
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")

# Sync configuration
# 500 rows ≈ 35 KB of inline INSERT-OR-REPLACE SQL — well under D1's
# per-statement length cap. The Track 0 local scripts validated up to 800;
# this leaves extra headroom for occasional longer UUIDs / edge rows.
BATCH_SIZE = 500
SYNC_LOOKBACK_DAYS = 2  # Fallback lookback when D1 has no last_sync_timestamp

# Widgets — leave blank for the standard nightly behavior.
dbutils.widgets.text("sync_from_override", "", "sync_from override (UTC ISO, blank=read from D1)")
dbutils.widgets.text("sync_to_override", "", "sync_to override (UTC ISO, blank=now)")

SYNC_FROM_OVERRIDE = dbutils.widgets.get("sync_from_override").strip()
SYNC_TO_OVERRIDE = dbutils.widgets.get("sync_to_override").strip()


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 Helpers

# COMMAND ----------

def d1_execute(sql_text: str, params: list = None) -> dict:
    """Execute SQL against Cloudflare D1 via REST API."""
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"sql": sql_text}
    if params:
        payload["params"] = params
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


def d1_batch_insert(rows: list) -> int:
    """Insert/update rows in D1 using INSERT OR REPLACE. Returns rows written."""
    if not rows:
        return 0
    values_list = []
    for row in rows:
        work_id = row["work_id"]
        pdf_uuid = f"'{row['pdf_uuid']}'" if row.get("pdf_uuid") else "NULL"
        grobid_uuid = f"'{row['grobid_uuid']}'" if row.get("grobid_uuid") else "NULL"
        values_list.append(f"({work_id}, {pdf_uuid}, {grobid_uuid})")
    sql_text = (
        "INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES "
        + ",".join(values_list)
    )
    d1_execute(sql_text)
    return len(rows)


def update_content_stats():
    """Update the content_stats counts in D1."""
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_all', COUNT(*), datetime('now') FROM content_index;"
    )
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_pdf', COUNT(*), datetime('now') FROM content_index WHERE pdf_uuid IS NOT NULL;"
    )
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index WHERE grobid_uuid IS NOT NULL;"
    )
    print("Updated content_stats table")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine sync window

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

sync_from = last_sync - timedelta(hours=2)
sync_to = _parse_iso(SYNC_TO_OVERRIDE) if SYNC_TO_OVERRIDE else datetime.utcnow()
print(f"Syncing changes: {sync_from} → {sync_to}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query + stream + upsert

# COMMAND ----------

# Filter uses `openalex_updated_dt` so late-arriving PDFs land in D1 even when
# the underlying work row was created long ago. The content-presence predicate
# (`pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL`) and a one-day window
# keep the daily scan small (~110K distinct work_ids).
df = spark.sql(f"""
    SELECT
      work_id,
      MIN(REPLACE(pdf_s3_id, '.pdf', ''))      AS pdf_uuid,
      MIN(REPLACE(grobid_s3_id, '.xml.gz', '')) AS grobid_uuid
    FROM openalex.works.locations_mapped
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL
      AND openalex_updated_dt >= '{sync_from.strftime("%Y-%m-%d")}'
      AND openalex_updated_dt <  '{sync_to.strftime("%Y-%m-%d")}'
    GROUP BY work_id
""")

t0 = time.time()
total_count = 0
total_synced = 0
failed_batches: list = []


def _flush(batch: list) -> None:
    global total_synced
    if not batch:
        return
    try:
        d1_batch_insert(batch)
        total_synced += len(batch)
    except Exception as e:
        failed_batches.append({
            "first_work_id": batch[0]["work_id"],
            "last_work_id": batch[-1]["work_id"],
            "size": len(batch),
            "error": str(e),
        })


rows = df.collect()
print(f"\nCollected {len(rows):,} rows; upserting to D1…")
batch: list = []
for row in rows:
    batch.append({"work_id": row.work_id, "pdf_uuid": row.pdf_uuid, "grobid_uuid": row.grobid_uuid})
    if len(batch) >= BATCH_SIZE:
        total_count += len(batch)
        _flush(batch)
        batch = []
        if total_count % 10_000 == 0:
            elapsed = time.time() - t0
            rate = total_synced / elapsed if elapsed else 0
            print(f"  synced {total_synced:,}  ({rate:.0f} rows/s, {elapsed:.0f}s elapsed)")
if batch:
    total_count += len(batch)
    _flush(batch)

elapsed = time.time() - t0
rate = total_synced / elapsed if elapsed else 0
print(f"\nSync complete: {total_synced:,} / {total_count:,} rows synced to D1 "
      f"({elapsed:.0f}s, {rate:.0f} rows/s)")

if failed_batches:
    print(f"\nWARNING: {len(failed_batches)} failed batch(es). First 20:")
    for f in failed_batches[:20]:
        print(f"  {f}")
    raise RuntimeError(f"{len(failed_batches)} D1 batches failed; see logs above")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Stats and Last Sync Timestamp

# COMMAND ----------

update_content_stats()
sync_timestamp = sync_to.isoformat() + "Z"
d1_execute(
    "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) VALUES (?, ?, datetime('now'))",
    ["last_sync_timestamp", sync_timestamp],
)
print(f"Updated last_sync_timestamp to {sync_timestamp}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sync

# COMMAND ----------

result = d1_execute("SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
if result.get("result") and result["result"][0].get("results"):
    print("\nD1 Content Stats:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")
