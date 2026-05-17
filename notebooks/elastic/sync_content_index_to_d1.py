# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Content Index to Cloudflare D1
# MAGIC
# MAGIC Syncs work_id → pdf_uuid/grobid_uuid mappings from `locations_mapped` to the
# MAGIC `openalex-content-index` D1 database that powers `api.openalex.org/content/*`.
# MAGIC
# MAGIC **Runs**: Daily after end2end completes.
# MAGIC **Source**: `openalex.works.locations_mapped` (queried via SQL warehouse)
# MAGIC **Target**: Cloudflare D1 `openalex-content-index` database
# MAGIC
# MAGIC The sync is incremental: only works whose locations_mapped row was created
# MAGIC since the last sync (`openalex_created_dt`) are processed. A separate
# MAGIC reconciliation pass (antijoin against the D1 snapshot) covers in-place
# MAGIC updates that this filter misses.
# MAGIC
# MAGIC The query runs against the SQL warehouse (not Spark) because Spark on a
# MAGIC small cluster is 10-20x slower than the warehouse for this date-filtered
# MAGIC scan, and the warehouse is serverless so spot termination isn't a risk.

# COMMAND ----------

# MAGIC %pip install requests databricks-sql-connector
# MAGIC %restart_python

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta, timezone

import requests
from databricks import sql

# Cloudflare API configuration
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")

# SQL warehouse: xlarge for backfill volumes, medium would work for nightly
WAREHOUSE_HTTP_PATH_XLARGE = "/sql/1.0/warehouses/3996dc0a9b183ce3"

# Sync configuration
BATCH_SIZE = 10000  # Rows per D1 API call (D1 limit is 10MB per request)
SYNC_LOOKBACK_DAYS = 2  # Fallback lookback when D1 has no last_sync_timestamp

# Widgets — leave blank for the standard nightly behavior.
dbutils.widgets.text("sync_from_override", "", "sync_from override (UTC ISO, blank=read from D1)")
dbutils.widgets.text("sync_to_override", "", "sync_to override (UTC ISO, blank=now)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (count only, no D1 writes)")

SYNC_FROM_OVERRIDE = dbutils.widgets.get("sync_from_override").strip()
SYNC_TO_OVERRIDE = dbutils.widgets.get("sync_to_override").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"


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
print(f"Syncing changes: {sync_from} → {sync_to}  (dry_run={DRY_RUN})")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to SQL warehouse

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_HOST = ctx.tags().apply("browserHostName")
WORKSPACE_TOKEN = ctx.apiToken().get()

print(f"Workspace: {WORKSPACE_HOST}")
print(f"Warehouse: {WAREHOUSE_HTTP_PATH_XLARGE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query + stream + upsert

# COMMAND ----------

# Single date-bounded query, no Spark, no chunking. The warehouse parallelizes
# the scan; we just stream rows back via cursor.fetchmany and batch into D1.
#
# Filter uses `openalex_created_dt` (row-level row-creation date) rather than
# `openalex_updated_dt` — the latter ticks on any field change (~5M rows/day
# churn) and over a multi-month window effectively scans the whole table.
# `created_dt` is sparse and catches exactly: new works, and old works that
# got a new pdf/grobid location row.
query = f"""
SELECT
  work_id,
  MIN(REPLACE(pdf_s3_id, '.pdf', ''))      AS pdf_uuid,
  MIN(REPLACE(grobid_s3_id, '.xml.gz', '')) AS grobid_uuid
FROM openalex.works.locations_mapped
WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
  AND work_id IS NOT NULL
  AND openalex_created_dt >= '{sync_from.strftime("%Y-%m-%d")}'
  AND openalex_created_dt <  '{sync_to.strftime("%Y-%m-%d")}'
GROUP BY work_id
"""

t0 = time.time()
total_count = 0
total_synced = 0
failed_batches: list = []

with sql.connect(
    server_hostname=WORKSPACE_HOST,
    http_path=WAREHOUSE_HTTP_PATH_XLARGE,
    access_token=WORKSPACE_TOKEN,
) as connection:
    with connection.cursor() as cursor:
        if DRY_RUN:
            # Cheap count query; warehouse handles it in seconds.
            count_sql = f"SELECT COUNT(*) AS n FROM ({query})"
            cursor.execute(count_sql)
            total_count = cursor.fetchone()[0]
            print(f"\nDry run — rows in window: {total_count:,}")
            print(f"Elapsed: {time.time() - t0:.1f}s")
        else:
            print(f"\nExecuting query against warehouse…")
            cursor.execute(query)
            print(f"  query started ({time.time() - t0:.1f}s)")

            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                total_count += len(rows)
                batch = [
                    {"work_id": r[0], "pdf_uuid": r[1], "grobid_uuid": r[2]}
                    for r in rows
                ]
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

                if total_count % (10 * BATCH_SIZE) == 0:
                    elapsed = time.time() - t0
                    rate = total_synced / elapsed if elapsed else 0
                    print(f"  synced {total_synced:,}  ({rate:.0f} rows/s, {elapsed:.0f}s elapsed)")

            elapsed = time.time() - t0
            print(f"\nSync complete: {total_synced:,} / {total_count:,} rows synced to D1 "
                  f"({elapsed:.0f}s, {total_synced/elapsed:.0f} rows/s)")

if failed_batches:
    print(f"\nWARNING: {len(failed_batches)} failed batch(es). First 20:")
    for f in failed_batches[:20]:
        print(f"  {f}")
    raise RuntimeError(f"{len(failed_batches)} D1 batches failed; see logs above")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Stats and Last Sync Timestamp

# COMMAND ----------

if DRY_RUN:
    print("Dry run — skipping content_stats and last_sync_timestamp writes")
else:
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
