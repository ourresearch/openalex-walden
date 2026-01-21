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
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Cloudflare API configuration
CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")

# Sync configuration
BATCH_SIZE = 10000  # Rows per D1 API call (D1 limit is 10MB per request)
SYNC_LOOKBACK_DAYS = 2  # How far back to look for updates (overlap for safety)

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

# Get the last sync time from D1 stats table
try:
    result = d1_execute("SELECT stat_value FROM content_stats WHERE stat_key = 'last_sync_timestamp'")
    if result.get("result") and result["result"][0].get("results"):
        last_sync_str = result["result"][0]["results"][0]["stat_value"]
        last_sync = datetime.fromisoformat(last_sync_str.replace("Z", "+00:00"))
        print(f"Last sync: {last_sync}")
    else:
        # First sync or no timestamp - use lookback
        last_sync = datetime.utcnow() - timedelta(days=SYNC_LOOKBACK_DAYS)
        print(f"No previous sync found, using lookback: {last_sync}")
except Exception as e:
    print(f"Error getting last sync time: {e}")
    last_sync = datetime.utcnow() - timedelta(days=SYNC_LOOKBACK_DAYS)
    print(f"Using lookback: {last_sync}")

# Add safety buffer
sync_from = last_sync - timedelta(hours=2)
print(f"Syncing changes since: {sync_from}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Updated Works with Content

# COMMAND ----------

# Query for works with content that were updated since last sync
# Deduplicate by work_id, keeping one PDF/Grobid per work
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
    AND openalex_updated_dt >= '{sync_from.strftime("%Y-%m-%d %H:%M:%S")}'
)
SELECT work_id, pdf_uuid, grobid_uuid
FROM ranked
WHERE rn = 1
ORDER BY work_id
"""

print(f"Executing query for works updated since {sync_from}...")
df = spark.sql(query)
total_count = df.count()
print(f"Found {total_count:,} works with content updates to sync")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync to D1 in Batches

# COMMAND ----------

if total_count == 0:
    print("No updates to sync")
    dbutils.notebook.exit("No updates to sync")

# Collect and process in batches
rows = df.collect()
total_synced = 0
batch_num = 0

for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i + BATCH_SIZE]
    batch_data = [
        {
            "work_id": row.work_id,
            "pdf_uuid": row.pdf_uuid,
            "grobid_uuid": row.grobid_uuid
        }
        for row in batch
    ]

    try:
        result = d1_batch_insert(batch_data)
        if result["success"]:
            total_synced += result["inserted"]
            batch_num += 1
            if batch_num % 10 == 0:
                print(f"Progress: {total_synced:,} / {total_count:,} ({100*total_synced/total_count:.1f}%)")
        else:
            print(f"Batch {batch_num} failed: {result}")
    except Exception as e:
        print(f"Error in batch {batch_num}: {e}")
        # Continue with next batch rather than failing entirely
        continue

print(f"\nSync complete: {total_synced:,} rows synced to D1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Stats and Last Sync Timestamp

# COMMAND ----------

# Update content_stats table with new counts
update_content_stats()

# Update last sync timestamp
sync_timestamp = datetime.utcnow().isoformat() + "Z"
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
