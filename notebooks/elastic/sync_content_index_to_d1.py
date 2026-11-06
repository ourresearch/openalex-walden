# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Content Index to Cloudflare D1
# MAGIC
# MAGIC Syncs work_id → pdf_uuid/grobid_uuid mappings from `locations_mapped` to the
# MAGIC `openalex-content-index` D1 database that powers `api.openalex.org/content/*`.
# MAGIC
# MAGIC **Runs** (oxjob #2041): job *Sync Content Index to D1* fires when the End 2 End rewrites
# MAGIC `locations_mapped` (Databricks table-update trigger, ~06:00-08:30 UTC), so a night's new PDFs are
# MAGIC downloadable before the night publishes; job *Sync Content Index to D1 (backstop)* runs at 17:00 UTC and
# MAGIC normally finds nothing. Until 5 Nov 2026 it ran at 03:00 UTC, before the End 2 End, so every night's new
# MAGIC content 404'd at content.openalex.org for 10-38 h after the publish.
# MAGIC **Source**: `openalex.works.locations_mapped` (via `spark.sql()`)
# MAGIC **Target**: Cloudflare D1 `openalex-content-index` database
# MAGIC
# MAGIC **What it writes** (#2041, 6 Nov 2026): only works whose (pdf, grobid) pair differs between the table
# MAGIC version D1 last synced (`content_stats.last_synced_lm_version`) and the current one. The End 2 End re-stamps
# MAGIC far more rows than it changes: night 2026-11-06 stamped 29.4M works with content and changed 104K, and
# MAGIC writing every stamped work took 5.8 h, done 11 min before the publish. An extra run on the same version
# MAGIC writes nothing.
# MAGIC Fallback (manual sync_from/sync_to overrides, no synced version in D1, or that version older than the
# MAGIC table's 7-day VACUUM retention): every row stamped after the newest stamp already synced
# MAGIC (`content_stats.last_synced_updated_dt`; each End 2 End stamps one timestamp a night).
# MAGIC Writes are paced: D1 serves content.openalex.org's lookups from the same database, and
# MAGIC 8 parallel writers made ~8% of downloads fail with "D1 DB is overloaded" during each run (#2041).
# MAGIC Works that lose all content stay in D1 (nothing is deleted). A separate antijoin reconciliation pass
# MAGIC exists as a backstop.

# COMMAND ----------

import random
import time
from concurrent.futures import ThreadPoolExecutor
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
D1_MAX_RETRIES = 8
D1_RETRY_STATUSES = {429, 500, 502, 503, 504}
SYNC_LOOKBACK_DAYS = 2  # Fallback lookback when D1 has no last_sync_timestamp
HWM_KEY = "last_synced_updated_dt"  # newest openalex_updated_dt already synced (UTC, microseconds); #2041
VERSION_KEY = "last_synced_lm_version"  # locations_mapped version D1 fully reflects; diff base (#2041)
DIFF_MAX_AGE_DAYS = 6  # locations_mapped VACUUM keeps 7 days of files; an older version may be unreadable
LM_TABLE = "openalex.works.locations_mapped"

# Widgets — leave blank for the standard nightly behavior.
dbutils.widgets.text("sync_from_override", "", "sync_from override (UTC ISO, blank=read from D1)")
dbutils.widgets.text("sync_to_override", "", "sync_to override (UTC ISO, blank=now)")
dbutils.widgets.text("diff_against_version", "", "diff base version (blank=D1's last synced, none=stamp window)")
# Pacing (#2041): one writer that rests as long as each D1 write took leaves D1 headroom for downloads.
# A manual catch-up at a quiet hour can raise workers / lower the pause.
dbutils.widgets.text("write_workers", "1", "parallel D1 writers")
dbutils.widgets.text("write_pause_ratio", "1.0", "pause after each write, as a multiple of its duration")
# The three COUNT(*) scans of the whole table failed 40-100% of downloads for ~2 min (#2041); nothing user-facing
# reads content_stats counts, so the nightly run skips them.
dbutils.widgets.text("refresh_stats", "false", "refresh content_stats counts (full-table scans)")

SYNC_FROM_OVERRIDE = dbutils.widgets.get("sync_from_override").strip()
SYNC_TO_OVERRIDE = dbutils.widgets.get("sync_to_override").strip()
WINDOW_OVERRIDE = bool(SYNC_FROM_OVERRIDE or SYNC_TO_OVERRIDE)  # a manual window: no diff, version not recorded
DIFF_AGAINST = dbutils.widgets.get("diff_against_version").strip().lower()
WRITE_WORKERS = max(1, int(dbutils.widgets.get("write_workers").strip() or "1"))
WRITE_PAUSE_RATIO = max(0.0, float(dbutils.widgets.get("write_pause_ratio").strip() or "1.0"))
REFRESH_STATS = dbutils.widgets.get("refresh_stats").strip().lower() in ("true", "1", "yes")

# Timestamp literals and date_format below are UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")


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
    for attempt in range(D1_MAX_RETRIES + 1):
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        if response.status_code not in D1_RETRY_STATUSES or attempt == D1_MAX_RETRIES:
            response.raise_for_status()
            return response.json()
        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after and retry_after.isdigit() else min(60.0, 2.0 ** attempt)
        time.sleep(delay + random.uniform(0, 1))


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


def d1_stat(key: str):
    """One content_stats value, or None."""
    result = d1_execute("SELECT stat_value FROM content_stats WHERE stat_key = ?", [key])
    rows = (result.get("result") or [{}])[0].get("results") or []
    return rows[0]["stat_value"] if rows else None


def d1_set_stat(key: str, value: str) -> None:
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) VALUES (?, ?, datetime('now'))",
        [key, value],
    )


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

TS_FMT = "%Y-%m-%d %H:%M:%S.%f"  # matches date_format(..., 'yyyy-MM-dd HH:mm:ss.SSSSSS') below
run_started = datetime.utcnow()

# Pin one version of the table, so the rows read and the high-water mark stored agree.
from delta.tables import DeltaTable

lm_delta = DeltaTable.forName(spark, LM_TABLE)
_h = (lm_delta.history(1)
      .selectExpr("version", "date_format(timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS ts").first())
lm_version, lm_commit = _h["version"], _h["ts"]
print(f"{LM_TABLE} version {lm_version}, committed {lm_commit} UTC")

hwm = d1_stat(HWM_KEY)
if SYNC_FROM_OVERRIDE:
    lower, lower_op = _parse_iso(SYNC_FROM_OVERRIDE).strftime(TS_FMT), ">="
    print(f"sync_from override: {lower}")
else:
    if hwm:
        # Each End 2 End stamps one timestamp, taken before its commit; a later commit stamps a later time.
        lower, lower_op = hwm, ">"
        print(f"Newest stamp already synced: {hwm}")
    else:
        # First run with a high-water mark: the old wall-clock window, 2 h overlap.
        last = d1_stat("last_sync_timestamp")
        start = _parse_iso(last) if last else datetime.utcnow() - timedelta(days=SYNC_LOOKBACK_DAYS)
        lower, lower_op = (start - timedelta(hours=2)).strftime(TS_FMT), ">="
        print(f"No {HWM_KEY}; last_sync_timestamp {last} -> window from {lower}")
upper = _parse_iso(SYNC_TO_OVERRIDE).strftime(TS_FMT) if SYNC_TO_OVERRIDE else None

# Diff base: the version D1 last fully synced, if it is still readable (see the header).
base_version = None
if WINDOW_OVERRIDE or DIFF_AGAINST == "none":
    print("Diff off (sync_from/sync_to override or diff_against_version=none): stamp window")
else:
    cand = DIFF_AGAINST or d1_stat(VERSION_KEY)
    cand = int(cand) if cand else None
    base_h = lm_delta.history().where(f"version = {cand}").select("timestamp").first() if cand is not None else None
    if base_h is None or cand > lm_version:
        print(f"Diff base {cand} not in {LM_TABLE} history: stamp window")
    elif base_h["timestamp"] < datetime.utcnow() - timedelta(days=DIFF_MAX_AGE_DAYS):
        print(f"Diff base {cand} committed {base_h['timestamp']}, older than {DIFF_MAX_AGE_DAYS} days: stamp window")
    else:
        base_version = cand


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query + stream + upsert

# COMMAND ----------

# Stamp waves can push either set to 40M+ rows (2026-08-12 cutover), so the driver must never hold the full
# result set: stream with toLocalIterator() and flush as it goes.


def content_pairs(version: int, where: str = "") -> str:
    """One row per work with content at `version`: its (pdf, grobid) pair and newest stamp."""
    return f"""
    SELECT
      work_id,
      MIN(REPLACE(pdf_s3_id, '.pdf', ''))      AS pdf_uuid,
      MIN(REPLACE(grobid_s3_id, '.xml.gz', '')) AS grobid_uuid,
      date_format(MAX(openalex_updated_dt), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS max_dt
    FROM {LM_TABLE} VERSION AS OF {version}
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL {where}
    GROUP BY work_id"""


if base_version is not None:
    print(f"Syncing works whose content pair differs between version {base_version} and {lm_version}")
    try:
        # subtract = EXCEPT DISTINCT; NULLs compare equal. Cached and counted here, so an unreadable base version
        # falls back now.
        pairs = lambda v: spark.sql(content_pairs(v)).drop("max_dt")
        df = pairs(lm_version).subtract(pairs(base_version))
        if base_version == lm_version:
            df = df.limit(0)
        df = df.selectExpr("*", "CAST(NULL AS STRING) AS max_dt").cache()
        print(f"{df.count():,} works changed")
    except Exception as e:  # e.g. the base version's files were vacuumed
        print(f"Diff failed ({str(e)[:300]}): stamp window")
        base_version = None
if base_version is None:
    # In-place edits (a PDF arriving on an older work) carry a new stamp, so the window picks them up.
    print(f"Syncing rows with openalex_updated_dt {lower_op} {lower}" + (f" and < {upper}" if upper else ""))
    upper_sql = f"AND openalex_updated_dt < TIMESTAMP'{upper}'" if upper else ""
    df = spark.sql(content_pairs(lm_version, f"AND openalex_updated_dt {lower_op} TIMESTAMP'{lower}' {upper_sql}"))

t0 = time.time()
total_count = 0
total_synced = 0
failed_batches: list = []

FLUSH_WORKERS = WRITE_WORKERS
max_dt_seen = ""


def _flush(batch: list) -> int:
    if not batch:
        return 0
    try:
        t_write = time.time()
        d1_batch_insert(batch)
        if WRITE_PAUSE_RATIO:
            # Rest in proportion to how long D1 took (retries and backoff included): slower D1, slower writes.
            time.sleep((time.time() - t_write) * WRITE_PAUSE_RATIO)
        return len(batch)
    except Exception as e:
        failed_batches.append({
            "first_work_id": batch[0]["work_id"],
            "last_work_id": batch[-1]["work_id"],
            "size": len(batch),
            "error": str(e),
        })
        return 0


executor = ThreadPoolExecutor(max_workers=FLUSH_WORKERS)
in_flight: list = []


def _drain() -> None:
    global total_synced
    total_synced += sum(f.result() for f in in_flight)
    in_flight.clear()


def _submit(batch: list) -> None:
    global total_count
    total_count += len(batch)
    in_flight.append(executor.submit(_flush, batch))
    if len(in_flight) >= FLUSH_WORKERS * 4:
        _drain()
        if total_count % 160_000 == 0:
            elapsed = time.time() - t0
            rate = total_synced / elapsed if elapsed else 0
            print(f"  synced {total_synced:,}  ({rate:.0f} rows/s, {elapsed:.0f}s elapsed)")


batch: list = []
for row in df.toLocalIterator():
    if row.max_dt and row.max_dt > max_dt_seen:
        max_dt_seen = row.max_dt
    batch.append({"work_id": row.work_id, "pdf_uuid": row.pdf_uuid, "grobid_uuid": row.grobid_uuid})
    if len(batch) >= BATCH_SIZE:
        _submit(batch)
        batch = []
if batch:
    _submit(batch)
_drain()
executor.shutdown()
df.unpersist()  # no-op unless the diff was cached

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
# MAGIC ## Update Last Sync Timestamp, then Stats

# COMMAND ----------

# High-water mark: the newest stamp written, never past the version's commit time (a future-dated stamp would
# otherwise hide later nights), and never moved back. A run that found nothing keeps the mark; a first run that
# found nothing stores its window start, so the next run starts there. A run with sync_to_override leaves it.
# A diff run brought D1 up to the whole version, so every stamp up to its commit is covered.
if base_version is not None:
    max_dt_seen = lm_commit
if not SYNC_TO_OVERRIDE:
    seen = min(max_dt_seen, lm_commit) if max_dt_seen else None
    new_hwm = max(v for v in (hwm, seen) if v) if (hwm or seen) else lower
    if new_hwm != hwm:
        d1_set_stat(HWM_KEY, new_hwm)
    print(f"{HWM_KEY}: {hwm} -> {new_hwm}")
# The next run diffs against this version, so record it only when D1 now reflects all of it.
if not WINDOW_OVERRIDE:
    d1_set_stat(VERSION_KEY, str(lm_version))
    print(f"{VERSION_KEY}: {lm_version}")

sync_timestamp = run_started.isoformat() + "Z"
d1_set_stat("last_sync_timestamp", sync_timestamp)  # wall clock of this run; scripts/run_d1_backfill.py reads it
print(f"Updated last_sync_timestamp to {sync_timestamp}")

if REFRESH_STATS:
    update_content_stats()
else:
    print("Skipped content_stats counts (refresh_stats=false)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sync

# COMMAND ----------

result = d1_execute("SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
if result.get("result") and result["result"][0].get("results"):
    print("\nD1 Content Stats:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")
