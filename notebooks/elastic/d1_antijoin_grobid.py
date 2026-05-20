# Databricks notebook source
# MAGIC %md
# MAGIC # D1 Antijoin Reconciliation — grobid_uuid (oxjob #202 Track 1)
# MAGIC
# MAGIC Closes the column-level gap between `openalex.works.locations_mapped.grobid_s3_id`
# MAGIC and D1 `content_index.grobid_uuid`. The existing
# MAGIC `scripts/run_d1_antijoin.py` only finds work_ids ENTIRELY missing from D1;
# MAGIC this notebook finds work_ids that ARE in D1 (with pdf_uuid) but have
# MAGIC `grobid_uuid = NULL`, despite LM having a `grobid_s3_id` for them.
# MAGIC
# MAGIC The 2026-05-20 10K customer-view validation found this gap manifests as
# MAGIC ~9.35% of `has_content.grobid_xml:true` Content API requests returning 404
# MAGIC (BAD_404). Extrapolated to the corpus: ~4.4M failed customer requests.
# MAGIC
# MAGIC **Bad-cohort exclusion**: bad UUIDs (HTML wrappers, empty TEI) from the two
# MAGIC source tables (`grobid_processing_results`, `grobid_xml_backfill`) are
# MAGIC anti-joined out before the upsert. Without this, ~216K customers would
# MAGIC transition `404 → BAD_BYTE` (getting garbage instead of an honest 404).
# MAGIC Multi-XML works keep their good sibling: ANTI JOIN drops the bad uuid,
# MAGIC `MIN(grobid_uuid)` picks among the remaining good ones.
# MAGIC
# MAGIC **Source**: `openalex.works.locations_mapped` (via `spark.sql()`)
# MAGIC **Target**: Cloudflare D1 `openalex-content-index` (`content_index` table)
# MAGIC
# MAGIC Idempotent via `INSERT OR REPLACE`. Safe to re-run.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `dry_run` (default `false`): if `true`, sizes the gap but doesn't write to D1.
# MAGIC   - `pilot_limit` (default `0`): if >0, only upsert the first N rows (sanity check).

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")
CF_API_BASE = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/d1/database/{CF_D1_DATABASE_ID}/query"
)

BATCH_SIZE = 500   # matches sync_content_index_to_d1 (well under D1 stmt-length cap)
CONCURRENCY = 8
D1_PAGE_SIZE = 1_000_000

dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (no D1 writes)")
dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full corpus)")

DRY_RUN = dbutils.widgets.get("dry_run") == "true"
PILOT_LIMIT = int(dbutils.widgets.get("pilot_limit"))
print(f"dry_run={DRY_RUN}  pilot_limit={PILOT_LIMIT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 helpers

# COMMAND ----------

def d1_execute(sql_text: str, max_retries: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    payload = {"sql": sql_text}
    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.post(CF_API_BASE, headers=headers, json=payload, timeout=180)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            last_err = RuntimeError(f"D1 {r.status_code}: {r.text[:300]}")
            time.sleep(2 ** attempt)
            continue
        if not r.ok:
            raise RuntimeError(f"D1 {r.status_code}: {r.text[:500]}")
        return r.json()
    raise RuntimeError(f"D1 retries exhausted: {last_err}")


def d1_batch_upsert(rows):
    """rows: list of (work_id, pdf_uuid_to_preserve, grobid_uuid_to_set)."""
    if not rows:
        return 0
    values = []
    for work_id, pdf_uuid, grobid_uuid in rows:
        pdf_lit = f"'{pdf_uuid}'" if pdf_uuid is not None else "NULL"
        grobid_lit = f"'{grobid_uuid}'"
        values.append(f"({work_id}, {pdf_lit}, {grobid_lit})")
    sql_text = (
        "INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES "
        + ",".join(values)
    )
    d1_execute(sql_text)
    return len(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — D1 rows with NULL grobid_uuid (paginated)

# COMMAND ----------

print("Fetching D1 rows where grobid_uuid IS NULL…")
d1_null_grobid: dict = {}
last_id = 0
page = 0
t0 = time.time()
while True:
    sql = (
        f"SELECT work_id, pdf_uuid FROM content_index "
        f"WHERE grobid_uuid IS NULL AND work_id > {last_id} "
        f"ORDER BY work_id LIMIT {D1_PAGE_SIZE}"
    )
    result = d1_execute(sql)
    rows = result["result"][0]["results"]
    if not rows:
        break
    page += 1
    for r in rows:
        d1_null_grobid[r["work_id"]] = r.get("pdf_uuid")
    last_id = rows[-1]["work_id"]
    print(f"  page {page}: +{len(rows):,}  (total {len(d1_null_grobid):,}, last W{last_id}, {time.time() - t0:.0f}s)")
    if len(rows) < D1_PAGE_SIZE:
        break

print(f"  → {len(d1_null_grobid):,} D1 rows with NULL grobid_uuid in {time.time() - t0:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — locations_mapped with grobid_s3_id, EXCLUDING bad cohort
# MAGIC
# MAGIC Without exclusion: ~216K customers would transition `404 → BAD_BYTE` (garbage).
# MAGIC The bad cohort spans both source tables that feed `pdf_works → locations_mapped`:
# MAGIC `grobid_processing_results` (empty-TEI envelopes) and `grobid_xml_backfill`
# MAGIC (HTML wrappers carrying `[BAD_INPUT_DATA]` / `[NO_BLOCKS]` / `[TIMEOUT]` /
# MAGIC `[NO_GROBID_RESPONSES]` markers, plus empty-TEI envelopes).
# MAGIC
# MAGIC Multi-XML works keep their good sibling: LEFT ANTI JOIN drops the bad
# MAGIC uuid row, `MIN(grobid_uuid)` picks among the remaining good ones.

# COMMAND ----------

print("Querying locations_mapped (bad cohort excluded)…")
t0 = time.time()
lm_df = spark.sql("""
WITH bad_grobid_uuids AS (
  SELECT id FROM openalex.pdf.grobid_processing_results
  WHERE status IN ('success', 'success - cached response')
    AND (
         xml_content LIKE '%<body/>%'
      OR xml_content LIKE '%[BAD_INPUT_DATA]%'
      OR xml_content LIKE '%[NO_BLOCKS]%'
      OR xml_content LIKE '%[TIMEOUT]%'
      OR xml_content LIKE '%[NO_GROBID_RESPONSES]%'
    )
  UNION
  SELECT id FROM openalex.pdf.grobid_xml_backfill
  WHERE (
       xml_content LIKE '%<body/>%'
    OR xml_content LIKE '%[BAD_INPUT_DATA]%'
    OR xml_content LIKE '%[NO_BLOCKS]%'
    OR xml_content LIKE '%[TIMEOUT]%'
    OR xml_content LIKE '%[NO_GROBID_RESPONSES]%'
  )
),
lm_good AS (
  SELECT
    lm.work_id,
    REGEXP_REPLACE(lm.grobid_s3_id, '\\\\.xml\\\\.gz$', '') AS grobid_uuid
  FROM openalex.works.locations_mapped lm
  WHERE lm.grobid_s3_id IS NOT NULL
    AND lm.work_id IS NOT NULL
)
SELECT g.work_id, MIN(g.grobid_uuid) AS grobid_uuid
FROM lm_good g
LEFT ANTI JOIN bad_grobid_uuids b ON b.id = g.grobid_uuid
GROUP BY g.work_id
""")

# Materialize into Python — same as run_d1_antijoin.py pattern
lm_rows = lm_df.collect()
print(f"  → fetched {len(lm_rows):,} (work_id, grobid_uuid) rows in {time.time() - t0:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Find rows that need upsert

# COMMAND ----------

upserts = []
for row in lm_rows:
    work_id = row["work_id"]
    grobid_uuid = row["grobid_uuid"]
    if grobid_uuid is None:
        continue
    if work_id in d1_null_grobid:
        pdf_uuid = d1_null_grobid[work_id]  # may be None
        upserts.append((work_id, pdf_uuid, grobid_uuid))

print(f"Gap to close: {len(upserts):,} D1 rows need grobid_uuid populated")
print(f"  (D1 NULL-grobid rows: {len(d1_null_grobid):,}, LM with good grobid: {len(lm_rows):,})")

if PILOT_LIMIT > 0:
    upserts = upserts[:PILOT_LIMIT]
    print(f"\n  PILOT MODE — capping to first {PILOT_LIMIT:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Upsert (skipped if dry_run)

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — not upserting. Set dry_run=false to apply.")
elif not upserts:
    print("No gap — nothing to do.")
else:
    batches = [upserts[i : i + BATCH_SIZE] for i in range(0, len(upserts), BATCH_SIZE)]
    print(f"Uploading {len(batches):,} batches of {BATCH_SIZE} with {CONCURRENCY} workers…")
    total = 0
    failed = []

    def _upload(batch):
        try:
            return d1_batch_upsert(batch), None
        except Exception as e:
            return 0, {
                "first_work_id": batch[0][0],
                "last_work_id": batch[-1][0],
                "size": len(batch),
                "error": str(e)[:500],
            }

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(_upload, b) for b in batches]
        for i, fut in enumerate(as_completed(futures), 1):
            n, err = fut.result()
            total += n
            if err is not None:
                failed.append(err)
            if i % 100 == 0:
                elapsed = time.time() - t0
                rate = total / elapsed if elapsed else 0
                print(f"  {i}/{len(batches)} batches; upserted {total:,}  "
                      f"({rate:.0f} rows/s, {elapsed:.0f}s, fails={len(failed)})")

    elapsed = time.time() - t0
    print(f"\nUpserted {total:,} rows in {elapsed:.0f}s ({total / max(elapsed, 1):.0f} rows/s)")

    if failed:
        print(f"\nWARNING: {len(failed)} failed batch(es). First 20:")
        for f in failed[:20]:
            print(f"  {f}")
        raise RuntimeError(f"{len(failed)} batches failed — see log above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Refresh `content_stats.count_grobid`

# COMMAND ----------

if not DRY_RUN and upserts:
    print("Refreshing content_stats.count_grobid…")
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index "
        "WHERE grobid_uuid IS NOT NULL;"
    )
    result = d1_execute("SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
    print("\nD1 content_stats now:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")
