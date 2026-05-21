# Databricks notebook source
# MAGIC %md
# MAGIC # D1 NULL-out invalid grobid_uuids (#202 Track 2)
# MAGIC
# MAGIC Sets `grobid_uuid = NULL` in D1 `content_index` for the bad cohort —
# MAGIC works whose grobid_s3_id in `locations_mapped` matches an entry in
# MAGIC `openalex.pdf.invalid_grobids`. Pairs with the pdf_works strip and the
# MAGIC R2 delete (oxjob #202 Track 2).
# MAGIC
# MAGIC ## Why NULL out (not DELETE the row like #185 did for pdfs)
# MAGIC
# MAGIC The grobid cleanup leaves `docs.pdf` intact in pdf_works → `pdf_s3_id`
# MAGIC stays populated in locations_mapped → the row keeps `has_content.pdf:true`
# MAGIC for the customer. We only need to flip `has_content.grobid_xml:false`,
# MAGIC which corresponds to NULL-ing the `grobid_uuid` column. The work_id row
# MAGIC in D1 stays alive for PDF serving.
# MAGIC
# MAGIC ## Key strategy
# MAGIC
# MAGIC D1 has only the PK index on `work_id`. Pulling (work_id, grobid_uuid)
# MAGIC pairs from LM lets us do work_id-keyed UPDATEs (uses the PK) rather
# MAGIC than grobid_uuid-keyed (which would full-scan since grobid_uuid is
# MAGIC unindexed). The AND grobid_uuid IN (...) clause is a safety guard:
# MAGIC if a rescrape produced a new (good) uuid between this scan and the
# MAGIC UPDATE, the per-row predicate fails and we preserve the new value.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC UPDATE to NULL on a row whose grobid_uuid is already NULL is a no-op.
# MAGIC Safe to re-run.
# MAGIC
# MAGIC ## Sequencing — must run BEFORE strip-propagation
# MAGIC
# MAGIC Cohort sourcing joins `locations_mapped × invalid_grobids` on
# MAGIC `grobid_s3_id`. Once the strip notebook's effect propagates through
# MAGIC CDF → locations_parsed → MERGE → locations_mapped, the bad
# MAGIC grobid_s3_id rows in LM are NULLed and drop out of the join. After
# MAGIC that point:
# MAGIC   - Multi-XML works: harmless — next sync_content_index_to_d1 picks
# MAGIC     the good sibling via MIN(grobid_s3_id).
# MAGIC   - Single-XML bad works: stale forever — LM has no grobid_s3_id row
# MAGIC     to drive an update, and the nightly sync's `IS NOT NULL` filter
# MAGIC     skips them. Only this script can clean them.
# MAGIC Run this BEFORE the strip propagates (i.e. before the next end2end
# MAGIC run that consumes pdf_works CDF) to avoid leaving single-XML rows
# MAGIC stale in D1.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `dry_run`: "true" = compute counts only, no D1 writes
# MAGIC   - `pilot_limit`: 0 = full cohort, >0 = process only first N

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

# 500 ids/batch matches sync_content_index_to_d1 (well under D1 stmt-length cap)
BATCH_SIZE = 500
CONCURRENCY = 8

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no D1 writes)")
dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full)")

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


def d1_batch_null_grobid(rows):
    """rows: list of (work_id, grobid_uuid).

    UPDATE content_index SET grobid_uuid = NULL
    WHERE work_id IN (...) AND grobid_uuid IN (...)

    The AND clause is a cross-product, not strict pair-matching: any row
    whose work_id is in the batch's id list AND whose current grobid_uuid
    is in the batch's uuid set will be NULLed. Safe because every uuid in
    the cohort is bad by construction:
      - A good rescraped uuid would NOT be in uuid_set → predicate fails →
        row preserved (this is the rescrape-safety property).
      - A loose cross-match (W_a has U_b where (W_b, U_b) is in the batch)
        is still bad-uuid → still want to NULL it.
    Strict pair-matching would require `(work_id, grobid_uuid) IN (VALUES…)`,
    which inflates statement length without changing the outcome.
    """
    if not rows:
        return 0
    work_id_list = ",".join(str(r[0]) for r in rows)
    uuid_set = {r[1] for r in rows}
    uuid_list = ",".join(f"'{u}'" for u in uuid_set)
    sql_text = (
        "UPDATE content_index SET grobid_uuid = NULL "
        f"WHERE work_id IN ({work_id_list}) "
        f"AND grobid_uuid IN ({uuid_list})"
    )
    d1_execute(sql_text)
    return len(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the cohort: (work_id, grobid_uuid) where grobid_s3_id is in invalid_grobids

# COMMAND ----------

print("Querying locations_mapped for bad-cohort (work_id, grobid_uuid) pairs…")
t0 = time.time()
df = spark.sql("""
WITH bad AS (
  SELECT grobid_uuid FROM openalex.pdf.invalid_grobids
)
SELECT
  lm.work_id,
  REGEXP_REPLACE(lm.grobid_s3_id, '\\\\.xml\\\\.gz$', '') AS grobid_uuid
FROM openalex.works.locations_mapped lm
JOIN bad b ON b.grobid_uuid = REGEXP_REPLACE(lm.grobid_s3_id, '\\\\.xml\\\\.gz$', '')
WHERE lm.grobid_s3_id IS NOT NULL
  AND lm.work_id IS NOT NULL
""")

rows = df.collect()
print(f"  → fetched {len(rows):,} (work_id, grobid_uuid) pairs in {time.time() - t0:.0f}s")

# Convert to plain tuples
cohort = [(r["work_id"], r["grobid_uuid"]) for r in rows]
if PILOT_LIMIT > 0:
    cohort = cohort[:PILOT_LIMIT]
    print(f"PILOT MODE — capped to first {PILOT_LIMIT:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE (skipped if dry_run)

# COMMAND ----------

if DRY_RUN:
    print(f"DRY RUN — would NULL out grobid_uuid for {len(cohort):,} D1 rows.")
elif not cohort:
    print("No cohort — nothing to do.")
else:
    batches = [cohort[i : i + BATCH_SIZE] for i in range(0, len(cohort), BATCH_SIZE)]
    print(f"Uploading {len(batches):,} batches of {BATCH_SIZE} with {CONCURRENCY} workers…")
    total = 0
    failed = []

    def _upload(batch):
        try:
            return d1_batch_null_grobid(batch), None
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
                print(f"  {i}/{len(batches)} batches; processed {total:,}  "
                      f"({rate:.0f} rows/s, {elapsed:.0f}s, fails={len(failed)})")

    elapsed = time.time() - t0
    print(f"\nProcessed {total:,} rows in {elapsed:.0f}s ({total / max(elapsed, 1):.0f} rows/s)")

    if failed:
        print(f"\nWARNING: {len(failed)} failed batch(es). First 20:")
        for f in failed[:20]:
            print(f"  {f}")
        raise RuntimeError(f"{len(failed)} batches failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh count_grobid

# COMMAND ----------

if not DRY_RUN and cohort:
    d1_execute(
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index "
        "WHERE grobid_uuid IS NOT NULL;"
    )
    result = d1_execute("SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
    print("D1 content_stats now:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")
