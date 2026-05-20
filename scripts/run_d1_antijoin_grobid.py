#!/usr/bin/env python3
"""
Reconciliation pass for the D1↔LM grobid_uuid gap (oxjob #202 Track 1).

The 2026-05-20 measurement found a 3.2M gap:
  D1 content_index count_grobid = 44,181,799
  LM distinct work_id with grobid_s3_id = 47,397,790
  Gap (LM − D1) = 3,215,991

`run_d1_antijoin.py` only catches work_ids ENTIRELY MISSING from D1. The
residual gap here is a *column-level* one: work_ids that ARE in D1 (with
pdf_uuid populated) but have `grobid_uuid = NULL`, despite LM having a
grobid_s3_id for them.

This is the upstream cause of ~9.35% of customer requests for grobid-xml
returning 404 from Content API even though metadata advertises
has_content.grobid_xml:true (per the 10K customer-view validation in
evidence/xml_validation_set_2026-05-20.jsonl).

Approach:
  1. Pull (work_id, pdf_uuid) from D1 where `grobid_uuid IS NULL` (paginated).
     These are the rows we might need to update.
  2. Query locations_mapped for (work_id, grobid_uuid) with grobid_s3_id NOT NULL.
  3. Join in memory → rows where D1.work_id matches LM and D1.grobid_uuid is NULL.
  4. INSERT OR REPLACE into content_index preserving the existing pdf_uuid and
     setting grobid_uuid to the LM value.
  5. Refresh content_stats.

Idempotent via INSERT OR REPLACE. Safe to re-run.

Usage:
  python scripts/run_d1_antijoin_grobid.py --dry-run     # count gap, no writes
  python scripts/run_d1_antijoin_grobid.py               # do the upserts
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.databricks_sql import get_connection  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
CF_D1_DATABASE_ID = "c2e1cc17-1810-400b-a7c8-c5103ab366de"
CF_API_BASE = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/d1/database/{CF_D1_DATABASE_ID}/query"
)
BATCH_SIZE = 800
CONCURRENCY = 8
D1_PAGE_SIZE = 1_000_000


def d1_execute(token: str, sql_text: str, max_retries: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload: dict = {"sql": sql_text}
    last_err: Exception | None = None
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


def d1_batch_upsert(token: str, rows: list[tuple[int, str | None, str]]) -> None:
    """rows: (work_id, pdf_uuid_to_preserve, grobid_uuid_to_set)."""
    if not rows:
        return
    values = []
    for work_id, pdf_uuid, grobid_uuid in rows:
        pdf_lit = f"'{pdf_uuid}'" if pdf_uuid is not None else "NULL"
        grobid_lit = f"'{grobid_uuid}'"  # always non-null by construction
        values.append(f"({work_id}, {pdf_lit}, {grobid_lit})")
    sql_text = (
        "INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES "
        + ",".join(values)
    )
    d1_execute(token, sql_text)


def fetch_d1_rows_missing_grobid(token: str) -> dict[int, str | None]:
    """Return {work_id: pdf_uuid} for D1 rows where grobid_uuid IS NULL."""
    print(f"\nFetching D1 rows where grobid_uuid IS NULL (page size {D1_PAGE_SIZE:,})…")
    out: dict[int, str | None] = {}
    last_id = 0
    page = 0
    t0 = time.time()
    while True:
        sql = (
            f"SELECT work_id, pdf_uuid FROM content_index "
            f"WHERE grobid_uuid IS NULL AND work_id > {last_id} "
            f"ORDER BY work_id LIMIT {D1_PAGE_SIZE}"
        )
        result = d1_execute(token, sql)
        rows = result["result"][0]["results"]
        if not rows:
            break
        page += 1
        for r in rows:
            out[r["work_id"]] = r.get("pdf_uuid")
        last_id = rows[-1]["work_id"]
        print(f"  page {page}: +{len(rows):,}  "
              f"(total {len(out):,}, last W{last_id}, {time.time() - t0:.0f}s)")
        if len(rows) < D1_PAGE_SIZE:
            break
    print(f"  → {len(out):,} D1 rows with NULL grobid_uuid in {time.time() - t0:.0f}s")
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute gap size only; no D1 writes")
    args = parser.parse_args()

    token = os.environ.get("CLOUDFLARE_D1_API_TOKEN")
    if not token:
        sys.stderr.write("ERROR: CLOUDFLARE_D1_API_TOKEN not set\n")
        sys.exit(2)

    t_start = time.time()

    # Step 1: D1 rows with NULL grobid_uuid → potential update targets
    d1_null_grobid = fetch_d1_rows_missing_grobid(token)

    # Step 2: locations_mapped — work_ids with a grobid_s3_id, EXCLUDING the bad cohort
    #
    # Without the exclude: we'd transition ~3% of synced rows (~161K of 5.4M) from
    # an honest 404 to "Content API redirects to garbage bytes". The bad cohort
    # comes from BOTH source tables that feed pdf_works → locations_mapped:
    #   - grobid_processing_results: empty-TEI envelopes (structural emptiness)
    #   - grobid_xml_backfill: HTML wrappers carrying [BAD_INPUT_DATA] / [NO_BLOCKS] /
    #     [TIMEOUT] / [NO_GROBID_RESPONSES] markers, plus empty-TEI envelopes
    #
    # Per the 2026-05-20 10K customer-view validation (oxjob #202 evidence/), the
    # rules below match every bad-byte class we found. Multi-XML works keep their
    # good sibling: LEFT ANTI JOIN drops the bad uuid row, MIN picks among the rest.
    print(f"\nQuerying locations_mapped (work_ids with grobid_s3_id, bad cohort excluded)…")
    t0 = time.time()
    query = """
    WITH bad_grobid_uuids AS (
      SELECT id FROM openalex.pdf.grobid_processing_results
      WHERE status IN ('success', 'success - cached response')
        AND (
             xml_content LIKE '%<body/>%'                  -- catches TEI_EMPTY_*
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
    SELECT
      g.work_id,
      MIN(g.grobid_uuid) AS grobid_uuid
    FROM lm_good g
    LEFT ANTI JOIN bad_grobid_uuids b ON b.id = g.grobid_uuid
    GROUP BY g.work_id
    """
    with get_connection(size="xlarge") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print(f"  query started ({time.time() - t0:.0f}s); fetching rows…")
            lm_rows = cursor.fetchall()
    print(f"  → fetched {len(lm_rows):,} (work_id, grobid_uuid) rows from LM in {time.time() - t0:.0f}s")

    # Step 3: rows that need updating
    # - work_id is in D1 with NULL grobid_uuid
    # - AND LM has a grobid_uuid for it
    # Preserve existing pdf_uuid from D1 (don't clobber).
    upserts: list[tuple[int, str | None, str]] = []
    for work_id, grobid_uuid in lm_rows:
        if grobid_uuid is None:
            continue
        if work_id in d1_null_grobid:
            pdf_uuid = d1_null_grobid[work_id]  # may be None
            upserts.append((work_id, pdf_uuid, grobid_uuid))

    print(f"\nGap to close: {len(upserts):,} D1 rows need grobid_uuid populated")
    print(f"  (D1 NULL-grobid rows: {len(d1_null_grobid):,}; "
          f"LM with-grobid rows: {len(lm_rows):,})")

    if args.dry_run or not upserts:
        if args.dry_run:
            print("\nDry run — not upserting.")
        else:
            print("\nNo gap — nothing to do.")
        print(f"\nTotal elapsed: {time.time() - t_start:.0f}s")
        return

    # Step 4: batched upsert
    batches = [upserts[i : i + BATCH_SIZE] for i in range(0, len(upserts), BATCH_SIZE)]
    print(f"\nUploading {len(batches):,} batches of {BATCH_SIZE} with {CONCURRENCY} workers…")
    total = 0
    failed: list = []

    def _upload(batch):
        try:
            d1_batch_upsert(token, batch)
            return len(batch), None
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
        sys.exit(1)

    # Step 5: refresh stats
    print("\nRefreshing content_stats.count_grobid…")
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index "
        "WHERE grobid_uuid IS NOT NULL;",
    )
    result = d1_execute(token, "SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
    print("\nD1 content_stats now:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")

    print(f"\nTotal elapsed: {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
