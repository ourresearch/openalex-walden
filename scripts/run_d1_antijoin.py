#!/usr/bin/env python3
"""
Reconciliation pass: find works in `openalex.works.locations_mapped` whose
work_id is NOT in D1 `content_index`, and upsert them.

Why this exists separately from `run_d1_backfill.py`:
The Track-0 backfill caught new rows since the 2026-01-21 D1 sync was paused
(`created_dt >= 2026-01-21`). But the original 2026-01-21 D1 bulk load itself
was incomplete — ~2.5M pre-pause rows were never in D1. Track 0's filter
correctly excluded those because `created_dt < 2026-01-21`. This script does
the structurally-correct antijoin to find and close that residual gap.

Approach:
  1. Pull all work_ids from D1 (paginated 1M/call, ~3-4 min for ~62M rows)
  2. Query locations_mapped for all (work_id, pdf_uuid, grobid_uuid) with
     content (a single warehouse scan, ~3-10 min depending on warehouse)
  3. Antijoin in memory: rows where work_id NOT IN D1 set
  4. Upsert the missing rows to D1 (8 workers, 800-row batches)

Idempotent via INSERT OR REPLACE. Safe to re-run.

Note: this catches WORK-ID-LEVEL gaps. It does NOT catch in-place pdf_uuid
replacements on existing D1 rows. That's a separate, smaller concern that
would need a (work_id, pdf_uuid) tuple-level diff.

Usage:
  python scripts/run_d1_antijoin.py --dry-run     # count gap, no writes
  python scripts/run_d1_antijoin.py               # do the upserts
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
D1_PAGE_SIZE = 1_000_000  # rows per pagination call against D1


def d1_execute(token: str, sql_text: str, params: list | None = None,
               max_retries: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload: dict = {"sql": sql_text}
    if params is not None:
        payload["params"] = params
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


def d1_batch_insert(token: str, rows: list[tuple]) -> None:
    if not rows:
        return
    values = []
    for r in rows:
        wid = r[0]
        pdf = f"'{r[1]}'" if r[1] is not None else "NULL"
        grobid = f"'{r[2]}'" if r[2] is not None else "NULL"
        values.append(f"({wid}, {pdf}, {grobid})")
    sql_text = (
        "INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES "
        + ",".join(values)
    )
    d1_execute(token, sql_text)


def fetch_d1_work_ids(token: str) -> set[int]:
    """Paginate D1's content_index, return all work_ids as a set."""
    print(f"\nFetching all work_ids from D1 (page size {D1_PAGE_SIZE:,})…")
    ids: set[int] = set()
    last_id = 0
    page = 0
    t0 = time.time()
    while True:
        sql = (
            f"SELECT work_id FROM content_index "
            f"WHERE work_id > {last_id} "
            f"ORDER BY work_id LIMIT {D1_PAGE_SIZE}"
        )
        result = d1_execute(token, sql)
        rows = result["result"][0]["results"]
        if not rows:
            break
        page += 1
        for r in rows:
            ids.add(r["work_id"])
        last_id = rows[-1]["work_id"]
        print(f"  page {page}: +{len(rows):,} rows  "
              f"(total {len(ids):,}, last W{last_id}, {time.time() - t0:.0f}s)")
        if len(rows) < D1_PAGE_SIZE:
            break
    print(f"  → fetched {len(ids):,} work_ids in {time.time() - t0:.0f}s")
    return ids


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

    # Step 1: pull D1 work_ids
    d1_ids = fetch_d1_work_ids(token)

    # Step 2: full scan of locations_mapped
    print(f"\nQuerying locations_mapped (all works with content)…")
    t0 = time.time()
    query = """
    SELECT
      work_id,
      MIN(REPLACE(pdf_s3_id, '.pdf', ''))      AS pdf_uuid,
      MIN(REPLACE(grobid_s3_id, '.xml.gz', '')) AS grobid_uuid
    FROM openalex.works.locations_mapped
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL
    GROUP BY work_id
    """
    with get_connection(size="xlarge") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print(f"  query started ({time.time() - t0:.0f}s); fetching rows…")
            all_rows = cursor.fetchall()
    print(f"  → fetched {len(all_rows):,} rows from locations_mapped in {time.time() - t0:.0f}s")

    # Step 3: antijoin
    missing = [r for r in all_rows if r[0] not in d1_ids]
    print(f"\nAntijoin: {len(missing):,} work_ids in locations_mapped but not in D1 "
          f"({100 * len(missing) / max(len(all_rows), 1):.2f}% gap)")

    if args.dry_run or len(missing) == 0:
        if args.dry_run:
            print("Dry run — not upserting.")
        else:
            print("No gap — nothing to do.")
        print(f"\nTotal elapsed: {time.time() - t_start:.0f}s")
        return

    # Step 4: upsert missing rows
    batches = [missing[i : i + BATCH_SIZE] for i in range(0, len(missing), BATCH_SIZE)]
    print(f"\nUploading {len(batches):,} batches of {BATCH_SIZE} with {CONCURRENCY} workers…")
    total = 0
    failed: list = []

    def _upload(batch):
        try:
            d1_batch_insert(token, batch)
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

    print("\nRefreshing content_stats counts…")
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_all', COUNT(*), datetime('now') FROM content_index;",
    )
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_pdf', COUNT(*), datetime('now') FROM content_index "
        "WHERE pdf_uuid IS NOT NULL;",
    )
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
