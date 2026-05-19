#!/usr/bin/env python3
"""
Delete bad-cohort rows from Cloudflare D1's `content_index`.

The cohort is `openalex.pdf.invalid_pdfs` — source_pdf_id values that
scan_pdf_bytes classified as non-servable (HTML / TEXT_ERROR / R2_MISSING /
JSON_ERROR / GZIPPED_PDF / GZIPPED_OTHER / GZIPPED_TRUNCATED). The same
cohort drove the in-place `pdf_works.ids` strip MERGE (oxjob #185).

D1 keys:
  - content_index PK: work_id (one row per work)
  - pdf_uuid: bare source_pdf_id (no `.pdf` suffix) — matches openalex.pdf.invalid_pdfs

Approach:
  1. Pull cohort source_pdf_ids from Databricks (~2.97M).
  2. Batch DELETE by pdf_uuid IN (...) with 8 concurrent workers.

This DELETEs entire rows (work_id removed from D1) rather than NULL-ing
pdf_uuid + grobid_uuid. Justification:
  - The cohort is "bad PDF in R2" → strip docs.pdf from pdf_works → next
    end2end run NULLs locations_mapped.pdf_s3_id → nightly D1 sync's filter
    (`WHERE pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL`) skips the
    work going forward. So D1 row stays orphaned with stale UUIDs forever
    unless we actively remove it.
  - The corresponding grobid_uuid is bad-by-cascade (96.9% of bad-PDF
    works have empty/error-stub XML per 2026-05-18 probe), same reason we
    stripped docs.parsed-pdf from pdf_works alongside docs.pdf.
  - If a future rescrape produces a new (valid) source_pdf_id for the same
    work_id, the nightly D1 sync INSERTs a fresh row — no orphan to evict.

Idempotent: DELETE on a non-existent pdf_uuid is a no-op. Safe to re-run.

Usage:
  python scripts/run_d1_delete_invalid_pdfs.py --dry-run        # count only
  python scripts/run_d1_delete_invalid_pdfs.py --limit 100      # pilot
  python scripts/run_d1_delete_invalid_pdfs.py                  # full
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

# 500 UUIDs × ~40 bytes each ≈ 20 KB per statement — well under D1's per-
# statement length cap. Matches sync_content_index_to_d1.py's choice.
BATCH_SIZE = 500
CONCURRENCY = 8


def d1_execute(token: str, sql_text: str, max_retries: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"sql": sql_text}
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


def d1_batch_delete(token: str, pdf_uuids: list[str]) -> int:
    if not pdf_uuids:
        return 0
    # Single-quoted IN list. source_pdf_id is a UUID — no quote-escaping needed.
    in_list = ",".join(f"'{u}'" for u in pdf_uuids)
    sql = f"DELETE FROM content_index WHERE pdf_uuid IN ({in_list})"
    d1_execute(token, sql)
    return len(pdf_uuids)


def fetch_d1_stats(token: str) -> dict:
    """Return {stat_key: int(stat_value)} for the count_* stats."""
    result = d1_execute(
        token,
        "SELECT stat_key, stat_value FROM content_stats "
        "WHERE stat_key IN ('count_all','count_pdf','count_grobid')"
    )
    out: dict = {}
    rows = (result.get("result", [{}])[0] or {}).get("results", []) or []
    for r in rows:
        try:
            out[r["stat_key"]] = int(r["stat_value"])
        except (KeyError, TypeError, ValueError):
            pass
    return out


def update_d1_stats(token: str) -> None:
    """Recompute count_all/count_pdf/count_grobid from content_index."""
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_all', COUNT(*), datetime('now') FROM content_index"
    )
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_pdf', COUNT(*), datetime('now') FROM content_index "
        "WHERE pdf_uuid IS NOT NULL"
    )
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index "
        "WHERE grobid_uuid IS NOT NULL"
    )


def fetch_cohort(limit: int | None) -> list[str]:
    """Pull source_pdf_ids from openalex.pdf.invalid_pdfs via the warehouse."""
    print("\nFetching cohort from openalex.pdf.invalid_pdfs…")
    t0 = time.time()
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT source_pdf_id FROM openalex.pdf.invalid_pdfs {limit_clause}"
    with get_connection(size="medium") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    cohort = [r[0] for r in rows]
    print(f"  → fetched {len(cohort):,} source_pdf_ids in {time.time() - t0:.0f}s")
    return cohort


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute cohort size + show pre-state; no D1 writes")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap cohort size (pilot mode)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"UUIDs per DELETE statement (default {BATCH_SIZE})")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Concurrent DELETE workers (default {CONCURRENCY})")
    args = parser.parse_args()

    token = os.environ.get("CLOUDFLARE_D1_API_TOKEN")
    if not token:
        sys.stderr.write("ERROR: CLOUDFLARE_D1_API_TOKEN not set\n")
        sys.exit(2)

    t_start = time.time()

    print("Pre-DELETE D1 stats:")
    pre = fetch_d1_stats(token)
    for k, v in pre.items():
        print(f"  {k}: {v:,}")

    cohort = fetch_cohort(args.limit)
    if not cohort:
        print("Empty cohort — nothing to do.")
        return

    if args.dry_run:
        print(f"\nDRY RUN — would issue DELETEs for {len(cohort):,} pdf_uuids "
              f"in {-(-len(cohort) // args.batch_size):,} batches of {args.batch_size}.")
        return

    batches = [cohort[i : i + args.batch_size] for i in range(0, len(cohort), args.batch_size)]
    print(f"\nDeleting {len(cohort):,} rows in {len(batches):,} batches of "
          f"{args.batch_size} with {args.concurrency} workers…")

    deleted = 0
    failed: list = []

    def _delete(batch):
        try:
            return d1_batch_delete(token, batch), None
        except Exception as e:
            return 0, {
                "first_uuid": batch[0],
                "last_uuid": batch[-1],
                "size": len(batch),
                "error": str(e)[:500],
            }

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [ex.submit(_delete, b) for b in batches]
        for i, fut in enumerate(as_completed(futures), 1):
            n, err = fut.result()
            deleted += n
            if err:
                failed.append(err)
            if i % 50 == 0 or i == len(batches):
                elapsed = time.time() - t0
                rate = deleted / elapsed if elapsed else 0
                print(f"  batch {i}/{len(batches)}  deleted={deleted:,}  "
                      f"failed={len(failed)}  rate={rate:.0f} rows/s  elapsed={elapsed:.0f}s")

    elapsed = time.time() - t0
    print(f"\nDELETE complete: {deleted:,} rows in {elapsed:.0f}s "
          f"({deleted / max(elapsed, 1):.0f} rows/s, {len(failed)} failed batches)")

    if failed:
        print("\nFailed batches (first 10):")
        for f in failed[:10]:
            print(f"  {f}")

    print("\nRecomputing content_stats…")
    update_d1_stats(token)

    print("\nPost-DELETE D1 stats:")
    post = fetch_d1_stats(token)
    for k, v in post.items():
        delta = v - pre.get(k, 0)
        print(f"  {k}: {v:,}  (Δ {delta:+,})")

    print(f"\nTotal elapsed: {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
