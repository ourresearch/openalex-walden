#!/usr/bin/env python3
"""
NULL out `grobid_uuid` in Cloudflare D1's `content_index` for the bad cohort
in `openalex.pdf.invalid_grobids` (#202 Track 2).

Mirror of `scripts/run_d1_delete_invalid_pdfs.py` (#185), with three deltas:
  1. UPDATE (NULL out grobid_uuid), don't DELETE the row. The grobid cleanup
     leaves docs.pdf intact in pdf_works → pdf_s3_id stays populated in
     locations_mapped → the row keeps `has_content.pdf:true` for customers.
     Only `has_content.grobid_xml` needs to flip false, which corresponds to
     NULL-ing the grobid_uuid column. Don't kill the whole D1 row.
  2. Cohort source is `locations_mapped × openalex.pdf.invalid_grobids` (not
     a classification table) — we need (work_id, grobid_uuid) pairs, and
     invalid_grobids only carries grobid_uuid; work_id comes from LM.
  3. Only `count_grobid` needs refresh in content_stats. count_pdf and
     count_all are untouched because no rows are removed.

D1 keys:
  - content_index PK: work_id (one row per work)
  - grobid_uuid: bare uuid (no `.xml.gz` suffix). NOT indexed in D1.

Approach:
  1. Pull (work_id, grobid_uuid) pairs from LM × invalid_grobids.
  2. Batch UPDATE WHERE work_id IN (...) AND grobid_uuid IN (...) with 8
     workers. work_id is PK → index lookup; grobid_uuid IN (...) is the
     in-memory per-row guard.

The AND clause is a cross-product, not strict pair-matching: any row whose
work_id is in the batch's id list AND whose current grobid_uuid is in the
batch's uuid set will be NULLed. Safe because every uuid in the cohort is
bad by construction:
  - A good rescraped uuid would NOT be in uuid_set → predicate fails →
    row preserved (this is the rescrape-safety property).
  - A loose cross-match (W_a has U_b where (W_b, U_b) is in the batch) is
    still a bad-uuid → still want to NULL it.

## Sequencing — must run BEFORE strip-propagation

Cohort sourcing joins `locations_mapped × invalid_grobids`. Once the strip
notebook's effect propagates through CDF → locations_parsed → MERGE →
locations_mapped, the bad grobid_s3_id rows in LM are NULLed and drop out
of the join. After that point:
  - Multi-XML works: harmless — next sync_content_index_to_d1 picks the
    good sibling via MIN(grobid_s3_id).
  - Single-XML bad works: stale forever — LM has no grobid_s3_id row to
    drive an update, and the nightly sync's `IS NOT NULL` filter skips
    them. Only this script can clean them.
Run BEFORE the next walden_end2end cycle (05:00 UTC daily).

Idempotent: UPDATE to NULL on a row whose grobid_uuid is already NULL is a
no-op. Safe to re-run.

Usage:
  python scripts/run_d1_null_invalid_grobids.py --dry-run        # count only
  python scripts/run_d1_null_invalid_grobids.py --limit 100      # pilot
  python scripts/run_d1_null_invalid_grobids.py                  # full
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


def d1_batch_null_grobid(token: str, pairs: list[tuple[int, str]]) -> int:
    """UPDATE content_index SET grobid_uuid = NULL by (work_id, grobid_uuid).

    work_id is PK → index used for row lookup. grobid_uuid IN (...) clause
    is the in-memory per-row safety guard (preserves a row whose grobid_uuid
    was rescraped to a NEW good value between cohort fetch and this UPDATE).
    """
    if not pairs:
        return 0
    work_ids = ",".join(str(w) for w, _ in pairs)
    uuid_set = {u for _, u in pairs}
    uuid_list = ",".join(f"'{u}'" for u in uuid_set)
    sql = (
        "UPDATE content_index SET grobid_uuid = NULL "
        f"WHERE work_id IN ({work_ids}) "
        f"AND grobid_uuid IN ({uuid_list})"
    )
    d1_execute(token, sql)
    return len(pairs)


def fetch_d1_stats(token: str) -> dict:
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


def update_d1_count_grobid(token: str) -> None:
    """Only count_grobid changes. count_pdf and count_all are untouched
    because we don't remove rows."""
    d1_execute(
        token,
        "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
        "SELECT 'count_grobid', COUNT(*), datetime('now') FROM content_index "
        "WHERE grobid_uuid IS NOT NULL"
    )


def fetch_cohort(limit: int | None) -> list[tuple[int, str]]:
    """Pull (work_id, grobid_uuid) pairs from locations_mapped × invalid_grobids."""
    print("\nFetching (work_id, grobid_uuid) pairs from LM × invalid_grobids…")
    t0 = time.time()
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
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
    {limit_clause}
    """
    with get_connection(size="xlarge") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    cohort = [(r[0], r[1]) for r in rows]
    print(f"  → fetched {len(cohort):,} pairs in {time.time() - t0:.0f}s")
    return cohort


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute cohort size + show pre-state; no D1 writes")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap cohort size (pilot mode)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Pairs per UPDATE statement (default {BATCH_SIZE})")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Concurrent UPDATE workers (default {CONCURRENCY})")
    args = parser.parse_args()

    token = os.environ.get("CLOUDFLARE_D1_API_TOKEN")
    if not token:
        sys.stderr.write("ERROR: CLOUDFLARE_D1_API_TOKEN not set\n")
        sys.exit(2)

    t_start = time.time()

    print("Pre-UPDATE D1 stats:")
    pre = fetch_d1_stats(token)
    for k, v in pre.items():
        print(f"  {k}: {v:,}")

    cohort = fetch_cohort(args.limit)
    if not cohort:
        print("Empty cohort — nothing to do.")
        return

    if args.dry_run:
        print(f"\nDRY RUN — would issue UPDATEs for {len(cohort):,} (work_id, grobid_uuid) "
              f"pairs in {-(-len(cohort) // args.batch_size):,} batches of {args.batch_size}.")
        return

    batches = [cohort[i : i + args.batch_size] for i in range(0, len(cohort), args.batch_size)]
    print(f"\nNULLing grobid_uuid on {len(cohort):,} rows in {len(batches):,} batches of "
          f"{args.batch_size} with {args.concurrency} workers…")

    updated = 0
    failed: list = []

    def _update(batch):
        try:
            return d1_batch_null_grobid(token, batch), None
        except Exception as e:
            return 0, {
                "first_work_id": batch[0][0],
                "last_work_id": batch[-1][0],
                "size": len(batch),
                "error": str(e)[:500],
            }

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [ex.submit(_update, b) for b in batches]
        for i, fut in enumerate(as_completed(futures), 1):
            n, err = fut.result()
            updated += n
            if err:
                failed.append(err)
            if i % 50 == 0 or i == len(batches):
                elapsed = time.time() - t0
                rate = updated / elapsed if elapsed else 0
                print(f"  batch {i}/{len(batches)}  updated={updated:,}  "
                      f"failed={len(failed)}  rate={rate:.0f} rows/s  elapsed={elapsed:.0f}s")

    elapsed = time.time() - t0
    print(f"\nUPDATE complete: {updated:,} rows in {elapsed:.0f}s "
          f"({updated / max(elapsed, 1):.0f} rows/s, {len(failed)} failed batches)")

    if failed:
        print("\nFailed batches (first 10):")
        for f in failed[:10]:
            print(f"  {f}")

    print("\nRecomputing count_grobid…")
    update_d1_count_grobid(token)

    print("\nPost-UPDATE D1 stats:")
    post = fetch_d1_stats(token)
    for k, v in post.items():
        delta = v - pre.get(k, 0)
        print(f"  {k}: {v:,}  (Δ {delta:+,})")

    print(f"\nTotal elapsed: {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
