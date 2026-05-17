#!/usr/bin/env python3
"""
One-off catch-up: sync openalex.works.locations_mapped → Cloudflare D1.

Why this exists outside Databricks: the Spark-based notebook was 10-20x slower
than the xlarge SQL warehouse for the same query, and the Databricks job loop
(deploy → cluster spin-up → run → fail → repeat) was adding ~30 min per
iteration. This script does the same work in ~10-15 min end-to-end with no
cluster, no deploy, and no spot fragility.

The query:
  - Filters on `openalex_created_dt` (row-level, sparse) rather than
    `openalex_updated_dt` (which ticks on any field change → effectively
    whole-table scan).
  - GROUP BY work_id with MIN(...) picks pdf_uuid and grobid_uuid
    independently across the work's location rows — maximizes coverage and
    fixes a NULL-sort dedup bug from the old ROW_NUMBER ordering.

Idempotent: INSERT OR REPLACE in D1 means crashed runs can be retried.

Setup:
  - Set CLOUDFLARE_D1_API_TOKEN in .env. Pull from Databricks:
      databricks secrets get-secret cloudflare d1_api_token --output JSON \\
        --profile dbc-ce570f73-0362 | jq -r '.value' | base64 -d

Usage:
  python scripts/run_d1_backfill.py --dry-run            # count only
  python scripts/run_d1_backfill.py                       # uses last_sync from D1
  python scripts/run_d1_backfill.py \\
      --sync-from 2026-01-21 --sync-to 2026-05-17        # explicit window
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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
# D1 caps SQL statement at ~100 KB. Each row tuple is ~85-110 bytes, so 800
# rows is well under the limit even with maximally-wide tuples.
BATCH_SIZE = 800
# D1 handles parallel inserts fine; ~8 concurrent keeps wall time reasonable
# without overwhelming the API.
CONCURRENCY = 8


def d1_execute(token: str, sql_text: str, params: list | None = None,
               max_retries: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload: dict = {"sql": sql_text}
    if params is not None:
        payload["params"] = params
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = requests.post(CF_API_BASE, headers=headers, json=payload, timeout=120)
        except (requests.ConnectionError, requests.Timeout) as e:
            # Transient network/DNS hiccup on the laptop side; retry with backoff.
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


def get_last_sync(token: str) -> str | None:
    result = d1_execute(
        token,
        "SELECT stat_value FROM content_stats WHERE stat_key = 'last_sync_timestamp'",
    )
    try:
        return result["result"][0]["results"][0]["stat_value"]
    except (KeyError, IndexError):
        return None


def update_content_stats(token: str) -> None:
    """Refresh count_all / count_pdf / count_grobid (D1-side aggregates)."""
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync-from", default=None, help="UTC date YYYY-MM-DD")
    parser.add_argument("--sync-to", default=None, help="UTC date YYYY-MM-DD (exclusive)")
    parser.add_argument("--dry-run", action="store_true", help="Count only, no D1 writes")
    parser.add_argument(
        "--no-update-timestamp",
        action="store_true",
        help="Skip advancing last_sync_timestamp at the end",
    )
    args = parser.parse_args()

    token = os.environ.get("CLOUDFLARE_D1_API_TOKEN")
    if not token:
        sys.stderr.write(
            "ERROR: CLOUDFLARE_D1_API_TOKEN not set in .env\n"
            "Pull from Databricks:\n"
            "  databricks secrets get-secret cloudflare d1_api_token --output JSON \\\n"
            "    --profile dbc-ce570f73-0362 | jq -r '.value' | base64 -d\n"
        )
        sys.exit(2)

    # Determine sync window
    if args.sync_from:
        sync_from_str = args.sync_from
    else:
        last = get_last_sync(token)
        if last:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00")).replace(tzinfo=None)
            sync_from_dt = last_dt - timedelta(hours=2)
        else:
            sync_from_dt = datetime.utcnow() - timedelta(days=2)
        sync_from_str = sync_from_dt.strftime("%Y-%m-%d")

    sync_to_str = args.sync_to or (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Sync window: created_dt >= '{sync_from_str}' AND < '{sync_to_str}'")
    print(f"Dry run: {args.dry_run}")

    query = f"""
    SELECT
      work_id,
      MIN(REPLACE(pdf_s3_id, '.pdf', ''))      AS pdf_uuid,
      MIN(REPLACE(grobid_s3_id, '.xml.gz', '')) AS grobid_uuid
    FROM openalex.works.locations_mapped
    WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
      AND work_id IS NOT NULL
      AND openalex_created_dt >= '{sync_from_str}'
      AND openalex_created_dt <  '{sync_to_str}'
    GROUP BY work_id
    """

    t0 = time.time()
    print(f"\nConnecting to xlarge SQL warehouse…")
    with get_connection(size="xlarge") as conn:
        with conn.cursor() as cursor:
            if args.dry_run:
                cursor.execute(f"SELECT COUNT(*) AS n FROM ({query})")
                n = cursor.fetchone()[0]
                print(f"\nDry run — rows in window: {n:,}  ({time.time() - t0:.1f}s)")
                return

            print(f"Executing query…")
            cursor.execute(query)
            print(f"  query started ({time.time() - t0:.1f}s); fetching all rows…")
            all_rows = cursor.fetchall()
            print(f"  fetched {len(all_rows):,} rows ({time.time() - t0:.1f}s)")

    # Build batches in memory, then upload in parallel.
    batches = [
        all_rows[i : i + BATCH_SIZE] for i in range(0, len(all_rows), BATCH_SIZE)
    ]
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
                print(
                    f"  {i}/{len(batches)} batches; upserted {total:,}  "
                    f"({rate:.0f} rows/s, {elapsed:.0f}s, fails={len(failed)})"
                )

    elapsed = time.time() - t0
    print(f"\nUpserted {total:,} rows in {elapsed:.0f}s ({total / max(elapsed, 1):.0f} rows/s)")

    if failed:
        print(f"\nWARNING: {len(failed)} failed batch(es). First 20:")
        for f in failed[:20]:
            print(f"  {f}")
        sys.exit(1)

    if not args.no_update_timestamp:
        new_ts = datetime.utcnow().isoformat() + "Z"
        d1_execute(
            token,
            "INSERT OR REPLACE INTO content_stats (stat_key, stat_value, updated_at) "
            "VALUES (?, ?, datetime('now'))",
            ["last_sync_timestamp", new_ts],
        )
        print(f"Advanced last_sync_timestamp to {new_ts}")

    print("\nRefreshing content_stats counts…")
    update_content_stats(token)
    result = d1_execute(token, "SELECT stat_key, stat_value FROM content_stats ORDER BY stat_key")
    print("\nD1 content_stats now:")
    for row in result["result"][0]["results"]:
        print(f"  {row['stat_key']}: {row['stat_value']}")


if __name__ == "__main__":
    main()
