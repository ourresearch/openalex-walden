#!/usr/bin/env python3
"""
RepoBackfill via SQL - Chunked with Progress Tracking

Processes repo backfill data endpoint-by-endpoint using SQL:
- ✅ Checkpointing (resume from where we left off)
- ✅ Progress reporting (every endpoint)
- ✅ Rolling rate calculation
- ✅ ETA estimation
- ✅ 10-minute rule compliance

Usage:
    python scripts/repo_backfill_sql.py [--dry-run] [--limit N]
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from collections import deque

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.databricks_sql import run_query, run_query_df

# Configuration
TARGET_TABLE = "openalex.repo.repo_works_backfill"
SOURCE_TABLE = "openalex.repo.repo_items_backfill"
BATCH_LOG_INTERVAL = 10  # Log every N endpoints
ROLLING_WINDOW = 50  # Calculate rate over last N endpoints


def get_endpoints():
    """Get all endpoints with row counts, ordered by size (smallest first)."""
    print("Loading endpoint list...")
    df = run_query_df(f"""
        SELECT endpoint_id, COUNT(*) as row_count
        FROM {SOURCE_TABLE}
        GROUP BY endpoint_id
        ORDER BY row_count ASC
    """)
    endpoints = [(row['endpoint_id'], row['row_count']) for _, row in df.iterrows()]
    total_rows = df['row_count'].sum()
    print(f"✓ Found {len(endpoints):,} endpoints with {total_rows:,} total rows")
    return endpoints, total_rows


def get_done_endpoints():
    """Get endpoints already processed (checkpoint)."""
    try:
        df = run_query_df(f"SELECT DISTINCT repository_id FROM {TARGET_TABLE}")
        done = set(df['repository_id'].tolist())
        print(f"✓ Checkpoint: {len(done):,} endpoints already processed")
        return done
    except Exception as e:
        print(f"✓ No checkpoint found (table may not exist): {e}")
        return set()


def process_endpoint(endpoint_id: str, dry_run: bool = False) -> int:
    """Process a single endpoint via SQL INSERT. Returns row count."""

    sql = f"""
    INSERT INTO {TARGET_TABLE}
    SELECT
        regexp_extract(api_raw, '<identifier>(.*?)</identifier>', 1) as native_id,
        'pmh' as native_id_namespace,
        substring(regexp_extract(api_raw, '<dc:title.*?>(.*?)</dc:title>', 1), 1, 5000) as title,
        lower(regexp_replace(
            regexp_extract(api_raw, '<dc:title.*?>(.*?)</dc:title>', 1),
            '[^a-zA-Z]', ''
        )) as normalized_title,
        NULL as authors,
        NULL as ids,
        regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1) as raw_native_type,
        CASE
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%article%' THEN 'article'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%thesis%' THEN 'dissertation'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%dissertation%' THEN 'dissertation'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%book%' THEN 'book'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%report%' THEN 'report'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%preprint%' THEN 'preprint'
            ELSE 'other'
        END as type,
        'submittedVersion' as version,
        NULL as license,
        NULL as language,
        NULL as published_date,
        NULL as created_date,
        to_date(regexp_extract(api_raw, '<datestamp>(.*?)</datestamp>', 1)) as updated_date,
        NULL as issue,
        NULL as volume,
        NULL as first_page,
        NULL as last_page,
        NULL as is_retracted,
        substring(regexp_extract(api_raw, '<dc:description>(.*?)</dc:description>', 1), 1, 10000) as abstract,
        regexp_extract(api_raw, '<dc:source.*?>(.*?)</dc:source>', 1) as source_name,
        regexp_extract(api_raw, '<dc:publisher.*?>(.*?)</dc:publisher>', 1) as publisher,
        NULL as funders,
        NULL as refs,
        NULL as urls,
        NULL as mesh,
        false as is_oa,
        endpoint_id as repository_id
    FROM {SOURCE_TABLE}
    WHERE endpoint_id = '{endpoint_id}'
    """

    if dry_run:
        # Just count what we would insert
        count_sql = f"SELECT COUNT(*) as cnt FROM {SOURCE_TABLE} WHERE endpoint_id = '{endpoint_id}'"
        result = run_query(count_sql)
        return result[0]['cnt']
    else:
        run_query(sql)
        # Get count of what we inserted
        count_sql = f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE} WHERE repository_id = '{endpoint_id}'"
        result = run_query(count_sql)
        return result[0]['cnt']


def ensure_table_exists():
    """Create target table if it doesn't exist."""
    print("Checking target table...")
    try:
        run_query(f"DESCRIBE TABLE {TARGET_TABLE}")
        print(f"✓ Target table {TARGET_TABLE} exists")
    except:
        print(f"Creating target table {TARGET_TABLE}...")
        run_query(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                native_id STRING,
                native_id_namespace STRING,
                title STRING,
                normalized_title STRING,
                authors STRING,
                ids STRING,
                raw_native_type STRING,
                type STRING,
                version STRING,
                license STRING,
                language STRING,
                published_date DATE,
                created_date DATE,
                updated_date DATE,
                issue STRING,
                volume STRING,
                first_page STRING,
                last_page STRING,
                is_retracted BOOLEAN,
                abstract STRING,
                source_name STRING,
                publisher STRING,
                funders STRING,
                refs STRING,
                urls STRING,
                mesh STRING,
                is_oa BOOLEAN,
                repository_id STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true'
            )
        """)
        print(f"✓ Created {TARGET_TABLE}")


def main():
    parser = argparse.ArgumentParser(description='RepoBackfill via SQL with progress tracking')
    parser.add_argument('--dry-run', action='store_true', help='Count only, do not insert')
    parser.add_argument('--limit', type=int, help='Process only N endpoints')
    args = parser.parse_args()

    print("=" * 70)
    print("RepoBackfill SQL - Chunked with Progress Tracking")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S CT')}")
    print(f"Target: {TARGET_TABLE}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'PRODUCTION'}")
    print("=" * 70)
    print()

    # Setup
    if not args.dry_run:
        ensure_table_exists()

    endpoints, total_rows = get_endpoints()
    done_endpoints = get_done_endpoints()

    # Filter to remaining
    remaining = [(e, r) for e, r in endpoints if e not in done_endpoints]
    remaining_rows = sum(r for _, r in remaining)

    if args.limit:
        remaining = remaining[:args.limit]
        print(f"✓ Limited to {args.limit} endpoints")

    print(f"✓ Remaining: {len(remaining):,} endpoints with {remaining_rows:,} rows")
    print()

    if not remaining:
        print("Nothing to process - all endpoints already done!")
        return

    # Progress tracking
    start_time = time.time()
    rows_processed = 0
    endpoints_processed = 0
    recent_times = deque(maxlen=ROLLING_WINDOW)
    errors = []

    print("=" * 70)
    print("PROCESSING ENDPOINTS")
    print("=" * 70)
    print()

    for i, (endpoint_id, expected_rows) in enumerate(remaining):
        endpoint_start = time.time()

        try:
            actual_rows = process_endpoint(endpoint_id, dry_run=args.dry_run)
            rows_processed += actual_rows
            endpoints_processed += 1
            remaining_rows -= actual_rows

            endpoint_time = time.time() - endpoint_start
            recent_times.append((actual_rows, endpoint_time))

            # Calculate rolling rate
            if recent_times:
                recent_rows = sum(r for r, t in recent_times)
                recent_time = sum(t for r, t in recent_times)
                rolling_rate = recent_rows / recent_time if recent_time > 0 else 0
            else:
                rolling_rate = actual_rows / endpoint_time if endpoint_time > 0 else 0

            # Calculate ETA
            eta_seconds = remaining_rows / rolling_rate if rolling_rate > 0 else 0
            eta = timedelta(seconds=int(eta_seconds))

            # Log progress
            if (i + 1) % BATCH_LOG_INTERVAL == 0 or i == 0 or i == len(remaining) - 1:
                elapsed = timedelta(seconds=int(time.time() - start_time))
                pct = 100 * rows_processed / total_rows
                print(f"[{i+1:,}/{len(remaining):,}] {endpoint_id[:15]}... | "
                      f"{actual_rows:,} rows | {rolling_rate:,.0f}/sec | "
                      f"{pct:.1f}% | ETA: {eta} | Elapsed: {elapsed}")

        except Exception as e:
            error_msg = f"[{i+1}] {endpoint_id}: {str(e)[:100]}"
            errors.append(error_msg)
            print(f"✗ ERROR: {error_msg}")

    # Summary
    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Endpoints processed: {endpoints_processed:,}")
    print(f"Rows processed: {rows_processed:,}")
    print(f"Total time: {timedelta(seconds=int(elapsed))}")
    print(f"Average rate: {rows_processed/elapsed:,.0f} rows/sec" if elapsed > 0 else "N/A")
    print(f"Errors: {len(errors)}")

    if errors:
        print("\nErrors encountered:")
        for e in errors[:10]:
            print(f"  - {e}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")


if __name__ == "__main__":
    main()
