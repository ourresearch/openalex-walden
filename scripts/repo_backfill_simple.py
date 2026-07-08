#!/usr/bin/env python3
"""
RepoBackfill - Simple token-based version with progress tracking.
Uses environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID
"""

import os
import time
from datetime import datetime, timedelta
from databricks import sql

# Configuration
TARGET_TABLE = "openalex.repo.repo_works_backfill"
SOURCE_TABLE = "openalex.repo.repo_items_backfill"


def get_connection():
    """Create connection using token auth."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "")
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    )


def run_query(query, fetch=True):
    """Execute a query and optionally fetch results."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if fetch:
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            return None


def main():
    print("=" * 70)
    print("RepoBackfill - Simple Version")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Step 1: Get endpoint counts
    print("\n[1/4] Loading endpoint list...")
    start = time.time()
    result = run_query(f"""
        SELECT COUNT(DISTINCT endpoint_id) as endpoints, COUNT(*) as rows
        FROM {SOURCE_TABLE}
    """)
    total_endpoints = result[0]['endpoints']
    total_rows = result[0]['rows']
    print(f"      Found {total_endpoints:,} endpoints with {total_rows:,} rows ({time.time()-start:.1f}s)")

    # Step 2: Check existing progress
    print("\n[2/4] Checking checkpoint...")
    start = time.time()
    try:
        result = run_query(f"SELECT COUNT(DISTINCT repository_id) as done FROM {TARGET_TABLE}")
        done_endpoints = result[0]['done']
        print(f"      {done_endpoints:,} endpoints already processed ({time.time()-start:.1f}s)")
    except:
        done_endpoints = 0
        print(f"      No existing data (table may not exist)")

    remaining = total_endpoints - done_endpoints
    print(f"      Remaining: {remaining:,} endpoints")

    if remaining == 0:
        print("\nNothing to do - all endpoints processed!")
        return

    # Step 3: Process all remaining in one INSERT
    print("\n[3/4] Processing remaining endpoints...")
    print("      (This will take a while - progress via row count checks)")

    insert_sql = f"""
    INSERT INTO {TARGET_TABLE}
    SELECT
        regexp_extract(api_raw, '<identifier>(.*?)</identifier>', 1) as native_id,
        'pmh' as native_id_namespace,
        substring(regexp_extract(api_raw, '<dc:title.*?>(.*?)</dc:title>', 1), 1, 5000) as title,
        lower(regexp_replace(regexp_extract(api_raw, '<dc:title.*?>(.*?)</dc:title>', 1), '[^a-zA-Z]', '')) as normalized_title,
        NULL as authors,
        NULL as ids,
        regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1) as raw_native_type,
        CASE
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%article%' THEN 'article'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%thesis%' THEN 'dissertation'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%dissertation%' THEN 'dissertation'
            WHEN lower(regexp_extract(api_raw, '<dc:type.*?>(.*?)</dc:type>', 1)) LIKE '%book%' THEN 'book'
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
    FROM {SOURCE_TABLE} src
    WHERE NOT EXISTS (
        SELECT 1 FROM {TARGET_TABLE} tgt
        WHERE tgt.repository_id = src.endpoint_id
    )
    """

    start = time.time()
    print(f"      Starting INSERT at {datetime.now().strftime('%H:%M:%S')}...")

    try:
        run_query(insert_sql, fetch=False)
        elapsed = time.time() - start
        print(f"      INSERT completed in {timedelta(seconds=int(elapsed))}")
    except Exception as e:
        print(f"      ERROR: {e}")
        return

    # Step 4: Verify
    print("\n[4/4] Verifying...")
    result = run_query(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT repository_id) as endpoints,
            COUNT(repository_id) as has_repo_id
        FROM {TARGET_TABLE}
    """)
    print(f"      Total rows: {result[0]['total_rows']:,}")
    print(f"      Endpoints: {result[0]['endpoints']:,}")
    print(f"      Has repository_id: {result[0]['has_repo_id']:,}")

    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
