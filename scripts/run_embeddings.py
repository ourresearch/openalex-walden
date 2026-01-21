#!/usr/bin/env python3
"""
Continuous embedding generation for vector search.

Run from openalex-walden directory:
    python scripts/run_embeddings.py

Uses ai_query via SQL warehouse (not job clusters) to call OpenAI embeddings.
Runs batches sequentially to avoid DELTA_CONCURRENT_APPEND errors.

Required env vars:
    DATABRICKS_HOST - e.g., https://dbc-xxx.cloud.databricks.com
    DATABRICKS_TOKEN - Personal access token
    DATABRICKS_SQL_WAREHOUSE_ID - SQL warehouse ID
"""

import time
from datetime import datetime
import os
import sys
from databricks import sql

# Unbuffered output
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)


def get_connection():
    """Connect using token auth (simpler than OAuth service principal)."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "")
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    token = os.environ["DATABRICKS_TOKEN"]
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )


def run_query(query: str):
    """Execute SQL and return results."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

BATCH_SIZE = 50000
TARGET = 217000000
OUTPUT_TABLE = "openalex.vector_search.work_embeddings"
SOURCE_TABLE = "openalex.works.openalex_works"
ENDPOINT_NAME = "openai-embedding-3-small"

INSERT_SQL = f"""
INSERT INTO {OUTPUT_TABLE}
SELECT
    CAST(w.id AS STRING) as work_id,
    ai_query(
        '{ENDPOINT_NAME}',
        CONCAT('Title: ', LEFT(w.title, 500), '\\n\\nAbstract: ', LEFT(w.abstract, 5500))
    ) as embedding,
    md5(CONCAT('Title: ', LEFT(w.title, 500), '\\n\\nAbstract: ', LEFT(w.abstract, 5500))) as text_hash,
    w.publication_year,
    w.type,
    w.open_access.is_oa as is_oa,
    true as has_abstract,
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    w.has_content.pdf as has_content_pdf,
    w.has_content.grobid_xml as has_content_grobid_xml
FROM {SOURCE_TABLE} w
WHERE w.type != 'dataset'
  AND w.abstract IS NOT NULL
  AND w.title IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {OUTPUT_TABLE} e WHERE e.work_id = CAST(w.id AS STRING)
  )
LIMIT {BATCH_SIZE}
"""

COUNT_SQL = f"SELECT COUNT(*) as n FROM {OUTPUT_TABLE}"


def get_count():
    """Get current embedding count."""
    result = run_query(COUNT_SQL)
    return result[0]['n']


def process_batch():
    """Process a single batch of embeddings."""
    run_query(INSERT_SQL)


def main():
    start_time = time.time()
    start_count = get_count()
    batch_num = 0

    print(f"Starting from {start_count:,} embeddings")
    print(f"Target: {TARGET:,}")
    print(f"Batch size: {BATCH_SIZE:,}")
    print("=" * 70)

    while True:
        batch_start = time.time()
        batch_num += 1

        try:
            process_batch()
            batch_elapsed = time.time() - batch_start

            current = get_count()
            added_this_batch = current - start_count - (batch_num - 1) * BATCH_SIZE

            if added_this_batch <= 0 and batch_num > 1:
                print(f"\nNo new rows added - complete!")
                break

            total_added = current - start_count
            total_elapsed = time.time() - start_time
            rate = total_added / total_elapsed if total_elapsed > 0 else 0
            remaining = TARGET - current
            eta_hours = remaining / rate / 3600 if rate > 0 else 0

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch {batch_num}: "
                  f"{batch_elapsed:.0f}s | "
                  f"Total: {current:,} ({100*current/TARGET:.2f}%) | "
                  f"Rate: {rate:.0f}/s | "
                  f"ETA: {eta_hours:.1f}h")

        except Exception as e:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
            print("Waiting 60s before retry...")
            time.sleep(60)

    print("=" * 70)
    final = get_count()
    print(f"Done! {final:,} total embeddings")
    print(f"Added {final - start_count:,} in {(time.time() - start_time)/3600:.1f}h")


if __name__ == "__main__":
    main()
