# Databricks notebook source
# MAGIC %md
# MAGIC # Title-Only Embeddings
# MAGIC
# MAGIC Generates embeddings for ~197M works that have titles but no abstracts.
# MAGIC Uses `ai_query()` with `databricks-gte-large-en` (free foundation model).
# MAGIC
# MAGIC Reads from `works_for_embedding` (materialized Delta table — fast scans).
# MAGIC Run 5 instances in parallel — NOT EXISTS prevents duplicates.

# COMMAND ----------

BATCH_SIZE = 100000
SOURCE_TABLE = "openalex.vector_search.works_for_embedding"
OUTPUT_TABLE = "openalex.vector_search.work_embeddings_v2"

# COMMAND ----------

from datetime import datetime, timedelta
import time

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_embedded_count():
    return spark.sql(f"SELECT COUNT(*) FROM {OUTPUT_TABLE}").first()[0]

# COMMAND ----------

# Initial status — just count embeddings table (fast)
start_count = get_embedded_count()
start_time = time.time()
log(f"Current embeddings: {start_count:,}")
log(f"Batch size: {BATCH_SIZE:,}")
log(f"Source: {SOURCE_TABLE} (title-only works with has_abstract=false)")

# COMMAND ----------

batch_num = 0
recent_rates = []

while True:
    batch_num += 1
    batch_start = time.time()

    # Only embed title-only works (has_abstract=false) that aren't already embedded
    result = spark.sql(f"""
        INSERT INTO {OUTPUT_TABLE}
        SELECT
          src.work_id,
          ai_query('databricks-gte-large-en', SUBSTRING(src.text_to_embed, 1, 2000)) as embedding,
          src.publication_year,
          src.type,
          src.is_oa,
          src.has_abstract,
          src.has_content_pdf,
          src.has_content_grobid_xml
        FROM {SOURCE_TABLE} src
        WHERE src.has_abstract = false
          AND NOT EXISTS (
            SELECT 1 FROM {OUTPUT_TABLE} dst
            WHERE dst.work_id = src.work_id
          )
        LIMIT {BATCH_SIZE}
    """)

    batch_elapsed = time.time() - batch_start
    total_elapsed = time.time() - start_time

    # Track rolling rate (last 20 batches)
    batch_rate = BATCH_SIZE / batch_elapsed if batch_elapsed > 0 else 0
    recent_rates.append(batch_rate)
    if len(recent_rates) > 20:
        recent_rates.pop(0)
    rolling_rate = sum(recent_rates) / len(recent_rates)

    # Check count every batch
    current_count = get_embedded_count()
    added_this_session = current_count - start_count
    batch_added = current_count - (start_count + (batch_num - 1) * BATCH_SIZE)

    # If nothing was added, we're done
    if batch_num > 1 and current_count == start_count + (batch_num - 2) * BATCH_SIZE:
        log("No new rows added — all title-only works embedded!")
        break

    eta_seconds = (197000000 - added_this_session) / rolling_rate if rolling_rate > 0 else 0
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)

    log(f"Batch {batch_num}: {batch_elapsed:.0f}s | "
        f"Total: {current_count:,} (+{added_this_session:,}) | "
        f"Rate: {rolling_rate:,.0f}/s | "
        f"ETA: {eta_time.strftime('%b %d %H:%M')} ({eta_seconds/3600:.1f}h)")

    time.sleep(2)

# COMMAND ----------

# Final status
final_count = get_embedded_count()
total_time = time.time() - start_time
log(f"Final count: {final_count:,}")
log(f"Added: {final_count - start_count:,}")
log(f"Total time: {total_time/3600:.1f} hours")
