# Databricks notebook source
# MAGIC %md
# MAGIC # Title-Only Embeddings
# MAGIC
# MAGIC Generates embeddings for ~197M works that have titles but no abstracts.
# MAGIC Uses `ai_query()` with `databricks-gte-large-en` (free foundation model).
# MAGIC
# MAGIC Run 5 instances in parallel — NOT EXISTS prevents duplicates.

# COMMAND ----------

BATCH_SIZE = 100000
TARGET = 197000000  # title-only works to embed
SOURCE_VIEW = "openalex.vector_search.title_only_works_for_embedding"
OUTPUT_TABLE = "openalex.vector_search.work_embeddings_v2"

# COMMAND ----------

from datetime import datetime, timedelta
import time

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_remaining():
    return spark.sql(f"""
        SELECT COUNT(*) FROM {SOURCE_VIEW} src
        WHERE NOT EXISTS (
            SELECT 1 FROM {OUTPUT_TABLE} dst
            WHERE dst.work_id = src.work_id
        )
    """).first()[0]

# COMMAND ----------

# Initial status
start_remaining = get_remaining()
start_time = time.time()
log(f"Title-only works remaining: {start_remaining:,}")
log(f"Batch size: {BATCH_SIZE:,}")

# COMMAND ----------

batch_num = 0
total_embedded = 0
recent_rates = []

while True:
    batch_num += 1
    batch_start = time.time()

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
        FROM {SOURCE_VIEW} src
        WHERE NOT EXISTS (
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

    total_embedded += BATCH_SIZE
    estimated_remaining = start_remaining - total_embedded
    if estimated_remaining < 0:
        estimated_remaining = 0

    eta_seconds = estimated_remaining / rolling_rate if rolling_rate > 0 else 0
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)

    log(f"Batch {batch_num}: +{BATCH_SIZE:,} | "
        f"~{total_embedded:,} done | "
        f"Rate: {rolling_rate:,.0f}/s ({batch_elapsed:.0f}s/batch) | "
        f"Remaining: ~{estimated_remaining:,} | "
        f"ETA: {eta_time.strftime('%b %d %H:%M')} ({eta_seconds/3600:.1f}h)")

    # Every 50 batches, get exact remaining count
    if batch_num % 50 == 0:
        exact_remaining = get_remaining()
        log(f"  >> Exact remaining: {exact_remaining:,}")
        if exact_remaining == 0:
            log("=" * 60)
            log("COMPLETE! All title-only works embedded.")
            log("=" * 60)
            break
        estimated_remaining = exact_remaining
        total_embedded = start_remaining - exact_remaining

    time.sleep(2)

# COMMAND ----------

# Final status
log(f"Total time: {(time.time() - start_time)/3600:.1f} hours")
log(f"Batches: {batch_num}")
