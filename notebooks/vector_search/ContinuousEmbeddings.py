# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Embeddings
# MAGIC
# MAGIC Runs embedding batches continuously until all 217M works are embedded.
# MAGIC Uses `ai_query()` with `databricks-gte-large-en` (free foundation model).

# COMMAND ----------

BATCH_SIZE = 100000
TARGET = 217000000

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import time

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_count():
    return spark.sql("SELECT COUNT(*) FROM openalex.vector_search.work_embeddings_v2").first()[0]

# COMMAND ----------

# Initial status
start_count = get_count()
start_time = time.time()
log(f"Starting: {start_count:,} embeddings ({100*start_count/TARGET:.2f}%)")
log(f"Target: {TARGET:,}")
log(f"Remaining: {TARGET - start_count:,}")

# COMMAND ----------

batch_num = 0
while True:
    batch_num += 1
    batch_start = time.time()

    # Run batch
    result = spark.sql(f"""
        INSERT INTO openalex.vector_search.work_embeddings_v2
        SELECT
          work_id,
          ai_query('databricks-gte-large-en', SUBSTRING(text_to_embed, 1, 2000)) as embedding,
          publication_year, type, is_oa, has_abstract, has_content_pdf, has_content_grobid_xml
        FROM openalex.vector_search.works_for_embedding src
        WHERE NOT EXISTS (
            SELECT 1 FROM openalex.vector_search.work_embeddings_v2 dst
            WHERE dst.work_id = src.work_id
        )
        LIMIT {BATCH_SIZE}
    """)

    batch_elapsed = time.time() - batch_start

    # Check progress
    current = get_count()
    total_elapsed = time.time() - start_time
    rows_done = current - start_count
    remaining = TARGET - current

    if rows_done > 0:
        rate = rows_done / (total_elapsed / 60)  # rows per minute
        eta_minutes = remaining / rate if rate > 0 else float('inf')
        eta_time = datetime.now() + timedelta(minutes=eta_minutes)

        log(f"Batch {batch_num}: {current:,} ({100*current/TARGET:.2f}%) | "
            f"Rate: {rate:,.0f}/min | Batch: {batch_elapsed:.0f}s | "
            f"ETA: {eta_time.strftime('%b %d %H:%M')} ({eta_minutes/60:.1f}h)")
    else:
        log(f"Batch {batch_num}: {current:,} - no new rows")

    # Check completion
    if current >= TARGET:
        log("=" * 60)
        log(f"COMPLETE! {current:,} embeddings")
        log("=" * 60)
        break

    # Brief pause
    time.sleep(2)

# COMMAND ----------

# Final status
final_count = get_count()
total_time = time.time() - start_time
log(f"Final count: {final_count:,}")
log(f"Total time: {total_time/3600:.1f} hours")
log(f"Average rate: {(final_count - start_count) / (total_time/60):,.0f} rows/min")
