# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Qwen3 embeddings (nightly, oxjob #1275)
# MAGIC
# MAGIC Keeps `openalex.vector_search.work_embeddings_qwen3` (the semantic-search corpus vectors, served from
# MAGIC ES `works-vectors-v2`) current with `openalex.works.openalex_works`. Runs nightly from
# MAGIC `jobs/embed_qwen3_nightly.yaml`; the ES incremental sync is the next task in that job.
# MAGIC
# MAGIC Replaces `ContinuousEmbeddings.py` (gte-large-en), whose source `works_for_embedding` was a snapshot with
# MAGIC no builder, which is why 109M works never got a gte vector.
# MAGIC
# MAGIC Steps, all idempotent (a failed run is simply re-run):
# MAGIC 1. Candidates = works updated in the last `lookback_days` whose embedding text (title / abstract / venue)
# MAGIC    is new or differs from `work_text_qwen3_source`, plus every titled work with no vector at all.
# MAGIC 2. MERGE the candidates' text into the source table (same bucket hash as the bulk run).
# MAGIC 3. DELETE their stale vectors, then embed in chunks via `ai_query('databricks-qwen3-embedding-0-6b', text)`,
# MAGIC    `parallel` chunks at a time as BLIND appends (no read of the target inside the parallel path: a
# MAGIC    NOT EXISTS guard there conflicts at commit, DELTA_CONCURRENT_APPEND).
# MAGIC    Documents are embedded bare: the instruction prefix is query-side only (elastic-api).
# MAGIC
# MAGIC One writer at a time: must not overlap the bulk runner (`oxjobs .../scratch/embed_loop.py`).
# MAGIC Measured 2026-09-20: ~2,370 rows/s at parallel=4, ≈$3.4 per 1M works (endpoint) + warehouse time.

# COMMAND ----------

dbutils.widgets.text("lookback_days", "3")
dbutils.widgets.text("parallel", "4")
dbutils.widgets.text("chunk_rows", "250000")
dbutils.widgets.text("max_works", "50000000")  # guardrail: fail before spending if the candidate set is absurd

LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
PARALLEL = int(dbutils.widgets.get("parallel"))
CHUNK_ROWS = int(dbutils.widgets.get("chunk_rows"))
MAX_WORKS = int(dbutils.widgets.get("max_works"))

ENDPOINT = "databricks-qwen3-embedding-0-6b"
SRC = "openalex.vector_search.work_text_qwen3_source"
DST = "openalex.vector_search.work_embeddings_qwen3"
CHG = "openalex.vector_search.qwen3_incremental_changed"
N_BUCKETS = 500  # same hash space as the bulk run

# Same text as the bulk run (embed_loop.py build-source). Changing this re-embeds the whole corpus.
TEXT_EXPR = """SUBSTRING(CONCAT('Title: ', w.title,
    CASE WHEN w.abstract IS NOT NULL THEN CONCAT('\\n\\nAbstract: ', w.abstract) ELSE '' END,
    CASE WHEN w.primary_location.source.display_name IS NOT NULL
         THEN CONCAT('\\n\\nVenue: ', w.primary_location.source.display_name) ELSE '' END), 1, 2000)"""

META_COLS = """w.publication_year, w.type, w.open_access.is_oa AS is_oa, w.has_abstract,
       w.has_content.pdf AS has_content_pdf, w.has_content.grobid_xml AS has_content_grobid_xml, w.updated_date"""

print(f"lookback={LOOKBACK_DAYS}d parallel={PARALLEL} chunk_rows={CHUNK_ROWS:,} max_works={MAX_WORKS:,}")

# COMMAND ----------

# 1. Candidates. `changed` = recently updated works whose text is new/different or which lack a vector.
#    `missing` = everything else with a title but no vector (self-heals the residual gap; bounded by MAX_WORKS).
import time
t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {CHG} AS
WITH recent AS (
  SELECT CAST(w.id AS STRING) AS work_id, {TEXT_EXPR} AS text, {META_COLS}
  FROM openalex.works.openalex_works w
  WHERE w.updated_date >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    AND w.title IS NOT NULL AND length(trim(w.title)) > 0
),
changed AS (
  SELECT r.*, 'changed' AS reason
  FROM recent r
  LEFT JOIN {SRC} s ON s.work_id = r.work_id
  LEFT JOIN (SELECT DISTINCT work_id FROM {DST}) e ON e.work_id = r.work_id
  WHERE s.work_id IS NULL OR s.text <> r.text OR e.work_id IS NULL
),
missing AS (
  SELECT CAST(w.id AS STRING) AS work_id, {TEXT_EXPR} AS text, {META_COLS}, 'missing' AS reason
  FROM openalex.works.openalex_works w
  LEFT ANTI JOIN {DST} e ON e.work_id = CAST(w.id AS STRING)
  WHERE (w.updated_date IS NULL OR w.updated_date < current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS)
    AND w.title IS NOT NULL AND length(trim(w.title)) > 0
),
cand AS (SELECT * FROM changed UNION ALL SELECT * FROM missing)
SELECT cand.*,
       abs(hash(work_id)) % {N_BUCKETS} AS bucket,
       CAST((row_number() OVER (ORDER BY work_id) - 1) / {CHUNK_ROWS} AS INT) AS chunk
FROM cand
""")
stats = spark.sql(f"SELECT reason, count(*) n FROM {CHG} GROUP BY reason").collect()
n_changed = sum(r.n for r in stats)
print(f"{n_changed:,} works to (re)embed ({', '.join(f'{r.reason}={r.n:,}' for r in stats)}) in {(time.time()-t0)/60:.1f} min")
if n_changed > MAX_WORKS:
    raise RuntimeError(f"{n_changed:,} candidates > max_works={MAX_WORKS:,}: refusing to spend ≈${n_changed*3.4/1e6:,.0f} "
                       f"unattended. If intended (e.g. TEXT_EXPR changed), re-run with a larger max_works.")

# COMMAND ----------

# 2. Keep the source table current (bulk runner reconciliation and future runs compare against it).
spark.sql(f"""
MERGE INTO {SRC} t USING {CHG} c ON t.work_id = c.work_id
WHEN MATCHED THEN UPDATE SET text = c.text, publication_year = c.publication_year, type = c.type, is_oa = c.is_oa,
     has_abstract = c.has_abstract, has_content_pdf = c.has_content_pdf, has_content_grobid_xml = c.has_content_grobid_xml,
     updated_date = c.updated_date
WHEN NOT MATCHED THEN INSERT (work_id, bucket, text, publication_year, type, is_oa, has_abstract, has_content_pdf, has_content_grobid_xml, updated_date)
     VALUES (c.work_id, c.bucket, c.text, c.publication_year, c.type, c.is_oa, c.has_abstract, c.has_content_pdf, c.has_content_grobid_xml, c.updated_date)
""")
print("source table merged")

# COMMAND ----------

# 3. Replace stale vectors (only the text-changed rows have any), then embed chunk by chunk, PARALLEL at a time.
from concurrent.futures import ThreadPoolExecutor, as_completed

deleted = spark.sql(f"DELETE FROM {DST} WHERE work_id IN (SELECT work_id FROM {CHG} WHERE reason = 'changed')").first()
print(f"stale vectors deleted: {deleted[0] if deleted else '?'}")

n_chunks = (n_changed + CHUNK_ROWS - 1) // CHUNK_ROWS if n_changed else 0

def embed_chunk(k, retries=3):
    for attempt in range(1, retries + 1):
        t = time.time()
        try:
            spark.sql(f"""
                INSERT INTO {DST} (work_id, embedding, publication_year, type, is_oa, has_abstract,
                                   has_content_pdf, has_content_grobid_xml, embedded_at, bucket)
                SELECT c.work_id, ai_query('{ENDPOINT}', c.text), c.publication_year, c.type, c.is_oa, c.has_abstract,
                       c.has_content_pdf, c.has_content_grobid_xml, current_timestamp(), c.bucket
                FROM {CHG} c WHERE c.chunk = {k}
            """)
            return k, attempt, time.time() - t, None
        except Exception as e:  # noqa: BLE001
            err = str(e)[:300]
            print(f"chunk {k} attempt {attempt} failed after {time.time()-t:.0f}s: {err}")
            time.sleep(30 * attempt)
    return k, retries, None, err

t0 = time.time()
failed = []
with ThreadPoolExecutor(max_workers=PARALLEL) as ex:
    futures = [ex.submit(embed_chunk, k) for k in range(n_chunks)]
    for i, f in enumerate(as_completed(futures), 1):
        k, attempt, secs, err = f.result()
        if err:
            failed.append(k)
        else:
            print(f"[{i}/{n_chunks}] chunk {k} ok in {secs:.0f}s (attempt {attempt}); "
                  f"elapsed {(time.time()-t0)/60:.1f} min")
print(f"embedding done: {n_chunks - len(failed)}/{n_chunks} chunks in {(time.time()-t0)/60:.1f} min; failed chunks: {failed}")

# COMMAND ----------

# 4. Report. A failed chunk fails the run (email), but everything else stays embedded; re-running picks the rest up.
embedded = spark.sql(f"SELECT count(*) FROM {DST} e JOIN {CHG} c ON c.work_id = e.work_id").first()[0]
gap = spark.sql(f"""
  SELECT count(*) FROM openalex.works.openalex_works w
  LEFT ANTI JOIN {DST} e ON e.work_id = CAST(w.id AS STRING)
  WHERE w.title IS NOT NULL AND length(trim(w.title)) > 0
""").first()[0]
total = spark.sql("SELECT count(*) FROM openalex.works.openalex_works").first()[0]
print(f"embedded this run: {embedded:,} of {n_changed:,} candidates (≈${embedded*3.4/1e6:,.0f} endpoint)")
print(f"coverage gap after run: {gap:,} titled works without a vector = {100*gap/total:.3f}% (ACCEPTANCE #1275 test 2 wants < 0.1%)")
if failed:
    raise RuntimeError(f"{len(failed)} chunks failed: {failed}. Re-run the job; it only redoes what is missing.")
import json
dbutils.notebook.exit(json.dumps({"candidates": n_changed, "by_reason": {r.reason: r.n for r in stats}, "embedded": embedded,
                                  "chunks": n_chunks, "embed_minutes": round((time.time() - t0) / 60, 1),
                                  "gap": gap, "gap_pct": round(100 * gap / total, 4)}))  # visible via jobs get-run-output
