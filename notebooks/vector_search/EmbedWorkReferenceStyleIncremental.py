# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental reference-style work vectors (nightly, oxjob #2000)
# MAGIC
# MAGIC Keeps `openalex.vector_search.work_reference_style_embeddings_qwen3` current with `openalex.works.openalex_works`. The reference
# MAGIC matcher (#2000) searches these vectors with a reference string: each work is embedded as it would be cited,
# MAGIC "first 3 authors (et al). title. journal. year" (Casey, 10 Nov 2026: the cited work came first 88% of the time vs 50%
# MAGIC with the title + abstract vectors). Runs nightly from `jobs/embed_qwen3_nightly.yaml` after the `embed` task, on the same
# MAGIC endpoint. Built once from the 434.4M-work backfill of 11 Nov 2026 (oxjobs reference-matcher `work/cite_embed.py`).
# MAGIC
# MAGIC Steps, all idempotent (a failed run is simply re-run):
# MAGIC 1. Candidates = works updated in the last `lookback_days` whose reference-style text differs from the table's `text` (or that
# MAGIC    have no vector), plus every titled work with no vector at all.
# MAGIC 2. Embed them in chunks via `ai_query('databricks-qwen3-embedding-0-6b', text)` into a staging table (unit float16).
# MAGIC 3. Each new vector's list = its nearest centroid (`/Volumes/openalex/vector_search/work_reference_style_index/centroids.npy`),
# MAGIC    then one MERGE into the table. A work whose embedding failed keeps its old row, so it is a candidate again next run.
# MAGIC 4. Delete rows of works no longer in `openalex_works` (merged or deleted works must never be linked).
# MAGIC
# MAGIC ≈$3.4 per 1M works (endpoint), as the `embed` task.

# COMMAND ----------

dbutils.widgets.text("lookback_days", "3")
dbutils.widgets.text("parallel", "4")
dbutils.widgets.text("chunk_rows", "250000")
dbutils.widgets.text("max_works", "50000000")  # guardrail: fail before spending if the candidate set is absurd
dbutils.widgets.text("max_gone", "2000000")    # guardrail: refuse to delete more rows than this for works no longer present

LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
PARALLEL = int(dbutils.widgets.get("parallel"))
CHUNK_ROWS = int(dbutils.widgets.get("chunk_rows"))
MAX_WORKS = int(dbutils.widgets.get("max_works"))
MAX_GONE = int(dbutils.widgets.get("max_gone"))

ENDPOINT = "databricks-qwen3-embedding-0-6b"
W = "openalex.works.openalex_works"
DST = "openalex.vector_search.work_reference_style_embeddings_qwen3"
CHG = "openalex.vector_search.reference_style_incremental_changed"
STG = "openalex.vector_search.reference_style_incremental_staged"
VOL = "/Volumes/openalex/vector_search/work_reference_style_index"
D, LISTS_PER_GROUP = 1024, 16

# Same text as the backfill (oxjobs reference-matcher work/cite_embed.py). Changing this re-embeds the whole corpus.
TEXT = """concat_ws('. ',
  nullif(concat(array_join(slice(transform(w.authorships, a -> a.author.display_name), 1, 3), ', '),
                CASE WHEN size(w.authorships) > 3 THEN ' et al' ELSE '' END), ''),
  w.title, w.primary_location.source.display_name, CAST(w.publication_year AS STRING))"""

print(f"lookback={LOOKBACK_DAYS}d parallel={PARALLEL} chunk_rows={CHUNK_ROWS:,} max_works={MAX_WORKS:,}")

# COMMAND ----------

# 1. Candidates.
import time
t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {CHG} AS
WITH recent AS (
  SELECT w.id AS work_id, {TEXT} AS text
  FROM {W} w
  WHERE w.updated_date >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    AND w.title IS NOT NULL AND length(trim(w.title)) > 0
),
changed AS (
  SELECT r.*, 'changed' AS reason
  FROM recent r
  LEFT JOIN (SELECT work_id, text FROM {DST}) d ON d.work_id = r.work_id
  WHERE d.work_id IS NULL OR d.text <> r.text
),
missing AS (
  SELECT w.id AS work_id, {TEXT} AS text, 'missing' AS reason
  FROM {W} w
  LEFT ANTI JOIN {DST} d ON d.work_id = w.id
  WHERE (w.updated_date IS NULL OR w.updated_date < current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS)
    AND w.title IS NOT NULL AND length(trim(w.title)) > 0
),
cand AS (SELECT * FROM changed UNION ALL SELECT * FROM missing)
SELECT cand.*, CAST((row_number() OVER (ORDER BY work_id) - 1) / {CHUNK_ROWS} AS INT) AS chunk
FROM cand
""")
stats = spark.sql(f"SELECT reason, count(*) n FROM {CHG} GROUP BY reason").collect()
n_cand = sum(r.n for r in stats)
print(f"{n_cand:,} works to (re)embed ({', '.join(f'{r.reason}={r.n:,}' for r in stats)}) in {(time.time()-t0)/60:.1f} min")
if n_cand > MAX_WORKS:
    raise RuntimeError(f"{n_cand:,} candidates > max_works={MAX_WORKS:,}: refusing to spend ≈${n_cand*3.4/1e6:,.0f} "
                       f"unattended. If intended (e.g. TEXT changed), re-run with a larger max_works.")

# COMMAND ----------

# 2. Embed chunk by chunk, PARALLEL at a time, as blind appends into the staging table.
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from concurrent.futures import ThreadPoolExecutor, as_completed


@pandas_udf("binary")
def unit_fp16(s: pd.Series) -> pd.Series:
    def one(a):
        if a is None:
            return None
        v = np.asarray(a, dtype=np.float32)
        n = np.linalg.norm(v)
        return (v / n).astype(np.float16).tobytes() if n > 0 else None
    return s.map(one)


spark.udf.register("ref_style_unit_fp16", unit_fp16)
spark.sql(f"CREATE OR REPLACE TABLE {STG} (work_id BIGINT, text STRING, v BINARY, error STRING)")
n_chunks = (n_cand + CHUNK_ROWS - 1) // CHUNK_ROWS if n_cand else 0


def embed_chunk(k, retries=3):
    for attempt in range(1, retries + 1):
        t = time.time()
        try:
            spark.sql(f"""
                INSERT INTO {STG}
                SELECT work_id, text, ref_style_unit_fp16(r.result), r.errorMessage
                FROM (SELECT work_id, text, ai_query('{ENDPOINT}', text, failOnError => false) AS r FROM {CHG} WHERE chunk = {k})
            """)
            return k, attempt, time.time() - t, None
        except Exception as e:  # noqa: BLE001
            err = str(e)[:300]
            print(f"chunk {k} attempt {attempt} failed after {time.time()-t:.0f}s: {err}")
            time.sleep(30 * attempt)
    return k, retries, None, err


t1 = time.time()
failed = []
with ThreadPoolExecutor(max_workers=PARALLEL) as ex:
    for i, f in enumerate(as_completed([ex.submit(embed_chunk, k) for k in range(n_chunks)]), 1):
        k, attempt, secs, err = f.result()
        if err:
            failed.append(k)
        else:
            print(f"[{i}/{n_chunks}] chunk {k} ok in {secs:.0f}s (attempt {attempt}); elapsed {(time.time()-t1)/60:.1f} min")
st = spark.sql(f"SELECT count(*) n, count_if(v IS NULL) bad FROM {STG}").first()
print(f"embedded {st.n - st.bad:,} of {n_cand:,} ({st.bad:,} without a vector) in {(time.time()-t1)/60:.1f} min; failed chunks: {failed}")

# COMMAND ----------

# 3. List per vector (nearest centroid, read from the volume on each executor), then one MERGE into the table.
import pyarrow as pa
from pyspark.sql import functions as F

def assign(batches):
    import numpy as np, pyarrow as pa
    C = np.load(f"{VOL}/centroids.npy").astype(np.float32)
    for b in batches:
        n = b.num_rows
        if n == 0:
            continue
        col = b.column("v")
        off = np.frombuffer(col.buffers()[1], dtype=np.int32)[col.offset: col.offset + n + 1]
        V = np.frombuffer(col.buffers()[2], dtype=np.uint8)[off[0]: off[-1]].view(np.float16).reshape(n, D).astype(np.float32)
        lid = np.argmax(V @ C.T, axis=1).astype(np.int32)
        yield pa.RecordBatch.from_arrays([b.column("work_id"), pa.array(lid), col, b.column("text")],
                                         names=["work_id", "list_id", "v", "text"])


(spark.sql(f"SELECT work_id, v, text FROM {STG} WHERE v IS NOT NULL").dropDuplicates(["work_id"])
 .mapInArrow(assign, "work_id bigint, list_id int, v binary, text string")
 .withColumn("lg", (F.col("list_id") / LISTS_PER_GROUP).cast("int"))
 .createOrReplaceTempView("ref_style_assigned"))
spark.sql("CACHE TABLE ref_style_assigned_c AS SELECT * FROM ref_style_assigned")
m = spark.sql(f"""
MERGE INTO {DST} t USING ref_style_assigned_c s ON t.work_id = s.work_id
WHEN MATCHED THEN UPDATE SET list_id = s.list_id, v = s.v, text = s.text, embedded_at = current_timestamp(), lg = s.lg
WHEN NOT MATCHED THEN INSERT (work_id, list_id, v, text, embedded_at, lg)
     VALUES (s.work_id, s.list_id, s.v, s.text, current_timestamp(), s.lg)
""").first()
print(f"merged: {m.asDict() if m else '?'}")

# COMMAND ----------

# 4. Works no longer in openalex_works lose their row (guarded: a half-written works table must not empty this one).
gone = spark.sql(f"SELECT count(*) FROM {DST} d LEFT ANTI JOIN {W} w ON w.id = d.work_id").first()[0]
print(f"rows for works no longer present: {gone:,}")
if gone > MAX_GONE:
    raise RuntimeError(f"{gone:,} rows for works no longer in {W} > max_gone={MAX_GONE:,}: not deleting. "
                       f"Check {W}; if intended, re-run with a larger max_gone.")
if gone:
    spark.sql(f"""
    MERGE INTO {DST} t USING (SELECT d.work_id FROM {DST} d LEFT ANTI JOIN {W} w ON w.id = d.work_id) g ON t.work_id = g.work_id
    WHEN MATCHED THEN DELETE
    """)

# COMMAND ----------

# 5. Report. A failed chunk fails the task, but everything else is merged; re-running picks the rest up.
import json
rows = spark.sql(f"SELECT count(*) FROM {DST}").first()[0]
titled_gap = spark.sql(f"""
  SELECT count(*) FROM {W} w LEFT ANTI JOIN {DST} d ON d.work_id = w.id
  WHERE w.title IS NOT NULL AND length(trim(w.title)) > 0
""").first()[0]
print(f"table rows {rows:,}; titled works without a vector {titled_gap:,}; ≈${(st.n - st.bad)*3.4/1e6:,.2f} endpoint")
if failed:
    raise RuntimeError(f"{len(failed)} chunks failed: {failed}. Re-run the task; it only redoes what is missing or changed.")
dbutils.notebook.exit(json.dumps({"candidates": n_cand, "by_reason": {r.reason: r.n for r in stats}, "embedded": st.n - st.bad,
                                  "no_vector": st.bad, "gone_deleted": gone, "rows": rows, "titled_gap": titled_gap,
                                  "minutes": round((time.time() - t0) / 60, 1)}))
