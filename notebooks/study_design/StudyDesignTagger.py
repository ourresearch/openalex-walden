# Databricks notebook source
# MAGIC %md
# MAGIC # Study-design tagger (oxjob #1312)
# MAGIC
# MAGIC Drains `works_study_design_queue` chunk by chunk in priority order: one Jev
# MAGIC request per work from the driver (threads + requests, paced on requests/s),
# MAGIC derived scores + code gates + thresholds from `utils/study_design.py`, one
# MAGIC append per chunk into `works_study_design_tagger`. Restart-safe: finished
# MAGIC chunks are recorded in `works_study_design_progress`; a retried run skips
# MAGIC them and anti-joins the first unfinished chunk against the tagger table.
# MAGIC
# MAGIC Stops at `max_works`, `max_usd` or `max_minutes`, whichever first. The
# MAGIC nightly run uses small caps; the backfill is the same notebook with big
# MAGIC ones (docs/study_design.md).
# MAGIC
# MAGIC Batch-job rules: progress every 10K works, per-chunk rate + ETA, errors to
# MAGIC a table, no monolithic writes, checkpoint per chunk.

# COMMAND ----------

import os
import sys
import threading
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "study_design.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "study_design.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, BooleanType, FloatType, IntegerType, LongType, MapType,  # noqa: E402
                               StringType, StructField, StructType)

dbutils.widgets.text("schema", "openalex.works", "target schema")
dbutils.widgets.text("max_works", "300000", "stop after this many works this run")
dbutils.widgets.text("max_usd", "30", "stop when Jev spend this run reaches this")
dbutils.widgets.text("max_minutes", "150", "stop starting new chunks after this long")
dbutils.widgets.text("rps", "250", "Jev requests/s cap (account cap 400 req/s, 1.2M tok/s)")
dbutils.widgets.text("concurrency", "64", "Jev threads")
dbutils.widgets.text("dry_run", "false", "true = report the queue and projected cost, call nothing")

SCHEMA = dbutils.widgets.get("schema").strip()
MAX_WORKS = int(dbutils.widgets.get("max_works"))
MAX_USD = float(dbutils.widgets.get("max_usd"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
RPS = float(dbutils.widgets.get("rps"))
CONCURRENCY = int(dbutils.widgets.get("concurrency"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

QUEUE = f"{SCHEMA}.works_study_design_queue"
TAGGER = f"{SCHEMA}.works_study_design_tagger"
ERRORS = f"{SCHEMA}.works_study_design_errors"
PROGRESS = f"{SCHEMA}.works_study_design_progress"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS} (
  work_id BIGINT, status INT, error STRING, attempts INT, tagger_version STRING, build_id STRING, ts TIMESTAMP
) USING DELTA COMMENT 'Jev failures per work (oxjob #1312); the work stays in the queue and is retried next run'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PROGRESS} (
  build_id STRING, chunk_id BIGINT, n_queued INT, n_tagged INT, n_failed INT, input_tokens BIGINT, usd DOUBLE,
  seconds DOUBLE, tagger_version STRING, done_at TIMESTAMP
) USING DELTA COMMENT 'One row per finished queue chunk (oxjob #1312); the tagger skips chunks listed here for the same build_id'
""")

ROW_SCHEMA = StructType([
    StructField("work_id", LongType(), False),
    StructField("tagger_values", ArrayType(StringType()), True),
    StructField("scores", MapType(StringType(), FloatType()), True),
    StructField("probabilities", MapType(StringType(), FloatType()), True),
    StructField("is_rct", FloatType(), True),
    StructField("human_subjects", FloatType(), True),
    StructField("stated_random", BooleanType(), True),
    StructField("abstract_chars", IntegerType(), True),
    StructField("input_tokens", IntegerType(), True),
    StructField("tagger_version", StringType(), True),
    StructField("jev_model", StringType(), True),
])
ERR_SCHEMA = StructType([
    StructField("work_id", LongType(), False), StructField("status", IntegerType(), True),
    StructField("error", StringType(), True), StructField("attempts", IntegerType(), True),
])

# COMMAND ----------

build_id = spark.sql(f"SELECT max(build_id) AS b FROM {QUEUE}").collect()[0].b
if not build_id:
    raise RuntimeError(f"{QUEUE} is empty; run BuildStudyDesignQueue first")
done = {int(r.chunk_id) for r in spark.sql(f"SELECT chunk_id FROM {PROGRESS} WHERE build_id = '{build_id}'").collect()}
chunks = [(int(r.chunk_id), int(r.n)) for r in
          spark.sql(f"SELECT chunk_id, count(*) AS n FROM {QUEUE} GROUP BY chunk_id ORDER BY chunk_id").collect()]
todo = [(c, n) for c, n in chunks if c not in done]
n_queue = sum(n for _, n in todo)
print(f"queue build {build_id}: {len(chunks)} chunks / {sum(n for _, n in chunks):,} works; "
      f"{len(done)} chunks already done this build; {len(todo)} chunks / {n_queue:,} works to go")
print(f"this run: up to {MAX_WORKS:,} works, ${MAX_USD:,.0f}, {MAX_MINUTES:.0f} min at {RPS:.0f} req/s x {CONCURRENCY} threads; "
      f"projected ${min(MAX_WORKS, n_queue) * 1500 * sd.JEV_USD_PER_TOKEN:,.2f} at ~1,500 tokens/work; tagger_version {sd.TAGGER_VERSION}")
if DRY_RUN:
    dbutils.notebook.exit("dry run")

# COMMAND ----------

client = sd.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=CONCURRENCY, rps=RPS)
run_t0 = time.time()
tot_tagged = tot_failed = tot_queued = 0
first = True

for chunk_id, n_chunk in todo:
    if tot_queued >= MAX_WORKS:
        print(f"stop: max_works {MAX_WORKS:,} reached")
        break
    if client.usd >= MAX_USD:
        print(f"stop: max_usd ${MAX_USD:.2f} reached (${client.usd:.2f})")
        break
    if (time.time() - run_t0) / 60 >= MAX_MINUTES:
        print(f"stop: max_minutes {MAX_MINUTES:.0f} reached")
        break

    c_t0 = time.time()
    pdf = (spark.table(QUEUE).where(F.col("chunk_id") == chunk_id)
           .select("work_id", "title", "venue", "abstract").toPandas())
    works = pdf.where(pdf.notna(), None).to_dict("records")
    del pdf
    n_already = 0
    if first:
        # Only the first unfinished chunk can be partially tagged (a retried run); anti-join it once.
        ids_df = spark.createDataFrame([(int(w["work_id"]),) for w in works], "work_id BIGINT")
        already = {int(r.work_id) for r in spark.table(TAGGER).where(F.col("tagger_version") == sd.TAGGER_VERSION)
                   .join(ids_df, "work_id", "left_semi").select("work_id").collect()}
        if already:
            works = [w for w in works if int(w["work_id"]) not in already]
            n_already = len(already)
            print(f"chunk {chunk_id}: {n_already:,} works already tagged this version, skipping them")
        first = False
    truncated = False
    if len(works) + tot_queued > MAX_WORKS:
        works = works[: MAX_WORKS - tot_queued]
        truncated = True

    rows, errs = [], []
    seen = [0]
    lock = threading.Lock()

    def on_result(w, r):
        with lock:
            seen[0] += 1
            k = seen[0]
        if k % 10000 == 0:
            el = time.time() - c_t0
            print(f"  chunk {chunk_id}: {k:,}/{len(works):,} ({k / el:.0f}/s) ok={client.n_ok:,} fail={client.n_fail:,} "
                  f"retries={client.n_retry:,} ${client.usd:.2f}", flush=True)

    results = client.tag_many(works, on_result=on_result)
    for w, r in results:
        row = sd.answer_row(w, r) if r.get("ok") else None
        if row is None:
            errs.append((int(w["work_id"]), int(r.get("status") or -2), str(r.get("error") or "malformed answer")[:300],
                         int(r.get("attempts") or 0)))
        else:
            rows.append(row)

    if rows:
        (spark.createDataFrame(rows, ROW_SCHEMA).withColumn("updated_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(TAGGER))
    if errs:
        (spark.createDataFrame(errs, ERR_SCHEMA)
         .withColumn("tagger_version", F.lit(sd.TAGGER_VERSION)).withColumn("build_id", F.lit(build_id))
         .withColumn("ts", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(ERRORS))
    chunk_tokens = sum(r["input_tokens"] for r in rows)
    chunk_usd = chunk_tokens * sd.JEV_USD_PER_TOKEN
    secs = time.time() - c_t0
    # A chunk counts as done only if every queued work was attempted (not cut by max_works) and <= 2% of calls failed;
    # otherwise the next run re-reads it (the anti-join skips the works that did get tagged).
    complete = (not truncated) and len(errs) <= 0.02 * max(1, len(works))
    if complete:
        spark.createDataFrame([(build_id, chunk_id, n_chunk, len(rows), len(errs), chunk_tokens, chunk_usd, secs,
                                sd.TAGGER_VERSION)],
                              "build_id STRING, chunk_id BIGINT, n_queued INT, n_tagged INT, n_failed INT, input_tokens BIGINT, "
                              "usd DOUBLE, seconds DOUBLE, tagger_version STRING") \
            .withColumn("done_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(PROGRESS)
        marked = "done"
    else:
        marked = "NOT marked done (partial or too many failures; retried next run)"
    tot_tagged += len(rows); tot_failed += len(errs); tot_queued += len(works)
    el = time.time() - run_t0
    rate = tot_queued / max(el, 1)
    remaining = min(MAX_WORKS, n_queue) - tot_queued
    print(f"chunk {chunk_id}: {len(rows):,} tagged, {len(errs):,} failed, {chunk_tokens / max(1, len(rows)):.0f} tok/work, "
          f"${chunk_usd:.2f}, {secs:.0f}s ({len(works) / max(secs, 1):.0f}/s) -> {marked}. "
          f"run: {tot_queued:,} works, ${client.usd:.2f}, {rate:.0f}/s, ETA {remaining / max(rate, 1) / 60:.0f} min", flush=True)
    if len(errs) > 0.5 * max(1, len(works)):
        raise RuntimeError(f"chunk {chunk_id}: {len(errs)} of {len(works)} calls failed; Jev down or key invalid?")

print(f"run done: {tot_tagged:,} tagged, {tot_failed:,} failed, {client.total_tokens:,} tokens, ${client.usd:.2f}, "
      f"{(time.time() - run_t0) / 60:.0f} min; {n_queue - tot_queued:,} works left in the queue")

# COMMAND ----------

display(spark.sql(f"""
SELECT v AS value, count(*) AS works
FROM {TAGGER} LATERAL VIEW explode(tagger_values) AS v
WHERE tagger_version = '{sd.TAGGER_VERSION}'
GROUP BY v ORDER BY works DESC
"""))
