# Databricks notebook source
# MAGIC %md
# MAGIC # Publication-year resolver (oxjob #1644)
# MAGIC
# MAGIC Drains `publication_year_queue` chunk by chunk: one Jev request per work from the driver (threads, paced by the
# MAGIC shared Jev broker), prompt and parsing from `utils/publication_year.py` (the evaluated jev-v1 prompt), one append
# MAGIC per chunk into `publication_year_resolved`. Restart-safe: finished chunks are recorded in
# MAGIC `publication_year_progress` and skipped; a retried run anti-joins each chunk against the resolved table.
# MAGIC Stops at `max_works`, `max_usd` or `max_minutes`, and never starts past `max_total_usd` (spend summed over the
# MAGIC progress table). Writes only its own tables; `BuildPublicationDateFill` reads the accepted years.
# MAGIC Batch-job rules: progress every 10K works, per-chunk rate + ETA, errors to a table, checkpoint per chunk.

# COMMAND ----------

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, REPO_ROOT)
from utils import publication_year as py  # noqa: E402
from utils import study_design as sd  # noqa: E402  (JevClient, broker lease, pricing)

from pyspark.sql import functions as F  # noqa: E402

dbutils.widgets.text("schema", "openalex.works", "target schema")
dbutils.widgets.text("max_works", "100000", "stop after this many works this run")
dbutils.widgets.text("max_usd", "10", "stop when Jev spend this run reaches this")
dbutils.widgets.text("max_minutes", "60", "stop starting new chunks after this long")
dbutils.widgets.text("rps", "350", "Jev requests/s asked of the broker (account cap 400 req/s, 1.2M tok/s)")
dbutils.widgets.text("concurrency", "110", "Jev threads")
dbutils.widgets.text("max_total_usd", "900", "ceiling on Jev spend across ALL runs (progress table)")
dbutils.widgets.text("dry_run", "false", "true = report the queue and projected cost, call nothing")

SCHEMA = dbutils.widgets.get("schema").strip()
MAX_WORKS = int(dbutils.widgets.get("max_works"))
MAX_USD = float(dbutils.widgets.get("max_usd"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
RPS = float(dbutils.widgets.get("rps"))
CONCURRENCY = int(dbutils.widgets.get("concurrency"))
MAX_TOTAL_USD = float(dbutils.widgets.get("max_total_usd"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

QUEUE = f"{SCHEMA}.publication_year_queue"
RESOLVED = f"{SCHEMA}.publication_year_resolved"
ERRORS = f"{SCHEMA}.publication_year_errors"
PROGRESS = f"{SCHEMA}.publication_year_progress"

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESOLVED} (
  work_id BIGINT, year INT, conf DOUBLE, certainty STRING, how_conf DOUBLE, n_cands INT, input_tokens INT,
  prompt_version STRING, jev_model STRING, updated_at TIMESTAMP
) USING DELTA COMMENT 'oxjob #1644: Jev reading of each queued work: the item year it picked (NULL = unknown) and confidence'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {ERRORS} (
  work_id BIGINT, status INT, error STRING, attempts INT, prompt_version STRING, ts TIMESTAMP
) USING DELTA COMMENT 'oxjob #1644: Jev failures per work; the work is retried next run'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PROGRESS} (
  chunk_id BIGINT, n_queued INT, n_resolved INT, n_failed INT, input_tokens BIGINT, usd DOUBLE, seconds DOUBLE,
  prompt_version STRING, done_at TIMESTAMP
) USING DELTA COMMENT 'oxjob #1644: one row per finished queue chunk; skipped on later runs'""")

done = {int(r.chunk_id) for r in spark.sql(
    f"SELECT chunk_id FROM {PROGRESS} WHERE prompt_version = '{py.PROMPT_VERSION}'").collect()}
chunks = [(int(r.chunk_id), int(r.n)) for r in
          spark.sql(f"SELECT chunk_id, count(*) n FROM {QUEUE} GROUP BY 1 ORDER BY 1").collect()]
todo = [(c, n) for c, n in chunks if c not in done]
n_queue = sum(n for _, n in todo)
spent_before = float(spark.sql(f"SELECT coalesce(sum(usd), 0) u FROM {PROGRESS} WHERE prompt_version = "
                               f"'{py.PROMPT_VERSION}'").collect()[0].u)
print(f"queue: {len(chunks)} chunks / {sum(n for _, n in chunks):,} works; {len(todo)} chunks / {n_queue:,} works to go; "
      f"this run up to {MAX_WORKS:,} works, ${MAX_USD:,.0f}, {MAX_MINUTES:.0f} min; projected "
      f"${min(MAX_WORKS, n_queue) * 1100 * sd.JEV_USD_PER_TOKEN:,.2f} at ~1,100 tokens/work; spent so far ${spent_before:,.2f} "
      f"of max_total_usd ${MAX_TOTAL_USD:,.0f}; prompt {py.PROMPT_VERSION}")
if spent_before >= MAX_TOTAL_USD:
    raise RuntimeError(f"Jev spend ${spent_before:,.2f} is at the max_total_usd ceiling; raise it deliberately")
if DRY_RUN:
    dbutils.notebook.exit("dry run")

# COMMAND ----------

client = sd.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=CONCURRENCY, rps=RPS,
                      broker_token=sd.broker_token(dbutils), cls="backfill", job="publication_year")
run_t0 = time.time()
tot_queued = tot_ok = tot_failed = 0
ROW_DDL = ("work_id BIGINT, year INT, conf DOUBLE, certainty STRING, how_conf DOUBLE, n_cands INT, input_tokens INT, "
           "prompt_version STRING, jev_model STRING")

for chunk_id, n_chunk in todo:
    stop = (tot_queued >= MAX_WORKS and "max_works") or (client.usd >= MAX_USD and "max_usd") or \
           (spent_before + client.usd >= MAX_TOTAL_USD and "max_total_usd") or \
           ((time.time() - run_t0) / 60 >= MAX_MINUTES and "max_minutes")
    if stop:
        print(f"stop: {stop} reached (run ${client.usd:.2f}, {tot_queued:,} works)")
        break
    c_t0 = time.time()
    ids = spark.table(QUEUE).where(F.col("chunk_id") == chunk_id).select("work_id")
    already = ids.join(spark.table(RESOLVED).where(F.col("prompt_version") == py.PROMPT_VERSION), "work_id", "left_semi")
    todo_df = spark.table(QUEUE).where(F.col("chunk_id") == chunk_id).join(already, "work_id", "left_anti")
    works = [r.asDict(recursive=True) for r in todo_df.select("work_id", "title", "type", "records").collect()]
    truncated = len(works) + tot_queued > MAX_WORKS
    if truncated:
        works = works[: MAX_WORKS - tot_queued]
    rows, errs, seen, lock = [], [], [0], threading.Lock()

    def one(w):
        req = py.request(w, w["records"] or [])
        if req is None:
            out = (int(w["work_id"]), None, None, "none", None, 0, 0, py.PROMPT_VERSION, py.JEV_MODEL), None
        else:
            state, qs, n_cands = req
            r = client.decide(state, qs)
            if r.get("ok"):
                p = py.parse(r.get("answers") or {})
                out = (int(w["work_id"]), p["year"], p["conf"], p["certainty"], p["how_conf"], n_cands,
                       int(r.get("input_tokens") or 0), py.PROMPT_VERSION, r.get("model") or py.JEV_MODEL), None
            else:
                out = None, (int(w["work_id"]), int(r.get("status") or -2), str(r.get("error"))[:300],
                             int(r.get("attempts") or 0))
        with lock:
            seen[0] += 1
            if seen[0] % 10000 == 0:
                el = time.time() - c_t0
                print(f"  chunk {chunk_id}: {seen[0]:,}/{len(works):,} ({seen[0] / el:.0f}/s) ok={client.n_ok:,} "
                      f"fail={client.n_fail:,} retries={client.n_retry:,} ${client.usd:.2f}", flush=True)
        return out

    with ThreadPoolExecutor(CONCURRENCY) as ex:
        for row, err in ex.map(one, works):
            (rows.append(row) if row else errs.append(err))
    if rows:
        spark.createDataFrame(rows, ROW_DDL).withColumn("updated_at", F.current_timestamp()) \
            .write.format("delta").mode("append").saveAsTable(RESOLVED)
    if errs:
        spark.createDataFrame(errs, "work_id BIGINT, status INT, error STRING, attempts INT") \
            .withColumn("prompt_version", F.lit(py.PROMPT_VERSION)).withColumn("ts", F.current_timestamp()) \
            .write.format("delta").mode("append").saveAsTable(ERRORS)
    tok = sum(r[6] for r in rows)
    usd = tok * sd.JEV_USD_PER_TOKEN
    secs = time.time() - c_t0
    complete = (not truncated) and len(errs) <= 0.02 * max(1, len(works))
    if complete:
        spark.createDataFrame([(chunk_id, n_chunk, len(rows), len(errs), tok, usd, secs, py.PROMPT_VERSION)],
                              "chunk_id BIGINT, n_queued INT, n_resolved INT, n_failed INT, input_tokens BIGINT, usd DOUBLE, "
                              "seconds DOUBLE, prompt_version STRING") \
            .withColumn("done_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(PROGRESS)
    tot_queued += len(works); tot_ok += len(rows); tot_failed += len(errs)
    el = time.time() - run_t0
    rate = tot_queued / max(el, 1)
    n_year = sum(1 for r in rows if r[1] is not None and (r[2] or 0) >= py.ACCEPT_CONF)
    print(f"chunk {chunk_id}: {len(rows):,} resolved ({n_year:,} accepted), {len(errs):,} failed, "
          f"{tok / max(1, len(rows)):.0f} tok/work, ${usd:.2f}, {secs:.0f}s ({len(works) / max(secs, 1):.0f}/s) -> "
          f"{'done' if complete else 'NOT marked done (retried next run)'}. run: {tot_queued:,} works, ${client.usd:.2f}, "
          f"{rate:.0f}/s, ETA {(min(MAX_WORKS, n_queue) - tot_queued) / max(rate, 1) / 60:.0f} min", flush=True)
    if client.out_of_credit:
        raise RuntimeError(f"Jev out of credit ({client.out_of_credit}); unresolved works stay queued")
    if len(errs) > 0.5 * max(1, len(works)):
        raise RuntimeError(f"chunk {chunk_id}: {len(errs)} of {len(works)} calls failed; Jev down or key invalid?")

print(f"run done: {tot_queued:,} works, {tot_ok:,} resolved, {tot_failed:,} failed, ${client.usd:.2f}, "
      f"{(time.time() - run_t0) / 60:.0f} min")
