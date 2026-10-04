# Databricks notebook source
# MAGIC %md
# MAGIC # Topics nightly 1/3: build the scorer queue (oxjob #1531)
# MAGIC
# MAGIC Runs inside End 2 End (job `Topics`, after `Works_Base`, before `Author_Matching`), so tonight's new works get topics the same
# MAGIC night. Queue = every work in `openalex_works_base` with a title or an abstract and no row in the served table `target`
# MAGIC (`work_topics_v2`, which keeps a row for not-classifiable works too, so nothing is re-scored), newest first (highest id), at
# MAGIC most `max_works`; the rest waits for the next night. Fields are the corpus pass's: title, abstract cut to 4,000 characters, venue =
# MAGIC primary location's source name. Shards `pmod(hash(work_id), n)` with n = ceil(works / shard_works).
# MAGIC
# MAGIC A retry of the whole job (End 2 End retries `Topics` twice) reuses the last queue when its build never finished
# MAGIC (no row in `<state_prefix>runs`) and is younger than `reuse_hours`, so it resumes the same Modal call instead of scoring twice.
# MAGIC `force_ids_table` (tests / backfills): queue exactly the works in that table (column `id`) that are not yet served.

# COMMAND ----------

import datetime as dt
import math
import time

for name, default in [("state_prefix", "openalex.works.work_topics_v2_"), ("target", "openalex.works.work_topics_v2"),
                      ("works_table", "openalex.works.openalex_works_base"), ("max_works", "3000000"), ("shard_works", "25000"),
                      ("force_ids_table", ""), ("reuse_hours", "20"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
WORKS = dbutils.widgets.get("works_table").strip()
MAX_WORKS = int(dbutils.widgets.get("max_works"))
SHARD_WORKS = int(dbutils.widgets.get("shard_works"))
FORCE = dbutils.widgets.get("force_ids_table").strip()
REUSE_H = float(dbutils.widgets.get("reuse_hours"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RUNS = f"{P}queue", f"{P}runs"
for t in (P + "x", TARGET, WORKS) + ((FORCE,) if FORCE else ()):
    assert t.startswith("openalex.") and t.replace(".", "").replace("_", "").isalnum(), f"bad table name {t!r}"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

# COMMAND ----------

# The served table must exist (the anti-join is what keeps already-scored works out); never created here.
if not spark.catalog.tableExists(TARGET):
    raise RuntimeError(f"{TARGET} does not exist: create it first (oxjob #1531 PLAN § 2b); refusing to queue the whole corpus")
spark.sql(f"SELECT 1 FROM {TARGET} LIMIT 1").collect()

# Resume an unfinished build (task or job retry) instead of re-queueing under a new build id.
if spark.catalog.tableExists(QUEUE) and not DRY:
    prev = spark.sql(f"SELECT max(build_id) AS b, count(*) AS n, max(queued_at) AS at FROM {QUEUE}").collect()[0]
    finished = spark.catalog.tableExists(RUNS) and spark.sql(f"SELECT count(*) AS n FROM {RUNS} WHERE build_id = '{prev.b}'").collect()[0].n > 0
    age_h = (dt.datetime.utcnow() - prev.at).total_seconds() / 3600 if prev.at else 1e9
    if prev.n and not finished and age_h < REUSE_H and spark.sql(f"SELECT count(*) AS n FROM {QUEUE} q JOIN {TARGET} t ON t.work_id = q.work_id").collect()[0].n == 0:
        log(f"reusing unfinished build {prev.b} ({prev.n:,} works, queued {age_h:.1f} h ago)")
        dbutils.jobs.taskValues.set(key="build_id", value=prev.b)
        dbutils.jobs.taskValues.set(key="queued", value=int(prev.n))
        dbutils.notebook.exit(f"reused build {prev.b}")

# COMMAND ----------

scorable = "(length(trim(w.title)) > 0 OR length(w.abstract) > 0)"   # = the corpus export filter
cand = f"""SELECT w.id AS work_id, w.title, w.abstract, w.primary_location.source.display_name AS venue FROM {WORKS} w
  LEFT ANTI JOIN {TARGET} v ON v.work_id = w.id
  WHERE {scorable}""" + (f" AND w.id IN (SELECT id FROM {FORCE})" if FORCE else "")
n = spark.sql(f"SELECT count(*) AS n FROM ({cand})").collect()[0].n
take = min(n, MAX_WORKS)
n_shards = max(1, math.ceil(take / SHARD_WORKS))
build_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
log(f"unscored works: {n:,}" + (f" (forced from {FORCE})" if FORCE else "") + f"; queueing {take:,} in {n_shards} shards; build {build_id}")
if n > MAX_WORKS:
    log(f"over max_works: the newest {MAX_WORKS:,} go now, {n - MAX_WORKS:,} wait for the next run")
if DRY:
    dbutils.notebook.exit(f"dry run: would queue {take:,}")

spark.sql(f"""
CREATE OR REPLACE TABLE {QUEUE} CLUSTER BY (shard)
COMMENT 'Topics nightly queue (oxjob #1531): scorable works with no row in {TARGET}; rebuilt every run by BuildTopicQueue; exported once by the Modal app openalex-topics-nightly'
AS SELECT CAST(pmod(hash(c.work_id), {n_shards}) AS INT) AS shard, c.work_id, c.title, substr(c.abstract, 1, 4000) AS abstract, c.venue,
          '{build_id}' AS build_id, current_timestamp() AS queued_at
FROM ({cand}) c
QUALIFY row_number() OVER (ORDER BY c.work_id DESC) <= {MAX_WORKS}
""")
q = spark.sql(f"SELECT count(*) AS n, count(DISTINCT work_id) AS ids, count(DISTINCT shard) AS shards, min(work_id) AS lo, max(work_id) AS hi FROM {QUEUE}").collect()[0]
if q.n != take or q.ids != take:
    raise RuntimeError(f"queue holds {q.n:,} rows / {q.ids:,} ids, expected {take:,}")
log(f"queue {QUEUE}: {q.n:,} works, {q.shards} shards, ids W{q.lo} .. W{q.hi}")
dbutils.jobs.taskValues.set(key="build_id", value=build_id)
dbutils.jobs.taskValues.set(key="queued", value=int(q.n))
