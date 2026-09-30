# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords nightly 4/4: append the new works (oxjob #1322)
# MAGIC
# MAGIC Checks `<state_prefix>append_rows` (one row per work, none already in the target, no NULLs, the served struct type, every
# MAGIC keyword id + name in `openalex.common.keywords_v2`, every work in this build's queue) and only then INSERTs the new
# MAGIC work_ids into `target` (append-only, the #1312 rule; rows already there are never touched). Any failed check fails the task
# MAGIC and appends nothing. After the append, every work of the build (with or without keywords) goes into the tagged-ids record
# MAGIC `<state_prefix>tagged`, so it is never queued again, and one line goes into `<state_prefix>runs`.
# MAGIC The next End 2 End merges the appended keywords into `openalex_works` (one re-stamp per new work).

# COMMAND ----------

import datetime as dt
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "keywords_nightly.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "keywords_nightly.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import keywords_nightly as kn  # noqa: E402

for name, default in [("state_prefix", "openalex.works.work_keywords_v2_"), ("target", "openalex.works.work_keywords_v2"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW, ROWS, TAGGED, RUNS = f"{P}queue", f"{P}raw", f"{P}append_rows", f"{P}tagged", f"{P}runs"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def task_value(task, key, default):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task, key=key, default=default, debugValue=default)
    except Exception:
        return default

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (build_id STRING, queued BIGINT, tagged BIGINT, appended BIGINT, no_keywords BIGINT,
  assignments BIGINT, usd_est DOUBLE, modal_wall_s DOUBLE, target STRING, finished_at TIMESTAMP)
  COMMENT 'Keywords nightly (oxjob #1322): one row per finished run'""")
q = spark.sql(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}").collect()[0]
n, build_id = int(q.n), q.b
if DRY:
    dbutils.notebook.exit("dry run")
if n == 0:
    spark.sql(f"INSERT INTO {RUNS} VALUES (NULL, 0, 0, 0, 0, 0, 0, 0, '{TARGET}', current_timestamp())")
    dbutils.notebook.exit("empty queue: nothing to append")
tagged_n = spark.sql(f"SELECT count(*) AS n FROM {RAW} WHERE build_id = '{build_id}'").collect()[0].n
if tagged_n != n:
    raise RuntimeError(f"{RAW} holds {tagged_n:,} rows for build {build_id}, queue {n:,}")

# COMMAND ----------

res = {k: spark.sql(sql).collect()[0].asDict() for k, sql in kn.check_statements(ROWS, TARGET, QUEUE).items()}
print(json.dumps(res, indent=1, default=str))
ok, bad = kn.checks_pass(res)
if not ok:
    raise RuntimeError("pre-append checks failed, NOTHING appended: " + "; ".join(bad))
rows_n = int(res["shape"]["works"])
log(f"checks pass: {rows_n:,} rows for build {build_id}")

# COMMAND ----------

before = spark.table(TARGET).count()
spark.sql(f"""INSERT INTO {TARGET} (work_id, keywords, tagger_version, updated_at)
SELECT r.work_id, r.keywords, r.tagger_version, current_timestamp() FROM {ROWS} r LEFT ANTI JOIN {TARGET} t ON t.work_id = r.work_id""")
after = spark.table(TARGET).count()
log(f"appended {after - before:,} rows to {TARGET} ({before:,} -> {after:,})")
if after - before != rows_n:
    raise RuntimeError(f"expected {rows_n:,} new rows in {TARGET}, got {after - before:,} (another writer?)")

spark.sql(f"""INSERT INTO {TAGGED} (work_id, tagger_version, tagged_at, run)
SELECT DISTINCT CAST(substr(r.id, 2) AS BIGINT), r.tagger_version, r.tagged_at, r.build_id FROM {RAW} r
LEFT ANTI JOIN {TAGGED} t ON t.work_id = CAST(substr(r.id, 2) AS BIGINT) WHERE r.build_id = '{build_id}'""")
a = spark.sql(f"SELECT sum(size(keywords)) AS s FROM {ROWS}").collect()[0].s or 0
spark.sql(f"""INSERT INTO {RUNS} VALUES ('{build_id}', {n}, {tagged_n}, {after - before}, {n - rows_n}, {a},
  {float(task_value('tag', 'usd_est', 0.0))}, {float(task_value('tag', 'modal_wall_s', 0.0))}, '{TARGET}', current_timestamp())""")
log(f"build {build_id}: {n:,} queued, {rows_n:,} appended, {n - rows_n:,} with no keywords, recorded in {TAGGED}")
