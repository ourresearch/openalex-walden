# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 6/6: decisions (oxjob #1668)
# MAGIC
# MAGIC Runs `sql/decisions.sql` for the night into
# MAGIC `openalex.works.location_work_match_decisions`, then completes the night's row in `location_work_match_runs`: record counts,
# MAGIC binds, the comparison with the nightly, Qwen's estimated cost, every task's seconds and the critical path (the longest chain
# MAGIC of task execution times: no cluster start-up or queueing, the time the matcher would add to the nightly). Fails if any record that should
# MAGIC have a Qwen score has none (no fallback, Casey 2026-10-26). Shadow mode never writes `location_work_ids` or `work_id_map`.

# COMMAND ----------

import datetime as dt
import json
import os
import sys
import time

sys.path.insert(0, os.getcwd())
import lwm  # noqa: E402

dbutils.widgets.text("mode", "shadow")
dbutils.widgets.text("run_id", "")
MODE = dbutils.widgets.get("mode").strip()
NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue="")
T = "openalex.works.location_work_match_"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


lwm.run_sql_file("sql/decisions.sql", {"NIGHT": NIGHT, "MODE": MODE})
missing = spark.sql(f"SELECT count(*) AS n FROM {T}decisions WHERE night = DATE'{NIGHT}' AND mode = '{MODE}' AND layer = 'qwen_missing'").collect()[0].n
if missing:
    raise RuntimeError(f"{missing:,} records at >= 0.01 have no Qwen score: the night is not decided")

# COMMAND ----------

s = spark.sql(f"""SELECT count(*) AS records, count(xgb_work) AS with_candidates, count_if(xgb_p >= 0.01) AS to_qwen,
    count(model_work) AS model_binds, count_if(nightly_decision = 'title_bind') AS nightly_binds,
    count_if(cmp = 'same_bind') AS same_bind, count_if(cmp = 'both_mint') AS both_mint, count_if(cmp = 'model_binds') AS model_binds_nightly_mints,
    count_if(cmp = 'model_mints') AS model_mints_nightly_binds, count_if(cmp = 'other_work') AS other_work
  FROM {T}decisions WHERE night = DATE'{NIGHT}' AND mode = '{MODE}'""").collect()[0].asDict()
secs = {}
for task in ("prepare", "keys", "embed", "candidates", "knn", "features", "score_xgb", "score_qwen"):
    try:
        secs[task] = float(dbutils.jobs.taskValues.get(taskKey=task, key="seconds", debugValue=0))
    except Exception:
        pass
secs["write_decisions"] = round(time.time() - T0, 1)
run_id = dbutils.widgets.get("run_id").strip()
if run_id.isdigit():   # critical path over the job's task graph, execution time only (no cluster start-up, no queueing)
    tasks = {t.task_key: t for t in lwm.client().jobs.get_run(int(run_id)).tasks}
    dur = {k: (t.execution_duration or 0) / 1000 for k, t in tasks.items()}
    dur["write_decisions"] = secs["write_decisions"]
    memo = {}

    def longest(k):
        if k not in memo:
            deps = [d.task_key for d in (tasks[k].depends_on or []) if d.task_key in tasks]
            memo[k] = dur.get(k, 0) + max((longest(d) for d in deps), default=0)
        return memo[k]
    secs["critical_path"] = round(max(longest(k) for k in tasks), 1)
    if "knn" in tasks:
        secs["knn_cluster_start"] = round((tasks["knn"].setup_duration or 0) / 1000, 1)
usd = float(dbutils.jobs.taskValues.get(taskKey="score_qwen", key="qwen_usd_est", debugValue=0.0))
note = dbutils.jobs.taskValues.get(taskKey="score_qwen", key="qwen_note", debugValue="")
smap = ", ".join(f"'{k}', CAST({v} AS DOUBLE)" for k, v in secs.items())
spark.sql(f"""UPDATE {T}runs SET status = 'done', finished_at = current_timestamp(),
    records = {s['records']}, with_candidates = {s['with_candidates']}, to_qwen = {s['to_qwen']}, qwen_ok = true,
    qwen_usd_est = {usd}, model_binds = {s['model_binds']}, nightly_binds = {s['nightly_binds']}, same_bind = {s['same_bind']},
    both_mint = {s['both_mint']}, model_binds_nightly_mints = {s['model_binds_nightly_mints']},
    model_mints_nightly_binds = {s['model_mints_nightly_binds']}, other_work = {s['other_work']}, seconds = map({smap}),
    notes = :note
  WHERE night = DATE'{NIGHT}' AND mode = '{MODE}'""", args={"note": note})
summary = {"night": NIGHT, "mode": MODE, **s, "qwen_usd_est": usd, "seconds": secs}
log(json.dumps(summary))
dbutils.notebook.exit(json.dumps(summary))
