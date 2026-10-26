# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 6/6: decisions (oxjob #1668)
# MAGIC
# MAGIC Runs `sql/decisions.sql` for the night into
# MAGIC `openalex.works.location_work_match_decisions`, then completes the night's row in `location_work_match_runs`: record counts,
# MAGIC binds, the comparison with the nightly, Qwen's estimated cost and every task's seconds. Fails if any record that should
# MAGIC have a Qwen score has none (no fallback, Casey 2026-10-26). Shadow mode never writes `location_work_ids` or `work_id_map`.

# COMMAND ----------

import datetime as dt
import json
import time

dbutils.widgets.text("mode", "shadow")
MODE = dbutils.widgets.get("mode").strip()
NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue="")
T = "openalex.works.location_work_match_"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


text = open("sql/decisions.sql").read()
for k, v in {"NIGHT": NIGHT, "MODE": MODE}.items():
    text = text.replace(f"__{k}__", v)
for stmt in [s.strip() for s in text.split("\n;\n") if s.strip()]:
    df = spark.sql(stmt)
for r in df.collect():
    print("   ", r.asDict())
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
for task in ("prepare", "knn", "features", "score_xgb", "score_qwen"):
    try:
        secs[task] = float(dbutils.jobs.taskValues.get(taskKey=task, key="seconds", debugValue=0))
    except Exception:
        pass
secs["write_decisions"] = round(time.time() - T0, 1)
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
