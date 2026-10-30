# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 1/6: pins and inputs (oxjob #1668)
# MAGIC
# MAGIC Shadow mode: runs after the nightly's MapWorkIds and never writes `location_work_ids` or `work_id_map`.
# MAGIC
# MAGIC Pins the night from Delta history: NEW = the night's first `location_work_ids` MERGE (MapWorkIds), OLD = the MERGE before
# MAGIC it, V = the last `openalex_works` version before NEW (the works as the records found them). Exits with `go = false` while an
# MAGIC End 2 End run is in progress (Casey 2026-10-30: the shadow never runs during one; the job tries hourly), when the night
# MAGIC has no MERGE yet, or when it was already decided (unless `force`). Then builds `sql/frame.sql` (the anchors that reached the
# MAGIC title tier or minted / parked) and `sql/seeds.sql` on PROD-WH. The work keys, embeddings and candidates are their own tasks,
# MAGIC so the candidate lookup runs alongside the GPU search.

# COMMAND ----------

import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.getcwd())
import lwm  # noqa: E402

for name, default in [("night", ""), ("mode", "shadow"), ("force", "false")]:
    dbutils.widgets.text(name, default)
NIGHT = dbutils.widgets.get("night").strip() or dt.datetime.utcnow().strftime("%Y-%m-%d")
MODE = dbutils.widgets.get("mode").strip()
FORCE = dbutils.widgets.get("force").strip().lower() == "true"
T = "openalex.works.location_work_match_"
END2END_JOB = 616701029470182
T0 = time.time()


log = lwm.log

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T}runs (night DATE, mode STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
  lwi_new BIGINT, lwi_old BIGINT, works_version BIGINT, records BIGINT, with_candidates BIGINT, to_qwen BIGINT, qwen_ok BOOLEAN,
  qwen_usd_est DOUBLE, model_binds BIGINT, nightly_binds BIGINT, same_bind BIGINT, both_mint BIGINT, model_binds_nightly_mints BIGINT,
  model_mints_nightly_binds BIGINT, other_work BIGINT, seconds MAP<STRING, DOUBLE>, notes STRING, qwen_over_cap BIGINT) CLUSTER BY (night)
  COMMENT 'Match Locations To Works (oxjob #1668): one row per night and mode, written by the job (status running -> done)'""")

merges = spark.sql("""SELECT version, timestamp FROM (DESCRIBE HISTORY openalex.works.location_work_ids)
  WHERE operation = 'MERGE' ORDER BY version""").collect()
night_merges = [m for m in merges if m.timestamp.strftime("%Y-%m-%d") == NIGHT]
end2end = list(lwm.client().jobs.list_runs(job_id=END2END_JOB, active_only=True))
go = bool(night_merges) and not end2end
if end2end:
    log(f"End 2 End run {end2end[0].run_id} is in progress: the shadow waits for it to finish")
elif not go:
    log(f"no location_work_ids MERGE on {NIGHT} yet: the nightly has not reached MapWorkIds")
else:
    new = night_merges[0]
    old = max((m for m in merges if m.version < new.version), key=lambda m: m.version)
    v = spark.sql(f"""SELECT max(version) AS v FROM (DESCRIBE HISTORY openalex.works.openalex_works)
      WHERE timestamp < '{new.timestamp.strftime('%Y-%m-%d %H:%M:%S')}'""").collect()[0].v
    done = spark.sql(f"SELECT count(*) AS n FROM {T}runs WHERE night = DATE'{NIGHT}' AND mode = '{MODE}' AND status = 'done'").collect()[0].n
    if done and not FORCE:
        go = False
        log(f"{NIGHT} ({MODE}) already decided; force = true to redo it")
    else:
        pins = {"NEW": new.version, "OLD": old.version, "V": v}
        log(f"night {NIGHT}: location_work_ids {old.version} -> {new.version} ({new.timestamp} UTC), openalex_works {v}")
        spark.sql(f"DELETE FROM {T}runs WHERE night = DATE'{NIGHT}' AND mode = '{MODE}'")
        spark.sql(f"""INSERT INTO {T}runs (night, mode, status, started_at, lwi_new, lwi_old, works_version)
          VALUES (DATE'{NIGHT}', '{MODE}', 'running', current_timestamp(), {new.version}, {old.version}, {v})""")
        for f in ("sql/frame.sql", "sql/seeds.sql"):
            lwm.run_sql_file(f, pins)
        for k, val in pins.items():
            dbutils.jobs.taskValues.set(key=k, value=int(val))

dbutils.jobs.taskValues.set(key="go", value="true" if go else "false")
dbutils.jobs.taskValues.set(key="night", value=NIGHT)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit("go" if go else "nothing to do")
