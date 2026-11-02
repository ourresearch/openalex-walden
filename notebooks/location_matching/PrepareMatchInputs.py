# Databricks notebook source
# MAGIC %md
# MAGIC # Map_Work_Ids 2/3, the location matcher: inputs (oxjob #1668)
# MAGIC
# MAGIC Runs after `match_locations_by_identifier`, before any pin is written. The night's frame = the first-sight records that step
# MAGIC left without a work (`sql/frame.sql`, from `location_work_ids_verdicts`); seeds and their records (`sql/seeds.sql`, the
# MAGIC records' latest `locations_w_types` rows). V = the latest `openalex_works` version: the works as tonight's records find them.
# MAGIC `go = false` when the frame is empty (the model steps are skipped; `assign_work_ids` still runs). The work keys, embeddings
# MAGIC and candidates are their own tasks, so the candidate lookup runs alongside the GPU search.

# COMMAND ----------

import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.getcwd())
import lwm  # noqa: E402

for name, default in [("mode", "live"), ("verdicts", "openalex.works.location_work_ids_verdicts"),
                      ("locations", "openalex.works.locations_w_types")]:
    dbutils.widgets.text(name, default)
NIGHT = dt.datetime.utcnow().strftime("%Y-%m-%d")
MODE = dbutils.widgets.get("mode").strip()
VERDICTS = dbutils.widgets.get("verdicts").strip()
LOCATIONS = dbutils.widgets.get("locations").strip()
T = "openalex.works.location_work_match_"
T0 = time.time()


log = lwm.log

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T}runs (night DATE, mode STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
  lwi_new BIGINT, lwi_old BIGINT, works_version BIGINT, records BIGINT, with_candidates BIGINT, to_qwen BIGINT, qwen_ok BOOLEAN,
  qwen_usd_est DOUBLE, model_binds BIGINT, nightly_binds BIGINT, same_bind BIGINT, both_mint BIGINT, model_binds_nightly_mints BIGINT,
  model_mints_nightly_binds BIGINT, other_work BIGINT, seconds MAP<STRING, DOUBLE>, notes STRING, qwen_over_cap BIGINT) CLUSTER BY (night)
  COMMENT 'Match Locations To Works (oxjob #1668): one row per night and mode, written by the job (status running -> done)'""")

v = spark.sql("SELECT max(version) AS v FROM (DESCRIBE HISTORY openalex.works.openalex_works)").collect()[0].v
pins = {"V": v, "VERDICTS": VERDICTS, "LOCATIONS": LOCATIONS, "NIGHT": NIGHT}
log(f"night {NIGHT}: {VERDICTS}, openalex_works {v}")
spark.sql(f"DELETE FROM {T}runs WHERE night = DATE'{NIGHT}' AND mode = '{MODE}'")
spark.sql(f"""INSERT INTO {T}runs (night, mode, status, started_at, works_version)
  VALUES (DATE'{NIGHT}', '{MODE}', 'running', current_timestamp(), {v})""")
for f in ("sql/frame.sql", "sql/seeds.sql"):
    lwm.run_sql_file(f, pins)
seeds, found = spark.sql(f"""SELECT count(*) AS n, count(r.native_id) AS found FROM {T}seeds s
  LEFT JOIN (SELECT DISTINCT provenance, native_id_namespace, native_id FROM {T}records) r
  USING (provenance, native_id_namespace, native_id)""").collect()[0]
go = seeds > 0
log(f"{seeds:,} records for the location matcher, {found:,} with their record")
# Tripwire: every seed comes from locations_w_types, so a seed without its record means the inputs are broken (the matcher
# would find no candidates and call it a new work)
if seeds - found > 0.01 * seeds:
    raise RuntimeError(f"{seeds - found:,} of {seeds:,} records for the location matcher have no record row")

dbutils.jobs.taskValues.set(key="V", value=int(v))
dbutils.jobs.taskValues.set(key="go", value="true" if go else "false")
dbutils.jobs.taskValues.set(key="night", value=NIGHT)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit("go" if go else "nothing to decide")
