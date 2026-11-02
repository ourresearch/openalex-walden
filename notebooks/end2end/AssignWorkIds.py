# Databricks notebook source
# MAGIC %md
# MAGIC # Map_Work_Ids 3/3: assign work ids (oxjob #764, #1668)
# MAGIC
# MAGIC Turns the night's answers in `location_work_ids_verdicts` into pins: the identifier matches from
# MAGIC `match_locations_by_identifier`, and for the first-sight records the location matcher decided, its answer from
# MAGIC `location_work_match_decisions` (an existing work, or a new one). Writes `work_id_map` (aliases born pointing at the right
# MAGIC work, new works, enrichment), `location_work_ids` (insert-only) and `location_work_ids_run_stats`; the only writer of the
# MAGIC registry and the map in the nightly. Fails when a record the matcher was given has no decision (no fallback, Casey
# MAGIC 2026-11-01). SQL: `assign_work_ids.sql` + `work_id_lookups.sql`, on PROD-WH. The widgets name the tables so the step can
# MAGIC run against copies; End 2 End uses the defaults.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "..", "location_matching"))
import lwm  # noqa: E402

DEFAULTS = {"REG": "openalex.works.location_work_ids", "MAP": "openalex.works.work_id_map",
            "PENDING": "openalex.works.location_work_ids_pending", "VERDICTS": "openalex.works.location_work_ids_verdicts",
            "MERGED": "openalex.works.merged_work_ids", "DELETED": "openalex.works.deleted_works",
            "RUN_STATS": "openalex.works.location_work_ids_run_stats",
            "DECISIONS": "openalex.works.location_work_match_decisions", "FRAME": "openalex.works.location_work_match_frame"}
for k, v in DEFAULTS.items():
    dbutils.widgets.text(k.lower(), v)
dbutils.widgets.text("mode", "live")
TABLES = {k: dbutils.widgets.get(k.lower()).strip() for k in DEFAULTS}
MODE = dbutils.widgets.get("mode").strip()
NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue="")
FRAMED = dbutils.jobs.taskValues.get(taskKey="prepare", key="go", debugValue="false") == "true"

# every record the matcher was given has its decision, or the night is not decided
if FRAMED:
    missing = lwm.run_statement(f"""SELECT count(*) AS n FROM {TABLES['FRAME']} f
      LEFT ANTI JOIN (SELECT * FROM {TABLES['DECISIONS']} WHERE night = DATE'{NIGHT}' AND mode = '{MODE}') d
      USING (provenance, native_id_namespace, native_id)""")[0]["n"]
    if int(missing):
        raise RuntimeError(f"{missing} records given to the location matcher have no decision for {NIGHT} ({MODE})")

cat, sch, tbl = TABLES['RUN_STATS'].split('.')
cols = {r["column_name"] for r in lwm.run_statement(f"""SELECT column_name FROM {cat}.information_schema.columns
  WHERE table_schema = '{sch}' AND table_name = '{tbl}'""", max_rows=100)}
for col in ("src_location_matcher", "src_new_work", "src_merged_winner"):
    if cols and col not in cols:   # an existing stats table gains the new counts; a new one is created with them below
        lwm.run_statement(f"ALTER TABLE {TABLES['RUN_STATS']} ADD COLUMNS ({col} BIGINT)")

# the map's id high-water mark and the warehouse's clock before the first insert: a new work's id is above the one, and the
# second pass looks a title key up only among rows inserted after the other
now = lwm.run_statement(f"SELECT max(id) AS maxid, current_date() AS d, current_timestamp() AS t FROM {TABLES['MAP']}")[0]
pins = {"LOOKUPS": open("work_id_lookups.sql").read(), **TABLES, "NIGHT": NIGHT or now["d"], "MODE": MODE,
        "MAXID": now["maxid"], "RUN_DATE": now["d"], "RUN_START": now["t"].replace("T", " ").rstrip("Z")}
lwm.run_sql_file("assign_work_ids.sql", pins)
