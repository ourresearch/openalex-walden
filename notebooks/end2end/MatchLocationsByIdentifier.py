# Databricks notebook source
# MAGIC %md
# MAGIC # Map_Work_Ids 1/3: match locations by identifier (oxjob #764, #1668)
# MAGIC
# MAGIC Every record that needs a work tonight (absent from `location_work_ids`, or registered NULL: NULL records retry nightly)
# MAGIC goes into `location_work_ids_pending` with its keys, and its first answer into `location_work_ids_verdicts`: the work
# MAGIC that already holds its DOI, PMID or arXiv id, the work it declares itself a version or preprint of, or its legacy MAG / PMH
# MAGIC id. A first-sight record (never pinned before) that none of these matches is left for the location matcher, with the title
# MAGIC rule's pick kept as one of the matcher's inputs. Retries and records a merge wave or cleanup unpinned since the previous
# MAGIC registry MERGE keep the title rule as before. SQL: `match_locations_by_identifier.sql` + `work_id_lookups.sql`, on PROD-WH.
# MAGIC The widgets name the tables so the step can run against copies; End 2 End uses the defaults.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "..", "location_matching"))
import lwm  # noqa: E402

DEFAULTS = {"REG": "openalex.works.location_work_ids", "MAP": "openalex.works.work_id_map",
            "PENDING": "openalex.works.location_work_ids_pending", "VERDICTS": "openalex.works.location_work_ids_verdicts",
            "MERGED": "openalex.works.merged_work_ids", "DELETED": "openalex.works.deleted_works",
            "LOCATIONS": "openalex.works.locations_w_types", "REG_HISTORY": "openalex.works.location_work_ids"}
for k, v in DEFAULTS.items():
    dbutils.widgets.text(k.lower(), v)
dbutils.widgets.text("reg_old_version", "")
TABLES = {k: dbutils.widgets.get(k.lower()).strip() for k in DEFAULTS}

# the registry as of its previous MERGE: a record pinned there and absent now was unpinned since by a correction job
old = dbutils.widgets.get("reg_old_version").strip() or lwm.run_statement(
    f"""SELECT max(version) AS v FROM (DESCRIBE HISTORY {TABLES['REG_HISTORY']}) WHERE operation = 'MERGE'""")[0]["v"]
lwm.log(f"previous registry MERGE: {TABLES['REG_HISTORY']} version {old}")
pins = {"LOOKUPS": open("work_id_lookups.sql").read(),
        **{k: v for k, v in TABLES.items() if k != "REG_HISTORY"},
        "REG_OLD": f"{TABLES['REG_HISTORY']} VERSION AS OF {old}"}
lwm.run_sql_file("match_locations_by_identifier.sql", pins)
