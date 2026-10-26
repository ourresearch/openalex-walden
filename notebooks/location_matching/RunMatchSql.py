# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works: run one SQL file with the night's pins (oxjob #1668)
# MAGIC
# MAGIC Fills `__NEW__` / `__OLD__` / `__V__` from the `prepare` task's values and runs the statements of each `sql/<file>` in the
# MAGIC comma-separated `sql` list, in order, on PROD-WH (the warehouse End 2 End's SQL tasks use). Used for the keys, embed,
# MAGIC candidates and features tasks.

# COMMAND ----------

import os
import sys
import time

sys.path.insert(0, os.getcwd())
import lwm  # noqa: E402

dbutils.widgets.text("sql", "features.sql")
for k in ("NEW", "OLD", "V"):
    dbutils.widgets.text(k, "")
PINS = {k: dbutils.jobs.taskValues.get(taskKey="prepare", key=k, debugValue=dbutils.widgets.get(k)) for k in ("NEW", "OLD", "V")}
T0 = time.time()
for f in [x.strip() for x in dbutils.widgets.get("sql").split(",") if x.strip()]:
    lwm.run_sql_file(f"sql/{f}", PINS)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
