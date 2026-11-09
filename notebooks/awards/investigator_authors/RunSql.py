# Databricks notebook source
# MAGIC %md
# MAGIC # Investigator authors: run SQL files with the job's pins (oxjob #1847)
# MAGIC
# MAGIC Runs each `sql/<file>` of the comma-separated `sql` list, in order and in one Spark session, with the job's parameters as pins,
# MAGIC after building the temp views the `views` list names (iaa.VIEWS).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.getcwd())
import iaa  # noqa: E402

dbutils.widgets.text("sql", "")
dbutils.widgets.text("views", "")
PINS = iaa.pins(iaa.params(dbutils))
for v in [x.strip() for x in dbutils.widgets.get("views").split(",") if x.strip()]:
    iaa.VIEWS[v](spark, PINS)
for f in [x.strip() for x in dbutils.widgets.get("sql").split(",") if x.strip()]:
    iaa.run_sql_file(spark, f, PINS)
