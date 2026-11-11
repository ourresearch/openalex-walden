# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.41-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # IRDB re-parse dry run (oxjob #2147)
# MAGIC
# MAGIC Reads every raw IRDB file (`s3://openalex-ingest/irdb/`) with today's `IRDB.py` (schema and `irdb_parsed`
# MAGIC rules, loaded under a stub `dlt`, never a copy), as a full refresh of the IRDB pipeline would, and keeps per
# MAGIC record the copy `repo_works` would keep (newest datestamp, then newest file). Writes scratch tables only:
# MAGIC
# MAGIC - `<out_prefix>_winners`: one row per native_id, the re-parsed winner
# MAGIC - `<out_prefix>_raw_counts`: rows read, rows after the parse filters
# MAGIC
# MAGIC Diff it against `openalex.repo.irdb_parsed` / `repo_works` in SQL (oxjob #2147 work/).

# COMMAND ----------

import os
import sys
import types

import pyspark.sql.functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("out_prefix", "openalex.transient.oxjob2147_reparse", "scratch table prefix")
dbutils.widgets.text("path", "s3a://openalex-ingest/irdb/", "raw IRDB files")
OUT = dbutils.widgets.get("out_prefix")
PATH = dbutils.widgets.get("path")
assert OUT.startswith("openalex.transient."), "scratch tables only"
print(f"out={OUT} path={PATH}")

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# IRDB.py under a stub dlt (pattern: RepoReparse.py): read_stream("irdb_items") returns the batch read below.
_dlt = types.ModuleType("dlt")


def _decorator(*args, **kwargs):
    if args and callable(args[0]) and not kwargs:
        return args[0]
    return lambda f: f


for _name in ("table", "view", "expect", "expect_or_drop", "expect_all", "expect_or_fail"):
    setattr(_dlt, _name, _decorator)
_RAW = {}
_dlt.read_stream = lambda name: _RAW[name]
_dlt.read = lambda name: _RAW[name]
sys.modules["dlt"] = _dlt

_irdb_path = os.path.join(os.path.dirname(os.getcwd()), "ingest", "IRDB.py")  # notebooks/maintenance in a GIT run
irdb = {"spark": spark, "dbutils": dbutils, "__name__": "irdb_reparse"}
exec(compile(open(_irdb_path).read(), _irdb_path, "exec"), irdb)
print("loaded", _irdb_path, "prefixes", irdb["VARIABLE_NS_PREFIXES"])

# COMMAND ----------

raw = (
    spark.read.format("xml")
    .option("rowTag", "ns0:record")
    .option("ignoreMissingFiles", "true")
    .schema(irdb["jpcoar_schema"])
    .load(PATH)
    .withColumn("ingested_at", F.col("_metadata.file_modification_time"))
)
_RAW["irdb_items"] = raw
parsed = irdb["irdb_parsed"]()

w = Window.partitionBy("native_id").orderBy(F.col("updated_date").desc(), F.col("ingested_at").desc())
winners = parsed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
winners.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_winners")
print("winners", spark.table(f"{OUT}_winners").count())

# COMMAND ----------

raw_rows = raw.select("`ns0:header`.`ns0:identifier`").count()
spark.createDataFrame([(raw_rows,)], "raw_rows long").write.mode("overwrite").saveAsTable(f"{OUT}_raw_counts")
print("raw rows", raw_rows)
