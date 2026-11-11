# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.41-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # IRDB re-parse dry run (oxjob #2147)
# MAGIC
# MAGIC What a full refresh of the IRDB pipeline would send to `repo_works`, without writing anything but scratch tables.
# MAGIC
# MAGIC **stage=parse** reads every raw IRDB file (`s3://openalex-ingest/irdb/`) with today's `IRDB.py` (its schema and
# MAGIC `irdb_parsed` rules, loaded under a stub `dlt`, never a copy) and keeps per record the copy `repo_works` would keep
# MAGIC (newest datestamp, then newest file): `<out_prefix>_winners`.
# MAGIC
# MAGIC **stage=enrich** runs those winners through the Repo pipeline's own IRDB path (`Repo.py`:
# MAGIC `apply_initial_processing` + `finish_repo_enriched`, as `repo_enriched` does for the `repo_parsed_irdb` stream) and
# MAGIC diffs every column against `repo_works`: `<out_prefix>_diff` (kind = same / update / dropped / insert). A refresh
# MAGIC sends a CDF delete for every stored row, so a `dropped` record (its re-sent row filtered out) leaves `repo_works`.
# MAGIC
# MAGIC **stage=all** runs both.

# COMMAND ----------

import os
import sys
import types

import pyspark.sql.functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("out_prefix", "openalex.transient.oxjob2147_reparse", "scratch table prefix")
dbutils.widgets.text("path", "s3a://openalex-ingest/irdb/", "raw IRDB files")
dbutils.widgets.dropdown("stage", "all", ["all", "parse", "enrich"], "parse, enrich, or both")
OUT = dbutils.widgets.get("out_prefix")
PATH = dbutils.widgets.get("path")
STAGE = dbutils.widgets.get("stage")
assert OUT.startswith("openalex.transient."), "scratch tables only"
print(f"out={OUT} path={PATH} stage={STAGE}")

# DLT runs ANSI off (jobs/repo_pipeline.yaml; RepoReparse.py)
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# IRDB.py and Repo.py under a stub dlt (pattern: RepoReparse.py). read_stream("irdb_items") returns the batch read below.
_dlt = types.ModuleType("dlt")


def _decorator(*args, **kwargs):
    if args and callable(args[0]) and not kwargs:
        return args[0]
    return lambda f: f


for _name in ("table", "view", "expect", "expect_all", "expect_or_fail"):
    setattr(_dlt, _name, _decorator)


def _expect_or_drop(name, cond):  # DLT drops these rows; so does the dry run (irdb_parsed's has_title)
    return lambda f: (lambda *a, **k: f(*a, **k).filter(cond))


_dlt.expect_or_drop = _expect_or_drop
for _name in ("create_streaming_table", "apply_changes", "create_auto_cdc_flow"):
    setattr(_dlt, _name, lambda *a, **k: None)
_RAW = {}
_dlt.read_stream = lambda name: _RAW[name]
_dlt.read = lambda name: _RAW[name]
sys.modules["dlt"] = _dlt

_ingest = os.path.join(os.path.dirname(os.getcwd()), "ingest")  # notebooks/maintenance in a GIT run


def _load(name):
    path = os.path.join(_ingest, name)
    ns = {"spark": spark, "dbutils": dbutils, "__name__": f"irdb_reparse_{name}"}
    exec(compile(open(path).read(), path, "exec"), ns)
    print("loaded", path)
    return ns


irdb = _load("IRDB.py")
print("prefixes", irdb["VARIABLE_NS_PREFIXES"])

# COMMAND ----------

if STAGE in ("all", "parse"):
    raw = (
        spark.read.format("xml")
        .option("rowTag", "ns0:record")
        .option("ignoreMissingFiles", "true")
        .option("recursiveFileLookup", "true")  # files sit in yyyy/mm/dd folders; a batch read does not descend
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

if STAGE in ("all", "enrich"):
    repo = _load("Repo.py")
    # the CDF read adds these; 'insert' and a commit version newer than any stored row
    src = (spark.table(f"{OUT}_winners")
           .withColumn("_change_type", F.lit("insert"))
           .withColumn("_commit_version", F.lit(10 ** 9).cast("long")))
    en = repo["finish_repo_enriched"](
        repo["apply_initial_processing"](src, "repo", repo["walden_works_schema_with_raw_type"]))
    en.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_enriched")
    en = spark.table(f"{OUT}_enriched")
    stored = spark.table("openalex.repo.repo_works").filter("endpoint_id = 'irdb_nii_ac_jp'")
    cmp_cols = [c for c in stored.columns if c != "native_id" and c in en.columns]
    old = stored.select("native_id", F.lit(True).alias("_stored"), *[F.col(c).alias(f"old_{c}") for c in cmp_cols])
    new = en.select("native_id", F.lit(True).alias("_new"), *cmp_cols)
    changed = F.filter(
        F.array(*[F.when(~F.col(c).eqNullSafe(F.col(f"old_{c}")), F.lit(c)) for c in cmp_cols]),
        lambda x: x.isNotNull())
    (
        new.join(old, "native_id", "full")
        .withColumn("changed_cols", changed)
        .withColumn("kind",
                    F.when(F.col("_stored").isNull(), "insert")
                    .when(F.col("_new").isNull(), "dropped")
                    .when(F.size("changed_cols") == 0, "same")
                    .otherwise("update"))
        .select("native_id", "kind", "changed_cols", "merge_key", "old_merge_key", "published_date",
                "old_published_date", "version", "old_version", "license", "old_license", "is_oa", "old_is_oa",
                "title", "old_title")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_diff")
    )
    d = spark.table(f"{OUT}_diff")
    d.groupBy("kind").count().orderBy("kind").show()
    d.filter("kind = 'update'").select(F.explode("changed_cols").alias("col")).groupBy("col").count() \
        .orderBy(F.desc("count")).show(60, truncate=False)
