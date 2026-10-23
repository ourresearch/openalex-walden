# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.32-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # HAL TEI cutover dry run (oxjob #1786)
# MAGIC
# MAGIC Builds, in batch, exactly the rows the HAL TEI flow would send to `repo_works`, by running the production code of
# MAGIC `notebooks/ingest/HalTei.py` (`build_hal_tei_parsed`) and `notebooks/ingest/Repo.py` (`parse_repo_records`,
# MAGIC `finish_repo_enriched`) under a stub `dlt` module. Writes only to `<out_prefix>_*` scratch tables; reads
# MAGIC `openalex.repo.hal_tei_items`, the endpoint map and the endpoint registry. Compare the output with `repo_works`
# MAGIC in SQL (queries in the oxjob's `scratch/diff_*.sql`).

# COMMAND ----------

import os
import sys
import types

import pyspark.sql.functions as F

dbutils.widgets.text("endpoint_map", "openalex.transient.oxjob1786_hal_endpoints", "native_id -> endpoint_id map")
dbutils.widgets.text("out_prefix", "openalex.transient.oxjob1786", "scratch table prefix")
dbutils.widgets.text("sample_mod", "0", "0 = every record; N = only records with pmod(hash(oai_id), N) = 0")
dbutils.widgets.text("reuse_parsed", "false", "true = keep <out_prefix>_hal_tei_parsed from an earlier run")
ENDPOINT_MAP = dbutils.widgets.get("endpoint_map")
OUT = dbutils.widgets.get("out_prefix")
SAMPLE_MOD = int(dbutils.widgets.get("sample_mod"))
REUSE_PARSED = dbutils.widgets.get("reuse_parsed").lower() == "true"

# The pipelines' Spark settings, so the same code behaves the same: DLT runs with ANSI off (Repo.py indexes possibly
# empty arrays, e.g. filter(ids, ...)[0]), and the Repo pipeline sets timeParserPolicy CORRECTED (jobs/repo_pipeline.yaml).
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

_dlt = types.ModuleType("dlt")


def _decorator(*args, **kwargs):
    if args and callable(args[0]) and not kwargs:
        return args[0]
    return lambda f: f


for _name in ("table", "view", "expect", "expect_or_drop", "expect_all", "expect_or_fail"):
    setattr(_dlt, _name, _decorator)
for _name in ("create_streaming_table", "apply_changes", "create_auto_cdc_flow", "read_stream", "read"):
    setattr(_dlt, _name, lambda *a, **k: None)
sys.modules["dlt"] = _dlt

HERE = os.getcwd()  # notebooks/maintenance in a GIT-sourced run


def load(rel_path):
    path = os.path.abspath(os.path.join(HERE, rel_path))
    ns = {"spark": spark, "dbutils": dbutils, "__name__": "hal_tei_dry_run"}
    exec(compile(open(path).read(), path, "exec"), ns)
    return ns


hal = load("../ingest/HalTei.py")
repo = load("../ingest/Repo.py")
print("loaded HalTei.py and Repo.py")

# COMMAND ----------

items = spark.read.table("openalex.repo.hal_tei_items")
if SAMPLE_MOD:
    items = items.filter(F.expr(f"pmod(hash(oai_id), {SAMPLE_MOD}) = 0"))
if not (REUSE_PARSED and spark.catalog.tableExists(f"{OUT}_hal_tei_parsed")):
    parsed = hal["build_hal_tei_parsed"](items, spark.read.table(ENDPOINT_MAP))
    parsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_hal_tei_parsed")
n_parsed = spark.table(f"{OUT}_hal_tei_parsed").count()
n_err = spark.table(f"{OUT}_hal_tei_parsed").filter("tei_error IS NOT NULL OR `ns0:metadata` IS NULL").count()
print(f"hal_tei_parsed: {n_parsed} rows, {n_err} with a parse error or no metadata")

# COMMAND ----------

src = spark.table(f"{OUT}_hal_tei_parsed").withColumn("_commit_version", F.lit(0).cast("long"))
# {} instead of read_endpoint_hosts(spark): the endpoint registry only re-keys placeholder hosts (ojs.pkp.sfu.ca,
# localhost, ...), never oai:HAL:, and a user without the openalex_sources grant can't read it.
parsed_repo = repo["parse_repo_records"](
    src, {}, dedupe=False,
    abstract=F.col("tei_abstract"), issue=F.col("tei_issue"), volume=F.col("tei_volume"),
    first_page=F.col("tei_first_page"), last_page=F.col("tei_last_page"),
)
enriched = repo["finish_repo_enriched"](
    repo["apply_initial_processing"](parsed_repo, "repo", repo["walden_works_schema_with_raw_type"]),
    source_rank=F.lit(2),
)
enriched.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_repo_enriched_hal_tei")
print("repo_enriched_hal_tei:", spark.table(f"{OUT}_repo_enriched_hal_tei").count(), "rows")
print("_sequence type:", spark.table(f"{OUT}_repo_enriched_hal_tei").schema["_sequence"].dataType.simpleString())
