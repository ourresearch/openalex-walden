# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.35-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # Repository re-parse (oxjob #1828)
# MAGIC
# MAGIC Reads chosen endpoints' raw OAI files again (`s3://openalex-ingest/repositories/<endpoint_id>/`) with today's
# MAGIC `repository_schema` from `Repo.py`, into `openalex.repo.repo_reparse_items`. The Repo pipeline feeds that table
# MAGIC into `repo_works` as its third CDC flow (`repo_works_reparse`), rank 2 in `_sequence`: on the same datestamp the
# MAGIC re-parsed record replaces the stored one.
# MAGIC
# MAGIC **Use it when** a parser change needs a field `repo_items` never stored. `repo_items` keeps the parsed struct, not
# MAGIC the XML, so neither a refresh of Repo's parsed tables nor a re-harvest delivers a field added to the schema later
# MAGIC (repo_parsed's streaming dedup keeps the first copy of each record forever). First use: Persée's
# MAGIC `dcterms:accessRights` (CC BY-NC-SA), in the schema since 27 Sep, after ~838K Persée records were stored.
# MAGIC
# MAGIC **mode=dry_run** (default) writes only `<out_prefix>_*` scratch tables: the items, the rows the flow would send to
# MAGIC `repo_works` (the pipeline's own functions, under a stub `dlt`), and a diff against `repo_works`.
# MAGIC **mode=write** MERGEs the items into the target: new and changed records in, these endpoints' records no longer in
# MAGIC the files out (the flow ignores deletes). A re-run with the same schema changes nothing; after a schema change it
# MAGIC re-emits every record whose parsed struct changed.
# MAGIC
# MAGIC Every record a write changes is re-sent to `repo_works` and wins its tie, so size the works it re-stamps from the
# MAGIC dry run and file a rider before the night it rides.

# COMMAND ----------

import os
import re
import sys
import types

import pyspark.sql.functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("endpoint_ids", "", "comma-separated endpoint ids (folders under repositories/)")
dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "write"], "dry_run: scratch tables only; write: MERGE into target")
dbutils.widgets.text("target", "openalex.repo.repo_reparse_items", "side table Repo.py reads")
dbutils.widgets.text("out_prefix", "openalex.transient.repo_reparse", "dry run: scratch table prefix")
ENDPOINTS = [e.strip() for e in dbutils.widgets.get("endpoint_ids").split(",") if e.strip()]
MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target")
OUT = dbutils.widgets.get("out_prefix")
assert ENDPOINTS, "endpoint_ids is empty"
assert all(re.fullmatch(r"[A-Za-z0-9_-]+", e) for e in ENDPOINTS), f"bad endpoint id in {ENDPOINTS}"
print(f"mode={MODE} endpoints={ENDPOINTS}")

# The Repo pipeline's Spark settings, so the same code behaves the same (jobs/repo_pipeline.yaml; DLT runs ANSI off).
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# Repo.py under a stub dlt module, as notebooks/maintenance/HalTeiCutoverDryRun.py does: the schema and the parse are
# the pipeline's own code, never a copy.
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

_repo_path = os.path.join(os.getcwd(), "Repo.py")  # notebooks/ingest in a GIT-sourced run
repo = {"spark": spark, "dbutils": dbutils, "__name__": "repo_reparse"}
exec(compile(open(_repo_path).read(), _repo_path, "exec"), repo)
print("loaded Repo.py")

# COMMAND ----------

# repo_items' read, in batch: same reader options, same derived columns. recursiveFileLookup because the files sit in
# <endpoint>/<yyyy>/<mm>/<dd>/ folders (Auto Loader recurses by itself).
raw = (
    spark.read
    .format("xml")
    .option("rowTag", "ns0:record")
    .option("compression", "gzip")
    .option("recursiveFileLookup", "true")
    .schema(repo["repository_schema"])
    .load([f"s3a://openalex-ingest/repositories/{e}/" for e in ENDPOINTS])
    .withColumn("repository_id", F.regexp_extract(F.col("_metadata.file_path"), r"repositories/([^/]+)/", 1))
    .withColumn("ingested_at", F.col("_metadata.file_modification_time"))
    .withColumn("file_path", F.col("_metadata.file_path"))
)
# One row per (endpoint, OAI id, datestamp): a record delivered in several files is the same record; keep the latest
# delivery. A record with no OAI id has no key in repo_works.
_copy = Window.partitionBy(
    "repository_id", F.col("`ns0:header`.`ns0:identifier`"), F.col("`ns0:header`.`ns0:datestamp`")
).orderBy(F.col("ingested_at").desc(), F.col("file_path").desc())
items = (
    raw
    .filter(F.col("`ns0:header`.`ns0:identifier`").isNotNull())
    .withColumn("_copy", F.row_number().over(_copy))
    .filter("_copy = 1")
    .drop("_copy")
    .select("`ns0:header`", "`ns0:metadata`", "repository_id", "ingested_at", "file_path")
)

# COMMAND ----------

if MODE == "dry_run":
    items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_items")
    items_t = spark.table(f"{OUT}_items")
    print(f"{OUT}_items:")
    items_t.groupBy("repository_id").agg(
        F.count("*").alias("records"),
        F.countDistinct("`ns0:header`.`ns0:identifier`").alias("oai_ids"),
        F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:accessRights`") > 0).alias("with_accessRights"),
        F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:license`") > 0).alias("with_license"),
        F.countDistinct("file_path").alias("files"),
    ).show(truncate=False)

    # The flow's own two steps; _commit_version as the CDF read would add it.
    src = items_t.withColumn("_commit_version", F.lit(1).cast("long"))
    enriched = repo["enrich_reparse"](repo["parse_reparse_records"](src, repo["read_endpoint_hosts"](spark)))
    enriched.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_enriched")
    _seq = spark.table(f"{OUT}_enriched").schema["_sequence"].dataType
    print("_sequence type:", _seq.json())
    # repo_works' flows all carry an all-nullable struct<updated_date, col2..col5>; DLT refuses a flow whose sequence
    # type differs in anything, nullability included (oxjob #1786).
    assert [f.name for f in _seq.fields] == ["updated_date", "col2", "col3", "col4", "col5"], _seq.json()
    assert all(f.nullable for f in _seq.fields), _seq.json()

# COMMAND ----------

if MODE == "dry_run":
    # What the flow would do to repo_works. Per native_id the flow's winner is its highest _sequence. It replaces the
    # stored row when its updated_date is at least as new: rank 2 wins every tie against rank 0 (backfill) and 1 (live).
    # shortcut: a stored row that itself came from a rank-2 flow (HAL TEI) would need the full _sequence; repo_works
    # does not keep it. Fine for non-HAL endpoints.
    works_cols = [c for c in spark.table("openalex.repo.repo_works").columns if c != "native_id"]
    en = spark.table(f"{OUT}_enriched")
    cmp_cols = [c for c in works_cols if c in en.columns]
    winners = (
        en.groupBy("native_id")
        .agg(F.max_by(F.struct(*cmp_cols), "_sequence").alias("n"), F.count("*").alias("copies"))
        .select("native_id", "copies", "n.*")
    )
    stored = spark.table("openalex.repo.repo_works").select(
        "native_id", F.lit(True).alias("_stored"), *[F.col(c).alias(f"old_{c}") for c in cmp_cols])
    j = winners.join(stored, "native_id", "left")
    changed = F.filter(
        F.array(*[F.when(~F.col(c).eqNullSafe(F.col(f"old_{c}")), F.lit(c)) for c in cmp_cols]),
        lambda x: x.isNotNull())
    newer_or_same = (F.coalesce("updated_date", F.lit("0001-01-01").cast("date"))
                     >= F.coalesce("old_updated_date", F.lit("0001-01-01").cast("date")))
    diff = (
        j.withColumn("changed_cols", changed)
        .withColumn("kind",
                    F.when(F.col("_stored").isNull(), "insert")
                    .when(~newer_or_same, "loses")
                    .when(F.size("changed_cols") == 0, "same")
                    .otherwise("update"))
        .select("native_id", "kind", "copies", "changed_cols",
                "endpoint_id", "old_endpoint_id", "license", "old_license", "is_oa", "old_is_oa",
                "version", "old_version", "type", "old_type", "updated_date", "old_updated_date",
                "ingested_at", "old_ingested_at")
    )
    diff.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_diff")
    d = spark.table(f"{OUT}_diff")
    print("kind:")
    d.groupBy("kind").count().orderBy("kind").show()
    print("changed columns (update rows):")
    d.filter("kind = 'update'").select(F.explode("changed_cols").alias("col")).groupBy("col").count() \
        .orderBy(F.desc("count")).show(40, truncate=False)
    print("licence and is_oa transitions (update rows):")
    d.filter("kind = 'update'").groupBy("old_license", "license", "old_is_oa", "is_oa").count() \
        .orderBy(F.desc("count")).show(40, truncate=False)
    # Works the update rows reach (repo locations are keyed provenance 'repo', namespace 'pmh', native_id).
    reach = d.filter("kind IN ('update', 'insert')").join(
        spark.table("openalex.works.location_work_ids")
        .filter("provenance = 'repo' AND native_id_namespace = 'pmh'")
        .select("native_id", "work_id"),
        "native_id", "left")
    reach.agg(F.count("*").alias("records"), F.countDistinct("work_id").alias("works"),
              F.count_if(F.col("work_id").isNull()).alias("no_work_yet")).show()

# COMMAND ----------

if MODE == "write":
    if not spark.catalog.tableExists(TARGET):
        (items.limit(0).writeTo(TARGET)
         .tableProperty("delta.enableChangeDataFeed", "true")  # Repo.py reads it as a change feed
         .tableProperty("delta.autoOptimize.optimizeWrite", "true")
         .create())
        print(f"created {TARGET}")
    items.createOrReplaceTempView("repo_reparse_source")
    endpoint_list = ", ".join(f"'{e}'" for e in ENDPOINTS)  # validated above
    # Row-level MERGE, never an overwrite: an overwrite is not a row-level change feed and breaks the pipeline's
    # readChangeFeed stream until a full refresh (jobs/repo_backfill.yaml). Unchanged records are left alone, so a
    # re-run re-sends only what a schema change actually changed.
    metrics = spark.sql(f"""
        MERGE INTO {TARGET} t
        USING repo_reparse_source s
        ON t.repository_id = s.repository_id
           AND t.`ns0:header`.`ns0:identifier` = s.`ns0:header`.`ns0:identifier`
           AND t.`ns0:header`.`ns0:datestamp` <=> s.`ns0:header`.`ns0:datestamp`
        WHEN MATCHED AND NOT (t.`ns0:header` <=> s.`ns0:header` AND t.`ns0:metadata` <=> s.`ns0:metadata`)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.repository_id IN ({endpoint_list}) THEN DELETE
    """)
    metrics.show(truncate=False)
    spark.table(TARGET).filter(F.col("repository_id").isin(ENDPOINTS)).groupBy("repository_id").agg(
        F.count("*").alias("records"),
        F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:accessRights`") > 0).alias("with_accessRights"),
    ).show(truncate=False)
