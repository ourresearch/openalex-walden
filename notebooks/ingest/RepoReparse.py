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
# MAGIC Both modes first stage the raw copies and the chosen copy per record in `<out_prefix>_raw` / `<out_prefix>_items`.
# MAGIC **mode=dry_run** (default) then writes only scratch tables: the rows the flow would send to `repo_works` (the
# MAGIC pipeline's own functions, under a stub `dlt`) and a diff against `repo_works`.
# MAGIC **mode=write** MERGEs the items into `openalex.repo.repo_reparse_items`: new and changed records in, these
# MAGIC endpoints' records no longer in the files out (the flow ignores deletes). A re-run with the same schema changes
# MAGIC nothing; after a schema change it re-emits every record whose parsed struct changed.
# MAGIC
# MAGIC Every record a write changes is re-sent to `repo_works` and wins its tie, so size the works it re-stamps from the
# MAGIC dry run and file a rider before the night it rides.
# MAGIC
# MAGIC **After a re-parse, a refresh of Repo's main tables no longer reaches these records**: they sit at rank 2, and a
# MAGIC re-sent repo_items copy (rank 1, same datestamp) loses to them. Include `repo_parsed_reparse` and
# MAGIC `repo_enriched_reparse` in any such refresh (jobs/repo_pipeline.yaml).

# COMMAND ----------

import os
import re
import sys
import types

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from openalex.dlt.oai_ids import read_endpoint_hosts, with_rekeyed_native_id

dbutils.widgets.text("endpoint_ids", "", "comma-separated endpoint ids (folders under repositories/)")
dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "write"], "dry_run: scratch tables only; write: MERGE into the side table")
dbutils.widgets.text("out_prefix", "openalex.transient.repo_reparse", "scratch table prefix")
ENDPOINTS = [e.strip() for e in dbutils.widgets.get("endpoint_ids").split(",") if e.strip()]
MODE = dbutils.widgets.get("mode")
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
TARGET = repo["REPO_REPARSE_ITEMS_TABLE"]
print(f"loaded Repo.py; target {TARGET}")

# COMMAND ----------

# repo_items' read, in batch: same reader options, same derived columns. recursiveFileLookup because the files sit in
# <endpoint>/<yyyy>/<mm>/<dd>/ folders (Auto Loader recurses by itself). Keyed after the #1407 re-key, as repo_parsed
# is, so placeholder-host records of different journals never collide. Staged once: serverless has no cache, and every
# later step would otherwise list and parse the S3 files again.
ID_HOSTS = read_endpoint_hosts(spark)
(
    spark.read
    .format("xml")
    .option("rowTag", "ns0:record")
    .option("compression", "gzip")
    .option("recursiveFileLookup", "true")
    .schema(repo["repository_schema"])
    .load([f"s3a://openalex-ingest/repositories/{e}/" for e in ENDPOINTS])
    .withColumn("repository_id", F.regexp_extract(F.col("_metadata.file_path"), r"repositories/([^/]+)/", 1))
    # S3's LastModified in whole seconds; repo_items got the file event's milliseconds, so a re-sent record's
    # ingested_at moves by under a second. Harmless: rank 2 already orders it ahead of the stored copy.
    .withColumn("ingested_at", F.col("_metadata.file_modification_time"))
    .withColumn("file_path", F.col("_metadata.file_path"))
    .filter(F.col("`ns0:header`.`ns0:identifier`").isNotNull())  # no OAI id = no key in repo_works
    .withColumn("oai_identifier", F.col("`ns0:header`.`ns0:identifier`"))
    .transform(lambda d: with_rekeyed_native_id(d, ID_HOSTS, "repository_id"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_raw")
)
raw = spark.table(f"{OUT}_raw")
WORKS = spark.table("openalex.repo.repo_works")
# repo_works rows for these records only, before any wide join (a left join can't broadcast its 218M-row right side).
stored = WORKS.join(F.broadcast(raw.select("native_id").distinct()), "native_id", "left_semi")

# Re-send the copy the pipeline holds. A record often arrives in several files, or from two endpoints of one repository
# (Persée has two), and repo_parsed keeps whichever copy of a (native_id, datestamp) it read first. So per (native_id,
# datestamp): the copy from the endpoint repo_works holds the record under, else the earliest delivery. Letting the
# latest copy win moved 131K Persée locations to the other endpoint's source in the 27 Oct dry run.
_pick = Window.partitionBy("native_id", F.col("`ns0:header`.`ns0:datestamp`")).orderBy(
    (F.col("repository_id") == F.col("_stored_endpoint")).desc_nulls_last(),
    F.col("ingested_at").asc(), F.col("file_path").asc())
(
    raw.join(stored.select("native_id", F.col("endpoint_id").alias("_stored_endpoint")), "native_id", "left")
    .withColumn("_copy", F.row_number().over(_pick))
    .filter("_copy = 1")
    .select("`ns0:header`", "`ns0:metadata`", "repository_id", "ingested_at", "file_path")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_items")
)
items = spark.table(f"{OUT}_items")
# HAL's records already come in through the HAL TEI flow at the same rank 2; a tie there has no defined winner.
assert items.filter(F.col("`ns0:header`.`ns0:identifier`").startswith("oai:HAL:")).isEmpty(), \
    "HAL records are re-parsed by the HAL TEI flow (oxjob #1786), not here"
print(f"{OUT}_items:")
items.groupBy("repository_id").agg(
    F.count("*").alias("records"),
    F.countDistinct("`ns0:header`.`ns0:identifier`").alias("oai_ids"),
    F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:accessRights`") > 0).alias("with_accessRights"),
    F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:license`") > 0).alias("with_license"),
    F.countDistinct("file_path").alias("files"),
).show(truncate=False)

# COMMAND ----------

if MODE == "dry_run":
    # The flow's own two steps; _commit_version as the CDF read would add it.
    src = items.withColumn("_commit_version", F.lit(1).cast("long"))
    repo["enrich_reparse"](repo["parse_reparse_records"](src, ID_HOSTS)) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_enriched")
    en = spark.table(f"{OUT}_enriched")
    _seq = en.schema["_sequence"].dataType
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
    # does not keep it. HAL never comes through here (assert above).
    cmp_cols = [c for c in WORKS.columns if c != "native_id" and c in en.columns]
    winners = (
        en.groupBy("native_id")
        .agg(F.max_by(F.struct(*cmp_cols), "_sequence").alias("n"), F.count("*").alias("copies"))
        .select("native_id", "copies", "n.*")
    )
    old = stored.select("native_id", F.lit(True).alias("_stored"), *[F.col(c).alias(f"old_{c}") for c in cmp_cols])
    changed = F.filter(
        F.array(*[F.when(~F.col(c).eqNullSafe(F.col(f"old_{c}")), F.lit(c)) for c in cmp_cols]),
        lambda x: x.isNotNull())
    newer_or_same = (F.coalesce("updated_date", F.lit("0001-01-01").cast("date"))
                     >= F.coalesce("old_updated_date", F.lit("0001-01-01").cast("date")))
    (
        winners.join(old, "native_id", "left")
        .withColumn("changed_cols", changed)
        .withColumn("kind",
                    F.when(F.col("_stored").isNull(), "insert")
                    .when(~newer_or_same, "loses")
                    .when(F.size("changed_cols") == 0, "same")
                    .otherwise("update"))
        .select("native_id", "kind", "copies", "changed_cols",
                "endpoint_id", "old_endpoint_id", "license", "old_license", "is_oa", "old_is_oa",
                "version", "old_version", "type", "old_type", "updated_date", "old_updated_date",
                "ingested_at", "old_ingested_at")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_diff")
    )
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
    touched = d.filter("kind IN ('update', 'insert')").select("native_id")
    work_ids = (spark.table("openalex.works.location_work_ids")
                .filter("provenance = 'repo' AND native_id_namespace = 'pmh'")
                .join(F.broadcast(touched), "native_id", "left_semi")
                .select("native_id", "work_id"))
    touched.join(work_ids, "native_id", "left").agg(
        F.count("*").alias("records"), F.countDistinct("work_id").alias("works"),
        F.count_if(F.col("work_id").isNull()).alias("no_work_yet")).show()

# COMMAND ----------

if MODE == "write":
    if not spark.catalog.tableExists(TARGET):
        (items.limit(0).writeTo(TARGET)
         .tableProperty("delta.enableChangeDataFeed", "true")  # Repo.py reads it as a change feed
         .tableProperty("delta.autoOptimize.optimizeWrite", "true")
         .create())
        print(f"created {TARGET}")
    endpoint_list = ", ".join(f"'{e}'" for e in ENDPOINTS)  # validated above
    # Row-level MERGE, never an overwrite: an overwrite is not a row-level change feed and breaks the pipeline's
    # readChangeFeed stream until a full refresh (jobs/repo_backfill.yaml). Unchanged records are left alone, so a
    # re-run re-sends only what a schema change actually changed.
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING {OUT}_items s
        ON t.repository_id = s.repository_id
           AND t.`ns0:header`.`ns0:identifier` = s.`ns0:header`.`ns0:identifier`
           AND t.`ns0:header`.`ns0:datestamp` <=> s.`ns0:header`.`ns0:datestamp`
        WHEN MATCHED AND NOT (t.`ns0:header` <=> s.`ns0:header` AND t.`ns0:metadata` <=> s.`ns0:metadata`)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.repository_id IN ({endpoint_list}) THEN DELETE
    """).show(truncate=False)
    spark.table(TARGET).filter(F.col("repository_id").isin(ENDPOINTS)).groupBy("repository_id").agg(
        F.count("*").alias("records"),
        F.count_if(F.size("`ns0:metadata`.`ns1:dc`.`ns4:accessRights`") > 0).alias("with_accessRights"),
    ).show(truncate=False)
