# Databricks notebook source
# oxjob #2083 test, branch oxjob-2083-final-partitions only (never merged to main).
# Runs the new partition code in _utils against real tables, with a scratch cutoffs
# table and a scratch S3 prefix outside full/, then removes both.

# COMMAND ----------

# MAGIC %run ./_utils

# COMMAND ----------

import json
from pyspark.sql import functions as F

TEST_TABLE = "openalex.transient.snapshot_partition_cutoffs_test_2083"
PARTITION_CUTOFFS_TABLE = TEST_TABLE          # globals the _utils functions read
S3_BASE = "s3://openalex-snapshots/test-2083"
PUBLISHED = "2026-11-07"   # the snapshot tables hold this export (checked against its manifest)
NEXT = "2026-11-08"
out = {"session_tz": spark.conf.get("spark.sql.session.timeZone")}

# Serverless refuses the parquet writer's outputTimestampType setting (the job's
# classic clusters allow it); skip it here, it doesn't touch partitioning.
_conf_set = spark.conf.set
def _conf_set_or_skip(k, v):
    try:
        _conf_set(k, v)
    except Exception as e:
        out.setdefault("conf_skipped", []).append(k)
spark.conf.set = _conf_set_or_skip

spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
SNAP = {
    "authors": "openalex.authors.openalex_authors_snapshot",
    "sources": "openalex.sources.openalex_sources_snapshot",
    "institutions": "openalex.institutions.openalex_institutions_snapshot",
    "funders": "openalex.funders.openalex_funders_snapshot",
    "licenses": "openalex.common.openalex_licenses_snapshot",
}
for e, t in SNAP.items():
    _record_partition_cutoff(spark, spark.read.table(t), e, PUBLISHED)
out["seed"] = [r.asDict() for r in spark.read.table(TEST_TABLE).orderBy("entity").collect()]

# COMMAND ----------

# Test 1: re-running on the published content moves nothing.
# Test 2: tomorrow's input (the live tables now) never adds to a published partition.
LIVE = {
    "authors": spark.read.table("openalex.authors.openalex_authors"),
    "sources": spark.read.table("openalex.sources.sources_api"),
    "institutions": spark.read.table("openalex.institutions.institutions_api"),
    "funders": spark.read.table("openalex.funders.funders_api"),
}

def counts(df, col):
    return {str(r[0]): r[1] for r in df.groupBy(col).count().collect()}

for e, t in SNAP.items():
    pub = spark.read.table(t)
    published = counts(pub.withColumn("p", _partition_date_col()), "p")
    same = counts(_with_partition_date(spark, pub, e, NEXT), "_partition_date")
    res = {"test1_equal": same == published}
    if e in LIVE:
        live = LIVE[e].select("id", "updated_date", "created_date")
        new = counts(_with_partition_date(spark, live, e, NEXT), "_partition_date")
        plain = counts(live.withColumn("p", _partition_date_col()), "p")
        res["grown_published_partitions_plain"] = {d: [published.get(d, 0), plain[d]] for d in plain
                                                   if d <= PUBLISHED and plain[d] > published.get(d, 0)}
        res["grown_published_partitions_new"] = {d: [published.get(d, 0), new[d]] for d in new
                                                 if d <= PUBLISHED and new[d] > published.get(d, 0)}
        res["new_partition_next"] = new.get(NEXT, 0)
        res["rows_equal"] = sum(new.values()) == sum(plain.values())
    out[e] = res

# COMMAND ----------

# Test 3: the writers, salted and unsalted, into the scratch prefix; then the cutoff row.
try:
    src = (spark.read.table("openalex.sources.sources_api")
           .withColumn("id", F.concat(F.lit("https://openalex.org/S"), F.col("id"))))
    export_partitioned_all_formats(spark, dbutils, src, NEXT, "sources", salt=True)
    export_partitioned_all_formats(spark, dbutils, spark.read.table(SNAP["licenses"]), NEXT, "licenses", salt=False)
    for ent in ("sources", "licenses"):
        for fmt in ("jsonl", "parquet"):
            meta = json.loads(dbutils.fs.head(f"{S3_BASE}/{NEXT}/_meta/{fmt}/{ent}.json", 10_000_000))
            parts = {}
            for f in meta["files"]:
                p = f["url"].split("/")[-2]
                parts[p] = parts.get(p, 0) + f["meta"]["record_count"]
            out[f"test3_{ent}_{fmt}"] = {"records": meta["record_count"],
                                         "newest": {k: v for k, v in sorted(parts.items())[-3:]}}
except Exception as e:
    out["test3_error"] = repr(e)[:2000]
out["cutoffs_after"] = [r.asDict() for r in spark.read.table(TEST_TABLE).orderBy("entity", "snapshot_date").collect()]

# COMMAND ----------

dbutils.fs.rm(S3_BASE, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
dbutils.notebook.exit(json.dumps(out, default=str))
