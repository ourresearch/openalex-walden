# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.41-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # oai_openaire switch dry run (oxjob #2429)
# MAGIC
# MAGIC Builds, in batch, exactly the rows the Repo pipeline's openaire flow would send to `repo_works` for the given
# MAGIC endpoints, by running `notebooks/ingest/Repo.py` (`parse_openaire_file_udf`, `parse_openaire_records`,
# MAGIC `enrich_hal_tei`) under a stub `dlt` module on the files already harvested to
# MAGIC `s3://openalex-ingest/openaire/<endpoint id>/`. Writes only `<out_prefix>_*` scratch tables, then compares each
# MAGIC record with the `repo_works` row it would replace: OA flips, URL / title / author / id / date changes, records new
# MAGIC to `repo_works`, and records `repo_works` holds that the feed no longer lists.

# COMMAND ----------

import os
import sys
import types

import pyspark.sql.functions as F

dbutils.widgets.text("endpoint_ids", "0a26303df1172c2974a", "comma-separated endpoint ids (folders under openaire/)")
dbutils.widgets.text("out_prefix", "openalex.transient.oxjob2429", "scratch table prefix")
# read_endpoint_hosts reads the endpoint registry (catalog openalex_sources), which only the pipeline's service principal
# can use; it re-keys only placeholder OAI hosts (ojs.pkp.sfu.ca, localhost, ...), so a run as a person passes false.
dbutils.widgets.text("rekey_hosts", "false", "true = re-key placeholder hosts from the registry (run as the SP)")
REKEY = dbutils.widgets.get("rekey_hosts").lower() == "true"
ENDPOINTS = [e.strip() for e in dbutils.widgets.get("endpoint_ids").split(",") if e.strip()]
OUT = dbutils.widgets.get("out_prefix")

# The Repo pipeline's Spark settings (jobs/repo_pipeline.yaml), as HalTeiCutoverDryRun.py sets them.
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

path = os.path.abspath(os.path.join(os.getcwd(), "../ingest/Repo.py"))
repo = {"spark": spark, "dbutils": dbutils, "__name__": "openaire_dry_run"}
exec(compile(open(path).read(), path, "exec"), repo)
print("loaded Repo.py; endpoints:", ENDPOINTS)

# COMMAND ----------

# repo_items_openaire, in batch: the same select as the DLT table
files = (spark.read.format("binaryFile").option("pathGlobFilter", "*.xml.gz").option("recursiveFileLookup", "true")
         .load([f"s3a://openalex-ingest/openaire/{e}/" for e in ENDPOINTS]))
items = (files
         .select(F.explode(repo["parse_openaire_file_udf"](F.col("content"), F.col("path"))).alias("r"),
                 F.regexp_extract(F.col("path"), r"openaire/([^/]+)/", 1).alias("repository_id"),
                 F.col("path").alias("file_path"), F.col("modificationTime").alias("ingested_at"))
         .select("r.*", "repository_id", "file_path", "ingested_at"))
items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_items")
display(spark.sql(f"""SELECT repository_id, count(*) records, count_if(parse_error IS NOT NULL) parse_errors,
  count_if(is_deleted) deleted, count_if(dc IS NULL AND NOT is_deleted) not_openaire, count_if(is_open) open,
  count_if(access = 'open' AND NOT is_open) open_without_file, count_if(access IN ('closed', 'restricted', 'embargoed')) closed,
  count_if(access IS NULL AND dc IS NOT NULL) no_access_right, count_if(size(pdf_urls) > 0) with_pdf
  FROM {OUT}_items GROUP BY 1"""))

# COMMAND ----------

src = spark.table(f"{OUT}_items").withColumn("_commit_version", F.lit(0).cast("long"))
enriched = repo["enrich_hal_tei"](repo["parse_openaire_records"](src, repo["read_endpoint_hosts"](spark) if REKEY else {}))
enriched.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUT}_enriched")
_seq = enriched.schema["_sequence"].dataType
# every flow into repo_works carries one all-nullable struct<updated_date, col2..col5> (HalTeiCutoverDryRun.py)
assert [f.name for f in _seq.fields] == ["updated_date", "col2", "col3", "col4", "col5"], _seq.json()
assert all(f.nullable for f in _seq.fields), _seq.json()
print("enriched:", spark.table(f"{OUT}_enriched").count(), "rows; _sequence type OK")

# COMMAND ----------

# What repo_works would hold after the flow: per native_id, the winner of (stored row, latest openaire row) by _sequence.
# The stored row's rank is 1 (repo) or 0 (backfill); openaire rows are rank 2, so they win every tie on updated_date.
spark.sql(f"""
CREATE OR REPLACE TABLE {OUT}_diff AS
WITH new AS (
  SELECT * FROM {OUT}_enriched
  QUALIFY row_number() OVER (PARTITION BY native_id ORDER BY _sequence DESC) = 1
), old AS (
  SELECT * FROM openalex.repo.repo_works WHERE endpoint_id IN ({",".join(f"'{e}'" for e in ENDPOINTS)})
)
SELECT coalesce(n.native_id, o.native_id) AS native_id, coalesce(n.endpoint_id, o.endpoint_id) AS endpoint_id,
  n.native_id IS NOT NULL AS in_feed, o.native_id IS NOT NULL AS in_repo_works,
  n.native_id IS NOT NULL AND (o.native_id IS NULL OR n.updated_date >= o.updated_date) AS replaces,
  o.is_oa AS old_is_oa, n.is_oa AS new_is_oa, o.license AS old_license, n.license AS new_license,
  o.version AS old_version, n.version AS new_version,
  o.title AS old_title, n.title AS new_title, o.normalized_title AS old_normalized_title,
  n.normalized_title AS new_normalized_title,
  transform(o.authors, a -> a.name) AS old_authors, transform(n.authors, a -> a.name) AS new_authors,
  filter(o.ids, i -> i.namespace = 'doi') AS old_dois, filter(n.ids, i -> i.namespace = 'doi') AS new_dois,
  o.urls AS old_urls, n.urls AS new_urls, o.published_date AS old_published_date, n.published_date AS new_published_date,
  o.raw_native_type AS old_raw_native_type, n.raw_native_type AS new_raw_native_type,
  o.updated_date AS old_updated_date, n.updated_date AS new_updated_date
FROM new n FULL OUTER JOIN old o ON n.native_id = o.native_id
""")
display(spark.sql(f"""SELECT endpoint_id, count(*) rows, count_if(in_feed) in_feed, count_if(in_repo_works) in_repo_works,
  count_if(in_feed AND NOT in_repo_works) new_records, count_if(in_repo_works AND NOT in_feed) not_in_feed,
  count_if(replaces AND in_repo_works) replaced,
  count_if(replaces AND in_repo_works AND NOT old_is_oa AND new_is_oa) closed_to_open,
  count_if(replaces AND in_repo_works AND old_is_oa AND NOT new_is_oa) open_to_closed,
  count_if(replaces AND in_repo_works AND old_title <=> new_title) title_same,
  count_if(replaces AND in_repo_works AND old_normalized_title <=> new_normalized_title) normalized_title_same,
  count_if(replaces AND in_repo_works AND old_authors <=> new_authors) authors_same,
  count_if(replaces AND in_repo_works AND old_dois <=> new_dois) dois_same,
  count_if(replaces AND in_repo_works AND size(old_dois) = 0 AND size(new_dois) > 0) doi_added,
  count_if(replaces AND in_repo_works AND old_published_date <=> new_published_date) published_date_same,
  count_if(replaces AND in_repo_works AND old_version <=> new_version) version_same,
  count_if(replaces AND in_repo_works AND old_raw_native_type <=> new_raw_native_type) raw_type_same,
  count_if(replaces AND in_repo_works AND exists(new_urls, u -> u.content_type = 'pdf')
           AND NOT exists(old_urls, u -> u.content_type = 'pdf')) pdf_added
  FROM {OUT}_diff GROUP BY 1"""))
