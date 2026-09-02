# Databricks notebook source
# MAGIC %md
# MAGIC # CreateMagWorks
# MAGIC
# MAGIC Batch rebuild of `openalex.mag.mag_works` from the raw (frozen) MAG source
# MAGIC table `openalex.mag.mag_works_raw`. Replaces the retired Mag DLT pipeline (oxjob #733):
# MAGIC same walden-schema enrichment, same Deleted Journal / DOAJ exclusion, latest row
# MAGIC per native_id (was `apply_changes` SCD1 by updated_date), plus the URL-less husk
# MAGIC filter applied at the end.
# MAGIC
# MAGIC Run on a compute cluster (pandas UDFs), not a SQL warehouse. MAG is frozen upstream,
# MAGIC so this only needs to run when the enrichment library changes and a rebuild is
# MAGIC intentional. Re-running re-enriches with the CURRENT library code — merge_key and
# MAGIC author_key values can drift from the previous build, so only rebuild alongside a
# MAGIC `locations_parsed` full refresh (the union streams this table; replacing it
# MAGIC invalidates the mag flow checkpoint).

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.21-py3-none-any.whl

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

from openalex.dlt.normalize import walden_works_schema
from openalex.dlt.transform import (
    apply_initial_processing,
    enrich_with_features_and_author_keys,
    apply_final_merge_key_and_filter,
)

df = spark.table("openalex.mag.mag_works_raw").withColumn("provenance", F.lit("mag"))

df = apply_initial_processing(df, "mag", walden_works_schema)
# oxjob #911: ingested_at = when the corpus arrived on the platform. MAG is frozen, so
# every row carries mag_works_raw's createdAt (2025-02-28T17:09:35Z) — same convention
# as repo_works_backfill's parquet-export batch stamp. Stamped physically 2026-08-31;
# set here so rebuilds keep it (the schema pads it to NULL otherwise).
df = df.withColumn("ingested_at", F.lit("2025-02-28 17:09:35").cast("timestamp"))
df = enrich_with_features_and_author_keys(df)
df = apply_final_merge_key_and_filter(df)

df = df.filter(
    F.col("source_name").isNull()
    | ~F.col("source_name").isin(
        "Deleted Journal", "DOAJ (DOAJ: Directory of Open Access Journals)"
    )
)

# COMMAND ----------

dedup_window = Window.partitionBy("native_id").orderBy(
    F.col("updated_date").desc_nulls_last(),
    F.xxhash64(*[F.col(c) for c in df.columns]).desc(),
)
df = (
    df.withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

df = df.filter(F.expr("exists(urls, x -> x.url IS NOT NULL)"))

# COMMAND ----------

# Same-URL-set dedup (oxjob #733), MAG analog of the repo_works content key.
# MAG has no endpoint_id, and 64% of raw URL-set collisions span different
# titles (journal/issue-level URLs shared by distinct papers) — so the title
# is part of the key and only same-title re-registrations collapse (~141K).
url_set_key = F.concat_ws(
    "\u0001",
    F.array_sort(F.array_distinct(F.expr(
        "transform(filter(urls, u -> u.url IS NOT NULL), u -> trim(u.url))"
    ))),
)
same_location_window = Window.partitionBy(
    url_set_key, F.col("normalized_title")
).orderBy(
    F.col("updated_date").desc_nulls_last(),
    F.xxhash64(F.col("native_id")).desc(),
)
df = (
    df.withColumn("_rn", F.row_number().over(same_location_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

df.createOrReplaceTempView("mag_works_build")

spark.sql("""
CREATE OR REPLACE TABLE openalex.mag.mag_works
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT * FROM mag_works_build
""")

# COMMAND ----------

built = spark.table("openalex.mag.mag_works")
n = built.count()
husks = built.filter(~F.expr("exists(urls, x -> x.url IS NOT NULL)")).count()
dup_keys = built.groupBy("native_id").count().filter("count > 1").count()
print(f"rows: {n:,}  husks: {husks}  duplicate native_ids: {dup_keys}")
assert husks == 0
assert dup_keys == 0
assert n > 85_000_000, f"suspiciously low row count: {n:,}"
