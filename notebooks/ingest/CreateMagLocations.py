# Databricks notebook source
# MAGIC %md
# MAGIC # CreateMagLocations
# MAGIC
# MAGIC Batch rebuild of `openalex.mag.mag_locations` from the raw (frozen) MAG source
# MAGIC table `openalex.mag.mag_works`. Replaces the retired Mag DLT pipeline (oxjob #733):
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

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.4-py3-none-any.whl

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

from openalex.dlt.normalize import walden_works_schema
from openalex.dlt.transform import (
    apply_initial_processing,
    enrich_with_features_and_author_keys,
    apply_final_merge_key_and_filter,
)

df = spark.table("openalex.mag.mag_works").withColumn("provenance", F.lit("mag"))

df = apply_initial_processing(df, "mag", walden_works_schema)
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

df.createOrReplaceTempView("mag_locations_build")

spark.sql("""
CREATE OR REPLACE TABLE openalex.mag.mag_locations
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT * FROM mag_locations_build
""")

# COMMAND ----------

built = spark.table("openalex.mag.mag_locations")
n = built.count()
husks = built.filter(~F.expr("exists(urls, x -> x.url IS NOT NULL)")).count()
dup_keys = built.groupBy("native_id").count().filter("count > 1").count()
print(f"rows: {n:,}  husks: {husks}  duplicate native_ids: {dup_keys}")
assert husks == 0
assert dup_keys == 0
assert n > 85_000_000, f"suspiciously low row count: {n:,}"
