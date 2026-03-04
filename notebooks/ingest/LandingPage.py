# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.3-py3-none-any.whl

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, TimestampType

from openalex.dlt.normalize import normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys


# COMMAND ----------

import dlt

# COMMAND ----------

MAX_ABSTRACT_LENGTH = 65535
MAX_AUTHOR_NAME_LENGTH = 1000
MAX_AFFILIATION_STRING_LENGTH = 1000

# COMMAND ----------

@dlt.table(
    comment="Landing page data streamed from Parseland results",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["native_id_namespace"]
)
def landing_page_parsed():
    return (
        spark.readStream
            .format("delta")
            .table("openalex.parseland.parsed_pages")
    )

@dlt.table(
    name="landing_page_staged",
    comment="Intermediate staging table for landing page works",
    temporary=True
)
def landing_page_staged():
    return (
        dlt.read_stream("landing_page_parsed")
        .select(
            F.col("url").alias("native_id"),
            F.lit("url").alias("native_id_namespace"),
            F.col("authors"),
            F.array(
                F.struct(
                    F.col("url").alias("id"),
                    F.lit("url").alias("namespace"),
                    F.lit("self").alias("relationship")
                ),
                F.struct(
                    F.col("native_id").alias("id"),
                    F.col("native_id_namespace").alias("namespace"),
                    F.lit(None).alias("relationship")
                ),
                F.struct(
                    F.concat(F.col("taxicab_id"), F.lit(".html.gz")).alias("id"),
                    F.lit("docs.html").alias("namespace"),
                    F.lit(None).alias("relationship")
                )
            ).alias("ids"),
            F.col("version"),
            F.when(
                F.col("license") == "other-oa",  # need to set to None due to parseland detection too broad
                F.lit(None)
            ).otherwise(
                normalize_license_udf(F.col("license"))
            ).alias("license"),
            F.when(
                F.length(F.col("abstract")) > MAX_ABSTRACT_LENGTH,
                F.substring(F.col("abstract"), 1, MAX_ABSTRACT_LENGTH)
            ).otherwise(F.col("abstract")).alias("abstract"),
            F.expr("""
                array_distinct(
                    array_union(
                        coalesce(urls, array()),
                        array_union(
                            CASE WHEN url IS NOT NULL THEN
                                array(struct(url as url, 'html' as content_type))
                            ELSE array()
                            END,
                            CASE WHEN resolved_url IS NOT NULL THEN
                                array(struct(resolved_url as url, 'html' as content_type))
                            ELSE array()
                            END
                        )
                    )
                )
            """).alias("urls"),
            F.when(
                F.col("license").isNotNull() &
                F.lower(F.col("license")).like("%cc%"),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("is_oa"),
            F.current_timestamp().alias("updated_date"),
            F.current_timestamp().alias("created_date"),
            F.col("had_error"),
        )
        .filter(
            # Drop records where parsing returned nothing useful.
            # Prevents bad re-scrapes (bot blocks, Cloudflare) from overwriting
            # existing good data via apply_changes(sequence_by="updated_date").
            (F.col("had_error") == False) &
            (
                (F.size(F.col("authors")) > 0) |
                (F.col("abstract").isNotNull() & (F.length(F.col("abstract")) > 0)) |
                (F.col("license").isNotNull() & (F.length(F.col("license")) > 0))
            )
        )
    )

# COMMAND ----------

@dlt.view(
    name="landing_page_backfill",
    comment="Landing page backfill view"
)
def landing_page_backfill():
    return (
        spark.readStream
        .format("delta")
        .option("skipChangeCommits", "true")
        .table("openalex.landing_page.landing_page_works_backfill")
        .withColumn("license",
            F.when(
                F.col("license") == "other-oa",
                F.lit(None)
            ).otherwise(normalize_license_udf(F.col("license")))
        )
    )

@dlt.table(
    name="landing_page_combined",
    comment="Combined data from staged and backfill sources"
)
def landing_page_combined():
    staged_data = dlt.read_stream("landing_page_staged")
    backfill_data = dlt.read_stream("landing_page_backfill")
    return staged_data.unionByName(backfill_data, allowMissingColumns=True)

# COMMAND ----------

@dlt.table(name="landing_page_enriched",
           comment="DataCite data after full parsing and author/feature enrichment.")
def landing_page_enriched():
    df_parsed_input = dlt.read_stream("landing_page_combined")
    df_walden_works_schema = apply_initial_processing(df_parsed_input, "landing_page", walden_works_schema)

    # enrich_with_features_and_author_keys is imported from your openalex.dlt.transform
    # It applies udf_last_name_only (Pandas UDF) and udf_f_generate_inverted_index (Pandas UDF)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return apply_final_merge_key_and_filter(df_enriched)

dlt.create_streaming_table(
    name="landing_page_works",
    comment="Final landing page works table with unique records",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

dlt.apply_changes(
    target="landing_page_works",
    source="landing_page_enriched",
    keys=["native_id"],
    sequence_by="updated_date",
    ignore_null_updates=True
)
