# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill Landing Page Works
# MAGIC
# MAGIC This notebook propagates fixed records from `taxicab_enriched_new` to `landing_page_works`,
# MAGIC bypassing the DLT streaming pipeline which cannot reprocess MERGE'd records.
# MAGIC
# MAGIC **Background:** The RefreshStaleParserResponses job MERGE'd corrected parser responses into
# MAGIC `taxicab_enriched_new`. However, DLT streaming tables require append-only sources, so the
# MAGIC `landing_page_works_staged_new` streaming table is now blocked.
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Reset the DLT checkpoint (separate command) to unblock new records
# MAGIC 2. Run this notebook to backfill the ~1.22M fixed records directly into `landing_page_works`
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC Run as a standalone Databricks job after resetting the DLT checkpoint.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC - `start_date`: Start of affected period (default: 2025-12-27)
# MAGIC - `end_date`: End of affected period (default: 2026-01-03)
# MAGIC - `publisher_filter`: DOI prefix to filter (default: % for all)
# MAGIC - `dry_run`: If true, process but don't write (default: false)

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.3-py3-none-any.whl nameparser

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DateType
from datetime import datetime

# Import the same transformation functions used by the DLT pipeline
from openalex.dlt.normalize import (
    normalize_license_udf,
    normalize_title_udf,
    walden_works_schema,
    udf_last_name_only,
    create_merge_column,
    normalize_doi_spark_col
)
from openalex.dlt.transform import (
    apply_initial_processing,
    enrich_with_features_and_author_keys,
    apply_final_merge_key_and_filter,
    udf_f_generate_inverted_index
)

# COMMAND ----------

# Parameters
# Note: Original regression was Dec 27 - Jan 3, but those records were already backfilled.
# The current gap is Feb 2 - Feb 3 due to DLT pipeline failure after MERGE commits.
# LandingPage.py startingTimestamp is set to 2026-02-03T01:00:00, so we backfill before that.
dbutils.widgets.text("start_date", "2026-02-02", "Start Date")
dbutils.widgets.text("end_date", "2026-02-03", "End Date")
dbutils.widgets.text("publisher_filter", "%", "Publisher Filter (DOI prefix, % for all)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run")

START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")
PUBLISHER_FILTER = dbutils.widgets.get("publisher_filter")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

MAX_ABSTRACT_LENGTH = 65535

print(f"Configuration:")
print(f"  Start Date: {START_DATE}")
print(f"  End Date: {END_DATE}")
print(f"  Publisher Filter: {PUBLISHER_FILTER}")
print(f"  Dry Run: {DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Query Affected Records from taxicab_enriched_new
# MAGIC
# MAGIC These are records that were fixed by RefreshStaleParserResponses (now have authors)
# MAGIC but haven't flowed through to landing_page_works due to the DLT streaming blockage.

# COMMAND ----------

# Query records that were fixed (have authors now) in the affected date range
# We want records where the fix worked (authors > 0)
affected_query = f"""
SELECT *
FROM openalex.landing_page.taxicab_enriched_new
WHERE DATE(processed_date) BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND size(parser_response.authors) > 0
  AND parser_response.had_error = false
  AND url NOT LIKE '%/pdf%'
  AND url NOT LIKE '%.pdf%'
  AND native_id LIKE '{PUBLISHER_FILTER}'
"""

print(f"Querying affected records...")
source_df = spark.sql(affected_query)
total_affected = source_df.count()
print(f"Found {total_affected:,} records to backfill")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Apply Staging Transformations
# MAGIC
# MAGIC Replicate the transformations from `landing_page_works_staged_new` in LandingPage.py

# COMMAND ----------

# Author and URL schemas (same as LandingPage.py)
author_schema = StructType([
    StructField("name", StringType(), True),
    StructField("is_corresponding", BooleanType(), True),
    StructField("affiliations", ArrayType(
        StructType([
            StructField("name", StringType(), True)
        ])
    ), True)
])

url_schema = StructType([
    StructField("url", StringType(), True),
    StructField("content_type", StringType(), True)
])

# Apply the same transformations as landing_page_works_staged_new
staged_df = (
    source_df
    .select(
        F.col("url").alias("native_id"),
        F.lit("url").alias("native_id_namespace"),
        F.col("parser_response.authors").alias("authors"),
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
        F.col("parser_response.version").alias("version"),
        F.when(
            F.col("parser_response.license") == "other-oa",
            F.lit(None)
        ).otherwise(
            normalize_license_udf(F.col("parser_response.license"))
        ).alias("license"),
        F.when(
            F.length(F.col("parser_response.abstract")) > MAX_ABSTRACT_LENGTH,
            F.substring(F.col("parser_response.abstract"), 1, MAX_ABSTRACT_LENGTH)
        ).otherwise(F.col("parser_response.abstract")).alias("abstract"),
        F.expr("""
            array_distinct(
                array_union(
                    coalesce(parser_response.urls, array()),
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
            F.col("parser_response.license").isNotNull() &
            F.lower(F.col("parser_response.license")).like("%cc%"),
            F.lit(True)
        ).otherwise(F.lit(False)).alias("is_oa"),
        F.current_timestamp().alias("updated_date"),
        F.current_timestamp().alias("created_date"),
        F.col("parser_response.had_error").alias("had_error"),
    )
)

print(f"Staged records: {staged_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply DLT Enrichment Transformations
# MAGIC
# MAGIC Replicate the transformations from `landing_page_enriched` in LandingPage.py

# COMMAND ----------

# Apply initial processing (schema alignment, DOI normalization, author struct transformation)
df_processed = apply_initial_processing(staged_df, "landing_page", walden_works_schema)

# Apply enrichment (author key generation, abstract inverted index, feature flags)
df_enriched = enrich_with_features_and_author_keys(df_processed)

# Apply merge key and filter
df_final = apply_final_merge_key_and_filter(df_enriched)

final_count = df_final.count()
print(f"Records after enrichment and filtering: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: MERGE into landing_page_works
# MAGIC
# MAGIC Use the same key (`native_id`) and sequence logic (`updated_date`) as the DLT apply_changes

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN - showing sample of records that would be written:")
    df_final.select("native_id", "native_id_namespace", "authors", "merge_key").show(5, truncate=False)
else:
    print(f"Writing {final_count:,} records to landing_page_works...")

    # Create temp view for MERGE
    df_final.createOrReplaceTempView("backfill_source")

    # Get the columns from the target table to ensure we only update existing columns
    target_columns = [f.name for f in spark.table("openalex.landing_page.landing_page_works").schema.fields]
    source_columns = [f.name for f in df_final.schema.fields]
    common_columns = [c for c in source_columns if c in target_columns]

    # Build the SET clause dynamically
    set_clause = ", ".join([f"target.{c} = source.{c}" for c in common_columns if c != "native_id"])
    insert_columns = ", ".join(common_columns)
    insert_values = ", ".join([f"source.{c}" for c in common_columns])

    merge_sql = f"""
        MERGE INTO openalex.landing_page.landing_page_works AS target
        USING backfill_source AS source
        ON target.native_id = source.native_id
        WHEN MATCHED AND source.updated_date >= target.updated_date THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """

    start_time = datetime.now()
    result = spark.sql(merge_sql)
    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"MERGE completed in {elapsed/60:.1f} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verification

# COMMAND ----------

if not DRY_RUN:
    # Verify the records are now in landing_page_works
    verification_query = f"""
    SELECT
        COUNT(*) as total_in_landing_page_works,
        SUM(CASE WHEN size(authors) > 0 THEN 1 ELSE 0 END) as with_authors,
        SUM(CASE WHEN affiliations_exist = true THEN 1 ELSE 0 END) as with_affiliations
    FROM openalex.landing_page.landing_page_works
    WHERE native_id IN (
        SELECT url
        FROM openalex.landing_page.taxicab_enriched_new
        WHERE DATE(processed_date) BETWEEN '{START_DATE}' AND '{END_DATE}'
          AND native_id LIKE '{PUBLISHER_FILTER}'
    )
    """

    print("Verification - records in landing_page_works for affected URLs:")
    display(spark.sql(verification_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("BACKFILL SUMMARY")
print("="*60)
print(f"\nSource records from taxicab_enriched_new: {total_affected:,}")
print(f"Records after enrichment: {final_count:,}")
print(f"Date range: {START_DATE} to {END_DATE}")
print(f"Publisher filter: {PUBLISHER_FILTER}")

if DRY_RUN:
    print("\n*** DRY RUN - No records were written ***")
else:
    print(f"\n*** {final_count:,} records merged into landing_page_works ***")
    print("\nNext steps:")
    print("1. The normal pipeline will pick up these records from landing_page_works")
    print("2. They will flow to locations_parsed and then to openalex_works")
    print("3. Run the end2end job to propagate the fixes downstream")
