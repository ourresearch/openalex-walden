# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Stale Parser Responses
# MAGIC
# MAGIC This notebook fixes records in `taxicab_enriched_new` where the cached `parser_response`
# MAGIC is stale (empty authors when Parseland now has data).
# MAGIC
# MAGIC **Problem:** DLT streaming tables don't re-process existing records, so stale
# MAGIC `parser_response` data persists even when the underlying HTML or parser has been fixed.
# MAGIC
# MAGIC **Solution:** Query affected UUIDs, call Parseland API directly, and UPDATE the
# MAGIC `parser_response` column in place.
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC Run as a standalone Databricks job with dedicated cluster. Does not need to
# MAGIC coordinate with the daily end2end job.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC - `start_date`: Start of affected period (default: 2025-12-27)
# MAGIC - `end_date`: End of affected period (default: 2026-01-03)
# MAGIC - `publisher_filter`: DOI prefix to filter (default: 10.1016/% for Elsevier)
# MAGIC - `batch_size`: Records per batch (default: 1000)
# MAGIC - `max_workers`: Parallel Parseland calls (default: 50)
# MAGIC - `dry_run`: If true, query and call Parseland but don't UPDATE (default: false)

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

import requests
from requests.exceptions import Timeout
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType

# COMMAND ----------

# Parameters (can be overridden via job parameters)
dbutils.widgets.text("start_date", "2025-12-27", "Start Date")
dbutils.widgets.text("end_date", "2026-01-03", "End Date")
dbutils.widgets.text("publisher_filter", "10.1016/%", "Publisher Filter (DOI prefix)")
dbutils.widgets.text("batch_size", "1000", "Batch Size")
dbutils.widgets.text("max_workers", "50", "Max Parallel Workers")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run")

START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")
PUBLISHER_FILTER = dbutils.widgets.get("publisher_filter")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

PARSELAND_URL = "http://parseland-load-balancer-667160048.us-east-1.elb.amazonaws.com/parseland"

print(f"Configuration:")
print(f"  Start Date: {START_DATE}")
print(f"  End Date: {END_DATE}")
print(f"  Publisher Filter: {PUBLISHER_FILTER}")
print(f"  Batch Size: {BATCH_SIZE}")
print(f"  Max Workers: {MAX_WORKERS}")
print(f"  Dry Run: {DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Query Affected Records

# COMMAND ----------

# Query affected UUIDs
affected_query = f"""
SELECT
    taxicab_id,
    native_id,
    url
FROM openalex.landing_page.taxicab_enriched_new
WHERE DATE(processed_date) BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND size(parser_response.authors) = 0
  AND parser_response.had_error = false
  AND url NOT LIKE '%/pdf%'
  AND url NOT LIKE '%.pdf%'
  AND native_id LIKE '{PUBLISHER_FILTER}'
"""

print(f"Querying affected records...")
affected_df = spark.sql(affected_query)
total_affected = affected_df.count()
print(f"Found {total_affected:,} affected records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Parseland Call Functions

# COMMAND ----------

# Schema for parser response (must match existing schema in taxicab_enriched_new)
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

response_schema = StructType([
    StructField("authors", ArrayType(author_schema), True),
    StructField("urls", ArrayType(url_schema), True),
    StructField("license", StringType(), True),
    StructField("version", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("had_error", BooleanType(), True)
])

# Schema for the update DataFrame (explicit to avoid type inference failures on empty arrays)
update_schema = StructType([
    StructField("taxicab_id", StringType(), True),
    StructField("new_authors", ArrayType(author_schema), True),
    StructField("new_urls", ArrayType(url_schema), True),
    StructField("new_license", StringType(), True),
    StructField("new_version", StringType(), True),
    StructField("new_abstract", StringType(), True),
    StructField("new_had_error", BooleanType(), True)
])


def call_parseland(taxicab_id, max_retries=2, timeout=20):
    """Call Parseland API for a single UUID."""
    retries = 0
    while retries <= max_retries:
        try:
            response = requests.get(f"{PARSELAND_URL}/{taxicab_id}", timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                return {
                    "taxicab_id": taxicab_id,
                    "success": True,
                    "authors": data.get("authors", []) or [],
                    "urls": data.get("urls", []) or [],
                    "license": str(data.get("license", "")) if data.get("license") else "",
                    "version": str(data.get("version", "")) if data.get("version") else "",
                    "abstract": str(data.get("abstract", "")) if data.get("abstract") else "",
                    "had_error": False
                }
            elif response.status_code == 504:
                time.sleep(1 * (retries + 1))
                retries += 1
                continue
            else:
                return {
                    "taxicab_id": taxicab_id,
                    "success": False,
                    "error": f"HTTP {response.status_code}"
                }

        except Timeout:
            time.sleep(2 * (retries + 1))
            retries += 1
            continue
        except Exception as e:
            return {
                "taxicab_id": taxicab_id,
                "success": False,
                "error": str(e)
            }

    return {
        "taxicab_id": taxicab_id,
        "success": False,
        "error": "Max retries exceeded"
    }


def process_batch(taxicab_ids):
    """Process a batch of UUIDs in parallel."""
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(call_parseland, tid): tid for tid in taxicab_ids}

        for future in as_completed(future_to_id):
            try:
                result = future.result(timeout=30)
                results.append(result)
            except Exception as e:
                tid = future_to_id[future]
                results.append({
                    "taxicab_id": tid,
                    "success": False,
                    "error": str(e)
                })

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Process in Batches and UPDATE

# COMMAND ----------

# Collect all affected UUIDs
print("Collecting affected UUIDs...")
affected_records = affected_df.select("taxicab_id").collect()
taxicab_ids = [row.taxicab_id for row in affected_records]
print(f"Collected {len(taxicab_ids):,} UUIDs")

# COMMAND ----------

# Process in batches
total_batches = (len(taxicab_ids) + BATCH_SIZE - 1) // BATCH_SIZE
successful_updates = 0
failed_updates = 0
records_with_authors = 0

print(f"\nProcessing {len(taxicab_ids):,} records in {total_batches} batches...")
print(f"Estimated time: {len(taxicab_ids) / (MAX_WORKERS * 60):.1f} minutes\n")

start_time = datetime.now()

for batch_num in range(total_batches):
    batch_start = batch_num * BATCH_SIZE
    batch_end = min(batch_start + BATCH_SIZE, len(taxicab_ids))
    batch_ids = taxicab_ids[batch_start:batch_end]

    # Call Parseland for this batch
    results = process_batch(batch_ids)

    # Separate successful and failed
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    failed_updates += len(failed)

    if successful and not DRY_RUN:
        # Create DataFrame with new parser_response values
        update_data = []
        for r in successful:
            has_authors = len(r.get("authors", [])) > 0
            if has_authors:
                records_with_authors += 1

            update_data.append({
                "taxicab_id": r["taxicab_id"],
                "new_authors": r.get("authors", []),
                "new_urls": r.get("urls", []),
                "new_license": r.get("license", ""),
                "new_version": r.get("version", ""),
                "new_abstract": r.get("abstract", ""),
                "new_had_error": r.get("had_error", False)
            })

        # Create temp view and UPDATE (use explicit schema to handle empty arrays)
        # Deduplicate by taxicab_id to avoid MERGE conflicts
        update_df = spark.createDataFrame(update_data, schema=update_schema)
        update_df = update_df.dropDuplicates(["taxicab_id"])
        update_df.createOrReplaceTempView("batch_updates")

        # UPDATE using MERGE
        spark.sql("""
            MERGE INTO openalex.landing_page.taxicab_enriched_new AS target
            USING batch_updates AS source
            ON target.taxicab_id = source.taxicab_id
            WHEN MATCHED THEN UPDATE SET
                target.parser_response = struct(
                    source.new_authors AS authors,
                    source.new_urls AS urls,
                    source.new_license AS license,
                    source.new_version AS version,
                    source.new_abstract AS abstract,
                    source.new_had_error AS had_error
                )
        """)

        successful_updates += len(successful)

    elif successful and DRY_RUN:
        # Dry run - just count
        for r in successful:
            if len(r.get("authors", [])) > 0:
                records_with_authors += 1
        successful_updates += len(successful)

    # Progress update every 10 batches
    if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = (batch_end) / elapsed if elapsed > 0 else 0
        eta_seconds = (len(taxicab_ids) - batch_end) / rate if rate > 0 else 0

        print(f"Batch {batch_num + 1}/{total_batches}: "
              f"Processed {batch_end:,}/{len(taxicab_ids):,} "
              f"({rate:.0f}/sec, ETA: {eta_seconds/60:.1f} min) "
              f"| With authors: {records_with_authors:,} | Failed: {failed_updates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Summary

# COMMAND ----------

elapsed_total = (datetime.now() - start_time).total_seconds()

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"\nTotal records processed: {len(taxicab_ids):,}")
print(f"Successful Parseland calls: {successful_updates:,}")
print(f"Failed Parseland calls: {failed_updates:,}")
print(f"Records with authors extracted: {records_with_authors:,}")
print(f"Success rate: {100*records_with_authors/max(successful_updates,1):.1f}%")
print(f"\nTotal time: {elapsed_total/60:.1f} minutes")
print(f"Average rate: {len(taxicab_ids)/elapsed_total:.1f} records/second")

if DRY_RUN:
    print("\n*** DRY RUN - No records were actually updated ***")
else:
    print(f"\n*** {successful_updates:,} records updated in taxicab_enriched_new ***")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verification Query
# MAGIC
# MAGIC Run this after the job completes to verify the fix worked.

# COMMAND ----------

if not DRY_RUN:
    verification_query = f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as with_authors,
        SUM(CASE WHEN size(parser_response.authors) = 0 THEN 1 ELSE 0 END) as without_authors
    FROM openalex.landing_page.taxicab_enriched_new
    WHERE DATE(processed_date) BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND url NOT LIKE '%/pdf%'
      AND native_id LIKE '{PUBLISHER_FILTER}'
    """

    print("Verification Query Results:")
    display(spark.sql(verification_query))
