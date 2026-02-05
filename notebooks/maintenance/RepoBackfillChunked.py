# Databricks notebook source
# MAGIC %md
# MAGIC # RepoBackfill - Chunked with Progress Tracking
# MAGIC
# MAGIC Processes repo backfill data endpoint-by-endpoint with:
# MAGIC - ✅ Checkpointing (resume from where we left off)
# MAGIC - ✅ Progress reporting (every endpoint)
# MAGIC - ✅ Rolling rate calculation
# MAGIC - ✅ ETA estimation
# MAGIC - ✅ Error logging

# COMMAND ----------

import time
from datetime import datetime, timedelta
from collections import deque
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import re
import unicodedata

# Configuration
TARGET_TABLE = "openalex.repo.repo_works_backfill"
CHECKPOINT_TABLE = "openalex.repo.repo_backfill_checkpoint"
BATCH_LOG_INTERVAL = 10  # Log every N endpoints
ROLLING_WINDOW = 50  # Calculate rate over last N endpoints

print("=" * 70)
print("RepoBackfill Chunked - Production Run")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S CT')}")
print(f"Target: {TARGET_TABLE}")
print(f"Checkpoint: {CHECKPOINT_TABLE}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: UDFs and helper functions

# COMMAND ----------

# UDFs (same as RepoBackfill.py)
def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
    return re.sub(cleanr, '', raw_html) if raw_html else ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""
    if isinstance(title, bytes):
        title = str(title, 'ascii')
    text = title[0:500].lower()
    text = remove_accents(text)
    text = clean_html(text)
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)
    text = "".join(e for e in text if e.isalpha())
    return text.strip()

TYPES_TO_DELETE = [
    "person", "image", "newspaper", "info:eu-repo/semantics/lecture", "photograph",
    "bildband", "dvd-video", "video", "fotografia", "cd", "sound recording",
    "text and image", "moving image", "photographs", "cd-rom", "blu-ray-disc",
    "stillimage", "image; text", "image;stillimage", "still image", "image;",
    "ilustraciones y fotos", "fotografie", "fotografía"
]

REPO_TYPE_MAPPING = {
    "article": "article", "journal article": "article", "text": "article",
    "book": "book", "thesis": "dissertation", "dissertation": "dissertation",
    "report": "report", "preprint": "preprint", "dataset": "dataset",
}

def get_openalex_type_from_repo(input_type):
    if not input_type:
        return "other"
    input_type_lower = input_type.strip().lower()
    if "info:eu-repo/semantics/" in input_type_lower:
        input_type_lower = input_type_lower.split("info:eu-repo/semantics/")[-1]
    return REPO_TYPE_MAPPING.get(input_type_lower, "other")

def normalize_license(text):
    if not text:
        return None
    normalized_text = text.replace(" ", "").replace("-", "").lower()
    if "ccby" in normalized_text:
        return "cc-by"
    if "openaccess" in normalized_text:
        return "other-oa"
    return None

@pandas_udf(StringType())
def normalize_title_udf(s: pd.Series) -> pd.Series:
    return s.apply(normalize_title)

@pandas_udf(StringType())
def get_openalex_type_udf(s: pd.Series) -> pd.Series:
    return s.apply(get_openalex_type_from_repo)

@pandas_udf(StringType())
def normalize_license_udf(s: pd.Series) -> pd.Series:
    return s.apply(normalize_license)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get endpoint list and checkpoint state

# COMMAND ----------

# Get all endpoints with row counts
print("Loading endpoint list...")
endpoint_df = spark.sql("""
    SELECT endpoint_id, COUNT(*) as row_count
    FROM openalex.repo.repo_items_backfill
    GROUP BY endpoint_id
    ORDER BY row_count ASC
""").collect()

endpoints = [(row.endpoint_id, row.row_count) for row in endpoint_df]
total_endpoints = len(endpoints)
total_rows = sum(r[1] for r in endpoints)

print(f"✓ Found {total_endpoints:,} endpoints with {total_rows:,} total rows")

# Get already-processed endpoints (checkpoint)
try:
    done_df = spark.sql(f"SELECT DISTINCT repository_id FROM {TARGET_TABLE}")
    done_endpoints = set(row.repository_id for row in done_df.collect())
    print(f"✓ Checkpoint: {len(done_endpoints):,} endpoints already processed")
except:
    done_endpoints = set()
    print("✓ No checkpoint found - starting fresh")

remaining = [(e, r) for e, r in endpoints if e not in done_endpoints]
remaining_rows = sum(r[1] for r in remaining)
print(f"✓ Remaining: {len(remaining):,} endpoints with {remaining_rows:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Process endpoints with progress tracking

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500

spark.conf.set("spark.sql.ansi.enabled", "false")

def process_endpoint(endpoint_id):
    """Process a single endpoint and return row count."""
    # Load data
    df = spark.table("openalex.repo.repo_items_backfill").filter(col("endpoint_id") == endpoint_id)

    # Clean XML
    clean_df = df.withColumn("cleaned_xml",
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
            col("api_raw"), r'^"{3}', ''), r'"{3}$', ''), r'\\\"', '"'), r'""', '"'))
    ).drop("api_raw")

    # Parse
    parsed_df = clean_df \
        .withColumn("native_id", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
        .withColumn("native_id_namespace", lit("pmh")) \
        .withColumn("title", substring(regexp_extract(col("cleaned_xml"), r"<dc:title.*?>(.*?)</dc:title>", 1), 0, MAX_TITLE_LENGTH)) \
        .withColumn("normalized_title", normalize_title_udf(col("title"))) \
        .withColumn("authors", expr(f"""
            transform(regexp_extract_all(cleaned_xml, '<dc:creator>(.*?)</dc:creator>'),
                x -> struct(cast(null as string) as given, cast(null as string) as family,
                    substring(x, 0, {MAX_AUTHOR_NAME_LENGTH}) as name, cast(null as string) as orcid,
                    array(struct(cast(null as string) as name, cast(null as string) as department,
                        cast(null as string) as ror_id)) as affiliations))
        """)) \
        .withColumn("raw_native_type", regexp_extract(col("cleaned_xml"), r"<dc:type.*?>(.*?)</dc:type>", 1)) \
        .withColumn("type", get_openalex_type_udf(col("raw_native_type"))) \
        .filter(~lower(col("raw_native_type")).isin(TYPES_TO_DELETE)) \
        .withColumn("ids", array(struct(col("native_id").alias("id"), lit("pmh").alias("namespace"), lit("self").alias("relationship")))) \
        .withColumn("version", lit("submittedVersion")) \
        .withColumn("language", lit(None).cast("string")) \
        .withColumn("published_date", lit(None).cast("date")) \
        .withColumn("created_date", lit(None).cast("date")) \
        .withColumn("updated_date", to_date(regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
        .withColumn("abstract", substring(element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1), 0, MAX_ABSTRACT_LENGTH)) \
        .withColumn("source_name", regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
        .withColumn("publisher", regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
        .withColumn("urls", array(struct(lit(None).cast("string").alias("url"), lit(None).cast("string").alias("content_type")))) \
        .withColumn("raw_license", element_at(expr("regexp_extract_all(cleaned_xml, '<dc:rights>(.*?)</dc:rights>')"), 1)) \
        .withColumn("license", normalize_license_udf(col("raw_license"))) \
        .withColumn("issue", lit(None).cast("string")) \
        .withColumn("volume", lit(None).cast("string")) \
        .withColumn("first_page", lit(None).cast("string")) \
        .withColumn("last_page", lit(None).cast("string")) \
        .withColumn("is_retracted", lit(None).cast("boolean")) \
        .withColumn("funders", array(struct(lit(None).cast("string").alias("doi"), lit(None).cast("string").alias("ror"),
            lit(None).cast("string").alias("name"), array(lit(None).cast("string")).alias("awards")))) \
        .withColumn("references", array(struct(lit(None).cast("string").alias("doi"), lit(None).cast("string").alias("pmid"),
            lit(None).cast("string").alias("arxiv"), lit(None).cast("string").alias("title"),
            lit(None).cast("string").alias("authors"), lit(None).cast("string").alias("year"), lit(None).cast("string").alias("raw")))) \
        .withColumn("mesh", lit(None).cast("string")) \
        .withColumn("is_oa", lit(False)) \
        .withColumn("repository_id", col("endpoint_id"))

    # Select and dedupe
    parsed_df = parsed_df.select(
        "native_id", "native_id_namespace", "title", "normalized_title", "authors",
        "ids", "raw_native_type", "type", "version", "license", "language",
        "published_date", "created_date", "updated_date", "issue", "volume",
        "first_page", "last_page", "is_retracted", "abstract", "source_name",
        "publisher", "funders", "references", "urls", "mesh", "is_oa", "repository_id"
    ).sort(col("updated_date").desc()).dropDuplicates(["native_id"])

    row_count = parsed_df.count()

    # Append to target table (not MERGE - append is faster for chunked approach)
    parsed_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

    return row_count

# COMMAND ----------

# Progress tracking
start_time = time.time()
rows_processed = 0
endpoints_processed = 0
recent_times = deque(maxlen=ROLLING_WINDOW)
errors = []

print("\n" + "=" * 70)
print("PROCESSING ENDPOINTS")
print("=" * 70 + "\n")

for i, (endpoint_id, expected_rows) in enumerate(remaining):
    endpoint_start = time.time()

    try:
        actual_rows = process_endpoint(endpoint_id)
        rows_processed += actual_rows
        endpoints_processed += 1

        endpoint_time = time.time() - endpoint_start
        recent_times.append((actual_rows, endpoint_time))

        # Calculate rolling rate
        if recent_times:
            recent_rows = sum(r for r, t in recent_times)
            recent_time = sum(t for r, t in recent_times)
            rolling_rate = recent_rows / recent_time if recent_time > 0 else 0
        else:
            rolling_rate = actual_rows / endpoint_time if endpoint_time > 0 else 0

        # Calculate ETA
        remaining_rows = remaining_rows - actual_rows
        eta_seconds = remaining_rows / rolling_rate if rolling_rate > 0 else 0
        eta = timedelta(seconds=int(eta_seconds))

        # Log progress
        if (i + 1) % BATCH_LOG_INTERVAL == 0 or i == 0:
            elapsed = timedelta(seconds=int(time.time() - start_time))
            pct = 100 * rows_processed / total_rows
            print(f"[{i+1:,}/{len(remaining):,}] {endpoint_id[:12]}... | "
                  f"{actual_rows:,} rows | {rolling_rate:,.0f} rows/sec | "
                  f"{pct:.1f}% done | ETA: {eta} | Elapsed: {elapsed}")

    except Exception as e:
        error_msg = f"[{i+1}] {endpoint_id}: {str(e)[:100]}"
        errors.append(error_msg)
        print(f"✗ ERROR: {error_msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = time.time() - start_time
print("\n" + "=" * 70)
print("COMPLETE")
print("=" * 70)
print(f"Endpoints processed: {endpoints_processed:,}")
print(f"Rows processed: {rows_processed:,}")
print(f"Total time: {timedelta(seconds=int(elapsed))}")
print(f"Average rate: {rows_processed/elapsed:,.0f} rows/sec")
print(f"Errors: {len(errors)}")

if errors:
    print("\nErrors encountered:")
    for e in errors[:10]:
        print(f"  - {e}")
    if len(errors) > 10:
        print(f"  ... and {len(errors) - 10} more")
