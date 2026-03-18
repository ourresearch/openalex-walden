# Databricks notebook source
# MAGIC %md
# MAGIC # RepoBackfill Test - Single Endpoint
# MAGIC
# MAGIC Tests the backfill logic on ONE endpoint (~3K rows) to validate:
# MAGIC 1. Logic works correctly
# MAGIC 2. `repository_id` column is populated
# MAGIC 3. Get real timing data for extrapolation

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Test configuration
TEST_ENDPOINT = "18de06b670edd240b21"  # ~3,005 rows (median size)
TARGET_TABLE = "openalex.repo.repo_works_backfill_test"  # Write to test table, not prod

print(f"=" * 60)
print(f"RepoBackfill Test Run")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S CT')}")
print(f"Test endpoint: {TEST_ENDPOINT}")
print(f"Target table: {TARGET_TABLE}")
print(f"=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load source data for test endpoint

# COMMAND ----------

step_start = time.time()

# Load just our test endpoint
df = spark.table("openalex.repo.repo_items_backfill").filter(col("endpoint_id") == TEST_ENDPOINT)
row_count = df.count()

step_time = time.time() - step_start
print(f"✓ Step 1: Loaded {row_count:,} rows in {step_time:.1f}s")
print(f"  Rate: {row_count/step_time:.0f} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean XML data

# COMMAND ----------

step_start = time.time()

clean_df = df.withColumn("cleaned_xml",
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("api_raw"),
                        r'^"{3}', ''
                    ),
                    r'"{3}$', ''
                ),
                r'\\\"', '"'
            ),
            r'""', '"'
        )
    )
).drop("api_raw")

# Force evaluation
clean_count = clean_df.count()
step_time = time.time() - step_start
print(f"✓ Step 2: Cleaned XML for {clean_count:,} rows in {step_time:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse and transform (the big step)

# COMMAND ----------

import re
import unicodedata
import pandas as pd

# UDFs (copied from RepoBackfill.py)
def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""
    if isinstance(title, bytes):
        title = str(title, 'ascii')
    text = title[0:500]
    text = text.lower()
    text = remove_accents(text)
    text = clean_html(text)
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)
    text = remove_everything_but_alphas(text)
    return text.strip()

# Type mappings
TYPES_TO_DELETE = [
    "person", "image", "newspaper", "info:eu-repo/semantics/lecture", "photograph",
    "bildband", "dvd-video", "video", "fotografia", "cd", "sound recording",
    "text and image", "moving image", "photographs", "cd-rom",
    "blu-ray-disc", "stillimage", "image; text", "image;stillimage", "still image",
    "image;", "ilustraciones y fotos", "fotografie", "fotografía"
]

REPO_TYPE_MAPPING = {
    "article": "article", "journal article": "article", "text": "article",
    "book": "book", "thesis": "dissertation", "dissertation": "dissertation",
    "report": "report", "preprint": "preprint", "dataset": "dataset",
}

def get_openalex_type_from_repo(input_type):
    if not input_type or not isinstance(input_type, str):
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

# Register UDFs
@pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

@pandas_udf(StringType())
def get_openalex_type_udf(type_series: pd.Series) -> pd.Series:
    return type_series.apply(get_openalex_type_from_repo)

@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

# COMMAND ----------

step_start = time.time()

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500

spark.conf.set("spark.sql.ansi.enabled", "false")

parsed_df = clean_df \
    .withColumn("native_id", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
    .withColumn("native_id_namespace", lit("pmh")) \
    .withColumn("title", substring(regexp_extract(col("cleaned_xml"), r"<dc:title.*?>(.*?)</dc:title>", 1), 0, MAX_TITLE_LENGTH)) \
    .withColumn("normalized_title", normalize_title_udf(col("title"))) \
    .withColumn("authors",
        expr(f"""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:creator>(.*?)</dc:creator>'),
                x -> struct(
                    cast(null as string) as given,
                    cast(null as string) as family,
                    substring(x, 0, {MAX_AUTHOR_NAME_LENGTH}) as name,
                    cast(null as string) as orcid,
                    array(struct(
                        cast(null as string) as name,
                        cast(null as string) as department,
                        cast(null as string) as ror_id
                    )) as affiliations
                )
            )
        """)) \
    .withColumn("raw_native_type", regexp_extract(col("cleaned_xml"), r"<dc:type.*?>(.*?)</dc:type>", 1)) \
    .withColumn("type", get_openalex_type_udf(col("raw_native_type"))) \
    .filter(~lower(col("raw_native_type")).isin(TYPES_TO_DELETE)) \
    .withColumn("ids", array(
        struct(
            col("native_id").alias("id"),
            lit("pmh").alias("namespace"),
            lit("self").alias("relationship")
        )
    )) \
    .withColumn("version", lit("submittedVersion")) \
    .withColumn("language", lit(None).cast("string")) \
    .withColumn("published_date", lit(None).cast("date")) \
    .withColumn("created_date", lit(None).cast("date")) \
    .withColumn("updated_date", to_date(
        regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
    .withColumn("abstract",
        substring(element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1), 0, MAX_ABSTRACT_LENGTH)) \
    .withColumn("source_name",
        regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
    .withColumn("publisher",
        regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
    .withColumn("urls", array(struct(
        lit(None).cast("string").alias("url"),
        lit(None).cast("string").alias("content_type")
    ))) \
    .withColumn("raw_license",
        element_at(expr("regexp_extract_all(cleaned_xml, '<dc:rights>(.*?)</dc:rights>')"), 1)) \
    .withColumn("license", normalize_license_udf(col("raw_license"))) \
    .withColumn("issue", lit(None).cast("string")) \
    .withColumn("volume", lit(None).cast("string")) \
    .withColumn("first_page", lit(None).cast("string")) \
    .withColumn("last_page", lit(None).cast("string")) \
    .withColumn("is_retracted", lit(None).cast("boolean")) \
    .withColumn("funders", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("ror"),
            lit(None).cast("string").alias("name"),
            array(lit(None).cast("string")).alias("awards")
        )
    )) \
    .withColumn("references", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("pmid"),
            lit(None).cast("string").alias("arxiv"),
            lit(None).cast("string").alias("title"),
            lit(None).cast("string").alias("authors"),
            lit(None).cast("string").alias("year"),
            lit(None).cast("string").alias("raw")
        )
    )) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn("is_oa", lit(False)) \
    .withColumn("repository_id", col("endpoint_id"))  # THE KEY COLUMN!

# Select final columns
parsed_df = parsed_df.select(
    "native_id", "native_id_namespace", "title", "normalized_title", "authors",
    "ids", "raw_native_type", "type", "version", "license", "language",
    "published_date", "created_date", "updated_date", "issue", "volume",
    "first_page", "last_page", "is_retracted", "abstract", "source_name",
    "publisher", "funders", "references", "urls", "mesh", "is_oa", "repository_id"
)

# Deduplicate
parsed_df = parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])

# Force evaluation
parsed_count = parsed_df.count()
step_time = time.time() - step_start
print(f"✓ Step 3: Parsed {parsed_count:,} rows in {step_time:.1f}s")
print(f"  Rate: {parsed_count/step_time:.0f} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate output

# COMMAND ----------

print("=" * 60)
print("OUTPUT VALIDATION")
print("=" * 60)

# Check repository_id is populated
repo_id_check = parsed_df.select("repository_id").filter(col("repository_id").isNotNull()).count()
print(f"✓ repository_id populated: {repo_id_check:,}/{parsed_count:,} rows ({100*repo_id_check/parsed_count:.1f}%)")

# Check repository_id value
sample_repo_id = parsed_df.select("repository_id").first()[0]
print(f"  Sample repository_id: {sample_repo_id}")
print(f"  Expected: {TEST_ENDPOINT}")
print(f"  Match: {'✓ YES' if sample_repo_id == TEST_ENDPOINT else '✗ NO'}")

# Show sample rows
print("\nSample output (5 rows):")
parsed_df.select("native_id", "title", "type", "repository_id").show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to test table

# COMMAND ----------

step_start = time.time()

# Write to test table (not prod!)
parsed_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TARGET_TABLE)

step_time = time.time() - step_start
print(f"✓ Step 5: Wrote {parsed_count:,} rows in {step_time:.1f}s")
print(f"  Rate: {parsed_count/step_time:.0f} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Extrapolation

# COMMAND ----------

# Get total time
print("=" * 60)
print("SUMMARY")
print("=" * 60)

# Calculate extrapolation
total_rows = 310_686_040
total_endpoints = 4980
rows_processed = parsed_count
# Assume we tested a median-sized endpoint

print(f"Test endpoint: {TEST_ENDPOINT}")
print(f"Rows processed: {rows_processed:,}")
print(f"")
print(f"EXTRAPOLATION TO FULL DATASET:")
print(f"  Total rows: {total_rows:,}")
print(f"  Total endpoints: {total_endpoints:,}")
print(f"")
print(f"If processing endpoint-by-endpoint with checkpointing:")
print(f"  - Each checkpoint saves progress")
print(f"  - Can resume from any failure")
print(f"  - Progress visible every ~3K rows")
print(f"")
print("Next: Run RepoBackfillChunked.py for full dataset with progress tracking")
