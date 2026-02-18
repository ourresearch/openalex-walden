# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Backfill Published Dates
# MAGIC
# MAGIC One-off fix for ~14M records in `repo_works_backfill` where `published_date`
# MAGIC was set to the repository deposit date instead of the actual publication date.
# MAGIC
# MAGIC **Root cause**: Before the Nov 2025 `array_min` fix, the backfill pipeline
# MAGIC grabbed the first `<dc:date>` value (often a deposit timestamp) instead of
# MAGIC the earliest date (the actual publication date).
# MAGIC
# MAGIC **Scope**: Only updates records where:
# MAGIC 1. `published_date` is within 1 day of `updated_date` (deposit date signal)
# MAGIC 2. The raw XML contains an older valid date that `array_min` would pick
# MAGIC 3. The recomputed date is actually earlier than the current one

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import datetime

TARGET_TABLE = "openalex.repo.repo_works_backfill"
RAW_TABLE = "openalex.repo.repo_items_backfill"

print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Target: {TARGET_TABLE}")
print(f"Source: {RAW_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify suspect records and recompute published_date

# COMMAND ----------

recomputed_df = spark.sql(f"""
    WITH suspect AS (
        SELECT native_id, published_date AS current_pub_date
        FROM {TARGET_TABLE}
        WHERE published_date IS NOT NULL
          AND updated_date IS NOT NULL
          AND abs(datediff(published_date, updated_date)) <= 1
    ),
    with_new_dates AS (
        SELECT
            s.native_id,
            s.current_pub_date,
            array_min(
                filter(
                    transform(
                        regexp_extract_all(i.api_raw, '<dc:date.*?>(.*?)</dc:date>'),
                        date_str -> coalesce(
                            to_date(try_to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss'Z'")),
                            to_date(try_to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss")),
                            try_to_date(date_str, 'yyyy-MM-dd'),
                            try_to_date(date_str, 'yyyy-MM'),
                            try_to_date(regexp_replace(date_str, '\\\\.', '-'), 'yyyy-MM-dd'),
                            to_date(
                                if(length(trim(date_str)) = 4, concat(date_str, '-01-01'), null),
                                'yyyy-MM-dd'
                            )
                        )
                    ),
                    d -> d IS NOT NULL AND year(d) >= 1900
                )
            ) AS new_pub_date
        FROM suspect s
        JOIN {RAW_TABLE} i ON s.native_id = i.pmh_id
    )
    SELECT native_id, new_pub_date
    FROM with_new_dates
    WHERE new_pub_date < current_pub_date
""")

recomputed_df = recomputed_df.dropDuplicates(["native_id"])
recomputed_df.cache()

count = recomputed_df.count()
print(f"Records to fix: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Preview a sample before updating

# COMMAND ----------

if count > 0:
    recomputed_df.createOrReplaceTempView("recomputed_fixes")
    sample = spark.sql(f"""
        SELECT b.native_id, b.published_date AS old_date, r.new_pub_date AS new_date,
               b.updated_date, b.title
        FROM {TARGET_TABLE} b
        JOIN recomputed_fixes r ON b.native_id = r.native_id
        LIMIT 10
    """)
    sample.show(truncate=60)
else:
    print("No records to fix.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: MERGE updated dates into repo_works_backfill

# COMMAND ----------

if count > 0:
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    delta_table.alias("target").merge(
        recomputed_df.alias("source"),
        "target.native_id = source.native_id"
    ).whenMatchedUpdate(
        set={
            "published_date": "source.new_pub_date",
            "created_date": "source.new_pub_date"
        }
    ).execute()

    print(f"Updated {count:,} records in {TARGET_TABLE}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
else:
    print("Nothing to update.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify fix with known example

# COMMAND ----------

spark.sql(f"""
    SELECT native_id, published_date, created_date, updated_date, title
    FROM {TARGET_TABLE}
    WHERE native_id = 'oai:papyrus.bib.umontreal.ca:1866/36387'
""").show(truncate=80)
