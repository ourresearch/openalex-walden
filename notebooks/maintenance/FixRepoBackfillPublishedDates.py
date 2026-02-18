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
# MAGIC **Two steps**:
# MAGIC 1. Fix `published_date` in `repo_works_backfill` (recompute from raw XML)
# MAGIC 2. Bump `updated_date` by 1 day ONLY for fixed records where backfill is
# MAGIC    already the winner in `repo_works` — this forces `apply_changes` to
# MAGIC    pick up the corrected dates on the next Repo pipeline run without
# MAGIC    flipping provenance on records where repo already wins.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import datetime

TARGET_TABLE = "openalex.repo.repo_works_backfill"
RAW_TABLE = "openalex.repo.repo_items_backfill"
REPO_WORKS_TABLE = "openalex.repo.repo_works"

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
                            try_to_date(
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
# MAGIC ## Step 3: MERGE fixed dates into repo_works_backfill

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
else:
    print("Nothing to update.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Bump updated_date to propagate fixes through Repo pipeline
# MAGIC
# MAGIC `apply_changes` in Repo.py uses `sequence_by="updated_date"` and only
# MAGIC replaces records when the incoming value is strictly greater. Since Step 3
# MAGIC didn't change `updated_date`, the fixes won't propagate to `repo_works`.
# MAGIC
# MAGIC We bump `updated_date` by 1 day, but ONLY for records where backfill is
# MAGIC already the winning provenance in `repo_works`. This avoids flipping
# MAGIC provenance on records where repo already wins with the correct date.

# COMMAND ----------

# Identify already-fixed records independently of Step 1.
# Fixed records have published_date far from updated_date (corrected) but
# repo_works still shows the old date because updated_date wasn't bumped.
# We find backfill records where repo_works.published_date != repo_works_backfill.published_date
# and backfill is currently winning.
backfill_winning = spark.sql(f"""
    SELECT b.native_id
    FROM {TARGET_TABLE} b
    JOIN {REPO_WORKS_TABLE} w ON b.native_id = w.native_id
    WHERE w.provenance = 'repo_backfill'
      AND b.published_date < w.published_date
""").dropDuplicates(["native_id"])

bump_count = backfill_winning.count()
print(f"Records to bump updated_date (backfill-winning with stale date in repo_works): {bump_count:,}")

if bump_count > 0:
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    delta_table.alias("target").merge(
        backfill_winning.alias("source"),
        "target.native_id = source.native_id"
    ).whenMatchedUpdate(
        set={
            "updated_date": F.date_add("target.updated_date", 1)
        }
    ).execute()

    print(f"Bumped updated_date for {bump_count:,} records")
else:
    print("No records to bump — repo_works is already up to date.")

print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify fix with known example

# COMMAND ----------

spark.sql(f"""
    SELECT native_id, published_date, created_date, updated_date, title
    FROM {TARGET_TABLE}
    WHERE native_id = 'oai:papyrus.bib.umontreal.ca:1866/36387'
""").show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Run the Repo DLT pipeline. The bumped `updated_date` on backfill-winning
# MAGIC records will cause `apply_changes` to pick up the corrected `published_date`.
# MAGIC Records where repo already wins are unaffected.
