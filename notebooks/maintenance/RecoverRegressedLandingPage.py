# Databricks notebook source
# MAGIC %md
# MAGIC # Recover Regressed Landing Page Records
# MAGIC
# MAGIC One-off backfill for ~59K records in `landing_page_works` where good author
# MAGIC data was overwritten by bad re-scrapes (bot blocks, Cloudflare pages,
# MAGIC transient Parseland failures). The `apply_changes(sequence_by="updated_date")`
# MAGIC pipeline accepted the newer but empty records.
# MAGIC
# MAGIC **Strategy**: Find the best historical record in `taxicab_enriched_new` for
# MAGIC each regressed URL, transform it into `landing_page_works_backfill` schema,
# MAGIC and INSERT with `CURRENT_TIMESTAMP()` so it wins on the next pipeline run.
# MAGIC
# MAGIC **Why this is safe**: `landing_page_works_backfill` is a streaming source
# MAGIC with `skipChangeCommits=true` — INSERT (append) is the approved operation.

# COMMAND ----------

from datetime import datetime

BACKFILL_TABLE = "openalex.landing_page.landing_page_works_backfill"
ENRICHED_TABLE = "openalex.landing_page.taxicab_enriched_new"
LP_WORKS_TABLE = "openalex.landing_page.landing_page_works"

print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

regressed_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {LP_WORKS_TABLE} lp
    WHERE SIZE(lp.authors) = 0
      AND EXISTS (
        SELECT 1 FROM {ENRICHED_TABLE} te
        WHERE te.url = lp.native_id
          AND SIZE(te.parser_response.authors) > 0
          AND te.parser_response.had_error = false
      )
""").collect()[0]["cnt"]

print(f"Regressed records (0 authors in landing_page_works, good data in taxicab_enriched_new): {regressed_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dry Run — Preview 10 sample records

# COMMAND ----------

sample = spark.sql(f"""
    SELECT
      best.native_id,
      best.author_count,
      best.version,
      best.license,
      best.is_oa,
      best.created_date
    FROM (
      SELECT
        te.url as native_id,
        SIZE(te.parser_response.authors) as author_count,
        te.parser_response.version as version,
        te.parser_response.license as license,
        CASE WHEN te.parser_response.license IS NOT NULL
          AND LOWER(te.parser_response.license) LIKE '%cc%' THEN true ELSE false END as is_oa,
        te.created_date,
        ROW_NUMBER() OVER (
          PARTITION BY te.url
          ORDER BY SIZE(te.parser_response.authors) DESC, te.created_date DESC
        ) as rn
      FROM {ENRICHED_TABLE} te
      INNER JOIN {LP_WORKS_TABLE} lp
        ON te.url = lp.native_id
      WHERE SIZE(te.parser_response.authors) > 0
        AND te.parser_response.had_error = false
        AND SIZE(lp.authors) = 0
    ) best
    WHERE best.rn = 1
    LIMIT 10
""")

sample.show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Backfill
# MAGIC
# MAGIC INSERT best historical records into `landing_page_works_backfill`.
# MAGIC `CURRENT_TIMESTAMP()` as `updated_date` ensures these win over the
# MAGIC bad records on the next `apply_changes` run.

# COMMAND ----------

result = spark.sql(f"""
    INSERT INTO {BACKFILL_TABLE}
    SELECT
      best.url as native_id,
      'url' as native_id_namespace,
      best.authors,
      best.ids,
      best.version,
      best.license,
      best.abstract,
      best.urls,
      best.is_oa,
      CURRENT_TIMESTAMP() as updated_date,
      best.created_date,
      false as had_error
    FROM (
      SELECT te.url, te.native_id, te.native_id_namespace, te.taxicab_id,
        te.resolved_url, te.created_date,
        te.parser_response.authors as authors,
        array(
          named_struct('id', te.url, 'namespace', 'url', 'relationship', 'self'),
          named_struct('id', te.native_id, 'namespace', te.native_id_namespace, 'relationship', cast(null as string)),
          named_struct('id', concat(te.taxicab_id, '.html.gz'), 'namespace', 'docs.html', 'relationship', cast(null as string))
        ) as ids,
        te.parser_response.version as version,
        te.parser_response.license as license,
        CASE WHEN LENGTH(te.parser_response.abstract) > 65535
          THEN SUBSTRING(te.parser_response.abstract, 1, 65535)
          ELSE te.parser_response.abstract END as abstract,
        array_distinct(array_union(
          coalesce(te.parser_response.urls, array()),
          array_union(
            array(named_struct('url', te.url, 'content_type', 'html')),
            CASE WHEN te.resolved_url IS NOT NULL
              THEN array(named_struct('url', te.resolved_url, 'content_type', 'html'))
              ELSE array() END
          )
        )) as urls,
        CASE WHEN te.parser_response.license IS NOT NULL
          AND LOWER(te.parser_response.license) LIKE '%cc%' THEN true ELSE false END as is_oa,
        ROW_NUMBER() OVER (
          PARTITION BY te.url
          ORDER BY SIZE(te.parser_response.authors) DESC, te.created_date DESC
        ) as rn
      FROM {ENRICHED_TABLE} te
      INNER JOIN {LP_WORKS_TABLE} lp
        ON te.url = lp.native_id
      WHERE SIZE(te.parser_response.authors) > 0
        AND te.parser_response.had_error = false
        AND SIZE(lp.authors) = 0
    ) best
    WHERE best.rn = 1
""")

print(f"Backfill INSERT complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Count recently inserted records and spot-check a few.

# COMMAND ----------

verify = spark.sql(f"""
    SELECT COUNT(*) as backfilled_count
    FROM {BACKFILL_TABLE}
    WHERE updated_date >= current_date()
""")
verify.show()

spot_check = spark.sql(f"""
    SELECT native_id, SIZE(authors) as author_count, updated_date, is_oa, version
    FROM {BACKFILL_TABLE}
    WHERE updated_date >= current_date()
    ORDER BY SIZE(authors) DESC
    LIMIT 10
""")
spot_check.show(truncate=80)

print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("Next: Run the LandingPageWork pipeline to propagate these records through apply_changes.")
