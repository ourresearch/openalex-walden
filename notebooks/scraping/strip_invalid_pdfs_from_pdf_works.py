# Databricks notebook source
# MAGIC %md
# MAGIC # Strip invalid PDFs from `openalex.pdf.pdf_works`
# MAGIC
# MAGIC One-shot cleanup: for every pdf_works row whose `ids` array contains a
# MAGIC `docs.pdf` entry pointing at a known-bad `source_pdf_id` (per
# MAGIC `openalex.pdf.invalid_pdfs`), strip both the `docs.pdf` and the
# MAGIC corresponding `docs.parsed-pdf` (GROBID-XML cascade) entries from `ids`.
# MAGIC
# MAGIC Pairs with `notebooks/ingest/PDF.py`'s `grobid_raw` + `pdf_backfill`
# MAGIC stream-static anti-join filter. The filter prevents *future* bad
# MAGIC source_pdf_ids from entering the pipeline; this notebook handles the
# MAGIC *existing* ~2.78M rows in pdf_works.
# MAGIC
# MAGIC **Source**: `openalex.pdf.invalid_pdfs` (seeded from pdf_byte_classification)
# MAGIC **Target**: `openalex.pdf.pdf_works` (Delta MERGE, in-place UPDATE of ids)
# MAGIC
# MAGIC ## What propagates
# MAGIC
# MAGIC 1. CDF emits `update_postimage` on the touched pdf_works rows.
# MAGIC 2. `UnionAllWorksIntoLocationsParsed` consumes the CDF → apply_changes
# MAGIC    updates `locations_parsed` (same native_id, ids array minus bad entries).
# MAGIC 3. Next `CreateSuperLocations` run: `get(filter(ids, x -> x.namespace = 'docs.pdf').id, 0)`
# MAGIC    returns NULL → `pdf_s3_id` is NULL in `super_locations`.
# MAGIC 4. Next `CreateLocationsMapped` MERGE: `target.pdf_s3_id IS DISTINCT FROM
# MAGIC    source.pdf_s3_id` triggers UPDATE → `locations_mapped.pdf_s3_id = NULL`.
# MAGIC 5. Next `CreateWorksBase`: `has_content.pdf = false`.
# MAGIC 6. Next `sync_works` ES indexing: API flips.
# MAGIC
# MAGIC ## Rescrape coexistence
# MAGIC
# MAGIC A successful rescrape produces a NEW source_pdf_id (different UUID), which
# MAGIC is NOT in invalid_pdfs. apply_changes on pdf_works treats the rescrape
# MAGIC event as an UPDATE keyed by native_id and replaces the (stripped) ids
# MAGIC array with the fresh one — naturally re-adding the good docs.pdf entry.
# MAGIC No coordination needed.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `pilot_limit`: 0 = full cohort. >0 = strip only first N source_pdf_ids
# MAGIC     (for pilot validation — exercise the full propagation chain on a tiny
# MAGIC     subset before going wide).
# MAGIC   - `dry_run`: "true" = compute victim count + diff preview only, no MERGE.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full cohort)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no MERGE)")

PILOT_LIMIT = int(dbutils.widgets.get("pilot_limit"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

print(f"pilot_limit={PILOT_LIMIT:,}  dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the cohort
# MAGIC
# MAGIC `bad` has one row per bad source_pdf_id with both the `.pdf` and the
# MAGIC `.xml.gz` forms precomputed so the per-row FILTER is a straight
# MAGIC array-contains check.

# COMMAND ----------

pilot_clause = f"LIMIT {PILOT_LIMIT}" if PILOT_LIMIT > 0 else ""

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW _invalid_pdf_cohort AS
  SELECT
    source_pdf_id,
    CONCAT(source_pdf_id, '.pdf')    AS bad_pdf_id,
    CONCAT(source_pdf_id, '.xml.gz') AS bad_xml_id
  FROM openalex.pdf.invalid_pdfs
  {pilot_clause}
""")

cohort_count = spark.sql("SELECT COUNT(*) AS n FROM _invalid_pdf_cohort").collect()[0]["n"]
print(f"Cohort size: {cohort_count:,} source_pdf_ids")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find victims + compute ids-to-drop per native_id

# COMMAND ----------

# A single MERGE source: for each native_id that has at least one bad docs.pdf
# entry, collect the set of (id, namespace) pairs to drop. We then FILTER
# the target's ids array to exclude those.
spark.sql("""
  CREATE OR REPLACE TEMP VIEW _strip_plan AS
  SELECT
    p.native_id,
    COLLECT_SET(i.id) AS ids_to_drop
  FROM openalex.pdf.pdf_works p
  LATERAL VIEW EXPLODE(p.ids) AS i
  JOIN _invalid_pdf_cohort c ON
      (i.namespace = 'docs.pdf'        AND i.id = c.bad_pdf_id)
   OR (i.namespace = 'docs.parsed-pdf' AND i.id = c.bad_xml_id)
  GROUP BY p.native_id
""")

victim_count = spark.sql("SELECT COUNT(*) AS n FROM _strip_plan").collect()[0]["n"]
print(f"Victim rows in pdf_works: {victim_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview the change on 5 sample rows

# COMMAND ----------

spark.sql("""
  SELECT
    p.native_id,
    SIZE(p.ids)                                              AS ids_before_count,
    SIZE(FILTER(p.ids, x -> NOT ARRAY_CONTAINS(s.ids_to_drop, x.id))) AS ids_after_count,
    TRANSFORM(
      FILTER(p.ids, x -> ARRAY_CONTAINS(s.ids_to_drop, x.id)),
      x -> STRUCT(x.namespace, x.id)
    ) AS ids_being_dropped
  FROM openalex.pdf.pdf_works p
  JOIN _strip_plan s ON p.native_id = s.native_id
  LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE — strip the bad ids entries
# MAGIC
# MAGIC Skipped if `dry_run=true`. The MERGE only touches rows in `_strip_plan`,
# MAGIC so it's bounded by `victim_count` (much smaller than pdf_works' ~71M).

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — skipping MERGE. Set dry_run=false to apply.")
else:
    spark.sql("""
      MERGE INTO openalex.pdf.pdf_works AS t
      USING _strip_plan                  AS s
      ON t.native_id = s.native_id
      WHEN MATCHED THEN UPDATE SET
        t.ids = FILTER(t.ids, x -> NOT ARRAY_CONTAINS(s.ids_to_drop, x.id))
    """)
    print(f"MERGE complete — stripped invalid docs.pdf/docs.parsed-pdf entries from {victim_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Post-MERGE: every previously-victim row should now have zero `docs.pdf`
# MAGIC entries whose stripped id is in invalid_pdfs.

# COMMAND ----------

residual = spark.sql("""
  SELECT COUNT(*) AS n
  FROM openalex.pdf.pdf_works p
  LATERAL VIEW EXPLODE(p.ids) AS i
  JOIN _invalid_pdf_cohort c ON
      (i.namespace = 'docs.pdf'        AND i.id = c.bad_pdf_id)
   OR (i.namespace = 'docs.parsed-pdf' AND i.id = c.bad_xml_id)
""").collect()[0]["n"]

if DRY_RUN:
    print(f"DRY RUN residual (pre-MERGE): {residual:,} entries would be stripped")
else:
    print(f"POST-MERGE residual: {residual:,} bad entries remaining (expected: 0)")
