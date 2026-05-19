# Databricks notebook source
# MAGIC %md
# MAGIC # Strip invalid PDFs from `openalex.pdf.pdf_works`
# MAGIC
# MAGIC One-shot cleanup: for every pdf_works row whose `ids` array contains a
# MAGIC `docs.pdf` entry pointing at a known-bad `source_pdf_id` (per
# MAGIC `openalex.pdf.invalid_pdfs`), strip the `docs.pdf` entry AND the row's
# MAGIC `docs.parsed-pdf` entry (GROBID-XML cascade — the XML was built from
# MAGIC the same bad PDF input, so it's bad too).
# MAGIC
# MAGIC Pairs with `notebooks/ingest/PDF.py`'s `grobid_raw` + `pdf_backfill`
# MAGIC stream-static anti-join filter. The filter prevents *future* bad
# MAGIC source_pdf_ids from entering the pipeline; this notebook handles the
# MAGIC *existing* ~2.97M rows in pdf_works.
# MAGIC
# MAGIC **Source**: `openalex.pdf.invalid_pdfs` (seeded from pdf_byte_classification)
# MAGIC **Target**: `openalex.pdf.pdf_works` (Delta MERGE, in-place UPDATE of ids)
# MAGIC
# MAGIC ## Why we cascade docs.parsed-pdf wholesale on victim rows
# MAGIC
# MAGIC The GROBID XML has its OWN R2 UUID (independent of source_pdf_id — see
# MAGIC PDF.py:401 `concat(col("id"), lit(".xml.gz"))` where `id` is
# MAGIC `grobid_processing_results.id`, not `source_pdf_id`). So we can't match
# MAGIC docs.parsed-pdf entries by predicted UUID. Instead, on any victim row,
# MAGIC we drop the docs.parsed-pdf entry by cascade. Justified by:
# MAGIC   - One pdf_works row has at most one docs.pdf + one docs.parsed-pdf,
# MAGIC     both built from the same grobid_processing_results row.
# MAGIC   - 96.9% of bad-PDF rows have empty/error-stub XML (2026-05-18 probe);
# MAGIC     the residual ~3% are "successful" GROBID parses of HTML content —
# MAGIC     also bad customer-facing.
# MAGIC
# MAGIC ## What propagates
# MAGIC
# MAGIC 1. CDF emits `update_postimage` on the touched pdf_works rows.
# MAGIC 2. `UnionAllWorksIntoLocationsParsed` consumes the CDF → apply_changes
# MAGIC    updates `locations_parsed` (same native_id, ids array minus bad entries).
# MAGIC 3. Next `CreateSuperLocations` run: pdf_s3_id NULL, grobid_s3_id NULL.
# MAGIC 4. Next `CreateLocationsMapped` MERGE: IS DISTINCT FROM → UPDATE both
# MAGIC    `locations_mapped.pdf_s3_id` and `.grobid_s3_id` to NULL.
# MAGIC 5. Next `CreateWorksBase`: has_content.{pdf, grobid_xml} both false.
# MAGIC 6. Next `sync_works`: API reflects.
# MAGIC
# MAGIC ## Rescrape coexistence
# MAGIC
# MAGIC A successful rescrape produces a NEW source_pdf_id (different UUID) NOT
# MAGIC in invalid_pdfs. apply_changes on pdf_works treats the rescrape event
# MAGIC as an UPDATE keyed by native_id and replaces the (stripped) ids array
# MAGIC with the fresh one — naturally re-adding the good docs.pdf entry.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `pilot_limit`: 0 = full cohort. >0 = strip only first N source_pdf_ids.
# MAGIC   - `dry_run`: "true" = compute counts + preview only, no MERGE.

# COMMAND ----------

dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full cohort)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no MERGE)")

PILOT_LIMIT = int(dbutils.widgets.get("pilot_limit"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

print(f"pilot_limit={PILOT_LIMIT:,}  dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the cohort

# COMMAND ----------

pilot_clause = f"LIMIT {PILOT_LIMIT}" if PILOT_LIMIT > 0 else ""

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW _invalid_pdf_cohort AS
  SELECT
    source_pdf_id,
    CONCAT(source_pdf_id, '.pdf') AS bad_pdf_id
  FROM openalex.pdf.invalid_pdfs
  {pilot_clause}
""")

cohort_count = spark.sql("SELECT COUNT(*) AS n FROM _invalid_pdf_cohort").collect()[0]["n"]
print(f"Cohort size: {cohort_count:,} source_pdf_ids")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find victims + collect the specific docs.pdf ids to drop per native_id
# MAGIC
# MAGIC Subquery-EXPLODE form (avoids the `LATERAL VIEW ... JOIN` parser pitfall
# MAGIC where some Spark/DBSQL versions reject the clause order).

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TEMP VIEW _exploded_pdf_works AS
  SELECT native_id, EXPLODE(ids) AS i
  FROM openalex.pdf.pdf_works
""")

spark.sql("""
  CREATE OR REPLACE TEMP VIEW _strip_plan AS
  SELECT
    e.native_id,
    COLLECT_SET(e.i.id) AS bad_pdf_ids_to_drop
  FROM _exploded_pdf_works e
  JOIN _invalid_pdf_cohort c
    ON e.i.namespace = 'docs.pdf'
   AND e.i.id        = c.bad_pdf_id
  GROUP BY e.native_id
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
    SIZE(p.ids) AS ids_before_count,
    SIZE(FILTER(
      p.ids,
      x -> NOT (
        (x.namespace = 'docs.pdf' AND ARRAY_CONTAINS(s.bad_pdf_ids_to_drop, x.id))
        OR x.namespace = 'docs.parsed-pdf'
      )
    )) AS ids_after_count,
    TRANSFORM(
      FILTER(
        p.ids,
        x ->
          (x.namespace = 'docs.pdf' AND ARRAY_CONTAINS(s.bad_pdf_ids_to_drop, x.id))
          OR x.namespace = 'docs.parsed-pdf'
      ),
      x -> STRUCT(x.namespace, x.id)
    ) AS ids_being_dropped
  FROM openalex.pdf.pdf_works p
  JOIN _strip_plan s ON p.native_id = s.native_id
  LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE — strip docs.pdf (bad ones) + docs.parsed-pdf (cascade)

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — skipping MERGE. Set dry_run=false to apply.")
else:
    spark.sql("""
      MERGE INTO openalex.pdf.pdf_works AS t
      USING _strip_plan                  AS s
      ON t.native_id = s.native_id
      WHEN MATCHED THEN UPDATE SET
        t.ids = FILTER(
          t.ids,
          x -> NOT (
            (x.namespace = 'docs.pdf' AND ARRAY_CONTAINS(s.bad_pdf_ids_to_drop, x.id))
            OR x.namespace = 'docs.parsed-pdf'
          )
        )
    """)
    print(f"MERGE complete — stripped invalid docs.pdf + cascaded docs.parsed-pdf from {victim_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Post-MERGE on victim rows:
# MAGIC   - zero `docs.pdf` entries whose stripped id is in invalid_pdfs
# MAGIC   - zero `docs.parsed-pdf` entries (cascade)

# COMMAND ----------

residual_pdf = spark.sql("""
  SELECT COUNT(*) AS n
  FROM _exploded_pdf_works e
  JOIN _invalid_pdf_cohort c
    ON e.i.namespace = 'docs.pdf'
   AND e.i.id        = c.bad_pdf_id
""").collect()[0]["n"]

residual_xml_on_victims = spark.sql("""
  SELECT COUNT(*) AS n
  FROM _exploded_pdf_works e
  JOIN _strip_plan s ON e.native_id = s.native_id
  WHERE e.i.namespace = 'docs.parsed-pdf'
""").collect()[0]["n"]

if DRY_RUN:
    print(f"DRY RUN — would strip {residual_pdf:,} bad docs.pdf + {residual_xml_on_victims:,} cascaded docs.parsed-pdf entries")
else:
    print(f"POST-MERGE residual: docs.pdf {residual_pdf:,} (expected 0); docs.parsed-pdf on victim rows {residual_xml_on_victims:,} (expected 0)")
