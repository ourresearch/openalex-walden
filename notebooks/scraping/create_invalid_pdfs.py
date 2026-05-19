# Databricks notebook source
# MAGIC %md
# MAGIC # Create + Seed `openalex.pdf.invalid_pdfs`
# MAGIC
# MAGIC Audit log of PDF `source_pdf_id` values that the byte-scanner has classified
# MAGIC as bad (HTML stored as PDF, error text, R2-missing, etc.). Consumed by
# MAGIC `notebooks/ingest/PDF.py` as a stream-static left-anti-join filter in
# MAGIC `grobid_raw` and `pdf_backfill` — flagged source_pdf_ids never enter the
# MAGIC DLT pipeline.
# MAGIC
# MAGIC **Source**: `openalex.works.pdf_byte_classification`
# MAGIC **Target**: `openalex.pdf.invalid_pdfs` (Delta, CDF enabled)
# MAGIC
# MAGIC Idempotent via MERGE on `source_pdf_id`. Safe to re-run after a fresh
# MAGIC `scan_pdf_bytes` scan adds new bad rows.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `pilot_limit`: 0 = full cohort. >0 = seed only N rows (for pilot validation).
# MAGIC   - `scan_run_id`: stamped onto every seeded row for traceability.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full cohort)")
dbutils.widgets.text("scan_run_id", "2026-05-19_full_scan_v1", "scan_run_id stamp")

PILOT_LIMIT = int(dbutils.widgets.get("pilot_limit"))
SCAN_RUN_ID = dbutils.widgets.get("scan_run_id").strip()

print(f"pilot_limit={PILOT_LIMIT:,}  scan_run_id={SCAN_RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create table if not exists
# MAGIC
# MAGIC CDF is required so PDF.py's stream-static join sees fresh rows on next
# MAGIC microbatch (and so downstream tooling can stream the additions).

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS openalex.pdf.invalid_pdfs (
    source_pdf_id  STRING NOT NULL  COMMENT 'Bare UUID, no .pdf suffix — matches grobid_processing_results.source_pdf_id',
    label          STRING            COMMENT 'Classification label from scan_pdf_bytes (HTML / TEXT_ERROR / R2_MISSING / etc.)',
    classified_at  TIMESTAMP         COMMENT 'When scan_pdf_bytes flagged this row',
    scan_run_id    STRING            COMMENT 'Traceability — which scan run flagged it'
  )
  USING DELTA
  CLUSTER BY (source_pdf_id)
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  )
  COMMENT 'Filter list for PDF.py — source_pdf_ids in here are excluded from the ingest pipeline'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the source dataframe from pdf_byte_classification

# COMMAND ----------

# Bad-cohort definition: anything that isn't a real, customer-servable PDF.
#   - VALID_PDF: real PDF bytes, keep.
#   - R2_ERROR: transient R2 read failure, leave for re-probe (don't permanently flag).
#   - Everything else (HTML / OTHER / TEXT_ERROR / R2_MISSING / JSON_ERROR /
#     GZIPPED_PDF / GZIPPED_OTHER / GZIPPED_TRUNCATED): excluded from the
#     PDF pipeline. GZIPPED_PDF is technically a valid PDF behind a gzip
#     wrapper — until/unless the gunzip-in-place fix ships, treat as
#     non-servable so customers don't get gzipped bytes with a .pdf extension.
bad_cohort = spark.sql(f"""
  SELECT
    REPLACE(pdf_s3_id, '.pdf', '')           AS source_pdf_id,
    label,
    scanned_at                                AS classified_at,
    '{SCAN_RUN_ID}'                           AS scan_run_id
  FROM openalex.works.pdf_byte_classification
  WHERE label NOT IN ('VALID_PDF', 'R2_ERROR')
    AND pdf_s3_id IS NOT NULL
""")

if PILOT_LIMIT > 0:
    bad_cohort = bad_cohort.limit(PILOT_LIMIT)

# Deduplicate by source_pdf_id (a single UUID may map to multiple work_ids)
bad_cohort = bad_cohort.dropDuplicates(["source_pdf_id"])

src_count = bad_cohort.count()
print(f"Source rows to upsert: {src_count:,}")

bad_cohort.createOrReplaceTempView("invalid_pdfs_src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into invalid_pdfs
# MAGIC
# MAGIC Idempotent: re-running with the same scan_run_id is a no-op. A fresh
# MAGIC scan_run_id will overwrite `label`/`classified_at`/`scan_run_id` on
# MAGIC matching rows (useful if a re-scan refines the label).

# COMMAND ----------

spark.sql("""
  MERGE INTO openalex.pdf.invalid_pdfs AS t
  USING invalid_pdfs_src               AS s
  ON t.source_pdf_id = s.source_pdf_id
  WHEN MATCHED THEN UPDATE SET
    t.label = s.label,
    t.classified_at = s.classified_at,
    t.scan_run_id = s.scan_run_id
  WHEN NOT MATCHED THEN INSERT (source_pdf_id, label, classified_at, scan_run_id)
    VALUES (s.source_pdf_id, s.label, s.classified_at, s.scan_run_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

total = spark.sql("SELECT COUNT(*) AS n FROM openalex.pdf.invalid_pdfs").collect()[0]["n"]
by_label = spark.sql("""
  SELECT label, COUNT(*) AS n
  FROM openalex.pdf.invalid_pdfs
  GROUP BY label
  ORDER BY n DESC
""")

print(f"openalex.pdf.invalid_pdfs total rows: {total:,}")
by_label.show(20, truncate=False)
