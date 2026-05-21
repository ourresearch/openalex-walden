# Databricks notebook source
# MAGIC %md
# MAGIC # Create + Seed `openalex.pdf.invalid_grobids`
# MAGIC
# MAGIC Audit log of grobid_uuid values that the classifier flagged as bad
# MAGIC (structurally-empty TEI envelopes + `[BAD_INPUT_DATA]` HTML wrappers +
# MAGIC other error markers). Consumed by `notebooks/ingest/PDF.py` as a
# MAGIC stream-static left-anti-join filter — flagged grobid_uuids never enter
# MAGIC the DLT pipeline going forward.
# MAGIC
# MAGIC **Source**: `openalex.works.grobid_xml_classification`
# MAGIC **Target**: `openalex.pdf.invalid_grobids` (Delta, CDF enabled)
# MAGIC
# MAGIC Idempotent via MERGE on `grobid_uuid`. Safe to re-run after a fresh
# MAGIC classifier run adds new bad rows.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `pilot_limit`: 0 = full cohort. >0 = seed only N rows (for pilot validation).
# MAGIC   - `scan_run_id`: stamped onto every seeded row for traceability.

# COMMAND ----------

dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full cohort)")
dbutils.widgets.text("scan_run_id", "2026-05-21_union_v1", "scan_run_id stamp")

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
  CREATE TABLE IF NOT EXISTS openalex.pdf.invalid_grobids (
    grobid_uuid    STRING NOT NULL  COMMENT 'Bare UUID, no .xml.gz suffix — matches grobid_processing_results.id / grobid_xml_backfill.id',
    source_pdf_id  STRING           COMMENT 'The PDF UUID whose GROBID parse produced this XML — for join debugging',
    src_table      STRING           COMMENT 'Which source table held the bad xml_content: grobid_processing_results or grobid_xml_backfill',
    label          STRING           COMMENT 'Classification: TEI_FULLY_EMPTY / TEI_EMPTY_NO_REFS / TEI_EMPTY_BODY / BAD_INPUT_DATA / NO_BLOCKS / TIMEOUT / NO_GROBID_RESPONSE / HTML_OTHER / NOT_TEI / NULL_XML',
    classified_at  TIMESTAMP,
    scan_run_id    STRING
  )
  USING DELTA
  CLUSTER BY (grobid_uuid)
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  )
  COMMENT 'Filter list for PDF.py — grobid_uuids in here are excluded from the ingest pipeline. Source: grobid_xml_classification.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the source dataframe from grobid_xml_classification
# MAGIC
# MAGIC Bad-cohort = anything that isn't TEI_VALID. Includes:
# MAGIC   - TEI_EMPTY_* (structurally empty TEI envelopes — `<body/>` self-closing)
# MAGIC   - BAD_INPUT_DATA, NO_BLOCKS, TIMEOUT, NO_GROBID_RESPONSE (taxicab/GROBID error wrappers)
# MAGIC   - HTML_OTHER, NOT_TEI, NULL_XML (other unexpected shapes)

# COMMAND ----------

bad_cohort = spark.sql(f"""
  SELECT
    grobid_uuid,
    source_pdf_id,
    src_table,
    label,
    classified_at,
    '{SCAN_RUN_ID}' AS scan_run_id
  FROM openalex.works.grobid_xml_classification
  WHERE label != 'TEI_VALID'
    AND grobid_uuid IS NOT NULL
""")

if PILOT_LIMIT > 0:
    bad_cohort = bad_cohort.limit(PILOT_LIMIT)

bad_cohort = bad_cohort.dropDuplicates(["grobid_uuid"])

src_count = bad_cohort.count()
print(f"Source rows to upsert: {src_count:,}")

bad_cohort.createOrReplaceTempView("invalid_grobids_src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into invalid_grobids
# MAGIC
# MAGIC Idempotent: re-running with the same scan_run_id is a no-op. A fresh
# MAGIC scan_run_id will overwrite label / classified_at / scan_run_id on
# MAGIC matching rows (useful if a re-classify refines the label).

# COMMAND ----------

spark.sql("""
  MERGE INTO openalex.pdf.invalid_grobids AS t
  USING invalid_grobids_src              AS s
  ON t.grobid_uuid = s.grobid_uuid
  WHEN MATCHED THEN UPDATE SET
    t.source_pdf_id = s.source_pdf_id,
    t.src_table     = s.src_table,
    t.label         = s.label,
    t.classified_at = s.classified_at,
    t.scan_run_id   = s.scan_run_id
  WHEN NOT MATCHED THEN INSERT (grobid_uuid, source_pdf_id, src_table, label, classified_at, scan_run_id)
    VALUES (s.grobid_uuid, s.source_pdf_id, s.src_table, s.label, s.classified_at, s.scan_run_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

total = spark.sql("SELECT COUNT(*) AS n FROM openalex.pdf.invalid_grobids").collect()[0]["n"]
by_label = spark.sql("""
  SELECT label, src_table, COUNT(*) AS n
  FROM openalex.pdf.invalid_grobids
  GROUP BY label, src_table
  ORDER BY n DESC
""")

print(f"openalex.pdf.invalid_grobids total rows: {total:,}")
by_label.show(30, truncate=False)
