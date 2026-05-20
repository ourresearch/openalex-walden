# Databricks notebook source
# MAGIC %md
# MAGIC # Classify GROBID XMLs (unified — both source tables)
# MAGIC
# MAGIC SQL-only classifier over BOTH `openalex.pdf.grobid_processing_results.xml_content`
# MAGIC AND `openalex.pdf.grobid_xml_backfill.xml_content`.
# MAGIC
# MAGIC The two source tables cover disjoint XML uuid spaces (different upload eras):
# MAGIC   - `grobid_processing_results`: ~38.5M `success`/`cached` rows. Bad cohort here is
# MAGIC     structurally-empty TEI envelopes (`<body/>` self-closing).
# MAGIC   - `grobid_xml_backfill`: ~37.9M rows. Bad cohort is HTML wrappers carrying
# MAGIC     `[BAD_INPUT_DATA]` / `[NO_BLOCKS]` / `[TIMEOUT]` markers.
# MAGIC
# MAGIC The 2026-05-20 10K customer-view validation found BAD_BYTE = 7.65%, with the
# MAGIC HTML-wrapper class (~6.5%) entirely in `grobid_xml_backfill`. The expanded
# MAGIC classifier UNIONs both tables so we don't miss this class. See oxjob #202.
# MAGIC
# MAGIC **Target**: `openalex.works.grobid_xml_classification{suffix}` (Delta)
# MAGIC
# MAGIC ## Labels
# MAGIC
# MAGIC - `TEI_VALID` — has body content.
# MAGIC - `TEI_EMPTY_BODY` — `<body/>` self-closing (may have refs/abstract).
# MAGIC - `TEI_EMPTY_NO_REFS` — `<body/>` + `<listBibl/>` self-closing.
# MAGIC - `TEI_FULLY_EMPTY` — `<body/>` + `<listBibl/>` + `<abstract/>` all self-closing.
# MAGIC - `BAD_INPUT_DATA` — HTML wrapper containing `[BAD_INPUT_DATA]` marker.
# MAGIC - `NO_BLOCKS` — `[NO_BLOCKS]` marker.
# MAGIC - `TIMEOUT` — `[TIMEOUT]` marker (rare).
# MAGIC - `NO_GROBID_RESPONSE` — `[NO_GROBID_RESPONSES]` marker.
# MAGIC - `HTML_OTHER` — `<html` head but no recognized marker.
# MAGIC - `NOT_TEI` — neither TEI nor HTML.
# MAGIC - `NULL_XML` — `xml_content IS NULL` (shouldn't happen for success rows).
# MAGIC
# MAGIC Bad cohort for cleanup = everything except `TEI_VALID`.
# MAGIC
# MAGIC ## Dedupe
# MAGIC
# MAGIC Each table can have duplicate rows per source_pdf_id (rare). Cross-table, the two
# MAGIC are disjoint by `id` (the XML uuid), so no cross-table dedupe is needed — every
# MAGIC XML uuid in R2 maps to exactly one table.
# MAGIC
# MAGIC Within each table, keep the latest row per (source_pdf_id, id) by created_date.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `target_suffix`: appended to output table name (e.g. `_pilot`)
# MAGIC   - `scan_run_id`: stamped onto every row for traceability

# COMMAND ----------

dbutils.widgets.text("target_suffix", "", "Target table suffix (e.g. '_pilot' or '')")
dbutils.widgets.text("scan_run_id", "2026-05-20_union_classifier_v1", "scan_run_id stamp")

SUFFIX = dbutils.widgets.get("target_suffix").strip()
SCAN_RUN_ID = dbutils.widgets.get("scan_run_id").strip()
TARGET = f"openalex.works.grobid_xml_classification{SUFFIX}"
print(f"target={TARGET}  scan_run_id={SCAN_RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the unified classifier
# MAGIC
# MAGIC One CASE expression, applied to both tables via UNION ALL.

# COMMAND ----------

CLASSIFY_CASE = """
CASE
  WHEN xml_content IS NULL THEN 'NULL_XML'
  WHEN xml_content LIKE '%[BAD_INPUT_DATA]%' THEN 'BAD_INPUT_DATA'
  WHEN xml_content LIKE '%[NO_BLOCKS]%' THEN 'NO_BLOCKS'
  WHEN xml_content LIKE '%[TIMEOUT]%' THEN 'TIMEOUT'
  WHEN xml_content LIKE '%[NO_GROBID_RESPONSES]%' THEN 'NO_GROBID_RESPONSE'
  WHEN LOWER(SUBSTRING(xml_content, 1, 200)) NOT LIKE '%<tei%'
       AND LOWER(SUBSTRING(xml_content, 1, 200)) LIKE '%<html%' THEN 'HTML_OTHER'
  WHEN LOWER(SUBSTRING(xml_content, 1, 200)) NOT LIKE '%<tei%' THEN 'NOT_TEI'
  WHEN xml_content LIKE '%<body/>%' AND xml_content LIKE '%<listBibl/>%' AND xml_content LIKE '%<abstract/>%' THEN 'TEI_FULLY_EMPTY'
  WHEN xml_content LIKE '%<body/>%' AND xml_content LIKE '%<listBibl/>%' THEN 'TEI_EMPTY_NO_REFS'
  WHEN xml_content LIKE '%<body/>%' THEN 'TEI_EMPTY_BODY'
  ELSE 'TEI_VALID'
END
"""

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET}
USING DELTA
CLUSTER BY (grobid_uuid)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH pr_latest AS (
  SELECT
    id          AS grobid_uuid,
    source_pdf_id,
    xml_content,
    LENGTH(xml_content) AS xml_len,
    created_date,
    'grobid_processing_results' AS src_table,
    ROW_NUMBER() OVER (PARTITION BY source_pdf_id, id ORDER BY created_date DESC) AS rn
  FROM openalex.pdf.grobid_processing_results
  WHERE status IN ('success', 'success - cached response')
),
bf_latest AS (
  SELECT
    id          AS grobid_uuid,
    -- backfill table has pdf_s3_key like '<uuid>.pdf'; align to source_pdf_id (bare uuid)
    REGEXP_REPLACE(pdf_s3_key, '\\\\.pdf$', '') AS source_pdf_id,
    xml_content,
    LENGTH(xml_content) AS xml_len,
    CAST(created_date AS TIMESTAMP) AS created_date,
    'grobid_xml_backfill' AS src_table,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_date DESC) AS rn
  FROM openalex.pdf.grobid_xml_backfill
),
unioned AS (
  SELECT grobid_uuid, source_pdf_id, xml_content, xml_len, created_date, src_table FROM pr_latest WHERE rn = 1
  UNION ALL
  SELECT grobid_uuid, source_pdf_id, xml_content, xml_len, created_date, src_table FROM bf_latest WHERE rn = 1
)
SELECT
  grobid_uuid,
  source_pdf_id,
  src_table,
  xml_len,
  {CLASSIFY_CASE} AS label,
  created_date AS classified_at,
  '{SCAN_RUN_ID}' AS scan_run_id
FROM unioned
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by label × source table

# COMMAND ----------

summary = spark.sql(f"""
SELECT
  label,
  src_table,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
  MIN(xml_len) AS min_len,
  PERCENTILE(xml_len, 0.5) AS p50_len
FROM {TARGET}
GROUP BY label, src_table
ORDER BY label, src_table
""")
summary.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bad-cohort total (everything except TEI_VALID)

# COMMAND ----------

bad = spark.sql(f"""
SELECT COUNT(*) AS n_bad
FROM {TARGET}
WHERE label != 'TEI_VALID'
""").collect()[0]["n_bad"]

total = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET}").collect()[0]["n"]
print(f"Total rows:    {total:,}")
print(f"Bad cohort:    {bad:,} ({100 * bad / total:.2f}%)")
print(f"Good (TEI_VALID): {total - bad:,} ({100 * (total-bad) / total:.2f}%)")
