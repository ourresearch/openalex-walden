-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tag PDF Awards (Incremental)
-- MAGIC Incremental pipeline: extract funder sections from recent PDFs,
-- MAGIC match to screened funders, then match awards by funder.
-- MAGIC
-- MAGIC Writes to: `openalex.works.fulltext_work_funders`, `openalex.pdf.grobid_award_matches`
-- MAGIC
-- MAGIC Uses a checkpoint table with two columns:
-- MAGIC - `window_start`: start of the range to process (set by previous run)
-- MAGIC - `run_cutoff`: set to `now()` at the start of each run, used as the end of the range

-- COMMAND ----------

-- Step 0: Initialize checkpoint table (first run only)
CREATE TABLE IF NOT EXISTS openalex.pdf.funder_award_parse_checkpoint (
  window_start TIMESTAMP,
  run_cutoff TIMESTAMP
);

-- COMMAND ----------

-- Seed with initial values if empty
INSERT INTO openalex.pdf.funder_award_parse_checkpoint
SELECT
  now() - INTERVAL 6 HOURS AS window_start,
  now() AS run_cutoff
WHERE NOT EXISTS (SELECT 1 FROM openalex.pdf.funder_award_parse_checkpoint);

-- COMMAND ----------

-- Step 1: Stamp run_cutoff to now() — captures the end of our processing window
-- before any queries run, so no rows are lost between query start and commit
UPDATE openalex.pdf.funder_award_parse_checkpoint
SET run_cutoff = now();

-- COMMAND ----------

-- Step 2: Extract funder/acknowledgement/funding sections from PDFs in window,
--         map (native_id, native_id_namespace) -> work_id
CREATE OR REPLACE TEMP VIEW funder_sections AS
WITH checkpoint AS (
  SELECT window_start, run_cutoff FROM openalex.pdf.funder_award_parse_checkpoint
),
recent_pdfs AS (
  SELECT g.native_id, g.native_id_namespace, g.xml_content
  FROM openalex.pdf.grobid_processing_results g
  CROSS JOIN checkpoint cp
  WHERE g.created_date >= cp.window_start
    AND g.created_date < cp.run_cutoff
    AND g.xml_content IS NOT NULL
),
work_id_map AS (
  SELECT
    rp.native_id, rp.native_id_namespace,
    MAX(lm.work_id) AS work_id
  FROM recent_pdfs rp
  JOIN openalex.works.locations_mapped lm
    ON rp.native_id = lm.native_id AND rp.native_id_namespace = lm.native_id_namespace
  WHERE lm.work_id IS NOT NULL
  GROUP BY rp.native_id, rp.native_id_namespace
),
with_work_id AS (
  SELECT DISTINCT wm.work_id, rp.xml_content
  FROM recent_pdfs rp
  JOIN work_id_map wm
    ON rp.native_id = wm.native_id AND rp.native_id_namespace = wm.native_id_namespace
),
raw_sections AS (
  SELECT
    work_id,
    array_join(flatten(transform(
      regexp_extract_all(xml_content, '<funder[^>]*>(.*?)</funder>', 1),
      block -> regexp_extract_all(block, '<orgName[^>]*>([^<]+)</orgName>', 1)
    )), ', ') AS funders,
    array_join(transform(
      regexp_extract_all(xml_content, '<div[^>]*type="acknowledgement"[^>]*>(.*?)</div>', 1),
      block -> regexp_replace(block, '<[^>]+>', ' ')
    ), ' ') AS acknowledgement,
    array_join(transform(
      regexp_extract_all(xml_content, '<div[^>]*type="funding"[^>]*>(.*?)</div>', 1),
      block -> regexp_replace(block, '<[^>]+>', ' ')
    ), ' ') AS funding
  FROM with_work_id
)
SELECT
  work_id, funders, acknowledgement, funding,
  concat_ws(' ', funders, acknowledgement, funding) AS all_sections
FROM raw_sections
WHERE funders != '' OR acknowledgement != '' OR funding != '';

-- COMMAND ----------

-- Step 3: Match sections to screened funders -> materialize as staging table
-- Persisting avoids re-evaluating funder_sections (XML-heavy) in later cells
CREATE OR REPLACE TABLE openalex.pdf.funder_matches_staging AS
WITH funder_regexes AS (
  SELECT
    fnk.name AS funder_name,
    fnk.id AS funder_id,
    CAST(regexp_extract(fnk.id, 'F(\\d+)', 1) AS BIGINT) AS funder_id_numeric,
    fa.display_name AS funder_display_name,
    fa.ids.ror AS ror_id,
    fa.ids.doi AS doi,
    CASE
      WHEN fnk.name RLIKE '^[A-Z0-9\\.\\-\\s]+$' AND LENGTH(fnk.name) <= 10
      THEN CONCAT('\\b', regexp_replace(fnk.name, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'), '\\b')
      ELSE CONCAT('(?i)\\b', regexp_replace(fnk.name, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'), '\\b')
    END AS match_regex
  FROM openalex.common.funder_names_keep fnk
  JOIN openalex.funders.funders_api fa
    ON CAST(regexp_extract(fnk.id, 'F(\\d+)', 1) AS BIGINT) = fa.id
)
SELECT DISTINCT
  fs.work_id,
  fs.all_sections,
  fr.funder_name,
  fr.funder_display_name,
  fr.funder_id,
  fr.funder_id_numeric,
  fr.ror_id,
  fr.doi
FROM funder_sections fs
CROSS JOIN funder_regexes fr
WHERE fs.all_sections RLIKE fr.match_regex;

-- COMMAND ----------

-- Step 4: INSERT work-funder pairs
INSERT INTO openalex.works.fulltext_work_funders
SELECT s.work_id, s.funder_name, s.funder_display_name, s.funder_id, s.ror_id, s.doi
FROM openalex.pdf.funder_matches_staging s
LEFT ANTI JOIN openalex.works.fulltext_work_funders f
  ON s.work_id = f.work_id AND s.funder_id = f.funder_id;

-- COMMAND ----------

-- Step 5: Match awards for matched funders -> INSERT work-award pairs
INSERT INTO openalex.pdf.grobid_award_matches
WITH matched_funders AS (
  SELECT DISTINCT funder_id_numeric FROM openalex.pdf.funder_matches_staging
),
funder_alt_names AS (
  SELECT fa.id AS funder_id, fa.display_name AS alt_name
  FROM openalex.funders.funders_api fa
  JOIN matched_funders mf ON fa.id = mf.funder_id_numeric
  UNION ALL
  SELECT fa.id AS funder_id, alt_name
  FROM openalex.funders.funders_api fa
  JOIN matched_funders mf ON fa.id = mf.funder_id_numeric
  LATERAL VIEW explode(fa.alternate_titles) alt AS alt_name
),
candidate_awards AS (
  SELECT
    oa.funder_id,
    oa.funder_award_id,
    -- Word-boundary regex match. \b prevents short/truncated award IDs
    -- from matching inside longer alphanumeric runs in PDF text
    -- (e.g. "DE-AC0" falsely matching "DE-AC02-05CH11231", or "NIH R01"
    -- matching "NIH R01CA253329"). Escape regex metacharacters per
    -- Cell 5's funder-regex pattern.
    CONCAT('\\b',
           regexp_replace(oa.funder_award_id, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'),
           '\\b') AS award_match_pattern
  FROM openalex.awards.openalex_awards oa
  JOIN matched_funders mf ON oa.funder_id = mf.funder_id_numeric
  WHERE openalex.common.is_usable_award_id(oa.funder_award_id)
),
usable_awards AS (
  SELECT ca.*
  FROM candidate_awards ca
  LEFT ANTI JOIN funder_alt_names fan
    ON ca.funder_award_id = fan.alt_name
),
paper_funder_sections AS (
  SELECT /*+ REPARTITION(512, work_id) */
  DISTINCT fm.work_id, fm.funder_id_numeric, fm.all_sections
  FROM openalex.pdf.funder_matches_staging fm
)
SELECT
  /*+ BROADCAST(ua) */
  pfs.work_id AS paper_id,
  ua.funder_id,
  ua.funder_award_id,
  pfs.all_sections AS funding_sections,
  now() AS batch_time
FROM usable_awards ua
JOIN paper_funder_sections pfs
  ON pfs.funder_id_numeric = ua.funder_id
  AND pfs.all_sections RLIKE ua.award_match_pattern
LEFT ANTI JOIN openalex.pdf.grobid_award_matches g
  ON pfs.work_id = g.paper_id
  AND ua.funder_id = g.funder_id
  AND ua.funder_award_id = g.funder_award_id;

-- COMMAND ----------

-- Step 6: Advance checkpoint — promote run_cutoff to window_start
UPDATE openalex.pdf.funder_award_parse_checkpoint
SET window_start = run_cutoff;
