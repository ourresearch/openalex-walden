-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Backfill PDF award matches for specific funders
-- MAGIC
-- MAGIC The daily `TagPdfAwardsIncremental` job only scans PDFs parsed inside its
-- MAGIC checkpoint window, so a funder registry ingested AFTER a PDF was parsed never
-- MAGIC gets matched against that PDF's funding sections. This notebook closes that gap
-- MAGIC for a chosen list of funders: it re-extracts funding/acknowledgement sections
-- MAGIC from the stored GROBID XML of works where the funder was already detected
-- MAGIC (`openalex.works.fulltext_work_funders`), regex-matches the funder's CURRENT
-- MAGIC award list, and inserts only new (work, funder, award) rows into
-- MAGIC `openalex.pdf.grobid_award_matches` (which WorkAwards consumes nightly).
-- MAGIC
-- MAGIC Extraction and matching semantics are copied verbatim from
-- MAGIC `TagPdfAwardsIncremental.sql` (steps 2 and 5). Idempotent: safe to re-run.
-- MAGIC
-- MAGIC To backfill more funders, extend the `backfill_funders` VALUES list.
-- MAGIC First run 2026-08-20: FCT (4320334779) after the SciPROJ 7.6k -> 99k upgrade.

-- COMMAND ----------

-- Funders to backfill (numeric funder_id as used in grobid_award_matches)
CREATE OR REPLACE TEMP VIEW backfill_funders AS
SELECT * FROM VALUES (4320306230) AS t(funder_id_numeric);  -- 2026-09-24: AHA (oxjob #1358); previous run 2026-08-20: FCT 4320334779

-- COMMAND ----------

-- Step 1: target works — funder already detected in their fulltext funding sections
CREATE OR REPLACE TEMP VIEW backfill_target_works AS
SELECT DISTINCT wf.work_id, bf.funder_id_numeric
FROM openalex.works.fulltext_work_funders wf
JOIN backfill_funders bf
  ON wf.funder_id = CONCAT('https://openalex.org/F', bf.funder_id_numeric);

-- COMMAND ----------

SELECT funder_id_numeric, COUNT(DISTINCT work_id) AS target_works
FROM backfill_target_works GROUP BY 1;

-- COMMAND ----------

-- Step 2: re-extract funding sections from stored GROBID XML for those works
-- (regexes identical to TagPdfAwardsIncremental Step 2)
CREATE OR REPLACE TABLE openalex.pdf.backfill_funder_sections
USING delta
AS
WITH work_native AS (
  SELECT DISTINCT tw.work_id, lm.native_id, lm.native_id_namespace
  FROM (SELECT DISTINCT work_id FROM backfill_target_works) tw
  JOIN openalex.works.locations_mapped lm ON lm.work_id = tw.work_id
  WHERE lm.native_id IS NOT NULL
),
xmls AS (
  SELECT DISTINCT wn.work_id, g.xml_content
  FROM work_native wn
  JOIN openalex.pdf.grobid_processing_results g
    ON g.native_id = wn.native_id AND g.native_id_namespace = wn.native_id_namespace
  WHERE g.xml_content IS NOT NULL
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
  FROM xmls
)
SELECT DISTINCT
  work_id, concat_ws(' ', funders, acknowledgement, funding) AS all_sections
FROM raw_sections
WHERE funders != '' OR acknowledgement != '' OR funding != '';

-- COMMAND ----------

SELECT COUNT(DISTINCT work_id) AS works_with_sections FROM openalex.pdf.backfill_funder_sections;

-- COMMAND ----------

-- Step 3: match the funders' current award lists against the re-extracted sections
-- (semantics identical to TagPdfAwardsIncremental Step 5), insert only NEW matches
INSERT INTO openalex.pdf.grobid_award_matches
WITH funder_regexes AS (
  SELECT
    fnk.name AS funder_name,
    fnk.id AS funder_id,
    CAST(regexp_extract(fnk.id, 'F(\\d+)', 1) AS BIGINT) AS funder_id_numeric,
    fa.display_name AS funder_display_name,
    fa.ids.ror AS ror_id,
    fa.ids.doi AS doi,
    CONCAT(
      CASE
        WHEN fnk.id = 'https://openalex.org/F4320306076'
          AND LOWER(TRIM(fnk.name)) = 'national science foundation'
        THEN '(?i)(?<!\\bchinese\\s+)'
        WHEN fnk.id = 'https://openalex.org/F4320306076'
          AND LOWER(TRIM(fnk.name)) = 'nsf'
        THEN '(?<!(?i:\\bchinese)\\s+)'
        ELSE ''
      END,
      CASE
        WHEN fnk.name RLIKE '^[A-Z0-9\\.\\-\\s]+$' AND LENGTH(fnk.name) <= 10
        THEN CONCAT('\\b', regexp_replace(fnk.name, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'), '\\b')
        ELSE CONCAT('(?i)\\b', regexp_replace(fnk.name, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'), '\\b')
      END,
      CASE
        -- oxjob #874: these aliases prefix distinct funder names.
        WHEN fnk.id = 'https://openalex.org/F4320324089'
          AND LOWER(TRIM(fnk.name)) IN (
            'centre for quantum technologies',
            'center for quantum technologies'
          )
        THEN '(?i)(?!\\s*(?:,\\s*)?(?:and|&)\\s+applications\\b)'
        WHEN fnk.id = 'https://openalex.org/F4320306076'
          AND LOWER(TRIM(fnk.name)) IN (
            'national science foundation',
            'nsf'
          )
        THEN '(?i)(?!\\s*(?:,\\s*)?(?:\\(\\s*NSFC?\\s*\\)\\s*(?:,\\s*)?)?of\\s+china\\b)'
        ELSE ''
      END
    ) AS match_regex
  FROM openalex.common.funder_names_keep fnk
  JOIN openalex.funders.funders_api fa
    ON CAST(regexp_extract(fnk.id, 'F(\\d+)', 1) AS BIGINT) = fa.id
),
matched_aliases AS (
  SELECT DISTINCT fs.work_id, fs.all_sections, fr.*
  FROM openalex.pdf.backfill_funder_sections fs
  CROSS JOIN funder_regexes fr
  WHERE fs.all_sections RLIKE fr.match_regex
),
alias_spans AS (
  -- Zero-width lookahead enumerates overlapping occurrences on ORIGINAL text.
  -- Capture the suffix after the literal alias; LENGTH gives 1-based [start,end).
  -- Existing regexes contain no capturing groups; do not slice away lookbehinds.
  SELECT m.*,
    LENGTH(m.all_sections) - LENGTH(tail) - LENGTH(m.funder_name) + 1 AS span_start,
    LENGTH(m.all_sections) - LENGTH(tail) + 1 AS span_end
  FROM matched_aliases m
  LATERAL VIEW explode(regexp_extract_all(
    m.all_sections, CONCAT('(?=(?:', m.match_regex, ')([\\s\\S]*))'), 1
  )) occurrences AS tail
),
parent_child AS (
  SELECT ror_id AS parent_ror, related_ror_id AS child_ror
  FROM openalex.institutions.ror_relationships
  WHERE LOWER(relationship_type) = 'child'
  UNION
  SELECT related_ror_id AS parent_ror, ror_id AS child_ror
  FROM openalex.institutions.ror_relationships
  WHERE LOWER(relationship_type) = 'parent'
),
suppressed_spans AS (
  SELECT s.work_id, s.all_sections, s.funder_id_numeric, s.span_start, s.span_end
  FROM alias_spans s
  JOIN alias_spans l
    ON s.work_id = l.work_id AND s.all_sections = l.all_sections
    AND s.funder_id_numeric <> l.funder_id_numeric
    AND l.span_start <= s.span_start AND l.span_end >= s.span_end
    AND l.span_end - l.span_start > s.span_end - s.span_start
  LEFT JOIN parent_child pc
    ON pc.parent_ror = REPLACE(s.ror_id, 'https://ror.org/', '')
    AND pc.child_ror = REPLACE(l.ror_id, 'https://ror.org/', '')
  GROUP BY s.work_id, s.all_sections, s.funder_id_numeric, s.span_start, s.span_end
  -- A registered child or same-ROR duplicate preserves umbrella/entity credit.
  HAVING MAX(CASE WHEN pc.parent_ror IS NOT NULL
    OR (NULLIF(REPLACE(s.ror_id, 'https://ror.org/', ''), '') =
        NULLIF(REPLACE(l.ror_id, 'https://ror.org/', ''), ''))
    THEN 1 ELSE 0 END) = 0
),
surviving_funder_windows AS (
  SELECT DISTINCT s.work_id, s.all_sections, s.funder_id_numeric
  FROM alias_spans s
  LEFT ANTI JOIN suppressed_spans x
    ON s.work_id = x.work_id AND s.all_sections = x.all_sections
    AND s.funder_id_numeric = x.funder_id_numeric
    AND s.span_start = x.span_start AND s.span_end = x.span_end
),
suppressed_funder_windows AS (
  -- Drop only if ALL occurrences of ALL aliases are covered in this window.
  -- Overlapping/nested longer aliases cannot double-count away a standalone span.
  SELECT DISTINCT m.work_id, m.all_sections, m.funder_id_numeric
  FROM matched_aliases m
  LEFT ANTI JOIN surviving_funder_windows k
    ON m.work_id = k.work_id AND m.all_sections = k.all_sections
    AND m.funder_id_numeric = k.funder_id_numeric
),
funder_alt_names AS (
  SELECT fa.id AS funder_id, fa.display_name AS alt_name
  FROM openalex.funders.funders_api fa
  JOIN backfill_funders bf ON fa.id = bf.funder_id_numeric
  UNION ALL
  SELECT fa.id AS funder_id, alt_name
  FROM openalex.funders.funders_api fa
  JOIN backfill_funders bf ON fa.id = bf.funder_id_numeric
  LATERAL VIEW explode(fa.alternate_titles) alt AS alt_name
),
candidate_awards AS (
  SELECT
    oa.funder_id,
    oa.funder_award_id,
    -- Word-boundary regex match; escape regex metacharacters (verbatim from
    -- the incremental job — \b prevents short ids matching inside longer runs)
    CONCAT('\\b',
           regexp_replace(oa.funder_award_id, '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1'),
           '\\b') AS award_match_pattern
  FROM openalex.awards.openalex_awards oa
  JOIN backfill_funders bf ON oa.funder_id = bf.funder_id_numeric
  WHERE openalex.common.is_usable_award_id(oa.funder_award_id)
),
usable_awards AS (
  SELECT ca.*
  FROM candidate_awards ca
  -- PDF L1b: broader than mint-time decision='suppress'; preserve salvage.
  LEFT ANTI JOIN openalex.awards.award_id_guard g
    ON ca.funder_id = g.funder_id AND ca.funder_award_id = g.funder_award_id
    AND g.verdict = 'garbage'
    AND COALESCE(g.reason, '') NOT LIKE 'salvaged:%'
  LEFT ANTI JOIN funder_alt_names fan
    ON ca.funder_award_id = fan.alt_name
),
paper_funder_sections AS (
  SELECT /*+ REPARTITION(512, work_id) */
    s.work_id, tw.funder_id_numeric, s.all_sections
  FROM openalex.pdf.backfill_funder_sections s
  JOIN backfill_target_works tw ON tw.work_id = s.work_id
  -- No currently matched alias => no containment verdict; preserve old behavior.
  LEFT ANTI JOIN suppressed_funder_windows x
    ON s.work_id = x.work_id AND s.all_sections = x.all_sections
    AND tw.funder_id_numeric = x.funder_id_numeric
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

-- Verification: matches now present for the backfilled funders
SELECT g.funder_id, COUNT(*) AS match_rows, COUNT(DISTINCT g.paper_id) AS works
FROM openalex.pdf.grobid_award_matches g
JOIN backfill_funders bf ON g.funder_id = bf.funder_id_numeric
GROUP BY 1;
