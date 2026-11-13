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
  -- oxjob #789: campaign RE-parses land in the window with fresh created_date
  -- but were already award-scanned from their original parse — skip them here.
  -- Their improved 0.9.1 sections get harvested in a deliberate batch later
  -- (the #738 extract-then-join precondition), not through the daily window.
  LEFT ANTI JOIN openalex.pdf.reparse_campaign_789 c
    ON g.source_pdf_id = c.source_pdf_id
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
  FROM funder_sections fs
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
)
SELECT DISTINCT
  m.work_id,
  m.all_sections,
  m.funder_name,
  m.funder_display_name,
  m.funder_id,
  m.funder_id_numeric,
  m.ror_id,
  m.doi
FROM matched_aliases m
LEFT ANTI JOIN suppressed_funder_windows x
  ON m.work_id = x.work_id AND m.all_sections = x.all_sections
  AND m.funder_id_numeric = x.funder_id_numeric;

-- COMMAND ----------

-- Step 3b (oxjob #473): distrust a funder matched only by acronyms that an unrelated active ROR organisation abroad
-- also uses, when no author is from the funder's country and one is from that organisation's. Crossref rule 1's test
-- (#1890) with the twins read from ROR. Step 4 skips these pairs; step 5 still matches their award numbers.
CREATE OR REPLACE TEMP VIEW distrusted_acronym_matches AS
WITH ror_family AS (
  SELECT ror_id AS ror, related_ror_id AS related FROM openalex.institutions.ror_relationships
  UNION
  SELECT related_ror_id, ror_id FROM openalex.institutions.ror_relationships
),
ror_acronyms AS (
  SELECT REPLACE(r.id, 'https://ror.org/', '') AS ror, UPPER(n.value) AS acronym,
    r.locations[0].geonames_details.country_code AS country
  FROM openalex.institutions.ror r LATERAL VIEW explode(r.names) x AS n
  WHERE r.status = 'active' AND array_contains(n.types, 'acronym')
),
matched_names AS (
  SELECT DISTINCT m.work_id, m.funder_id, m.funder_name, fa.country_code AS home,
    REPLACE(fa.ids.ror, 'https://ror.org/', '') AS funder_ror
  FROM openalex.pdf.funder_matches_staging m
  JOIN openalex.funders.funders_api fa ON fa.id = CAST(regexp_extract(m.funder_id, 'F(\\d+)', 1) AS BIGINT)
),
name_twins AS (
  SELECT DISTINCT n.funder_id, n.funder_name, ra.country AS twin
  FROM (SELECT DISTINCT funder_id, funder_name, home, funder_ror FROM matched_names) n
  JOIN ror_acronyms ra
    ON ra.acronym = UPPER(n.funder_name) AND ra.country <> n.home AND ra.ror <> COALESCE(n.funder_ror, '')
  LEFT ANTI JOIN ror_family f ON f.ror = n.funder_ror AND f.related = ra.ror
),
acronym_only AS (
  SELECT m.work_id, m.funder_id, FIRST(m.home) AS home
  FROM matched_names m
  LEFT JOIN (SELECT DISTINCT funder_id, funder_name FROM name_twins) t
    ON t.funder_id = m.funder_id AND t.funder_name = m.funder_name
  GROUP BY m.work_id, m.funder_id
  HAVING bool_and(t.funder_name IS NOT NULL)
),
author_countries AS (
  SELECT DISTINCT w.work_id, c.country
  FROM openalex.works.work_authorships w
  LEFT SEMI JOIN acronym_only a ON a.work_id = w.work_id
  LATERAL VIEW explode(flatten(w.authorships.countries)) c AS country
)
SELECT DISTINCT a.work_id, a.funder_id
FROM acronym_only a
JOIN matched_names m ON m.work_id = a.work_id AND m.funder_id = a.funder_id
JOIN name_twins t ON t.funder_id = m.funder_id AND t.funder_name = m.funder_name
JOIN author_countries c ON c.work_id = a.work_id AND c.country = t.twin
LEFT ANTI JOIN author_countries h ON h.work_id = a.work_id AND h.country = a.home;

-- COMMAND ----------

-- Step 4: INSERT work-funder pairs
INSERT INTO openalex.works.fulltext_work_funders
SELECT s.work_id, s.funder_name, s.funder_display_name, s.funder_id, s.ror_id, s.doi
FROM openalex.pdf.funder_matches_staging s
LEFT ANTI JOIN openalex.works.fulltext_work_funders f
  ON s.work_id = f.work_id AND s.funder_id = f.funder_id
LEFT ANTI JOIN distrusted_acronym_matches d
  ON s.work_id = d.work_id AND s.funder_id = d.funder_id;

-- COMMAND ----------

-- Step 5: Match awards for matched funders -> materialize as staging table (the costly match runs once;
-- step 5b reads its result several times)
CREATE OR REPLACE TABLE openalex.pdf.award_matches_staging AS
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
    -- TRUE when any award with this number comes from a funder's own grant feed, not from paper metadata (the list
    -- is DEPOSITED in nightly_grants/lib/work_awards.py). Per number, so two awards sharing a number carry the same flag.
    bool_or(NOT COALESCE(oa.provenance IN ('crossref_work_funders','crossref_work.grants','crossref_work','europepmc_work_funders','datacite_work_funders'), false))
      OVER (PARTITION BY oa.funder_id, oa.funder_award_id) AS from_funder_feed,
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
  DISTINCT fm.work_id, fm.funder_id_numeric, fm.all_sections
  FROM openalex.pdf.funder_matches_staging fm
)
SELECT
  /*+ BROADCAST(ua) */
  pfs.work_id,
  ua.funder_id,
  ua.funder_award_id,
  ua.from_funder_feed,
  ua.award_match_pattern,
  pfs.all_sections
FROM usable_awards ua
JOIN paper_funder_sections pfs
  ON pfs.funder_id_numeric = ua.funder_id
  AND pfs.all_sections RLIKE ua.award_match_pattern;

-- COMMAND ----------

-- Step 5b: drop cut-short numbers -> INSERT work-award pairs
INSERT INTO openalex.pdf.grobid_award_matches
WITH nested_awards AS (
  -- Longest number wins. \b breaks at ".", "-" and "/", so a cut-short number ("016.Veni", "DE-AC02") matches inside
  -- every full number ("016.Veni.173.076", "DE-AC02-05CH11231"). Drop a matched number when EVERY place it occurs in the
  -- text is the start of a longer matched number of the same funder; the paper keeps that longer number. Never dropped:
  -- a number from a funder's own feed, and a number whose longer form only adds a label, a word or a code ("/AEI/...",
  -- "-EUROfusion", "-Im2Pact", "-5GSMART", "/H0903") or a funder DOI: the short one is then the real number. One
  -- occurrence that is not explained keeps the number.
  SELECT s.work_id, s.funder_id, s.funder_award_id, s.all_sections, s.award_match_pattern
  FROM openalex.pdf.award_matches_staging s
  JOIN openalex.pdf.award_matches_staging l
    ON l.work_id = s.work_id AND l.funder_id = s.funder_id AND l.all_sections = s.all_sections
    AND startswith(l.funder_award_id, s.funder_award_id)
    AND substr(l.funder_award_id, LENGTH(s.funder_award_id) + 1) RLIKE '^(?!/[A-Za-z ]+(/|$))(?!/[A-Za-z]{2,}-)(?!.*10\\.13039)(?!.*/[A-Z][0-9]{4}$)(?!.*[a-z].*[a-z])(?!.*[A-Z]{4}$)[.\\-/]?(?=[0-9A-Za-z]).*[0-9]'
  GROUP BY s.work_id, s.funder_id, s.funder_award_id, s.all_sections, s.award_match_pattern
  HAVING NOT bool_or(s.from_funder_feed)
    AND NOT s.all_sections RLIKE CONCAT(s.award_match_pattern, '(?!(?:',
      array_join(collect_set(regexp_replace(substr(l.funder_award_id, LENGTH(s.funder_award_id) + 1), '([\\[\\](){}+*?^$.|\\\\])', '\\\\$1')), '|'),
      ')\\b)')
),
standalone_awards AS (
  -- A number that papers cite on its own at least as often as inside a longer number is a real number, and the longer
  -- form is that number plus a code ("81171037/H0903", "639088/ERC-2014-STG"). Counted per paper over the match table:
  -- the number's own rows and every number that starts with it, so dropping a nested row never changes the count.
  SELECT funder_id, funder_award_id
  FROM (
    SELECT /*+ BROADCAST(n) */ n.funder_id, n.funder_award_id, g.paper_id,
      bool_or(g.funder_award_id = n.funder_award_id
        AND g.funding_sections RLIKE CONCAT(n.award_match_pattern, '(?![.\\-/]?[0-9A-Za-z])')) AS alone
    FROM (SELECT DISTINCT funder_id, funder_award_id, award_match_pattern FROM nested_awards) n
    JOIN openalex.pdf.grobid_award_matches g
      ON g.funder_id = n.funder_id AND startswith(g.funder_award_id, n.funder_award_id)
    GROUP BY n.funder_id, n.funder_award_id, g.paper_id
  )
  GROUP BY funder_id, funder_award_id
  HAVING count_if(alone) >= count_if(NOT alone)
),
cut_short_awards AS (
  SELECT n.work_id, n.funder_id, n.funder_award_id, n.all_sections
  FROM nested_awards n
  LEFT ANTI JOIN standalone_awards k
    ON n.funder_id = k.funder_id AND n.funder_award_id = k.funder_award_id
)
SELECT
  m.work_id AS paper_id,
  m.funder_id,
  m.funder_award_id,
  m.all_sections AS funding_sections,
  now() AS batch_time
FROM openalex.pdf.award_matches_staging m
LEFT ANTI JOIN cut_short_awards x
  ON m.work_id = x.work_id AND m.funder_id = x.funder_id
  AND m.funder_award_id = x.funder_award_id AND m.all_sections = x.all_sections
LEFT ANTI JOIN openalex.pdf.grobid_award_matches g
  ON m.work_id = g.paper_id
  AND m.funder_id = g.funder_id
  AND m.funder_award_id = g.funder_award_id;

-- COMMAND ----------

-- Step 6: Advance checkpoint — promote run_cutoff to window_start
UPDATE openalex.pdf.funder_award_parse_checkpoint
SET window_start = run_cutoff;
