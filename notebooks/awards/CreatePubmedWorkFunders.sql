-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### PubMed GrantList -> pubmed_work_funders, a Nightly Grants leg (oxjob #2544)
-- MAGIC
-- MAGIC PubMed's own GrantList (`openalex.works.locations_mapped`, provenance `pubmed`: one `funders` entry per agency with its
-- MAGIC grant numbers, as NLM indexed them) -> the work each PubMed record maps to -> `(work_id, funder_id, award_ids)`.
-- MAGIC Keyed on the PubMed record's own work id, so works without a DOI, works before 1985 and new works are reached (the
-- MAGIC Europe PMC leg, #477, is one June 2026 harvest of 1985-2025 records joined by DOI).
-- MAGIC
-- MAGIC **Scope (v1):** PubMed records published before `before_year` (2000) whose agency is a US Public Health Service code
-- MAGIC (`... NIH HHS`, `... CDC HHS`, `... HHS`, `... VA`): NLM indexed these grant numbers from the article itself, before
-- MAGIC PubMed Central existed, so none of them can be the at-scale PubMed Central grant assertions that keep the Europe PMC
-- MAGIC leg paused (oxjob #2544). Later records wait until those can be told apart; lifting the cutoff is one parameter, but
-- MAGIC non-HHS agencies then need CreateEuropePmcWorkFunders' full Python resolver.
-- MAGIC
-- MAGIC **Agency -> funder:** CreateEuropePmcWorkFunders' resolver as it acts on these strings: the verified NIH/HHS/VA acronym
-- MAGIC map (`nih_funder_map.json`, the same S3 object), parent fallback for `NIH HHS` / `CDC HHS` / `VA`, otherwise the NIH
-- MAGIC display names and the unambiguous registry names (dd1016-b3 withdrawn funders still count toward ambiguity). Merged
-- MAGIC funders resolve to their survivor. A test keeps the acronym branch the same as the Python resolver's.
-- MAGIC
-- MAGIC **Outputs:**
-- MAGIC 1. `openalex.awards.pubmed_work_funders (work_id, funder_id, award_ids)`: read by Nightly Grants (leg `pubmed_work_funders`,
-- MAGIC    priority 4, deposited, like Europe PMC) and by the additive `funder_reported_work_funders` union (funder-only rows too).
-- MAGIC 2. Award shells in `openalex.awards.openalex_awards_raw`, provenance `pubmed_work_funders`, priority 0 (paper-side, never
-- MAGIC    registry), only for grants no award carries yet and one spelling per grant (cell `pwf_shell_spellings` says why: a
-- MAGIC    shell for an existing grant takes that award's paper links and is served as its twin), minted as the Europe PMC
-- MAGIC    leg mints them (same id hash, #690 guard, ERC call-code guard). Every other number reaches its existing award
-- MAGIC    through Nightly's key match, or NIH's project record through ProduceNihPubmedRelinks.
-- MAGIC
-- MAGIC Both are written by MERGE: unchanged rows are left alone, so a night with no PubMed change writes no new table version.
-- MAGIC Gates run before either write (shrink guard, growth ceiling, live funders); a stopped run writes nothing.
-- MAGIC
-- MAGIC **Undo:** drop the leg (git revert); then `DELETE FROM openalex.awards.funder_reported_work_funders WHERE provenance =
-- MAGIC 'pubmed_work_funders'` and `DELETE FROM openalex.awards.openalex_awards_raw WHERE provenance = 'pubmed_work_funders'`
-- MAGIC (the shells turn GONE in the next Nightly; raise `gone_fuse` for that one night). One night only: RESTORE TABLE
-- MAGIC openalex.awards.pubmed_work_funders TO VERSION AS OF the version before the run.
-- MAGIC
-- MAGIC Parameters: `leg_table` / `raw_table` (production, or `openalex.oxlane.oxjob2544_*` scratch copies for a dry run),
-- MAGIC `before_year` (2000), `first_fill_max` (growth ceiling while the leg is empty).

-- COMMAND ----------

SELECT assert_true((:leg_table = 'openalex.awards.pubmed_work_funders' AND :raw_table = 'openalex.awards.openalex_awards_raw')
    OR (:leg_table RLIKE '^openalex[.]oxlane[.]oxjob2544_[a-z0-9_]+$' AND :raw_table RLIKE '^openalex[.]oxlane[.]oxjob2544_[a-z0-9_]+$'),
    'leg_table and raw_table must both be production or both openalex.oxlane.oxjob2544_ scratch copies'),
  assert_true(try_cast(:before_year AS INT) BETWEEN 1950 AND 2100, 'before_year must be a year'),
  assert_true(try_cast(:first_fill_max AS INT) BETWEEN 1 AND 5000000, 'first_fill_max must be in 1..5,000,000')

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE pwf_leg STRING DEFAULT :leg_table

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE pwf_raw STRING DEFAULT :raw_table

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE pwf_before DATE DEFAULT make_date(CAST(:before_year AS INT), 1, 1)

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE pwf_first_fill_max BIGINT DEFAULT CAST(:first_fill_max AS BIGINT)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(pwf_leg) (work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta

-- COMMAND ----------

-- Regex escaping guard: regex backslashes are double-escaped ('\\s'), as walden's SQL does. Stop if this engine reads
-- them differently (agency strings would stop matching and the leg would shrink).
SELECT assert_true(regexp_replace('NCI  NIH HHS ', '\\s+', ' ') = 'NCI NIH HHS ' AND 'NCI NIH HHS' RLIKE '(HHS| VA)\\s*$',
  'PWF_REGEX_MODE: double-escaped regex backslashes did not reach the regex engine as single ones, STOP')

-- COMMAND ----------

-- The verified NIH/HHS/VA acronym map CreateEuropePmcWorkFunders reads (step 2b): acronym -> funder id, and the NIH
-- display names (lower case, single spaces) it also accepts.
CREATE OR REPLACE TEMPORARY VIEW pwf_nih_map AS
SELECT m.acr, CAST(regexp_extract(m.v.funder_id, 'F([0-9]+)', 1) AS BIGINT) funder_id,
       lower(trim(regexp_replace(m.v.display_name, '\\s+', ' '))) name_norm
FROM (SELECT explode(from_json(value, 'map<string,struct<funder_id:string,display_name:string>>')) AS (acr, v)
      FROM read_files('s3://openalex-ingest/awards/europepmc/nih_funder_map.json', format => 'text', wholeText => true)) m

-- COMMAND ----------

-- Unambiguous registry names (display_name and alternate_titles; merged funders as their survivor), CreateEuropePmcWorkFunders
-- step 2a. Funders withdrawn in batch dd1016-b3 count toward ambiguity as negative ids that can never be a target.
CREATE OR REPLACE TEMPORARY VIEW pwf_registry_names AS
WITH fv AS (
  SELECT coalesce(merge_into_id, funder_id) funder_id, nm
  FROM openalex.funders.funders
  LATERAL VIEW explode(array_union(array(display_name), coalesce(from_json(alternate_titles, 'array<string>'), array()))) t AS nm
  WHERE display_name IS NOT NULL
  UNION ALL
  SELECT -funder_id, nm
  FROM openalex.funders.deleted_funders
  LATERAL VIEW explode(array_union(array(get_json_object(before_image_json, '$.display_name')),
    coalesce(from_json(get_json_object(before_image_json, '$.alternate_titles'), 'array<string>'), array()))) t AS nm
  WHERE batch = 'dd1016-b3' AND nm IS NOT NULL)
SELECT lower(trim(regexp_replace(nm, '\\s+', ' '))) nl, max(funder_id) funder_id
FROM fv GROUP BY 1 HAVING count(DISTINCT funder_id) = 1 AND max(funder_id) > 0

-- COMMAND ----------

-- PubMed grant mentions in scope: one row per (work, agency string, grant string; NULL when the agency names no grant).
CREATE OR REPLACE TEMPORARY VIEW pwf_mentions AS
SELECT DISTINCT x.work_id, x.agency, g.grant_id
FROM (SELECT lm.work_id, trim(f.name) agency, f.awards
      FROM openalex.works.locations_mapped lm LATERAL VIEW explode(lm.funders) t AS f
      WHERE lm.provenance = 'pubmed' AND lm.work_id IS NOT NULL AND size(lm.funders) > 0
        AND lm.published_date < pwf_before
        AND trim(f.name) RLIKE '(HHS| VA)$') x
LATERAL VIEW OUTER explode(x.awards) g AS grant_id

-- COMMAND ----------

-- Agency -> funder: CreateEuropePmcWorkFunders.resolve() on these strings. For 'X NIH HHS' / 'X CDC HHS' / 'X VA' the
-- acronym X (as written, else upper case) or the parent; for 'X HHS' the acronym X; otherwise the whole string as an NIH
-- display name, then as an unambiguous registry name. The result resolves through funder merges to the live survivor.
CREATE OR REPLACE TEMPORARY VIEW pwf_agency_funder AS
WITH a AS (SELECT DISTINCT agency, split(agency, ' ')[0] first_token, lower(trim(regexp_replace(agency, '\\s+', ' '))) nl
           FROM pwf_mentions),
spec AS (
  SELECT a.*, coalesce(m1.funder_id, m2.funder_id) spec
  FROM a LEFT JOIN pwf_nih_map m1 ON m1.acr = a.first_token LEFT JOIN pwf_nih_map m2 ON m2.acr = upper(a.first_token)),
r AS (
  SELECT s.agency,
    CASE WHEN s.agency LIKE '%NIH HHS' THEN coalesce(s.spec, (SELECT funder_id FROM pwf_nih_map WHERE acr = 'NIH'))
         WHEN s.agency LIKE '%CDC HHS' THEN coalesce(s.spec, (SELECT funder_id FROM pwf_nih_map WHERE acr = 'CDC'))
         WHEN s.agency LIKE '% VA' THEN coalesce(s.spec, (SELECT funder_id FROM pwf_nih_map WHERE acr = 'VA'))
         WHEN s.agency LIKE '%HHS' AND s.spec IS NOT NULL THEN s.spec
         ELSE coalesce((SELECT max(funder_id) FROM pwf_nih_map n WHERE n.name_norm = s.nl), u.funder_id) END funder_id
  FROM spec s LEFT JOIN pwf_registry_names u ON u.nl = s.nl)
SELECT r.agency, coalesce(f.merge_into_id, r.funder_id) funder_id
FROM r LEFT JOIN openalex.funders.funders f ON f.funder_id = r.funder_id
WHERE r.funder_id IS NOT NULL

-- COMMAND ----------

-- Candidate leg rows, CreateEuropePmcWorkFunders step 4: a grant string can pack several numbers ("CA11051, CA11198"),
-- split on , or ;. Unusable numbers (openalex.common.is_usable_award_id) keep the funder link without a grant.
CREATE OR REPLACE TEMPORARY VIEW pwf_candidate AS
WITH resolved AS (
  SELECT m.work_id, a.funder_id, m.grant_id FROM pwf_mentions m JOIN pwf_agency_funder a ON a.agency = m.agency),
split_awards AS (
  SELECT work_id, funder_id, trim(aid_raw) aid_raw FROM resolved LATERAL VIEW OUTER explode(split(grant_id, '[,;]')) t AS aid_raw),
flattened AS (
  SELECT work_id, funder_id,
    CASE WHEN aid_raw IS NOT NULL AND aid_raw <> '' AND openalex.common.is_usable_award_id(aid_raw) THEN aid_raw END aid
  FROM split_awards)
SELECT work_id, funder_id, array_sort(array_distinct(collect_list(aid))) award_ids
FROM flattened GROUP BY work_id, funder_id

-- COMMAND ----------

-- Which grant numbers get a shell: only grants no award carries yet, one spelling each. Nightly Grants links a paper's
-- number to ONE award per (funder, generic key), preferring an award's own spellings to those it reaches only through an
-- alias, so a new shell for a grant that is already an award takes that award's links and is served as its twin. The
-- check run that minted every spelling (oxjob #2544) moved 588 works off NIH's registry record for AM 22125 onto a new
-- 'AM 22125.' shell, and 20,642 of its 91,102 new awards shared a key with an existing one. A spelling is minted when:
--   1. no award row of another source has the same generic key (Nightly's stable_award_ids.generic_sql) under the same
--      funder, or under any NIH funder for an NIH funder's number: the work's number reaches that award through Nightly's
--      own key match;
--   2. for an NIH funder (nih_fold.NIH_FAMILY), no award row of another source in that family has the same institute +
--      serial: NIH ExPORTER files projects under the NIH parent while papers name the institute, so an institute shell
--      would twin the project across funders; ProduceNihPubmedRelinks links PubMed's NIH grants to project records;
--   3. it is the grant's one spelling in this leg: per generic key (per funder; one NIH-wide key), then per NIH institute +
--      serial, the spelling most PubMed records use. A work citing another spelling of it gets no link to it, never a twin.
-- NIH institute + serial = Nightly's sharp key for the NIH parent (AwardNormKey.sql, funder 4320332161: the paper-side
-- form, else the registry form): 'AM-19920-4' -> AM019920, 'BNS 13464' -> NS013464, '5R01CA011051-14' -> CA011051.
-- A second check run with a looser key of its own still made 611 NIH twins (oxjob #2544).
CREATE OR REPLACE TEMPORARY VIEW pwf_shell_spellings AS
WITH spelled AS (
  SELECT c.funder_id, award_id AS funder_award_id, count(*) mentions
  FROM pwf_candidate c LATERAL VIEW EXPLODE(award_ids) AS award_id
  GROUP BY 1, 2),
-- other sources' rows: the production table even on a scratch run (a view cannot name its table by variable; this notebook
-- never writes those rows, and a scratch raw_table is a clone of them)
other_rows AS (
  SELECT r.funder_id, r.funder_award_id FROM openalex.awards.openalex_awards_raw r
  WHERE r.provenance <> 'pubmed_work_funders' AND r.funder_award_id IS NOT NULL
    AND (r.funder_id IN (SELECT funder_id FROM spelled) OR r.funder_id IN (4320332161, 4320337611, 4320337356, 4320337376,
      4320337472, 4320337358, 4320337593, 4320337375, 4320337355, 4320337362, 4320337363, 4320337364, 4320337365, 4320337357,
      4320337361, 4320337354, 4320337346, 4320337359, 4320337353, 4320337337, 4320337330, 4320337352, 4320337347, 4320337534,
      4320337372, 4320337351, 4320337374, 4320337350, 4320337338, 4320337348))),
keyed AS (
  SELECT 'leg' side, funder_id, funder_award_id, mentions FROM spelled
  UNION ALL SELECT 'other', funder_id, funder_award_id, 0 FROM other_rows),
keys AS (
  SELECT side, funder_id, funder_award_id, mentions, fam,
    CASE WHEN length(regexp_replace(lower(funder_award_id), '[^a-z0-9]', '')) >= 4
         THEN regexp_replace(lower(funder_award_id), '[^a-z0-9]', '') ELSE lower(trim(funder_award_id)) END gkey,
    CASE WHEN fam THEN coalesce(
      CASE WHEN regexp_extract(n, '([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)', 1) <> ''
           THEN concat(regexp_extract(n, '([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)', 1),
                       lpad(regexp_extract(n, '([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)', 2), 6, '0')) END,
      nullif(regexp_extract(n, '([A-Z]{2}\\d{6})', 1), '')) END nkey
  FROM (SELECT *, funder_id IN (4320332161, 4320337611, 4320337356, 4320337376, 4320337472, 4320337358, 4320337593, 4320337375,
      4320337355, 4320337362, 4320337363, 4320337364, 4320337365, 4320337357, 4320337361, 4320337354, 4320337346, 4320337359,
      4320337353, 4320337337, 4320337330, 4320337352, 4320337347, 4320337534, 4320337372, 4320337351, 4320337374, 4320337350,
      4320337338, 4320337348) fam,
          regexp_replace(regexp_replace(regexp_replace(upper(trim(funder_award_id)),
            '[\\u2010-\\u2015\\u2212\\uFE58\\uFE63\\uFF0D]', '-'), '[\\u00A0\\u1680\\u2000-\\u200B\\u202F\\u205F\\u3000]', ' '), '  +', ' ') n
        FROM keyed)),
fresh AS (
  SELECT l.* FROM keys l
  LEFT ANTI JOIN (SELECT DISTINCT funder_id, gkey FROM keys WHERE side = 'other') o ON o.funder_id = l.funder_id AND o.gkey = l.gkey
  LEFT ANTI JOIN (SELECT DISTINCT gkey FROM keys WHERE side = 'other' AND fam) f ON l.fam AND f.gkey = l.gkey
  LEFT ANTI JOIN (SELECT DISTINCT nkey FROM keys WHERE side = 'other' AND nkey IS NOT NULL) n ON n.nkey = l.nkey
  WHERE l.side = 'leg'),
one_per_key AS (
  SELECT * FROM fresh QUALIFY row_number() OVER (PARTITION BY CASE WHEN fam THEN 0 ELSE funder_id END, gkey
    ORDER BY mentions DESC, funder_id, funder_award_id) = 1)
SELECT funder_id, funder_award_id FROM one_per_key
QUALIFY nkey IS NULL OR row_number() OVER (PARTITION BY nkey ORDER BY mentions DESC, funder_id, funder_award_id) = 1

-- COMMAND ----------

-- Candidate award shells, CreateEuropePmcWorkFunders step 5 with this leg's provenance (same id hash, same #690 guard,
-- same ERC call-code guard), for the spellings above. Priority 0 is added at the write: a paper-side shell, never a registry record.
CREATE OR REPLACE TEMPORARY VIEW pwf_shells AS
WITH exploded AS (
  SELECT DISTINCT funder_id, LOWER(funder_award_id) AS normalized_award_id, funder_award_id
  FROM pwf_shell_spellings
),
guarded AS (
  SELECT e.funder_id, e.normalized_award_id, e.funder_award_id
  FROM exploded e
  LEFT JOIN openalex.awards.award_id_guard g
    ON g.funder_id = e.funder_id AND g.funder_award_id = e.funder_award_id
  WHERE COALESCE(g.decision, 'mint') <> 'suppress'
),
funders AS (SELECT funder_id, display_name, ror_id, doi FROM openalex.funders.funders)
SELECT
  ABS(XXHASH64(CONCAT(f.funder_id, ':', e.normalized_award_id))) % 9000000000 AS id,
  CAST(NULL AS STRING) AS display_name, CAST(NULL AS STRING) AS description,
  f.funder_id, e.funder_award_id,
  CAST(NULL AS DOUBLE) AS amount, CAST(NULL AS STRING) AS currency,
  STRUCT(CONCAT('https://openalex.org/F', f.funder_id) AS id, f.display_name, f.ror_id, f.doi) AS funder,
  CAST(NULL AS STRING) AS funding_type, CAST(NULL AS STRING) AS funder_scheme,
  'pubmed_work_funders' AS provenance,
  CAST(NULL AS DATE) AS start_date, CAST(NULL AS DATE) AS end_date,
  CAST(NULL AS BIGINT) AS start_year, CAST(NULL AS BIGINT) AS end_year,
  CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS lead_investigator,
  CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS co_lead_investigator,
  CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) AS investigators,
  CAST(NULL AS STRING) AS landing_page_url,
  openalex.common.extract_grant_doi(e.funder_award_id) AS doi,
  CONCAT('https://api.openalex.org/works?filter=awards.id:G', ABS(XXHASH64(CONCAT(f.funder_id, ':', e.normalized_award_id))) % 9000000000) AS works_api_url,
  CURRENT_TIMESTAMP() AS created_date, CURRENT_TIMESTAMP() AS updated_date
FROM guarded e JOIN funders f ON f.funder_id = e.funder_id
-- #690 ticket 7682: an ERC call identifier names an ERC call, never a grant (CreateEuropePmcWorkFunders step 5, verbatim).
WHERE NOT (coalesce(f.display_name, '') RLIKE '^(European Commission|European Research Council|Seventh Framework Programme|Horizon 2020( Framework Programme)?|(FP7|H2020|HORIZON EUROPE) .+)$'
  AND regexp_replace(upper(trim(regexp_replace(e.funder_award_id, '[\\u2010-\\u2015\\u2212\\uFE58\\uFE63\\uFF0D]', '-'))), '\\s+', ' ')
      RLIKE '^ERC[-_ ]?(19|20)[0-9]{2}[-_ ]?(STG|COG|ADG|SYG|POC[0-9]?|POC[-_ ]?(LS|SM)[0-9]?)$')

-- COMMAND ----------

-- Gates, before any write. A stopped run writes nothing; the next run tries again.
SELECT
  assert_true((SELECT count(*) FROM pwf_candidate WHERE work_id IS NULL OR funder_id IS NULL OR award_ids IS NULL) = 0,
    'PWF_NULL_KEY: a candidate row has a NULL key'),
  -- the funder_reported union (CreateFunderReportedWorkFunders GATE 2) and Nightly need live, unmerged funders
  assert_true((SELECT count(*) FROM (SELECT DISTINCT funder_id FROM pwf_candidate) c
               LEFT JOIN openalex.funders.funders f ON f.funder_id = c.funder_id
               WHERE f.funder_id IS NULL OR f.merge_into_id IS NOT NULL) = 0,
    'PWF_FUNDER_NOT_LIVE: a resolved funder is missing from openalex.funders.funders or is a merge loser'),
  assert_true((SELECT count(*) FROM pwf_candidate) >= 0.95 * (SELECT count(*) FROM IDENTIFIER(pwf_leg)),
    'PWF_SHRINK_GUARD: candidate rows are more than 5% below the current leg, check the PubMed load before replacing'),
  assert_true((SELECT count(*) FROM pwf_candidate)
      <= greatest(pwf_first_fill_max, 1.25 * (SELECT count(*) FROM IDENTIFIER(pwf_leg))),
    'PWF_GROWTH_CEILING: candidate rows are over first_fill_max and 25% above the current leg, check before replacing'),
  assert_true((SELECT count(*) FROM pwf_shells)
      <= greatest(pwf_first_fill_max, 1.25 * (SELECT count(*) FROM IDENTIFIER(pwf_raw) WHERE provenance = 'pubmed_work_funders')),
    'PWF_SHELL_CEILING: candidate shells are over first_fill_max and 25% above the current ones, check before replacing')

-- COMMAND ----------

MERGE INTO IDENTIFIER(pwf_leg) t
USING pwf_candidate s
ON t.work_id = s.work_id AND t.funder_id = s.funder_id
WHEN MATCHED AND NOT (t.award_ids <=> s.award_ids) THEN UPDATE SET award_ids = s.award_ids
WHEN NOT MATCHED THEN INSERT (work_id, funder_id, award_ids) VALUES (s.work_id, s.funder_id, s.award_ids)
WHEN NOT MATCHED BY SOURCE THEN DELETE

-- COMMAND ----------

-- Shells: only this leg's rows are touched. Unchanged shells keep their row (and created_date); a funder's renamed
-- record or a new grant DOI refreshes the copy.
MERGE INTO IDENTIFIER(pwf_raw) t
USING pwf_shells s
ON t.provenance = 'pubmed_work_funders' AND t.id = s.id AND t.funder_id = s.funder_id AND t.funder_award_id = s.funder_award_id
WHEN MATCHED AND NOT (t.funder <=> s.funder AND t.doi <=> s.doi AND t.priority = 0)
  THEN UPDATE SET funder = s.funder, doi = s.doi, works_api_url = s.works_api_url, priority = 0, updated_date = s.updated_date
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type,
    funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators,
    landing_page_url, doi, works_api_url, created_date, updated_date, priority)
  VALUES (s.id, s.display_name, s.description, s.funder_id, s.funder_award_id, s.amount, s.currency, s.funder, s.funding_type,
    s.funder_scheme, s.provenance, s.start_date, s.end_date, s.start_year, s.end_year, s.lead_investigator, s.co_lead_investigator,
    s.investigators, s.landing_page_url, s.doi, s.works_api_url, s.created_date, s.updated_date, 0)
WHEN NOT MATCHED BY SOURCE AND t.provenance = 'pubmed_work_funders' THEN DELETE

-- COMMAND ----------

-- What landed (read in the run output): leg rows, works, funders, rows with a grant, shells.
SELECT
  (SELECT count(*) FROM IDENTIFIER(pwf_leg)) leg_rows,
  (SELECT count(DISTINCT work_id) FROM IDENTIFIER(pwf_leg)) works,
  (SELECT count(DISTINCT funder_id) FROM IDENTIFIER(pwf_leg)) funders,
  (SELECT count(*) FROM IDENTIFIER(pwf_leg) WHERE size(award_ids) > 0) rows_with_grants,
  (SELECT count(*) FROM IDENTIFIER(pwf_raw) WHERE provenance = 'pubmed_work_funders') shells,
  (SELECT count(*) FROM pwf_mentions m LEFT ANTI JOIN pwf_agency_funder a ON a.agency = m.agency) mentions_unresolved
