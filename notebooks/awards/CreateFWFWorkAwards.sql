-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### FWF work-award links from FWF's own output lists (oxjob #1765)
-- MAGIC
-- MAGIC FWF publishes, per project, the outputs its researchers report: the Open API `output` index, linked to projects from
-- MAGIC both sides. openalex-ingest `fwf_awards.py` harvests both indexes in one weekly load and writes each project's
-- MAGIC `outputs` (DOI, PMID, title, the authors' surnames) into `s3://openalex-ingest/awards/fwf_open_api/`. This notebook
-- MAGIC reads the newest object there (the Awards job's pin: newest modification time, then path) and writes:
-- MAGIC - `openalex.awards.fwf_work_award_matches`: one row per (project, listed output), resolved or not, with the tier, the
-- MAGIC   evidence, `accepted` and the object read;
-- MAGIC - `openalex.awards.fwf_work_funders`: the accepted links, one row per `(work_id, funder_id)` with the FWF project codes
-- MAGIC   (`award_ids`), read by Nightly Grants' `fwf_work_funders` leg (priority 3, `lib/work_awards.py` LEGS).
-- MAGIC
-- MAGIC **Rule:** the output's DOI (`openalex_works.doi`), else its PMID (`ids.pmid`), resolves when exactly one work holds it.
-- MAGIC The link is accepted when the work's title is close to FWF's or a surname FWF lists is the last name of one of the
-- MAGIC work's authors (works with over `max_authors_checked` authors: the title decides). Measured 11-13 on that day's load:
-- MAGIC 115,630 accepted links; the PMID adds 161, the author check 1,550. FWF's `year` is not used (placeholder years).
-- MAGIC
-- MAGIC **Deletions are mirrored:** a link FWF removes leaves the leg on the next run (only this leg's: a link another source
-- MAGIC asserts stays). Only projects that are FWF awards in `openalex_awards_raw` (provenance `fwf_open_api`) are linked: a code
-- MAGIC Nightly holds no award for would sit unserved in `work_unmatched` (NO_CANDIDATE). A task of Funder Reported Work Funders
-- MAGIC (02:30 UTC, before Nightly Grants), outside that job's additive union. FWF writes a new object weekly (Mondays), so a run
-- MAGIC whose object is the one the last finished run read writes nothing and takes seconds.
-- MAGIC Undo: `RESTORE TABLE openalex.awards.fwf_work_funders TO VERSION AS OF <the version before the last run that changed
-- MAGIC rows>` (DESCRIBE HISTORY: a skipped run commits an empty MERGE, so it is not simply the previous version).

-- COMMAND ----------

-- Both tables exist before anything can fail: Nightly Grants always has its input, and the skip below can read the last run.
CREATE TABLE IF NOT EXISTS openalex.awards.fwf_work_funders (work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.awards.fwf_work_award_matches (
  project_id STRING, output_id STRING, ptype STRING, doi STRING, pmid STRING, title_key STRING, surname_keys ARRAY<STRING>,
  work_id BIGINT, tier STRING, title_distance DOUBLE, author_on_work BOOLEAN, accepted BOOLEAN, fwf_object STRING
) USING delta;

-- COMMAND ----------

-- Regex escaping guard: regex backslashes are double-escaped ('\\s', '\\d'), as walden's SQL does. Stop if this engine
-- reads them differently (the PMID and title normalization would shrink silently).
SELECT assert_true(regexp_extract('see 10.1234/ab.c x', '(10\\.[0-9]{4,9}/[^\\s]+)', 1) = '10.1234/ab.c',
  'FWF_REGEX_MODE: double-escaped regex backslashes did not reach the regex engine as single ones, STOP');

-- COMMAND ----------

-- Titles and names are compared as location_work_match_keys.nt (notebooks/location_matching/sql/keys.sql): markup
-- stripped, common accents folded, lower case, letters and digits only. A test keeps the two the same.
CREATE OR REPLACE TEMPORARY FUNCTION match_text(s STRING) RETURNS STRING RETURN
  nullif(trim(regexp_replace(lower(translate(regexp_replace(coalesce(s, ''), '<[^>]+>', ' '),
    'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
    'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')), '');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE fwf_object STRING;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE fwf_changed BOOLEAN;

-- COMMAND ----------

-- The object this run reads (the Awards job's pin). Every read below filters on it, so only that object's file is read.
SET VAR fwf_object = (SELECT max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path))
  FROM read_files('s3://openalex-ingest/awards/fwf_open_api/*.parquet', format => 'parquet', mergeSchema => true));

-- COMMAND ----------

-- Skip: nothing below writes when the object is the one the last run read and that run finished (the leg holds its
-- accepted links). A run stopped by a gate is redone on the next run.
SET VAR fwf_changed = NOT (
  (SELECT max(fwf_object) FROM openalex.awards.fwf_work_award_matches) <=> session.fwf_object
  AND (SELECT count(DISTINCT work_id, project_id) FROM openalex.awards.fwf_work_award_matches WHERE accepted)
    = (SELECT count(*) FROM (SELECT DISTINCT work_id, a FROM openalex.awards.fwf_work_funders LATERAL VIEW explode(award_ids) t AS a)));

-- COMMAND ----------

SELECT session.fwf_object AS fwf_object, session.fwf_changed AS fwf_changed;

-- COMMAND ----------

-- 1. What FWF lists: one row per (project, output) of the object, for projects that are FWF awards in raw.
DELETE FROM openalex.awards.fwf_work_award_matches WHERE session.fwf_changed;

-- COMMAND ----------

INSERT INTO openalex.awards.fwf_work_award_matches (project_id, output_id, ptype, doi, pmid, title_key, surname_keys, accepted, fwf_object)
WITH
fwf_projects AS (
  SELECT code AS project_id, outputs
  FROM read_files('s3://openalex-ingest/awards/fwf_open_api/*.parquet', format => 'parquet', mergeSchema => true)
  WHERE _metadata.file_path = session.fwf_object AND outputs IS NOT NULL AND session.fwf_changed
),
fwf_award_keys AS (
  SELECT DISTINCT funder_award_id AS project_id
  FROM openalex.awards.openalex_awards_raw
  WHERE provenance = 'fwf_open_api' AND funder_id = 4320321181
),
exploded AS (
  -- explode first, join after: no JOIN directly after LATERAL VIEW (#244)
  SELECT p.project_id, o.output_id, o.type, o.doi, o.pmid, o.title, s AS surname
  FROM fwf_projects p LATERAL VIEW explode(p.outputs) t AS o LATERAL VIEW OUTER explode(o.surnames) u AS s
)
SELECT e.project_id, e.output_id, any_value(e.type), any_value(e.doi), any_value(e.pmid), match_text(any_value(e.title)),
  -- a surname's last word: "Von Haeseler" -> haeseler
  filter(collect_set(element_at(split(match_text(e.surname), ' '), -1)), s -> length(s) >= 2), false, session.fwf_object
FROM exploded e
JOIN fwf_award_keys k ON k.project_id = e.project_id
GROUP BY e.project_id, e.output_id;

-- COMMAND ----------

-- 2. Which work each output is. openalex_works is read once (work_authors).
MERGE INTO openalex.awards.fwf_work_award_matches t
USING (
  WITH
  fwf_rules AS (
    SELECT 0.2 AS max_title_distance, 100 AS max_authors_checked
  ),
  listed AS (
    SELECT * FROM openalex.awards.fwf_work_award_matches WHERE session.fwf_changed
  ),
  listed_ids AS (
    -- each output's identifiers, in the order they are tried
    SELECT project_id, output_id, 'doi' AS tier, 1 AS rank, concat('https://doi.org/', doi) AS id FROM listed WHERE doi IS NOT NULL
    UNION ALL
    SELECT project_id, output_id, 'pmid', 2, pmid FROM listed WHERE pmid IS NOT NULL
  ),
  work_authors AS (
    -- the works those identifiers point at, one row per author (none for big teams)
    SELECT w.id AS work_id, w.doi_key, w.pmid_key, w.title, a.author.display_name AS name
    FROM (SELECT ow.id, lower(ow.doi) AS doi_key, regexp_extract(ow.ids['pmid'], '(\\d+)$', 1) AS pmid_key, ow.title,
            CASE WHEN size(ow.authorships) <= r.max_authors_checked THEN ow.authorships END AS checked_authors
          FROM openalex.works.openalex_works ow CROSS JOIN fwf_rules r
          WHERE lower(ow.doi) IN (SELECT id FROM listed_ids)
             OR regexp_extract(ow.ids['pmid'], '(\\d+)$', 1) IN (SELECT id FROM listed_ids)) w
    LATERAL VIEW OUTER explode(w.checked_authors) t AS a
  ),
  works AS (
    SELECT work_id, doi_key, pmid_key, match_text(any_value(title)) AS title_key,
      collect_set(element_at(split(match_text(name), ' '), -1)) AS last_names
    FROM work_authors
    GROUP BY work_id, doi_key, pmid_key
  ),
  work_ids AS (
    SELECT w.work_id, w.title_key, w.last_names, k AS id FROM works w LATERAL VIEW explode(array(w.doi_key, w.pmid_key)) t AS k
  ),
  resolved AS (
    -- an identifier resolves only when exactly one work holds it
    SELECT l.project_id, l.output_id, l.tier, l.rank, any_value(struct(w.work_id, w.title_key, w.last_names)) AS work
    FROM listed_ids l JOIN work_ids w ON w.id = l.id
    GROUP BY l.project_id, l.output_id, l.tier, l.rank
    HAVING count(DISTINCT w.work_id) = 1
  ),
  picked AS (
    -- the DOI before the PMID
    SELECT project_id, output_id, min_by(struct(tier, work), rank) AS hit FROM resolved GROUP BY project_id, output_id
  ),
  judged AS (
    SELECT p.project_id, p.output_id, p.hit.tier AS tier, p.hit.work.work_id AS work_id,
      levenshtein(substr(p.hit.work.title_key, 1, 200), substr(l.title_key, 1, 200))
        / greatest(length(substr(p.hit.work.title_key, 1, 200)), length(substr(l.title_key, 1, 200)), 1) AS title_distance,
      coalesce(arrays_overlap(l.surname_keys, p.hit.work.last_names), false) AS author_on_work
    FROM picked p JOIN listed l ON l.project_id = p.project_id AND l.output_id = p.output_id
  )
  SELECT j.*, coalesce(j.title_distance <= r.max_title_distance, false) OR j.author_on_work AS accepted
  FROM judged j CROSS JOIN fwf_rules r
) s
ON t.project_id = s.project_id AND t.output_id = s.output_id
WHEN MATCHED THEN UPDATE SET work_id = s.work_id, tier = s.tier, title_distance = s.title_distance,
  author_on_work = s.author_on_work, accepted = s.accepted;

-- COMMAND ----------

-- Gates, before the leg table changes. Each stops the run (the leg keeps the last run's links) and the next run redoes it.
-- 1. The object holds outputs, and its projects are FWF awards in raw: catches an object written with its outputs
--    withheld, a stale raw, or a code-format change (P 31233 vs P31233).
SELECT assert_true(
  (SELECT count(DISTINCT project_id) FROM openalex.awards.fwf_work_award_matches)
  >= greatest(1, 0.99 * (SELECT count(DISTINCT code)
    FROM read_files('s3://openalex-ingest/awards/fwf_open_api/*.parquet', format => 'parquet', mergeSchema => true)
    WHERE _metadata.file_path = session.fwf_object AND outputs IS NOT NULL)),
  'FWF_RAW_COVERAGE: the FWF object lists no outputs, or under 99% of its projects with outputs are fwf_open_api awards in openalex_awards_raw (stale raw or a code-format change), STOP');

-- COMMAND ----------

-- 2. Shrink guard: accepted links may not fall more than 5% below the links the leg holds now (FWF's real deletions are
--    small week to week; a larger drop is a broken load).
SELECT assert_true(
  (SELECT count(DISTINCT work_id, project_id) FROM openalex.awards.fwf_work_award_matches WHERE accepted)
  >= 0.95 * (SELECT count(*) FROM (SELECT DISTINCT work_id, a FROM openalex.awards.fwf_work_funders LATERAL VIEW explode(award_ids) t AS a)),
  'FWF_SHRINK_GUARD: accepted FWF links are more than 5% below the current fwf_work_funders, check the object before replacing');

-- COMMAND ----------

-- 3. Growth ceiling: an over-linking bug stops here (the 11-13 dry run accepted ~115.6K links; a week adds a few hundred).
SELECT assert_true(
  (SELECT count(DISTINCT work_id, project_id) FROM openalex.awards.fwf_work_award_matches WHERE accepted)
  <= greatest(200000, 1.25 * (SELECT count(*) FROM (SELECT DISTINCT work_id, a FROM openalex.awards.fwf_work_funders LATERAL VIEW explode(award_ids) t AS a))),
  'FWF_GROWTH_CEILING: accepted FWF links are over 200,000 and 25% above the current fwf_work_funders, check before replacing');

-- COMMAND ----------

MERGE INTO openalex.awards.fwf_work_funders t
USING (
  SELECT work_id, CAST(4320321181 AS BIGINT) AS funder_id, array_sort(array_distinct(collect_list(project_id))) AS award_ids
  FROM openalex.awards.fwf_work_award_matches
  WHERE accepted AND session.fwf_changed
  GROUP BY work_id
) s
ON t.work_id = s.work_id
WHEN MATCHED AND t.award_ids <> s.award_ids THEN UPDATE SET award_ids = s.award_ids
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND session.fwf_changed THEN DELETE;

-- COMMAND ----------

-- Sanity: how each tier did.
SELECT tier, accepted, count(*) AS listed_outputs, count(DISTINCT work_id) AS works, count(DISTINCT project_id) AS projects
FROM openalex.awards.fwf_work_award_matches GROUP BY ALL ORDER BY ALL;
