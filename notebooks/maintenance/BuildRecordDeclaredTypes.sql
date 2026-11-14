-- Databricks notebook source
-- openalex.works.record_declared_types: one row per record (provenance, native_id, as in locations_mapped) whose own host
-- declares a work type other than article, for records our pipeline types article only because they say nothing (mostly
-- MAG crawls of repository and catalogue pages, which fall to the type cascade's 'default: -> article'). Read by
-- CreateWorksBase's final type CASE (rdt join): when such a record leads a work typed article, the work takes the declared
-- type. Type only: location types, matching and merges are untouched.
-- Shared like openalex.sources.article_page_journal_sources: each job inserts and deletes only its own platform's rows.
--   platform 'diva' (oxjob #2236): DiVA, the shared repository of ~50 Swedish universities (diva-portal.org, diva2:<pid>)
-- Revert one platform: DELETE FROM openalex.works.record_declared_types WHERE platform = '<platform>' (the next night types
-- its works back). Revert the mechanism: drop the rdt join and its WHEN in CreateWorksBase first (its LEFT JOIN fails on a
-- missing table), then this table.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.works.record_declared_types (
  provenance STRING COMMENT 'locations_mapped.provenance of the record (mag, repo, ...)',
  native_id STRING COMMENT 'locations_mapped.native_id of the record (for mag: the MAG paper id)',
  type STRING COMMENT 'the OpenAlex work type the host''s own declaration maps to (never article)',
  platform STRING COMMENT 'whose rows these are: diva (oxjob 2236). Each job inserts and deletes only its own platform',
  record_key STRING COMMENT 'the host''s own id for the record (diva2:<pid>)',
  declared_code STRING COMMENT 'the type the host declares, in its own words (DiVA publicationTypeCode)',
  oxjob INT COMMENT 'job that wrote the row',
  harvested_at TIMESTAMP COMMENT 'when the host''s declarations were read'
)
COMMENT 'One row per record whose own host declares a work type other than article (theses, conference papers, chapters, reports on repository and catalogue pages our pipeline would type article). Read by the final type CASE of the End 2 End notebook CreateWorksBase. Each job inserts and deletes only its own platform: diva (oxjob 2236). Built by notebooks/maintenance/BuildRecordDeclaredTypes.sql.'

-- COMMAND ----------

-- platform 'diva' (oxjob #2236). DiVA's own publicationTypeCode per record, from DiVA's public search export
-- (https://www.diva-portal.org/smash/export.jsf, format=csv: PIDs only), one request per code and year range, harvested
-- 14 Nov 2026 by oxjobs #2236 work/harvest_diva_pids.py and joined into one file `pid,diva_type_code` at
-- /Volumes/openalex/works/data/diva/pid_types_2026_11.csv. Records whose URL names a diva2 pid get its type:
--   studentThesis, comprehensive/monograph Doctoral/Licentiate Thesis -> dissertation
--   conferencePaper -> conference-paper          chapter -> book-chapter          report -> report
--   book, collection (edited volume), conferenceProceedings -> book                manuscript ('Manuskript (preprint)') -> preprint
--   review (research review article) -> review   bookReview -> book-review        dataset -> dataset
--   other, patent, artisticOutput -> other
-- DiVA's article records are not harvested (they stay article). Manuscripts carry no dateIssued, so they were fetched with no
-- date filter (work/harvest_nodate.py); the rest by code and year. A record whose URLs name pids of two different types, or a
-- pid listed under two codes, gets no row. To refresh: harvest again, copy the new file there, set the path below, re-run.
DELETE FROM openalex.works.record_declared_types WHERE platform = 'diva';

-- COMMAND ----------

INSERT INTO openalex.works.record_declared_types
WITH harvest AS (
  SELECT TRY_CAST(_c0 AS BIGINT) AS pid, _c1 AS code
  FROM read_files('/Volumes/openalex/works/data/diva/pid_types_2026_11.csv',
                  format => 'csv', header => true, schema => '_c0 STRING, _c1 STRING')
),
pid_code AS (
  SELECT pid, min(code) AS code FROM harvest WHERE pid IS NOT NULL GROUP BY pid HAVING count(DISTINCT code) = 1
),
mapped AS (
  SELECT pid, code,
    CASE
      WHEN code IN ('studentThesis', 'comprehensiveDoctoralThesis', 'monographDoctoralThesis',
                    'comprehensiveLicentiateThesis', 'monographLicentiateThesis') THEN 'dissertation'
      WHEN code = 'conferencePaper' THEN 'conference-paper'
      WHEN code = 'chapter' THEN 'book-chapter'
      WHEN code = 'report' THEN 'report'
      WHEN code IN ('book', 'collection', 'conferenceProceedings') THEN 'book'
      WHEN code = 'manuscript' THEN 'preprint'
      WHEN code = 'review' THEN 'review'
      WHEN code = 'bookReview' THEN 'book-review'
      WHEN code = 'dataset' THEN 'dataset'
      WHEN code IN ('other', 'patent', 'artisticOutput') THEN 'other'
    END AS type
  FROM pid_code
),
recs AS (
  -- every record of ours whose URL is a DiVA page naming a pid
  SELECT DISTINCT provenance, native_id,
         TRY_CAST(REGEXP_EXTRACT(landing_page_url, '(?i)diva2(?::|%3A)([0-9]+)', 1) AS BIGINT) AS pid
  FROM openalex.works.locations_mapped
  WHERE landing_page_url ILIKE '%diva-portal.org/%' AND landing_page_url RLIKE '(?i)diva2(:|%3A)[0-9]+'
),
typed AS (
  SELECT r.provenance, r.native_id, min(m.type) AS type, count(DISTINCT m.type) AS n_types,
         min(concat('diva2:', CAST(r.pid AS STRING))) AS record_key, min(m.code) AS declared_code
  FROM recs r JOIN mapped m USING (pid)
  GROUP BY r.provenance, r.native_id
)
SELECT provenance, native_id, type, 'diva' AS platform, record_key, declared_code, 2236 AS oxjob,
       TIMESTAMP '2026-11-14 19:57:00' AS harvested_at  -- UTC: harvest finished 13:57 CT
FROM typed
WHERE n_types = 1 AND type IS NOT NULL

-- COMMAND ----------

SELECT platform, provenance, declared_code, type, count(*) AS records
FROM openalex.works.record_declared_types GROUP BY ALL ORDER BY records DESC
