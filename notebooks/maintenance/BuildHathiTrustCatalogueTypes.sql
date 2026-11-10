-- Databricks notebook source
-- oxjob #2139: build openalex.works.hathitrust_catalogue_types, one row per HathiTrust catalogue record as the University of
-- Michigan OAI feed (source S7407064297) names it: oai:quod.lib.umich.edu:MIU01-<ht_bib_key>. Each record is a whole
-- digitised volume (a book, or a serial's run), but its dc:type is only 'text', which the type classifier read as article
-- (2.57M works, 0 books). The catalogue's own bibliographic level decides instead (CreateLocationsMapped's ht join):
--   BK (a monograph)  -> book
--   SE (a serial run) -> other   (as the type dictionary's ('repo','serial','other') and ('repo','periodical','other'))
-- It also keeps the year the catalogue's imprint states (MARC 260/264 $c): the first year of a range ('1862-69' -> 1862),
-- the corrected year after 'i.e.', none for decade or century guesses ('[189-?]', '[19--]'); imprint_probable marks a
-- cataloguer's probable date ('[1904?]'). NOT USED: as a fill for the 962K undated works it failed its pre-registered bar
-- (oxjob #2139, 10 Nov 2026: off by more than a year on 4.0% of independently checked books, 26.7% of serial runs;
-- bar 2%, #1644's), so no year is written from it. HathiTrust's coded rights date (rights_date_used) is not used either:
-- it turns unknown digits into 9s and dates a multi-volume set by a later volume.
--
-- Source: HathiTrust's monthly Hathifile (https://www.hathitrust.org/hathifiles, hathi_full_YYYYMMDD.txt.gz), cut to 9
-- columns (cut -f1,2,3,4,5,13,16,17,20: htid, access, rights, ht_bib_key, description, imprint, us_gov_doc_flag,
-- rights_date_used, bib_fmt) and copied to /Volumes/openalex/works/data/hathitrust/. To refresh: fetch a newer file, cut
-- the same columns, copy it there, set the path and the hathifile name below, run this notebook.
-- Revert: drop the ht join in CreateLocationsMapped first (its LEFT JOIN fails on a missing table), then this table.

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.hathitrust_catalogue_types
CLUSTER BY (provenance, native_id_namespace, native_id)
COMMENT 'oxjob #2139: HathiTrust catalogue records (University of Michigan OAI feed) -> work type from the bibliographic level (BK book, SE other) and the imprint year. Built by notebooks/maintenance/BuildHathiTrustCatalogueTypes.sql.'
AS
WITH items AS (
  SELECT _c3 AS ht_bib_key, _c5 AS imprint, _c8 AS bib_fmt, _c0 AS htid
  FROM read_files('/Volumes/openalex/works/data/hathitrust/hathi_full_20261101_slim.tsv.gz',
                  format => 'csv', sep => '\t', header => false, quote => '\u0000', escape => '\u0000',
                  schema => '_c0 STRING, _c1 STRING, _c2 STRING, _c3 STRING, _c4 STRING, _c5 STRING, _c6 STRING, _c7 STRING, _c8 STRING')
  WHERE _c3 RLIKE '^[0-9]+$'
),
recs AS (
  SELECT ht_bib_key, min(bib_fmt) AS bib_fmt, count(DISTINCT bib_fmt) AS n_fmt, max_by(imprint, htid) AS imprint
  FROM items GROUP BY ht_bib_key
),
yr AS (
  SELECT *,
    (imprint RLIKE '(^|[^0-9])1[0-9]{2}-' OR imprint RLIKE '(^|[^0-9])1[0-9]--') AS decade_guess,
    coalesce(
      try_cast(nullif(regexp_extract(imprint, 'i\\.\\s?e\\.,?\\s*\\[?(1[4-9][0-9]{2}|20[0-2][0-9])', 1), '') AS INT),
      try_cast(nullif(regexp_extract(imprint, '(1[4-9][0-9]{2}|20[0-2][0-9])', 1), '') AS INT)) AS first_year
  FROM recs WHERE n_fmt = 1 AND bib_fmt IN ('BK', 'SE')
)
SELECT
  'repo' AS provenance,
  'pmh' AS native_id_namespace,
  concat('oai:quod.lib.umich.edu:MIU01-', ht_bib_key) AS native_id,
  ht_bib_key,
  bib_fmt,
  CASE bib_fmt WHEN 'BK' THEN 'book' WHEN 'SE' THEN 'other' END AS catalogue_type,
  imprint,
  CASE WHEN NOT decade_guess AND first_year <= year(current_date()) THEN first_year END AS imprint_year,
  imprint RLIKE '[0-9]{4} ?\\?' AS imprint_probable,
  'hathi_full_20261101' AS hathifile,
  current_timestamp() AS built_at
FROM yr

-- COMMAND ----------

SELECT bib_fmt, catalogue_type, count(*) AS records, count(imprint_year) AS with_year, count_if(imprint_probable) AS probable
FROM openalex.works.hathitrust_catalogue_types GROUP BY 1, 2 ORDER BY 1
