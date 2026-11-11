-- Databricks notebook source
-- oxjob #2158: build openalex.works.citeseerx_publication_date_fix, the one-time date fix CreateLocationsMapped applies to
-- CiteSeerX locations (repository endpoint 7e4fec42e1147584aae). The feed is frozen: every record sits in
-- openalex.repo.repo_items_backfill (9.05M rows, last datestamp 2016-11-22; none in the live harvest), so this table is
-- complete and needs no parser change.
--
-- CiteSeerX dates no work (wave 2, 11 Nov 2026). Its oai_dc records carry two record-keeping dc:dates, the datestamp and
-- the day CiteSeerX created the record (often years earlier: 2007-11-21 / 2008-04-01 are its bulk loads), and, when
-- CiteSeerX's extractor found one in the PDF, a bare 4-digit year. The parser takes the earliest dc:date, so a record
-- with no bare year was dated by a crawl day (wave 1: 56 of 57 checkable works had another year). The bare years fail
-- too: on 151 independently confirmed CiteSeerX-only works, 13.9% were off by more than a year (download stamps, birth
-- years, deposit stamps), against the 2% bar. So every CiteSeerX record with a date gets NULL (xpac-integration charter,
-- "No year beats a doubtful year", Jason 26 Oct 2026); bare_year is kept for reference only.
--
-- Reads locations_w_types (the parsed dates, before CreateLocationsMapped's override), never locations_mapped, so a re-run
-- rebuilds the same rows. The override applies only while the location still holds fix_old_date.
-- Revert: drop the cx join and WHEN line in CreateLocationsMapped first, then this table (the LEFT JOIN fails on a
-- missing table). Emptying the table alone also reverts: the next nightly serves the parsed dates again. Wave 2 alone:
-- rebuild from this file at walden 63c4f36e (bare years kept).

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.citeseerx_publication_date_fix
COMMENT 'oxjob #2158: CiteSeerX locations date no work: their dc:dates are record-keeping dates or extracted years that fail the 2% bar. Applied by CreateLocationsMapped while the location still holds fix_old_date'
AS
WITH raw AS (
  SELECT pmh_id AS native_id,
         to_date(regexp_extract(api_raw, '<datestamp>(.*?)</datestamp>', 1)) AS ds,
         array_min(filter(
           transform(filter(regexp_extract_all(api_raw, '<dc:date[^>]*>(.*?)</dc:date>', 1), x -> trim(x) RLIKE '^[0-9]{4}$'),
                     x -> cast(trim(x) AS INT)),
           y -> y >= 1900)) AS bare_year
  FROM openalex.repo.repo_items_backfill
  WHERE endpoint_id = '7e4fec42e1147584aae'
),
latest AS (
  -- every copy of a record carries the same bare year (checked 11 Nov 2026: 0 records with two)
  SELECT native_id, max_by(bare_year, ds) AS bare_year
  FROM raw
  GROUP BY native_id
),
lt AS (
  SELECT native_id_namespace, native_id, published_date
  FROM openalex.works.locations_w_types
  WHERE provenance = 'repo' AND endpoint_id = '7e4fec42e1147584aae'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id ORDER BY updated_date DESC) = 1
)
SELECT lt.native_id_namespace, lt.native_id, l.bare_year,
       lt.published_date AS fix_old_date,
       CAST(NULL AS DATE) AS fix_new_date,
       current_timestamp() AS built_at
FROM lt LEFT JOIN latest l USING (native_id)
WHERE lt.published_date IS NOT NULL

-- COMMAND ----------

SELECT count(*) AS rows, count(DISTINCT native_id_namespace, native_id) AS keys,
       count_if(fix_new_date IS NULL) AS to_no_date, count_if(bare_year IS NOT NULL) AS had_bare_year
FROM openalex.works.citeseerx_publication_date_fix
