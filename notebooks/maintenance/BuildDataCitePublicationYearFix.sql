-- Databricks notebook source
-- oxjob #2091: build openalex.works.datacite_publication_year_fix, the one-time date fix CreateLocationsMapped applies to
-- DataCite locations parsed before DataCite.py's DATACITE_NODATES_PY_SQL rule. One row per DataCite DOI whose latest
-- record has no dates[], whose parsed location date IS the record's registration date (least(registered, created)), and
-- whose publicationYear (1900 .. this year) is earlier than that date's year. New date = 1 Jan of publicationYear.
-- EMSL/PNNL (10.25582) is ~1.33M of the ~2.0M rows (Sun 8 Nov 2026).
--
-- Reads locations_w_types (the parsed dates, before CreateLocationsMapped's override), never locations_mapped, so a
-- re-run rebuilds the same rows instead of finding the fixed dates and emptying the table. Records re-parsed under the
-- new rule drop out by themselves (their date is no longer the registration date, so the override stops applying).
-- Revert: drop the fx join and CASE in CreateLocationsMapped first, then this table (the LEFT JOIN fails on a missing
-- table). Emptying the table alone also reverts: the next nightly serves the registration dates again.

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.datacite_publication_year_fix
COMMENT 'oxjob #2091: 1 Jan of publicationYear for DataCite records with no dates[] that were dated by a DOI registration in a later year; applied by CreateLocationsMapped while the location still holds fix_old_date'
AS
WITH raw AS (
  SELECT id AS native_id,
         max_by(struct(attributes.publicationYear AS py, attributes.dates AS dates,
                       attributes.registered AS registered, attributes.created AS created), ingested_at) AS r
  FROM openalex.datacite.datacite_items
  GROUP BY id
),
nodates AS (
  SELECT native_id, try_cast(r.py AS INT) AS py,
         least(to_date(r.registered), to_date(r.created)) AS reg_date
  FROM raw
  WHERE r.dates IS NULL OR size(r.dates) = 0
),
lt AS (
  SELECT native_id_namespace, native_id, published_date
  FROM openalex.works.locations_w_types
  WHERE provenance = 'datacite'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id ORDER BY updated_date DESC) = 1
)
SELECT lt.native_id_namespace, lt.native_id, n.py AS publication_year,
       lt.published_date AS fix_old_date, make_date(n.py, 1, 1) AS fix_new_date,
       current_timestamp() AS built_at
FROM lt JOIN nodates n ON n.native_id = lt.native_id
WHERE n.py BETWEEN 1900 AND year(current_date())
  AND year(lt.published_date) > n.py
  AND lt.published_date = n.reg_date

-- COMMAND ----------

SELECT count(*) AS rows, count(DISTINCT native_id_namespace, native_id) AS keys,
       count_if(native_id LIKE '10.25582/%') AS emsl, min(publication_year) AS min_py, max(publication_year) AS max_py
FROM openalex.works.datacite_publication_year_fix
