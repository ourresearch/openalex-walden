-- Databricks notebook source
-- oxjob #2148: build openalex.works.b3kat_catalogue_types, one row per B3Kat union-catalogue record (Bavarian and
-- Berlin-Brandenburg library networks; OAI endpoint 5a4c71918fc62d3e4ce, ids oai:aleph.bib-bvb.de:BVB01-<n>) that our
-- feed holds and that B3Kat's own catalogue calls a monograph or a serial. The feed's dc:type is only 'text' (or a German
-- genre word such as 'Zeitschrift'), which the type classifier reads as article (3.65M works typed article, ~12K book).
-- The catalogue's own MARC leader decides instead (CreateLocationsMapped's b3 join):
--   leader/06 'a' (language material) and leader/07 'm' (a monograph) -> book   (where the classifier says article)
--   leader/07 's' (a serial: journal, newspaper, monographic series)   -> other  (where it says article or book;
--                                                                                as the type dictionary's ('repo','serial','other'))
-- Component parts (leader/07 a, b), collections, integrating resources and every other classifier type are left alone.
--
-- Source: B3Kat's CC0 open-data dump (https://www.bib-bvb.de/web/b3kat/open-data, b3kat_export_YYYY_MM_teilNN.xml.gz,
-- MARC-XML), cut per record by oxjobs #2148 work/b3kat_dump_slim.py to 9 columns (oai_ids = MARC 943 $a, bv = 001, leader,
-- has_773, host_zdb, has_020, has_502, gnd_content, year008) and copied to /Volumes/openalex/works/data/b3kat/<dump>/.
-- To refresh (new records harvested since): fetch a newer dump, cut it the same way, copy it there, set the path and the
-- dump name below, run this notebook. Records missing from the table keep the classifier's type.
-- Revert: drop the b3 join in CreateLocationsMapped first (its LEFT JOIN fails on a missing table), then this table.

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.b3kat_catalogue_types
CLUSTER BY (provenance, native_id_namespace, native_id)
COMMENT 'oxjob #2148: B3Kat union-catalogue records (OAI endpoint 5a4c71918fc62d3e4ce) that the catalogue calls a monograph (-> book) or a serial (-> other), from the MARC leader in B3Kat''s CC0 open-data dump. Built by notebooks/maintenance/BuildB3KatCatalogueTypes.sql.'
AS
WITH dump AS (
  SELECT explode(split(_c0, '[|]')) AS native_id, _c1 AS bv, _c2 AS leader
  FROM read_files('/Volumes/openalex/works/data/b3kat/slim_2026_11/',
                  format => 'csv', sep => '\t', header => false, quote => '\u0000', escape => '\u0000',
                  schema => '_c0 STRING, _c1 STRING, _c2 STRING, _c3 STRING, _c4 STRING, _c5 STRING, _c6 STRING, _c7 STRING, _c8 STRING')
),
recs AS (
  -- one row per OAI id; an id on two dump records keeps a type only if both records agree
  SELECT native_id, min(bv) AS bv, min(leader) AS leader, count(DISTINCT substr(leader, 7, 2)) AS n_levels
  FROM dump WHERE native_id LIKE 'oai:aleph.bib-bvb.de:BVB01-%' GROUP BY native_id
),
typed AS (
  SELECT *, substr(leader, 7, 1) AS record_type, substr(leader, 8, 1) AS bib_level,
         CASE WHEN substr(leader, 7, 1) = 'a' AND substr(leader, 8, 1) = 'm' THEN 'book'
              WHEN substr(leader, 8, 1) = 's' THEN 'other' END AS catalogue_type
  FROM recs WHERE n_levels = 1
),
ours AS (
  -- only records our B3Kat feed holds, so the nightly join stays small
  SELECT DISTINCT native_id FROM openalex.works.locations_mapped
  WHERE provenance = 'repo' AND native_id_namespace = 'pmh' AND native_id LIKE 'oai:aleph.bib-bvb.de:BVB01-%'
)
SELECT 'repo' AS provenance, 'pmh' AS native_id_namespace, t.native_id, t.bv, t.leader, t.record_type, t.bib_level,
       t.catalogue_type, 'b3kat_export_2026_11' AS dump, current_timestamp() AS built_at
FROM typed t JOIN ours o USING (native_id)
WHERE t.catalogue_type IS NOT NULL

-- COMMAND ----------

SELECT record_type, bib_level, catalogue_type, count(*) AS records
FROM openalex.works.b3kat_catalogue_types GROUP BY 1, 2, 3 ORDER BY 4 DESC
