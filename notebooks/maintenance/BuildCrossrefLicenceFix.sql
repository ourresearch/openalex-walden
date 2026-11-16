-- Databricks notebook source
-- oxjob #1228: build openalex.works.crossref_licence_fix, the one-time licence fix CreateLocationsMapped applies to
-- Crossref locations parsed before normalize_license read the publishers' own open access licences (dlt_utils 0.3.42):
-- IEEE's Open Access Publishing Agreement (ieeexplore.ieee.org/Xplorehelp/downloads/license-information/OAPA.html,
-- 24,347 Crossref DOIs on 16 Nov 2026) and Optica's open access licence (doi.org/10.1364/OA_License_v1#VOR-OA 59,286,
-- OA_License_v2#VOR-OA 23,596). crossref_parsed re-parses a record only when Crossref re-sends it, so until then these
-- copies hold no licence and, since Crossref.py sets is_oa from the licence, are closed on the Crossref record: works in
-- toll-access journals read bronze (free file found elsewhere), green or closed instead of hybrid.
-- One row per Crossref DOI whose parsed location holds no licence and whose record's licence pick is one of those URLs.
--
-- raw_license is the parser's pick (Crossref.py): drop content-version 'tdm', then the first URL containing
-- 'creativecommons.org', else the first URL. The CASE is normalize_license's three new keys only (spaces and hyphens
-- stripped, lower case); every other value is left alone on purpose.
--
-- Reads locations_w_types (the parsed licence, before CreateLocationsMapped's override), never locations_mapped, so a
-- re-run rebuilds the same rows. The override applies only while the location still holds no licence AND the
-- updated_date it was built against, so a record Crossref re-sends takes its own freshly parsed licence.
-- Revert: drop the cl join and the two cl CASE lines in CreateLocationsMapped first, then this table (the LEFT JOIN
-- fails on a missing table). Emptying the table alone also reverts: the next nightly serves no licence on these again.

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.crossref_licence_fix
COMMENT 'oxjob #1228: the IEEE OAPA / Optica VOR-OA licence (other-oa) of Crossref records parsed before normalize_license read those URLs; applied by CreateLocationsMapped (licence and is_oa) while the location still holds no licence and fix_updated_date'
AS
WITH rl AS (
  SELECT native_id,
         coalesce(get(filter(filter(license, x -> x.`content-version` != 'tdm').URL, u -> contains(u, 'creativecommons.org')), 0),
                  get(filter(license, x -> x.`content-version` != 'tdm').URL, 0)) AS raw_license
  FROM openalex.crossref.crossref_deduplicated
  WHERE exists(license, x -> lower(x.URL) LIKE '%license-information/oapa.html%'
                          OR lower(x.URL) LIKE '%10.1364/oa_license_v%#vor-oa%')
),
nl AS (
  SELECT native_id, raw_license,
         CASE WHEN contains(t, 'licenseinformation/oapa.html')
                OR contains(t, '10.1364/oa_license_v1#voroa')
                OR contains(t, '10.1364/oa_license_v2#voroa') THEN 'other-oa'
         END AS fix_license
  FROM (SELECT *, lower(replace(replace(raw_license, ' ', ''), '-', '')) AS t FROM rl WHERE raw_license IS NOT NULL)
),
lt AS (
  SELECT native_id_namespace, native_id, license, updated_date
  FROM openalex.works.locations_w_types
  WHERE provenance = 'crossref'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id ORDER BY updated_date DESC) = 1
)
SELECT lt.native_id_namespace, lt.native_id, nl.fix_license, nl.raw_license, lt.updated_date AS fix_updated_date,
       current_timestamp() AS built_at
FROM lt JOIN nl ON nl.native_id = lt.native_id
WHERE lt.license IS NULL
  AND nl.fix_license IS NOT NULL

-- COMMAND ----------

SELECT family, count(*) AS rows, count(DISTINCT native_id_namespace, native_id) AS keys
FROM (SELECT *, CASE WHEN raw_license ILIKE '%OAPA.html%' THEN 'ieee-oapa'
                     ELSE regexp_extract(raw_license, 'OA_License_v[0-9]#VOR-OA', 0) END AS family
      FROM openalex.works.crossref_licence_fix)
GROUP BY ROLLUP(family) ORDER BY rows DESC
