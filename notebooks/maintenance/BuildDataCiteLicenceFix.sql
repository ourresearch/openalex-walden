-- Databricks notebook source
-- oxjob #2178: build openalex.works.datacite_licence_fix, the one-time licence fix CreateLocationsMapped applies to
-- DataCite locations parsed before normalize_license learned the URL form of the non-BY Creative Commons licences
-- (oxjob #314, dlt_utils 0.3.27, 8 Sep 2026). Until then creativecommons.org/licenses/by-nc/4.0 and its by-sa, by-nd,
-- by-nc-sa, by-nc-nd siblings parsed to NULL, and datacite_works only re-parses a record when DataCite re-sends it, so
-- ~11.1M locations (GBIF downloads 3.7M, UNITE species hypotheses 3.5M, Huma-Num, Zenodo, arXiv) still hold no licence.
-- One row per DataCite DOI whose parsed location holds no licence and whose latest record's rights URI is one of those
-- five licences under today's normalize_license.
--
-- raw_license is the parser's pick (DataCite.py): the first rightsUri containing 'creativecommons.org', else the first
-- rightsUri. The CASE is normalize_license's lookup order for the five flavours (spaces and hyphens stripped, lower
-- case; the longer flavour first). Other values are left alone on purpose: gpl / apache are nulled by CreateWorksBase
-- anyway, and the 2.2K NULL -> cc-by rows are Open Data Commons BY (opendatacommons.org/licenses/by), not CC BY.
--
-- Reads locations_w_types (the parsed licence, before CreateLocationsMapped's override), never locations_mapped, so a
-- re-run rebuilds the same rows. The override applies only while the location still holds no licence AND the
-- updated_date it was built against, so a record DataCite re-sends takes its own freshly parsed licence.
-- Revert: drop the lf join and CASE in CreateLocationsMapped first, then this table (the LEFT JOIN fails on a missing
-- table). Emptying the table alone also reverts: the next nightly serves no licence on these locations again.
--
-- Latest record per DOI = the one DataCite.py's apply_changes keeps (attributes.updated, then ingested_at); never
-- max_by(..., ingested_at) alone (oxjob #2091: ~200M rows have NULL ingested_at).

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.datacite_licence_fix
COMMENT 'oxjob #2178: the Creative Commons BY-NC / BY-SA / BY-ND / BY-NC-SA / BY-NC-ND licence of DataCite records parsed before normalize_license read those URLs (oxjob #314); applied by CreateLocationsMapped while the location still holds no licence and fix_updated_date'
AS
WITH raw AS (
  SELECT id AS native_id,
         max_by(attributes.rightsList.rightsUri,
                struct(try_cast(attributes.updated AS TIMESTAMP) AS u, coalesce(ingested_at, TIMESTAMP'1970-01-01') AS i)) AS uris
  FROM openalex.datacite.datacite_items
  GROUP BY id
),
rl AS (
  SELECT native_id,
         coalesce(get(filter(uris, x -> contains(x, 'creativecommons.org')), 0), get(uris, 0)) AS raw_license
  FROM raw
),
nl AS (
  SELECT native_id, raw_license,
         CASE WHEN contains(t, 'elsevier.com/openaccess/userlicense')
                OR contains(t, 'pubs.acs.org/page/policy/authorchoice_termsofuse.html')
                OR contains(t, 'arxiv.orgperpetual') OR contains(t, 'arxiv.orgnonexclusive') THEN NULL
              WHEN contains(t, 'ccbyncnd') OR contains(t, 'licenses/byncnd') THEN 'cc-by-nc-nd'
              WHEN contains(t, 'ccbyncsa') OR contains(t, 'licenses/byncsa') THEN 'cc-by-nc-sa'
              WHEN contains(t, 'ccbynd') OR contains(t, 'licenses/bynd') THEN 'cc-by-nd'
              WHEN contains(t, 'ccbysa') OR contains(t, 'licenses/bysa') THEN 'cc-by-sa'
              WHEN contains(t, 'ccbync') OR contains(t, 'licenses/bync') THEN 'cc-by-nc'
         END AS fix_license
  FROM (SELECT *, lower(replace(replace(raw_license, ' ', ''), '-', '')) AS t FROM rl WHERE raw_license IS NOT NULL)
),
lt AS (
  SELECT native_id_namespace, native_id, license, updated_date
  FROM openalex.works.locations_w_types
  WHERE provenance = 'datacite'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id ORDER BY updated_date DESC) = 1
)
SELECT lt.native_id_namespace, lt.native_id, nl.fix_license, nl.raw_license, lt.updated_date AS fix_updated_date,
       current_timestamp() AS built_at
FROM lt JOIN nl ON nl.native_id = lt.native_id
WHERE lt.license IS NULL
  AND nl.fix_license IS NOT NULL

-- COMMAND ----------

SELECT fix_license, count(*) AS rows, count(DISTINCT native_id_namespace, native_id) AS keys
FROM openalex.works.datacite_licence_fix
GROUP BY ROLLUP(fix_license) ORDER BY rows DESC
