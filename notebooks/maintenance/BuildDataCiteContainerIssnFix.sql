-- Databricks notebook source
-- oxjob #693: build openalex.works.datacite_container_issn_fix, the one-time list of journal ISSNs that DataCite records
-- name only in `container` or in relatedItems, never in a relatedIdentifier, so the parsed location's `ids` lacks them and
-- CreateLocationsWithSources' container-ISSN tier cannot put the record on its journal.
--
-- Why a table and not a re-parse: walden 6022b344 (9 Nov 2026) taught DataCite.py to add the container ISSN to `ids`, but
-- datacite_parsed re-reads a record only when DataCite re-sends it. Records harvested before DataCite began filling
-- `container` from relatedItems carry `container: {}` and only the relatedItems entry (Open Gender Journal,
-- 10.17169/ogj.2023.227); later ones carry both (Journal of Nanostructure in Chemistry, 10.57647/jnsc.2025.1501.01).
-- On 16 Nov 2026: 170,189 DOIs (117,680 with the ISSN in container, 52,514 in relatedItems only).
--
-- One row per (DataCite location, normalized ISSN) where the DOI's latest raw record names the ISSN as
--   container.identifier with identifierType ISSN / EISSN / LISSN, or
--   a relatedItem with relationType IsPublishedIn / IsPartOf and relatedItemIdentifierType ISSN / EISSN / LISSN,
-- and the location's `ids` holds no IsPartOf / IsPublishedIn *issn entry with that ISSN. The tier still applies its own
-- uniqueness rule (every ISSN of a location names one live journal), genre guard and skip-when-present to these rows.
--
-- Guard: a row counts only while the location keeps fix_updated_date, the updated_date it was built against, so a record
-- DataCite re-sends is read from its own freshly parsed `ids`.
-- Latest record per DOI = the one DataCite.py's apply_changes keeps (attributes.updated, then ingested_at); never
-- max_by(..., ingested_at) alone (oxjob #2091: ~200M rows have NULL ingested_at).
-- Revert: delete the oxjob #693 union branch in CreateLocationsWithSources' datacite_issn_exploded first, then this table
-- (the join fails on a missing table). Emptying the table alone also reverts: the next nightly matches as before.

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.datacite_container_issn_fix
COMMENT 'oxjob #693: journal ISSNs DataCite records name only in container or relatedItems (never in ids), read by CreateLocationsWithSources container-ISSN tier while the location keeps fix_updated_date'
AS
WITH has_issn AS (
  SELECT DISTINCT id FROM openalex.datacite.datacite_items
  WHERE (attributes.container.identifier IS NOT NULL AND lower(attributes.container.identifierType) IN ('issn', 'eissn', 'lissn'))
     OR exists(attributes.relatedItems, x -> lower(x) LIKE '%issn%')
),
latest AS (
  SELECT d.id AS native_id,
         max_by(struct(d.attributes.container AS c, d.attributes.relatedItems AS ri, d.attributes.updated AS updated),
                struct(try_cast(d.attributes.updated AS TIMESTAMP) AS u, coalesce(d.ingested_at, TIMESTAMP'1970-01-01') AS i)) AS r
  FROM openalex.datacite.datacite_items d JOIN has_issn h ON h.id = d.id
  GROUP BY d.id
),
cand AS (
  SELECT native_id, r.c.identifier AS issn, lower(r.c.identifierType) AS namespace, 'container' AS via, r.updated AS raw_updated
  FROM latest
  WHERE lower(r.c.identifierType) IN ('issn', 'eissn', 'lissn') AND r.c.identifier rlike '\\d{4}-?\\d{3}[\\dXx]'
  UNION ALL
  -- relatedItems arrive as JSON strings
  SELECT native_id, get_json_object(x, '$.relatedItemIdentifier.relatedItemIdentifier'),
         lower(get_json_object(x, '$.relatedItemIdentifier.relatedItemIdentifierType')), 'relatedItems', r.updated
  FROM latest LATERAL VIEW explode(r.ri) t AS x
  WHERE lower(get_json_object(x, '$.relationType')) IN ('ispublishedin', 'ispartof')
    AND lower(get_json_object(x, '$.relatedItemIdentifier.relatedItemIdentifierType')) IN ('issn', 'eissn', 'lissn')
    AND get_json_object(x, '$.relatedItemIdentifier.relatedItemIdentifier') rlike '\\d{4}-?\\d{3}[\\dXx]'
),
norm AS (
  -- the tier's normalization, pinned to datacite_issn_journal_map's
  SELECT native_id, namespace, via, raw_updated,
         upper(concat(regexp_extract(issn, '(\\d{4})-?(\\d{3}[\\dXx])', 1), '-',
                      regexp_extract(issn, '(\\d{4})-?(\\d{3}[\\dXx])', 2))) AS norm_issn
  FROM cand
),
sl AS (
  SELECT native_id_namespace, native_id, updated_date, raw_type, ids
  FROM openalex.works.superlocations
  WHERE provenance = 'datacite'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY native_id_namespace, native_id ORDER BY updated_date DESC) = 1
)
SELECT sl.native_id_namespace, sl.native_id, n.norm_issn AS issn, min(n.namespace) AS namespace,
       array_join(array_sort(collect_set(n.via)), '+') AS via, max(n.raw_updated) AS raw_updated,
       sl.updated_date AS fix_updated_date, sl.raw_type, current_timestamp() AS built_at
FROM norm n
JOIN sl ON sl.native_id = n.native_id
WHERE NOT exists(sl.ids, x -> contains(lower(x.namespace), 'issn')
                   AND lower(x.relationship) IN ('ispartof', 'ispublishedin')
                   AND upper(concat(regexp_extract(x.id, '(\\d{4})-?(\\d{3}[\\dXx])', 1), '-',
                                    regexp_extract(x.id, '(\\d{4})-?(\\d{3}[\\dXx])', 2))) = n.norm_issn)
GROUP BY sl.native_id_namespace, sl.native_id, n.norm_issn, sl.updated_date, sl.raw_type

-- COMMAND ----------

SELECT via, namespace, count(*) AS rows, count(DISTINCT native_id) AS dois
FROM openalex.works.datacite_container_issn_fix
GROUP BY ROLLUP(via, namespace) ORDER BY rows DESC
