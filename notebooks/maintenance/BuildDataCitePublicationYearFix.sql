-- Databricks notebook source
-- oxjob #2091 + #126: build openalex.works.datacite_publication_year_fix, the one-time date fix CreateLocationsMapped
-- applies to DataCite locations parsed before DataCite.py's date rules. One row per DataCite DOI whose stored date the
-- current rules would change; `rule` says which:
--   2091_nodates: the latest record has no dates[], its parsed location date IS the record's registration date
--     (least(registered, created)), and its publicationYear (1900 .. this year) is earlier than that date's year. New
--     date = 1 Jan of publicationYear. EMSL/PNNL (10.25582) is ~1.33M of the ~2.0M rows (Sun 8 Nov 2026).
--   126_late / 126_content / 126_prepub1: the latest record HAS dates[] and DataCite.py's DATACITE_PY_AGREE_SQL (oxjob
--     #126, 16 Nov 2026) moves its stored date: a pick later than the record's own publicationYear, or a Collected/
--     Coverage/Valid pick (any year earlier), or a Submitted/Accepted/Created pick the year before, replaced by the
--     record's own Issued/Available/Copyrighted date (or 1 Jan of publicationYear). The CASE below is that expression
--     verbatim (notebooks/ingest/tests/test_datacite_py_agree.py checks they stay identical); it reads the stored date
--     as published_date and the record as attributes.
--
-- Reads locations_w_types (the parsed dates, before CreateLocationsMapped's override), never locations_mapped, so a
-- re-run rebuilds the same rows instead of finding the fixed dates and emptying the table. Records re-parsed under the
-- new rules drop out by themselves (their stored date changes, so fix_old_date no longer matches and the override stops
-- applying).
-- Revert: #126 only: DELETE FROM openalex.works.datacite_publication_year_fix WHERE rule LIKE '126%' (the next nightly
-- serves the old dates again). All: drop the fx join and CASE in CreateLocationsMapped first, then this table (the LEFT
-- JOIN fails on a missing table). Emptying the table alone also reverts.
--
-- Latest record per DOI = the one DataCite.py's apply_changes keeps (attributes.updated, then ingested_at). Never
-- max_by(..., ingested_at) alone: ~200M of the 240M datacite_items rows have NULL ingested_at (pre-streaming backfill)
-- and max_by skips NULL keys, which hid ~95M DOIs from the first build (9 Nov 2026: 2.0M -> 7.7M rows, +5.6M works,
-- 5.5M of them xpac Geoscience Australia samples).

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.datacite_publication_year_fix
COMMENT 'oxjob #2091 + #126: corrected dates for DataCite locations parsed before DataCite.py''s date rules (rule: 2091_nodates, 126_late, 126_content, 126_prepub1); applied by CreateLocationsMapped while the location still holds fix_old_date'
AS
WITH raw AS (
  SELECT id AS native_id,
         max_by(struct(attributes.publicationYear AS py, attributes.dates AS dates,
                       attributes.registered AS registered, attributes.created AS created),
                struct(try_cast(attributes.updated AS TIMESTAMP) AS u, coalesce(ingested_at, TIMESTAMP'1970-01-01') AS i)) AS r
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
),
withdates AS (
  SELECT lt.native_id_namespace, lt.native_id, lt.published_date,
         named_struct('publicationYear', try_cast(raw.r.py AS INT), 'dates', raw.r.dates) AS attributes
  FROM lt JOIN raw ON raw.native_id = lt.native_id
  WHERE size(raw.r.dates) > 0 AND lt.published_date IS NOT NULL
),
fix126 AS (
  SELECT native_id_namespace, native_id, attributes, attributes.publicationYear AS publication_year,
         published_date AS fix_old_date,
         -- DataCite.py DATACITE_PY_AGREE_SQL, verbatim
         CASE
           WHEN published_date IS NULL OR attributes.dates IS NULL OR size(attributes.dates) = 0
                OR NOT coalesce(attributes.publicationYear BETWEEN 1900 AND year(current_date()), false) THEN published_date
           WHEN year(published_date) > CAST(attributes.publicationYear AS INT) THEN
             coalesce(array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) = 'issued' AND length(trim(d.date)) >= 7), d -> try_cast(d.date AS DATE)), x -> x >= DATE'1900-01-01' AND x <= make_date(CAST(attributes.publicationYear AS INT), 12, 31))),
                      coalesce(array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) IN ('issued', 'available', 'copyrighted') AND length(trim(d.date)) >= 7), d -> try_cast(d.date AS DATE)), x -> year(x) = CAST(attributes.publicationYear AS INT))), array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) IN ('issued', 'available', 'copyrighted')), d -> try_cast(d.date AS DATE)), x -> year(x) = CAST(attributes.publicationYear AS INT)))),
                      array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) = 'issued'), d -> try_cast(d.date AS DATE)), x -> x >= DATE'1900-01-01' AND x <= make_date(CAST(attributes.publicationYear AS INT), 12, 31))),
                      make_date(CAST(attributes.publicationYear AS INT), 1, 1))
           WHEN year(published_date) < CAST(attributes.publicationYear AS INT)
                AND NOT exists(attributes.dates, d -> lower(d.dateType) IN ('issued', 'available', 'copyrighted') AND coalesce(try_cast(d.date AS DATE) = published_date, false))
                AND (exists(attributes.dates, d -> lower(d.dateType) IN ('collected', 'coverage', 'valid') AND coalesce(try_cast(d.date AS DATE) = published_date, false))
                     OR (year(published_date) = CAST(attributes.publicationYear AS INT) - 1 AND exists(attributes.dates, d -> lower(d.dateType) IN ('submitted', 'accepted', 'created') AND coalesce(try_cast(d.date AS DATE) = published_date, false))))
             THEN coalesce(coalesce(array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) IN ('issued', 'available', 'copyrighted') AND length(trim(d.date)) >= 7), d -> try_cast(d.date AS DATE)), x -> year(x) = CAST(attributes.publicationYear AS INT))), array_min(filter(transform(filter(attributes.dates, d -> lower(d.dateType) IN ('issued', 'available', 'copyrighted')), d -> try_cast(d.date AS DATE)), x -> year(x) = CAST(attributes.publicationYear AS INT)))), published_date)
           ELSE published_date
         END AS fix_new_date
  FROM withdates
)
SELECT lt.native_id_namespace, lt.native_id, n.py AS publication_year,
       lt.published_date AS fix_old_date, make_date(n.py, 1, 1) AS fix_new_date,
       '2091_nodates' AS rule, current_timestamp() AS built_at
FROM lt JOIN nodates n ON n.native_id = lt.native_id
WHERE n.py BETWEEN 1900 AND year(current_date())
  AND year(lt.published_date) > n.py
  AND lt.published_date = n.reg_date
UNION ALL
SELECT native_id_namespace, native_id, publication_year, fix_old_date, fix_new_date,
       CASE WHEN year(fix_old_date) > publication_year THEN '126_late'
            WHEN exists(attributes.dates, d -> lower(d.dateType) IN ('collected', 'coverage', 'valid')
                                                AND coalesce(try_cast(d.date AS DATE) = fix_old_date, false)) THEN '126_content'
            ELSE '126_prepub1'
       END AS rule, current_timestamp() AS built_at
FROM fix126
WHERE fix_new_date IS NOT NULL AND fix_new_date <> fix_old_date

-- COMMAND ----------

SELECT rule, count(*) AS rows, count(DISTINCT native_id_namespace, native_id) AS keys,
       count_if(native_id LIKE '10.25582/%') AS emsl, min(publication_year) AS min_py, max(publication_year) AS max_py
FROM openalex.works.datacite_publication_year_fix
GROUP BY rule
