-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Freeze current main Crossref output before redirects
CREATE TABLE openalex.awards.crossref_no_doi_main_cut
USING delta
AS
WITH exploded AS (
  SELECT
    lm.work_id,
    f.doi AS funder_doi,
    f.name AS funder_name,
    f.awards AS award_ids
  FROM openalex.works.locations_mapped lm
  LATERAL VIEW EXPLODE(funders) AS f
  WHERE lm.provenance = 'crossref'
    AND lm.work_id IS NOT NULL
    AND f.doi IS NOT NULL
),
matched AS (
  SELECT
    e.work_id,
    -- resolve merged funders (openalex.funders.funders.merge_into_id alias rows) to the canonical id
    COALESCE(cf.merge_into_id, cf.funder_id) AS funder_id,
    e.award_ids
  FROM exploded e
  JOIN openalex.funders.funders cf ON e.funder_doi = cf.doi
  UNION ALL
  -- #1303 resolver (2026-10), LINKS ONLY: Crossref funder names with no funder id, resolved by Jev (p >= 0.9) and
  -- checked by two judges on 200 links (gate 2: 0.98-0.995). Frozen, reviewed table; recent no-funder works only.
  -- award_ids is empty on purpose: these rows add work -> funder links and never create grant records.
  SELECT DISTINCT
    l.work_id,
    COALESCE(cf.merge_into_id, cf.funder_id) AS funder_id,
    CAST(array() AS ARRAY<STRING>) AS award_ids
  FROM openalex.awards.cr1303_work_funder_links_frozen l
  JOIN openalex.funders.funders cf ON cf.funder_id = l.funder_id
),
-- Explode each award_id so we can apply is_usable_award_id per-element.
-- Databricks rejects UDF calls inside higher-order functions (FILTER,
-- TRANSFORM, etc.), so we can't do FILTER(..., x -> is_usable_award_id(x)).
-- OUTER EXPLODE preserves rows where award_ids is NULL or empty, so the
-- work-funder link is kept even when no specific awards survive.
flattened AS (
  SELECT
    work_id,
    funder_id,
    CASE WHEN openalex.common.is_usable_award_id(aid) THEN aid END AS aid
  FROM matched
  LATERAL VIEW OUTER EXPLODE(award_ids) AS aid
)
-- Re-aggregate one row per (work_id, funder_id). COLLECT_LIST ignores
-- NULLs, so the CASE WHEN drops junk elements naturally. ARRAY_DISTINCT
-- preserves the original dedup semantics.
SELECT
  work_id,
  funder_id,
  ARRAY_DISTINCT(COLLECT_LIST(aid)) AS award_ids
FROM flattened
GROUP BY work_id, funder_id;
