-- Map_Work_Ids 2/3, the location matcher (oxjob #1668): tonight's frame = the first-sight records match_locations_by_identifier
-- left without a work (__VERDICTS__), minus, when the title rule has no pick for them, two kinds that bind or mint without it, as
-- when the shadow measured the model: a record declaring a version / preprint link to a record minting or bound tonight (it binds
-- to that work in assign_work_ids' second pass), and a record whose only identifiers are a new PMID or arXiv id (it mints on them).
-- `decision` / `work_id` say what the title rule would have done (title_bind = its pick, the matcher's `bound` input).
-- oxjob #2065: a minted data-series record (provenance `openalex-series`, written by CreateLocationsWithTypes) is never given to
-- the matcher: no source names that work, so it mints on its own synthetic key in assign_work_ids and can match nothing.
CREATE OR REPLACE TABLE openalex.works.location_work_match_frame AS
WITH v AS (SELECT * FROM __VERDICTS__),
f AS (
  SELECT s.* FROM v s
  WHERE s.first_sight AND s.work_id IS NULL AND s.provenance <> 'openalex-series'
    AND (s.title_pick IS NOT NULL OR (
    NOT (s.doi IS NULL AND (s.pmid IS NOT NULL OR s.arxiv IS NOT NULL))
    AND NOT EXISTS (
      SELECT 1 FROM v x
      WHERE s.has_version_doi_clean IS NOT NULL AND x.doi_clean = s.has_version_doi_clean
        AND x.has_version_doi_clean IS NULL
        AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))
    AND NOT EXISTS (
      SELECT 1 FROM v x
      WHERE s.version_of_doi_clean IS NOT NULL AND s.has_version_doi_clean IS NULL AND x.doi_clean = s.version_of_doi_clean
        AND x.has_version_doi_clean IS NULL AND x.version_of_doi_clean IS NULL
        AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))
    AND NOT EXISTS (
      SELECT 1 FROM v x
      WHERE s.preprint_link_is_pre AND SIZE(s.preprint_link_dois) = 1 AND x.doi_clean = s.preprint_link_dois[0]
        AND NOT x.preprint_link_is_pre
        AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))))
)
SELECT f.provenance, f.native_id_namespace, f.native_id, f.title_pick AS work_id,
  CASE WHEN f.title_pick IS NOT NULL THEN 'title_author' END AS work_id_source,
  DATE'__NIGHT__' AS night,
  CASE WHEN f.title_pick IS NOT NULL THEN 'title_bind'
       WHEN NOT f.mintable THEN 'park'
       WHEN f.doi IS NOT NULL THEN 'mint_doi'
       ELSE 'mint_nodoi' END AS decision,
  CASE WHEN f.provenance IN ('crossref', 'datacite', 'repo', 'pubmed') THEN f.provenance ELSE 'other' END AS prov,
  xxhash64('oxjob1668', f.provenance, f.native_id_namespace, f.native_id) AS h
FROM f
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.provenance, f.native_id_namespace, f.native_id ORDER BY f.title_pick) = 1
;
SELECT prov, decision, count(*) n FROM openalex.works.location_work_match_frame GROUP BY 1, 2 ORDER BY 1, 3 DESC
