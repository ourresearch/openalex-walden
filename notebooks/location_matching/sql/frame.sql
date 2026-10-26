-- Match_Locations_To_Works (oxjob #1668): the night's frame = anchors first pinned by this run's MapWorkIds
-- (location_work_ids __NEW__, after the night's MERGE, minus __OLD__, after the previous MERGE) that the nightly title-bound,
-- minted or parked, with the nightly's decision. PID binds and the version / preprint tiers are out: they never reach the model.
-- Candidate works = openalex_works __V__ (the last version before the run).
CREATE OR REPLACE TABLE openalex.works.location_work_match_frame AS
WITH old AS (SELECT provenance, native_id_namespace, native_id, work_id FROM openalex.works.location_work_ids VERSION AS OF __OLD__),
oldw AS (SELECT DISTINCT work_id FROM old WHERE work_id IS NOT NULL),
nw AS (
  SELECT n.* FROM openalex.works.location_work_ids VERSION AS OF __NEW__ n
  LEFT ANTI JOIN old o USING (provenance, native_id_namespace, native_id)
),
d AS (
  SELECT n.provenance, n.native_id_namespace, n.native_id, n.work_id, n.work_id_source, n.openalex_created_dt AS night,
    CASE WHEN n.work_id IS NULL THEN 'park'
         WHEN w.work_id IS NOT NULL AND n.work_id_source = 'title_author' THEN 'title_bind'
         WHEN w.work_id IS NOT NULL THEN 'pid_bind'
         WHEN n.work_id_source IN ('version_of', 'has_version', 'crossref_preprint', 'arxiv', 'pmid') THEN 'pid_mint'
         WHEN n.work_id_source = 'doi' THEN 'mint_doi'
         ELSE 'mint_nodoi' END AS decision
  FROM nw n LEFT JOIN oldw w ON w.work_id = n.work_id
)
SELECT d.*,
  CASE WHEN d.provenance IN ('crossref', 'datacite', 'repo', 'pubmed') THEN d.provenance ELSE 'other' END AS prov,
  xxhash64('oxjob1668', d.provenance, d.native_id_namespace, d.native_id) AS h
FROM d
WHERE d.decision IN ('title_bind', 'mint_doi', 'mint_nodoi', 'park')
;
SELECT prov, decision, count(*) n FROM openalex.works.location_work_match_frame GROUP BY 1, 2 ORDER BY 1, 3 DESC
