-- DEV ONLY. Writes only to openalex_dev.rohan_lab. v2: adds the same NULL-key filter
-- the hardened production MERGE now carries, so the candidate models the post-patch
-- insert set exactly.
CREATE OR REPLACE TABLE openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate
USING DELTA
AS
WITH additive_rows AS (
  SELECT work_id, funder_id, provenance
  FROM openalex.awards.funder_reported_work_funders

  UNION

  SELECT work_id, funder_id, 'europepmc_work_funders' AS provenance
  FROM openalex.awards.europepmc_work_funders

  UNION

  SELECT work_id, funder_id, 'datacite_work_funders' AS provenance
  FROM openalex.awards.datacite_work_funders

  UNION

  SELECT work_id, funder_id, 'kaken_work_funders' AS provenance
  FROM openalex.awards.kaken_work_funders

  UNION

  SELECT work_id, funder_id, 'anr_work_funders' AS provenance
  FROM openalex.awards.anr_work_funders
)
SELECT work_id, funder_id, provenance
FROM additive_rows
WHERE work_id IS NOT NULL
  AND funder_id IS NOT NULL
  AND provenance IS NOT NULL;
