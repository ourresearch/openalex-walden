-- Precheck v2 (read-only, run after dev_ctas_v2.sql): NULL-key and duplicate checks
-- over both the candidate and the currently served table. All four *_null_keys and
-- dup_triples columns must be 0; source_null_keys_dropped is informational (how many
-- raw source rows the NULL filter excluded).
SELECT
  (SELECT COUNT(*) FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate
    WHERE work_id IS NULL OR funder_id IS NULL OR provenance IS NULL) AS candidate_null_keys,
  (SELECT COUNT(*) FROM openalex.awards.funder_reported_work_funders
    WHERE work_id IS NULL OR funder_id IS NULL OR provenance IS NULL) AS served_null_keys,
  (SELECT COUNT(*) FROM (
     SELECT work_id, funder_id, provenance
     FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate
     GROUP BY 1,2,3 HAVING COUNT(*) > 1)) AS candidate_dup_triples,
  (SELECT COUNT(*) FROM (
     SELECT work_id, funder_id, provenance
     FROM openalex.awards.funder_reported_work_funders
     GROUP BY 1,2,3 HAVING COUNT(*) > 1)) AS served_dup_triples,
  (SELECT COUNT(*) FROM (
     SELECT work_id, funder_id FROM openalex.awards.hakai_work_funders
     UNION ALL SELECT work_id, funder_id FROM openalex.awards.europepmc_work_funders
     UNION ALL SELECT work_id, funder_id FROM openalex.awards.datacite_work_funders
     UNION ALL SELECT work_id, funder_id FROM openalex.awards.kaken_work_funders
     UNION ALL SELECT work_id, funder_id FROM openalex.awards.anr_work_funders)
    WHERE work_id IS NULL OR funder_id IS NULL) AS source_null_keys_dropped,
  (SELECT COUNT(*) FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate) AS candidate_rows;
