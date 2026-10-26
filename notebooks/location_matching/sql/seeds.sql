-- Match_Locations_To_Works (oxjob #1668), step 2 of the inputs: seeds, their blocking features, the SQL-channel candidates
-- (#1577 keys: ids, title key, MinHash bands, author + year; the title tier's own pick as `bound`) and the qwen3 seed vectors.
-- Seeds = every record of the night's frame (no sampling, weight 1). Candidates = works in openalex_works __V__ (pre-run),
-- through location_work_match_keys. The SQL of the 7-night shadow replay (oxjobs location-work-match-model/work/shadow).
CREATE OR REPLACE TABLE openalex.works.location_work_match_seeds AS
SELECT f.provenance, f.native_id_namespace, f.native_id, f.work_id, f.work_id_source, f.night, f.decision, f.prov, f.h AS seed_id,
  CAST(1 AS DOUBLE) AS weight
FROM openalex.works.location_work_match_frame f
;
SELECT prov, decision, count(*) AS seeds FROM openalex.works.location_work_match_seeds GROUP BY 1, 2 ORDER BY 1, 2
