SELECT assert_true(current_timestamp() >= to_timestamp(concat(:switch_day, ' 08:00:00 America/Los_Angeles')) AND current_timestamp() < to_timestamp(concat(:switch_day, ' 18:30:00 America/Los_Angeles')), '058 abort deadline reached: STOP and recover');
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Registry then identifiers then compat then signed lookup; keep readers paused
-- Readers remain on main until the last publication statement. Operator keeps
-- their schedules paused during activation and restores them for the next night.
SELECT assert_true((SELECT count(*) FROM (
 (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.funders EXCEPT ALL SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_before)
 UNION ALL (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_before EXCEPT ALL SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.funders)
))=0,'catalogue changed since stage: restage and reapprove');
-- No-merge switch: registry, identifiers and compat must remain exact.
SELECT assert_true((SELECT count(*) FROM (
 (SELECT * FROM openalex.funders.funder EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_registry_before)
 UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_registry_before EXCEPT ALL SELECT * FROM openalex.funders.funder)))=0,'registry changed since stage');
SELECT assert_true((SELECT count(*) FROM (
 (SELECT * FROM openalex.funders.funder_id EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_identifiers_before)
 UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_identifiers_before EXCEPT ALL SELECT * FROM openalex.funders.funder_id)))=0,'identifier ownership changed since stage');
SELECT assert_true(count(*)=0 AND count_if(l.status<>'active' OR l.merged_into_id IS NOT NULL OR w.status<>'active' OR l.funder_id IS NULL OR w.funder_id IS NULL)=0,'registry merge preconditions')
FROM openalex.funders.nm1013_integrated_merges m
LEFT JOIN openalex.funders.funder l ON l.funder_id=m.loser_id
LEFT JOIN openalex.funders.funder w ON w.funder_id=m.survivor_id;
SELECT assert_true((SELECT count(*) FROM (
 (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.funders EXCEPT ALL SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_after)
 UNION ALL (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_after EXCEPT ALL SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.funders)
))=0,'catalogue publication mismatch: keep readers paused');
SELECT assert_true(count(*)=0,'funder activation mismatch: keep readers paused') FROM ((SELECT * FROM openalex.funders.funder EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_registry_after) UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_registry_after EXCEPT ALL SELECT * FROM openalex.funders.funder));
SELECT assert_true(count(*)=0,'funder_id activation mismatch: keep readers paused') FROM ((SELECT * FROM openalex.funders.funder_id EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_identifiers_after) UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_identifiers_after EXCEPT ALL SELECT * FROM openalex.funders.funder_id));
CREATE OR REPLACE TABLE openalex.funders.funder_name_lookup AS SELECT * FROM openalex.funders.nm1013_integrated_lookup;
