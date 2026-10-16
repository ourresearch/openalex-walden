SELECT assert_true(funder_inputs_ok='PASS' AND ids_checked=30 AND compat_rows=41864 AND compat_hash='-568336946169178197326' AND basis_rows=43762 AND basis_hash='-528848368602275172432','post-switch identity/hash drift: STOP') FROM (
-- READ ONLY. s0b = s0 for AFTER the Funders switch-over (name fixes + no-DOI-forward), fold-ship Tue 10-13.
-- Names may change; identity may not. Compares only funder_id, merge_into_id, doi, ror_id, crossref_id of the ~30 pinned NIH ids against v41.
-- compat_hash / basis_hash are REPORTED, not gated: after the switch, pin the first post-switch values here and require them equal every morning.
--   post-switch compat_hash: ________   basis_hash: ________ (fill from the first s0b run after the switch is green)
WITH ids AS (
  SELECT DISTINCT CAST(get_json_object(evidence_json, '$.project_funder_id') AS BIGINT) funder_id FROM openalex_dev.rohan_lab.npy1007r_all_newest_manifest
    WHERE get_json_object(evidence_json, '$.project_funder_id') IS NOT NULL
  UNION SELECT funder_id FROM openalex_dev.rohan_lab.npy1007n_ic_map
  UNION SELECT explode(array(4320332161,4320337611,4320337356,4320337376,4320337472,4320337358,4320337593,4320337375,4320337355,4320337362,4320337363,4320337364,4320337365,4320337357,4320337361,4320337354,4320337346,4320337359,4320337353,4320337337,4320337330,4320337352,4320337347,4320337534,4320337372,4320337351,4320337374,4320337350,4320337338,4320337348))),
c AS (SELECT * FROM openalex.funders.funders c),
ref AS (SELECT * FROM openalex_dev.rohan_lab.funders_v41_ref),
m AS (SELECT
  (SELECT count(*) FROM ids) ids_checked,
  (SELECT count(*) FROM ids LEFT ANTI JOIN c USING(funder_id)) missing_in_compat,
  (SELECT count(*) FROM ids JOIN c USING(funder_id) WHERE c.merge_into_id IS NOT NULL) merged_in_compat,
  (SELECT count(*) FROM ids JOIN c USING(funder_id) JOIN ref r USING(funder_id) WHERE to_json(struct(c.funder_id,c.merge_into_id,c.doi,c.ror_id,c.crossref_id)) <> to_json(struct(r.funder_id,r.merge_into_id,r.doi,r.ror_id,r.crossref_id))) identity_differs_from_v41,
  (SELECT count(*) FROM c) compat_rows,
  (SELECT CAST(sum(CAST(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING) FROM c) compat_hash,
  (SELECT count(*) FROM openalex.awards.award_funders_basis) basis_rows,                                            -- 43,762
  (SELECT CAST(sum(CAST(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING) FROM openalex.awards.award_funders_basis) basis_hash)   -- -528848368602275172432
SELECT CASE WHEN missing_in_compat = 0 AND merged_in_compat = 0 AND identity_differs_from_v41 = 0
             THEN 'PASS' ELSE 'FAIL' END funder_inputs_ok, * FROM m
);
