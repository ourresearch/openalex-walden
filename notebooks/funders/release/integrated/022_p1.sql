-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 36
SELECT
  assert_true((SELECT count(*) FROM (SELECT ror_id FROM openalex.funders.funder WHERE status = 'active' AND ror_primary GROUP BY 1 HAVING count(*) > 1)) = 0,
    'ROR id primary on two active funders') c_ror_unique,
  assert_true((SELECT count(*) FROM openalex.funders.funder m LEFT JOIN openalex.funders.funder w ON w.funder_id = m.merged_into_id
    WHERE m.status = 'merged' AND (w.funder_id IS NULL OR w.status <> 'active')) = 0, 'merged funder not pointing at an active funder') c_merge_target,
  assert_true((SELECT count(*) FROM openalex.funders.funder_id i LEFT ANTI JOIN openalex.funders.funder f ON f.funder_id = i.funder_id) = 0, 'funder_id points at a missing funder') c_id_target,
  assert_true((SELECT count(*) FROM (SELECT id_type, id_value FROM openalex.funders.funder_id GROUP BY 1, 2 HAVING count(*) > 1)) = 0, 'identifier mapped twice') c_id_unique,
  assert_true((SELECT count(*) FROM (SELECT funder_id FROM openalex.funders.funder GROUP BY 1 HAVING count(*) > 1)) = 0, 'F id issued twice') c_f_unique,
  assert_true((SELECT count(*) FROM (SELECT funder_id FROM registry_prev_funder EXCEPT SELECT funder_id FROM openalex.funders.funder)) = 0,
    'an issued F id disappeared') c_never_deleted,
  assert_true((SELECT count(*) FROM openalex.funders.funder f LEFT ANTI JOIN openalex.funders.funder_id i ON i.id_type = 'ror' AND i.id_value = lower(regexp_extract(f.ror_id, '([0-9a-z]{9})/*$', 1)) AND i.funder_id = f.funder_id
    WHERE f.status = 'active' AND f.ror_primary) = 0, 'primary ror_id without its funder_id row') c_ror_indexed;
