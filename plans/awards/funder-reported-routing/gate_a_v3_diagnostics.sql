-- Gate A v3 companion diagnostics — measures the remaining Codex objections that the
-- gate itself can't fold in. All should be 0 (or explained) before trusting the PASS.
SELECT
  -- Objection: at_risk parse failures. work_awards rows whose award.funder_id is set
  -- but doesn't end in digits parse to NULL and silently fall out of at_risk.
  (SELECT COUNT(*) FROM openalex.awards.work_awards
    WHERE award.funder_id IS NOT NULL
      AND REGEXP_EXTRACT(award.funder_id, '([0-9]+)$', 1) = '') AS work_awards_funder_parse_failures,
  -- Objection: non-canonical funder id strings (leading zeros / wrong host) that would
  -- collapse distinct string identities into one numeric id.
  (SELECT COUNT(*) FROM openalex.awards.work_awards
    WHERE award.funder_id IS NOT NULL
      AND NOT award.funder_id RLIKE '^https://openalex\\.org/F[1-9][0-9]*$') AS work_awards_noncanonical_funder_ids,
  (SELECT COUNT(*) FROM openalex.works.fulltext_work_funders
    WHERE funder_id IS NOT NULL
      AND NOT funder_id RLIKE '^https://openalex\\.org/F[1-9][0-9]*$') AS fulltext_noncanonical_funder_ids,
  -- Objection: orphan funder ids (present in source, absent from mid.funder) — these
  -- pairs never publish, so counting them as coverage is what v2 got wrong. Measured
  -- per leg; nonzero here is exactly the population v3 stops crediting.
  (SELECT COUNT(*) FROM (SELECT DISTINCT c.funder_id
     FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate c
     LEFT ANTI JOIN openalex.mid.funder f ON f.funder_id = c.funder_id)) AS candidate_orphan_funder_ids,
  (SELECT COUNT(*) FROM (SELECT DISTINCT cwf.funder_id
     FROM openalex.awards.crossref_work_funders cwf
     LEFT ANTI JOIN openalex.mid.funder f ON f.funder_id = cwf.funder_id)) AS crossref_orphan_funder_ids;
