-- Gate A v3: label-faithful coverage model. Supersedes v2 after the 2026-08-14 Codex
-- adversarial pass REFUTED v2's fidelity: v2 accepted coverage from raw source pairs,
-- but CreateWorksEnriched only PUBLISHES funder_reported and crossref pairs whose
-- funder_id resolves in openalex.mid.funder (inner joins), and its fulltext leg is
-- keep-list gated. v3 reproduces the exact label-producing legs:
--   backfill (mid.work_funder, no dim join — matches notebook)
--   fulltext JOIN funder_names_keep (matches notebook)
--   crossref JOIN mid.funder (matches notebook)
--   funder_reported(post-merge = current ∪ candidate) JOIN mid.funder (matches notebook)
-- PASS = zero at-risk pairs uncovered under this faithful model.
-- Companion diagnostics (parse failures, orphan funder ids) in gate_a_v3_diagnostics.sql.
-- Operational requirement unchanged: run in the SAME attended session as the prod MERGE
-- and next scoring run, so gate snapshot == consumed snapshot.

WITH sup AS (
  SELECT funder_id, funder_award_id
  FROM openalex.awards.award_id_guard
  WHERE decision = 'suppress'
     OR reason = 'label_entangled_held'
),
at_risk AS (
  SELECT work_id, funder_id
  FROM (
    SELECT
      wa.work_id,
      CAST(REGEXP_EXTRACT(wa.award.funder_id, '([0-9]+)$', 1) AS BIGINT) AS funder_id,
      MAX(CASE WHEN s.funder_award_id IS NULL THEN 1 ELSE 0 END) AS has_kept
    FROM openalex.awards.work_awards wa
    LEFT JOIN sup s
      ON CAST(REGEXP_EXTRACT(wa.award.funder_id, '([0-9]+)$', 1) AS BIGINT) = s.funder_id
     AND wa.award.funder_award_id = s.funder_award_id
    GROUP BY 1, 2
  )
  WHERE has_kept = 0
),
post_patch_funder_reported AS (
  SELECT fr.work_id, fr.funder_id
  FROM openalex.awards.funder_reported_work_funders fr
  JOIN openalex.mid.funder f ON f.funder_id = fr.funder_id
  UNION
  SELECT c.work_id, c.funder_id
  FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate c
  JOIN openalex.mid.funder f ON f.funder_id = c.funder_id
),
covered_faithful AS (
  SELECT cwf.work_id, cwf.funder_id
  FROM openalex.awards.crossref_work_funders cwf
  JOIN openalex.mid.funder f ON f.funder_id = cwf.funder_id
  UNION
  SELECT work_id, funder_id FROM post_patch_funder_reported
  UNION
  SELECT paper_id AS work_id, funder_id FROM openalex.mid.work_funder
  UNION
  SELECT DISTINCT
    ft.work_id,
    CAST(REGEXP_EXTRACT(ft.funder_id, '([0-9]+)$', 1) AS BIGINT) AS funder_id
  FROM openalex.works.fulltext_work_funders ft
  JOIN openalex.common.funder_names_keep keep ON keep.name = ft.funder_name
),
uncovered AS (
  SELECT a.work_id, a.funder_id
  FROM at_risk a
  LEFT ANTI JOIN covered_faithful c
    ON a.work_id = c.work_id AND a.funder_id = c.funder_id
)
SELECT
  (SELECT COUNT(*) FROM at_risk)   AS at_risk_pairs,
  (SELECT COUNT(*) FROM uncovered) AS uncovered_label_faithful,
  CASE WHEN (SELECT COUNT(*) FROM uncovered) = 0 THEN 'PASS' ELSE 'FAIL' END AS gate_a_v3;
