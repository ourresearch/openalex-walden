-- Gate B: read-only additive-only check.
--
-- Prerequisite: the DEV-only staging CTAS in notebook_patch.md has been run by
-- an authorized owner. This file performs no writes.
--
-- PASS means every pair assembled by the six current CreateWorksEnriched legs
-- remains present when only the funder_reported input is replaced by the DEV
-- candidate. Provenance duplicates do not affect the pair set.

WITH unchanged_leg_pairs AS (
  -- Leg 1: MAG/backfill.
  SELECT paper_id AS work_id, funder_id
  FROM openalex.mid.work_funder

  UNION

  -- Leg 2: fulltext extraction, including CreateWorksEnriched's keep-list gate.
  SELECT
    ft.work_id,
    CAST(REGEXP_EXTRACT(ft.funder_id, '([0-9]+)$', 1) AS BIGINT) AS funder_id
  FROM openalex.works.fulltext_work_funders ft
  JOIN openalex.common.funder_names_keep keep
    ON keep.name = ft.funder_name

  UNION

  -- Leg 3: GTR.
  SELECT
    work_id,
    CAST(REGEXP_EXTRACT(funder.id, '([0-9]+)$', 1) AS BIGINT) AS funder_id
  FROM openalex.awards.gtr_awards
  WHERE work_id IS NOT NULL

  UNION

  -- Leg 4: Crossref, including CreateWorksEnriched's known-funder join.
  SELECT cwf.work_id, cwf.funder_id
  FROM openalex.awards.crossref_work_funders cwf
  JOIN openalex.mid.funder f
    ON f.funder_id = cwf.funder_id

  UNION

  -- Leg 6: award-derived funders.
  SELECT
    wa.work_id,
    CAST(REGEXP_EXTRACT(wa.award.funder_id, '([0-9]+)$', 1) AS BIGINT) AS funder_id
  FROM openalex.awards.work_awards wa
  WHERE wa.award.funder_id IS NOT NULL
),
old_enriched_pairs AS (
  SELECT work_id, funder_id FROM unchanged_leg_pairs

  UNION

  -- Current leg 5.
  SELECT fr.work_id, fr.funder_id
  FROM openalex.awards.funder_reported_work_funders fr
  JOIN openalex.mid.funder f
    ON f.funder_id = fr.funder_id
),
candidate_enriched_pairs AS (
  SELECT work_id, funder_id FROM unchanged_leg_pairs

  UNION

  -- Proposed leg 5.
  SELECT fr.work_id, fr.funder_id
  FROM openalex_dev.rohan_lab.funder_reported_work_funders_enrichment_candidate fr
  JOIN openalex.mid.funder f
    ON f.funder_id = fr.funder_id
),
old_missing AS (
  SELECT o.work_id, o.funder_id
  FROM old_enriched_pairs o
  LEFT ANTI JOIN candidate_enriched_pairs n
    ON o.work_id = n.work_id
   AND o.funder_id = n.funder_id
),
net_new AS (
  SELECT n.work_id, n.funder_id
  FROM candidate_enriched_pairs n
  LEFT ANTI JOIN old_enriched_pairs o
    ON n.work_id = o.work_id
   AND n.funder_id = o.funder_id
)
SELECT
  (SELECT COUNT(*) FROM old_enriched_pairs) AS old_distinct_pairs,
  (SELECT COUNT(*) FROM candidate_enriched_pairs) AS candidate_distinct_pairs,
  (SELECT COUNT(*) FROM old_missing) AS old_pairs_missing_from_candidate,
  (SELECT COUNT(*) FROM net_new) AS candidate_net_new_pairs,
  CASE
    WHEN (SELECT COUNT(*) FROM old_missing) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS gate_b;

-- Read-only inline rehearsal of the full six-leg sets on 2026-08-10:
-- old_distinct_pairs                 = 64,856,946
-- candidate_distinct_pairs           = 65,344,482
-- old_pairs_missing_from_candidate   = 0
-- candidate_net_new_pairs            = 487,536
-- gate_b                             = PASS
