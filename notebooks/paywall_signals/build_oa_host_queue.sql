-- oxjob #695: direct-drain queue for open-access hosts that need NO paywall classification.
-- These hosts have no paywall to detect, so we skip HTML ingest entirely and drain on the
-- host prior alone. Membership is gated on a measured historical PDF win rate (see README);
-- a host only belongs here if it clears ~60% live yield.

CREATE OR REPLACE TABLE openalex.parseland.pdf_oa_host_queue
CLUSTER BY AUTO
COMMENT 'Direct-drain queue for OA hosts needing no classifier (oxjob #695)'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY work_key, work_key_ns ORDER BY file_key) AS rn
  FROM openalex.parseland.pdf_candidate_cohort
  -- mdpi EXCLUDED 2026-08-02: 63% historical but 0/40 on cohort URLs (same URL shape as
  -- winners; our rows are older articles) — needs its own investigation before draining.
  WHERE url_host IN ('www.jstage.jst.go.jp', 'www.scielo.br')
)
SELECT
  work_key, work_key_ns, native_id, native_id_namespace,
  pdf_url, url_host, publisher,
  'oa_host_prior_v1' AS classifier_rule,
  ROW_NUMBER() OVER (PARTITION BY url_host ORDER BY XXHASH64(work_key)) AS host_rank,
  current_timestamp() AS queued_at
FROM deduped
WHERE rn = 1;
