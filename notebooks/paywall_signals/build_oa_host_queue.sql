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
  -- Membership decided by probing ACTUAL cohort URLs (40/host), not host-level history —
  -- history was wrong for 3 of 5. Measured 2026-08-02: jstage 95.7%, zenodo 100.0%.
  -- EXCLUDED: mdpi 0/40 (63% historical; cohort rows are older articles, same URL shape),
  -- figshare 0/40 (wrong host form entirely), scielo 35.3% (below the 60% gate; the
  -- earlier 100% read was 4 rows).
  WHERE url_host IN ('www.jstage.jst.go.jp', 'zenodo.org')
)
SELECT
  work_key, work_key_ns, native_id, native_id_namespace,
  pdf_url, url_host, publisher,
  'oa_host_prior_v1' AS classifier_rule,
  ROW_NUMBER() OVER (PARTITION BY url_host ORDER BY XXHASH64(work_key)) AS host_rank,
  current_timestamp() AS queued_at
FROM deduped
WHERE rn = 1;
