-- oxjob #695: direct-drain queue for hosts that need NO paywall classification.
-- Membership is DATA-DRIVEN: any host whose measured free-rate cleared the 60% gate in
-- the live host-prior sweep (openalex.parseland.pdf_host_prior_verdicts, verdict='drain').
-- Those probes hit ACTUAL cohort URLs — aggregate historical win rates proved wrong on
-- 5 of the hosts we checked, so they are never used for membership.

CREATE OR REPLACE TABLE openalex.parseland.pdf_oa_host_queue
CLUSTER BY AUTO
COMMENT 'Direct-drain queue for OA hosts needing no classifier (oxjob #695)'
AS
WITH deduped AS (
  SELECT c.*,
    ROW_NUMBER() OVER (PARTITION BY c.work_key, c.work_key_ns ORDER BY c.file_key) AS rn
  FROM openalex.parseland.pdf_candidate_cohort c
  JOIN openalex.parseland.pdf_host_prior_verdicts v
    ON v.url_host = c.url_host AND v.verdict = 'drain'
)
SELECT
  work_key, work_key_ns, native_id, native_id_namespace,
  pdf_url, url_host, publisher,
  'oa_host_prior_v2' AS classifier_rule,
  ROW_NUMBER() OVER (PARTITION BY url_host ORDER BY XXHASH64(work_key)) AS host_rank,
  current_timestamp() AS queued_at
FROM deduped
WHERE rn = 1;
