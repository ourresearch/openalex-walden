-- oxjob #695: ranked drain queue from classifier likely_free slice (Test 4 deliverable).
-- Deduped by work; host_rank supports per-host capped drains via WHERE host_rank <= cap.

CREATE OR REPLACE TABLE openalex.parseland.pdf_candidate_drain_queue
CLUSTER BY AUTO
COMMENT 'Ranked likely_free PDF drain queue for oxjob #695'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY work_key, work_key_ns
      ORDER BY calibrated_pdf_yield DESC, file_key
    ) AS rn
  FROM openalex.parseland.pdf_candidate_classification
  WHERE class = 'likely_free'
)
SELECT
  work_key, work_key_ns, native_id, native_id_namespace,
  CASE WHEN url_host = 'linkinghub.elsevier.com'
         AND REGEXP_EXTRACT(pdf_url, 'pii/([A-Za-z0-9]+)', 1) != ''
       THEN CONCAT('https://www.sciencedirect.com/science/article/pii/',
                   REGEXP_EXTRACT(pdf_url, 'pii/([A-Za-z0-9]+)', 1), '/pdf')
       ELSE pdf_url
  END AS pdf_url,
  url_host, publisher,
  classifier_rule, calibrated_pdf_yield, classifier_version,
  ROW_NUMBER() OVER (
    PARTITION BY url_host
    ORDER BY calibrated_pdf_yield DESC, XXHASH64(work_key)
  ) AS host_rank,
  current_timestamp() AS queued_at
FROM deduped
WHERE rn = 1;
