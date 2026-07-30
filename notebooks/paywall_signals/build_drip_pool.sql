-- oxjob #695: slow-drip pool — marker-validated likely-free candidates on rate-blocked
-- publishers (Wiley/T&F/OUP + Hindawi-on-Wiley). Randomized stable order via rand_key;
-- the daily drip walks it with an anti-join on winning attempts, so blocked fetches retry
-- naturally on later passes. Budgets keyed by budget_host, not url_host (Hindawi counts
-- against Wiley's budget — same origin).

CREATE OR REPLACE TABLE openalex.parseland.pdf_drip_pool
CLUSTER BY AUTO
COMMENT 'Randomized slow-drip queue for rate-blocked publishers (oxjob #695)'
AS
SELECT
  CASE WHEN h.native_id LIKE 'https://doi.org/%' THEN SUBSTRING(h.native_id, 17) ELSE h.native_id END AS native_id,
  CASE WHEN h.native_id LIKE 'https://doi.org/%' THEN 'doi' ELSE h.native_id_namespace END AS native_id_namespace,
  CASE
    WHEN h.url_host = 'downloads.hindawi.com'
      THEN CONCAT('https://onlinelibrary.wiley.com/doi/pdfdirect/',
                  CASE WHEN h.native_id LIKE 'https://doi.org/%' THEN SUBSTRING(h.native_id, 17) ELSE h.native_id END)
    ELSE h.pdf_url
  END AS pdf_url,
  CASE
    WHEN h.url_host IN ('onlinelibrary.wiley.com', 'downloads.hindawi.com') THEN 'wiley'
    WHEN h.url_host = 'www.tandfonline.com' THEN 'tandfonline'
    WHEN h.url_host = 'academic.oup.com' THEN 'oup'
  END AS budget_host,
  CASE
    WHEN h.url_host = 'onlinelibrary.wiley.com' THEN 'wiley_doi_access_badge_v1'
    WHEN h.url_host = 'downloads.hindawi.com' THEN 'hindawi_wiley_rewrite_v1'
    WHEN h.url_host = 'www.tandfonline.com' THEN 'tf_free_access_badge_v1'
    WHEN h.url_host = 'academic.oup.com' THEN 'oup_oa_phrase_v1'
  END AS drip_rule,
  XXHASH64(h.work_key, 695) AS rand_key,
  current_timestamp() AS pooled_at
FROM openalex.landing_page.pdf_candidate_html h
JOIN openalex.parseland.pdf_candidate_classification c ON h.file_key = c.file_key
WHERE c.class = 'needs_validation' AND h.status = 'ok'
  AND (
    (h.url_host = 'onlinelibrary.wiley.com'
      AND h.html RLIKE '(?i)doi-access[^>]*>\\s*(Open|Free) Access')
    OR (h.url_host = 'downloads.hindawi.com'
      AND (h.native_id LIKE 'https://doi.org/10.1155/%' OR h.native_id LIKE '10.1155/%'))
    OR (h.url_host = 'www.tandfonline.com'
      AND h.html RLIKE '(?i)>\\s*Free Access\\s*<')
    OR (h.url_host = 'academic.oup.com'
      AND h.html RLIKE '(?i)Open Access article distributed under the terms')
  );
