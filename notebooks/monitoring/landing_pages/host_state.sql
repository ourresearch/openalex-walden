-- Works side of the scoreboard, one snapshot per run (oxjob #1789): for every Crossref work, how fresh its landing
-- page is and what the landing page gives it that Crossref does not, rolled up per host. Rewrites snapshot {start}.
-- One statement, no trailing semicolon. Heavy-ish (full Crossref x Taxicab x landing_page_works join): run weekly.
--
-- Freshness = age of the newest stored HTML fetch for the work's DOI in taxicab_results (new era, since Apr 2025);
--   older_era = only a Feb-2025 import copy (landing_page.dynamo_landing_page_s3_export); fetched_nothing_stored =
--   Taxicab tried, never stored HTML; never = no row anywhere.
-- fresh_useful = newest stored HTML is < 6 months old AND Parseland found authors or an abstract on it.
-- lp_* = the landing page supplies a field Crossref lacks for that DOI (authors, affiliations, corresponding author,
--   abstract) or a licence (landing page outranks every other licence source). Taken from landing_page_works, the
--   rows Walden actually merges.
-- host = landing_page_host of the newest fetch for the DOI, else the DOI prefix's host.
INSERT INTO openalex.monitoring.landing_page_host_state
REPLACE WHERE snapshot_date = DATE'{start}'
WITH cw AS (
  SELECT native_id doi, max(authors_exist) cr_authors, max(affiliations_exist) cr_affs,
         max(length(abstract) > 100) cr_abstract
  FROM openalex.crossref.crossref_works
  WHERE native_id IS NOT NULL
  GROUP BY native_id
),
tc AS (
  SELECT native_id doi,
    max_by(named_struct('url', url, 'resolved_url', resolved_url), processed_date) newest,
    max(CASE WHEN status_code = 200 AND NOT coalesce(is_soft_block, false) AND content_type = 'html' THEN processed_date END) last_html,
    max_by(CASE WHEN status_code = 200 AND NOT coalesce(is_soft_block, false) AND content_type = 'html' THEN taxicab_id END,
           CASE WHEN status_code = 200 AND NOT coalesce(is_soft_block, false) AND content_type = 'html' THEN processed_date END) last_html_id
  FROM openalex.taxicab.taxicab_results
  WHERE native_id_namespace = 'doi' AND processed_date < TIMESTAMP'{start}' + INTERVAL 1 DAY
  GROUP BY native_id
),
recent_parse AS (
  SELECT taxicab_id, max(CASE WHEN size(authors) > 0 OR length(abstract) > 100 THEN 1 ELSE 0 END) useful
  FROM openalex.parseland.parsed_pages
  WHERE parsed_date >= TIMESTAMP'{start}' - INTERVAL 190 DAYS
  GROUP BY taxicab_id
),
dyn AS (
  SELECT DISTINCT native_id doi FROM openalex.landing_page.dynamo_landing_page_s3_export WHERE native_id_namespace = 'doi'
),
lp AS (
  SELECT best_doi doi, max(authors_exist) lp_authors, max(affiliations_exist) lp_affs,
         max(is_corresponding_exists) lp_corr, max(length(abstract) > 100) lp_abstract, max(license IS NOT NULL) lp_license
  FROM openalex.landing_page.landing_page_works
  WHERE best_doi IS NOT NULL
  GROUP BY best_doi
),
w AS (
  SELECT cw.doi,
    coalesce(openalex.monitoring.landing_page_host(tc.newest.url, tc.newest.resolved_url, 'doi', cw.doi),
             openalex.monitoring.landing_page_doi_prefix_host('doi', cw.doi, NULL)) host,
    CASE
      WHEN tc.last_html >= TIMESTAMP'{start}' - INTERVAL 3 MONTHS THEN 'lt_3m'
      WHEN tc.last_html >= TIMESTAMP'{start}' - INTERVAL 6 MONTHS THEN '3_6m'
      WHEN tc.last_html >= TIMESTAMP'{start}' - INTERVAL 12 MONTHS THEN '6_12m'
      WHEN tc.last_html IS NOT NULL THEN 'gt_12m'
      WHEN dyn.doi IS NOT NULL THEN 'older_era'
      WHEN tc.doi IS NOT NULL THEN 'fetched_nothing_stored'
      ELSE 'never'
    END age,
    tc.last_html >= TIMESTAMP'{start}' - INTERVAL 6 MONTHS AND coalesce(rp.useful, 0) = 1 fresh_useful,
    coalesce(lp.lp_authors AND NOT coalesce(cw.cr_authors, false), false) win_authors,
    coalesce(lp.lp_affs AND NOT coalesce(cw.cr_affs, false), false) win_affiliations,
    coalesce(lp.lp_corr, false) win_corresponding,
    coalesce(lp.lp_abstract AND NOT coalesce(cw.cr_abstract, false), false) win_abstract,
    coalesce(lp.lp_license, false) win_license
  FROM cw
  LEFT JOIN tc ON tc.doi = cw.doi
  LEFT JOIN recent_parse rp ON rp.taxicab_id = tc.last_html_id
  LEFT JOIN dyn ON dyn.doi = cw.doi
  LEFT JOIN lp ON lp.doi = cw.doi
)
SELECT DATE'{start}' snapshot_date, host,
  count(*) works,
  count_if(age = 'lt_3m') age_lt_3m, count_if(age = '3_6m') age_3_6m, count_if(age = '6_12m') age_6_12m,
  count_if(age = 'gt_12m') age_gt_12m, count_if(age = 'older_era') age_older_era,
  count_if(age = 'fetched_nothing_stored') age_fetched_nothing_stored, count_if(age = 'never') age_never,
  count_if(fresh_useful) fresh_useful,
  count_if(win_authors) win_authors, count_if(win_affiliations) win_affiliations,
  count_if(win_corresponding) win_corresponding, count_if(win_abstract) win_abstract, count_if(win_license) win_license,
  current_timestamp() computed_at
FROM w
GROUP BY host
