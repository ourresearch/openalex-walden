-- Landing-page funnel for fetch days [{start}, {end}) (oxjob #1789). Rewrites exactly those days, atomically.
-- Run by notebooks/monitoring/LandingPageScoreboard.py (nightly) or scripts/landing_page_scoreboard.py (backfill).
-- One statement, no trailing semicolon: the notebook runs the file as is.
INSERT INTO openalex.monitoring.landing_page_funnel
REPLACE WHERE fetch_date >= DATE'{start}' AND fetch_date < DATE'{end}'
WITH win AS (
  SELECT taxicab_id, url, processed_date, date(processed_date) fetch_date,
         coalesce(native_id_namespace, 'none') id_namespace,
         coalesce(status_code, -1) sc,          -- -1: no status recorded; counted in fail_other
         coalesce(is_soft_block, false) sb,
         coalesce(status_code, -1) = 200 AND NOT coalesce(is_soft_block, false) stored,
         content_type ct,
         openalex.monitoring.landing_page_host(url, resolved_url, native_id_namespace, native_id) host,
         -- Zyte bills the URL we asked for: the requested URL's host, or for a doi.org URL the resolved one (Taxicab
         -- resolves DOIs with its own free HEAD first), or doi.org itself when that lookup failed and Zyte followed it.
         coalesce(openalex.monitoring.landing_page_netloc_host(
                    openalex.monitoring.landing_page_fetched_netloc(resolved_url, url)), 'doi.org') price_key
  FROM openalex.taxicab.taxicab_results
  WHERE processed_date >= TIMESTAMP'{start}' AND processed_date < TIMESTAMP'{end}'
),
stored_html AS (SELECT taxicab_id FROM win WHERE stored AND ct = 'html'),
-- first = no earlier row for this exact URL anywhere in taxicab_results. Retry-lane and re-fetch rows are
-- "repeat"; they fail far more often by design, so the alarm looks at first attempts only.
-- shortcut: reads url + processed_date of the whole table each run (~250M rows, seconds); keep a url -> first_seen
-- table instead if that ever takes minutes.
prior AS (
  SELECT url, min(processed_date) first_seen
  FROM openalex.taxicab.taxicab_results
  WHERE processed_date < TIMESTAMP'{end}' AND url IN (SELECT url FROM win)
  GROUP BY url
),
pp AS (
  SELECT taxicab_id,
    max(CASE WHEN had_error THEN 1 ELSE 0 END) err,
    max(CASE WHEN size(authors) > 0 THEN 1 ELSE 0 END) au,
    max(CASE WHEN exists(authors, a -> size(a.affiliations) > 0) THEN 1 ELSE 0 END) aff,
    max(CASE WHEN exists(authors, a -> coalesce(a.is_corresponding, false)) THEN 1 ELSE 0 END) corr,
    max(CASE WHEN length(abstract) > 100 THEN 1 ELSE 0 END) abs,
    max(CASE WHEN exists(urls, u -> u.content_type = 'pdf') THEN 1 ELSE 0 END) pdfl,
    max(CASE WHEN nullif(license, '') IS NOT NULL THEN 1 ELSE 0 END) lic
  FROM openalex.parseland.parsed_pages
  WHERE parsed_date >= TIMESTAMP'{start}' - INTERVAL 1 DAY AND taxicab_id IN (SELECT taxicab_id FROM stored_html)
  GROUP BY taxicab_id
),
mt AS (
  SELECT replace(file_key, '.html.gz', '') taxicab_id,
    max(CASE WHEN exists(meta_tags, x -> lower(x) RLIKE 'name=["\']?(citation_title|dc\\.title|eprints\\.title|bepress_citation_title|prism\\.title|dcterms\\.title)["\']') THEN 1 ELSE 0 END) cit,
    max(CASE WHEN lower(page_title) RLIKE 'just a moment|attention required|access denied|pardon our interruption|making sure you|checking your browser|are you a robot|verify you are human|captcha|security check|bot manager|robot check|request rejected|unusual traffic|one more step|ddos|incapsula|human verification|you have been blocked|cloudflare|radware|perfdrive|validate user|too many requests|please wait' THEN 1 ELSE 0 END) wall,
    max(CASE WHEN lower(page_title) RLIKE '(^| )(log ?in|sign ?in)( |$|:)|institutional login|shibboleth|wayf|openathens|choose your institution' THEN 1 ELSE 0 END) login
  FROM openalex.landing_page.meta_tags
  WHERE fetched_at >= TIMESTAMP'{start}' - INTERVAL 1 DAY AND status = 'ok'
    AND replace(file_key, '.html.gz', '') IN (SELECT taxicab_id FROM stored_html)
  GROUP BY 1
),
price AS (
  SELECT price_key, valid_from,
         coalesce(lead(valid_from) OVER (PARTITION BY price_key ORDER BY valid_from), DATE'9999-12-31') valid_to,
         usd_per_million_requests, requests_per_fetch
  FROM openalex.monitoring.landing_page_zyte_prices
),
j AS (
  SELECT w.fetch_date, w.host, w.id_namespace,
    CASE WHEN w.processed_date > p.first_seen THEN 'repeat' ELSE 'first' END attempt,
    w.sc, w.sb, w.stored,
    w.stored AND w.ct = 'pdf' stored_pdf,
    w.stored AND w.ct = 'html' stored_html,
    pp.taxicab_id IS NOT NULL parsed, pp.err, pp.au, pp.aff, pp.corr, pp.abs, pp.pdfl, pp.lic,
    mt.taxicab_id IS NOT NULL has_mt, mt.cit, mt.wall, mt.login,
    coalesce(pk.requests_per_fetch, pd.requests_per_fetch) req,
    coalesce(pk.usd_per_million_requests, pd.usd_per_million_requests) usd_m
  FROM win w
  LEFT JOIN prior p USING (url)
  LEFT JOIN pp ON pp.taxicab_id = w.taxicab_id
  LEFT JOIN mt ON mt.taxicab_id = w.taxicab_id
  LEFT JOIN price pk ON pk.price_key = w.price_key AND w.fetch_date >= pk.valid_from AND w.fetch_date < pk.valid_to
  LEFT JOIN price pd ON pd.price_key = '*' AND w.fetch_date >= pd.valid_from AND w.fetch_date < pd.valid_to
)
SELECT fetch_date, host, attempt, id_namespace,
  count(*) fetches,
  count_if(stored) stored,
  count_if(stored_pdf) stored_pdf,
  count_if(stored_html) stored_html,
  count_if(sb) fail_soft_block,
  count_if(NOT sb AND sc IN (401, 403)) fail_403,
  count_if(NOT sb AND sc = 429) fail_429,
  count_if(NOT sb AND sc IN (500, 502, 503, 520)) fail_5xx,
  count_if(NOT sb AND sc IN (0, 504)) fail_timeout,
  count_if(NOT sb AND sc = 400) fail_400,
  count_if(NOT sb AND sc IN (404, 410)) fail_404,
  count_if(NOT stored AND NOT sb AND sc NOT IN (401, 403, 429, 500, 502, 503, 520, 0, 504, 400, 404, 410)) fail_other,
  count_if(stored_html AND parsed) parsed,
  count_if(stored_html AND err = 1) parse_error,
  count_if(stored_html AND au = 1) has_authors,
  count_if(stored_html AND aff = 1) has_affiliations,
  count_if(stored_html AND corr = 1) has_corresponding,
  count_if(stored_html AND abs = 1) has_abstract,
  count_if(stored_html AND pdfl = 1) has_pdf_link,
  count_if(stored_html AND lic = 1) has_license,
  count_if(stored_pdf OR (stored_html AND (au = 1 OR abs = 1))) useful,
  count_if(stored_html AND has_mt) html_meta_read,
  count_if(stored_html AND cit = 1 AND wall = 0 AND login = 0) html_citation_meta,
  count_if(stored_html AND wall = 1) html_wall_title,
  count_if(stored_html AND login = 1) html_login_title,
  sum(req) zyte_requests_est,
  sum(req * usd_m / 1e6) zyte_usd_est,
  current_timestamp() computed_at
FROM j
GROUP BY ALL
