-- Scoreboard tables (oxjob #1789). Created by hand once; the nightly job only writes rows (REPLACE WHERE on its window).
-- Run before host_key.sql (landing_page_doi_prefix_host reads landing_page_doi_prefixes).

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_doi_prefixes (
  doi_prefix STRING,
  host STRING COMMENT 'landing_page_netloc_host of the resolved fetches',
  share DOUBLE COMMENT 'share of the prefix''s resolved fetches on that host (its most common one)',
  fetches BIGINT COMMENT 'resolved DOI fetches for the prefix in taxicab_results (>= 20)',
  built_at TIMESTAMP
)
COMMENT 'oxjob #1789: DOI prefix -> dominant resolved host. Filled by notebooks/monitoring/landing_pages/doi_prefixes.sql; read by openalex.monitoring.landing_page_doi_prefix_host.';

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_funnel (
  fetch_date DATE COMMENT 'UTC day of taxicab_results.processed_date',
  host STRING COMMENT 'openalex.monitoring.landing_page_host(url, resolved_url, native_id_namespace, native_id)',
  attempt STRING COMMENT 'first = no earlier taxicab_results row for this exact url; repeat = retry lane or re-fetch campaign',
  id_namespace STRING COMMENT 'taxicab_results.native_id_namespace: doi (publisher landing pages) or pmh (repository records)',
  fetches BIGINT COMMENT 'taxicab_results rows',
  stored BIGINT COMMENT 'status 200 and not soft-blocked',
  stored_pdf BIGINT,
  stored_html BIGINT,
  fail_soft_block BIGINT COMMENT 'is_soft_block (Taxicab saw a wall marker)',
  fail_403 BIGINT COMMENT '401 or 403',
  fail_429 BIGINT,
  fail_5xx BIGINT COMMENT '500, 502, 503, 520',
  fail_timeout BIGINT COMMENT 'status 0 or 504',
  fail_400 BIGINT COMMENT 'Zyte ban / bad request',
  fail_404 BIGINT COMMENT '404 or 410',
  fail_other BIGINT,
  parsed BIGINT COMMENT 'stored HTML with a Parseland row (parseland.parsed_pages, any parse)',
  parse_error BIGINT COMMENT 'stored HTML whose parse had_error',
  has_authors BIGINT COMMENT 'stored HTML, Parseland found >= 1 author',
  has_affiliations BIGINT COMMENT 'stored HTML, >= 1 author with an affiliation',
  has_corresponding BIGINT COMMENT 'stored HTML, >= 1 author flagged corresponding',
  has_abstract BIGINT COMMENT 'stored HTML, abstract longer than 100 characters',
  has_pdf_link BIGINT COMMENT 'stored HTML, Parseland found a PDF URL',
  has_license BIGINT COMMENT 'stored HTML, Parseland found a licence',
  useful BIGINT COMMENT 'stored PDF, or stored HTML with authors or an abstract',
  html_meta_read BIGINT COMMENT 'stored HTML with a landing_page.meta_tags row (cross-check coverage)',
  html_citation_meta BIGINT COMMENT 'stored HTML with citation_title / dc.title meta and no wall or login page title',
  html_wall_title BIGINT COMMENT 'stored HTML whose page title is a bot wall (Just a moment, captcha, ...)',
  html_login_title BIGINT COMMENT 'stored HTML whose page title is a login page',
  zyte_requests_est DOUBLE COMMENT 'fetches x billed Zyte requests per fetch (monitoring.landing_page_zyte_prices)',
  zyte_usd_est DOUBLE COMMENT 'zyte_requests_est x price per request for the domain we asked Zyte for',
  computed_at TIMESTAMP
)
CLUSTER BY (fetch_date, host)
COMMENT 'Landing-page scoreboard (oxjob #1789, charter plans/landing-pages.md): fetch -> stored -> parsed -> fields, per fetch day x host x attempt x id_namespace. Written nightly by walden notebooks/monitoring/LandingPageScoreboard.';

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_zyte_prices (
  price_key STRING COMMENT 'registered domain Zyte bills (the URL we asked for; for doi.org URLs the resolved host), or * for everything else',
  valid_from DATE,
  usd_per_million_requests DOUBLE,
  requests_per_fetch DOUBLE COMMENT 'billed Zyte requests per taxicab_results row (Wiley recipe makes 2)',
  source STRING
)
COMMENT 'Zyte price per domain for the landing-page scoreboard cost columns (oxjob #1789). Refresh monthly from the Zyte dashboard stats API (#1290 EXPLORE 1-2).';

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_alerts (
  alert_date DATE COMMENT 'fetch day that tripped',
  host STRING COMMENT 'landing_page_host, or (top 50 DOI hosts) for many_hosts_drop',
  id_namespace STRING COMMENT 'doi or pmh',
  rule STRING COMMENT 'useful_rate_drop (host: > 20 points under its median daily rate of the 7 days before) or many_hosts_drop (5+ of the top 50 DOI hosts trip the same day)',
  severity STRING COMMENT 'watch = first day; confirmed = second day running, or a drop over twice the line',
  page BOOLEAN COMMENT 'confirmed on a top-50 DOI host, or many_hosts_drop: oxjob #1789 is woken',
  fetches BIGINT COMMENT 'first-attempt fetches that day',
  useful_rate DOUBLE,
  baseline_rate DOUBLE COMMENT 'median of the daily first-attempt useful rates over the 7 days before (days with >= 50 fetches)',
  baseline_days INT,
  drop_points DOUBLE,
  stored_rate DOUBLE,
  top_failure STRING COMMENT 'largest failure class that day, with its count (stored_but_not_useful = stored HTML without authors or abstract)',
  computed_at TIMESTAMP
)
CLUSTER BY (alert_date)
COMMENT 'Landing-page scoreboard alarms (oxjob #1789). First attempts only. Lanes that ship on the gate revert when their host pages (charter decision 2026-10-23 14:50). Written by notebooks/monitoring/landing_pages/alerts.sql.';

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_host_state (
  snapshot_date DATE,
  host STRING COMMENT 'landing_page_host of the newest fetch for the DOI, else the DOI prefix host',
  works BIGINT COMMENT 'Crossref works (crossref.crossref_works DOIs)',
  age_lt_3m BIGINT COMMENT 'newest stored HTML fetch for the DOI under 3 months old',
  age_3_6m BIGINT,
  age_6_12m BIGINT,
  age_gt_12m BIGINT COMMENT 'stored in the new era (taxicab_results, since Apr 2025) but over 12 months ago',
  age_older_era BIGINT COMMENT 'only a Feb-2025 import copy (landing_page.dynamo_landing_page_s3_export)',
  age_fetched_nothing_stored BIGINT COMMENT 'Taxicab tried, never stored HTML',
  age_never BIGINT,
  fresh_useful BIGINT COMMENT 'newest stored HTML under 6 months old and Parseland found authors or an abstract: the headline numerator',
  win_authors BIGINT COMMENT 'landing page has authors, Crossref has none',
  win_affiliations BIGINT COMMENT 'landing page has affiliations, Crossref has none',
  win_corresponding BIGINT COMMENT 'landing page flags a corresponding author (Crossref never does)',
  win_abstract BIGINT COMMENT 'landing page has an abstract, Crossref has none',
  win_license BIGINT COMMENT 'landing page has a licence (outranks PDF and Crossref)',
  computed_at TIMESTAMP
)
CLUSTER BY (snapshot_date, host)
COMMENT 'Landing-page scoreboard, works side (oxjob #1789): per host, how fresh Crossref works'' landing pages are and what they add over Crossref. Weekly. Written by notebooks/monitoring/landing_pages/host_state.sql.';

CREATE TABLE IF NOT EXISTS openalex.monitoring.landing_page_alert_acks (
  host STRING COMMENT 'landing_page_host',
  id_namespace STRING COMMENT 'doi or pmh',
  failure STRING COMMENT 'the known failure class, as top_failure names it: soft_block, 403, 429, 5xx, timeout, 400, 404, other, stored_but_not_useful',
  owner_job STRING COMMENT 'oxjob that owns the fix, e.g. #1790',
  reason STRING,
  from_date DATE COMMENT 'first fetch day the ack covers',
  until_date DATE COMMENT 'last fetch day the ack covers; the alarm re-arms after it',
  created_at TIMESTAMP
)
COMMENT 'Landing-page scoreboard alarm acks (oxjob #1789): a known, owned failure on one host stops paging while the rest of its pages hold up. Rows live in notebooks/monitoring/landing_pages/alert_acks.sql (lps.sh ddl rewrites the table); read by alerts.sql.';
