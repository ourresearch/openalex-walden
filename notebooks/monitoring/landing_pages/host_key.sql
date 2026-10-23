-- Landing-page host key shared by every landing-pages lane (oxjob #1789). One definition, used by the
-- scoreboard (openalex.monitoring.landing_page_funnel), the Parseland gold gate (#1791) and the fetch guard (#1790).
-- Call openalex.monitoring.landing_page_host(url, resolved_url, native_id_namespace, native_id) on taxicab_results
-- columns. Run this file by hand when it changes (scripts/landing_page_scoreboard.py ddl) and log it in the charter
-- write log; the nightly job only reads these functions.
-- Order matters: landing_page_doi_prefixes (doi_prefixes.sql) must exist before landing_page_doi_prefix_host.

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_netloc(u STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Host part of a URL: lower-cased, leading www. stripped, NULL when none. Regex, not parse_url (parse_url fails on SICI URLs such as BioOne).'
RETURN nullif(regexp_replace(lower(regexp_extract(u, '^[A-Za-z][A-Za-z0-9+.-]*://([^/:?#]+)', 1)), '^www\\.', ''), '');

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_registered_domain(netloc STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Registered domain of a host name (onlinelibrary.wiley.com -> wiley.com; two-level suffixes such as ac.uk, com.br, edu.au kept whole). Falls back to the host itself (IP addresses, odd TLDs).'
RETURN coalesce(nullif(regexp_extract(netloc, '([a-z0-9-]+\\.(co|ac|com|org|edu|gov|net|or|ne|go|gob)\\.[a-z]{2}|[a-z0-9-]+\\.[a-z]+)$', 0), ''), netloc);

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_netloc_host(netloc STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Host key of a fetched netloc: its registered domain, with pdf.sciencedirectassets.com and linkinghub.elsevier.com folded into sciencedirect.com and wileyonlinelibrary.com into wiley.com. NULL for NULL.'
RETURN CASE
  WHEN netloc = 'linkinghub.elsevier.com' THEN 'sciencedirect.com'
  ELSE CASE openalex.monitoring.landing_page_registered_domain(netloc)
    WHEN 'sciencedirectassets.com' THEN 'sciencedirect.com'
    WHEN 'wileyonlinelibrary.com' THEN 'wiley.com'
    ELSE openalex.monitoring.landing_page_registered_domain(netloc) END
  END;

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_fetched_netloc(url STRING, resolved_url STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Host we actually reached: the resolved URL host, else the requested URL host; doi.org never counts (144M of 200M recent resolved_urls are doi.org).'
RETURN coalesce(
  nullif(nullif(openalex.monitoring.landing_page_netloc(resolved_url), 'doi.org'), 'dx.doi.org'),
  nullif(nullif(openalex.monitoring.landing_page_netloc(url), 'doi.org'), 'dx.doi.org'));

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_doi_prefix_host(native_id_namespace STRING, native_id STRING, url STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Host for a DOI fetch that never reached a publisher URL: the most common resolved host of its DOI prefix (openalex.monitoring.landing_page_doi_prefixes, which also records that host''s share), else "doi.org (unresolved)".'
RETURN coalesce(
  (SELECT max(p.host) FROM openalex.monitoring.landing_page_doi_prefixes p
    WHERE p.doi_prefix = regexp_extract(lower(CASE WHEN native_id_namespace = 'doi' THEN native_id ELSE url END), '(10\\.[0-9]{4,9})/', 1)),
  'doi.org (unresolved)');

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_host_of(netloc STRING, native_id_namespace STRING, native_id STRING, url STRING)
RETURNS STRING
COMMENT 'oxjob #1789. Host key from a fetched netloc (landing_page_netloc_host); no netloc = the DOI prefix''s host (landing_page_doi_prefix_host).'
RETURN coalesce(openalex.monitoring.landing_page_netloc_host(netloc),
                openalex.monitoring.landing_page_doi_prefix_host(native_id_namespace, native_id, url));

CREATE OR REPLACE FUNCTION openalex.monitoring.landing_page_host(url STRING, resolved_url STRING, native_id_namespace STRING, native_id STRING)
RETURNS STRING
COMMENT 'oxjob #1789. THE landing-page host key every landing-pages lane joins on. Call it on taxicab_results columns (older-era pages without a resolved_url: pass NULL for it).'
RETURN openalex.monitoring.landing_page_host_of(
  openalex.monitoring.landing_page_fetched_netloc(url, resolved_url), native_id_namespace, native_id, url);
