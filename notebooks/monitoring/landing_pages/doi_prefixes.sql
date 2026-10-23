-- DOI prefix -> the host its DOIs resolve to (oxjob #1789; idea from #1791). Used by landing_page_doi_prefix_host
-- for DOI fetches that never reached a publisher URL (failed fetches, and older-era pages stored without a
-- resolved_url). Every prefix with >= 20 resolved fetches gets its most common host; `share` says how sure that is
-- (10.1016 -> sciencedirect.com at 0.80: cell.com, thelancet.com take the rest). Filter on share for stricter use.
-- Rebuild by hand (monthly at most) and, in the same sitting, re-run the funnel for the last 14 days: rows move
-- between hosts, so a rebuild without the re-run shifts every alarm baseline.
INSERT OVERWRITE openalex.monitoring.landing_page_doi_prefixes
WITH r AS (
  SELECT regexp_extract(lower(native_id), '^(10\\.[0-9]{4,9})/', 1) doi_prefix,
         openalex.monitoring.landing_page_netloc_host(openalex.monitoring.landing_page_fetched_netloc(url, resolved_url)) host
  FROM openalex.taxicab.taxicab_results
  WHERE native_id_namespace = 'doi'
),
g AS (SELECT doi_prefix, host, count(*) n FROM r WHERE doi_prefix <> '' AND host IS NOT NULL GROUP BY 1, 2),
p AS (SELECT doi_prefix, sum(n) fetches, max_by(host, n) host, max(n) top_n FROM g GROUP BY doi_prefix)
SELECT doi_prefix, host, round(top_n / fetches, 3) share, fetches, current_timestamp() built_at
FROM p
WHERE fetches >= 20;
