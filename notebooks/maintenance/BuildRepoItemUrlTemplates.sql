-- Databricks notebook source
-- oxjob #1453: build openalex.repo.repo_item_url_templates, each EPrints-style repository's item-page address pattern,
-- learned from its own records. A record whose OAI id is oai:<host>:<n> has its item page at <prefix><n>/, and most of
-- a repository's records say so themselves: a file link (https://eprints.soton.ac.uk/id/eprint/473705/1/x.pdf) or the
-- item link (https://strathprints.strath.ac.uk/80158/) sits in their URL list. CreateSuperLocations uses this table
-- only as its last resort, for a repository record whose landing page is not the item (an ORCID iD, an author or
-- journal browse page, a link with citation markup glued on) and whose own URL list holds nothing better.
--
-- One row per OAI host: the prefix its records use most, kept only when at least 20 records show a prefix and at least
-- 95% of them agree on its web host (http/https and /id/eprint/ variants of one host all resolve to the same page).
-- The web host can differ from the OAI host (oai:USBKOELN.ub.uni-koeln.de:<n> lives at kups.ub.uni-koeln.de/<n>/).
-- Bar (12 Nov 2026): of 38,672 addresses built from this table, 34,632 appear word for word in the repository's own
-- raw record (dc:relation); 41 of 41 fetched pages that could be read were the item's own page.
--
-- Reads last night's openalex.works.superlocations, so run it by hand when a repository joins or moves; a missing row
-- only means such a record keeps the landing page it has. Revert: TRUNCATE this table (never DROP it while
-- CreateSuperLocations joins it).

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.repo.repo_item_url_templates
COMMENT 'oxjob #1453: item-page address prefix per EPrints-style OAI host (item page = item_url_prefix || <n> || /), learned from the host''s own records; last-resort landing page in CreateSuperLocations. Built by notebooks/maintenance/BuildRepoItemUrlTemplates.sql.'
AS
WITH r AS (
  SELECT regexp_extract(native_id, '^oai:(.+):[0-9]+$', 1) AS oai_host,
         regexp_extract(native_id, '([0-9]+)$', 1) AS n,
         urls, landing_page_url
  FROM openalex.works.superlocations
  WHERE provenance = 'repo' AND native_id RLIKE '^oai:.+:[0-9]+$'
), p AS (
  SELECT oai_host,
         get(filter(transform(array_union(transform(urls, u -> u.url), array(landing_page_url)),
               u -> regexp_extract(regexp_replace(u, '([<>"]|&quot;|&gt;|&lt;).*$', ''),
                                   concat('^(https?://[^/]+/+(id/eprint/)?)', n, '(/|$)'), 1)),
             u -> u IS NOT NULL AND u <> ''), 0) AS prefix
  FROM r
), g AS (
  SELECT oai_host, prefix, lower(regexp_extract(prefix, '^https?://(www\\.)?([^/]+)', 2)) AS web_host, count(*) AS c
  FROM p WHERE prefix IS NOT NULL GROUP BY 1, 2, 3
), h AS (
  SELECT oai_host, web_host, sum(c) AS host_records, max_by(prefix, c) AS prefix FROM g GROUP BY 1, 2
), t AS (
  SELECT oai_host, sum(host_records) AS records_with_prefix, max(host_records) AS records_on_host,
         max_by(prefix, host_records) AS item_url_prefix
  FROM h GROUP BY 1
)
SELECT oai_host, item_url_prefix, records_with_prefix, records_on_host,
       records_on_host / records_with_prefix AS host_share, current_timestamp() AS built_at
FROM t
WHERE records_on_host >= 20 AND records_on_host / records_with_prefix >= 0.95
