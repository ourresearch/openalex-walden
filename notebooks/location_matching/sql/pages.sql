-- Map_Work_Ids 2/3, the location matcher: same-page twins (oxjob #2026, Casey 2026-11-14). A repository record whose landing page
-- already sits on exactly one work at __V__, through that work's repository or MAG copy, and whose title is the work's title, is
-- that copy again under a new record id: a renamed OAI feed (oai:repositorio.itesm.mx:11285/623579 -> oai:repositorio.tec.mx:11285/623579,
-- one handle), a second feed of the same repository, or the repository's own record of a page MAG crawled (Persée, Banco de la
-- República, ORA). decisions.sql binds the record to that work where the model would mint (layer page_bind). Scope = what #2026
-- measured on the live nights 11-03..11-14 (Casey: "limit it to the scope we have seen, such as repos"): 5,363 records the matcher
-- minted, 22 of 22 read the same repository object; the type veto and Qwen's doubt kept them apart. The page is folded the way
-- keys.sql folds it; doi.org pages are identifiers (keys.sql 'd:'), not pages.
CREATE OR REPLACE TABLE openalex.works.location_work_match_pages AS
WITH s AS (
  SELECT s.seed_id, f.nt, regexp_replace(regexp_replace(lower(r.landing_page_url), '^https?://(www\\.)?', ''), '/+$', '') AS page
  FROM openalex.works.location_work_match_seeds s
  JOIN openalex.works.location_work_match_records r
    ON r.provenance = s.provenance AND r.native_id_namespace = s.native_id_namespace AND r.native_id = s.native_id
  JOIN openalex.works.location_work_match_seed_feat f ON f.seed_id = s.seed_id
  WHERE s.provenance = 'repo' AND f.nt IS NOT NULL AND NOT lower(r.landing_page_url) RLIKE '^https?://(dx\\.)?doi\\.org/'
),
sp AS (SELECT * FROM s WHERE page RLIKE '^[^/]+/.+'),
k AS (
  SELECT f.id, substr(t.k, 3) AS page
  FROM openalex.works.location_work_match_keys f LATERAL VIEW explode(f.keys) t AS k
  WHERE t.k IN (SELECT concat('u:', page) FROM sp)
),
one AS (SELECT page, min(id) AS work_id FROM k GROUP BY page HAVING count(DISTINCT id) = 1),
held AS (
  SELECT o.page, o.work_id
  FROM one o
  JOIN (SELECT id, locations FROM openalex.works.openalex_works VERSION AS OF __V__ WHERE id IN (SELECT work_id FROM one)) w
    ON w.id = o.work_id
  WHERE exists(w.locations, l -> l.provenance IN ('repo', 'mag')
    AND regexp_replace(regexp_replace(lower(l.landing_page_url), '^https?://(www\\.)?', ''), '/+$', '') = o.page)
)
SELECT sp.seed_id, h.work_id, sp.page
FROM sp
JOIN held h ON h.page = sp.page
JOIN openalex.works.location_work_match_keys f ON f.id = h.work_id AND f.nt = sp.nt
;
SELECT count(*) AS same_page_seeds, count(DISTINCT work_id) AS works FROM openalex.works.location_work_match_pages
