-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### NWO work-award links from NWO's own output lists (oxjob #244; refresh and title tier, oxjob #2014)
-- MAGIC
-- MAGIC NWO researchers report the outputs of each project in NWOpen (`products`: type, title, year, journal, volume, pages,
-- MAGIC authors, a link). openalex-ingest `nwo_awards.py` saves them weekly as `products_json` in
-- MAGIC `s3://openalex-ingest/awards/nwopen/`. This notebook reads the newest object there (the same pin as the Awards job:
-- MAGIC newest modification time, then path), resolves each listed output to one OpenAlex work, and writes:
-- MAGIC - `openalex.awards.nwo_work_award_matches`: one row per (project, listed output), resolved or not, with the tier, the evidence,
-- MAGIC   `accepted` and the object read;
-- MAGIC - `openalex.awards.nwo_work_funders`: the accepted links, one row per `(work_id, funder_id)` with the NWO project ids
-- MAGIC   (`award_ids`), the shape Nightly Grants' `nwo_work_funders` leg reads (priority 3, `lib/work_awards.py` LEGS).
-- MAGIC
-- MAGIC **Tiers** (one work per listed output; the first tier that resolves wins):
-- MAGIC 1. **DOI**: salvaged from `url_open_access` (NWO has no DOI field), matched to `openalex_works.doi`. Accepted when the
-- MAGIC    work's title is close to the listed one or a listed author's surname is on the work.
-- MAGIC 2. **Title**: the same normalized title (`location_work_match_keys.nt`, Match_Locations_To_Works #1668, same
-- MAGIC    normalization), at least `min_title_chars` long, year within `max_year_gap`, exactly one such work (or exactly one that
-- MAGIC    shares a listed author). Accepted only when a listed author's surname is on the work, the work is not a
-- MAGIC    `held_work_types` record while NWO lists an article, book or thesis, and the year is the same or (a year apart)
-- MAGIC    the volume or first page agrees: a year apart with no number in common is where the grade's errors sat (an abstract,
-- MAGIC    poster or preprint of a later article).
-- MAGIC
-- MAGIC Patents and contracts are not works. Only projects that are NWO awards in `openalex_awards_raw` (provenance `nwopen`)
-- MAGIC are linked: a project id Nightly holds no award for would sit in `work_unmatched` as NO_CANDIDATE, never served.
-- MAGIC Measured 11-10 on the 11-09 object: 193,488 listed outputs on 17,182 projects. Blind grade of 300 new title links
-- MAGIC (two graders, 15 planted wrong links all caught): 287 of 287 right under this rule (Wilson 95% low 98.7%).
-- MAGIC The job is PAUSED: run it on demand after the weekly NWOpen fetch and that day's Awards run, before Nightly Grants.
-- MAGIC Undo: `RESTORE TABLE openalex.awards.nwo_work_funders TO VERSION AS OF <the version before the run>`.

-- COMMAND ----------

-- Regex escaping guard: this notebook double-escapes regex backslashes ('\\s', '\\.'), as walden's SQL does. If this
-- engine reads them differently, the DOI salvage and title normalization would shrink silently, so stop here instead.
SELECT assert_true(regexp_extract('see 10.1234/ab.c x', '(10\\.[0-9]{4,9}/[^\\s]+)', 1) = '10.1234/ab.c',
  'NWO_REGEX_MODE: double-escaped regex backslashes did not reach the regex engine as single ones, STOP');

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.awards.nwo_work_award_matches USING delta AS
WITH
nwo_rules AS (
  SELECT 20 AS min_title_chars, 1 AS max_year_gap, 50 AS max_same_title_works, 0.2 AS max_doi_title_distance,
    array('dataset', 'paratext', 'erratum', 'retraction', 'peer-review', 'supplementary-materials', 'conference-abstract') AS held_work_types,
    array('Wetenschappelijk artikel', 'Boek', 'Proefschrift') AS typed_entries
),
nwo_object AS (
  SELECT max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path)) AS object
  FROM read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet')
),
nwo_projects AS (
  SELECT r.project_id, r.products_json
  FROM (SELECT project_id, products_json, _metadata.file_path AS file_path
        FROM read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet')) r
  JOIN nwo_object o ON r.file_path = o.object
  WHERE r.products_json IS NOT NULL AND r.products_json NOT IN ('', '[]', 'null')
),
nwo_award_keys AS (
  SELECT DISTINCT funder_award_id AS project_id
  FROM openalex.awards.openalex_awards_raw
  WHERE provenance = 'nwopen' AND funder_id = 4320321800
),
exploded AS (
  -- explode first, join after: no JOIN directly after LATERAL VIEW (#244)
  SELECT p.project_id, t.pidx, t.prod
  FROM nwo_projects p LATERAL VIEW posexplode(from_json(p.products_json, 'array<map<string,string>>')) t AS pidx, prod
),
listed AS (
  SELECT e.project_id, e.pidx, e.prod['type'] AS ptype, e.prod['title'] AS title, try_cast(e.prod['year'] AS INT) AS yr,
    e.prod['url_open_access'] AS url,
    nullif(regexp_extract(e.prod['edition'], '(\\d+)', 1), '') AS volume_number,      -- NWO edition = the volume
    nullif(regexp_extract(e.prod['start'], '(\\d+)', 1), '') AS first_page_number,
    from_json(e.prod['authors'], 'array<struct<last_name:string,initials:string,first_name:string,role:string>>') AS authors,
    regexp_extract(lower(regexp_replace(e.prod['url_open_access'], '\\s+', '')), '(10\\.[0-9]{2,}/[^\\s"<>]+)', 1) AS doi_raw
  FROM exploded e
  JOIN nwo_award_keys k ON k.project_id = e.project_id
  WHERE coalesce(e.prod['type'], '') NOT IN ('Octrooi', 'Contract')  -- patents and contracts are not works
),
entries AS (
  SELECT *,
    -- the salvaged DOI without URL encoding, query strings, trailing punctuation or a landing-page suffix
    nullif(regexp_replace(regexp_replace(regexp_replace(try_url_decode(doi_raw), '[?#].*$', ''), '[.,:\\x3B]+$', ''),
      '/(full|abstract|pdf|epdf|html)$', ''), '') AS doi,
    -- normalized title: location_work_match_keys.nt's expression (notebooks/location_matching/sql/keys.sql)
    nullif(trim(regexp_replace(lower(translate(regexp_replace(coalesce(title, ''), '<[^>]+>', ' '),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')), '') AS nt,
    -- listed authors' surnames: the last word of last_name ('van der Zwaag' -> 'zwaag'), letters only, accents folded
    filter(transform(authors, a -> lower(regexp_replace(translate(element_at(split(trim(coalesce(a.last_name, '')), ' +'), -1),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy'), '[^\\p{L}]', ''))), s -> length(s) >= 2) AS surnames
  FROM listed
),
title_cands AS (
  SELECT k.id, k.yr, k.nt
  FROM openalex.works.location_work_match_keys k
  JOIN (SELECT DISTINCT nt FROM entries WHERE length(nt) >= (SELECT min_title_chars FROM nwo_rules)) e ON k.nt = e.nt
),
usable_titles AS (
  SELECT nt FROM title_cands GROUP BY nt HAVING count(*) <= (SELECT max_same_title_works FROM nwo_rules)
),
entry_dois AS (
  -- each listed output's DOI in both spellings (cleaned, as salvaged), one row each, so the DOI tier is an equi-join
  SELECT DISTINCT project_id, pidx, nt, concat('https://doi.org/', d) AS doi_url
  FROM entries LATERAL VIEW explode(array(doi, nullif(doi_raw, ''))) x AS d WHERE d IS NOT NULL
),
doi_urls AS (
  SELECT DISTINCT doi_url FROM entry_dois
),
cand_ids AS (
  SELECT DISTINCT c.id FROM title_cands c JOIN usable_titles u ON u.nt = c.nt
),
works AS (
  SELECT w.id, lower(w.doi) AS doi, w.type, w.publication_year AS yr,
    nullif(regexp_extract(w.biblio.volume, '(\\d+)', 1), '') AS volume_number,
    nullif(regexp_extract(w.biblio.first_page, '(\\d+)', 1), '') AS first_page_number,
    nullif(trim(regexp_replace(lower(translate(regexp_replace(coalesce(w.title, ''), '<[^>]+>', ' '),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')), '') AS wnt,
    -- every word of every author name, accents folded, letters only
    array_distinct(filter(flatten(transform(w.authorships, a -> split(regexp_replace(lower(translate(coalesce(a.author.display_name, ''),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}]+', ' '), ' '))), x -> length(x) >= 2)) AS name_words
  FROM openalex.works.openalex_works w
  LEFT JOIN doi_urls d ON lower(w.doi) = d.doi_url
  LEFT JOIN cand_ids c ON w.id = c.id
  WHERE d.doi_url IS NOT NULL OR c.id IS NOT NULL
),
doi_hits AS (
  -- a DOI held by several works resolves only when exactly one of them has the listed title
  SELECT project_id, pidx, CASE WHEN count(*) = 1 THEN max(id) WHEN count_if(teq) = 1 THEN max_by(id, teq) END AS work_id
  FROM (SELECT DISTINCT d.project_id, d.pidx, w.id, w.wnt <=> d.nt AS teq FROM entry_dois d JOIN works w ON w.doi = d.doi_url)
  GROUP BY project_id, pidx
),
title_pool AS (
  SELECT e.project_id, e.pidx, c.id AS work_id, arrays_overlap(e.surnames, w.name_words) AS author_on_work
  FROM entries e
  JOIN title_cands c ON c.nt = e.nt
  JOIN usable_titles u ON u.nt = c.nt
  JOIN works w ON w.id = c.id
  CROSS JOIN nwo_rules r
  WHERE length(e.nt) >= r.min_title_chars AND abs(c.yr - e.yr) <= r.max_year_gap
),
title_hits AS (
  -- one candidate, or exactly one candidate that shares a listed author
  SELECT project_id, pidx, count(*) AS n_candidates,
    CASE WHEN count(*) = 1 THEN max(work_id) WHEN count_if(author_on_work) = 1 THEN max_by(work_id, author_on_work) END AS work_id
  FROM title_pool GROUP BY project_id, pidx
),
decided AS (
  SELECT e.project_id, e.pidx, e.ptype, e.yr, e.nt, e.doi, e.surnames, e.volume_number, e.first_page_number,
    coalesce(d.work_id, t.work_id) AS work_id,
    CASE WHEN d.work_id IS NOT NULL THEN 'doi' WHEN t.work_id IS NOT NULL THEN 'title' END AS tier,
    t.n_candidates
  FROM entries e
  LEFT JOIN doi_hits d ON d.project_id = e.project_id AND d.pidx = e.pidx
  LEFT JOIN title_hits t ON t.project_id = e.project_id AND t.pidx = e.pidx
)
SELECT x.project_id, x.pidx, x.ptype, x.work_id, x.tier, x.n_candidates, (SELECT object FROM nwo_object) AS nwopen_object,
  w.type AS work_type, x.yr AS listed_year, w.yr AS work_year,
  x.nt = w.wnt AS title_equal,
  coalesce(x.volume_number = w.volume_number OR x.first_page_number = w.first_page_number, false) AS numbers_agree,
  levenshtein(substr(w.wnt, 1, 200), substr(x.nt, 1, 200)) / greatest(length(substr(w.wnt, 1, 200)), length(substr(x.nt, 1, 200)), 1) AS title_distance,
  coalesce(arrays_overlap(x.surnames, w.name_words), false) AS author_on_work,
  coalesce(
    (x.tier = 'doi' AND (levenshtein(substr(w.wnt, 1, 200), substr(x.nt, 1, 200))
                           / greatest(length(substr(w.wnt, 1, 200)), length(substr(x.nt, 1, 200)), 1) <= r.max_doi_title_distance
                         OR arrays_overlap(x.surnames, w.name_words)))
    OR (x.tier = 'title' AND x.nt = w.wnt AND arrays_overlap(x.surnames, w.name_words)  -- the work's title today, not the keys' snapshot
        AND NOT (array_contains(r.held_work_types, w.type) AND array_contains(r.typed_entries, x.ptype))
        AND (x.yr = w.yr OR x.volume_number = w.volume_number OR x.first_page_number = w.first_page_number)),
    false) AS accepted
FROM decided x
LEFT JOIN works w ON w.id = x.work_id  -- unresolved outputs keep their row (work_id NULL, not accepted)
CROSS JOIN nwo_rules r;

-- COMMAND ----------

-- Gates, before the leg table is replaced. Each stops the run (the leg table keeps last week's links).
-- 1. The object this run read is not empty and its projects are NWO awards in raw (a stale raw or a changed id format
-- would leave links unserved without an error). The object is the one recorded in the matches table.
SELECT assert_true(
  (SELECT count(DISTINCT project_id) FROM (SELECT project_id, _metadata.file_path AS f
     FROM read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet'))
   WHERE f = (SELECT max(nwopen_object) FROM openalex.awards.nwo_work_award_matches)
     AND project_id IN (SELECT funder_award_id FROM openalex.awards.openalex_awards_raw WHERE provenance = 'nwopen' AND funder_id = 4320321800))
  >= greatest(1, 0.99 * (SELECT count(DISTINCT project_id) FROM (SELECT project_id, _metadata.file_path AS f
     FROM read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet'))
   WHERE f = (SELECT max(nwopen_object) FROM openalex.awards.nwo_work_award_matches))),
  'NWO_RAW_COVERAGE: the NWOpen object is empty or under 99% of its projects are nwopen awards in openalex_awards_raw, run the Awards job first');

-- COMMAND ----------

-- 2. One row per listed output.
SELECT assert_true((SELECT count(*) FROM (SELECT project_id, pidx FROM openalex.awards.nwo_work_award_matches
  GROUP BY 1, 2 HAVING count(*) > 1)) = 0, 'NWO_DUPLICATE_OUTPUTS: a listed output resolved twice');

-- COMMAND ----------

-- 3. Shrink guard: accepted links may not fall more than 5% below the links the leg holds now.
SELECT assert_true(
  (SELECT count(*) FROM (SELECT DISTINCT work_id, project_id FROM openalex.awards.nwo_work_award_matches WHERE accepted))
  >= 0.95 * (SELECT count(*) FROM (SELECT DISTINCT work_id, a FROM openalex.awards.nwo_work_funders LATERAL VIEW explode(award_ids) t AS a)),
  'NWO_SHRINK_GUARD: accepted NWO links are more than 5% below the current nwo_work_funders, check the object before replacing');

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.awards.nwo_work_funders USING delta AS
SELECT work_id, CAST(4320321800 AS BIGINT) AS funder_id, array_sort(array_distinct(collect_list(project_id))) AS award_ids
FROM openalex.awards.nwo_work_award_matches
WHERE accepted
GROUP BY work_id;

-- COMMAND ----------

-- Sanity: rows, links and how each tier did.
SELECT tier, accepted, count(*) AS listed_outputs, count(DISTINCT work_id) AS works, count(DISTINCT project_id) AS projects
FROM openalex.awards.nwo_work_award_matches GROUP BY ALL ORDER BY ALL;
