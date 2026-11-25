-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Funder-reported links: the papers funders list for their own grants
-- MAGIC
-- MAGIC Each source is one row of `link_sources` (its name, the identifiers tried in order, and whether a link must look like
-- MAGIC the listed output) and one mapping view, `<source>_listed`, that unpacks its newest object (the Awards job's pin) into
-- MAGIC the same columns. One matching path does the rest and writes:
-- MAGIC - `openalex.awards.funder_reported_link_matches`: one row per source, listed output and candidate work, with the tier,
-- MAGIC   the evidence, `accepted` and the object read;
-- MAGIC - `openalex.awards.funder_reported_links`: the accepted links, one row per `(source, work_id, funder_id)` with the
-- MAGIC   source's grant ids (`award_ids`, sorted; empty for a source that lists no grants). Nightly Grants reads it as one leg.
-- MAGIC
-- MAGIC **Rule.** The first tier, in the source's order, with a candidate decides. `doi`, `pmid`: every work holding the
-- MAGIC identifier. `work`: the OpenAlex work the list names, as given. `title`: the same normalized title (at least 20
-- MAGIC characters, year within 1, at most 50 works share it), and one such work or one that shares a listed author. A
-- MAGIC `title_or_author` source accepts an identifier link when the titles are close or a listed surname is an author's last
-- MAGIC name (`Wolf, M.` and `M. Wolf`: wolf; over 100 authors: the title decides), and a title link when the title is equal,
-- MAGIC a listed surname is an author's last name, the work is not a dataset-like record while the funder lists an article,
-- MAGIC book or thesis, and the year, volume or first page agrees. A link naming a grant is kept only when the grant is an award
-- MAGIC in `openalex_awards_raw` under provenance = source, and takes that award's `funder_id`.
-- MAGIC
-- MAGIC **`refmatch`** (last, for publications no identifier names): Casey's reference matcher (`notebooks/references/matcher`)
-- MAGIC decides each such output once, in the task `match_listed_outputs` after this one (`MatchListedOutputs.py`), and keeps
-- MAGIC its pick in `openalex.awards.funder_reported_refmatch_picks`, keyed by the output as listed (`listed_key`: title and surnames;
-- MAGIC FWF renumbers its output ids between fetches). The tier takes that linked pick, and accepts it only when the work's
-- MAGIC own title is the listed title (normalized, "; In: <book>" cut) and, as a title link, it is not a dataset-like or
-- MAGIC abstract record while the funder lists an article, book or thesis: the
-- MAGIC title guard of the 23 Nov FWF grade (205 of 209 right). A decision made today is read by tomorrow's run.
-- MAGIC
-- MAGIC **Gates, per source, before the links change:** it lists outputs and at least 99% of its grant ids are awards in raw;
-- MAGIC its links are at least 95% and (when it holds any) at most 125% of the links it holds now. A source that fails keeps its
-- MAGIC links, the others are written by one MERGE (deletions mirrored; a source dropped from `link_sources` loses its links),
-- MAGIC and the run then fails naming it. A source whose object cannot be read stops the run before anything is written.
-- MAGIC A task of Funder Reported Work Funders (02:30 UTC, before Nightly Grants).
-- MAGIC Undo: `RESTORE TABLE openalex.awards.funder_reported_links TO VERSION AS OF <the version before the run>`.

-- COMMAND ----------

-- The links table exists before anything can fail: its readers always have their input.
CREATE TABLE IF NOT EXISTS openalex.awards.funder_reported_links (source STRING, work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta;

-- COMMAND ----------

-- An intentional change bigger than the shrink and growth gates allow (a refmatch backfill batch, an undo) is written for one
-- source on one UTC day by a row here, added by hand with the reason and who said yes. Coverage is never overridden.
CREATE TABLE IF NOT EXISTS openalex.awards.funder_reported_links_gate_overrides (source STRING, run_date DATE, reason STRING, by STRING)
USING delta;

-- COMMAND ----------

-- The reference matcher's decisions (MatchListedOutputs.py writes them), one row per source and listed output it decided,
-- keyed by what the funder lists (listed_key: title and surnames), not its output id, which FWF renumbers between fetches.
-- The refmatch tier reads them, so the table exists before anything reads it.
CREATE TABLE IF NOT EXISTS openalex.awards.funder_reported_refmatch_picks (source STRING, listed_key STRING, output_key STRING,
  title_key STRING, guard_key STRING, text STRING, text_hash BIGINT, work_id BIGINT, score DOUBLE, checklist_p DOUBLE,
  in_band BOOLEAN, linked BOOLEAN, n_candidates INT, status STRING, decided_at TIMESTAMP, run_id STRING) USING delta;

-- COMMAND ----------

-- Regex backslashes are double-escaped ('\\s', '\\d'), as walden's SQL does. Stop if this engine reads them differently.
SELECT assert_true(regexp_extract('see 10.1234/ab.c x', '(10\\.[0-9]{4,9}/[^\\s]+)', 1) = '10.1234/ab.c',
  'LINKS_REGEX_MODE: double-escaped regex backslashes did not reach the regex engine as single ones, STOP');

-- COMMAND ----------

-- Titles and names are compared as location_work_match_keys.nt (notebooks/location_matching/sql/keys.sql).
CREATE OR REPLACE TEMPORARY FUNCTION match_text(s STRING) RETURNS STRING RETURN
  nullif(trim(regexp_replace(lower(translate(regexp_replace(coalesce(s, ''), '<[^>]+>', ' '),
    'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
    'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')), '');

-- COMMAND ----------

-- A listed output as the funder lists it: its title, and its surnames lower-cased, sorted, then normalized (a SQL function
-- cannot call match_text inside a lambda). The reference matcher keys its
-- decisions on it (MatchListedOutputs.py reads this definition), so a renumbered output keeps its decision.
CREATE OR REPLACE TEMPORARY FUNCTION listed_key(title STRING, surnames ARRAY<STRING>) RETURNS STRING RETURN
  concat_ws('|', match_text(title), match_text(array_join(array_sort(array_distinct(transform(coalesce(surnames, array()),
    x -> lower(x)))), ' ')));

-- COMMAND ----------

-- A title key that compares Japanese (and Chinese) titles as written either way: full-width digits and letters read as
-- ASCII, and no space next to a kanji or kana ('ICTを用いた' = 'ICT を用いた', '（２）' = '(2)'). A key without such a character
-- is unchanged, so no other source's key moves. The refmatch title guard compares it (MatchListedOutputs.py reads it too).
CREATE OR REPLACE TEMPORARY FUNCTION cjk_key(k STRING) RETURNS STRING RETURN
  CASE WHEN k RLIKE '[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]' THEN regexp_replace(translate(k, '０１２３４５６７８９ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ', '0123456789abcdefghijklmnopqrstuvwxyz'),
    ' (?=[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}])|(?<=[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]) ', '') ELSE k END;

-- COMMAND ----------

-- One row per source. source: the provenance of its awards in openalex_awards_raw (sources.csv), or of its file when it
-- lists no grants. tiers: identifiers tried, in order; the reference matcher (refmatch) comes last for every source, for
-- the publications with a title that nothing else names (a source that lists only DOIs never reaches it).
-- check: title_or_author, the work must look like the listed output; none, the funder's identifiers are trusted.
CREATE OR REPLACE TEMP VIEW link_sources AS SELECT * FROM VALUES
  ('nwopen', 'doi,title', 'title_or_author'),
  ('fwf_open_api', 'doi,pmid', 'title_or_author'),
  ('anr_opendata', 'doi,work', 'none'),
  ('kaken', 'doi,work', 'none'),
  ('hakai_publications', 'doi', 'none')
AS t(source, tiers, check);

-- COMMAND ----------

-- NWO: products_json lists each project's products (NWOpen), the DOI inside url_open_access. Patents and contracts are not works.
CREATE OR REPLACE TEMP VIEW nwopen_listed AS
WITH objects AS (
  SELECT project_id, products_json, _metadata FROM read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet')
),
products AS (
  SELECT project_id, _metadata.file_path AS object, posexplode(from_json(products_json, 'array<map<string,string>>')) AS (i, p)
  FROM objects
  WHERE _metadata.file_path = (SELECT max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path)) FROM objects)
)
SELECT project_id AS award_id, CAST(NULL AS BIGINT) AS funder_id, CAST(i AS STRING) AS output_key, p['url_open_access'] AS doi,
  CAST(NULL AS STRING) AS pmid, CAST(NULL AS BIGINT) AS work_id, p['title'] AS title, try_cast(p['year'] AS INT) AS year, p['edition'] AS volume, p['start'] AS first_page,
  CASE p['type'] WHEN 'Wetenschappelijk artikel' THEN 'article' WHEN 'Boek' THEN 'book' WHEN 'Proefschrift' THEN 'thesis' END AS output_type,
  from_json(p['authors'], 'array<struct<last_name:string>>').last_name AS surnames, object
FROM products
WHERE coalesce(p['type'], '') NOT IN ('Octrooi', 'Contract');

-- COMMAND ----------

-- FWF: outputs lists each project's outputs (Open API) with the authors' surnames. Older objects have no outputs column.
-- output_type: set for publications only (talks, prizes, data and the like stay NULL); a preprint counts as an article.
CREATE OR REPLACE TEMP VIEW fwf_open_api_listed AS
WITH objects AS (
  SELECT code, outputs, _metadata FROM read_files('s3://openalex-ingest/awards/fwf_open_api/*.parquet', format => 'parquet', mergeSchema => true)
),
outputs AS (
  SELECT code, _metadata.file_path AS object, explode(outputs) AS o
  FROM objects
  WHERE _metadata.file_path = (SELECT max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path)) FROM objects)
)
SELECT code AS award_id, CAST(NULL AS BIGINT) AS funder_id, o.output_id AS output_key, o.doi, o.pmid, CAST(NULL AS BIGINT) AS work_id,
  o.title, CAST(NULL AS INT) AS year,
  CAST(NULL AS STRING) AS volume, CAST(NULL AS STRING) AS first_page,
  CASE WHEN o.category = 'publications' THEN CASE o.type WHEN 'Journal Article' THEN 'article' WHEN 'Preprint' THEN 'article' WHEN 'Book' THEN 'book'
    WHEN 'PhD Thesis' THEN 'thesis' WHEN 'Postdoctoral Thesis' THEN 'thesis' WHEN 'Master Thesis' THEN 'thesis'
    ELSE 'publication' END END AS output_type, o.surnames, object
FROM outputs;

-- COMMAND ----------

-- ANR: one row per DOI with the grants that funded it (anr_work_links_to_s3.py), and the works whose HAL deposit names
-- an ANR code in its own text (hal_anr_work_links, oxjob #362).
CREATE OR REPLACE TEMP VIEW anr_opendata_listed AS
SELECT trim(grant_id) AS award_id, CAST(NULL AS BIGINT) AS funder_id, doi AS output_key, doi, CAST(NULL AS STRING) AS pmid,
  CAST(NULL AS BIGINT) AS work_id, CAST(NULL AS STRING) AS title, CAST(NULL AS INT) AS year, CAST(NULL AS STRING) AS volume,
  CAST(NULL AS STRING) AS first_page, CAST(NULL AS STRING) AS output_type, CAST(NULL AS ARRAY<STRING>) AS surnames, object
FROM (SELECT doi, explode(anr_grants) AS grant_id, _metadata.file_path AS object
      FROM read_files('s3://openalex-ingest/awards/anr/anr_work_links.parquet', format => 'parquet'))
WHERE trim(grant_id) <> ''
UNION ALL
SELECT funder_award_id, CAST(NULL AS BIGINT), concat(coalesce(hal_id, ''), '/', work_id), CAST(NULL AS STRING), CAST(NULL AS STRING), work_id,
  CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>),
  'openalex.awards.hal_anr_work_links'
FROM openalex.awards.hal_anr_work_links;

-- COMMAND ----------

-- KAKEN: products_json lists each project's research products (the KAKEN API fetch): the DOI, and for the refmatch tier a
-- journal article's title and year (KAKEN lists no authors; other product types stay without output_type, so are never
-- matched). Until the version-DOI fallback, the 8 Jul links are kept (their scrape is gone) except on a paper with no DOI now.
CREATE OR REPLACE TEMP VIEW kaken_listed AS
WITH objects AS (
  SELECT project_id, products_json, _metadata FROM read_files('s3://openalex-ingest/awards/kaken_api/*.parquet', format => 'parquet')
),
products AS (
  SELECT project_id, _metadata.file_path AS object, posexplode(from_json(products_json, 'array<map<string,string>>')) AS (i, p)
  FROM objects
  WHERE _metadata.file_path = (SELECT max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path)) FROM objects)
)
SELECT project_id AS award_id, CAST(NULL AS BIGINT) AS funder_id, CAST(i AS STRING) AS output_key, p['doi'] AS doi,
  CAST(NULL AS STRING) AS pmid, CAST(NULL AS BIGINT) AS work_id,
  CASE WHEN p['type'] = 'journal_article' THEN nullif(trim(p['title']), '') END AS title, try_cast(p['year'] AS INT) AS year,
  CAST(NULL AS STRING) AS volume, CAST(NULL AS STRING) AS first_page,
  CASE WHEN p['type'] = 'journal_article' THEN 'article' END AS output_type, CAST(array() AS ARRAY<STRING>) AS surnames, object
FROM products
UNION ALL
SELECT award_id, CAST(NULL AS BIGINT), concat('W', k.work_id), CAST(NULL AS STRING), CAST(NULL AS STRING), k.work_id, CAST(NULL AS STRING),
  CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>),
  'openalex.awards.kaken_work_funders'
FROM openalex.awards.kaken_work_funders k LATERAL VIEW explode(k.award_ids) t AS award_id
WHERE NOT EXISTS (SELECT 1 FROM openalex.works.openalex_works w WHERE w.id = k.work_id AND w.doi IS NULL);

-- COMMAND ----------

-- Hakai: the DOIs on its own publications list (hakai_outputs_to_s3.py). It names no grants, only the funder.
CREATE OR REPLACE TEMP VIEW hakai_publications_listed AS
SELECT CAST(NULL AS STRING) AS award_id, CAST(funder_id AS BIGINT) AS funder_id, doi AS output_key, doi, CAST(NULL AS STRING) AS pmid,
  CAST(NULL AS BIGINT) AS work_id, CAST(NULL AS STRING) AS title, CAST(NULL AS INT) AS year, CAST(NULL AS STRING) AS volume, CAST(NULL AS STRING) AS first_page,
  CAST(NULL AS STRING) AS output_type, CAST(NULL AS ARRAY<STRING>) AS surnames, _metadata.file_path AS object
FROM read_files('s3://openalex-ingest/awards/hakai/hakai_outputs.parquet', format => 'parquet');

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW link_listed AS
SELECT 'nwopen' AS source, * FROM nwopen_listed
UNION ALL SELECT 'fwf_open_api', * FROM fwf_open_api_listed
UNION ALL SELECT 'anr_opendata', * FROM anr_opendata_listed
UNION ALL SELECT 'kaken', * FROM kaken_listed
UNION ALL SELECT 'hakai_publications', * FROM hakai_publications_listed;

-- COMMAND ----------

-- 1. What each configured source lists, read once.
CREATE OR REPLACE TABLE openalex.awards.funder_reported_link_matches USING delta AS
SELECT l.source, l.award_id, l.output_key, l.funder_id, l.doi, l.pmid, l.work_id, l.title, match_text(l.title) AS title_key, l.year,
  nullif(regexp_extract(l.volume, '(\\d+)', 1), '') AS volume, nullif(regexp_extract(l.first_page, '(\\d+)', 1), '') AS first_page,
  l.output_type, l.surnames, l.object
FROM link_listed l LEFT SEMI JOIN link_sources s ON s.source = l.source;

-- COMMAND ----------

-- 2. Which works each listed output is. The table is replaced by its listed outputs with their candidates and evidence.
CREATE OR REPLACE TABLE openalex.awards.funder_reported_link_matches USING delta AS
WITH
rules AS (
  SELECT 20 AS min_title_chars, 1 AS max_year_gap, 50 AS max_same_title_works, 0.2 AS max_title_distance, 100 AS max_authors_checked,
    array('dataset', 'paratext', 'erratum', 'retraction', 'peer-review', 'supplementary-materials', 'conference-abstract') AS held_work_types
),
listed AS (
  SELECT m.*, s.check, concat(split(s.tiers, ','), array('refmatch')) AS tiers,   -- refmatch: on for every source
    nullif(regexp_extract(lower(regexp_replace(m.doi, '\\s+', '')), '(10\\.[0-9]{2,}/[^\\s"<>]+)', 1), '') AS doi_salvaged
  FROM openalex.awards.funder_reported_link_matches m JOIN link_sources s ON s.source = m.source
),
title_listed AS (
  SELECT l.* FROM listed l CROSS JOIN rules r WHERE array_contains(l.tiers, 'title') AND length(l.title_key) >= r.min_title_chars
),
refmatch_picks AS (
  -- the matcher's linked pick for each output as listed (the latest, if it decided it twice); a pick merged into another
  -- work since moves to the survivor, which must still pass the title guard
  SELECT p.source, p.listed_key, p.guard_key, coalesce(m.winner_work_id, p.work_id) AS work_id, p.score
  FROM (SELECT * FROM openalex.awards.funder_reported_refmatch_picks WHERE linked
        QUALIFY row_number() OVER (PARTITION BY source, listed_key ORDER BY decided_at DESC) = 1) p
  LEFT JOIN (SELECT loser_work_id, max(winner_work_id) AS winner_work_id FROM openalex.works.merged_work_ids GROUP BY 1) m
    ON m.loser_work_id = p.work_id
),
title_works AS (
  -- works with a listed title (location_work_match_keys.nt), unless more than max_same_title_works share it
  SELECT k.id, k.yr, k.nt
  FROM openalex.works.location_work_match_keys k LEFT SEMI JOIN title_listed l ON l.title_key = k.nt
  QUALIFY count(*) OVER (PARTITION BY k.nt) <= (SELECT max_same_title_works FROM rules)
),
listed_ids AS (
  -- each output's identifiers for its tiers. A DOI only when the value holds one: salvaged by regex, and also without URL
  -- leftovers (some works hold the salvaged spelling: '+' and '%28' are DOI characters). Work: the work id the list names.
  -- Title: the works with that title a year apart at most. Refmatch: the matcher's pick for the output as it is listed now.
  SELECT source, award_id, output_key, check, tiers, x.tier, x.id, array_sort(collect_set(x.spelling)) AS matched_by
  FROM listed LATERAL VIEW explode(array(
    named_struct('tier', 'doi', 'spelling', 'salvaged', 'id', concat('https://doi.org/', doi_salvaged)),
    named_struct('tier', 'doi', 'spelling', 'cleaned', 'id', concat('https://doi.org/', nullif(regexp_replace(regexp_replace(regexp_replace(
      try_url_decode(doi_salvaged), '[?#].*$', ''), '[.,:\\x3B]+$', ''), '/(full|abstract|pdf|epdf|html)$', ''), ''))),
    named_struct('tier', 'pmid', 'spelling', 'pmid', 'id', nullif(regexp_extract(trim(pmid), '(\\d+)$', 1), '')),
    named_struct('tier', 'work', 'spelling', 'work', 'id', CAST(work_id AS STRING)))) t AS x
  WHERE x.id IS NOT NULL AND array_contains(tiers, x.tier)
  GROUP BY source, award_id, output_key, check, tiers, x.tier, x.id
  UNION ALL
  SELECT l.source, l.award_id, l.output_key, l.check, l.tiers, 'title', CAST(k.id AS STRING), array('title')
  FROM title_listed l JOIN title_works k ON k.nt = l.title_key CROSS JOIN rules r
  WHERE abs(k.yr - l.year) <= r.max_year_gap
  UNION ALL
  SELECT l.source, l.award_id, l.output_key, l.check, l.tiers, 'refmatch', CAST(p.work_id AS STRING), array('refmatch')
  FROM listed l JOIN refmatch_picks p ON p.source = l.source AND p.listed_key = listed_key(l.title, l.surnames)
  WHERE array_contains(l.tiers, 'refmatch')
),
works AS (
  -- openalex_works, read once: the works an identifier or title points at, under each key they can be found by
  SELECT w.id AS work_id, k.tier, k.id, match_text(w.title) AS title_key, w.type, w.publication_year AS year,
    nullif(regexp_extract(w.biblio.volume, '(\\d+)', 1), '') AS volume, nullif(regexp_extract(w.biblio.first_page, '(\\d+)', 1), '') AS first_page,
    IF(size(w.authorships) <= r.max_authors_checked, w.authorships.author.display_name, NULL) AS author_names
  FROM (SELECT id, doi, regexp_extract(ids['pmid'], '(\\d+)$', 1) AS pmid, title, type, publication_year, biblio, authorships
        FROM openalex.works.openalex_works
        WHERE lower(doi) IN (SELECT id FROM listed_ids WHERE tier = 'doi')
           OR regexp_extract(ids['pmid'], '(\\d+)$', 1) IN (SELECT id FROM listed_ids WHERE tier = 'pmid')
           OR id IN (SELECT CAST(id AS BIGINT) FROM listed_ids WHERE tier IN ('title', 'work', 'refmatch'))) w
  CROSS JOIN rules r
  LATERAL VIEW explode(array(named_struct('tier', 'doi', 'id', lower(w.doi)), named_struct('tier', 'pmid', 'id', w.pmid),
    named_struct('tier', 'title', 'id', CAST(w.id AS STRING)), named_struct('tier', 'work', 'id', CAST(w.id AS STRING)),
    named_struct('tier', 'refmatch', 'id', CAST(w.id AS STRING)))) t AS k
),
candidates AS (
  -- a work the list names is a candidate even when it is merged away (Nightly moves the link to the survivor)
  SELECT i.source, i.award_id, i.output_key, i.check, i.tiers, i.tier, i.matched_by, coalesce(w.work_id, CAST(i.id AS BIGINT)) AS work_id,
    w.title_key, w.type, w.year, w.volume, w.first_page, w.author_names
  FROM listed_ids i LEFT JOIN works w ON w.tier = i.tier AND w.id = i.id
  WHERE w.work_id IS NOT NULL OR i.tier = 'work'
),
listed_names AS (
  -- the last word of each listed surname ('van der Linden' -> linden)
  SELECT DISTINCT source, award_id, output_key, name
  FROM (SELECT source, award_id, output_key, element_at(split(match_text(s), ' '), -1) AS name FROM listed LATERAL VIEW explode(surnames) t AS s)
  WHERE length(name) >= 2
),
candidate_names AS (
  -- an author's last name: the last word before a comma ('Wolf, M.'), else the last word
  SELECT source, award_id, output_key, work_id, element_at(split(match_text(IF(n LIKE '%,%', split_part(n, ',', 1), n)), ' '), -1) AS name
  FROM candidates LATERAL VIEW explode(author_names) t AS n
  WHERE check <> 'none'
),
author_matches AS (
  -- explode and join: a listed surname is the last name of one of the work's authors
  SELECT DISTINCT c.source, c.award_id, c.output_key, c.work_id
  FROM candidate_names c
  JOIN listed_names l ON l.source = c.source AND l.award_id <=> c.award_id AND l.output_key = c.output_key AND l.name = c.name
),
title_picks AS (
  -- the title tier names one work: the only candidate, or the only one sharing a listed author
  SELECT c.source, c.award_id, c.output_key,
    CASE WHEN count(*) = 1 THEN max(c.work_id) WHEN count(a.work_id) = 1 THEN max(a.work_id) END AS work_id
  FROM candidates c
  LEFT JOIN author_matches a ON a.source = c.source AND a.award_id <=> c.award_id AND a.output_key = c.output_key AND a.work_id = c.work_id
  WHERE c.tier = 'title'
  GROUP BY c.source, c.award_id, c.output_key
),
decided AS (
  -- the first tier with a candidate decides: no fall-through
  SELECT c.*
  FROM candidates c
  LEFT JOIN title_picks p ON p.source = c.source AND p.award_id <=> c.award_id AND p.output_key = c.output_key
  WHERE c.tier <> 'title' OR c.work_id = p.work_id
  QUALIFY rank() OVER (PARTITION BY c.source, c.award_id, c.output_key ORDER BY array_position(c.tiers, c.tier)) = 1
),
judged AS (
  SELECT l.source, l.award_id, l.output_key, l.check, l.funder_id AS listed_funder_id, l.doi, l.pmid, l.title, l.title_key, l.year, l.volume,
    l.first_page, l.output_type, l.surnames, l.object, d.work_id, d.tier, d.matched_by, d.type AS work_type,
    levenshtein(substr(d.title_key, 1, 200), substr(l.title_key, 1, 200))
      / greatest(length(substr(d.title_key, 1, 200)), length(substr(l.title_key, 1, 200)), 1) AS title_distance,
    coalesce(d.title_key = l.title_key, false) AS title_equal,
    a.work_id IS NOT NULL AS author_on_work,
    coalesce(d.year = l.year OR d.volume = l.volume OR d.first_page = l.first_page, false) AS year_or_numbers_agree,
    p.score AS refmatch_score, coalesce(cjk_key(d.title_key) = cjk_key(p.guard_key), false) AS refmatch_title_equal,
    -- a list without authors (KAKEN) keys a pick on the title alone: two papers with one title in two projects share it,
    -- so such a pick also needs the listed year (where both are known)
    size(coalesce(l.surnames, array())) > 0 OR l.year IS NULL OR d.year IS NULL OR abs(d.year - l.year) <= 1 AS refmatch_year_ok
  FROM listed l
  LEFT JOIN decided d ON d.source = l.source AND d.award_id <=> l.award_id AND d.output_key = l.output_key
  LEFT JOIN author_matches a ON a.source = d.source AND a.award_id <=> d.award_id AND a.output_key = d.output_key AND a.work_id = d.work_id
  LEFT JOIN refmatch_picks p ON d.tier = 'refmatch' AND p.source = d.source AND p.listed_key = listed_key(l.title, l.surnames)
    AND p.work_id = d.work_id
),
awards AS (
  SELECT DISTINCT provenance AS source, funder_award_id AS award_id, funder_id
  FROM openalex.awards.openalex_awards_raw
  WHERE provenance IN (SELECT source FROM link_sources)
),
linked AS (
  -- a link naming a grant takes its award's funder in raw, and none when the grant is not an award there
  SELECT j.*, IF(j.award_id IS NULL, j.listed_funder_id, a.funder_id) AS funder_id
  FROM judged j LEFT JOIN awards a ON a.source = j.source AND a.award_id = j.award_id
)
SELECT x.source, x.award_id, x.output_key, x.funder_id, x.doi, x.pmid, x.title, x.title_key, x.year, x.volume, x.first_page,
  x.output_type, x.surnames, x.object, x.work_id, x.tier, x.matched_by, x.work_type, x.title_distance, x.title_equal,
  x.author_on_work, x.year_or_numbers_agree, x.refmatch_score, x.refmatch_title_equal,
  -- a refmatch pick needs the title guard, whatever the source's check, and is held back like a title link when it is a
  -- dataset-like or abstract record while the funder lists an article, book or thesis
  x.work_id IS NOT NULL AND x.funder_id IS NOT NULL AND CASE WHEN x.tier = 'refmatch' THEN x.refmatch_title_equal AND x.refmatch_year_ok
      AND NOT coalesce(array_contains(r.held_work_types, x.work_type) AND x.output_type IN ('article', 'book', 'thesis'), false)
    ELSE x.check = 'none' OR coalesce(CASE x.tier
    WHEN 'title' THEN x.title_equal AND x.author_on_work AND x.year_or_numbers_agree
      AND NOT (array_contains(r.held_work_types, x.work_type) AND coalesce(x.output_type IN ('article', 'book', 'thesis'), false))
    ELSE x.title_distance <= r.max_title_distance OR x.author_on_work END, false) END AS accepted
FROM linked x CROSS JOIN rules r;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW link_rows AS
SELECT source, work_id, funder_id, array_sort(collect_set(award_id)) AS award_ids
FROM openalex.awards.funder_reported_link_matches
WHERE accepted
GROUP BY source, work_id, funder_id;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE link_results ARRAY<STRUCT<source: STRING, object: STRING, listed: BIGINT, resolved: BIGINT, award_ids: BIGINT,
  award_ids_in_raw: BIGINT, links: BIGINT, links_now: BIGINT, state: STRING>>;

-- COMMAND ----------

-- Gates, frozen before the write (views are lazy, and shrink and growth read the table the MERGE changes). A link is a
-- (work, funder, grant id), or a (work, funder) for a source that lists no grants.
SET VAR link_results = (
  WITH listed AS (
    SELECT source, max(object) AS object, count(DISTINCT struct(award_id, output_key)) AS listed,
      count(DISTINCT IF(work_id IS NOT NULL, struct(award_id, output_key), NULL)) AS resolved, count(DISTINCT award_id) AS award_ids,
      count(DISTINCT IF(funder_id IS NOT NULL, award_id, NULL)) AS award_ids_in_raw
    FROM openalex.awards.funder_reported_link_matches
    GROUP BY source
  ),
  links AS (SELECT source, sum(greatest(size(award_ids), 1)) AS links FROM link_rows GROUP BY source),
  links_now AS (SELECT source, sum(greatest(size(award_ids), 1)) AS links_now FROM openalex.awards.funder_reported_links GROUP BY source),
  counts AS (
    SELECT s.source, m.object, coalesce(m.listed, 0) AS listed, coalesce(m.resolved, 0) AS resolved, coalesce(m.award_ids, 0) AS award_ids,
      coalesce(m.award_ids_in_raw, 0) AS award_ids_in_raw, coalesce(l.links, 0) AS links, coalesce(n.links_now, 0) AS links_now,
      o.source IS NOT NULL AS overridden
    FROM link_sources s LEFT JOIN listed m USING (source) LEFT JOIN links l USING (source) LEFT JOIN links_now n USING (source)
    LEFT JOIN (SELECT DISTINCT source FROM openalex.awards.funder_reported_links_gate_overrides WHERE run_date = current_date()) o
      USING (source)
  )
  SELECT array_sort(collect_list(struct(source, object, listed, resolved, award_ids, award_ids_in_raw, links, links_now, CASE
    WHEN listed = 0 OR award_ids_in_raw < 0.99 * award_ids THEN 'kept: coverage'
    WHEN overridden THEN 'written'
    WHEN links < 0.95 * links_now THEN 'kept: shrink'
    WHEN links_now > 0 AND links > 1.25 * links_now THEN 'kept: growth'
    ELSE 'written' END AS state)))
  FROM counts
);

-- COMMAND ----------

SELECT inline(session.link_results);

-- COMMAND ----------

-- One write for every source. A source that failed a gate keeps its links. A source dropped from link_sources loses its links.
MERGE INTO openalex.awards.funder_reported_links t
USING (SELECT * FROM link_rows WHERE exists(session.link_results, r -> r.source = link_rows.source AND r.state = 'written')) s
ON t.source = s.source AND t.work_id = s.work_id AND t.funder_id = s.funder_id
WHEN MATCHED AND t.award_ids <> s.award_ids THEN UPDATE SET award_ids = s.award_ids
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND NOT exists(session.link_results, r -> r.source = t.source AND r.state <> 'written') THEN DELETE;

-- COMMAND ----------

SELECT CASE WHEN exists(session.link_results, r -> r.state <> 'written')
  THEN raise_error('LINKS_GATES: ' || concat_ws(', ', transform(filter(session.link_results, r -> r.state <> 'written'),
    r -> r.source || ' ' || r.state)) || '. Coverage: it lists no outputs or under 99% of its grant ids are awards in raw. '
    || 'Shrink, growth: its links fell under 95% or rose over 125% of the links it holds. Its links are kept as they were.')
  ELSE 'all sources written' END AS result;
