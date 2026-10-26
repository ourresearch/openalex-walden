-- Match_Locations_To_Works (oxjob #1668): blocking keys for every work in openalex_works __V__ (the last version before the
-- run), = #1577's features.sql (oxjobs duplicate-miss-rate-eval/work).
-- nt: normalized title (markup stripped, common accents folded, letters/digits only)
-- akey: first-author "initial surname"; bands: 6 MinHash bands (2 rows each) over title tokens >= 3 chars minus stopwords
-- keys: identity keys of the work and all its locations (d: DOI, p: PMID, u: URL, n: repository native id)
CREATE OR REPLACE TABLE openalex.works.location_work_match_keys AS
WITH b AS (
  SELECT id, publication_year AS yr,
    trim(regexp_replace(lower(translate(regexp_replace(coalesce(title, ''), '<[^>]+>', ' '),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')) AS nt,
    lower(translate(get(authorships, 0).author.display_name,
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')) AS a0,
    ids, locations
  FROM openalex.works.openalex_works VERSION AS OF __V__
),
n AS (
  SELECT id, yr, nt, ids, locations,
    regexp_replace(CASE WHEN a0 LIKE '%,%' THEN split_part(a0, ',', 1) ELSE element_at(split(trim(a0), ' +'), -1) END, '[^\\p{L}]', '') AS sur,
    substr(regexp_replace(CASE WHEN a0 LIKE '%,%' THEN split_part(a0, ',', 2) ELSE element_at(split(trim(a0), ' +'), 1) END, '[^\\p{L}]', ''), 1, 1) AS ini,
    filter(array_distinct(split(nt, ' ')), x -> length(x) >= 3 AND NOT array_contains(array(
      'the','and','for','with','from','into','its','are','was','were','that','this','des','les','und','der','die','das',
      'von','del','los','las','une','por','para','con','dans','sur','aux','ein','eine','den','het','een','van','som','och'), x)) AS mt
  FROM b
),
h AS (
  SELECT id, yr, nt, sur, ini, ids, locations, mt,
    transform(sequence(0, 11), k -> array_min(transform(mt, t -> xxhash64(t, k)))) AS mh
  FROM n
)
SELECT id, yr, nullif(nt, '') AS nt,
  CASE WHEN length(sur) >= 2 AND length(ini) = 1 THEN concat(ini, ' ', sur) END AS akey,
  CASE WHEN size(mt) >= 3 THEN transform(sequence(0, 5), i -> xxhash64(i, mh[2 * i], mh[2 * i + 1])) END AS bands,
  array_distinct(filter(concat(
    array(concat('d:', lower(ids['doi'])), concat('p:', regexp_extract(ids['pmid'], '(\\d+)$', 1))),
    transform(locations, l -> CASE
      WHEN lower(l.landing_page_url) RLIKE '^https?://(dx\\.)?doi\\.org/' THEN concat('d:', regexp_replace(lower(l.landing_page_url), '^https?://(dx\\.)?doi\\.org/', ''))
      ELSE concat('u:', regexp_replace(regexp_replace(lower(l.landing_page_url), '^https?://(www\\.)?', ''), '/+$', '')) END),
    transform(locations, l -> concat('u:', regexp_replace(regexp_replace(lower(l.pdf_url), '^https?://(www\\.)?', ''), '/+$', ''))),
    transform(locations, l -> CASE WHEN l.provenance NOT IN ('crossref', 'datacite') THEN concat('n:', lower(l.native_id)) END)
  ), x -> x IS NOT NULL AND x NOT IN ('d:', 'p:', 'u:', 'n:'))) AS keys
FROM h
