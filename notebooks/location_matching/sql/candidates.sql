-- Match_Locations_To_Works (oxjob #1668): the seeds' blocking features and their SQL-channel candidates (#1577 keys: ids,
-- title key, MinHash bands, author + year; the title tier's own pick as `bound`) among the works in openalex_works __V__
-- (location_work_match_keys). Runs alongside the embedding search.
-- Seed records' blocking features, built like #1577 features.sql but from the record's locations_mapped row.
CREATE OR REPLACE TABLE openalex.works.location_work_match_seed_feat AS
WITH b AS (
  SELECT s.seed_id, year(l.published_date) AS yr,
    trim(regexp_replace(lower(translate(regexp_replace(coalesce(l.title, ''), '<[^>]+>', ' '),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}\\p{N}]+', ' ')) AS nt,
    lower(translate(coalesce(nullif(concat_ws(', ', get(l.authors, 0).family, get(l.authors, 0).given), ''), get(l.authors, 0).name),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ',
      'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')) AS a0,
    l.ids, l.landing_page_url, l.pdf_url, l.provenance, l.native_id
  FROM openalex.works.location_work_match_seeds s
  JOIN openalex.works.locations_mapped l
    ON l.provenance = s.provenance AND l.native_id_namespace = s.native_id_namespace AND l.native_id = s.native_id
  QUALIFY row_number() OVER (PARTITION BY s.seed_id ORDER BY l.openalex_updated_dt DESC) = 1
),
n AS (
  SELECT *,
    regexp_replace(CASE WHEN a0 LIKE '%,%' THEN split_part(a0, ',', 1) ELSE element_at(split(trim(a0), ' +'), -1) END, '[^\\p{L}]', '') AS sur,
    substr(regexp_replace(CASE WHEN a0 LIKE '%,%' THEN split_part(a0, ',', 2) ELSE element_at(split(trim(a0), ' +'), 1) END, '[^\\p{L}]', ''), 1, 1) AS ini,
    filter(array_distinct(split(nt, ' ')), x -> length(x) >= 3 AND NOT array_contains(array(
      'the','and','for','with','from','into','its','are','was','were','that','this','des','les','und','der','die','das',
      'von','del','los','las','une','por','para','con','dans','sur','aux','ein','eine','den','het','een','van','som','och'), x)) AS mt
  FROM b
),
h AS (SELECT *, transform(sequence(0, 11), k -> array_min(transform(mt, t -> xxhash64(t, k)))) AS mh FROM n)
SELECT seed_id, yr, nullif(nt, '') AS nt,
  CASE WHEN length(sur) >= 2 AND length(ini) = 1 THEN concat(ini, ' ', sur) END AS akey,
  CASE WHEN size(mt) >= 3 THEN transform(sequence(0, 5), i -> xxhash64(i, mh[2 * i], mh[2 * i + 1])) END AS bands,
  array_distinct(filter(concat(
    transform(filter(ids, i -> i.namespace = 'doi' AND i.relationship = 'self'), i -> concat('d:', lower(i.id))),
    transform(filter(ids, i -> i.namespace = 'pmid'), i -> concat('p:', regexp_extract(i.id, '(\\d+)$', 1))),
    array(CASE
      WHEN lower(landing_page_url) RLIKE '^https?://(dx\\.)?doi\\.org/' THEN concat('d:', regexp_replace(lower(landing_page_url), '^https?://(dx\\.)?doi\\.org/', ''))
      ELSE concat('u:', regexp_replace(regexp_replace(lower(landing_page_url), '^https?://(www\\.)?', ''), '/+$', '')) END,
      concat('u:', regexp_replace(regexp_replace(lower(pdf_url), '^https?://(www\\.)?', ''), '/+$', '')),
      CASE WHEN provenance NOT IN ('crossref', 'datacite') THEN concat('n:', lower(native_id)) END)
  ), x -> x IS NOT NULL AND x NOT IN ('d:', 'p:', 'u:', 'n:'))) AS keys
FROM h
;
-- oxjob #1668 candidate works for every seed record: #1577 cands.sql channels against openalex_works v17025 features,
-- plus `bound` (the work a title_bind seed was bound to, always included).
--   id: shares a DOI / PMID / URL / repository native id (key on <= 50 works)   top 20 by title Jaccard
--   ti: identical normalized title                                             top 20 by same author, then year gap
--   ay: same first-author key, year within 2                                   top 5 by title Jaccard (> 0)
--   mh: any MinHash band (band on <= 5000 works), Jaccard >= 0.4, year gap <= 3 top 5 by title Jaccard
CREATE OR REPLACE TABLE openalex.works.location_work_match_cands AS
WITH s AS (SELECT * FROM openalex.works.location_work_match_seed_feat),
F AS (SELECT * FROM openalex.works.location_work_match_keys),
sk AS (SELECT seed_id, k FROM s LATERAL VIEW explode(keys) t AS k),
ck AS (SELECT f.id AS cid, t.k FROM F f LATERAL VIEW explode(f.keys) t AS k WHERE t.k IN (SELECT k FROM sk)),
kn AS (SELECT k, count(DISTINCT cid) AS nk FROM ck GROUP BY k),
c_id AS (SELECT sk.seed_id, ck.cid, 'id' AS ch FROM sk JOIN ck ON ck.k = sk.k JOIN kn ON kn.k = sk.k WHERE kn.nk <= 50),
c_ti AS (SELECT s.seed_id, f.id AS cid, 'ti' AS ch FROM s JOIN F f ON f.nt = s.nt),
c_ay AS (SELECT s.seed_id, f.id AS cid, 'ay' AS ch FROM s JOIN F f ON f.akey = s.akey AND abs(f.yr - s.yr) <= 2),
sb AS (SELECT seed_id, b FROM s LATERAL VIEW explode(bands) t AS b),
cb AS (SELECT f.id AS cid, t.b FROM F f LATERAL VIEW explode(f.bands) t AS b WHERE t.b IN (SELECT b FROM sb)),
bn AS (SELECT b, count(*) AS nb FROM cb GROUP BY b),
c_mh AS (SELECT sb.seed_id, cb.cid, 'mh' AS ch FROM sb JOIN cb ON cb.b = sb.b JOIN bn ON bn.b = sb.b WHERE bn.nb <= 5000),
c_bd AS (SELECT seed_id, work_id AS cid, 'bound' AS ch FROM openalex.works.location_work_match_seeds WHERE decision = 'title_bind'),
u AS (SELECT * FROM c_id UNION SELECT * FROM c_ti UNION SELECT * FROM c_ay UNION SELECT * FROM c_mh UNION SELECT * FROM c_bd),
x AS (
  SELECT u.seed_id, u.cid, u.ch,
    a.akey = b.akey AS same_akey, abs(a.yr - b.yr) AS yr_gap, a.nt = b.nt AS same_nt,
    size(array_intersect(a.tok, b.tok)) / greatest(size(array_union(a.tok, b.tok)), 1) AS jacc,
    size(array_intersect(a.keys, b.keys)) AS shared_keys
  FROM u
  JOIN (SELECT seed_id, akey, yr, nt, keys, filter(array_distinct(split(nt, ' ')), z -> length(z) >= 2) AS tok FROM s) a ON a.seed_id = u.seed_id
  LEFT JOIN (SELECT id, akey, yr, nt, keys, filter(array_distinct(split(nt, ' ')), z -> length(z) >= 2) AS tok FROM F
             WHERE id IN (SELECT cid FROM u)) b ON b.id = u.cid
),
r AS (
  SELECT *, row_number() OVER (PARTITION BY seed_id, ch ORDER BY
      CASE ch WHEN 'ti' THEN CAST(coalesce(same_akey, false) AS INT) ELSE jacc END DESC, coalesce(yr_gap, 99), cid) AS rk
  FROM x
  WHERE (ch <> 'ay' OR jacc > 0) AND (ch <> 'mh' OR (jacc >= 0.4 AND coalesce(yr_gap, 0) <= 3))
)
SELECT seed_id, cid, sort_array(collect_set(ch)) AS channels,
  first(same_akey) AS same_akey, first(yr_gap) AS yr_gap, first(same_nt) AS same_nt, first(jacc) AS jacc,
  first(shared_keys) AS shared_keys, min(rk) AS best_rank
FROM r
WHERE rk <= CASE ch WHEN 'id' THEN 20 WHEN 'ti' THEN 20 WHEN 'bound' THEN 1 ELSE 5 END
GROUP BY seed_id, cid
;
SELECT count(DISTINCT seed_id) AS seeds_with_candidates, count(*) AS pairs FROM openalex.works.location_work_match_cands
