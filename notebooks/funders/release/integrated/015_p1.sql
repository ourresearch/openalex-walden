-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 8
INSERT INTO openalex.funders.funder
WITH v AS (SELECT * FROM registry_seed_funders),
r AS (
  SELECT a.funder_id,
    CASE WHEN a.merge_into_id IS NULL THEN NULL WHEN b.merge_into_id IS NULL THEN a.merge_into_id
         WHEN c.merge_into_id IS NULL THEN b.merge_into_id WHEN d.merge_into_id IS NULL THEN c.merge_into_id END root
  FROM v a LEFT JOIN v b ON b.funder_id = a.merge_into_id LEFT JOIN v c ON c.funder_id = b.merge_into_id
  LEFT JOIN v d ON d.funder_id = c.merge_into_id
),
rorx AS (
  SELECT lower(regexp_extract(id, '([0-9a-z]{9})/*$', 1)) rid,
    array_distinct(filter(transform(flatten(transform(filter(coalesce(external_ids, array()), e -> lower(e.type) = 'fundref'),
      e -> coalesce(e.all, array()))), x -> regexp_extract(trim(x), '^(?:(?:https?://)?(?:dx\\.)?(?:doi\\.org/)?10\\.13039/)?([0-9]{5,})$', 1)), x -> x <> '')) fundrefs,
    array_distinct(filter(transform(coalesce(transform(filter(coalesce(names, array()), n -> NOT array_contains(n.types, 'acronym')), n -> n.value), array()), x -> regexp_replace(trim(regexp_replace(lower(x), '[^\\p{L}\\p{N}]+', ' ')), '^the ', '')), x -> length(x) >= 4)) nk
  FROM openalex.institutions.ror
),
live AS (
  SELECT v.funder_id, CAST(v.crossref_id AS STRING) fr, lower(regexp_extract(v.ror_id, '([0-9a-z]{9})/*$', 1)) rid, regexp_replace(trim(regexp_replace(lower(v.display_name), '[^\\p{L}\\p{N}]+', ' ')), '^the ', '') dn, coalesce(w.works_count, 0) works,
    array_distinct(filter(transform(coalesce(concat(array(v.display_name), coalesce(from_json(v.alternate_titles, 'array<string>'), array())), array()), x -> regexp_replace(trim(regexp_replace(lower(x), '[^\\p{L}\\p{N}]+', ' ')), '^the ', '')), x -> length(x) >= 4)) nk
  FROM v LEFT JOIN (SELECT id, works_count FROM openalex.funders.funders_api) w ON w.id = v.funder_id
  WHERE v.merge_into_id IS NULL AND nullif(v.ror_id, '') IS NOT NULL
),
shared AS (SELECT rid FROM live GROUP BY rid HAVING count(*) > 1),
mem AS (
  SELECT l.*, coalesce(true AND array_contains(x.fundrefs, l.fr), false) in_xw, coalesce(array_contains(x.nk, l.dn), false) exact,
    coalesce(arrays_overlap(l.nk, x.nk), false) name_ok
  FROM live l JOIN shared s ON s.rid = l.rid LEFT JOIN rorx x ON x.rid = l.rid
),
tier AS (SELECT *, CASE WHEN in_xw AND exact THEN 1 WHEN in_xw AND name_ok THEN 2 WHEN in_xw THEN 3 WHEN exact THEN 4
                        WHEN name_ok THEN 5 ELSE 9 END t FROM mem),
best AS (SELECT rid, min(t) bt FROM tier GROUP BY rid),
keep AS (SELECT t.rid, max_by(t.funder_id, t.works * 10000000000 - t.funder_id) keeper, count(*) n_at, max(b.bt) bt
         FROM tier t JOIN best b ON b.rid = t.rid AND t.t = b.bt WHERE b.bt < 9 GROUP BY t.rid),
shared_dec AS (
  SELECT m.funder_id, m.rid, m.t, k.keeper, coalesce(k.bt, 9) bt,
    CASE WHEN (k.n_at = 1 OR k.bt IN (1, 4)) AND m.funder_id = k.keeper THEN 'PRIMARY_SHARED_ROR'
         WHEN k.n_at = 1 OR k.bt IN (1, 4) THEN 'SECONDARY_SHARED_ROR' ELSE 'NO_ATTACH_SHARED_ROR_TIE' END d,
    coalesce((k.n_at = 1 OR k.bt IN (1, 4)) AND m.funder_id <> k.keeper AND m.exact, false) merge_candidate
  FROM tier m LEFT JOIN keep k ON k.rid = m.rid
)
SELECT v.funder_id, CASE WHEN v.merge_into_id IS NULL THEN 'active' ELSE 'merged' END, r.root,
  nullif(v.ror_id, ''),   -- every funder keeps the ROR it has; only the primary one resolves the ROR id
  CASE WHEN nullif(v.ror_id, '') IS NULL OR v.merge_into_id IS NOT NULL THEN NULL WHEN s.d IN ('SECONDARY_SHARED_ROR', 'NO_ATTACH_SHARED_ROR_TIE') THEN false ELSE true END,
  CAST(NULL AS BIGINT),
  v.display_name, from_json(v.alternate_titles, 'array<string>'), v.country_code, v.homepage_url, v.wikidata_id, v.description,
  v.image_url, v.image_thumbnail_url, coalesce(try_cast(substr(v.created_date, 1, 19) AS TIMESTAMP), v.updated_date), v.updated_date,
  'seed_v41', ('fd' || 'cutover_20261011'), CAST(NULL AS STRING)
FROM v JOIN r ON r.funder_id = v.funder_id LEFT JOIN shared_dec s ON s.funder_id = v.funder_id
UNION ALL
SELECT funder_id, 'withdrawn', CAST(NULL AS BIGINT), nullif(ror_id, ''), CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), get_json_object(before_image_json, '$.display_name'),
  from_json(get_json_object(before_image_json, '$.alternate_titles'), 'array<string>'), get_json_object(before_image_json, '$.country_code'), get_json_object(before_image_json, '$.homepage_url'), get_json_object(before_image_json, '$.wikidata_id'),
  get_json_object(before_image_json, '$.description'), get_json_object(before_image_json, '$.image_url'), get_json_object(before_image_json, '$.image_thumbnail_url'), try_cast(substr(get_json_object(before_image_json, '$.created_date'), 1, 19) AS TIMESTAMP),
  deleted_at, 'seed_deleted', ('fd' || 'cutover_20261011'), CAST(NULL AS STRING)
FROM openalex.funders.deleted_funders WHERE funder_id NOT IN (SELECT funder_id FROM registry_seed_funders);
