-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 7
INSERT INTO openalex.funders.funder_decision
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
SELECT ('fd' || 'cutover_20261011'), current_timestamp(), 'openalex_v41', CAST(s.funder_id AS STRING), s.d, s.funder_id, s.rid, v.crossref_id,
  v.display_name, v.country_code,
  to_json(named_struct('tier', s.t, 'best_tier', s.bt, 'keeper', s.keeper, 'merge_candidate', s.merge_candidate, 'works', l.works))
FROM shared_dec s JOIN v ON v.funder_id = s.funder_id JOIN live l ON l.funder_id = s.funder_id;
