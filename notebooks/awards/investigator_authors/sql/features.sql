-- Investigator authors (oxjob #1847): the model's features, one row per (entry, candidate) on 1-6 lists, as the registered model was
-- trained on them. The LISTWISE pin (the d_ and r_ columns) comes from iaa.listwise().
CREATE OR REPLACE TABLE __P__features AS
WITH sl AS (SELECT s.entry_id, s.author_id, s.n_short, e.award_id FROM __P__shortlist s JOIN entries e USING (entry_id)
            WHERE s.n_short BETWEEN 1 AND 6),
ent AS (SELECT * FROM entries WHERE entry_id IN (SELECT entry_id FROM sl)),
au AS (
  SELECT try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT) AS author_id, a.display_name,
    coalesce(a.display_name_alternatives, array()) AS alts,
    a.works_count, a.cited_by_count, coalesce(a.affiliations, array()) AS affs, coalesce(a.last_known_institutions, array()) AS lki,
    coalesce(a.counts_by_year, array()) AS cby
  FROM openalex.authors.authors_api a
  LEFT SEMI JOIN (SELECT DISTINCT author_id FROM sl) k ON k.author_id = try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT)
),
yr AS (  -- first and last publishing year (works with a title)
  SELECT au.author_id, min(w.publication_year) AS first_year, max(w.publication_year) AS last_year
  FROM openalex.works.work_authors au LEFT SEMI JOIN (SELECT DISTINCT author_id FROM sl) k ON k.author_id = au.author_id
  JOIN openalex.works.openalex_works w ON w.id = au.work_id
  WHERE coalesce(w.title, '') <> '' GROUP BY 1
),
ins AS (SELECT id, geo.geonames_city_id AS city, country_code AS cc,
          transform(coalesce(lineage, array()), l -> try_cast(regexp_extract(l, '([0-9]+)$', 1) AS BIGINT)) AS lin,
          transform(coalesce(associated_institutions, array()), x -> try_cast(regexp_extract(x.id, '([0-9]+)$', 1) AS BIGINT)) AS assoc
        FROM openalex.institutions.institutions_api),
ep AS (  -- the grant's places: its institutions, their ancestors and ROR relations, their cities and countries
  SELECT x.entry_id, first(x.insts) AS insts, array_distinct(flatten(collect_list(coalesce(i.lin, array())))) AS anc,
    array_distinct(flatten(collect_list(coalesce(i.assoc, array())))) AS assoc, collect_set(i.city) AS cities, collect_set(i.cc) AS ccs
  FROM (SELECT e.entry_id, e.insts, g FROM ent e LATERAL VIEW explode(e.insts) t AS g) x LEFT JOIN ins i ON i.id = x.g GROUP BY x.entry_id
),
af AS (  -- each candidate affiliation's relation to the grant: 5 exact, 4 child, 3 parent, 2 ROR related, 1 same city, 0.5 same country
  SELECT f.entry_id, f.author_id, f.i, f.ys,
    CASE WHEN array_contains(ep.insts, f.i) THEN 5.0 WHEN arrays_overlap(coalesce(i.lin, array()), ep.insts) THEN 4.0
         WHEN array_contains(ep.anc, f.i) THEN 3.0 WHEN array_contains(ep.assoc, f.i) THEN 2.0
         WHEN i.city IS NOT NULL AND array_contains(ep.cities, i.city) THEN 1.0 WHEN i.cc IS NOT NULL AND array_contains(ep.ccs, i.cc) THEN 0.5
         ELSE 0.0 END AS lvl
  FROM (SELECT s.entry_id, s.author_id, try_cast(regexp_extract(x.institution.id, '([0-9]+)$', 1) AS BIGINT) AS i, coalesce(x.years, array()) AS ys
        FROM sl s JOIN au a ON a.author_id = s.author_id LATERAL VIEW explode(a.affs) t AS x) f
  JOIN ep ON ep.entry_id = f.entry_id LEFT JOIN ins i ON i.id = f.i WHERE f.i IS NOT NULL
),
pl AS (
  SELECT a.entry_id, a.author_id,
    array_distinct(flatten(collect_list(CASE WHEN a.lvl = 5 THEN a.ys END))) AS ex_y,
    array_distinct(flatten(collect_list(CASE WHEN a.lvl = 4 THEN a.ys END))) AS ch_y,
    array_distinct(flatten(collect_list(CASE WHEN a.lvl >= 4 THEN a.ys END))) AS in_y,
    array_distinct(flatten(collect_list(CASE WHEN a.lvl BETWEEN 1 AND 3 THEN a.ys END))) AS ct_y,
    array_distinct(flatten(collect_list(a.ys))) AS all_y,
    count(DISTINCT CASE WHEN exists(a.ys, y -> y BETWEEN e.lo AND e.hi) THEN a.i END) AS n_insts_win
  FROM af a JOIN ent e ON e.entry_id = a.entry_id GROUP BY 1, 2
),
lk AS (
  SELECT q.entry_id, q.author_id, max(array_contains(ep.insts, q.x) OR arrays_overlap(coalesce(i.lin, array()), ep.insts)) AS lki_inst
  FROM (SELECT s.entry_id, s.author_id, x FROM sl s JOIN au a ON a.author_id = s.author_id
        LATERAL VIEW explode(transform(a.lki, z -> try_cast(regexp_extract(z.id, '([0-9]+)$', 1) AS BIGINT))) t AS x) q
  JOIN ep ON ep.entry_id = q.entry_id LEFT JOIN ins i ON i.id = q.x WHERE q.x IS NOT NULL GROUP BY 1, 2
),
nm AS (  -- every profile name (display name + all alternatives) against the investigator's tokens
  SELECT n.entry_id, n.author_id,
    max(length(e.g1) > 1 AND get(n.nt, 0) = e.g1) AS full_eq_any,
    max(length(get(n.nt, 0)) > 1 AND (get(n.nt, 0) = e.g1 OR (length(e.g1) = 1 AND left(get(n.nt, 0), 1) = e.g1))) AS cand_full_any,
    max(size(e.gt) >= 2 AND size(n.nt) >= 3 AND left(get(n.nt, 1), 1) = left(get(e.gt, 1), 1)) AS mid_any,
    max(array_contains(e.ft, try_element_at(n.nt, -1))) AS fam_seen
  FROM (SELECT s.entry_id, s.author_id, __P__name_tokens(x) AS nt FROM sl s JOIN au a ON a.author_id = s.author_id
        LATERAL VIEW explode(concat(array(a.display_name), a.alts)) t AS x) n
  JOIN (SELECT entry_id, gt, ft, get(gt, 0) AS g1 FROM ent) e ON e.entry_id = n.entry_id
  WHERE size(n.nt) >= 2 GROUP BY 1, 2
),
op AS (  -- other people on the award: distinct (last family token, initial) that are not this entry's
  SELECT e.entry_id,
    count(DISTINCT CASE WHEN NOT (k.kf = try_element_at(e.ft, -1) AND k.ki = left(e.given_key, 1)) THEN concat(k.kf, '|', k.ki) END) AS n
  FROM ent e LEFT JOIN (SELECT award_id, try_element_at(ft, -1) AS kf, coalesce(left(given_key, 1), '') AS ki FROM __P__people) k
    ON k.award_id = e.award_id GROUP BY 1
),
b AS (
  SELECT s.entry_id, s.author_id, CAST(s.n_short AS DOUBLE) AS n_short, CAST(count(*) OVER (PARTITION BY s.entry_id) AS DOUBLE) AS n_cands,
    __P__name_tokens(a.display_name) AS d, e.gt, e.ft, get(e.gt, 0) AS g1, size(a.alts) AS n_alts, pl.ex_y, pl.ch_y, pl.in_y, pl.ct_y, pl.all_y,
    pl.n_insts_win, e.lo, e.hi, coalesce(e.start_year, e.end_year - 5) AS s0, yr.first_year, yr.last_year, a.works_count, a.cited_by_count, a.cby,
    nm.full_eq_any, nm.cand_full_any, nm.mid_any, nm.fam_seen, lk.lki_inst, op.n AS n_other_people, fu.author_id IS NOT NULL AS funded
  FROM sl s JOIN ent e ON e.entry_id = s.entry_id LEFT JOIN au a ON a.author_id = s.author_id LEFT JOIN yr ON yr.author_id = s.author_id
  LEFT JOIN pl ON pl.entry_id = s.entry_id AND pl.author_id = s.author_id LEFT JOIN nm ON nm.entry_id = s.entry_id AND nm.author_id = s.author_id
  LEFT JOIN lk ON lk.entry_id = s.entry_id AND lk.author_id = s.author_id LEFT JOIN op ON op.entry_id = s.entry_id
  LEFT JOIN (SELECT DISTINCT award_id, author_id FROM __P__paper_authors) fu ON fu.award_id = s.award_id AND fu.author_id = s.author_id
),
f AS (
  SELECT entry_id, author_id, n_short, n_cands,
    CAST(length(g1) > 1 AND size(d) >= 2 AND get(d, 0) = g1 AS DOUBLE) AS nm_full_eq_disp,
    CAST(coalesce(full_eq_any, false) AS DOUBLE) AS nm_full_eq_any,
    CAST(length(g1) = 1 AS DOUBLE) AS nm_inv_initial, CAST(size(d) >= 2 AND length(get(d, 0)) = 1 AS DOUBLE) AS nm_cand_initial_disp,
    CAST(coalesce(cand_full_any, false) AS DOUBLE) AS nm_cand_full_any,
    CAST(CASE WHEN size(gt) >= 2 AND size(d) >= 3 THEN CASE WHEN left(get(d, 1), 1) = left(get(gt, 1), 1) THEN 1 ELSE -1 END
              WHEN size(gt) >= 2 AND coalesce(mid_any, false) THEN 1 ELSE 0 END AS DOUBLE) AS nm_mid,
    CAST(size(d) >= 2 AND try_element_at(d, -1) = try_element_at(ft, -1) AS DOUBLE) AS nm_fam_exact,
    CAST(size(ft) >= 2 AND size(array_except(ft, d)) = 0 AS DOUBLE) AS nm_fam_all_tokens,
    CAST(array_join(d, ' ') IN (array_join(concat(gt, ft), ' '), array_join(concat(ft, gt), ' ')) AS DOUBLE) AS nm_disp_exact,
    CAST(n_alts AS DOUBLE) AS nm_n_alts, CAST(NOT coalesce(fam_seen, false) AS DOUBLE) AS nm_swapped_only,
    CAST(size(filter(coalesce(ex_y, array()), y -> y BETWEEN lo AND hi)) AS DOUBLE) AS yrs_exact_win,
    CAST(size(filter(coalesce(ch_y, array()), y -> y BETWEEN lo AND hi)) AS DOUBLE) AS yrs_child_win,
    CAST(size(filter(coalesce(in_y, array()), y -> y BETWEEN lo AND hi)) AS DOUBLE) AS yrs_inst_win,
    CAST(size(coalesce(in_y, array())) AS DOUBLE) AS yrs_inst_any,
    CAST(size(coalesce(all_y, array())) AS DOUBLE) AS yrs_total,
    size(coalesce(in_y, array())) / greatest(1, size(coalesce(all_y, array()))) AS frac_inst,
    CAST(size(coalesce(ex_y, array())) > 0 AS DOUBLE) AS inst_exact_any,
    CAST(s0 IS NOT NULL AND exists(coalesce(in_y, array()), y -> abs(y - s0) <= 1) AS DOUBLE) AS at_inst_start,
    CAST(CASE WHEN size(coalesce(in_y, array())) > 0 THEN array_min(in_y) - s0 END AS DOUBLE) AS inst_first_minus_start,
    CAST(CASE WHEN size(coalesce(in_y, array())) > 0 THEN array_max(in_y) - s0 END AS DOUBLE) AS inst_last_minus_start,
    CAST(coalesce(n_insts_win, 0) AS DOUBLE) AS n_insts_win,
    CAST(size(filter(coalesce(ct_y, array()), y -> y BETWEEN lo AND hi)) AS DOUBLE) AS yrs_city_win,
    CAST(coalesce(lki_inst, false) AS DOUBLE) AS lki_inst, CAST(s0 - first_year AS DOUBLE) AS career_at_start,
    CAST(last_year - s0 AS DOUBLE) AS last_minus_start,
    CAST(s0 IS NOT NULL AND first_year IS NOT NULL AND first_year <= s0 + 2 AND last_year >= s0 - 2 AS DOUBLE) AS active_at_start,
    CAST(aggregate(filter(coalesce(cby, array()), c -> c.year BETWEEN lo AND hi), 0L,
                   (acc, c) -> acc + coalesce(c.works_count, 0)) AS DOUBLE) AS works_win,
    ln(1 + coalesce(works_count, 0)) AS works_log, ln(1 + coalesce(cited_by_count, 0)) AS cited_log,
    CAST(coalesce(n_other_people, 0) AS DOUBLE) AS n_other_people, CAST(funded AS DOUBLE) AS funded, ln(1 + n_short) AS n_short_log
  FROM b
)
SELECT f.*, __LISTWISE__ FROM f
