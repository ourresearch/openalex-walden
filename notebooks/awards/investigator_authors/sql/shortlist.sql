-- Investigator authors (oxjob #1847): entries and shortlists. An entry is one person on one award (a lead repeated in investigators[] is
-- asked once), scored when no slot of it has an orcid or linked_paper claim, it has a given name and an institution (on any of its
-- slots), and no field holds two people. Shortlist: a profile reading with one of the entry's family keys, a compatible first given
-- name, and an affiliation at the grant's institution or under it in a year of the window. A (family key, initial, institution) with
-- over 1,000 profiles is joined on neither side; its entries are kept as capped (too_many_candidates), never dropped.
CREATE OR REPLACE TEMPORARY VIEW entries AS
WITH lk AS (SELECT DISTINCT award_id, slot, position FROM __P__routes WHERE method IN ('orcid', 'linked_paper')),
pe AS (
  SELECT p.*, concat(p.award_id, '|', p.person_key) AS entry_id, lk.award_id IS NOT NULL AS linked,
    row_number() OVER (PARTITION BY p.award_id, p.person_key
                       ORDER BY CASE p.slot WHEN 'lead_investigator' THEN 0 WHEN 'co_lead_investigator' THEN 1 ELSE 2 END, p.position) AS rk
  FROM __P__people p LEFT JOIN lk ON lk.award_id = p.award_id AND lk.slot = p.slot AND lk.position = p.position
)
SELECT entry_id, award_id, funder_id, person_key, given_name, family_name, gt, ft, fam_keys, given_key, all_insts AS insts, lo, hi,
  start_year, end_year
FROM (SELECT pe.*, max(linked) OVER (PARTITION BY entry_id) AS any_linked, max(multi_name) OVER (PARTITION BY entry_id) AS any_multi,
        array_distinct(flatten(collect_list(insts) OVER (PARTITION BY entry_id))) AS all_insts FROM pe)
WHERE rk = 1 AND NOT any_linked AND NOT any_multi AND given_key IS NOT NULL AND size(all_insts) > 0
;
CREATE OR REPLACE TABLE __P__shortlist CLUSTER BY (entry_id) AS
WITH ek AS (
  SELECT DISTINCT e.entry_id, k AS fam, e.given_key AS giv, left(e.given_key, 1) AS gi, inst, e.lo, e.hi
  FROM entries e LATERAL VIEW explode(e.fam_keys) t AS k LATERAL VIEW explode(e.insts) t2 AS inst
),
ai AS (  -- every live profile's institutions (and their ancestors), with the years
  SELECT author_id, inst, array_distinct(flatten(collect_list(years))) AS years FROM (
    SELECT try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT) AS author_id,
      try_cast(regexp_extract(l, '([0-9]+)$', 1) AS BIGINT) AS inst, af.years
    FROM openalex.authors.authors_api a LATERAL VIEW explode(coalesce(a.affiliations, array())) t AS af
    LATERAL VIEW explode(coalesce(af.institution.lineage, array(af.institution.id))) t2 AS l
    WHERE a.works_count > 0 AND size(coalesce(af.years, array())) > 0)
  GROUP BY 1, 2
),
pk AS (
  SELECT n.author_id, n.last_key AS fam, n.first_key AS giv, left(n.first_key, 1) AS gi, ai.inst, ai.years
  FROM __P__profile_names n LEFT SEMI JOIN (SELECT DISTINCT fam FROM ek) f ON f.fam = n.last_key
  JOIN ai ON ai.author_id = n.author_id
  WHERE n.first_key IS NOT NULL
),
cap AS (SELECT fam, gi, inst FROM pk GROUP BY 1, 2, 3 HAVING count(DISTINCT author_id) > 1000),
capped AS (SELECT DISTINCT ek.entry_id FROM ek JOIN cap ON cap.fam = ek.fam AND cap.gi = ek.gi AND cap.inst = ek.inst),
e2 AS (SELECT * FROM ek LEFT ANTI JOIN capped USING (entry_id)),
p2 AS (SELECT * FROM pk LEFT ANTI JOIN cap USING (fam, gi, inst)),
pr AS (
  SELECT e.entry_id, p.author_id, p.years, e.lo, e.hi FROM e2 e JOIN p2 p ON p.fam = e.fam AND p.giv = e.giv AND p.inst = e.inst
  UNION ALL
  SELECT e.entry_id, p.author_id, p.years, e.lo, e.hi FROM e2 e JOIN p2 p ON p.fam = e.fam AND p.gi = e.giv AND p.inst = e.inst
  WHERE length(e.giv) = 1
  UNION ALL
  SELECT e.entry_id, p.author_id, p.years, e.lo, e.hi FROM e2 e JOIN p2 p ON p.fam = e.fam AND p.giv = e.gi AND p.inst = e.inst
  WHERE length(p.giv) = 1
),
pd AS (SELECT DISTINCT entry_id, author_id FROM pr WHERE exists(years, y -> y BETWEEN lo AND hi))
SELECT entry_id, author_id, count(*) OVER (PARTITION BY entry_id) AS n_short, false AS capped FROM pd
UNION ALL
SELECT entry_id, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), true FROM capped
