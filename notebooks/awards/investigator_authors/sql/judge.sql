-- Investigator authors (oxjob #1847): Judge_With_Jev's lists and items, after the score task. An entry (one person on one award, as
-- shortlist.sql) is listed when no other layer holds it (__P__jev_owned: no claim tonight by a route other than jev_link, nothing
-- served now by another layer, no student pick at its bar or relaxed one) and it has a same-name list of 1-20 live profiles: its
-- institution shortlist when it has one (a capped shortlist: no list), else every profile with its name key anywhere, as the graded
-- runs (JEV55, JWI55/JWE55, J20S63/J20S2A/J20S2B), less any profile another person on the award holds. list_hash: the list's sorted
-- author ids. Asked tonight: a listed entry with a slot never asked, or whose list or printed name changed since; at most __JEVMAX__,
-- never-asked first.
CREATE TABLE IF NOT EXISTS __P__authors (award_id BIGINT, slot STRING, position INT, given_name STRING, family_name STRING, orcid STRING,
  author_id BIGINT, method STRING, layer STRING, evidence STRING, model_version INT, run_date DATE)
CLUSTER BY (award_id)
COMMENT 'Investigator authors (oxjob #1847): the served grant slot -> author links; one MERGE a run'
;
-- Who holds which profile tonight, as an entry: a claim by a route other than jev_link, a slot served now by another layer, or the
-- student's pick at its bar or its relaxed one (decide.sql's rules without their guards). These entries are not asked, and their
-- profiles are never offered to another person on the award (decide.sql would hold both).
CREATE OR REPLACE TABLE __P__jev_owned AS
WITH th AS (SELECT 0.9649769 AS th99, 0.995 AS th_4_6, 0.5 AS guard_second, 0.95 AS th_1, 0.98 AS th_4_6_lo, 0.2 AS second_lo),
ms AS (
  SELECT entry_id, max(CASE WHEN rk = 1 THEN author_id END) AS top_author, max(CASE WHEN rk = 1 THEN s END) AS top,
    coalesce(max(CASE WHEN rk = 2 THEN s END), 0.0) AS second, max(n) AS n
  FROM (SELECT entry_id, author_id, s, row_number() OVER (PARTITION BY entry_id ORDER BY s DESC, author_id) AS rk,
          count(*) OVER (PARTITION BY entry_id) AS n FROM __P__scores)
  GROUP BY 1
),
c AS (SELECT award_id, slot, position, author_id FROM __P__routes WHERE method <> 'jev_link'
      UNION SELECT award_id, slot, position, author_id FROM __P__authors WHERE layer <> 'jev_link')
SELECT DISTINCT c.award_id, c.author_id, concat(p.award_id, '|', p.person_key) AS entry_id
FROM c JOIN __P__people p ON p.award_id = c.award_id AND p.slot = c.slot AND p.position = c.position
UNION
SELECT DISTINCT p.award_id, ms.top_author, ms.entry_id FROM ms CROSS JOIN th
JOIN __P__people p ON concat(p.award_id, '|', p.person_key) = ms.entry_id
WHERE ms.second < th.guard_second AND (ms.top >= CASE WHEN ms.n <= 3 THEN th.th99 ELSE th.th_4_6 END OR (ms.n = 1 AND ms.top >= th.th_1)
  OR (ms.n BETWEEN 4 AND 6 AND ms.second < th.second_lo AND ms.top >= th.th_4_6_lo))
;
CREATE OR REPLACE TABLE __P__jev_lists CLUSTER BY (entry_id) AS
WITH pe AS (
  SELECT p.*, concat(p.award_id, '|', p.person_key) AS entry_id,
    row_number() OVER (PARTITION BY p.award_id, p.person_key
                       ORDER BY CASE p.slot WHEN 'lead_investigator' THEN 0 WHEN 'co_lead_investigator' THEN 1 ELSE 2 END, p.position) AS rk
  FROM __P__people p WHERE size(p.fam_keys) > 0 AND p.given_key IS NOT NULL
),
en AS (
  SELECT pe.entry_id, max(pe.award_id) AS award_id, max(pe.multi_name) AS multi,
    collect_list(named_struct('slot', pe.slot, 'position', pe.position, 'given_name', pe.given_name, 'family_name', pe.family_name)) AS slots
  FROM pe LEFT ANTI JOIN (SELECT DISTINCT entry_id FROM __P__jev_owned) held ON held.entry_id = pe.entry_id
  GROUP BY 1
),
e1 AS (SELECT pe.* FROM pe JOIN en ON en.entry_id = pe.entry_id WHERE pe.rk = 1 AND NOT en.multi),
il AS (SELECT s.entry_id, s.author_id FROM __P__shortlist s LEFT SEMI JOIN e1 ON e1.entry_id = s.entry_id WHERE s.author_id IS NOT NULL),
ek AS (  -- no shortlist at all (no institution, or none at it): the name key anywhere
  SELECT DISTINCT e1.entry_id, k AS fam, e1.given_key AS giv
  FROM (SELECT * FROM e1 LEFT ANTI JOIN (SELECT DISTINCT entry_id FROM __P__shortlist) s ON s.entry_id = e1.entry_id) e1
  LATERAL VIEW explode(e1.fam_keys) t AS k
),
al AS (
  SELECT e.entry_id, n.author_id FROM ek e JOIN __P__profile_names n ON n.last_key = e.fam AND n.first_key = e.giv WHERE length(e.giv) > 1
  UNION SELECT e.entry_id, n.author_id FROM ek e JOIN __P__profile_names n ON n.last_key = e.fam AND n.first_key = left(e.giv, 1)
    WHERE length(e.giv) > 1
  UNION SELECT e.entry_id, n.author_id FROM ek e JOIN __P__profile_names n ON n.last_key = e.fam AND left(n.first_key, 1) = e.giv
    WHERE length(e.giv) = 1
),
rs AS (  -- profiles another person on the award holds: __P__jev_owned, and Jev's own links (claimed tonight or served now)
  SELECT award_id, author_id, entry_id FROM __P__jev_owned
  UNION
  SELECT c.award_id, c.author_id, concat(p.award_id, '|', p.person_key) FROM (
    SELECT award_id, slot, position, author_id FROM __P__routes WHERE method = 'jev_link'
    UNION SELECT award_id, slot, position, author_id FROM __P__authors WHERE layer = 'jev_link') c
  JOIN __P__people p ON p.award_id = c.award_id AND p.slot = c.slot AND p.position = c.position
),
li AS (
  SELECT x.entry_id, x.author_id, x.src FROM (
    SELECT entry_id, author_id, 'institution' AS src FROM il UNION ALL SELECT entry_id, author_id, 'anywhere' AS src FROM al) x
  JOIN en ON en.entry_id = x.entry_id
  LEFT ANTI JOIN rs o ON o.award_id = en.award_id AND o.author_id = x.author_id AND o.entry_id <> x.entry_id
),
ls AS (
  SELECT entry_id, max(src) AS src, count(DISTINCT author_id) AS n_cands, array_sort(collect_set(author_id)) AS authors FROM li
  GROUP BY 1 HAVING count(DISTINCT author_id) BETWEEN 1 AND 20
)
SELECT ls.entry_id, en.award_id, ls.src, ls.n_cands, ls.authors,
  sha2(array_join(transform(ls.authors, a -> CAST(a AS STRING)), ','), 256) AS list_hash,
  en.slots, e1.given_name, e1.family_name, e1.aff_name, e1.slot AS role_slot, e1.lo, e1.hi
FROM ls JOIN en ON en.entry_id = ls.entry_id JOIN e1 ON e1.entry_id = ls.entry_id
;
CREATE OR REPLACE TABLE __P__jev_items CLUSTER BY (entry_id) AS
WITH js AS (  -- asked: every slot has an answer; changed: one was on another list or another printed name (a seed has no list yet)
  SELECT x.entry_id, min(j.award_id IS NOT NULL) AS asked,
    max(coalesce(j.list_hash <> x.list_hash, false) OR coalesce(lower(trim(coalesce(j.given_name, ''))) <> lower(trim(coalesce(x.given_name, '')))
      OR lower(trim(j.family_name)) <> lower(trim(x.family_name)), false)) AS changed
  FROM (SELECT l.entry_id, l.award_id, l.list_hash, s.slot, s.position, s.given_name, s.family_name
        FROM __P__jev_lists l LATERAL VIEW explode(l.slots) t AS s) x
  LEFT JOIN __P__jev j ON j.award_id = x.award_id AND j.slot = x.slot AND j.position = x.position
  GROUP BY 1
),
q AS (
  SELECT * FROM (
    SELECT l.*, js.asked, row_number() OVER (ORDER BY CASE WHEN js.asked THEN 1 ELSE 0 END, sha2(l.entry_id, 256)) AS qn
    FROM __P__jev_lists l JOIN js ON js.entry_id = l.entry_id WHERE NOT js.asked OR js.changed)
  WHERE qn <= __JEVMAX__
),
ca AS (SELECT q.entry_id, q.award_id, q.lo, q.hi, a AS author_id FROM q LATERAL VIEW explode(q.authors) t AS a),
au AS (
  SELECT try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT) AS author_id, a.display_name, a.display_name_alternatives, a.works_count,
    a.affiliations, a.topics
  FROM openalex.authors.authors_api a
  LEFT SEMI JOIN (SELECT DISTINCT author_id FROM ca) k ON k.author_id = try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT)
),
ins AS (  -- up to 8 institutions with their years, the most recent first
  SELECT author_id, slice(transform(array_sort(collect_list(named_struct('k', -y1, 't', t))), x -> x.t), 1, 8) AS institutions FROM (
    SELECT au.author_id, array_max(af.years) AS y1, concat(coalesce(af.institution.display_name, '?'), ' (',
      coalesce(af.institution.country_code, '?'), ') ', array_min(af.years), '-', array_max(af.years)) AS t
    FROM au LATERAL VIEW explode(coalesce(au.affiliations, array())) x AS af WHERE size(coalesce(af.years, array())) > 0)
  GROUP BY 1
),
tp AS (  -- up to 5 topics, the most used first
  SELECT author_id, slice(transform(array_sort(collect_list(named_struct('k', -c, 't', t))), x -> x.t), 1, 5) AS top_topics FROM (
    SELECT au.author_id, tt.count AS c, tt.display_name AS t FROM au LATERAL VIEW explode(coalesce(au.topics, array())) x AS tt)
  GROUP BY 1
),
wt AS (  -- the candidates' titled works
  SELECT w.author_id, o.publication_year AS y, substr(o.title, 1, 150) AS t, coalesce(o.cited_by_count, 0) AS cc
  FROM (SELECT DISTINCT wa.author_id, wa.work_id FROM openalex.works.work_authors wa
        LEFT SEMI JOIN (SELECT DISTINCT author_id FROM ca) k ON k.author_id = wa.author_id) w
  JOIN openalex.works.openalex_works o ON o.id = w.work_id WHERE coalesce(o.title, '') <> ''
),
yr AS (SELECT author_id, min(y) AS first_year, max(y) AS last_year FROM wt GROUP BY 1),
pp AS (  -- up to 10 titles: inside the grant window first, then the most cited
  SELECT entry_id, author_id, slice(transform(array_sort(collect_list(named_struct('o', o, 'cc', -cc, 't', t,
    'p', concat(coalesce(CAST(y AS STRING), '?'), ': ', t)))), x -> x.p), 1, 10) AS papers FROM (
    SELECT ca.entry_id, ca.author_id, wt.y, wt.t, wt.cc, CASE WHEN wt.y BETWEEN ca.lo AND ca.hi THEN 0 ELSE 1 END AS o
    FROM ca JOIN wt ON wt.author_id = ca.author_id)
  GROUP BY 1, 2
),
fd AS (SELECT DISTINCT f.award_id, f.author_id FROM __P__paper_authors f
       LEFT SEMI JOIN ca ON ca.award_id = f.award_id AND ca.author_id = f.author_id),
card AS (  -- candidate keys c1..cN in hash order, so a position gives nothing away
  SELECT ca.entry_id, ca.author_id,
    row_number() OVER (PARTITION BY ca.entry_id ORDER BY sha2(concat(ca.entry_id, '|', ca.author_id, '|ck'), 256)) AS kn,
    named_struct('display_name', au.display_name,
      'name_variants', slice(filter(array_distinct(coalesce(au.display_name_alternatives, array())),
        n -> n <> au.display_name AND NOT lower(n) LIKE '%orcid%'), 1, 5),
      'institutions_with_years', coalesce(ins.institutions, array()), 'top_topics', coalesce(tp.top_topics, array()),
      'first_publishing_year', yr.first_year, 'last_publishing_year', yr.last_year, 'works_count', au.works_count,
      'paper_titles_with_years', coalesce(pp.papers, array()), 'orcid', '', 'on_a_paper_this_grant_funded', fd.author_id IS NOT NULL) AS card
  FROM ca JOIN au ON au.author_id = ca.author_id
  LEFT JOIN ins ON ins.author_id = ca.author_id LEFT JOIN tp ON tp.author_id = ca.author_id LEFT JOIN yr ON yr.author_id = ca.author_id
  LEFT JOIN pp ON pp.entry_id = ca.entry_id AND pp.author_id = ca.author_id
  LEFT JOIN fd ON fd.award_id = ca.award_id AND fd.author_id = ca.author_id
),
gs AS (
  SELECT entry_id, count(*) AS k,
    map_from_entries(transform(array_sort(collect_list(named_struct('kn', kn, 'k', concat('c', kn), 'v', card))), x -> struct(x.k, x.v))) AS cm,
    map_from_entries(transform(array_sort(collect_list(named_struct('kn', kn, 'k', concat('c', kn), 'a', author_id))), x -> struct(x.k, x.a)))
      AS ckeys
  FROM card GROUP BY 1
),
aw AS (
  SELECT a.id AS award_id, a.display_name AS title, substr(coalesce(a.description, ''), 1, 600) AS abstract, a.funder_award_id,
    a.start_year, a.end_year, transform(coalesce(a.institution_awarded, array()), i -> i.display_name) AS inst_names,
    concat(coalesce(f.display_name, ''), CASE WHEN coalesce(f.country_code, '') <> '' THEN concat(' (', f.country_code, ')') ELSE '' END) AS funder
  FROM openalex.awards.awards_api a LEFT JOIN openalex.funders.funders f ON f.funder_id = a.funder_id
  LEFT SEMI JOIN q ON q.award_id = a.id
)
SELECT q.entry_id, q.award_id, q.list_hash, q.slots, q.asked, q.qn, gs.k AS n_cands, concat('sp_k', gs.k) AS stratum, to_json(gs.ckeys) AS ckeys,
  regexp_replace(to_json(named_struct('item', q.entry_id,
    'GRANT', named_struct('funder', aw.funder, 'grant_number', coalesce(aw.funder_award_id, ''), 'title', coalesce(aw.title, ''),
      'abstract_first_600_characters', aw.abstract, 'start_year', aw.start_year, 'end_year', aw.end_year,
      'institutions', array_distinct(concat(aw.inst_names, CASE WHEN coalesce(q.aff_name, '') <> '' THEN array(q.aff_name) ELSE array() END)),
      'investigator_role', CASE q.role_slot WHEN 'lead_investigator' THEN 'lead investigator'
                                            WHEN 'co_lead_investigator' THEN 'co-lead investigator' ELSE 'investigator' END),
    'INVESTIGATOR', named_struct('given_name', coalesce(q.given_name, ''), 'family_name', coalesce(q.family_name, ''), 'orcid', '',
      'affiliation', coalesce(q.aff_name, '')),
    'CANDIDATES', gs.cm)), '(?i)(https?://)?(orcid\\.org/)?[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]', '') AS state_json  -- no ORCID reaches Jev
FROM q JOIN gs ON gs.entry_id = q.entry_id JOIN aw ON aw.award_id = q.award_id
