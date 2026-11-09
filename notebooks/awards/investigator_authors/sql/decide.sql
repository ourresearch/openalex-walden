-- Investigator authors (oxjob #1847): one decision per grant slot, with a named layer; the first match wins:
--   model_missing        a 1-6 list with a candidate unscored, or scored with other thresholds than th's: the run fails
--   route_conflict       two different authors among the slot's claims, or a claim and the model's servable pick
--   guard_orcid          the author carries another ORCID than the grant's, or the grant's ORCID is on one other profile, or an ORCID
--                        claim names the author and most of the profile's works carry another person's ORCID (whatever else agrees)
--   route_bind           one author from identifier routes (orcid, linked_paper, nih_core_papers, funder_person_id, crossref_orcid,
--                        nsfc_name)
--   kaken_nrid           one author from that route alone (an identifier: not held for common names)
--   jev_link             Jev's pick alone
--   guard_shared_author  the model's or Jev's pick is bound to another person on the same award
--   model_bind           the model alone at its bar: lists of 1-3 at s >= th99, 4-6 at s >= th_4_6, the second < guard_second
--   model_relaxed        the model below that bar on a slot no route claims: a list of 1 at s >= th_1, or 4-6 at s >= th_4_6_lo with
--                        the second < second_lo
--   two_plausible, below_threshold, too_many_candidates, no_name_key, no_candidates: not served
-- th must equal the registered model version's tags (ScoreCandidates copies them onto every score); a tie goes to the lower author id.
CREATE OR REPLACE TABLE __P__decisions AS
WITH th AS (SELECT 0.9649769 AS th99, 0.995 AS th_4_6, 0.5 AS guard_second, 0.95 AS th_1, 0.98 AS th_4_6_lo, 0.2 AS second_lo),
r AS (
  SELECT award_id, slot, position, array_sort(collect_set(method)) AS methods, array_sort(collect_set(author_id)) AS route_authors,
    array_sort(collect_list(named_struct('method', method, 'author_id', author_id, 'evidence', evidence))) AS claims
  FROM __P__routes GROUP BY 1, 2, 3
),
k AS (SELECT c.entry_id, c.author_id, c.s, c.model_version, row_number() OVER (PARTITION BY c.entry_id ORDER BY c.s DESC, c.author_id) AS rk,
        abs(c.th99 - th.th99) > 1e-6 OR abs(c.th_4_6 - th.th_4_6) > 1e-6 OR abs(c.guard_second - th.guard_second) > 1e-6 AS other_model
      FROM __P__scores c CROSS JOIN th),
m AS (
  SELECT entry_id, max(CASE WHEN rk = 1 THEN author_id END) AS m_author, max(CASE WHEN rk = 1 THEN s END) AS m_s,
    coalesce(max(CASE WHEN rk = 2 THEN s END), 0.0) AS m_second, CAST(count(s) AS INT) AS n_cands, max(model_version) AS model_version,
    max(other_model) AS other_model
  FROM k GROUP BY 1
),
sh AS (SELECT entry_id, max(capped) AS capped, max(n_short) AS n_short FROM __P__shortlist GROUP BY 1),
s AS (
  SELECT p.award_id, p.slot, p.position, p.given_name, p.family_name, p.person_key, p.orcid, size(p.fam_keys) = 0 AS no_key,
    concat(p.person_key, '|', array_join(coalesce(p.gt, array()), ' ')) AS full_key,   -- Ann A. and Ann B. Lee: one entry, two people
    r.methods, r.route_authors, r.claims,
    array_distinct(transform(filter(coalesce(r.claims, array()),
      c -> NOT array_contains(array('kaken_nrid', 'jev_link'), c.method)),
      c -> c.author_id)) AS id_authors,
    sh.capped, sh.n_short, th.guard_second, m.m_author, m.m_s, m.m_second, m.n_cands, m.model_version, m.other_model,
    CASE WHEN NOT m.other_model AND m.m_second < th.guard_second AND m.m_s >= CASE WHEN m.n_cands <= 3 THEN th.th99 ELSE th.th_4_6 END
         THEN m.m_author END AS pick,
    CASE WHEN NOT m.other_model AND r.award_id IS NULL AND m.m_second < th.guard_second
           AND ((m.n_cands = 1 AND m.m_s >= th.th_1 AND m.m_s < th.th99)
             OR (m.n_cands BETWEEN 4 AND 6 AND m.m_second < th.second_lo AND m.m_s >= th.th_4_6_lo AND m.m_s < th.th_4_6))
         THEN m.m_author END AS relaxed
  FROM __P__people p CROSS JOIN th
  LEFT JOIN r ON r.award_id = p.award_id AND r.slot = p.slot AND r.position = p.position
  LEFT JOIN sh ON sh.entry_id = concat(p.award_id, '|', p.person_key) LEFT JOIN m ON m.entry_id = concat(p.award_id, '|', p.person_key)
),
n AS (SELECT s.*, array_distinct(concat(coalesce(route_authors, array()), CASE WHEN pick IS NULL THEN array() ELSE array(pick) END)) AS named
      FROM s),
b AS (SELECT n.*, CASE WHEN size(named) = 1 THEN get(named, 0) END AS bound FROM n),
x AS (
  SELECT b.*, coalesce(b.orcid IS NOT NULL AND o.orcid <> b.orcid, false) OR coalesce(g.author_id <> b.bound, false) AS orcid_clash,
    coalesce(arrays_overlap(b.methods, array('orcid', 'crossref_orcid')) AND mo.majority_orcid IS NOT NULL
      AND NOT array_contains(transform(filter(b.claims, c -> c.method IN ('orcid', 'crossref_orcid')),
        c -> concat('https://orcid.org/', coalesce(get_json_object(c.evidence, '$.orcid'), ''))), mo.majority_orcid), false) AS orcid_minority,
    coalesce(b.orcid IS NOT NULL AND o2.orcid <> b.orcid, false) OR coalesce(g.author_id <> b.relaxed, false) AS relaxed_clash,
    size(collect_set(b.full_key) OVER (PARTITION BY b.award_id, b.bound)) AS bound_people,
    size(collect_set(b.full_key) OVER (PARTITION BY b.award_id, coalesce(b.bound, b.relaxed))) AS relaxed_people
  FROM b LEFT JOIN (SELECT author_id, max(orcid) AS orcid FROM __P__profile_orcids GROUP BY 1) o ON o.author_id = b.bound
  LEFT JOIN (SELECT author_id, max(orcid) AS orcid FROM __P__profile_orcids GROUP BY 1) o2 ON o2.author_id = b.relaxed
  LEFT JOIN (SELECT orcid, author_id FROM __P__profile_orcids WHERE n_profiles = 1) g ON g.orcid = b.orcid
  LEFT JOIN (SELECT author_id, max(majority_orcid) AS majority_orcid FROM openalex.authors.orcid_majority_lookup GROUP BY 1) mo
    ON mo.author_id = b.bound
),
d AS (
  SELECT x.*, CASE
    WHEN other_model OR (n_short BETWEEN 1 AND 6 AND (m_author IS NULL OR n_cands < n_short)) THEN 'model_missing'
    WHEN size(named) > 1 THEN 'route_conflict'
    WHEN bound IS NOT NULL AND orcid_clash THEN 'guard_orcid'
    WHEN bound IS NOT NULL AND orcid_minority THEN 'guard_orcid'
    WHEN size(id_authors) = 1 THEN 'route_bind'
    WHEN size(coalesce(route_authors, array())) = 1 AND array_contains(methods, 'kaken_nrid') THEN 'kaken_nrid'
    WHEN size(coalesce(route_authors, array())) = 1 AND array_contains(methods, 'jev_link') AND bound_people > 1 THEN 'guard_shared_author'
    WHEN size(coalesce(route_authors, array())) = 1 AND array_contains(methods, 'jev_link') THEN 'jev_link'
    WHEN pick IS NOT NULL AND bound_people > 1 THEN 'guard_shared_author'
    WHEN pick IS NOT NULL THEN 'model_bind'
    WHEN relaxed IS NOT NULL AND NOT relaxed_clash AND relaxed_people = 1 THEN 'model_relaxed'
    WHEN m_author IS NOT NULL AND m_second >= guard_second THEN 'two_plausible'
    WHEN m_author IS NOT NULL THEN 'below_threshold'
    WHEN capped OR n_short > 6 THEN 'too_many_candidates'
    WHEN no_key THEN 'no_name_key'
    ELSE 'no_candidates' END AS layer
  FROM x
)
SELECT award_id, slot, position, given_name, family_name, orcid, person_key, layer,
  coalesce(bound, CASE WHEN layer = 'model_relaxed' THEN relaxed END) AS author_id, methods, n_cands, m_author AS model_author, m_s AS model_s,
  m_second AS model_second, model_version,
  to_json(named_struct('claims', claims, 'student', CASE WHEN m_author IS NOT NULL THEN named_struct('author_id', m_author, 's', round(m_s, 4),
    'second', round(m_second, 4), 'n_cands', n_cands, 'version', model_version) END)) AS evidence
FROM d
;
-- The served table (Nightly Grants' extra input investigator_authors; only publish.sql writes it). Nightly Grants attaches a row only
-- while the slot still prints its names and ORCID (orcid: the grant's, cleaned as people.sql does).
CREATE TABLE IF NOT EXISTS __P__authors (award_id BIGINT, slot STRING, position INT, given_name STRING, family_name STRING, orcid STRING,
  author_id BIGINT, method STRING, layer STRING, evidence STRING, model_version INT, run_date DATE)
CLUSTER BY (award_id)
COMMENT 'Investigator authors (oxjob #1847): the served grant slot -> author links; one MERGE a run'
;
-- The served layers, in one place
CREATE OR REPLACE TEMPORARY VIEW to_serve AS
SELECT award_id, slot, position, given_name, family_name, orcid, author_id,
  CASE WHEN layer = 'model_bind' THEN 'student' WHEN layer = 'model_relaxed' THEN 'student_relaxed' ELSE array_join(methods, '+') END AS method,
  layer, evidence, CASE WHEN layer IN ('model_bind', 'model_relaxed') THEN model_version END AS model_version, DATE'__RUN__' AS run_date
FROM __P__decisions WHERE layer IN ('route_bind', 'kaken_nrid', 'jev_link', 'model_bind', 'model_relaxed')
;
SELECT CASE WHEN count_if(layer = 'model_missing') > 0
  THEN raise_error(concat('investigator authors: ', count_if(layer = 'model_missing'), ' slots are model_missing')) END AS failed,
  count(*) AS slots, count_if(layer = 'route_conflict') AS route_conflict
FROM __P__decisions
;
SELECT layer, count(*) AS n FROM __P__decisions GROUP BY 1 ORDER BY 1