-- Investigator authors (oxjob #1847): every route's claims on a grant slot, one shape (award_id, slot, position, author_id, method,
-- evidence). A claim is not a decision: decide.sql serves a slot only when every claim and the model's pick name the same author.
--   orcid             the grant's ORCID is on exactly one live profile and the names agree (the name rule, or any given token)
--   linked_paper      on the grant's own papers, exactly one profile has the family key + first initial (a KAKEN lead: and a Latin
--                     profile name must carry the lead's given name or its initial, else no claim and no NRID spread)
--   nih_core_papers   NIH: the only name-agreeing author on the papers work_awards links to the core project
--   funder_person_id  the NIH / NSF person id -> the one author orcid / linked_paper give that id on any grant
--   crossref_orcid    the ORCID a funder deposited on its Crossref grant record, on one profile whose name agrees, fitting one slot
--   kaken_nrid        the KAKEN lead's researcher number -> the one author orcid / linked_paper give it on any KAKEN grant
--   nsfc_name         NSFC (Chinese names only): on the grant's own papers, one author printed as the PI's full pinyin at the grant
--                     institution, and at least half of that profile's other works in the grant window there too
--   jev_link          Jev's pick among 1-6 same-name profiles (openalex.awards.investigator_jev_links, a frozen copy)
CREATE OR REPLACE TABLE __P__routes CLUSTER BY (award_id) AS
WITH pe AS (
  SELECT p.*, CASE p.provenance WHEN 'nih_exporter' THEN 'nih' WHEN 'nsf_award_search' THEN 'nsf' END AS src,
    CASE p.provenance
      WHEN 'nih_exporter' THEN nullif(regexp_extract(upper(regexp_replace(p.funder_award_id, '[^A-Za-z0-9]', '')),
                                                     '([A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{6})', 1), '')
      WHEN 'nsf_award_search' THEN nullif(regexp_extract(p.funder_award_id, '([0-9]{7})', 1), '') END AS rec
  FROM __P__people p
),
pn AS (SELECT * FROM __P__profile_names),
paw AS (SELECT award_id, author_id, count(*) AS works FROM __P__paper_authors GROUP BY 1, 2),
ov AS (  -- the names (display name + alternatives, as tokens) of the profiles the grants' ORCIDs point to
  SELECT DISTINCT o.orcid, o.author_id, __P__name_tokens(n) AS nt
  FROM __P__profile_orcids o JOIN openalex.authors.authors_api a ON try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT) = o.author_id
  LATERAL VIEW explode(concat(array(a.display_name), coalesce(a.display_name_alternatives, array()))) t AS n
  WHERE o.n_profiles = 1 AND o.orcid IN (SELECT orcid FROM pe WHERE orcid IS NOT NULL)
),
r_orcid AS (  -- the name rule, or any given token against the profile's own name tokens
  SELECT award_id, slot, position, author_id, 'orcid' AS method, to_json(named_struct('orcid', orcid)) AS evidence FROM (
    SELECT p.award_id, p.slot, p.position, o.author_id, p.orcid
    FROM pe p JOIN __P__profile_orcids o ON o.orcid = p.orcid AND o.n_profiles = 1
    JOIN pn ON pn.author_id = o.author_id AND array_contains(p.fam_keys, pn.last_key)
    WHERE __P__names_agree(p.fam_keys, p.given_key, array(pn.last_key), pn.first_key)
    UNION
    SELECT p.award_id, p.slot, p.position, o.author_id, p.orcid
    FROM pe p JOIN ov o ON o.orcid = p.orcid
    WHERE arrays_overlap(p.fam_keys, o.nt) AND (size(p.gt) = 0 OR size(filter(p.gt, g -> size(filter(array_except(o.nt, p.fam_keys),
      t -> g = t OR (length(g) = 1 AND left(t, 1) = g) OR (length(t) = 1 AND left(g, 1) = t))) > 0)) > 0))
),
ek AS (SELECT p.award_id, p.slot, p.position, k, left(p.given_key, 1) AS gi, p.given_key
       FROM pe p LATERAL VIEW explode(p.fam_keys) t AS k WHERE p.given_key IS NOT NULL),
ns AS (  -- another person on the grant shares the key + initial under a different first name: the route stays silent for both
  SELECT DISTINCT e.award_id, e.slot, e.position FROM ek e
  JOIN (SELECT award_id, k, gi FROM ek GROUP BY 1, 2, 3 HAVING count(DISTINCT CASE WHEN length(given_key) > 1 THEN given_key END) > 1) q
    ON q.award_id = e.award_id AND q.k = e.k AND q.gi = e.gi
),
lp AS (
  SELECT e.award_id, e.slot, e.position, a.author_id, max(a.works) AS works
  FROM ek e JOIN paw a ON a.award_id = e.award_id JOIN pn ON pn.author_id = a.author_id AND pn.last_key = e.k AND left(pn.first_key, 1) = e.gi
  LEFT ANTI JOIN ns ON ns.award_id = e.award_id AND ns.slot = e.slot AND ns.position = e.position
  GROUP BY 1, 2, 3, 4
),
lp1 AS (SELECT award_id, slot, position, min(author_id) AS author_id, max(works) AS works FROM lp
        GROUP BY 1, 2, 3 HAVING count(DISTINCT author_id) = 1),
kpx AS (  -- KAKEN leads matched on family key + initial alone: the profile's Latin name must carry the lead's given name (romaji_fold,
          -- a shared 3+ letter prefix, or 1-2 letters off) or its initial. Abe Koji is not Keishi Abe; a kanji-only profile name passes.
          -- (The fold runs on exploded tokens, not inside a lambda: Databricks can't call a SQL function there.)
  SELECT l.award_id, l.slot, l.position, l.author_id
  FROM lp1 l
  JOIN (SELECT award_id, slot, position, given_key, __P__romaji_fold(given_key) AS gk FROM pe WHERE provenance = 'kaken') p
    ON p.award_id = l.award_id AND p.slot = l.slot AND p.position = l.position
  JOIN (SELECT author_id, t, __P__romaji_fold(t) AS f
        FROM (SELECT try_cast(regexp_extract(id, '([0-9]+)$', 1) AS BIGINT) AS author_id, __P__name_tokens(display_name) AS dt
              FROM openalex.authors.authors_api WHERE try_cast(regexp_extract(id, '([0-9]+)$', 1) AS BIGINT) IN (SELECT author_id FROM lp1))
        LATERAL VIEW explode(dt) x AS t) a ON a.author_id = l.author_id
  WHERE length(p.given_key) > 1
  GROUP BY l.award_id, l.slot, l.position, l.author_id, p.given_key, p.gk
  HAVING count_if((length(a.t) = 1 AND a.t = left(p.given_key, 1)) OR (length(a.t) > 1 AND (a.f = p.gk
    OR (length(a.f) >= 3 AND length(p.gk) >= 3 AND (startswith(a.f, p.gk) OR startswith(p.gk, a.f)))
    OR levenshtein(a.f, p.gk) <= CASE WHEN length(p.given_key) >= 7 THEN 2 ELSE 1 END))) = 0
),
r_paper AS (
  SELECT l.award_id, l.slot, l.position, l.author_id, 'linked_paper' AS method, to_json(named_struct('works', l.works)) AS evidence
  FROM lp1 l LEFT ANTI JOIN kpx k ON k.award_id = l.award_id AND k.slot = l.slot AND k.position = l.position
),
nk AS (SELECT DISTINCT p.rec AS core, p.person_key, p.given_key, k FROM pe p LATERAL VIEW explode(p.fam_keys) t AS k
       WHERE p.src = 'nih' AND p.rec IS NOT NULL),
cw AS (SELECT DISTINCT p.rec AS core, a.work_id FROM pe p JOIN __P__paper_authors a ON a.award_id = p.award_id
       WHERE p.src = 'nih' AND p.rec IS NOT NULL),
ca AS (SELECT DISTINCT cw.core, au.author_id FROM cw JOIN openalex.works.work_authors au ON au.work_id = cw.work_id WHERE au.author_id IS NOT NULL),
nc AS (  -- one ask per (core, person), not per yearly record
  SELECT nk.core, nk.person_key, count(DISTINCT ca.author_id) AS n, min(ca.author_id) AS author_id
  FROM nk JOIN ca ON ca.core = nk.core JOIN pn ON pn.author_id = ca.author_id AND pn.last_key = nk.k
  WHERE __P__names_agree(array(nk.k), nk.given_key, array(pn.last_key), pn.first_key)
  GROUP BY 1, 2
),
r_nih AS (
  SELECT p.award_id, p.slot, p.position, nc.author_id, 'nih_core_papers' AS method, to_json(named_struct('core', p.rec)) AS evidence
  FROM pe p JOIN nc ON nc.core = p.rec AND nc.person_key = p.person_key WHERE p.src = 'nih' AND nc.n = 1
),
fm AS (SELECT lower(regexp_extract(trim(doi), '(10\\.[0-9]{4,9}/\\S+)', 1)) AS fdoi, min(coalesce(merge_into_id, funder_id)) AS fid
       FROM openalex.funders.funders WHERE doi IS NOT NULL GROUP BY 1 HAVING count(DISTINCT coalesce(merge_into_id, funder_id)) = 1),
cg AS (
  SELECT lower(regexp_extract(trim(g.DOI), '(10\\.[0-9]{4,9}/\\S+)', 1)) AS cdoi, upper(regexp_replace(g.award, '[^A-Za-z0-9]', '')) AS cnum,
    array_distinct(flatten(transform(coalesce(g.project, array()), p -> transform(
      filter(flatten(transform(coalesce(p.funding, array()), f -> coalesce(f.funder.id, array()))), i -> upper(i.`id-type`) = 'DOI'),
      i -> lower(regexp_extract(trim(i.id), '(10\\.[0-9]{4,9}/\\S+)', 1)))))) AS fdois,
    pos, person.given, person.family,
    nullif(regexp_extract(upper(coalesce(person.orcid, '')), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), '') AS orcid
  FROM openalex.awards.crossref_grants_deduplicated g
  LATERAL VIEW posexplode(flatten(transform(coalesce(g.project, array()), p -> concat(
    transform(coalesce(p.`lead-investigator`, array()), y -> named_struct('given', y.given, 'family', y.family, 'orcid', y.ORCID)),
    transform(coalesce(p.`co-lead-investigator`, array()), y -> named_struct('given', y.given, 'family', y.family, 'orcid', y.ORCID)),
    transform(coalesce(p.investigator, array()), y -> named_struct('given', y.given, 'family', y.family, 'orcid', y.ORCID)))))) x AS pos, person
  WHERE trim(coalesce(person.family, '')) <> ''
),
cr AS (SELECT DISTINCT cdoi, cnum, fdois FROM cg WHERE cdoi IS NOT NULL),
aw AS (SELECT DISTINCT award_id, funder_id, upper(regexp_replace(funder_award_id, '[^A-Za-z0-9]', '')) AS anum,
         lower(regexp_extract(trim(award_doi), '(10\\.[0-9]{4,9}/\\S+)', 1)) AS adoi FROM pe),
cm AS (
  SELECT cdoi, award_id, min(how) AS how FROM (
    SELECT r.cdoi, aw.award_id, 'doi' AS how FROM cr r JOIN aw ON aw.adoi = r.cdoi
    UNION ALL
    SELECT r.cdoi, aw.award_id, 'funder+number' FROM (SELECT cdoi, cnum, fd FROM cr LATERAL VIEW explode(fdois) t AS fd WHERE length(cnum) >= 4) r
    JOIN fm ON fm.fdoi = r.fd JOIN aw ON aw.funder_id = fm.fid AND aw.anum = r.cnum)
  GROUP BY 1, 2
),
cm1 AS (SELECT * FROM (SELECT cm.*, max(how = 'doi') OVER (PARTITION BY cdoi) AS has_doi FROM cm) WHERE how = 'doi' OR NOT has_doi),
cx AS (  -- Crossref people whose deposited ORCID is on exactly one profile with an agreeing name
  SELECT DISTINCT c.cdoi, c.pos, c.orcid, m.award_id, m.how, o.author_id, __P__family_keys(c.family) AS fam_keys,
    get(__P__name_tokens(c.given), 0) AS given_key
  FROM cg c JOIN cm1 m ON m.cdoi = c.cdoi JOIN __P__profile_orcids o ON o.orcid = c.orcid AND o.n_profiles = 1 JOIN pn ON pn.author_id = o.author_id
  WHERE __P__names_agree(__P__family_keys(c.family), get(__P__name_tokens(c.given), 0), array(pn.last_key), pn.first_key)
),
cs AS (  -- n_fit counts people by the full given name, as decide.sql does: Ann A. and Ann B. Lee share a person_key
  SELECT x.*, p.slot, p.position,
    size(collect_set(concat(p.person_key, '|', array_join(coalesce(p.gt, array()), ' '))) OVER (PARTITION BY x.cdoi, x.pos, x.award_id)) AS n_fit
  FROM cx x LEFT JOIN pe p ON p.award_id = x.award_id AND __P__names_agree(x.fam_keys, x.given_key, p.fam_keys, p.given_key)),
r_cr AS (
  SELECT award_id, slot, position, author_id, 'crossref_orcid' AS method,
    to_json(min(named_struct('crossref_doi', cdoi, 'matched_by', how, 'orcid', orcid))) AS evidence
  FROM cs WHERE slot IS NOT NULL AND n_fit = 1 GROUP BY award_id, slot, position, author_id
),
-- jev_link reads a frozen copy: the slot must still print the judged name (through award_id_redirects) and the profile must
-- still be live and agree
jv AS (SELECT coalesce(rd.canonical_id, j.award_id) AS award_id, j.slot, j.position, j.given_name, j.family_name, j.author_id, j.p
       FROM openalex.awards.investigator_jev_links j
       LEFT JOIN (SELECT old_id, max(canonical_id) AS canonical_id FROM openalex.awards.award_id_redirects GROUP BY 1) rd
         ON rd.old_id = j.award_id),
r_jev AS (
  SELECT p.award_id, p.slot, p.position, min(j.author_id) AS author_id, 'jev_link' AS method, to_json(named_struct('p', max(j.p))) AS evidence
  FROM jv j JOIN pe p ON p.award_id = j.award_id AND p.slot = j.slot AND p.position = j.position
    AND lower(trim(coalesce(p.given_name, ''))) = lower(trim(coalesce(j.given_name, ''))) AND lower(trim(p.family_name)) = lower(trim(j.family_name))
  JOIN pn ON pn.author_id = j.author_id AND array_contains(p.fam_keys, pn.last_key)
  WHERE __P__names_agree(p.fam_keys, p.given_key, array(pn.last_key), pn.first_key)
  GROUP BY 1, 2, 3 HAVING count(DISTINCT j.author_id) = 1),
fp AS (  -- the funders' own person ids, one union: NIH RePORTER PI ids per core project, NSF person ids per award
  SELECT 'nih' AS src, core_project_num AS rec, concat('nih:', pi_id) AS person_id, pos, given, family FROM (
    SELECT r.core_project_num, trim(regexp_replace(pid, '\\(contact\\)', '')) AS pi_id, i AS pos,
      split(regexp_replace(try_element_at(split(r.pi_names, ';'), i + 1), '\\(contact\\)', ''), ',') AS nm,
      trim(get(nm, 0)) AS family, trim(try_element_at(nm, 2)) AS given
    FROM openalex.awards.nih_grants_raw r LATERAL VIEW posexplode(split(r.pi_ids, ';')) x AS i, pid WHERE r.pi_ids IS NOT NULL)
  WHERE pi_id RLIKE '^[0-9]+$'
  UNION ALL
  SELECT 'nsf', awd_id, concat('nsf:', trim(pi.nsf_id)), pidx, pi.pi_first_name, pi.pi_last_name
  FROM openalex.awards.nsf_grants_raw
  LATERAL VIEW posexplode(from_json(pi_all, 'array<struct<pi_first_name:string,pi_last_name:string,nsf_id:string>>')) t AS pidx, pi
  WHERE trim(pi.nsf_id) RLIKE '^[0-9]+$'
),
fk AS (SELECT src, rec, person_id, min(pos) AS pos, __P__family_keys(min(family)) AS fam_keys, get(__P__name_tokens(min(given)), 0) AS given_key
       FROM fp GROUP BY 1, 2, 3),
sp AS (SELECT p.award_id, p.slot, p.position, f.person_id FROM pe p JOIN fk f ON f.src = p.src AND f.rec = p.rec
       WHERE __P__names_agree(p.fam_keys, p.given_key, f.fam_keys, f.given_key)),
s1 AS (SELECT award_id, slot, position, min(person_id) AS person_id FROM sp GROUP BY 1, 2, 3 HAVING count(DISTINCT person_id) = 1),
t0 AS (  -- the anchors: an orcid or linked_paper claim that no other claim on the slot contradicts (every route but the two built here)
  SELECT award_id, slot, position, min(author_id) AS author_id, max(kind = 'orcid') AS has_orcid, collect_set(orcid) AS orcids
  FROM (SELECT award_id, slot, position, author_id, 'orcid' AS kind, true AS anchor, get_json_object(evidence, '$.orcid') AS orcid FROM r_orcid
        UNION ALL SELECT award_id, slot, position, author_id, 'orcid', false, get_json_object(evidence, '$.orcid') FROM r_cr
        UNION ALL SELECT award_id, slot, position, author_id, 'paper', true, NULL FROM r_paper
        UNION ALL SELECT award_id, slot, position, author_id, 'nih', false, NULL FROM r_nih
        UNION ALL SELECT award_id, slot, position, author_id, 'jev', false, NULL FROM r_jev)
  GROUP BY 1, 2, 3 HAVING count(DISTINCT author_id) = 1 AND max(anchor)),
ta AS (  -- decide.sql's ORCID guards, on the anchors: an author they refuse on its own slot must not spread by person id or NRID
  SELECT t0.award_id, t0.slot, t0.position, t0.author_id FROM t0 JOIN pe p USING (award_id, slot, position)
  LEFT JOIN (SELECT author_id, max(orcid) AS orcid FROM __P__profile_orcids GROUP BY 1) o ON o.author_id = t0.author_id
  LEFT JOIN (SELECT orcid, author_id FROM __P__profile_orcids WHERE n_profiles = 1) g ON g.orcid = p.orcid
  LEFT JOIN (SELECT author_id, max(majority_orcid) AS majority_orcid FROM openalex.authors.orcid_majority_lookup GROUP BY 1) mo
    ON mo.author_id = t0.author_id
  WHERE NOT coalesce(o.orcid <> p.orcid OR g.author_id <> t0.author_id OR (t0.has_orcid AND mo.majority_orcid IS NOT NULL
                     AND NOT array_contains(transform(t0.orcids, x -> concat('https://orcid.org/', x)), mo.majority_orcid)), false)),
ia AS (SELECT s1.person_id, min(ta.author_id) AS author_id, count(*) AS ties FROM s1 JOIN ta USING (award_id, slot, position)
       GROUP BY 1 HAVING count(DISTINCT ta.author_id) = 1),
r_fid AS (
  SELECT s1.award_id, s1.slot, s1.position, ia.author_id, 'funder_person_id' AS method,
    to_json(named_struct('person_id', s1.person_id, 'ties', ia.ties)) AS evidence
  FROM s1 JOIN ia USING (person_id)
),
-- kaken_nrid is by slot, not name: a lead printed in kanji has no name key
kn AS (SELECT project_id, min(pi_nrid) AS nrid FROM openalex.awards.kaken_projects_raw
       WHERE pi_nrid RLIKE '^[0-9]+$' GROUP BY 1 HAVING count(DISTINCT pi_nrid) = 1),
kl AS (SELECT p.award_id, p.slot, p.position, kn.nrid FROM pe p JOIN kn ON kn.project_id = p.funder_award_id
       WHERE p.provenance = 'kaken' AND p.slot = 'lead_investigator'),
ka AS (SELECT kl.nrid, min(ta.author_id) AS author_id, count(*) AS ties FROM kl JOIN ta USING (award_id, slot, position)
       GROUP BY 1 HAVING count(DISTINCT ta.author_id) = 1),
r_kaken AS (  -- a number one of whose anchors kpx dropped spreads nothing (that anchor may have been the one that disagreed)
  SELECT kl.award_id, kl.slot, kl.position, ka.author_id, 'kaken_nrid' AS method, to_json(named_struct('nrid', kl.nrid, 'ties', ka.ties)) AS evidence
  FROM kl JOIN ka USING (nrid)
  LEFT ANTI JOIN (SELECT DISTINCT kl.nrid FROM kl JOIN kpx ON kpx.award_id = kl.award_id AND kpx.slot = kl.slot AND kpx.position = kl.position) kx
    ON kx.nrid = kl.nrid
),
-- nsfc_name: nsfc_pinyin is a temp view RunSql builds this run from __P__people (iaa.nsfc_pinyin_view: pypinyin, two-character given
-- names). Graded 11-09 with the rule frozen: 600 fresh links, 549/553 resolved right (95% low 98.2%); same-institution plants 23/23.
nz AS (SELECT DISTINCT award_id, slot, position, f, g, k AS key FROM nsfc_pinyin LATERAL VIEW explode(array(concat(f, g), concat(g, f))) t AS k),
nlw AS (SELECT DISTINCT award_id, work_id FROM __P__paper_authors WHERE award_id IN (SELECT award_id FROM nz)),
nau AS (SELECT w.work_id, try_cast(regexp_extract(a.author.id, '([0-9]+)$', 1) AS BIGINT) AS author_id,
          array_distinct(concat(transform(coalesce(a.institutions, array()), i -> try_cast(regexp_extract(i.id, '([0-9]+)$', 1) AS BIGINT)),
            flatten(transform(coalesce(a.institutions, array()),
              i -> transform(coalesce(i.lineage, array()), l -> try_cast(regexp_extract(l, '([0-9]+)$', 1) AS BIGINT)))))) AS ids,
          filter(split(regexp_replace(translate(lower(a.raw_author_name), 'üǖǘǚǜāáǎàēéěèīíǐìōóǒòūúǔù', 'uuuuuaaaaeeeeiiiioooouuuu'),
            '[^a-z]+', ' '), ' '), x -> x <> '') AS tk
        FROM openalex.works.work_authorships w LEFT SEMI JOIN nlw ON nlw.work_id = w.work_id LATERAL VIEW explode(w.authorships) t AS a),
nm AS (SELECT nz.award_id, nz.slot, nz.position, nz.f, nz.g, nau.work_id, nau.author_id, nau.ids
       FROM nau JOIN nlw ON nlw.work_id = nau.work_id JOIN nz ON nz.award_id = nlw.award_id AND nz.key = array_join(nau.tk, '')
       WHERE nau.author_id IS NOT NULL AND size(nau.tk) >= 2
         AND nz.f IN (get(nau.tk, 0), try_element_at(nau.tk, -1), concat(get(nau.tk, 0), get(nau.tk, 1)),
                      concat(try_element_at(nau.tk, -2), try_element_at(nau.tk, -1)))),
nme AS (SELECT m.award_id, m.slot, m.position, m.author_id, m.work_id, concat(m.f, ' ', m.g) AS pinyin, iid
        FROM nm m LATERAL VIEW explode(m.ids) t AS iid),
nmi AS (SELECT m.award_id, m.slot, m.position, m.author_id, min(m.pinyin) AS pinyin, count(DISTINCT m.work_id) AS works
        FROM nme m JOIN pe p ON p.award_id = m.award_id AND p.slot = m.slot AND p.position = m.position
        LEFT JOIN openalex.institutions.institution_ancestors ia ON ia.institution_id = m.iid
        WHERE array_contains(p.insts, m.iid) OR coalesce(arrays_overlap(p.insts, ia.lineage_ids), false)
        GROUP BY 1, 2, 3, 4),
nps AS (SELECT award_id, slot, position, min(author_id) AS author_id, max(pinyin) AS pinyin, max(works) AS works FROM nmi
        GROUP BY 1, 2, 3 HAVING count(DISTINCT author_id) = 1),
nwin AS (SELECT s.*, p.insts, a.start_year - 3 AS lo, coalesce(a.end_year, a.start_year) + 5 AS hi
         FROM nps s JOIN pe p ON p.award_id = s.award_id AND p.slot = s.slot AND p.position = s.position
         JOIN (SELECT id, max(start_year) AS start_year, max(end_year) AS end_year FROM openalex.awards.awards_api
               WHERE id IN (SELECT award_id FROM nps) GROUP BY 1) a ON a.id = s.award_id),
naw AS (SELECT DISTINCT work_id, author_id FROM openalex.works.work_authors WHERE author_id IN (SELECT author_id FROM nps)),
nwy AS (SELECT id AS work_id, publication_year AS y FROM openalex.works.openalex_works WHERE id IN (SELECT work_id FROM naw)),
nai AS (SELECT w.work_id, try_cast(regexp_extract(a.author.id, '([0-9]+)$', 1) AS BIGINT) AS author_id,
          array_distinct(concat(transform(coalesce(a.institutions, array()), i -> try_cast(regexp_extract(i.id, '([0-9]+)$', 1) AS BIGINT)),
            flatten(transform(coalesce(a.institutions, array()),
              i -> transform(coalesce(i.lineage, array()), l -> try_cast(regexp_extract(l, '([0-9]+)$', 1) AS BIGINT)))))) AS ids
        FROM openalex.works.work_authorships w LEFT SEMI JOIN naw ON naw.work_id = w.work_id LATERAL VIEW explode(w.authorships) t AS a),
ngx AS (  -- the profile's works in the window other than this grant's papers, and whether its authorship there is at the institution
  SELECT n.award_id, n.slot, n.position, i.work_id, coalesce(arrays_overlap(i.ids, n.insts), false) AS at_inst
  FROM nwin n JOIN nai i ON i.author_id = n.author_id JOIN nwy y ON y.work_id = i.work_id
  LEFT ANTI JOIN nlw ON nlw.award_id = n.award_id AND nlw.work_id = i.work_id
  WHERE y.y BETWEEN n.lo AND n.hi),
ng AS (  -- merged profiles: at least half of those works carry the grant institution
  SELECT award_id, slot, position, count(DISTINCT work_id) AS n_win, count(DISTINCT CASE WHEN at_inst THEN work_id END) AS n_inst
  FROM ngx GROUP BY 1, 2, 3 HAVING count(DISTINCT CASE WHEN at_inst THEN work_id END) >= 0.5 * count(DISTINCT work_id)),
r_nsfc AS (
  SELECT s.award_id, s.slot, s.position, s.author_id, 'nsfc_name' AS method,
    to_json(named_struct('pinyin', s.pinyin, 'works', s.works, 'inst_works', g.n_inst, 'window_works', g.n_win)) AS evidence
  FROM nps s JOIN ng g ON g.award_id = s.award_id AND g.slot = s.slot AND g.position = s.position
)
SELECT * FROM r_orcid UNION ALL SELECT * FROM r_paper UNION ALL SELECT * FROM r_nih UNION ALL SELECT * FROM r_fid
UNION ALL SELECT * FROM r_cr UNION ALL SELECT * FROM r_kaken
UNION ALL SELECT * FROM r_jev
UNION ALL SELECT * FROM r_nsfc
