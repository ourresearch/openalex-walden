-- Investigator authors (oxjob #1847): every person named on a grant, one row per (award_id, slot, position) of awards_api (position =
-- 0 for lead and co-lead, else the index in investigators[]), with the printed names, the cleaned grant ORCID (upper case keeps a
-- lowercase x), name keys, the grant's institutions (awarded + the person's affiliation RORs) and the window: 3 years before the start
-- to 5 after. Excluded funders (the temp view excluded_funders: a funder id, or every funder of a country) are dropped here only.
CREATE OR REPLACE TABLE __P__people CLUSTER BY (award_id) AS
WITH x AS (
  SELECT funder_id FROM excluded_funders WHERE funder_id IS NOT NULL
  UNION SELECT f.funder_id FROM openalex.funders.funders f JOIN excluded_funders e ON e.country_code = f.country_code
),
s AS (
  SELECT a.id AS award_id, a.funder_id, a.provenance, a.funder_award_id, a.doi AS award_doi, a.start_year, a.end_year, e.slot, e.position, e.p,
    transform(coalesce(a.institution_awarded, array()), i -> try_cast(regexp_extract(i.id, '([0-9]+)$', 1) AS BIGINT)) AS award_insts
  FROM openalex.awards.awards_api a LEFT ANTI JOIN x ON x.funder_id = a.funder_id
  LATERAL VIEW explode(concat(
    array(named_struct('slot', 'lead_investigator', 'position', 0, 'p', a.lead_investigator),
          named_struct('slot', 'co_lead_investigator', 'position', 0, 'p', a.co_lead_investigator)),
    transform(coalesce(a.investigators, array()), (p, i) -> named_struct('slot', 'investigators', 'position', i, 'p', p)))) t AS e
  WHERE e.p IS NOT NULL AND trim(coalesce(e.p.family_name, '')) <> ''
),
ror AS (SELECT lower(regexp_extract(ror, '([0-9a-z]{9})$', 1)) AS ror, min(id) AS iid FROM openalex.institutions.institutions_api
        WHERE ror IS NOT NULL GROUP BY 1),
ai AS (
  SELECT y.award_id, y.slot, y.position, collect_set(ror.iid) AS iids
  FROM (SELECT s.award_id, s.slot, s.position, i FROM s LATERAL VIEW explode(coalesce(s.p.affiliation.ids, array())) t AS i) y
  JOIN ror ON ror.ror = lower(regexp_extract(y.i.id, '([0-9a-z]{9})$', 1))
  WHERE upper(y.i.type) = 'ROR' GROUP BY 1, 2, 3
),
k AS (
  SELECT s.*, __P__name_tokens(s.p.given_name) AS gt, __P__name_tokens(s.p.family_name) AS ft, __P__family_keys(s.p.family_name) AS fam_keys,
    filter(array_distinct(concat(s.award_insts, coalesce(ai.iids, array()))), i -> i IS NOT NULL) AS insts
  FROM s LEFT JOIN ai ON ai.award_id = s.award_id AND ai.slot = s.slot AND ai.position = s.position
)
SELECT award_id, funder_id, provenance, funder_award_id, award_doi, slot, CAST(position AS INT) AS position,
  p.given_name, p.family_name, nullif(regexp_extract(upper(coalesce(p.orcid, '')), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), '') AS orcid,
  gt, ft, fam_keys, get(gt, 0) AS given_key, concat(array_join(ft, ''), '|', coalesce(get(gt, 0), '')) AS person_key,
  coalesce(p.given_name, '') RLIKE '[^/ ]+ +[^/ ]+ */' AS multi_name,  -- two people in one field ('John Smith / Mary'): never shortlisted
  p.affiliation.name AS aff_name, insts, start_year, end_year,
  coalesce(start_year - 3, end_year - 8, 0) AS lo, coalesce(start_year + 5, end_year + 2, 3000) AS hi
FROM k   -- a family name with no a-z key (Han, kana) stays: layer no_name_key
;
-- Shared piece: every live profile's ORCID, with how many profiles carry it (the ORCID and Crossref routes take n_profiles = 1;
-- decide.sql's ORCID guard reads any).
CREATE OR REPLACE TABLE __P__profile_orcids AS
SELECT author_id, orcid, count(*) OVER (PARTITION BY orcid) AS n_profiles
FROM (SELECT DISTINCT try_cast(regexp_extract(id, '([0-9]+)$', 1) AS BIGINT) AS author_id,
             nullif(regexp_extract(upper(coalesce(orcid, '')), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), '') AS orcid
      FROM openalex.authors.authors_api WHERE orcid IS NOT NULL)
WHERE orcid IS NOT NULL
;
-- Shared piece: the authors of the papers each grant funded (work_awards -> work_authors, the one source for "authors of a paper").
CREATE OR REPLACE TABLE __P__paper_authors CLUSTER BY (award_id) AS
SELECT DISTINCT w.award_id, w.work_id, a.author_id
FROM (SELECT try_cast(regexp_extract(award.id, '([0-9]+)$', 1) AS BIGINT) AS award_id, work_id FROM openalex.awards.work_awards) w
LEFT SEMI JOIN (SELECT DISTINCT award_id FROM __P__people) p ON p.award_id = w.award_id
JOIN openalex.works.work_authors a ON a.work_id = w.work_id
WHERE a.author_id IS NOT NULL
