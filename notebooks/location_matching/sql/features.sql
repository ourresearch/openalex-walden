-- Match_Locations_To_Works (oxjob #1668): model features for every (seed, candidate) pair = the training features.sql
-- (oxjobs location-work-match-model/work/features.sql) on this night's seeds, SQL-channel candidates and kNN top 10, with
-- candidate works at openalex_works __V__ (pre-run). Statement order and every feature are the training ones.
CREATE OR REPLACE TABLE openalex.works.location_work_match_fseeds AS
SELECT 'shadow' AS part, provenance, native_id_namespace, native_id, work_id, decision, prov, seed_id, weight
FROM openalex.works.location_work_match_seeds
;
CREATE OR REPLACE TABLE openalex.works.location_work_match_fp AS
WITH k AS (
  SELECT seed_id, CAST(regexp_extract(es_id, '([0-9]+)$', 1) AS BIGINT) AS cid, rank, score FROM openalex.works.location_work_match_knn WHERE rank BETWEEN 1 AND 10
),
c AS (SELECT seed_id, cid, channels FROM openalex.works.location_work_match_cands)
SELECT s.part, coalesce(c.seed_id, k.seed_id) AS seed_id, coalesce(c.cid, k.cid) AS cid,
  concat(coalesce(c.channels, CAST(array() AS ARRAY<STRING>)),
         CASE WHEN k.cid IS NOT NULL THEN array('emb') ELSE CAST(array() AS ARRAY<STRING>) END) AS channels,
  k.rank AS emb_rank, k.score AS emb_score
FROM c FULL JOIN k ON k.seed_id = c.seed_id AND k.cid = c.cid
JOIN openalex.works.location_work_match_fseeds s ON s.seed_id = coalesce(c.seed_id, k.seed_id)
WHERE coalesce(c.cid, k.cid) IN (SELECT id FROM openalex.works.location_work_match_keys)
;
-- Record side.
CREATE OR REPLACE TABLE openalex.works.location_work_match_fr AS
WITH sf AS (SELECT seed_id, nt, akey, keys FROM openalex.works.location_work_match_seed_feat)
SELECT s.part, s.seed_id, s.decision, s.prov AS stratum_prov, s.weight, s.work_id AS bound_work,
  sf.nt, sf.akey, sf.keys,
  l.provenance, l.type, l.raw_type, l.version, l.language, l.source_id, lower(l.publisher) AS publisher,
  year(l.published_date) AS yr, nullif(trim(l.volume), '') AS volume, nullif(trim(l.issue), '') AS issue,
  nullif(trim(l.first_page), '') AS first_page, l.is_retracted,
  size(l.authors) AS n_auth, coalesce(length(l.abstract), 0) AS abs_len, length(l.title) AS title_len,
  regexp_extract(get(filter(l.ids, i -> i.namespace = 'pmid'), 0).id, '([0-9]+)$', 1) AS pmid,
  get(filter(l.ids, i -> i.namespace = 'datacite_client'), 0).id AS dc_client,
  l.abstract, slice(l.authors, 1, 100) AS authors, l.ids
FROM openalex.works.location_work_match_fseeds s
JOIN openalex.works.locations_mapped l
  ON l.provenance = s.provenance AND l.native_id_namespace = s.native_id_namespace AND l.native_id = s.native_id
LEFT JOIN sf ON sf.seed_id = s.seed_id
QUALIFY row_number() OVER (PARTITION BY s.seed_id ORDER BY l.openalex_updated_dt DESC) = 1
;
-- Work side, at v17025.
CREATE OR REPLACE TABLE openalex.works.location_work_match_fw AS
WITH w AS (
  SELECT * FROM openalex.works.openalex_works VERSION AS OF __V__
  WHERE id IN (SELECT DISTINCT cid FROM openalex.works.location_work_match_fp)
)
SELECT w.id AS cid, f.nt, f.akey, f.keys,
  w.publication_year AS yr, w.type, w.language, lower(w.publisher) AS publisher,
  nullif(trim(w.biblio.volume), '') AS volume, nullif(trim(w.biblio.issue), '') AS issue,
  nullif(trim(w.biblio.first_page), '') AS first_page, w.is_retracted, w.is_xpac,
  size(w.authorships) AS n_auth, coalesce(length(w.abstract), 0) AS abs_len, length(w.title) AS title_len,
  regexp_extract(w.ids['pmid'], '([0-9]+)$', 1) AS pmid,
  w.locations_count, w.cited_by_count,
  CAST(exists(w.locations, l -> l.provenance = 'crossref') AS INT) AS w_crossref,
  CAST(exists(w.locations, l -> l.provenance = 'datacite') AS INT) AS w_datacite,
  CAST(exists(w.locations, l -> l.provenance = 'pubmed') AS INT) AS w_pubmed,
  CAST(exists(w.locations, l -> l.provenance IN ('repo', 'pmh')) AS INT) AS w_repo,
  CAST(exists(w.locations, l -> l.version = 'publishedVersion') AS INT) AS w_published,
  CAST(exists(w.locations, l -> l.version = 'submittedVersion') AS INT) AS w_submitted,
  CAST(exists(w.locations, l -> l.version = 'acceptedVersion') AS INT) AS w_accepted,
  w.primary_location.provenance AS w_prov, w.primary_location.version AS w_version,
  w.abstract, slice(w.authorships, 1, 100) AS authorships, w.locations
FROM w LEFT JOIN openalex.works.location_work_match_keys f ON f.id = w.id
;
-- Tokens, one row per (side, id, kind, token). Kinds: ti title tokens, ab abstract tokens (>= 4 chars, first 2,000 chars),
-- su author surnames (last token of family / name), oc ORCIDs, ky identity keys (#1577 keys: DOI / PMID / URL / native id),
-- do DOIs, dp DOI prefixes, so source ids, nu title tokens with a digit (years, volume / part numbers),
-- dc / dd DOIs registered with Crossref (crossref / pubmed records) / DataCite, for the same-agency DOI conflict.
CREATE OR REPLACE TABLE openalex.works.location_work_match_ftok AS
WITH t AS (
  SELECT 'r' AS side, seed_id AS id, 'ti' AS kind, x AS t FROM openalex.works.location_work_match_fr LATERAL VIEW explode(split(nt, ' ')) e AS x WHERE length(x) >= 2
  UNION ALL SELECT 'w', cid, 'ti', x FROM openalex.works.location_work_match_fw LATERAL VIEW explode(split(nt, ' ')) e AS x WHERE length(x) >= 2
  UNION ALL SELECT 'r', seed_id, 'ab', x FROM openalex.works.location_work_match_fr
    LATERAL VIEW explode(split(regexp_replace(lower(substr(abstract, 1, 2000)), '[^\\p{L}\\p{N}]+', ' '), ' ')) e AS x WHERE length(x) >= 4
  UNION ALL SELECT 'w', cid, 'ab', x FROM openalex.works.location_work_match_fw
    LATERAL VIEW explode(split(regexp_replace(lower(substr(abstract, 1, 2000)), '[^\\p{L}\\p{N}]+', ' '), ' ')) e AS x WHERE length(x) >= 4
  UNION ALL SELECT 'r', seed_id, 'su', regexp_replace(lower(translate(element_at(split(trim(
      CASE WHEN nullif(trim(a.family), '') IS NOT NULL THEN a.family WHEN a.name LIKE '%,%' THEN split_part(a.name, ',', 1) ELSE a.name END), ' +'), -1),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ', 'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}]', '')
    FROM openalex.works.location_work_match_fr LATERAL VIEW explode(authors) e AS a
  UNION ALL SELECT 'w', cid, 'su', regexp_replace(lower(translate(element_at(split(trim(
      CASE WHEN a.author.display_name LIKE '%,%' THEN split_part(a.author.display_name, ',', 1) ELSE a.author.display_name END), ' +'), -1),
      'ÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕØÚÙÛÜÝáàâäãåçéèêëíìîïñóòôöõøúùûüýÿ', 'AAAAAACEEEEIIIINOOOOOOUUUUYaaaaaaceeeeiiiinoooooouuuuyy')), '[^\\p{L}]', '')
    FROM openalex.works.location_work_match_fw LATERAL VIEW explode(authorships) e AS a
  UNION ALL SELECT 'r', seed_id, 'oc', upper(regexp_extract(a.orcid, '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9Xx])', 1))
    FROM openalex.works.location_work_match_fr LATERAL VIEW explode(authors) e AS a
  UNION ALL SELECT 'w', cid, 'oc', upper(regexp_extract(a.author.orcid, '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9Xx])', 1))
    FROM openalex.works.location_work_match_fw LATERAL VIEW explode(authorships) e AS a
  UNION ALL SELECT 'r', seed_id, 'ky', k FROM openalex.works.location_work_match_fr LATERAL VIEW explode(keys) e AS k
  UNION ALL SELECT 'w', cid, 'ky', k FROM openalex.works.location_work_match_fw LATERAL VIEW explode(keys) e AS k
  UNION ALL SELECT 'r', seed_id, 'do', substr(k, 3) FROM openalex.works.location_work_match_fr LATERAL VIEW explode(keys) e AS k WHERE k LIKE 'd:%'
  UNION ALL SELECT 'w', cid, 'do', substr(k, 3) FROM openalex.works.location_work_match_fw LATERAL VIEW explode(keys) e AS k WHERE k LIKE 'd:%'
  UNION ALL SELECT 'r', seed_id, 'dp', regexp_extract(k, '^d:(10\\.[0-9]+)/', 1) FROM openalex.works.location_work_match_fr LATERAL VIEW explode(keys) e AS k WHERE k LIKE 'd:%'
  UNION ALL SELECT 'w', cid, 'dp', regexp_extract(k, '^d:(10\\.[0-9]+)/', 1) FROM openalex.works.location_work_match_fw LATERAL VIEW explode(keys) e AS k WHERE k LIKE 'd:%'
  UNION ALL SELECT 'r', seed_id, 'nu', x FROM openalex.works.location_work_match_fr LATERAL VIEW explode(split(nt, ' ')) e AS x WHERE x RLIKE '[0-9]'
  UNION ALL SELECT 'w', cid, 'nu', x FROM openalex.works.location_work_match_fw LATERAL VIEW explode(split(nt, ' ')) e AS x WHERE x RLIKE '[0-9]'
  UNION ALL SELECT 'r', seed_id, CASE WHEN provenance = 'datacite' THEN 'dd' ELSE 'dc' END, substr(k, 3)
    FROM openalex.works.location_work_match_fr LATERAL VIEW explode(keys) e AS k WHERE k LIKE 'd:%' AND provenance IN ('crossref', 'pubmed', 'datacite')
  -- PubMed and repo records carry their DOI in ids, not in the #1577 keys. PubMed DOIs are Crossref's; a repo DOI's agency
  -- is unknown, so it joins do / dp only.
  UNION ALL SELECT 'r', seed_id, k, d FROM (
    SELECT seed_id, provenance, regexp_replace(lower(trim(i.id)), '^https?://(dx\\.)?doi\\.org/', '') AS d
    FROM openalex.works.location_work_match_fr LATERAL VIEW explode(ids) e AS i WHERE lower(i.namespace) = 'doi'
  ) LATERAL VIEW explode(CASE WHEN provenance = 'pubmed' THEN array('do', 'dc') ELSE array('do') END) e2 AS k
  UNION ALL SELECT 'r', seed_id, 'dp', regexp_extract(lower(trim(i.id)), '(10\\.[0-9]+)/', 1)
    FROM openalex.works.location_work_match_fr LATERAL VIEW explode(ids) e AS i WHERE lower(i.namespace) = 'doi'
  UNION ALL SELECT 'w', cid, CASE WHEN l.provenance = 'datacite' THEN 'dd' ELSE 'dc' END,
      regexp_replace(lower(l.doi), '^https?://(dx\\.)?doi\\.org/', '')
    FROM openalex.works.location_work_match_fw LATERAL VIEW explode(locations) e AS l WHERE l.provenance IN ('crossref', 'pubmed', 'datacite')
  UNION ALL SELECT 'r', seed_id, 'so', CAST(source_id AS STRING) FROM openalex.works.location_work_match_fr
  UNION ALL SELECT 'w', cid, 'so', regexp_extract(l.source.id, '([0-9]+)$', 1) FROM openalex.works.location_work_match_fw LATERAL VIEW explode(locations) e AS l
)
SELECT DISTINCT side, id, kind, t FROM t WHERE t IS NOT NULL AND t <> ''
;
CREATE OR REPLACE TABLE openalex.works.location_work_match_fcnt AS
SELECT side, id, kind, count(*) AS n FROM openalex.works.location_work_match_ftok GROUP BY 1, 2, 3
;
-- Each pair's record tokens first, then the work's tokens on (work, kind, token): the same counts as the training statement,
-- in an order the planner cannot turn into a token-level join of every record with every work (2026-10-26: that plan ran
-- 65 min on PROD-WH before it was cancelled; the replay's 6 s plan was pair-first).
CREATE OR REPLACE TABLE openalex.works.location_work_match_fpr AS
SELECT p.seed_id, p.cid, r.kind, r.t
FROM (SELECT DISTINCT seed_id, cid FROM openalex.works.location_work_match_fp) p
JOIN openalex.works.location_work_match_ftok r ON r.side = 'r' AND r.id = p.seed_id
;
CREATE OR REPLACE TABLE openalex.works.location_work_match_fov AS
SELECT pr.seed_id, pr.cid, pr.kind, count(*) AS inter
FROM openalex.works.location_work_match_fpr pr
JOIN openalex.works.location_work_match_ftok w ON w.side = 'w' AND w.id = pr.cid AND w.kind = pr.kind AND w.t = pr.t
GROUP BY 1, 2, 3
;
-- Declared relations between the record and the work, by class, in either direction: the record declares one to a DOI the
-- work holds (fwd), or a record holding one of the work's DOIs declares one to the record's DOI (rev). `mention` = a DOI
-- in the record's ids with no relationship (repository records listing the published DOI). sibling = both DOIs declare
-- a common version concept (or share a .vN base), as declared.sql.
CREATE OR REPLACE TABLE openalex.works.location_work_match_frel AS
WITH rd AS (SELECT DISTINCT id AS seed_id, t AS doi FROM openalex.works.location_work_match_ftok WHERE side = 'r' AND kind = 'do'),
wd AS (SELECT DISTINCT id AS cid, t AS doi FROM openalex.works.location_work_match_ftok WHERE side = 'w' AND kind = 'do'),
alld AS (SELECT doi FROM rd UNION SELECT doi FROM wd),
-- every declaration (self DOI, class, target DOI) touching a DOI of the pairs
dlx AS (
  SELECT DISTINCT lower(s.id) AS doi,
    CASE WHEN r.relationship IS NULL OR r.relationship IN ('None', '') THEN 'mention'
      ELSE CASE lower(regexp_replace(r.relationship, '[-_ ]', ''))
        WHEN 'isversionof' THEN 'version' WHEN 'hasversion' THEN 'version' WHEN 'isnewversionof' THEN 'version'
        WHEN 'ispreviousversionof' THEN 'version' WHEN 'ispreprintof' THEN 'version' WHEN 'haspreprint' THEN 'version'
        WHEN 'isidenticalto' THEN 'identical' WHEN 'isvariantformof' THEN 'identical' WHEN 'isoriginalformof' THEN 'identical'
        WHEN 'ispartof' THEN 'part' WHEN 'haspart' THEN 'part' WHEN 'ispublishedin' THEN 'part'
        WHEN 'issupplementto' THEN 'supplement' WHEN 'issupplementedby' THEN 'supplement'
        WHEN 'isderivedfrom' THEN 'derived' WHEN 'issourceof' THEN 'derived'
        ELSE 'other' END END AS class,
    regexp_replace(lower(r.id), '^https?://(dx\\.)?doi\\.org/', '') AS target
  FROM openalex.works.locations_mapped l
  LATERAL VIEW explode(filter(l.ids, x -> x.namespace = 'doi' AND x.relationship = 'self')) a AS s
  LATERAL VIEW explode(filter(l.ids, x -> x.namespace = 'doi' AND coalesce(x.relationship, 'None') <> 'self')) b AS r
  WHERE exists(l.ids, x -> x.namespace = 'doi' AND coalesce(x.relationship, 'None') <> 'self')
),
dl AS (
  SELECT * FROM dlx WHERE doi IN (SELECT doi FROM alld)
  UNION SELECT * FROM dlx WHERE target IN (SELECT doi FROM alld)
),
p AS (SELECT DISTINCT seed_id, cid FROM openalex.works.location_work_match_fp),
x AS (
  SELECT p.seed_id, p.cid, rd.doi AS sdoi, wd.doi AS cdoi
  FROM p JOIN rd ON rd.seed_id = p.seed_id JOIN wd ON wd.cid = p.cid
  WHERE rd.doi <> wd.doi
),
fwd AS (SELECT x.seed_id, x.cid, dl.class FROM x JOIN dl ON dl.doi = x.sdoi AND dl.target = x.cdoi),
rev AS (SELECT x.seed_id, x.cid, dl.class FROM x JOIN dl ON dl.doi = x.cdoi AND dl.target = x.sdoi),
-- repository records carry the DOI they mention without a self DOI: mention from the record's own ids
men AS (
  SELECT p.seed_id, p.cid, 'mention' AS class
  FROM p
  JOIN (SELECT r.seed_id, regexp_replace(lower(i.id), '^https?://(dx\\.)?doi\\.org/', '') AS doi
        FROM openalex.works.location_work_match_fr r
        LATERAL VIEW explode(filter(r.ids, i -> i.namespace = 'doi' AND (i.relationship IS NULL OR i.relationship = 'None'))) e AS i) m
    ON m.seed_id = p.seed_id
  JOIN wd ON wd.cid = p.cid AND wd.doi = m.doi
),
cs AS (
  SELECT doi, target AS concept FROM dl WHERE class = 'version'
  UNION SELECT target, doi FROM dl WHERE class = 'version'
  UNION SELECT doi, regexp_replace(doi, '\\.v[0-9]+$', '') FROM alld WHERE doi RLIKE '\\.v[0-9]+$'
  UNION SELECT regexp_replace(doi, '\\.v[0-9]+$', ''), doi FROM alld WHERE doi RLIKE '\\.v[0-9]+$'
),
sib AS (
  SELECT x.seed_id, x.cid, 'sibling' AS class FROM x JOIN cs a ON a.doi = x.sdoi JOIN cs b ON b.doi = x.cdoi AND b.concept = a.concept
),
dvn AS (  -- one DOI is the other's .vN, or one is the concept the other declares
  SELECT x.seed_id, x.cid, 'version' AS class FROM x JOIN cs ON cs.doi = x.sdoi AND cs.concept = x.cdoi
)
SELECT DISTINCT seed_id, cid, class FROM (
  SELECT * FROM fwd UNION ALL SELECT * FROM rev UNION ALL SELECT * FROM men UNION ALL SELECT * FROM sib UNION ALL SELECT * FROM dvn)
;
CREATE OR REPLACE TABLE openalex.works.location_work_match_ffan AS
SELECT nt, count(*) AS nt_fanout FROM openalex.works.location_work_match_keys
WHERE nt IN (SELECT nt FROM openalex.works.location_work_match_fr UNION SELECT nt FROM openalex.works.location_work_match_fw)
GROUP BY nt
;
-- One row per pair. Within-record features (rank / gap to the record's best candidate) let the model compare candidates.
CREATE OR REPLACE TABLE openalex.works.location_work_match_features AS
WITH ov AS (SELECT seed_id, cid, map_from_entries(collect_list(struct(kind, inter))) AS m FROM openalex.works.location_work_match_fov GROUP BY 1, 2),
rc AS (SELECT id, map_from_entries(collect_list(struct(kind, n))) AS m FROM openalex.works.location_work_match_fcnt WHERE side = 'r' GROUP BY id),
wc AS (SELECT id, map_from_entries(collect_list(struct(kind, n))) AS m FROM openalex.works.location_work_match_fcnt WHERE side = 'w' GROUP BY id),
rl AS (SELECT seed_id, cid, collect_set(class) AS cls FROM openalex.works.location_work_match_frel GROUP BY 1, 2),
b AS (
  SELECT p.part, p.seed_id, p.cid, r.decision, r.stratum_prov, r.weight,
    CAST(array_contains(p.channels, 'id') AS INT) AS ch_id, CAST(array_contains(p.channels, 'ti') AS INT) AS ch_ti,
    CAST(array_contains(p.channels, 'ay') AS INT) AS ch_ay, CAST(array_contains(p.channels, 'mh') AS INT) AS ch_mh,
    CAST(array_contains(p.channels, 'emb') AS INT) AS ch_emb, CAST(p.cid = r.bound_work AS INT) AS is_bound,
    p.emb_rank, p.emb_score,
    CAST(r.nt = w.nt AS INT) AS same_nt, CAST(r.akey = w.akey AS INT) AS same_akey, fa.nt_fanout,
    r.title_len AS title_len_a, w.title_len AS title_len_b,
    coalesce(rc.m['ti'], 0) AS ti_na, coalesce(wc.m['ti'], 0) AS ti_nb, coalesce(ov.m['ti'], 0) AS ti_inter,
    coalesce(rc.m['ab'], 0) AS ab_na, coalesce(wc.m['ab'], 0) AS ab_nb, coalesce(ov.m['ab'], 0) AS ab_inter,
    coalesce(rc.m['su'], 0) AS su_na, coalesce(wc.m['su'], 0) AS su_nb, coalesce(ov.m['su'], 0) AS su_inter,
    coalesce(rc.m['oc'], 0) AS oc_na, coalesce(wc.m['oc'], 0) AS oc_nb, coalesce(ov.m['oc'], 0) AS oc_inter,
    coalesce(rc.m['ky'], 0) AS ky_na, coalesce(wc.m['ky'], 0) AS ky_nb, coalesce(ov.m['ky'], 0) AS ky_inter,
    coalesce(rc.m['do'], 0) AS do_na, coalesce(wc.m['do'], 0) AS do_nb, coalesce(ov.m['do'], 0) AS do_inter,
    coalesce(rc.m['nu'], 0) AS nu_na, coalesce(wc.m['nu'], 0) AS nu_nb, coalesce(ov.m['nu'], 0) AS nu_inter,
    CAST(coalesce(rc.m['nu'], 0) + coalesce(wc.m['nu'], 0) > 2 * coalesce(ov.m['nu'], 0) AS INT) AS nu_mismatch,
    CAST((coalesce(rc.m['dc'], 0) > 0 AND coalesce(wc.m['dc'], 0) > 0 AND coalesce(ov.m['dc'], 0) = 0)
      OR (coalesce(rc.m['dd'], 0) > 0 AND coalesce(wc.m['dd'], 0) > 0 AND coalesce(ov.m['dd'], 0) = 0) AS INT) AS agency_doi_conflict,
    -- Split by agency: two DataCite DOIs are often versions of one item (.vN), two Crossref DOIs rarely.
    CAST(coalesce(rc.m['dc'], 0) > 0 AND coalesce(wc.m['dc'], 0) > 0 AND coalesce(ov.m['dc'], 0) = 0 AS INT) AS dc_conflict,
    CAST(coalesce(rc.m['dd'], 0) > 0 AND coalesce(wc.m['dd'], 0) > 0 AND coalesce(ov.m['dd'], 0) = 0 AS INT) AS dd_conflict,
    CASE WHEN coalesce(rc.m['dd'], 0) > 0 THEN coalesce(wc.m['dd'], 0) ELSE coalesce(wc.m['dc'], 0) END AS agency_dois_b,
    coalesce(ov.m['dp'], 0) AS dp_inter, coalesce(rc.m['so'], 0) AS so_na, coalesce(wc.m['so'], 0) AS so_nb, coalesce(ov.m['so'], 0) AS so_inter,
    r.n_auth AS n_auth_a, w.n_auth AS n_auth_b, r.abs_len AS abs_len_a, w.abs_len AS abs_len_b,
    w.yr - r.yr AS yr_diff, CAST(r.yr IS NULL AS INT) AS undated_a, CAST(w.yr IS NULL AS INT) AS undated_b,
    CASE WHEN r.pmid <> '' AND w.pmid <> '' THEN CAST(r.pmid = w.pmid AS INT) * 2 - 1 ELSE 0 END AS pmid_agree,
    CASE WHEN r.volume IS NOT NULL AND w.volume IS NOT NULL THEN CAST(r.volume = w.volume AS INT) * 2 - 1 ELSE 0 END AS vol_agree,
    CASE WHEN r.issue IS NOT NULL AND w.issue IS NOT NULL THEN CAST(r.issue = w.issue AS INT) * 2 - 1 ELSE 0 END AS issue_agree,
    CASE WHEN r.first_page IS NOT NULL AND w.first_page IS NOT NULL THEN CAST(r.first_page = w.first_page AS INT) * 2 - 1 ELSE 0 END AS page_agree,
    CASE WHEN r.language IS NOT NULL AND w.language IS NOT NULL THEN CAST(r.language = w.language AS INT) * 2 - 1 ELSE 0 END AS lang_agree,
    CASE WHEN r.publisher IS NOT NULL AND w.publisher IS NOT NULL THEN CAST(r.publisher = w.publisher AS INT) * 2 - 1 ELSE 0 END AS publisher_agree,
    r.provenance AS prov_a, r.type AS type_a, w.type AS type_b, r.version AS version_a, w.w_prov AS prov_b, w.w_version AS version_b,
    r.raw_type AS raw_type_a, r.dc_client, CAST(r.is_retracted AS INT) AS retracted_a, CAST(w.is_retracted AS INT) AS retracted_b,
    CAST(w.is_xpac AS INT) AS xpac_b, w.locations_count, w.cited_by_count,
    w.w_crossref, w.w_datacite, w.w_pubmed, w.w_repo, w.w_published, w.w_submitted, w.w_accepted,
    CAST(array_contains(rl.cls, 'version') AS INT) AS rel_version, CAST(array_contains(rl.cls, 'sibling') AS INT) AS rel_sibling,
    CAST(array_contains(rl.cls, 'identical') AS INT) AS rel_identical, CAST(array_contains(rl.cls, 'part') AS INT) AS rel_part,
    CAST(array_contains(rl.cls, 'supplement') AS INT) AS rel_supplement, CAST(array_contains(rl.cls, 'derived') AS INT) AS rel_derived,
    CAST(array_contains(rl.cls, 'mention') AS INT) AS rel_mention, CAST(array_contains(rl.cls, 'other') AS INT) AS rel_other
  FROM openalex.works.location_work_match_fp p
  JOIN openalex.works.location_work_match_fr r ON r.seed_id = p.seed_id
  JOIN openalex.works.location_work_match_fw w ON w.cid = p.cid
  LEFT JOIN ov ON ov.seed_id = p.seed_id AND ov.cid = p.cid
  LEFT JOIN rc ON rc.id = p.seed_id
  LEFT JOIN wc ON wc.id = p.cid
  LEFT JOIN rl ON rl.seed_id = p.seed_id AND rl.cid = p.cid
  LEFT JOIN openalex.works.location_work_match_ffan fa ON fa.nt = r.nt
),
j AS (
  SELECT *,
    ti_inter / greatest(ti_na + ti_nb - ti_inter, 1) AS ti_jacc, ti_inter / greatest(least(ti_na, ti_nb), 1) AS ti_cont,
    ab_inter / greatest(ab_na + ab_nb - ab_inter, 1) AS ab_jacc, ab_inter / greatest(least(ab_na, ab_nb), 1) AS ab_cont,
    su_inter / greatest(su_na + su_nb - su_inter, 1) AS su_jacc, su_inter / greatest(least(su_na, su_nb), 1) AS su_cont
  FROM b
)
SELECT *,
  count(*) OVER (PARTITION BY seed_id) AS n_cands,
  ti_jacc - max(ti_jacc) OVER (PARTITION BY seed_id) AS ti_jacc_gap,
  ab_jacc - max(ab_jacc) OVER (PARTITION BY seed_id) AS ab_jacc_gap,
  su_jacc - max(su_jacc) OVER (PARTITION BY seed_id) AS su_jacc_gap,
  emb_score - max(emb_score) OVER (PARTITION BY seed_id) AS emb_score_gap,
  sum(same_nt) OVER (PARTITION BY seed_id) AS n_same_nt,
  rank() OVER (PARTITION BY seed_id ORDER BY ti_jacc DESC) AS ti_jacc_rank
FROM j
;
SELECT part, count(*) n, count(DISTINCT seed_id) seeds, sum(ch_emb) emb, sum(same_nt) same_nt, sum(rel_version) rel_version,
  sum(rel_sibling) sib, sum(rel_mention) mention, sum(rel_supplement) supp, sum(CAST(do_inter > 0 AS INT)) same_doi, sum(nu_mismatch) nu_mismatch, sum(agency_doi_conflict) agency_conflict, avg(ti_jacc) ti_jacc
FROM openalex.works.location_work_match_features GROUP BY 1 ORDER BY 1
