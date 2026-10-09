# Databricks notebook source
# Create or Update Funders (redesign step 4): read -> resolve -> publish for the funder registry.
#   {T}funder               one row per F id ever issued, never deleted; status active | merged | withdrawn
#   {T}funder_id            every identifier -> F id: (id_type, id_value) unique; id_type ror | fundref | name
#   {T}funder_source_record ROR, cited ids, predecessor crosswalks and exact seed rows; fingerprint/outcome/reason
#   {T}funder_decision      append-only log of every non-trivial decision per run (mint, attach, alias, hold)
#   {T}funders_compat       snapshot with today's 22 openalex.funders.funders columns, so every reader keeps working
# Modes: seed (one time, from the current pinned funders version + deleted_funders) | daily (read, resolve, checks, publish).
# Sources: ROR (entities), committed FundRef predecessor crosswalk, Crossref/DataCite cited ids (matching evidence), edits.csv.
# ROR funder-typed records mint automatically. Citation feeds never mint or enter a mint-review list.
# Every funder keeps the ROR it has; one active funder per ROR id is PRIMARY (ror_primary) and resolves it. Mint ids keep today's rule (abs(xxhash64(key)) % 9e9, from RorFunderProposals); a mint id that
# was ever issued is held, never reused. Merges and withdrawals come only from edits.csv.
# Permissions: MINT_POLICY allows "auto", "review" or "off"; off never enters the mint review list. Ever-issued (D11): {T}f_ids_ever_issued holds every F id ever issued (live, deleted, minted here); a mint
# candidate found there fails the night closed, and every mint is added to it.
# Guards follow RorFunderProposals: fail closed before any write if a source is short, undated or shrinks.

import json
import re
import os

NORM = r"regexp_replace(trim(regexp_replace(lower({x}), '[^\\p{{L}}\\p{{N}}]+', ' ')), '^the ', '')"   # same as RorFunderProposals
RID = r"lower(regexp_extract({x}, '([0-9a-z]{{9}})/*$', 1))"
FR_RE = r"'^(?:(?:https?://)?(?:dx\\.)?(?:doi\\.org/)?10\\.13039/)?([0-9]{{5,}})$'"
MINT_MOD = 9000000000
MINT_POLICY = {"crossref": "off", "ror": "auto", "datacite": "off"}   # Only ROR funder-typed records create entities; off never queues a mint.
FUNDREF_INTERNAL_KEY = True   # Keep FundRef identifiers only as internal matching keys.
FUNDREF_PUBLIC = True   # Preserve legacy DOI columns until the link readers migrate (Codex #5).
ROR_OWNS_FIELDS = False   # Defer existing-record changes, including ROR attachment, until the first-sync impact is approved.
WITHDRAW_NO_ROR_NO_AWARD = False   # Withdraw this cohort only after Kyle confirms removal.
MINT_FEED = {"MINT": "crossref", "MINT_WITH_ROR": "crossref", "MINT_ROR": "ror", "MINT_ROR_DATACITE": "datacite"}


def nkeys(arr):
    """Name keys of an array<string>: normalized, >= 4 chars (RorFunderProposals rule)."""
    return f"array_distinct(filter(transform(coalesce({arr}, array()), x -> {NORM.format(x='x')}), x -> length(x) >= 4))"


def rid(x):
    return RID.format(x=x)


def frx(x):
    return f"regexp_extract(trim({x}), {FR_RE.format()}, 1)"


def tables(p):
    T = p["T"]
    return dict(F=T + "funder", I=T + "funder_id", S=T + "funder_source_record", D=T + "funder_decision", E=T + "f_ids_ever_issued",
                CR=T + "crossref_registry_raw", COMPAT=T + "funders_compat", API=T + "funders_api", APIH=T + "funders_api_hash")


# ---------------------------------------------------------------- DDL

def ddl(p):
    t = tables(p)
    return [
        ("create_funder", f"""CREATE TABLE {t['F']} (
  funder_id BIGINT NOT NULL, status STRING NOT NULL, merged_into_id BIGINT, ror_id STRING, ror_primary BOOLEAN, parent_id BIGINT,
  display_name STRING, alt_names ARRAY<STRING>, country_code STRING, homepage_url STRING, wikidata_id STRING, description STRING,
  image_url STRING, image_thumbnail_url STRING, created_date TIMESTAMP, updated_date TIMESTAMP, created_by STRING, run_id STRING,
  mint_key STRING) USING DELTA"""),
        ("create_funder_id", f"""CREATE TABLE {t['I']} (
  id_type STRING NOT NULL, id_value STRING NOT NULL, funder_id BIGINT NOT NULL, origin_funder_id BIGINT, is_primary BOOLEAN,
  source STRING, added_at TIMESTAMP, run_id STRING) USING DELTA"""),
        ("create_source_record", f"""CREATE TABLE {t['S']} (
  source STRING NOT NULL, source_key STRING NOT NULL, name STRING, alt_names ARRAY<STRING>, country STRING, ror_id STRING,
  fundref_ids ARRAY<STRING>, replaced_by ARRAY<STRING>, record_status STRING, is_funder BOOLEAN, homepage STRING, record_json STRING,
  fingerprint STRING, first_seen_at TIMESTAMP, fetched_at TIMESTAMP, changed_run_id STRING,
  outcome STRING, reason STRING, funder_id BIGINT, decided_run_id STRING) USING DELTA"""),
        ("create_decision", f"""CREATE TABLE {t['D']} (
  run_id STRING NOT NULL, decided_at TIMESTAMP, source STRING, source_key STRING, action STRING NOT NULL, funder_id BIGINT,
  ror_rid STRING, fundref STRING, display_name STRING, country STRING, evidence STRING) USING DELTA"""),
    ]


# ---------------------------------------------------------------- SEED (one time)

def _seed_ctes(p):
    """v41 rows with flattened merge roots, and the shared-ROR keeper rule. A ROR id held by several live funders stays on ONE member,
    by the best tier: 1 = the ROR record lists its FundRef and its display name is one of the ROR record's names; 2 = listed + any
    of its names agrees; 3 = listed; 4 = display name is a ROR name; 5 = any name agrees. Several members at tier 1 or 4 are
    same-name duplicates: the one with most works keeps the ROR (lowest F id on a tie) and the others are merge candidates.
    Several at tiers 2, 3 or 5, or no tier, is a tie: no member is primary (the ROR id resolves to nobody) and it is held for a person.
    Every member keeps its ror_id on the record (secondary); nothing is merged here (merges come only through edits.csv)."""
    return f"""v AS (SELECT * FROM {p['FUNDERS_V41']}),
r AS (
  SELECT a.funder_id,
    CASE WHEN a.merge_into_id IS NULL THEN NULL WHEN b.merge_into_id IS NULL THEN a.merge_into_id
         WHEN c.merge_into_id IS NULL THEN b.merge_into_id WHEN d.merge_into_id IS NULL THEN c.merge_into_id END root
  FROM v a LEFT JOIN v b ON b.funder_id = a.merge_into_id LEFT JOIN v c ON c.funder_id = b.merge_into_id
  LEFT JOIN v d ON d.funder_id = c.merge_into_id
),
rorx AS (
  SELECT {rid('id')} rid,
    array_distinct(filter(transform(flatten(transform(filter(coalesce(external_ids, array()), e -> lower(e.type) = 'fundref'),
      e -> coalesce(e.all, array()))), x -> {frx('x')}), x -> x <> '')) fundrefs,
    {nkeys("transform(filter(coalesce(names, array()), n -> NOT array_contains(n.types, 'acronym')), n -> n.value)")} nk
  FROM {p['ROR']}
),
live AS (
  SELECT v.funder_id, CAST(v.crossref_id AS STRING) fr, {rid('v.ror_id')} rid, {NORM.format(x='v.display_name')} dn, coalesce(w.works_count, 0) works,
    {nkeys("concat(array(v.display_name), coalesce(from_json(v.alternate_titles, 'array<string>'), array()))")} nk
  FROM v LEFT JOIN (SELECT id, works_count FROM {p['WORKS_COUNTS']}) w ON w.id = v.funder_id
  WHERE v.merge_into_id IS NULL AND nullif(v.ror_id, '') IS NOT NULL
),
shared AS (SELECT rid FROM live GROUP BY rid HAVING count(*) > 1),
mem AS (
  SELECT l.*, coalesce({str(FUNDREF_INTERNAL_KEY).lower()} AND array_contains(x.fundrefs, l.fr), false) in_xw, coalesce(array_contains(x.nk, l.dn), false) exact,
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
         WHEN k.n_at = 1 OR k.bt IN (1, 4) THEN 'SECONDARY_SHARED_ROR' ELSE 'HOLD_SHARED_ROR_TIE' END d,
    coalesce((k.n_at = 1 OR k.bt IN (1, 4)) AND m.funder_id <> k.keeper AND m.exact, false) merge_candidate
  FROM tier m LEFT JOIN keep k ON k.rid = m.rid
)"""


def seed(p):
    t, R = tables(p), p["RUN_ID"]
    ctes = _seed_ctes(p)
    dj = lambda f: f"get_json_object(before_image_json, '$.{f}')"
    stmts = ddl(p) + [
        ("seed_decisions", f"""INSERT INTO {t['D']}
WITH {ctes}
SELECT '{R}', current_timestamp(), 'openalex_v41', CAST(s.funder_id AS STRING), s.d, s.funder_id, s.rid, v.crossref_id,
  v.display_name, v.country_code,
  to_json(named_struct('tier', s.t, 'best_tier', s.bt, 'keeper', s.keeper, 'merge_candidate', s.merge_candidate, 'works', l.works))
FROM shared_dec s JOIN v ON v.funder_id = s.funder_id JOIN live l ON l.funder_id = s.funder_id"""),
        ("seed_funder", f"""INSERT INTO {t['F']}
WITH {ctes}
SELECT v.funder_id, CASE WHEN v.merge_into_id IS NULL THEN 'active' ELSE 'merged' END, r.root,
  nullif(v.ror_id, ''),   -- every funder keeps the ROR it has; only the primary one resolves the ROR id
  CASE WHEN nullif(v.ror_id, '') IS NULL OR v.merge_into_id IS NOT NULL THEN NULL WHEN s.d IN ('SECONDARY_SHARED_ROR', 'HOLD_SHARED_ROR_TIE') THEN false ELSE true END,
  CAST(NULL AS BIGINT),
  v.display_name, from_json(v.alternate_titles, 'array<string>'), v.country_code, v.homepage_url, v.wikidata_id, v.description,
  v.image_url, v.image_thumbnail_url, coalesce(try_cast(substr(v.created_date, 1, 19) AS TIMESTAMP), v.updated_date), v.updated_date,
  'seed_v41', '{R}', CAST(NULL AS STRING)
FROM v JOIN r ON r.funder_id = v.funder_id LEFT JOIN shared_dec s ON s.funder_id = v.funder_id
UNION ALL
SELECT funder_id, 'withdrawn', CAST(NULL AS BIGINT), nullif(ror_id, ''), CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), {dj('display_name')},
  from_json({dj('alternate_titles')}, 'array<string>'), {dj('country_code')}, {dj('homepage_url')}, {dj('wikidata_id')},
  {dj('description')}, {dj('image_url')}, {dj('image_thumbnail_url')}, try_cast(substr({dj('created_date')}, 1, 19) AS TIMESTAMP),
  deleted_at, 'seed_deleted', '{R}', CAST(NULL AS STRING)
FROM {p['DELETED']} WHERE funder_id NOT IN (SELECT funder_id FROM {p['FUNDERS_V41']})"""),
        ("seed_funder_id", f"""INSERT INTO {t['I']}
WITH {ctes}
SELECT 'fundref', CAST(v.crossref_id AS STRING), coalesce(r.root, v.funder_id), v.funder_id, true, 'seed_v41', current_timestamp(), '{R}'
FROM v JOIN r ON r.funder_id = v.funder_id WHERE v.crossref_id IS NOT NULL AND {str(FUNDREF_INTERNAL_KEY).lower()}
UNION ALL
SELECT 'fundref', CAST(d.crossref_id AS STRING), d.funder_id, d.funder_id, true, 'seed_deleted', current_timestamp(), '{R}'
FROM {p['DELETED']} d LEFT ANTI JOIN v ON CAST(v.crossref_id AS STRING) = CAST(d.crossref_id AS STRING)
WHERE {str(FUNDREF_INTERNAL_KEY).lower()} AND d.crossref_id IS NOT NULL AND d.funder_id NOT IN (SELECT funder_id FROM v)
UNION ALL
SELECT 'ror', {rid('f.ror_id')}, f.funder_id, f.funder_id, true, 'seed_v41', current_timestamp(), '{R}'
FROM {t['F']} f WHERE f.status = 'active' AND f.ror_primary
UNION ALL   -- a merge loser's own ROR id points at its winner, unless an active funder already holds it
SELECT 'ror', x.rid, max(x.root), max(x.funder_id), false, 'seed_v41_merged', current_timestamp(), '{R}'
FROM (SELECT {rid('v.ror_id')} rid, r.root, v.funder_id FROM v JOIN r ON r.funder_id = v.funder_id
      WHERE v.merge_into_id IS NOT NULL AND nullif(v.ror_id, '') IS NOT NULL) x
LEFT ANTI JOIN (SELECT {rid('ror_id')} rid FROM {t['F']} WHERE status = 'active' AND ror_id IS NOT NULL) a ON a.rid = x.rid   -- held by an active funder, primary or not
GROUP BY x.rid HAVING count(DISTINCT x.root) = 1"""),
        ("seed_source_v41", f"""INSERT INTO {t['S']}
WITH {ctes}
SELECT 'openalex_v41', CAST(v.funder_id AS STRING), v.display_name, from_json(v.alternate_titles, 'array<string>'), v.country_code,
  nullif(v.ror_id, ''), CASE WHEN v.crossref_id IS NOT NULL THEN array(CAST(v.crossref_id AS STRING)) END,
  CASE WHEN v.merge_into_id IS NOT NULL THEN array(CAST(v.merge_into_id AS STRING)) END,
  CASE WHEN v.merge_into_id IS NULL THEN 'active' ELSE 'merged' END, true, v.homepage_url,
  to_json(struct(v.*), map('timestampFormat', "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")),   -- microseconds, so the compat passthrough reproduces v41 exactly
  sha2(to_json(struct(v.*)), 256), current_timestamp(), current_timestamp(), '{R}',
  CASE WHEN s.d = 'HOLD_SHARED_ROR_TIE' THEN 'held' ELSE 'accepted' END,
  CASE WHEN s.d IS NOT NULL THEN s.d || CASE WHEN s.merge_candidate THEN ' (MERGE_CANDIDATE into F' || s.keeper || ')' ELSE '' END END,
  coalesce(r.root, v.funder_id), '{R}'
FROM v JOIN r ON r.funder_id = v.funder_id LEFT JOIN shared_dec s ON s.funder_id = v.funder_id"""),
        ever_issued(p),
    ]
    return stmts


def ever_issued(p):
    """D11: every F id ever issued, from the live table, deleted_funders and this registry's own mints (empty on a fresh seed)."""
    t = tables(p)
    return ("create_ever_issued", f"""CREATE TABLE {t['E']} USING DELTA AS
SELECT funder_id, 'live_v41' source, try_cast(substr(created_date, 1, 19) AS TIMESTAMP) issued_at FROM {p['FUNDERS_V41']}
UNION ALL
SELECT funder_id, 'deleted', deleted_at FROM {p['DELETED']} WHERE funder_id NOT IN (SELECT funder_id FROM {p['FUNDERS_V41']})
UNION ALL
SELECT funder_id, created_by, created_date FROM {t['F']} WHERE created_by LIKE 'mint%'""")


# ---------------------------------------------------------------- COMMITTED PREDECESSOR CROSSWALK

def predecessor_edges(path):
    """Read the complete frozen edge set; resolve successors against today's ROR external_ids below.
    successor_ror_id in the CSV is export-time provenance, not an override of daily ROR identity."""
    import csv
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    edges = sorted({(r["predecessor_fundref_id"], r["successor_fundref_id"]) for r in rows})
    assert edges and all(re.fullmatch(r"[0-9]{5,}", x) for edge in edges for x in edge), "Invalid predecessor bridge CSV"
    return "SELECT * FROM VALUES " + ", ".join(f"('{old}', '{new}')" for old, new in edges) + " AS edges(old_id, new_id)"


# ---------------------------------------------------------------- DAILY: source records

def guard_ror(p):
    """Same guard as RorFunderProposals: short, undated or history-incomplete ROR copy -> nothing written (runs before any write)."""
    return ("guard_ror", f"""SELECT assert_true(
  (SELECT count(*) FROM {p['ROR']}) >= {int(p['MIN_ROR_ROWS'])}
  AND (SELECT count_if(coalesce(array_contains(types, 'funder'), false)) FROM {p['ROR']}) >= {int(p['MIN_FUNDER_RECORDS'])}
  AND (SELECT max(updated_date) FROM {p['ROR']}) IS NOT NULL
  AND (SELECT count(*) FROM (SELECT DISTINCT {rid('id')} rid FROM {p['ROR']}) c
       LEFT ANTI JOIN (SELECT DISTINCT {rid('id')} rid FROM {p['ROR_RAW']}) h ON h.rid = c.rid) = 0,
  'ROR copy too small, undated, or history incomplete; nothing written (fail closed)') ok""")


def upsert_sources(p, bridge_path):
    t, R = tables(p), p["RUN_ID"]
    crosswalk = (predecessor_edges(bridge_path) if FUNDREF_INTERNAL_KEY
                 else "SELECT CAST(NULL AS STRING) old_id, CAST(NULL AS STRING) new_id WHERE false")
    return ("upsert_sources", f"""MERGE INTO {t['S']} t
USING (
  WITH ror AS (
    SELECT {rid('id')} rid, id ror_url, status, coalesce(array_contains(types, 'funder'), false) is_funder,
      nullif(trim(get(filter(names, n -> array_contains(n.types, 'ror_display')), 0).value), '') display_name,
      transform(coalesce(names, array()), n -> n.value) full_names,
      array_sort(array_distinct(filter(transform(flatten(transform(filter(coalesce(external_ids, array()), e -> lower(e.type) = 'fundref'),
        e -> coalesce(e.all, array()))), x -> {frx('x')}), x -> x <> ''))) fundrefs,
      nullif(upper(trim(get(locations, 0).geonames_details.country_code)), '') country,
      get(filter(links, l -> l.type = 'website'), 0).value homepage,
      transform(filter(coalesce(relationships, array()), r -> r.type = 'successor'), r -> {rid('r.id')}) successors,
      to_json(named_struct('s', status, 't', types, 'n', names, 'e', external_ids, 'r', relationships, 'l', locations, 'k', links)) j
    FROM {p['ROR']}
  )
  , edges AS ({crosswalk}),
  bridge AS (
    SELECT e.old_id, min(r.rid) rid FROM edges e JOIN (SELECT rid, explode(fundrefs) k FROM ror) r ON r.k = e.new_id
    LEFT ANTI JOIN (SELECT explode(fundrefs) k FROM ror) own ON own.k = e.old_id
    GROUP BY e.old_id HAVING count(DISTINCT r.rid) = 1
  ), xw AS (SELECT rid, array_sort(collect_set(old_id)) ids FROM bridge GROUP BY rid),
  cr_cited AS (
    SELECT {frx('f.doi')} v, max(f.name) name, count(DISTINCT lm.work_id) works
    FROM {p['CROSSREF_REFS']} lm LATERAL VIEW explode(lm.funders) AS f
    WHERE lm.provenance = 'crossref' AND lm.work_id IS NOT NULL AND {frx('f.doi')} <> '' GROUP BY 1
  ), dc AS (   -- DataCite fundingReferences: every ROR / FundRef id DataCite records name as a funder, with how many DOIs cite it
    SELECT kt, v, count(DISTINCT native_id) dois, max(funder_name) name FROM (
      SELECT native_id, funder_name, CASE WHEN lower(declared_type) = 'ror' THEN 'ror' ELSE 'fundref' END kt,   -- the declared namespace decides
        CASE WHEN lower(declared_type) = 'ror' THEN lower(regexp_extract(identifier, '(0[0-9a-z]{{8}})/*$', 1)) ELSE {frx('identifier')} END v
      FROM {p['DATACITE_REFS']}
      WHERE (lower(declared_type) = 'crossref funder id' AND {frx('identifier')} <> '')
         OR (lower(declared_type) = 'ror' AND trim(identifier) RLIKE '^((?i)https?://(www[.])?ror[.]org/)?0[0-9a-z]{{8}}/*$'))
    GROUP BY 1, 2
  )
  SELECT 'ror' source, r.rid source_key, display_name name, full_names alt_names, country, ror_url ror_id,
    array_sort(array_union(fundrefs, coalesce(xw.ids, array()))) fundref_ids,
    successors replaced_by, status record_status, is_funder, homepage, j record_json, sha2(concat(j, to_json(coalesce(xw.ids, array()))), 256) fingerprint
  FROM ror r LEFT JOIN xw ON xw.rid = r.rid
  WHERE is_funder OR size(fundrefs) > 0 OR r.rid IN (SELECT id_value FROM {t['I']} WHERE id_type = 'ror')
     OR r.rid IN (SELECT v FROM dc WHERE kt = 'ror')
     OR r.rid IN (SELECT {rid('ror_id')} FROM {t['F']} WHERE ror_id IS NOT NULL)
  UNION ALL   -- DataCite names funders by id but has no funder list: cited ids are matching evidence only
  SELECT 'datacite_cited', kt || ':' || v, name, CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    CASE WHEN kt = 'ror' THEN 'https://ror.org/' || v END, CASE WHEN kt = 'fundref' THEN array(v) END, CAST(NULL AS ARRAY<STRING>),
    'active', true, CAST(NULL AS STRING),
    to_json(named_struct('dois', dois)), sha2(concat_ws('|', kt, v, name, CAST(dois AS STRING)), 256)
  FROM dc
  UNION ALL
  SELECT 'crossref_cited', 'fundref:' || v, name, CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    CAST(NULL AS STRING), array(v), CAST(NULL AS ARRAY<STRING>), 'active', false, CAST(NULL AS STRING),
    to_json(named_struct('works', works)), sha2(concat_ws('|', v, name, CAST(works AS STRING)), 256)
  FROM cr_cited
  UNION ALL
  SELECT 'crossref_crosswalk', old_id, CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'https://ror.org/' || rid, array(old_id), CAST(NULL AS ARRAY<STRING>), 'active', false, CAST(NULL AS STRING),
    to_json(named_struct('ror', rid)), sha2(rid, 256)
  FROM bridge
) s
ON t.source = s.source AND t.source_key = s.source_key
WHEN MATCHED AND (t.fingerprint <> s.fingerprint OR t.record_status = 'gone') THEN UPDATE SET
  t.name = s.name, t.alt_names = s.alt_names, t.country = s.country, t.ror_id = s.ror_id, t.fundref_ids = s.fundref_ids,
  t.replaced_by = s.replaced_by, t.record_status = s.record_status, t.is_funder = s.is_funder, t.homepage = s.homepage,
  t.record_json = s.record_json, t.fingerprint = s.fingerprint, t.fetched_at = current_timestamp(), t.changed_run_id = '{R}'
WHEN NOT MATCHED THEN INSERT (source, source_key, name, alt_names, country, ror_id, fundref_ids, replaced_by, record_status, is_funder,
  homepage, record_json, fingerprint, first_seen_at, fetched_at, changed_run_id)
  VALUES (s.source, s.source_key, s.name, s.alt_names, s.country, s.ror_id, s.fundref_ids, s.replaced_by, s.record_status, s.is_funder,
  s.homepage, s.record_json, s.fingerprint, current_timestamp(), current_timestamp(), '{R}')
-- Legacy crossref_registry records retire to 'gone' once, then remain a no-op; keep their schema/history.
WHEN NOT MATCHED BY SOURCE AND t.source IN ('ror', 'crossref_registry', 'crossref_crosswalk', 'crossref_cited', 'datacite_cited') AND t.record_status <> 'gone' THEN UPDATE SET
  t.record_status = 'gone', t.changed_run_id = '{R}'""")


def matching_names(p):
    t = tables(p)
    return f"""SELECT f.funder_id,
  {nkeys("concat(array(f.display_name, seed.name), coalesce(f.alt_names, array()), coalesce(r.alt_names, array()), coalesce(a.names, array()))")} nk
FROM {t['F']} f
LEFT JOIN {t['S']} r ON r.source = 'ror' AND r.source_key = {rid('f.ror_id')} AND r.record_status <> 'gone'
LEFT JOIN {t['S']} seed ON seed.source = 'openalex_v41' AND seed.source_key = CAST(f.funder_id AS STRING)
LEFT JOIN (SELECT coalesce(f.merged_into_id, f.funder_id) funder_id, collect_set(a.name) names
  FROM {p['ALIASES']} a JOIN {t['F']} f ON f.funder_id = try_cast(regexp_extract(a.id, 'F([0-9]+)$', 1) AS BIGINT)
  WHERE upper(trim(a.keep)) = 'YES' GROUP BY 1) a ON a.funder_id = f.funder_id"""


# ---------------------------------------------------------------- DAILY: resolve

def decide(p):
    """Resolve ROR records; only funder-typed ROR records may mint. Sorted candidate sets keep holds identical on replay."""
    t = tables(p)
    return f"""WITH
names AS ({matching_names(p)}),
fa AS (SELECT f.funder_id, status, CASE WHEN ror_primary THEN {rid('ror_id')} END rid, country_code, n.nk
       FROM {t['F']} f JOIN names n ON n.funder_id = f.funder_id),
act AS (SELECT * FROM fa WHERE status = 'active'),
fid_fr AS (SELECT id_value k, funder_id FROM {t['I']} WHERE id_type = 'fundref'),
fid_ror AS (SELECT id_value rid, funder_id FROM {t['I']} WHERE id_type = 'ror'),
ror AS (SELECT r.source_key rid, r.ror_id ror_url, r.name, r.alt_names, r.country, r.fundref_ids fundrefs, r.is_funder, r.homepage,
          coalesce(from_json(get_json_object(r.record_json, '$.t'), 'array<string>'), array()) types,
          {nkeys("concat(array(r.name), coalesce(r.alt_names, array()))")} nk
        FROM {t['S']} r
        WHERE r.source = 'ror' AND r.record_status = 'active'),
fr_rors AS (SELECT k, array_sort(collect_set(rid)) rids FROM (SELECT rid, explode(fundrefs) k FROM ror) GROUP BY k),
allfr AS (SELECT * FROM fid_fr),
tinfo AS (SELECT funder_id, rid, nk, country_code FROM act),
owned AS (SELECT rid FROM fid_ror),
r1 AS (SELECT r.* FROM ror r LEFT ANTI JOIN owned o ON o.rid = r.rid),
r_t AS (   -- FundRef targets of each unowned ROR record, with name agreement and whether the target already has a ROR
  SELECT x.rid, x.k, a.funder_id, coalesce(arrays_overlap(x.nk, ti.nk), false) name_ok, ti.rid IS NOT NULL has_ror,
    size(coalesce(fr.rids, array())) n_rors
  FROM (SELECT rid, nk, explode(fundrefs) k FROM r1) x JOIN allfr a ON a.k = x.k JOIN tinfo ti ON ti.funder_id = a.funder_id
  LEFT JOIN fr_rors fr ON fr.k = x.k
  LEFT ANTI JOIN (SELECT split_part(id_value, ':', 1) rid, funder_id FROM {t['I']} WHERE id_type = 'ror_cleared') rc   -- a person cleared it
    ON rc.rid = x.rid AND rc.funder_id = a.funder_id
),
r_agg AS (
  SELECT rid, array_sort(collect_set(funder_id)) tids, array_sort(collect_set(CASE WHEN name_ok THEN funder_id END)) cands,
    array_sort(collect_set(CASE WHEN name_ok AND NOT has_ror THEN funder_id END)) cands_free,
    max(CASE WHEN name_ok THEN n_rors END) cand_n_rors
  FROM r_t GROUP BY rid
),
r_w AS (SELECT x.rid, array_sort(collect_set(f.funder_id)) ids FROM (SELECT rid, explode(fundrefs) k FROM r1) x JOIN allfr a ON a.k = x.k
        JOIN {t['F']} f ON f.funder_id = a.funder_id AND f.status = 'withdrawn' GROUP BY x.rid),
r_name AS (SELECT x.rid, array_sort(collect_set(a.funder_id)) ids FROM (SELECT rid, country, explode(nk) n FROM r1) x
           JOIN (SELECT funder_id, country_code, explode(nk) n FROM act WHERE rid IS NULL) a ON a.n = x.n AND a.country_code = x.country
           JOIN r1 rr ON rr.rid = x.rid AND a.funder_id <> abs(xxhash64(rr.ror_url)) % {MINT_MOD}
           GROUP BY x.rid),
r2 AS (
  SELECT r1.*, coalesce(g.tids, array()) tids, coalesce(g.cands, array()) cands, coalesce(g.cands_free, array()) cands_free,
    g.cand_n_rors, coalesce(rn.ids, array()) name_ids, coalesce(rw.ids, array()) withdrawn_ids, abs(xxhash64(r1.ror_url)) % {MINT_MOD} mint_id,
    CASE
      WHEN size(coalesce(g.tids, array())) > 0 THEN CASE
        WHEN size(g.cands) = 0 THEN 'HOLD_NAME_DISAGREES'            -- 9 of 10 such links were programmes on the parent's record (09-28 grading)
        WHEN size(g.cands) > 1 THEN 'HOLD_FUNDREF_MULTI_FUNDER'
        WHEN size(g.cands_free) = 0 THEN 'HOLD_FUNDER_HAS_OTHER_ROR'
        WHEN g.cand_n_rors > 1 THEN 'HOLD_FUNDREF_MULTI_ROR'
        ELSE '{'ATTACH_ROR' if ROR_OWNS_FIELDS else 'DEFER_ATTACH_ROR'}' END
      WHEN NOT r1.is_funder THEN 'IGNORE'
      WHEN size(coalesce(rw.ids, array())) > 0 THEN 'HOLD_FUNDREF_OF_WITHDRAWN'   -- its FundRef belongs to a funder deleted by hand
      WHEN size(coalesce(rn.ids, array())) > 0 THEN 'HOLD_NAME_ONLY_MATCH'
      WHEN r1.name IS NULL OR r1.country IS NULL THEN 'HOLD_MINT_INCOMPLETE'
      WHEN e.funder_id IS NOT NULL THEN 'HOLD_ID_COLLISION'
      ELSE 'MINT_ROR' END action
  FROM r1 LEFT JOIN r_agg g ON g.rid = r1.rid LEFT JOIN r_name rn ON rn.rid = r1.rid LEFT JOIN r_w rw ON rw.rid = r1.rid
  LEFT JOIN (SELECT funder_id, mint_key FROM {t['F']}) e ON e.funder_id = abs(xxhash64(r1.ror_url)) % {MINT_MOD}
    AND e.mint_key IS DISTINCT FROM 'ror:' || r1.rid
),
r_multi AS (SELECT get(cands_free, 0) f FROM r2 WHERE action = 'ATTACH_ROR' GROUP BY 1 HAVING count(*) > 1),
rfin AS (SELECT r2.* EXCEPT (action), CASE WHEN action = 'ATTACH_ROR' AND m.f IS NOT NULL THEN 'HOLD_FUNDER_MULTI_ROR' ELSE action END action
         FROM r2 LEFT JOIN r_multi m ON m.f = get(r2.cands_free, 0))
SELECT 'ror' source, rid source_key, action,
  CASE WHEN action = 'MINT_ROR' THEN mint_id WHEN action IN ('ATTACH_ROR', 'DEFER_ATTACH_ROR') THEN get(cands_free, 0) WHEN action = 'HOLD_ID_COLLISION' THEN mint_id
       WHEN action IN ('HOLD_FUNDER_HAS_OTHER_ROR', 'HOLD_FUNDREF_MULTI_ROR', 'HOLD_FUNDER_MULTI_ROR') THEN get(cands, 0)
       WHEN action = 'HOLD_NAME_DISAGREES' THEN get(tids, 0) WHEN action = 'HOLD_NAME_ONLY_MATCH' THEN get(name_ids, 0)
       WHEN action = 'HOLD_FUNDREF_OF_WITHDRAWN' THEN get(withdrawn_ids, 0) END funder_id,
  rid ror_rid, ror_url, CAST(NULL AS STRING) fundref, name display_name, alt_names, country, homepage,
  to_json(named_struct('fundrefs', fundrefs, 'fundref_targets', tids, 'name_agreeing', cands, 'name_agreeing_without_ror', cands_free,
    'cand_fundref_rors', cand_n_rors, 'name_matches', name_ids, 'withdrawn_targets', withdrawn_ids, 'is_funder', is_funder, 'types', types)) evidence
FROM rfin WHERE action <> 'IGNORE'"""


def materialize(p):
    t, R = tables(p), p["RUN_ID"]
    return ("decide", f"""CREATE OR REPLACE TABLE {t['D']}_tonight AS
SELECT '{R}' run_id, current_timestamp() decided_at, * FROM ({decide(p)})""")


def hold_batch_collisions(p):
    """Two records minting the same F id in one run: hold both (the uniqueness check would only catch it after the insert)."""
    N = tables(p)["D"] + "_tonight"
    return ("hold_batch_collisions", f"""UPDATE {N} SET action = 'HOLD_ID_COLLISION'
WHERE action LIKE 'MINT%' AND funder_id IN (SELECT funder_id FROM {N} WHERE action LIKE 'MINT%' GROUP BY 1 HAVING count(*) > 1)""")


def review_mints(p):
    """Review queues a candidate; off ignores it entirely. Citation feeds never enter the candidate resolver."""
    N = tables(p)["D"] + "_tonight"
    assert set(MINT_POLICY.values()) <= {"auto", "review", "off"}
    gated = {a: ("IGNORE_MINT_OFF" if MINT_POLICY[f] == "off" else f"REVIEW_MINT_{f.upper()}")
             for a, f in MINT_FEED.items() if MINT_POLICY[f] != "auto"}
    if not gated:
        return ("review_mints", "SELECT 0 ok")
    case = " ".join(f"WHEN '{a}' THEN '{action}'" for a, action in gated.items())
    return ("review_mints", f"UPDATE {N} SET action = CASE action {case} END WHERE action IN ({', '.join(repr(a) for a in gated)})")


def never_issued(p):
    """D11: a mint candidate's F id must never have been issued before, unless it is this same key's own mint resuming (mint_key)."""
    t = tables(p)
    N = t["D"] + "_tonight"
    return ("never_issued", f"""SELECT assert_true((SELECT count(*) FROM {N} d JOIN {t['E']} e ON e.funder_id = d.funder_id
  LEFT ANTI JOIN {t['F']} f ON f.funder_id = d.funder_id AND f.mint_key = CASE WHEN d.source = 'ror' THEN 'ror:' ELSE 'fundref:' END || d.source_key
  WHERE d.action LIKE 'MINT%') = 0, 'a mint candidate F id was issued before (ever-issued table); nothing applied (fail closed)') ok""")


def mint_fuse(p, max_mints):
    t = tables(p)
    return ("mint_fuse", f"""SELECT assert_true((SELECT count(*) FROM {t['D']}_tonight WHERE action LIKE 'MINT%') <= {int(max_mints)},
  'more than {int(max_mints)} mints tonight: stop for a person (fuse); nothing applied') ok""")


def apply(p):
    t, R = tables(p), p["RUN_ID"]
    N = f"{t['D']}_tonight"
    return [
        ("log_decisions", f"""INSERT INTO {t['D']} SELECT run_id, decided_at, source, source_key, action, funder_id, ror_rid, fundref,
  display_name, country, evidence FROM {N}"""),
        ("mint_funders", f"""MERGE INTO {t['F']} f USING (SELECT * FROM {N} WHERE action LIKE 'MINT%') d ON f.funder_id = d.funder_id
WHEN NOT MATCHED THEN INSERT (funder_id, status, ror_id, ror_primary, display_name, alt_names, country_code, homepage_url, created_date, updated_date,
  created_by, run_id, mint_key)
VALUES (d.funder_id, 'active', d.ror_url, CASE WHEN d.ror_url IS NOT NULL THEN true END, d.display_name, d.alt_names, d.country, d.homepage, current_timestamp(), current_timestamp(),
  'mint_ror', '{R}',
  CASE WHEN d.source = 'ror' THEN 'ror:' ELSE 'fundref:' END || d.source_key)"""),
        ("issue_ids", f"""INSERT INTO {t['E']} SELECT f.funder_id, f.created_by, f.created_date FROM {t['F']} f
JOIN (SELECT DISTINCT funder_id FROM {N} WHERE action LIKE 'MINT%') d ON d.funder_id = f.funder_id
LEFT ANTI JOIN {t['E']} e ON e.funder_id = f.funder_id"""),
        ("attach_ror", f"""MERGE INTO {t['F']} f USING (SELECT * FROM {N} WHERE action = 'ATTACH_ROR') d ON f.funder_id = d.funder_id
WHEN MATCHED AND (f.ror_id IS NULL OR NOT f.ror_primary) AND f.status = 'active'   -- a secondary (shared) ROR gives way to the funder's own
  THEN UPDATE SET f.ror_id = d.ror_url, f.ror_primary = true, f.updated_date = current_timestamp(), f.run_id = '{R}'"""),
        sync_fields(p),
        withdraw_unlinked(p),
        ("add_ids", f"""MERGE INTO {t['I']} i USING (
  SELECT 'ror' id_type, ror_rid id_value, funder_id, funder_id origin, true prim, lower(action) src
  FROM {N} WHERE action IN ('MINT_ROR', 'ATTACH_ROR')
  UNION ALL
  SELECT 'fundref', x.k, min(i.funder_id), min(i.funder_id), false, 'ror_crosswalk'
  FROM (SELECT source_key, explode(fundref_ids) k FROM {t['S']} WHERE source = 'ror' AND record_status = 'active') x
  JOIN (SELECT id_value, funder_id FROM {t['I']} WHERE id_type = 'ror' UNION SELECT ror_rid, funder_id FROM {N} WHERE action IN ('MINT_ROR', 'ATTACH_ROR')) i ON i.id_value = x.source_key
  WHERE {str(FUNDREF_INTERNAL_KEY).lower()} AND x.k NOT IN (
    SELECT k FROM (SELECT source_key, explode(fundref_ids) k FROM {t['S']} WHERE source = 'ror' AND record_status = 'active')
    GROUP BY k HAVING count(DISTINCT source_key) > 1)
  GROUP BY x.k HAVING count(DISTINCT x.source_key) = 1
) d ON i.id_type = d.id_type AND i.id_value = d.id_value
WHEN NOT MATCHED THEN INSERT (id_type, id_value, funder_id, origin_funder_id, is_primary, source, added_at, run_id)
VALUES (d.id_type, d.id_value, d.funder_id, d.origin, d.prim, d.src, current_timestamp(), '{R}')"""),
        ("refresh_names", f"""MERGE INTO {t['I']} i USING (   -- name keys owned by exactly one active funder; rebuilt every run
  SELECT n id_value, min(funder_id) funder_id FROM (
    SELECT f.funder_id, explode(n.nk) n FROM {t['F']} f JOIN ({matching_names(p)}) n ON n.funder_id = f.funder_id WHERE f.status = 'active')
  GROUP BY n HAVING count(DISTINCT funder_id) = 1
) d ON i.id_type = 'name' AND i.id_value = d.id_value
WHEN MATCHED AND i.funder_id <> d.funder_id THEN UPDATE SET i.funder_id = d.funder_id, i.origin_funder_id = d.funder_id, i.run_id = '{R}'
WHEN NOT MATCHED THEN INSERT (id_type, id_value, funder_id, origin_funder_id, is_primary, source, added_at, run_id)
  VALUES ('name', d.id_value, d.funder_id, d.funder_id, false, 'name_unique', current_timestamp(), '{R}')
WHEN NOT MATCHED BY SOURCE AND i.id_type = 'name' THEN DELETE"""),
        ("source_outcomes", f"""MERGE INTO {t['S']} s USING (
  SELECT source, source_key, CASE WHEN action LIKE 'HOLD%' OR action LIKE 'REVIEW%' THEN 'held' WHEN action LIKE 'DEFER%' THEN 'deferred' WHEN action LIKE 'IGNORE%' THEN 'unmatched' ELSE 'accepted' END outcome,
    CASE WHEN action LIKE 'MINT%' OR action = 'ATTACH_ROR' THEN 'MATCH' ELSE action END reason, funder_id FROM {N}
  UNION ALL   -- records already resolved by an id we hold
  SELECT s.source, s.source_key, 'accepted', 'MATCH', i.funder_id FROM {t['S']} s
  JOIN {t['I']} i ON i.id_type = CASE s.source WHEN 'ror' THEN 'ror' ELSE 'fundref' END AND i.id_value = s.source_key
  LEFT ANTI JOIN {N} n ON n.source = s.source AND n.source_key = s.source_key
  WHERE s.source IN ('ror', 'crossref_crosswalk')
  UNION ALL   -- DataCite-cited ids: accepted once a funder holds the id, else unmatched evidence, never a mint review
  SELECT s.source, s.source_key, CASE WHEN coalesce(i.funder_id, n.funder_id) IS NOT NULL THEN 'accepted' ELSE 'unmatched' END,
    CASE WHEN i.funder_id IS NOT NULL THEN 'MATCH' WHEN n.funder_id IS NOT NULL THEN 'MATCH_NAME' ELSE 'CITED_ID_UNMATCHED_MINT_OFF' END, coalesce(i.funder_id, n.funder_id)
  FROM {t['S']} s LEFT JOIN {t['I']} i ON i.id_type = split_part(s.source_key, ':', 1) AND i.id_value = split_part(s.source_key, ':', 2)
  LEFT JOIN {t['I']} n ON n.id_type = 'name' AND n.id_value = {NORM.format(x='s.name')}
  WHERE s.source IN ('datacite_cited', 'crossref_cited') AND s.record_status <> 'gone'
) d ON s.source = d.source AND s.source_key = d.source_key
WHEN MATCHED AND (s.outcome IS DISTINCT FROM d.outcome OR s.reason IS DISTINCT FROM d.reason OR s.funder_id IS DISTINCT FROM d.funder_id)
  THEN UPDATE SET s.outcome = d.outcome, s.reason = d.reason, s.funder_id = d.funder_id, s.decided_run_id = '{R}'"""),
    ]


def sync_fields(p):
    if not ROR_OWNS_FIELDS:
        return ("sync_fields", "SELECT 0 ok")
    t = tables(p)
    return ("sync_fields", f"""MERGE INTO {t['F']} f USING (SELECT * FROM {t['S']} WHERE source = 'ror' AND record_status <> 'gone') r
ON r.source_key = {rid('f.ror_id')} AND f.status = 'active'
WHEN MATCHED AND (f.display_name IS DISTINCT FROM r.name OR f.country_code IS DISTINCT FROM r.country OR f.homepage_url IS DISTINCT FROM r.homepage)
THEN UPDATE SET f.display_name = r.name, f.country_code = r.country, f.homepage_url = r.homepage,
  f.updated_date = current_timestamp(), f.run_id = '{p['RUN_ID']}'""")


def withdraw_unlinked(p):
    if not WITHDRAW_NO_ROR_NO_AWARD:
        return ("withdraw_unlinked", "SELECT 0 ok")
    t = tables(p)
    return ("withdraw_unlinked", f"""UPDATE {t['F']} SET status = 'withdrawn', updated_date = current_timestamp(), run_id = '{p['RUN_ID']}'
WHERE status = 'active' AND nullif(trim(ror_id), '') IS NULL
AND funder_id NOT IN (SELECT funder_id FROM {p['AWARDS']} WHERE funder_id IS NOT NULL)""")


# ---------------------------------------------------------------- EDITS (reviewed CSV; merges and withdrawals only come from here)

EDIT_ACTIONS = ("merge", "withdraw", "set_ror", "clear_ror")


def read_edits(path):
    import csv
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        rows = [r for r in csv.DictReader(f) if (r.get("action") or "").strip()]
    for r in rows:
        assert r["action"] in EDIT_ACTIONS, f"edits.csv: unknown action {r['action']!r}"
        assert (r.get("reviewed_by") or "").strip(), f"edits.csv: row for F{r['funder_id']} has no reviewed_by"
    return rows


def edit_statements(p, rows):
    """One statement list per edit, applied every night (a no-op once done). Each edit first asserts its preconditions, so a bad
    edits.csv row stops the run before it changes anything. A merge repoints the loser's ids and every row merged into the loser;
    no link table is touched. Cleared / withdrawn ROR ids leave a tombstone so the nightly resolver never re-attaches or re-mints them."""
    t, R = tables(p), p["RUN_ID"]
    out = []
    active = lambda f: f"(SELECT count(*) FROM {t['F']} WHERE funder_id = {f} AND status = 'active') = 1"
    tomb = lambda fid, cond: (f"UPDATE {t['I']} SET id_type = 'ror_cleared', id_value = id_value || ':{fid}', run_id = '{R}' "
                              f"WHERE funder_id = {fid} AND id_type = 'ror'{cond}")
    for r in rows:
        fid, tgt, act = int(r["funder_id"]), (r.get("target") or "").strip(), r["action"]
        if act == "merge":
            tgt = int(tgt)
            assert tgt != fid, f"edits.csv: F{fid} merged into itself"
            out += [(f"edit_merge_check_{fid}", f"""SELECT assert_true({active(tgt)} AND ({active(fid)} OR (SELECT count(*) FROM {t['F']}
  WHERE funder_id = {fid} AND status = 'merged' AND merged_into_id = {tgt}) = 1), 'edits.csv merge F{fid} -> F{tgt}: target not active or loser not active') ok"""),
                    (f"edit_merge_{fid}", f"""MERGE INTO {t['F']} f USING (SELECT {fid} l, {tgt} w) d ON f.funder_id = d.l OR f.merged_into_id = d.l
WHEN MATCHED AND f.funder_id <> d.w AND NOT (f.status = 'merged' AND f.merged_into_id = d.w) THEN UPDATE SET f.status = 'merged', f.merged_into_id = d.w,
  f.ror_id = CASE WHEN f.funder_id = d.l THEN NULL ELSE f.ror_id END, f.ror_primary = CASE WHEN f.funder_id = d.l THEN NULL ELSE f.ror_primary END,
  f.updated_date = current_timestamp(), f.run_id = '{R}'"""),
                    (f"edit_merge_ids_{fid}", f"UPDATE {t['I']} SET funder_id = {tgt}, run_id = '{R}' WHERE funder_id = {fid} AND id_type NOT IN ('name', 'ror_cleared')")]
        elif act == "withdraw":   # the ROR id keeps pointing at the withdrawn funder (tombstone), so it is never minted again
            out += [(f"edit_withdraw_{fid}", f"UPDATE {t['F']} SET status = 'withdrawn', ror_id = NULL, ror_primary = NULL, updated_date = current_timestamp(), run_id = '{R}' WHERE funder_id = {fid} AND status <> 'withdrawn'")]
        elif act == "clear_ror":
            out += [(f"edit_clear_ror_{fid}", f"UPDATE {t['F']} SET ror_id = NULL, ror_primary = NULL, updated_date = current_timestamp(), run_id = '{R}' WHERE funder_id = {fid} AND ror_id IS NOT NULL"),
                    (f"edit_clear_ror_id_{fid}", tomb(fid, ""))]
        elif act == "set_ror":
            assert re.fullmatch(r"(https?://(www\.)?ror\.org/)?0[0-9a-z]{8}/?", tgt, re.I), f"edits.csv: bad ROR id {tgt!r} for F{fid}"
            r9 = tgt.rstrip("/")[-9:].lower()
            out += [(f"edit_set_ror_check_{fid}", f"SELECT assert_true({active(fid)}, 'edits.csv set_ror: F{fid} is not active') ok"),
                    (f"edit_set_ror_{fid}", f"UPDATE {t['F']} SET ror_id = 'https://ror.org/{r9}', ror_primary = true, updated_date = current_timestamp(), run_id = '{R}' WHERE funder_id = {fid} AND (ror_id IS DISTINCT FROM 'https://ror.org/{r9}' OR NOT ror_primary)"),
                    (f"edit_set_ror_demote_{fid}", f"UPDATE {t['F']} SET ror_primary = false, run_id = '{R}' WHERE ror_id = 'https://ror.org/{r9}' AND funder_id <> {fid} AND ror_primary"),
                    (f"edit_set_ror_old_{fid}", tomb(fid, f" AND id_value <> '{r9}'")),
                    (f"edit_set_ror_id_{fid}", f"""MERGE INTO {t['I']} i USING (SELECT '{r9}' v) d ON i.id_type = 'ror' AND i.id_value = d.v
WHEN MATCHED AND i.funder_id <> {fid} THEN UPDATE SET i.funder_id = {fid}, i.run_id = '{R}'
WHEN NOT MATCHED THEN INSERT (id_type, id_value, funder_id, origin_funder_id, is_primary, source, added_at, run_id)
  VALUES ('ror', d.v, {fid}, {fid}, true, 'edit', current_timestamp(), '{R}')""")]
    return out


# ---------------------------------------------------------------- CHECKS (fail closed before publish)

def checks(p, prev_version):
    t = tables(p)
    return [("checks", f"""SELECT
  assert_true((SELECT count(*) FROM (SELECT ror_id FROM {t['F']} WHERE status = 'active' AND ror_primary GROUP BY 1 HAVING count(*) > 1)) = 0,
    'ROR id primary on two active funders') c_ror_unique,
  assert_true((SELECT count(*) FROM {t['F']} m LEFT JOIN {t['F']} w ON w.funder_id = m.merged_into_id
    WHERE m.status = 'merged' AND (w.funder_id IS NULL OR w.status <> 'active')) = 0, 'merged funder not pointing at an active funder') c_merge_target,
  assert_true((SELECT count(*) FROM {t['I']} i LEFT ANTI JOIN {t['F']} f ON f.funder_id = i.funder_id) = 0, 'funder_id points at a missing funder') c_id_target,
  assert_true((SELECT count(*) FROM (SELECT id_type, id_value FROM {t['I']} GROUP BY 1, 2 HAVING count(*) > 1)) = 0, 'identifier mapped twice') c_id_unique,
  assert_true((SELECT count(*) FROM (SELECT funder_id FROM {t['F']} GROUP BY 1 HAVING count(*) > 1)) = 0, 'F id issued twice') c_f_unique,
  assert_true((SELECT count(*) FROM (SELECT funder_id FROM {t['F']} VERSION AS OF {int(prev_version)} EXCEPT SELECT funder_id FROM {t['F']})) = 0,
    'an issued F id disappeared') c_never_deleted,
  assert_true((SELECT count(*) FROM {t['F']} f LEFT ANTI JOIN {t['I']} i ON i.id_type = 'ror' AND i.id_value = {rid('f.ror_id')} AND i.funder_id = f.funder_id
    WHERE f.status = 'active' AND f.ror_primary) = 0, 'primary ror_id without its funder_id row') c_ror_indexed""")]


# ---------------------------------------------------------------- PUBLISH

def compat_view(p):
    """A table (not a view), rebuilt only after every check passes, so a failed night leaves yesterday's snapshot in place. Today's 22 openalex.funders.funders columns over the registry, for every reader (link notebooks, award notebooks, API build).
    Withdrawn funders are left out, as deleted rows are today. Merged rows stay so their FundRef DOI keeps resolving.
    Columns the registry does not own (location, replaces, replaced_by, tokens, json_entity_hash) and the exact v41 spellings of
    created_date, uri and alternate_titles come from the funder's seed record (source openalex_v41) until a
    registry change replaces them, so on cutover day the compat output equals openalex.funders.funders v41 row for row (cutover rehearsal, 10-08)."""
    t = tables(p)
    g = lambda k: f"get_json_object(v41.j, '$.{k}')"
    alts = "CASE WHEN r.source_key IS NOT NULL THEN array_sort(array_distinct(concat(coalesce(f.alt_names, array()), coalesce(r.alt_names, array())))) ELSE f.alt_names END" if ROR_OWNS_FIELDS else "f.alt_names"
    return ("publish_compat", f"""CREATE OR REPLACE TABLE {t['COMPAT']} AS
SELECT f.funder_id, {'CAST(' + g('crossref_id') + ' AS BIGINT)' if FUNDREF_PUBLIC else 'CAST(NULL AS BIGINT)'} crossref_id,
  {g('location')} location, f.display_name,
  CASE WHEN {alts} <=> from_json({g('alternate_titles')}, 'array<string>') THEN {g('alternate_titles')} ELSE to_json(coalesce({alts}, array())) END alternate_titles,
  {g('uri') if FUNDREF_PUBLIC else 'CAST(NULL AS STRING)'} uri,
  coalesce({g('replaces')}, CASE WHEN v41.j IS NULL THEN '[]' END) replaces,
  coalesce({g('replaced_by')}, CASE WHEN v41.j IS NULL THEN '[]' END) replaced_by,
  {g('tokens')} tokens, f.ror_id,
  CASE WHEN f.status = 'merged' THEN f.merged_into_id END merge_into_id,
  CASE WHEN f.status = 'merged' THEN coalesce(CASE WHEN {g('merge_into_id')} = CAST(f.merged_into_id AS STRING) THEN CAST({g('merge_into_date')} AS TIMESTAMP) END, f.updated_date) END merge_into_date,
  coalesce({g('created_date')}, CAST(f.created_date AS STRING)) created_date, f.wikidata_id,
  {g('doi') if FUNDREF_PUBLIC else 'CAST(NULL AS STRING)'} doi,
  f.country_code, f.homepage_url, f.description, f.image_url, f.image_thumbnail_url, {g('json_entity_hash')} json_entity_hash, f.updated_date
FROM {t['F']} f
LEFT JOIN (SELECT CAST(source_key AS BIGINT) funder_id, record_json j FROM {t['S']} WHERE source = 'openalex_v41') v41 ON v41.funder_id = f.funder_id
LEFT JOIN {t['S']} r ON r.source = 'ror' AND r.source_key = {rid('f.ror_id')} AND r.record_status <> 'gone'
WHERE f.status IN ('active', 'merged')""")


def funders_api_statements(p, create_funders_api_ipynb):
    """The live CreateFundersAPI SQL, unchanged except for table names (reads the compat view, writes {T}funders_api)."""
    t = tables(p)
    cells = json.load(open(create_funders_api_ipynb))["cells"]
    out = []
    for i, c in enumerate(x for x in cells if x["cell_type"] == "code"):
        sql = "".join(c["source"])
        sql = (sql.replace("openalex.funders.funders_api_hash", "@@APIH@@").replace("openalex.funders.funders_api", "@@API@@")
                  .replace("openalex.funders.funders", "@@COMPAT@@"))
        sql = sql.replace("@@APIH@@", t["APIH"]).replace("@@API@@", t["API"]).replace("@@COMPAT@@", t["COMPAT"])
        out.append((f"funders_api_{i}", sql))
    return out


# ---------------------------------------------------------------- NIGHTLY DIFF (printed by the job; one row per line item)

def diff_report(p, run_id):
    t = tables(p)
    return f"""SELECT 'source records changed' item, source detail, count(*) n FROM {t['S']} WHERE changed_run_id = '{run_id}' GROUP BY source
UNION ALL SELECT 'decision', action, count(*) FROM {t['D']} WHERE run_id = '{run_id}' GROUP BY action
UNION ALL SELECT 'decision new since last run', d.action, count(*) FROM {t['D']} d
  LEFT ANTI JOIN {t['D']} o ON o.run_id <> '{run_id}' AND o.source = d.source AND o.source_key = d.source_key AND o.action = d.action
  WHERE d.run_id = '{run_id}' GROUP BY d.action
UNION ALL SELECT 'funders created', created_by, count(*) FROM {t['F']} WHERE run_id = '{run_id}' AND created_by LIKE 'mint%'
  AND mint_key IN (SELECT CASE WHEN source = 'ror' THEN 'ror:' ELSE 'fundref:' END || source_key FROM {t['D']} WHERE run_id = '{run_id}' AND action LIKE 'MINT%')
  GROUP BY created_by
UNION ALL SELECT 'funders changed (now)', status, count(*) FROM {t['F']} WHERE run_id = '{run_id}' AND NOT (created_by LIKE 'mint%' AND mint_key IN (
  SELECT CASE WHEN source = 'ror' THEN 'ror:' ELSE 'fundref:' END || source_key FROM {t['D']} WHERE run_id = '{run_id}' AND action LIKE 'MINT%')) GROUP BY status
UNION ALL SELECT 'ids added or repointed', id_type, count(*) FROM {t['I']} WHERE run_id = '{run_id}' GROUP BY id_type
UNION ALL SELECT 'registry now', status, count(*) FROM {t['F']} GROUP BY status
ORDER BY 1, 2"""


# ---------------------------------------------------------------- RUNNERS

DEFAULTS = dict(ROR="openalex.institutions.ror", ROR_RAW="openalex.institutions.ror_raw", FUNDERS_V41="openalex.funders.funders VERSION AS OF 41",
                DELETED="openalex.funders.deleted_funders", MIN_ROR_ROWS=100000, MIN_FUNDER_RECORDS=15000,
                ALIASES="openalex.common.funder_names_keep", AWARDS="openalex.awards.openalex_awards", CROSSREF_REFS="openalex.works.locations_mapped",
                MAX_MINTS=200, DATACITE_REFS="openalex.awards.datacite_work_funder_evidence", API_SEED_FROM="openalex.funders.funders_api",
                WORKS_COUNTS="openalex.funders.funders_api")   # seed only: works per funder, to pick the keeper among same-name duplicates


def run(spark, statements, log=print):
    for name, sql in statements:
        log(name)
        spark.sql(sql).collect()   # collect() forces execution: spark.sql on a SELECT is lazy, so assert_true would never run


def daily(spark, p, bridge_path, edits_path=None, api_ipynb=None, max_mints=None, log=print):
    """Order: every source guard and edits.csv precondition before the first registry write; checks before the compat snapshot and
    funders_api. Delta has no multi-table transaction: a run that stops between apply steps resumes on the next run (mints are keyed
    by mint_key), and nothing is published until the checks pass."""
    t = tables(p)
    run(spark, [guard_ror(p)], log)
    prev_version = spark.sql(f"DESCRIBE HISTORY {t['F']} LIMIT 1").collect()[0]["version"]
    edits = edit_statements(p, read_edits(edits_path))
    if not FUNDREF_INTERNAL_KEY:
        run(spark, [("drop_fundref_keys", f"DELETE FROM {t['I']} WHERE id_type = 'fundref'")], log)
    run(spark, edits, log)   # each edit asserts its preconditions first; reviewed edits apply even if tonight's fuse then trips
    run(spark, [upsert_sources(p, bridge_path), materialize(p), review_mints(p), hold_batch_collisions(p), mint_fuse(p, max_mints or p["MAX_MINTS"]),
                never_issued(p)], log)
    run(spark, apply(p), log)
    run(spark, checks(p, prev_version) + [compat_view(p)], log)
    if api_ipynb:
        if not spark.catalog.tableExists(t["API"]):   # first run: start from today's served table so updated_date behaves as in prod
            run(spark, [("api_seed", f"CREATE TABLE {t['API']} AS SELECT * FROM {p['API_SEED_FROM']}")], log)
        run(spark, funders_api_statements(p, api_ipynb), log)
    return spark.sql(diff_report(p, p["RUN_ID"])).collect()


# COMMAND ----------

if "dbutils" in globals():  # Databricks entrypoint; local runs import the functions above
    dbutils.widgets.text("prefix", "openalex_dev.rohan_lab.fr1006_")
    dbutils.widgets.dropdown("mode", "daily", ["daily", "seed"])
    dbutils.widgets.text("run_id", "")    # the job passes {{job.run_id}}
    dbutils.widgets.text("max_mints", "200")
    run_id = dbutils.widgets.get("run_id").strip()
    assert run_id, "run_id parameter required ({{job.run_id}})"
    prefix = dbutils.widgets.get("prefix").strip()
    assert prefix == "openalex_dev.rohan_lab.fr1006_", "v0 writes only to openalex_dev (switch to prod is a separate, reviewed change)"
    p = dict(DEFAULTS, T=prefix, RUN_ID="fd" + run_id)
    def repo_file(rel_to_notebook, rel_to_root):   # the cwd of a git-source notebook task is its folder; fall back to the repo root
        for cand in (os.path.join(os.getcwd(), rel_to_notebook), os.path.join(os.getcwd(), rel_to_root)):
            if os.path.exists(cand):
                return cand
        raise FileNotFoundError(rel_to_root)
    if dbutils.widgets.get("mode") == "seed":
        version = spark.sql("DESCRIBE HISTORY openalex.funders.funders LIMIT 1").collect()[0]["version"]
        p["FUNDERS_V41"] = f"openalex.funders.funders VERSION AS OF {int(version)}"
        print(f"seed version: {version}")
        run(spark, seed(p))
    else:
        for row in daily(spark, p, bridge_path=repo_file("fundref_predecessor_bridge.csv", "notebooks/funders/fundref_predecessor_bridge.csv"),
                         edits_path=repo_file("edits.csv", "notebooks/funders/edits.csv"),
                         api_ipynb=repo_file("CreateFundersAPI.ipynb", "notebooks/funders/CreateFundersAPI.ipynb"),
                         max_mints=int(dbutils.widgets.get("max_mints"))):
            print(row.asDict())
