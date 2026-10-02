# Databricks notebook source
# ROR funder proposals (propose only). Each night it classifies every funder-typed ROR record against openalex.funders.funders
# and keeps ONE table, {P} = openalex.funders.ror_funder_proposals: one row per proposal or hold.
#   status: proposed (automatic actions, applied by hand with apply.sql) | held (needs a person; hold_reason says why)
#           | applied (apply.sql flipped it; ror_id_before + applied_batch are the audit) | rejected (by a person, a rollback,
#           or 'obsolete': tonight's run no longer produces it).
# A row is keyed by (ROR record fingerprint, action, target, mint id); an unchanged record never produces a second row, and a row
# a person rejected stays rejected until the ROR record changes. Never writes openalex.funders.funders.
# Decisions, audit, "seen" and metrics are queries over this table (see README in grants-work/ror-funder-pipeline-build/ship2).

NORM = r"regexp_replace(trim(regexp_replace(lower({x}), '[^\\p{{L}}\\p{{N}}]+', ' ')), '^the ', '')"   # 'The X' = 'X' (Ferrier miss, 09-28)
RID = r"lower(regexp_extract({x}, '([0-9a-z]{{9}})/*$', 1))"   # normalized ROR id from any URL / bare form
AUTO_ACTIONS = ("ATTACH_FUNDREF", "MINT_FUNDREF", "REPAIR_DEAD_ROR")
PROPOSAL_ACTIONS = AUTO_ACTIONS + ("ATTACH_NAME_HOST", "MINT_ROR")
FR_RE = r"'^(?:(?:https?://)?(?:dx\\.)?(?:doi\\.org/)?10\\.13039/)?([0-9]{{5,}})$'"


def ddl(p):
    return [("create", f"""CREATE TABLE {p['P']} (
  proposal_id STRING NOT NULL, ror_id STRING, rid STRING, ror_fp STRING, fundrefs ARRAY<STRING>, action STRING NOT NULL,
  status STRING NOT NULL, hold_reason STRING, target_funder_id BIGINT, mint_funder_id BIGINT, display_name STRING, country STRING,
  evidence STRING, run_id STRING NOT NULL, last_seen_run_id STRING NOT NULL, created_at TIMESTAMP NOT NULL, last_seen_at TIMESTAMP NOT NULL,
  status_changed_at TIMESTAMP NOT NULL, applied_batch STRING, ror_id_before STRING) USING DELTA""")]


def _ror_cur(p):
    n = NORM.format(x="x")
    fr = FR_RE.format()
    return f"""ror_cur AS (
  SELECT {RID.format(x='id')} rid, id ror_url, status, types, coalesce(array_contains(types,'funder'), false) is_funder, updated_date,
    nullif(trim(get(filter(names, n -> array_contains(n.types,'ror_display')),0).value), '') display_name,
    transform(filter(coalesce(names, array()), n -> NOT array_contains(n.types,'acronym')), n -> n.value) full_names,
    flatten(transform(filter(coalesce(external_ids, array()), e -> lower(e.type)='fundref'), e -> coalesce(e.all, array()))) fr_raw,
    nullif(upper(trim(get(locations,0).geonames_details.country_code)), '') country,
    lower(regexp_extract(get(filter(links, l -> l.type='website'),0).value, '^(?:https?://)?(?:www\\\\.)?([^/:]+)', 1)) host,
    transform(filter(coalesce(relationships, array()), r -> r.type='successor'), r -> {RID.format(x='r.id')}) successors,
    sha2(to_json(named_struct('s',status,'t',types,'n',names,'e',external_ids,'r',relationships,'l',locations,'k',links)),256) fp
  FROM {p['ROR']}
),
ror_x AS (  -- FundRef validation: every value must be digits, optionally behind a 10.13039/ DOI prefix
  SELECT *, array_sort(array_distinct(filter(transform(fr_raw, x -> regexp_extract(trim(x), {fr}, 1)), x -> x IS NOT NULL AND x <> ''))) fundrefs,
    exists(fr_raw, x -> x IS NULL OR regexp_extract(trim(x), {fr}, 1) = '') fr_bad,
    array_distinct(filter(transform(full_names, x -> {n}), x -> length(x) >= 4)) nkeys
  FROM ror_cur
),
ror_hist AS (SELECT DISTINCT {RID.format(x='id')} rid FROM {p['ROR_RAW']})"""


def _funders(p):
    n = NORM.format(x="x")
    return f"""f AS (
  SELECT funder_id, coalesce(merge_into_id, funder_id) canon_id, merge_into_id, display_name,
    nullif({RID.format(x='ror_id')}, '') f_rid, CAST(crossref_id AS STRING) crossref_id, upper(country_code) country_code,
    lower(regexp_extract(homepage_url, '^(?:https?://)?(?:www\\\\.)?([^/:]+)', 1)) host,
    array_distinct(filter(transform(concat(array(display_name), coalesce(from_json(alternate_titles,'array<string>'), array())),
       x -> {n}), x -> length(x) >= 4)) name_keys
  FROM {p['FUNDERS']}
),
dead AS (  -- dead = in no ROR dump ever, not just missing today (a partial refresh must not make an id look dead)
  SELECT DISTINCT f.f_rid FROM f LEFT ANTI JOIN ror_hist h ON h.rid = f.f_rid LEFT ANTI JOIN ror_cur c ON c.rid = f.f_rid WHERE f.f_rid IS NOT NULL
),
fx AS (SELECT f.*, dd.f_rid IS NOT NULL rid_dead FROM f LEFT JOIN dead dd ON dd.f_rid = f.f_rid),
canon AS (  -- per canonical funder, ownership over the canonical row AND its redirect rows
  SELECT canon_id, collect_set(CASE WHEN f_rid IS NOT NULL AND NOT rid_dead THEN f_rid END) live_rors,
         max(CASE WHEN f_rid IS NOT NULL AND rid_dead THEN 1 ELSE 0 END) = 1 has_dead,
         flatten(collect_list(name_keys)) nkeys, collect_set(country_code) ccs
  FROM fx GROUP BY canon_id
)"""


def guard(p):
    """Fail closed before anything is written: a short or undated ROR table, or ROR history missing current ids, would make
    tonight's run mark good proposals obsolete."""
    return ("guard", f"""SELECT assert_true(
  (SELECT count(*) FROM {p['ROR']}) >= {int(p['MIN_ROR_ROWS'])}
  AND (SELECT count_if(coalesce(array_contains(types, 'funder'), false)) FROM {p['ROR']}) >= {int(p['MIN_FUNDER_RECORDS'])}
  AND (SELECT max(updated_date) FROM {p['ROR']}) IS NOT NULL
  AND (SELECT count(*) FROM (SELECT DISTINCT {RID.format(x='id')} rid FROM {p['ROR']}) c
       LEFT ANTI JOIN (SELECT DISTINCT {RID.format(x='id')} rid FROM {p['ROR_RAW']}) h ON h.rid = c.rid) = 0,
  'ROR table too small, undated, or history incomplete; nothing written (fail closed)') ok""")


def classify(p):
    """Tonight's full set of proposals and holds (LINKED_NOOP and SKIP_* are not stored)."""
    R = p["RUN_ID"]
    return f"""WITH {_ror_cur(p)},
{_funders(p)},
dead_fr AS (SELECT DISTINCT fx.crossref_id fr FROM fx JOIN canon c ON c.canon_id = fx.canon_id WHERE c.has_dead AND fx.crossref_id IS NOT NULL),
dead_cand AS (SELECT DISTINCT x.rid FROM (SELECT rid, explode(fundrefs) fr FROM ror_x WHERE is_funder AND status = 'active') x JOIN dead_fr d ON d.fr = x.fr),
linked AS (SELECT DISTINCT f_rid rid FROM fx WHERE f_rid IS NOT NULL),
was_funder AS (SELECT DISTINCT {RID.format(x='id')} rid FROM {p['FUNDER_HIST']}),   -- ever funder-typed in any ROR dump
ev AS (   -- every funder-typed record, every record that could repair a dead ROR id, and linked records that lost the funder type
  SELECT c.* FROM ror_x c LEFT JOIN dead_cand dc ON dc.rid = c.rid LEFT JOIN linked lk ON lk.rid = c.rid LEFT JOIN was_funder wf ON wf.rid = c.rid
  WHERE c.is_funder OR dc.rid IS NOT NULL OR (lk.rid IS NOT NULL AND wf.rid IS NOT NULL)
),
fr_rors AS (SELECT fr, count(DISTINCT rid) n_rors FROM (SELECT rid, explode(fundrefs) fr FROM ror_x) GROUP BY fr),  -- ALL records, any type/status
ror_own AS (SELECT e.rid, collect_set(fx.canon_id) ids FROM ev e JOIN fx ON fx.f_rid = e.rid GROUP BY 1),
fr_own AS (
  SELECT o.rid, collect_set(c.canon_id) ids, array_distinct(flatten(collect_list(c.live_rors))) owner_rors,
         collect_set(CASE WHEN c.has_dead THEN c.canon_id END) dead_owner,
         array_distinct(flatten(collect_list(c.ccs))) owner_cc, array_distinct(flatten(collect_list(c.nkeys))) owner_nkeys
  FROM (SELECT DISTINCT e.rid, fx.canon_id FROM (SELECT rid, explode(fundrefs) fr FROM ev) e JOIN fx ON fx.crossref_id = e.fr) o
  JOIN canon c ON c.canon_id = o.canon_id GROUP BY 1
),
fr_rev AS (SELECT e.rid, max(r.n_rors) max_rors FROM (SELECT rid, explode(fundrefs) fr FROM ev) e JOIN fr_rors r ON r.fr = e.fr GROUP BY 1),
name_own AS (
  SELECT n.rid, collect_set(fx.funder_id) ids, collect_set(CASE WHEN fx.f_rid IS NOT NULL AND NOT fx.rid_dead THEN fx.funder_id END) ids_with_ror,
         collect_set(CASE WHEN fx.host IS NOT NULL AND fx.host = n.host THEN fx.funder_id END) ids_host
  FROM (SELECT rid, host, country, explode(nkeys) k FROM ev) n
  JOIN (SELECT funder_id, f_rid, rid_dead, host, country_code, explode(name_keys) k FROM fx WHERE merge_into_id IS NULL) fx
    ON fx.k = n.k AND fx.country_code = n.country
  GROUP BY 1
),
succ_own AS (SELECT e.rid, collect_set(fx.canon_id) ids FROM (SELECT rid, explode(successors) s FROM ev) e JOIN fx ON fx.f_rid = e.s GROUP BY 1),
del AS (SELECT funder_id del_id, CAST(crossref_id AS STRING) del_fr FROM {p['DELETED']}),   -- funders we deleted on purpose: never propose re-creating them
j AS (
  SELECT e.*, coalesce(ro.ids, array()) ror_owner, coalesce(fo.ids, array()) fr_owner, coalesce(fo.owner_rors, array()) fr_owner_rors,
    coalesce(fo.dead_owner, array()) fr_dead_owner,
    coalesce(array_contains(fo.owner_cc, e.country), false) country_agree,
    coalesce(arrays_overlap(e.nkeys, fo.owner_nkeys), false) name_agree,
    coalesce(rv.max_rors, 0) fr_max_rors,
    coalesce(no.ids, array()) name_owner, coalesce(no.ids_with_ror, array()) name_owner_with_ror, coalesce(no.ids_host, array()) name_owner_host,
    coalesce(so.ids, array()) successor_owner,
    CASE WHEN size(e.fundrefs) > 0 THEN abs(xxhash64(concat('10.13039/', get(e.fundrefs,0)))) % 9000000000 END mint_id_doi,
    abs(xxhash64(e.ror_url)) % 9000000000 mint_id_ror
  FROM ev e LEFT JOIN ror_own ro ON ro.rid = e.rid LEFT JOIN fr_own fo ON fo.rid = e.rid
  LEFT JOIN fr_rev rv ON rv.rid = e.rid LEFT JOIN name_own no ON no.rid = e.rid LEFT JOIN succ_own so ON so.rid = e.rid
),
del_hit AS (SELECT DISTINCT j.rid FROM j JOIN del ON array_contains(j.fundrefs, del.del_fr) OR del.del_id IN (j.mint_id_doi, j.mint_id_ror)),
d AS (
  SELECT j.*,
  CASE
    WHEN NOT is_funder THEN 'REVIEW_TYPE_DROPPED'
    WHEN status IS NULL OR status NOT IN ('active','inactive','withdrawn') THEN 'HOLD_UNKNOWN_STATUS'
    WHEN status = 'withdrawn' THEN CASE WHEN size(ror_owner) > 0 THEN 'FLAG_WITHDRAWN_LINKED' ELSE 'SKIP_WITHDRAWN' END
    WHEN status = 'inactive' THEN CASE WHEN size(ror_owner) > 0 AND size(successor_owner) > 0 AND NOT arrays_overlap(ror_owner, successor_owner) THEN 'HOLD_SUCCESSOR_MERGE'
                                       WHEN size(ror_owner) > 0 THEN 'FLAG_INACTIVE_LINKED' ELSE 'SKIP_INACTIVE' END
    WHEN fr_bad THEN 'HOLD_MALFORMED_FUNDREF'
    WHEN size(ror_owner) > 1 THEN 'HOLD_ROR_MULTI_OWNER'
    WHEN size(ror_owner) = 1 THEN CASE WHEN size(array_except(fr_owner, ror_owner)) > 0 THEN 'HOLD_DUPLICATE_FUNDER' ELSE 'LINKED_NOOP' END
    WHEN size(fr_owner) > 1 THEN 'HOLD_MULTI_FUNDER'
    WHEN size(fr_owner) = 1 THEN CASE WHEN size(fr_owner_rors) > 0 THEN 'HOLD_REPLACE'
                                      WHEN fr_max_rors > 1 THEN 'HOLD_FUNDREF_MULTI_ROR'
                                      WHEN NOT (country_agree OR name_agree) THEN 'HOLD_COUNTRY_MISMATCH'
                                      -- replay + blind grading 09-28: 9 of 10 wrong links were programmes/sub-units whose FundRef ROR
                                      -- lists on the parent record, all with disagreeing names; name-agreeing links were 45/46 right
                                      WHEN NOT name_agree THEN 'HOLD_NAME_DISAGREES'
                                      WHEN size(fr_dead_owner) > 0 THEN 'REPAIR_DEAD_ROR'
                                      ELSE 'ATTACH_FUNDREF' END
    WHEN size(name_owner) > 1 THEN 'HOLD_NAME_AMBIGUOUS'
    WHEN size(name_owner) = 1 AND size(name_owner_with_ror) = 1 THEN 'HOLD_NAME_OWNER_HAS_ROR'
    WHEN size(name_owner) = 1 AND size(name_owner_host) = 1 THEN 'ATTACH_NAME_HOST'
    WHEN size(name_owner) = 1 THEN 'HOLD_NAME_ONLY'
    WHEN display_name IS NULL OR country IS NULL THEN 'HOLD_MINT_INCOMPLETE'
    WHEN dh.rid IS NOT NULL THEN 'HOLD_DELETED_FUNDER'
    WHEN size(fundrefs) > 0 AND fr_max_rors > 1 THEN 'HOLD_FUNDREF_MULTI_ROR'
    WHEN size(fundrefs) > 0 AND cd.cd_id IS NOT NULL THEN 'HOLD_ID_COLLISION'
    WHEN size(fundrefs) > 0 THEN 'MINT_FUNDREF'
    WHEN cr.cr_id IS NOT NULL THEN 'HOLD_ID_COLLISION'
    ELSE 'MINT_ROR'
  END action
  FROM j LEFT JOIN del_hit dh ON dh.rid = j.rid LEFT JOIN (SELECT funder_id cd_id FROM f) cd ON cd.cd_id = j.mint_id_doi LEFT JOIN (SELECT funder_id cr_id FROM f) cr ON cr.cr_id = j.mint_id_ror
),
out AS (
  SELECT d.*,
    CASE WHEN action IN ('ATTACH_FUNDREF','REPAIR_DEAD_ROR','HOLD_REPLACE','HOLD_COUNTRY_MISMATCH','HOLD_NAME_DISAGREES','HOLD_FUNDREF_MULTI_ROR') THEN get(fr_owner,0)
         WHEN action IN ('ATTACH_NAME_HOST','HOLD_NAME_ONLY','HOLD_NAME_OWNER_HAS_ROR') THEN get(name_owner,0)
         WHEN size(ror_owner) = 1 THEN get(ror_owner,0) END target,
    CASE WHEN action = 'MINT_FUNDREF' THEN mint_id_doi WHEN action = 'MINT_ROR' THEN mint_id_ror END mint
  FROM d
),
dead_hold AS (  -- funders holding a dead ROR id for which tonight proposes no repair: one funder-level review item
  SELECT c.canon_id FROM canon c
  LEFT ANTI JOIN (SELECT DISTINCT target FROM out WHERE action = 'REPAIR_DEAD_ROR') r ON r.target = c.canon_id
  WHERE c.has_dead
),
rows AS (
  SELECT rid, ror_url, fp, action, target, mint, display_name, country, fundrefs,
    to_json(named_struct('status',status,'is_funder',is_funder,'ror_owner',ror_owner,'fr_owner',fr_owner,'fr_owner_rors',fr_owner_rors,'fr_dead_owner',fr_dead_owner,
      'country_agree',country_agree,'name_agree',name_agree,'fr_max_rors',fr_max_rors,'fr_bad',fr_bad,'name_owner',name_owner,
      'name_owner_with_ror',name_owner_with_ror,'name_owner_host',name_owner_host,'successor_owner',successor_owner,'host',host,
      'updated_date',updated_date)) evidence
  FROM out WHERE action NOT IN ('LINKED_NOOP','SKIP_WITHDRAWN','SKIP_INACTIVE')
  UNION ALL
  SELECT NULL, NULL, NULL, 'HOLD_DEAD_ROR', canon_id, NULL, NULL, NULL, NULL, to_json(named_struct('reason', 'dead ROR id; no ROR record lists this funder FundRef'))
  FROM dead_hold
)
SELECT sha2(concat_ws('|', coalesce(rid, 'funder'), coalesce(fp, ''), action, coalesce(CAST(target AS STRING), ''), coalesce(CAST(mint AS STRING), '')), 256) proposal_id,
  ror_url ror_id, rid, fp ror_fp, fundrefs, action,
  CASE WHEN action IN {AUTO_ACTIONS} THEN 'proposed' ELSE 'held' END status,
  CASE WHEN action IN {AUTO_ACTIONS} THEN NULL WHEN action IN ('ATTACH_NAME_HOST','MINT_ROR') THEN 'NEEDS_REVIEW_' || action ELSE action END hold_reason,
  target target_funder_id, mint mint_funder_id, display_name, country, evidence
FROM rows"""


def merge(p):
    """One atomic write: insert new rows, refresh last-seen on rows tonight reproduces, mark open rows tonight no longer
    produces as rejected/obsolete. applied and rejected rows keep their status."""
    P, R = p["P"], p["RUN_ID"]
    return ("merge", f"""MERGE INTO {P} t
USING ({classify(p)}) s
ON t.proposal_id = s.proposal_id
WHEN MATCHED THEN UPDATE SET t.last_seen_run_id = '{R}', t.last_seen_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (proposal_id, ror_id, rid, ror_fp, fundrefs, action, status, hold_reason, target_funder_id, mint_funder_id,
  display_name, country, evidence, run_id, last_seen_run_id, created_at, last_seen_at, status_changed_at)
  VALUES (s.proposal_id, s.ror_id, s.rid, s.ror_fp, s.fundrefs, s.action, s.status, s.hold_reason, s.target_funder_id, s.mint_funder_id,
  s.display_name, s.country, s.evidence, '{R}', '{R}', current_timestamp(), current_timestamp(), current_timestamp())
WHEN NOT MATCHED BY SOURCE AND t.status IN ('proposed', 'held') THEN UPDATE SET t.status = 'rejected',
  t.hold_reason = 'OBSOLETE: not produced by run {R}', t.status_changed_at = current_timestamp()""")


def run_statements(p):
    return [guard(p), merge(p)]


DEFAULTS = dict(ROR="openalex.institutions.ror", ROR_RAW="openalex.institutions.ror_raw", FUNDERS="openalex.funders.funders",
                FUNDER_HIST="(SELECT id FROM openalex.institutions.ror_raw WHERE array_contains(types, 'funder'))",
                DELETED="openalex.funders.deleted_funders", MIN_ROR_ROWS=100000, MIN_FUNDER_RECORDS=15000)

# COMMAND ----------

if "dbutils" in globals():  # Databricks entrypoint; the local test runner imports the functions above instead
    dbutils.widgets.text("table", "openalex.funders.ror_funder_proposals")
    dbutils.widgets.text("run_id", "")   # the job passes {{job.run_id}}
    run_id = dbutils.widgets.get("run_id").strip()
    assert run_id, "run_id parameter required ({{job.run_id}})"
    p = dict(DEFAULTS, P=dbutils.widgets.get("table"), RUN_ID="rfp" + run_id)
    for name, sql in run_statements(p):
        print(name)
        spark.sql(sql).collect()   # collect() forces execution: spark.sql on a SELECT is lazy, so assert_true would never run
    print(spark.sql(f"SELECT status, hold_reason, count(*) n FROM {p['P']} WHERE last_seen_run_id = '{p['RUN_ID']}' GROUP BY 1, 2 ORDER BY 1, 3 DESC").collect())
