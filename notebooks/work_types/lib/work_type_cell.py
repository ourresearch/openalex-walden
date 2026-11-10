"""The work-type cell of CreateWorksEnriched (oxjob #1976, worktype-student-v2, 2026-11-09): the topics-shape re-cut.

CreateWorksEnriched runs on a SQL warehouse, so the cell is SQL: one compound statement, placed right after cell 3
(the build aside), generated here from the shipped policy files (lib/policy_*.json), so every shipped policy is
selectable by a parameter and nothing is typed in by hand. It reads:
  openalex.works.work_type_scores    the latest-seen score row per work for the policy's model (with the pack's stage-0
                                     signals), written by our job Classify_Work_Types after each End 2 End
  openalex.works.openalex_works_build  tonight's cascade type (CreateWorksBase), the primary location
  openalex.works.openalex_works      last night's published row: its type_decision (to explain each flip) and its type
  openalex.common.work_types         the live vocabulary
and sets on the build:
  type_decision   a struct: the decision (type, outcome, ...), its basis, the flip reason against last night, served/held
  type           the decision's type, only where the switch serves (the Works_Enriched task parameters wt_*), and
                 last night's type where the decision flipped for no reason (held)
If anything in it fails, its handler copies last night's type_decision and served types instead, and End 2 End
publishes as usual; Classify_Work_Types' Check_Flips task then fails loud (decided_on is not today).

decide_query() is the one SELECT the cell MERGEs from; the tests run the same text on DuckDB (dialect="duckdb").
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import sql_templates as T

HERE = Path(__file__).resolve().parent
V2_FIELDS = (("type", "STRING"), ("outcome", "STRING"), ("decided_by", "STRING"), ("confidence", "DECIMAL(5,4)"),
             ("input_hash", "STRING"), ("s2_type", "STRING"), ("s2_confidence", "DECIMAL(5,4)"), ("s2_input_hash", "STRING"),
             ("today_type", "STRING"), ("policy_id", "STRING"), ("model_version", "STRING"), ("vocab_version", "STRING"),
             ("is_traditional", "BOOLEAN"), ("flip", "STRING"), ("served", "BOOLEAN"), ("held", "BOOLEAN"),
             ("held_type", "STRING"), ("decided_on", "DATE"))
SCORED_MODELS = ("s2-0bc40dd7f812bdfe",)   # what the Modal app openalex-worktypes-nightly scores (v6c); a policy on another
#                                          model would decide nothing new (Codex 11-09 #6)
V2_TYPE = "STRUCT<" + ", ".join(f"{n}: {t}" for n, t in V2_FIELDS) + ">"
PROTECTED_RULES = ("#1647: physicalobject sample code ahead of title code -> other",)   # rule 4b (worktype-hub 11-08)
PARAMS = ("wt_policy_id", "wt_served", "wt_n_buckets", "wt_start_buckets", "wt_step", "wt_switch_date")
DEFAULTS = {"wt_policy_id": "pol5-4ed2aed64c8e", "wt_served": "rules", "wt_n_buckets": "4", "wt_start_buckets": "0",
            "wt_step": "1", "wt_switch_date": "2026-11-01"}       # the shadow: go-line 1's switch
SWITCH_FILE = HERE / "work_type_switch.json"
SPARK_TABLES = {"scores": "openalex.works.work_type_scores", "build": "openalex.works.openalex_works_build",
                "works": "openalex.works.openalex_works", "types": "openalex.common.work_types",
                "deleted": "openalex.works.deleted_works", "locs": "openalex.works.locations_w_types"}
FLIPS = ("new", "policy", "model", "vocabulary", "input", "stage0-list", "today_type", "unexplained")


def shipped_policies() -> list[str]:
    return sorted(f.stem[len("policy_"):] for f in HERE.glob("policy_*.json"))


class _D:
    """The few places Spark and DuckDB spell the same thing differently."""

    def __init__(self, dialect: str):
        if dialect not in ("spark", "duckdb"):
            raise T.TemplateError(f"dialect {dialect!r}")
        self.spark = dialect == "spark"
        self.dialect = dialect

    def lit(self, x: str) -> str:
        return T._lit(x, self.dialect)

    def arr(self, xs) -> str:
        return T._arr(list(xs), self.dialect)

    def sha32(self, x: str) -> str:
        return f"substr(sha2({x}, 256), 1, 32)" if self.spark else f"substr(sha256({x}), 1, 32)"

    def elem(self, a: str, i: str) -> str:
        return f"element_at({a}, {i})" if self.spark else f"list_extract({a}, {i})"

    def rlike(self, a: str, rx: str) -> str:
        return f"{a} RLIKE {rx}" if self.spark else f"regexp_matches({a}, {rx})"

    def size(self, a: str) -> str:
        return f"size({a})" if self.spark else f"len({a})"

    def contains(self, a: str, x: str) -> str:
        return f"array_contains({a}, {x})" if self.spark else f"list_contains({a}, {x})"

    def explode(self, a: str) -> str:
        return f"explode({a})" if self.spark else f"unnest({a})"

    def fail(self, msg: str) -> str:
        return f"raise_error({msg})" if self.spark else f"error({msg})"

    def bucket(self, x: str, n: str) -> str:
        return f"pmod(xxhash64({x}), {n})" if self.spark else f"(hash({x}) % {n})"

    def days(self, today: str, d: str) -> str:
        return f"datediff({today}, {d})" if self.spark else f"date_diff('day', {d}, {today})"

    def struct(self, pairs) -> str:
        if self.spark:
            return f"CAST(named_struct({', '.join(f'{self.lit(k)}, {v}' for k, v in pairs)}) AS {V2_TYPE})"
        return "{" + ", ".join(f"'{k}': {v}" for k, v in pairs) + "}"


def relations(policy_ids, dialect: str = "spark", docs: dict | None = None) -> tuple[str, str, str]:
    """The shipped policies as three one-line relations, each row tagged with its policy id:
    pol (policy_id, model_id, tau, classes, policy_kind), cuts (policy_id, type, cut), lists (policy_id, ord, list_id,
    type, doi_prefixes, doi_regex, names_exact, names_regex, url_substrings). Each policy file is loaded through
    sql_templates.load_policy, which recomputes its ids and refuses a tampered file (`docs`: policy files' content, tests)."""
    d = _D(dialect)
    pol, cuts, lists = [], [], []
    for pid in policy_ids:
        p = T.load_policy(pid, (docs or {}).get(pid))
        for c in p["classes"]:
            if not re.fullmatch(r"[a-z][a-z-]*", c):
                raise T.TemplateError(f"class {c!r}")
        pol.append(f"SELECT {d.lit(p['policy_id'])} AS policy_id, {d.lit(p['model_id'])} AS model_id, "
                   f"CAST({p['tau']} AS DECIMAL(5,4)) AS tau, {d.arr(p['classes'])} AS classes, {d.lit(p['policy_kind'])} AS policy_kind")
        for t, c in sorted(p["type_cuts"].items()):
            cuts.append(f"SELECT {d.lit(p['policy_id'])} AS policy_id, {d.lit(t)} AS type, "
                        f"CAST({'NULL' if c is None else f'{float(c):.4f}'} AS DECIMAL(5,4)) AS cut")
        lists.append(f"SELECT {d.lit(p['policy_id'])} AS policy_id, x.* FROM ({T.stage0_sql(p['stage0_lists'], dialect)}) x")
    if not cuts:
        cuts = ["SELECT CAST(NULL AS STRING) AS policy_id, CAST(NULL AS STRING) AS type, CAST(NULL AS DECIMAL(5,4)) AS cut WHERE 1 = 0"]
    return " UNION ALL ".join(pol), " UNION ALL ".join(cuts), " UNION ALL ".join(lists)


def _prm_sql(d: "_D", values: dict | None) -> str:
    """The switch as one row (policy_id, served, n_buckets, start_buckets, step, switch_date): literals, or on Spark the
    :wt_* parameter markers (unused by the cell, which carries lib/work_type_switch.json's values)."""
    if values is None:
        if not d.spark:
            raise T.TemplateError("DuckDB needs literal values")
        prm = {k: f":{k}" for k in PARAMS}
    else:
        if set(values) != set(PARAMS) or not all(T.SAFE.match(str(v)) for v in values.values()):
            raise T.TemplateError(f"values must be exactly {PARAMS}, identifier-safe")
        prm = {k: d.lit(str(v)) for k, v in values.items()}
    return (f"SELECT CAST({prm['wt_policy_id']} AS STRING) AS policy_id, CAST({prm['wt_served']} AS STRING) AS served, "
            f"CAST({prm['wt_n_buckets']} AS INT) AS n_buckets, CAST({prm['wt_start_buckets']} AS INT) AS start_buckets, "
            f"CAST({prm['wt_step']} AS INT) AS step, CAST({prm['wt_switch_date']} AS DATE) AS switch_date")


def switch_check_query(policy_ids=None, dialect: str = "spark", values: dict | None = None, docs: dict | None = None) -> str:
    """The cell's first statement: one row, always evaluated, that fails on an unknown policy or a bad switch value. The
    guard inside the decision is evaluated per row, and an unknown policy leaves no row to evaluate it on: Spark then
    decided nothing and raised nothing (the hub's dry run, 11-09), so it never reached the handler."""
    d = _D(dialect)
    pol_rows = relations(list(policy_ids or shipped_policies()), dialect, docs)[0]
    msg = d.fail("concat('work types: unknown policy or bad switch parameters: ', prm.policy_id, ' / ', prm.served)")
    return (f"WITH prm AS ({_prm_sql(d, values)}),\npol AS (SELECT p.* FROM ({pol_rows}) p JOIN prm ON p.policy_id = prm.policy_id)\n"
            "SELECT CASE WHEN (SELECT count(*) FROM pol) = 1 AND prm.served IN ('rules', prm.policy_id) AND prm.n_buckets >= 1\n"
            "                 AND prm.start_buckets BETWEEN 0 AND prm.n_buckets AND prm.step IN (-1, 1)\n"
            f"            THEN 'work types: switch ok' ELSE {msg} END AS work_types\nFROM prm")


def decide_query(policy_ids=None, dialect: str = "spark", values: dict | None = None, today: str | None = None,
                 tables: dict | None = None, docs: dict | None = None, prev: bool = True) -> str:
    """The SELECT the cell MERGEs from: one row per decided work (work_id, serve_type, v2). On Spark the switch comes
    from the notebook's parameters (:wt_*); the tests pass `values` (literals) and a fixed `today`. prev=False: the first
    night, before openalex_works has type_decision (every decision is 'new')."""
    d = _D(dialect)
    policy_ids = list(policy_ids or shipped_policies())
    tb = dict(SPARK_TABLES, **(tables or {}))
    prm_sql = _prm_sql(d, values)
    today = today or ("current_date()" if d.spark else "current_date")
    pol_rows, cut_rows, list_rows = relations(policy_ids, dialect, docs)
    rx1, rx2 = d.lit(r"^\s+"), d.lit(r"\s+$")
    prot = ", ".join(d.lit(r) for r in PROTECTED_RULES)
    vocab = ("SELECT concat('vocab-', substr(sha2(concat('[', concat_ws(',', array_sort(collect_list(concat('[\"', type, "
             "'\",', CASE WHEN is_traditional THEN 'true' ELSE 'false' END, ']')))), ']'), 256), 1, 12)) AS vocab_version FROM ty"
             if d.spark else "SELECT vocab_version FROM wt_vocab")
    s0_struct = [("type", "d_type"), ("outcome", "d_outcome"), ("decided_by", "d_decided_by"), ("confidence", "d_confidence"),
                 ("input_hash", "input_hash"), ("s2_type", "s2_type"), ("s2_confidence", "s2_confidence"),
                 ("s2_input_hash", "s2_input_hash"), ("today_type", "today_type"), ("policy_id", "policy_id"),
                 ("model_version", "model_id"), ("vocab_version", "vocab_version"), ("is_traditional", "d_trad"),
                 ("flip", "flip"), ("served", "serve"), ("held", "held"), ("held_type", "held_type"), ("decided_on", today)]
    assert [k for k, _ in s0_struct] == [n for n, _ in V2_FIELDS]
    v2_cast = V2_TYPE if d.spark else "STRUCT(" + ", ".join(f"{n} {t.replace('STRING', 'VARCHAR')}" for n, t in V2_FIELDS) + ")"
    prev_sql = (f"SELECT id AS work_id, type AS pub_type, type_decision AS pv FROM {tb['works']}" if prev else
                f"SELECT CAST(NULL AS BIGINT) AS work_id, CAST(NULL AS STRING) AS pub_type, CAST(NULL AS {v2_cast}) AS pv WHERE 1 = 0")
    return f"""
WITH prm AS ({prm_sql}),
pol AS (SELECT p.* FROM ({pol_rows}) p JOIN prm ON p.policy_id = prm.policy_id),
-- the switch (rule 6): buckets served tonight = clamp(start + step * days since switch_date, 0, N); a bad parameter or an
-- unknown policy fails the statement, and the cell's handler keeps last night's decisions and types
sw AS (
  SELECT prm.served, prm.n_buckets,
         CASE WHEN (SELECT count(*) FROM pol) = 1 AND prm.served IN ('rules', prm.policy_id) AND prm.n_buckets >= 1
                   AND prm.start_buckets BETWEEN 0 AND prm.n_buckets AND prm.step IN (-1, 1)
              THEN greatest(0, least(prm.n_buckets, prm.start_buckets + prm.step * {d.days(today, 'prm.switch_date')}))
              ELSE {d.fail("concat('work types: unknown policy or bad switch parameters: ', prm.policy_id, ' / ', prm.served)")} END AS k
  FROM prm
),
cuts AS (SELECT c.type, c.cut FROM ({cut_rows}) c JOIN prm ON c.policy_id = prm.policy_id),
lists AS (SELECT l.* FROM ({list_rows}) l JOIN prm ON l.policy_id = prm.policy_id),
ty AS (SELECT id AS type, is_traditional FROM {tb['types']}),
vv AS ({vocab}),
-- each work's current score: the latest-seen row of the policy's model (the queue builder touches a row when the
-- work's inputs come back to an already-scored text), with the pack's stage-0 signals
sc AS (
  SELECT s.work_id, s.input_hash AS s2_input_hash, s.class_idx, s.confidence AS conf_stored,
         coalesce(s.s0_doi, '') AS s0_doi, s.s0_names, s.s0_urls
  FROM {tb['scores']} s JOIN pol ON s.model_version = pol.model_id
  QUALIFY row_number() OVER (PARTITION BY s.work_id ORDER BY s.seen_at DESC, s.scored_at DESC, s.input_hash) = 1
),
-- tonight's cascade type (CreateWorksBase), normalised as vocab.norm_type; non-xpac works only (include_xpac is off)
bw AS (
  SELECT id AS work_id, type AS build_type, primary_location.provenance AS pl_prov, primary_location.native_id AS pl_nid,
         lower(regexp_replace(regexp_replace(type, {rx1}, ''), {rx2}, '')) AS x
  FROM {tb['build']}
  WHERE NOT coalesce(is_xpac, false)
),
j AS (
  SELECT bw.work_id, bw.build_type, bw.pl_prov, bw.pl_nid,
         CASE WHEN bw.x IS NULL OR bw.x = '' THEN NULL WHEN bw.x = 'libguide' THEN 'libguides'
              WHEN bw.x = 'conference-proceedings' THEN 'conference-paper' WHEN bw.x = 'thesis' THEN 'dissertation'
              ELSE bw.x END AS today_type,
         sc.s2_input_hash, sc.class_idx, sc.conf_stored, sc.s0_doi, sc.s0_names, sc.s0_urls
  FROM bw JOIN sc ON sc.work_id = bw.work_id
),
dd AS (
  SELECT j.*, pol.policy_id, pol.model_id, pol.tau,
         CASE WHEN j.class_idx BETWEEN 0 AND {d.size('pol.classes')} - 1 THEN {d.elem('pol.classes', 'j.class_idx + 1')}
              ELSE {d.fail("concat('class_idx outside the model class list: work ', CAST(j.work_id AS STRING))")} END AS s2_type,
         CASE WHEN CAST(j.conf_stored AS DECIMAL(5,4)) = j.conf_stored THEN CAST(j.conf_stored AS DECIMAL(5,4))
              ELSE {d.fail("concat('confidence is not a stored 4-dp value: work ', CAST(j.work_id AS STRING))")} END AS s2_confidence
  FROM j CROSS JOIN pol
),
-- C4, fallback today (student/prod writer_lists.write_rows_cuts_lists): stage 2's type where its stored 4-dp confidence
-- passes tau and the type's cut (a null cut = never), and the type is live; else today's type if live
pp AS (
  SELECT dd.*, ts.type IS NOT NULL AND dd.s2_confidence >= dd.tau
               AND (c.type IS NULL OR (c.cut IS NOT NULL AND dd.s2_confidence >= c.cut)) AS passes,
         ts.is_traditional AS s2_trad, tt.type IS NOT NULL AS today_live, tt.is_traditional AS today_trad
  FROM dd
  LEFT JOIN ty ts ON ts.type = dd.s2_type
  LEFT JOIN ty tt ON tt.type = dd.today_type
  LEFT JOIN cuts c ON c.type = dd.s2_type
),
b AS (
  SELECT work_id, build_type, pl_prov, pl_nid, s0_doi, s0_names, s0_urls, policy_id, model_id,
         CASE WHEN passes THEN s2_type WHEN today_live THEN today_type END AS type_v2,
         CASE WHEN passes THEN s2_confidence END AS confidence,
         CASE WHEN passes THEN concat('student-s2:', model_id) WHEN today_type IS NULL THEN 'no_today_type' ELSE 'fallback:today' END AS decided_by,
         CASE WHEN passes THEN 'student-s2' WHEN today_type IS NULL THEN 'no_today_type' ELSE 'fallback:today' END AS outcome,
         CASE WHEN passes THEN s2_trad WHEN today_live THEN today_trad END AS is_traditional,
         {d.sha32("concat_ws('|', '', s2_input_hash, coalesce(today_type, ''), '', '')")} AS input_hash,
         s2_type, s2_confidence, s2_input_hash, today_type
  FROM pp
),
-- stage-0 source lists (student/prod stage0.apply): a work matched by lists of exactly one type is settled by the first
-- such list in the policy's order; a list with both a DOI regex and URL substrings needs both to hit
l1 AS (SELECT * FROM lists WHERE NOT (doi_regex IS NOT NULL AND {d.size('url_substrings')} > 0)),
l2 AS (SELECT * FROM lists WHERE doi_regex IS NOT NULL AND {d.size('url_substrings')} > 0),
s0m AS (
  SELECT b.work_id, l1.ord, l1.list_id, l1.type FROM b CROSS JOIN l1
  WHERE (instr(b.s0_doi, '/') > 0 AND {d.contains('l1.doi_prefixes', "split_part(b.s0_doi, '/', 1)")})
     OR (l1.doi_regex IS NOT NULL AND {d.rlike('b.s0_doi', 'l1.doi_regex')})
  UNION ALL
  SELECT x.work_id, l1.ord, l1.list_id, l1.type
  FROM (SELECT work_id, {d.explode('s0_names')} AS n FROM b WHERE s0_names IS NOT NULL) x CROSS JOIN l1
  WHERE ({d.size('l1.names_exact')} > 0 OR l1.names_regex IS NOT NULL)
    AND ({d.contains('l1.names_exact', 'x.n')} OR (l1.names_regex IS NOT NULL AND {d.rlike('x.n', 'l1.names_regex')}))
  UNION ALL
  SELECT x.work_id, s.ord, s.list_id, s.type
  FROM (SELECT work_id, s0_doi AS doi, {d.explode('s0_urls')} AS u FROM b WHERE s0_urls IS NOT NULL) x
  CROSS JOIN (SELECT ord, list_id, type, doi_regex, {d.explode('url_substrings')} AS sub FROM l2) s
  WHERE {d.rlike('x.doi', 's.doi_regex')} AND instr(x.u, s.sub) > 0
),
s0 AS (
  SELECT work_id, min_by(list_id, ord) AS list_id, min(type) AS s0_type
  FROM (SELECT DISTINCT work_id, ord, list_id, type FROM s0m) m
  GROUP BY work_id
  HAVING count(DISTINCT type) = 1
),
dec AS (
  SELECT b.work_id, b.build_type, b.pl_prov, b.pl_nid, b.policy_id, b.model_id,
         CASE WHEN t0.type IS NOT NULL THEN s0.s0_type ELSE b.type_v2 END AS type_v2,
         CASE WHEN t0.type IS NOT NULL THEN NULL ELSE b.confidence END AS confidence,
         CASE WHEN t0.type IS NOT NULL THEN concat('stage0:', s0.list_id) ELSE b.decided_by END AS decided_by,
         CASE WHEN t0.type IS NOT NULL THEN 'stage0-list' ELSE b.outcome END AS outcome,
         CASE WHEN t0.type IS NOT NULL THEN t0.is_traditional ELSE b.is_traditional END AS is_traditional,
         CASE WHEN t0.type IS NOT NULL THEN {d.sha32("concat_ws('|', b.input_hash, s0.list_id)")} ELSE b.input_hash END AS input_hash,
         b.s2_type, b.s2_confidence, b.s2_input_hash, b.today_type
  FROM b
  LEFT JOIN s0 ON s0.work_id = b.work_id
  LEFT JOIN ty t0 ON t0.type = s0.s0_type                      -- a list type retired since the policy was made never settles
),
prot AS (SELECT DISTINCT provenance, native_id FROM {tb['locs']} WHERE classified_rule IN ({prot})),
del AS (SELECT DISTINCT work_id FROM {tb['deleted']}),
prev AS ({prev_sql}),
-- why the decision differs from last night's published one (null: it doesn't); 'unexplained' = nothing it rests on moved
f AS (
  SELECT dec.*, prev.pub_type, vv.vocab_version, prev.pv.type AS p_type, prev.pv.outcome AS p_outcome,
         prev.pv.decided_by AS p_decided_by, prev.pv.confidence AS p_confidence, prev.pv.is_traditional AS p_trad,
         CASE WHEN prev.pv IS NULL THEN 'new'
              WHEN prev.pv.type IS NOT DISTINCT FROM dec.type_v2 THEN NULL
              WHEN prev.pv.policy_id IS DISTINCT FROM dec.policy_id THEN 'policy'
              WHEN prev.pv.model_version IS DISTINCT FROM dec.model_id THEN 'model'
              WHEN prev.pv.vocab_version IS DISTINCT FROM vv.vocab_version THEN 'vocabulary'
              WHEN prev.pv.s2_input_hash IS DISTINCT FROM dec.s2_input_hash THEN 'input'
              WHEN (prev.pv.outcome = 'stage0-list' OR dec.outcome = 'stage0-list')
                   AND prev.pv.decided_by IS DISTINCT FROM dec.decided_by THEN 'stage0-list'
              WHEN dec.outcome <> 'student-s2' AND prev.pv.today_type IS DISTINCT FROM dec.today_type THEN 'today_type'
              ELSE 'unexplained' END AS flip,
         pr.native_id IS NOT NULL AS protected, dl.work_id IS NOT NULL AS deleted
  FROM dec CROSS JOIN vv
  LEFT JOIN prev ON prev.work_id = dec.work_id
  LEFT JOIN prot pr ON pr.provenance = dec.pl_prov AND pr.native_id = dec.pl_nid
  LEFT JOIN del dl ON dl.work_id = dec.work_id
),
-- an unexplained flip holds last night's decision (type, outcome, ...) until something it rests on moves: the next
-- night compares against the held decision, so the flip stays 'unexplained' (and Check_Flips loud) every night; the
-- new decision waits in held_type (Codex 11-09 #2)
h AS (
  SELECT f.*, flip = 'unexplained' AS held,
         CASE WHEN flip = 'unexplained' THEN p_type ELSE type_v2 END AS d_type,
         CASE WHEN flip = 'unexplained' THEN p_outcome ELSE outcome END AS d_outcome,
         CASE WHEN flip = 'unexplained' THEN p_decided_by ELSE decided_by END AS d_decided_by,
         CASE WHEN flip = 'unexplained' THEN p_confidence ELSE confidence END AS d_confidence,
         CASE WHEN flip = 'unexplained' THEN p_trad ELSE is_traditional END AS d_trad,
         CASE WHEN flip = 'unexplained' THEN type_v2 END AS held_type
  FROM f
),
-- served tonight: rule 1 stage-2 and list decisions only (fallback works keep tonight's cascade type); 2 live types only
-- (both kinds are live by construction); 3 never a removed work; 4 no study-registration written, and Jason's
-- study-registration rows stay; 4b protected cascade rules stay; 5 parent/child (#2065, not live yet); 6 the ramp
sv AS (
  SELECT h.*,
         sw.served = h.policy_id AND h.d_outcome IN ('student-s2', 'stage0-list') AND h.d_type IS NOT NULL
         AND h.d_type <> 'study-registration' AND coalesce(h.build_type, '') <> 'study-registration'
         AND NOT h.protected AND NOT h.deleted
         AND {d.bucket('h.work_id', 'sw.n_buckets')} < sw.k AS serve
  FROM h CROSS JOIN sw
)
SELECT work_id, d_type AS serve_type, {d.struct(s0_struct)} AS v2
FROM sv
""".strip()


def switch() -> dict:
    """The switch (lib/work_type_switch.json), inlined into the cell as literals when the kit is cut: the policy decided,
    and what is served ('rules' = nothing; the policy id = its decisions, on the ramp). Changing it is a kit re-cut and a
    Walden push, as a parameter of the bundle-owned End 2 End job would be; literals need no widgets in Jason's notebook."""
    v = json.loads(SWITCH_FILE.read_text())
    if set(v) != set(PARAMS) or not all(isinstance(x, str) and T.SAFE.match(x) for x in v.values()):
        raise T.TemplateError(f"{SWITCH_FILE.name}: exactly {PARAMS}, identifier-safe strings")
    if v["wt_policy_id"] not in shipped_policies() or v["wt_served"] not in ("rules", v["wt_policy_id"]):
        raise T.TemplateError(f"{SWITCH_FILE.name}: wt_policy_id must be a shipped policy, wt_served 'rules' or that policy")
    if not (int(v["wt_n_buckets"]) >= 1 and 0 <= int(v["wt_start_buckets"]) <= int(v["wt_n_buckets"]) and v["wt_step"] in ("1", "-1")):
        raise T.TemplateError(f"{SWITCH_FILE.name}: N >= 1, 0 <= start <= N, step 1 or -1")
    if T.load_policy(v["wt_policy_id"])["model_id"] not in SCORED_MODELS:
        raise T.TemplateError(f"{SWITCH_FILE.name}: {v['wt_policy_id']}'s model isn't one the Modal app scores {SCORED_MODELS}")
    return v


def cell_sql(policy_ids=None, values: dict | None = None, tables: dict | None = None) -> str:
    """The cell: one compound statement (cell 32's pattern), with a handler that keeps last night's state on any error.
    It writes openalex_works_build only (Walden CI: scripts/check_works_enriched_publish.py), so the first night, when
    the published table has no type_decision yet, decides without last night's (every row 'new'); the publish then takes
    its slow path once (~20 min) and is fast again from night 2 (the column is appended last, the same type every night)."""
    pids = list(policy_ids or shipped_policies())
    values = values or switch()
    tb = dict(SPARK_TABLES, **(tables or {}))           # `tables`: a dev dry run's stand-ins (goline1.py cell-dryrun)
    BUILD, WORKS = tb["build"], tb["works"]

    def has(full):
        cat, sch, tab = full.split(".")
        return (f"(SELECT count(*) FROM {cat}.information_schema.columns WHERE table_schema = '{sch}' "
                f"AND table_name = '{tab}' AND column_name = 'type_decision') > 0")
    add_col = (f"    IF NOT {has(BUILD)} THEN\n"
               f"      ALTER TABLE {BUILD} ADD COLUMNS (type_decision {V2_TYPE});\n"
               f"    END IF;")
    check = switch_check_query(pids, values=values).replace("\n", "\n  ")
    merge = lambda q: (f"MERGE INTO {BUILD} AS t\n    USING (\n{q}\n    ) AS s\n"  # noqa: E731
                       f"    ON t.id = s.work_id\n    WHEN MATCHED THEN UPDATE SET t.type_decision = s.v2,\n"
                       f"      t.type = CASE WHEN s.v2.served THEN s.serve_type ELSE t.type END;")
    head = [
        "-- Work types v2 (oxjob #1976): the decision beside type, and type itself only where the switch serves.",
        "-- GENERATED by notebooks/work_types/lib/work_type_cell.py from the shipped policies (" + ", ".join(pids) + ");",
        "-- edit the generator or the switch file, never this cell. Switch (lib/work_type_switch.json): "
        + ", ".join(f"{k} {values[k]}" for k in PARAMS) + ".",
        "-- Reads work_type_scores (our job writes it after",
        "-- each End 2 End), tonight's build (the cascade's type), last night's openalex_works (to explain flips) and",
        "-- common.work_types; writes openalex_works_build only. Any error: the handler keeps last night's type_decision and",
        "-- served types, End 2 End publishes as usual, and Classify_Work_Types' Check_Flips fails loud the same day. Runs before",
        "-- FWCI (cell 12) and the content hash (cell 26), which read type; type_decision is not hashed, so the shadow alone",
        "-- re-stamps nothing.",
    ]
    return "\n".join(head) + f"""
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
      SELECT 'work types: the decision and its fallback both failed; no type_decision tonight' AS work_types;
{add_col}
    IF {has(WORKS)} THEN
      MERGE INTO {BUILD} AS t
      USING (SELECT id, type, type_decision FROM {WORKS} WHERE type_decision IS NOT NULL) AS p
      ON t.id = p.id
      WHEN MATCHED THEN UPDATE SET t.type_decision = p.type_decision,
        t.type = CASE WHEN p.type_decision.served THEN p.type ELSE t.type END;
    END IF;
    SELECT 'work types: the decision failed; last night''s decisions and served types kept' AS work_types;
  END;
  -- the switch, checked in a statement of its own (one row, always evaluated): a bad value fails here, into the handler
  {check};
{add_col}
  IF {has(WORKS)} THEN
    {merge(decide_query(pids, values=values, tables=tables))}
  ELSE
    -- the first night: nothing published to compare with yet
    {merge(decide_query(pids, values=values, tables=tables, prev=False))}
  END IF;
END"""


def notebook_cell(policy_ids=None) -> dict:
    """The cell as an .ipynb code cell (the notebook's language is SQL)."""
    src = cell_sql(policy_ids)
    lines = src.split("\n")
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
            "source": [ln + "\n" for ln in lines[:-1]] + [lines[-1]]}


if __name__ == "__main__":
    print(json.dumps({"policies": shipped_policies(), "chars": len(cell_sql())}))
