#!/usr/bin/env python3
"""Generate the per-funder award-id key function + verdict table (oxjob #690).

Layers on the 2026-07-31 award-id-audit machinery: award_norm_key() replaces the
inline generic normalized-key CASE in CreateAwards (aliases + collapse) and
WorkAwards (canonical lookup). For the 15 audited funders it applies the
validated translation rule; everywhere else it reproduces the generic key
byte-for-byte, so unconfigured funders are untouched.

Rules source of truth: oxjob #690 audit (2.38M ids, 2026-07-28) — the FUNDERS
dict below mirrors ~/grants-work/690-audit/audit.py and translation-config.yaml.

Family policy (merge onto newest-year record, @kyle-approved 2026-08-03) is
implemented in the notebooks; this generator emits keys + weakness + verdicts.

Usage: python3 scripts/gen_award_norm_key.py [--schema openalex.awards]
Emits notebooks/awards/AwardNormKey.sql (function + verdict table).
"""
import argparse, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    # vendored rules (source of truth: #690 audit)
    from award_translation_rules import (FUNDERS, NORM, DECOR_LEAD, DECOR_TRAIL,
                                         MULTI_DETECT, MULTI_SPLIT, XGRAM,
                                         S3_NUMERIC_CHASSIS, EXTRACTIVE_FIDS,
                                         FOREIGN_SCHEMES, JUNK_POSITIVE)
except ImportError:  # legacy dev path
    sys.path.insert(0, "/tmp"); from rules_src import FUNDERS, NORM

GENERIC = ("CASE WHEN award_id IS NULL THEN NULL "
           "WHEN LENGTH(REGEXP_REPLACE(LOWER(award_id), '[^a-z0-9]', '')) >= 4 "
           "THEN REGEXP_REPLACE(LOWER(award_id), '[^a-z0-9]', '') "
           "ELSE LOWER(TRIM(award_id)) END")

# Funders whose registry keys are bare numbers in a dense space: a deposited
# id that is ONLY a bare number can coincidentally hit the registry (measured
# 2026-08-03: 23.4% of foreign numerics hit CIHR's 77k keys). Matches whose
# deposited form has no self-identifying structure get verdict confirmed_weak:
# kept + flagged, NEVER auto-merged. Self-identifying forms (prefix, 950-,
# _1 suffix, /Z/16/Z, scheme codes) stay 'confirmed'.
WEAK_BARE = {
  4320334506: r"^[0-9]{4,6}$",          # CIHR (foreign-hit 15.4%)
  4320311904: r"^[0-9]{5,6}$",          # Wellcome bare cores (3.2% but native-bare stream)
  4320320924: r"^[0-9]{4,6}$",          # SNSF bare numbers (17.3%)
  4320320300: r"^[0-9]{6}$",            # EC bare FP-era GAs (19.0%)
  4320306076: r"^[0-9]{7}$",            # NSF bare 7-digit (39.6% — worst measured; review F2)
  4320334593: r"^[0-9]{4,6}$",          # NSERC bare serials (19.9%; 70% 4-digit density; review F2)
}

# Legacy provenance still sourced from openalex_awards_raw: scored but NOT
# guarded (tiny: 3 suppressed-id survivors measured 2026-08-03), so no
# oscillation risk. 'crossref_work.grants' moved OUT of here when its door
# (CreateBackfillAwards) became the 4th guarded leg — its pre-guard source is
# the backfill_awards intermediate in JUNCTIONS.
LEGACY_PROV = "'crossref_work'"

# Pre-guard junction tables of the three guarded legs. Verdicts MUST source
# deposited ids from these, not from openalex_awards_raw: once the legs consume
# the guard, raw no longer contains suppressed ids and sourcing from it would
# oscillate (suppressed -> unscored -> minted -> scored -> suppressed ...).
JUNCTIONS = ("openalex.awards.crossref_work_funders",
             "openalex.awards.europepmc_work_funders",
             "openalex.awards.datacite_work_funders")


def case_over(field):
    return "\n".join(
        f"      WHEN funder_id = {r['fid']} THEN ({r[field].replace('norm', '_n')})"
        for r in FUNDERS.values())


def gram_case():
    return "\n".join(
        f"      WHEN k.funder_id = {r['fid']} THEN ({r['gram'].replace('norm', 'k._n')})"
        for r in FUNDERS.values())


def gram_case_over(fid_col, var):
    """Grammar CASE keyed by an arbitrary funder-id column over an arbitrary
    (already-normalized) string column — used by the salvage chain."""
    return "\n".join(
        f"      WHEN {fid_col} = {r['fid']} THEN ({r['gram'].replace('norm', var)})"
        for r in FUNDERS.values())


def sharp_fired_case(var):
    """Boolean CASE: TRUE when an EXTRACTIVE funder's deposited-side xkey
    fires (extracts a structured id) on `var`. Review pass-4 F2: a fired
    extractor that misses the registry means registry-coverage gap, not
    garbage — such ids score 'plausible', never suppress. Transform-based
    xkeys are excluded (they fire on everything)."""
    arms = "\n".join(
        f"      WHEN funder_id = {r['fid']} THEN (({r['xkey'].replace('norm', var)}) IS NOT NULL)"
        for r in FUNDERS.values() if r["fid"] in EXTRACTIVE_FIDS)
    return f"CASE\n{arms}\n      ELSE FALSE END"


def xgram_case_over(fid_col, var):
    """STRONG cross-grammar CASE for wrong-funder detection — only funders in
    XGRAM (i.e. with a distinctive lettered token structure) are targets."""
    return "\n".join(
        f"      WHEN {fid_col} = {fid} THEN ({pat.replace('norm', var)})"
        for fid, pat in XGRAM.items())


def gen(schema):
    norm_expr = NORM.format(c="award_id")
    fids = ",".join(str(r["fid"]) for r in FUNDERS.values())
    norm_expr2 = NORM.format(c="award_id")
    weak_cond_fn = " OR ".join(
        f"(funder_id = {fid} AND _n rlike '{pat}')"
        for fid, pat in WEAK_BARE.items()) or "FALSE"
    weak_cond = " OR ".join(
        f"(funder_id = {fid} AND _n rlike '{pat}')"
        for fid, pat in WEAK_BARE.items()) or "FALSE"
    xfids = ",".join(str(fid) for fid in XGRAM)
    foreign_cond = "(" + "\n      OR ".join(
        f"_n rlike '{p}'" for p in FOREIGN_SCHEMES) + ")"
    junk_cond = "(" + "\n         OR ".join(
        f"_n rlike '{p}'" for p in JUNK_POSITIVE) + ")"
    foreign_cond_s = foreign_cond.replace("_n rlike", "s rlike")
    dep_union = "\n    UNION ALL\n".join(
        f"    SELECT funder_id, aid AS funder_award_id\n"
        f"    FROM {j} LATERAL VIEW EXPLODE(award_ids) AS aid"
        for j in JUNCTIONS) + f"""
    UNION ALL
    -- 4th guarded door: CreateBackfillAwards ('crossref_work.grants'); its
    -- pre-guard source is the backfill_awards intermediate (already exploded)
    SELECT funder_id, funder_award_id
    FROM openalex.awards.backfill_awards
    UNION ALL
    -- legacy crossref provenance (scored, not guarded — no pre-guard table)
    SELECT funder_id, funder_award_id
    FROM openalex.awards.openalex_awards_raw
    WHERE provenance IN ({LEGACY_PROV}) AND funder_award_id IS NOT NULL"""
    return f"""-- GENERATED by scripts/gen_award_norm_key.py — DO NOT EDIT BY HAND.
-- Per-funder award-id translation key (oxjob #690), layered on the 2026-07-31
-- award-id-audit generic key. Family policy (merge onto newest year,
-- @kyle 2026-08-03) lives in the NOTEBOOKS (CreateAwards/WorkAwards) — this
-- file only defines keys, weakness, and verdicts.

-- Weakness predicate (review F1/F8): TRUE when a deposited string's NORMALIZED
-- form is a bare number at a dense-numeric-registry funder — such evidence is
-- never sufficient for an automatic merge. Single source of truth: WEAK_BARE.
CREATE OR REPLACE FUNCTION {schema}.award_id_is_weak(funder_id BIGINT, award_id STRING)
RETURNS BOOLEAN
RETURN
WITH _t AS (SELECT {norm_expr2} AS _n)
SELECT COALESCE({weak_cond_fn}, FALSE) FROM _t;

CREATE OR REPLACE FUNCTION {schema}.award_norm_key(funder_id BIGINT, award_id STRING, side STRING)
RETURNS STRING
RETURN
WITH _t AS (SELECT {norm_expr} AS _n)
SELECT COALESCE(
  CASE WHEN side = 'registry' THEN
    CASE
{case_over('rkey')}
    END
  ELSE
    CASE
{case_over('xkey')}
    END
  END,
  {GENERIC}
) FROM _t;

-- Verdict per deposited id (feeds the junk guard; read-only w.r.t. awards flow):
--   confirmed           key uniquely hits the funder's registry
--   confirmed_ambiguous key hits >1 registry award (renewal-year families)
--   plausible           fits the funder's id grammar, no registry hit
--   garbage             neither (salvage chain runs before any suppression)
-- Unconfigured funders get verdict 'unscored'.
CREATE OR REPLACE TABLE {schema}.award_id_verdicts
USING delta
AS
WITH dep AS (
  -- Deposited ids from the PRE-GUARD junction tables (see JUNCTIONS note in
  -- the generator) plus legacy crossref provenances that have no junction.
  SELECT DISTINCT funder_id, funder_award_id FROM (
{dep_union}
  ) WHERE funder_award_id IS NOT NULL
),
-- TWO-KEY registry check, mirroring the CreateAwards alias/collapse machinery
-- exactly (canonical_g on the pure generic key + canonical_s on the sharp
-- COALESCE key). Checking only the COALESCE key under-detects: an id whose
-- sharp extraction fires but misses the registry can still collapse via its
-- generic key — scoring it 'garbage' would suppress ids the fold merges today
-- (2,848 such ids caught by the collapse-safety gate, 2026-08-03).
reg AS (
  SELECT funder_id, {schema}.award_norm_key(funder_id, funder_award_id, 'registry') AS nk,
         COUNT(DISTINCT id) AS n_awards
  FROM openalex.awards.openalex_awards_raw
  WHERE priority >= 3 AND funder_award_id IS NOT NULL
  GROUP BY 1, 2
),
reg_g AS (
  SELECT funder_id, {GENERIC.replace("award_id", "funder_award_id")} AS nk_g,
         COUNT(DISTINCT id) AS n_awards
  FROM openalex.awards.openalex_awards_raw
  WHERE priority >= 3 AND funder_award_id IS NOT NULL
  GROUP BY 1, 2
),
keyed AS (
  SELECT funder_id, funder_award_id, _n,
    {schema}.award_norm_key(funder_id, funder_award_id, 'deposited') AS nk,
    {GENERIC.replace("award_id", "funder_award_id")} AS nk_g,
    -- review F2: extractor fired = evidence-bearing id even without a
    -- registry hit (registry-coverage gap, not garbage)
    {sharp_fired_case('_n')} AS sk_fired
  FROM (SELECT funder_id, funder_award_id,
          {NORM.format(c="funder_award_id")} AS _n
        FROM dep)
),
scored AS (
  SELECT k.funder_id, k.funder_award_id, k._n, k.nk, k.sk_fired,
    COALESCE(r.n_awards, rg.n_awards) AS n_awards,
    CASE
{gram_case()}
    END AS grammar_pass
  FROM keyed k
  LEFT JOIN reg r ON r.funder_id = k.funder_id AND r.nk = k.nk
  LEFT JOIN reg_g rg ON rg.funder_id = k.funder_id AND rg.nk_g = k.nk_g
)
SELECT funder_id, funder_award_id, nk, n_awards,
  CASE
    WHEN funder_id NOT IN ({fids}) THEN 'unscored'
    -- review F3: weak surface-form is checked FIRST — a bare number is weak
    -- evidence regardless of how many registry awards it happens to hit
    WHEN n_awards >= 1 AND ({weak_cond}) THEN 'confirmed_weak'
    WHEN n_awards = 1 THEN 'confirmed'
    WHEN n_awards > 1 THEN 'confirmed_ambiguous'
    WHEN grammar_pass THEN 'plausible'
    -- review F2: fired extractor without a registry hit = plausible, never
    -- garbage (harm class: "OPUS 2019/35/B/ST10/04141", real ids the
    -- registry doesn't carry yet)
    WHEN sk_fired THEN 'plausible'
    -- foreign-scheme keep-list (2026-08-03 shape census): a known grant-id
    -- scheme of a funder OUTSIDE the configured set never suppresses
    WHEN {foreign_cond} THEN 'foreign_scheme'
    ELSE 'garbage'
  END AS verdict,
  current_timestamp() AS scored_date
FROM scored;

-- ============================================================================
-- Salvage chain (#690 guard, 2026-08-03 priority 1). Runs over garbage-verdict
-- ids BEFORE any suppression. Three rescue steps:
--   decorated_own_id  generic decoration strip -> re-key -> own-registry hit
--                     (suffix _weak when the stripped form is a bare number at
--                     a WEAK_BARE funder: minted but never merge material;
--                     decorated_plausible when the stripped form only passes
--                     the funder's grammar — parity with S2 plausible parts)
--   multi_id_split    concatenated ids ("A123, B456"): split, score each part;
--                     rescued when any part confirms or is plausible
--   wrong_funder      DETECTION ONLY (re-link write = #624's): letter-bearing
--                     ids that pass another funder's STRONG cross-grammar
--                     (XGRAM: distinctive funder tokens only) AND hit that
--                     funder's registry. Bare numerics are excluded by
--                     construction (the 2026-08-03 foreign-numeric control
--                     measured 15-40% coincidental hits on dense-numeric
--                     registries), which also makes WEAK_BARE unreachable here.
-- A salvage row exempts the id from suppression; it never changes minting or
-- merging by itself.
-- ============================================================================
CREATE OR REPLACE TABLE {schema}.award_id_salvage
USING delta
AS
WITH reg AS (
  SELECT funder_id, nk, COUNT(DISTINCT id) AS n_awards FROM (
    SELECT funder_id, id,
      {schema}.award_norm_key(funder_id, funder_award_id, 'registry') AS nk
    FROM openalex.awards.openalex_awards_raw
    WHERE priority >= 3 AND funder_award_id IS NOT NULL
  ) WHERE nk IS NOT NULL GROUP BY 1, 2
),
-- generic-key registry arm: S1/S2 rescue checks BOTH keys, mirroring the
-- two-key alias/collapse machinery (same reasoning as in award_id_verdicts)
reg_g AS (
  SELECT funder_id, {GENERIC.replace("award_id", "funder_award_id")} AS nk_g,
         COUNT(DISTINCT id) AS n_awards
  FROM openalex.awards.openalex_awards_raw
  WHERE priority >= 3 AND funder_award_id IS NOT NULL
  GROUP BY 1, 2
),
garbage AS (
  SELECT funder_id, funder_award_id,
    {NORM.format(c="funder_award_id")} AS _n
  FROM {schema}.award_id_verdicts WHERE verdict = 'garbage'
),
-- S1: decorated own-id (strip applied lead-then-trail-twice: "12345 (ABC).")
stripped AS (
  SELECT funder_id, funder_award_id, _n,
    regexp_replace(regexp_replace(regexp_replace(_n,
      '{DECOR_LEAD}', ''), '{DECOR_TRAIL}', ''), '{DECOR_TRAIL}', '') AS s
  FROM garbage
),
s1_keyed AS (
  -- UDFs in projection only (Spark forbids them in JOIN ON / LATERAL VIEW)
  SELECT funder_id, funder_award_id, s,
    {schema}.award_norm_key(funder_id, s, 'deposited') AS s_nk,
    {GENERIC.replace("award_id", "s")} AS s_nk_g,
    {schema}.award_id_is_weak(funder_id, s) AS s_weak,
    CASE
{gram_case_over('funder_id', 's')}
    END AS s_gram
  FROM stripped WHERE s <> '' AND s <> _n
),
s1 AS (
  -- rescue on registry hit (either key), or — parity with S2's plausible
  -- parts — on the stripped form passing the funder's own grammar
  -- ("# 88887.684374/2022-00" at a funder whose registry can't confirm it)
  SELECT k.funder_id, k.funder_award_id,
    CASE WHEN COALESCE(r.nk, rg.nk_g) IS NOT NULL AND k.s_weak THEN 'decorated_own_id_weak'
         WHEN COALESCE(r.nk, rg.nk_g) IS NOT NULL THEN 'decorated_own_id'
         WHEN k.s_gram AND NOT k.s_weak THEN 'decorated_plausible'
         ELSE 'foreign_scheme_decorated' END AS action,
    k.funder_id AS target_funder_id,
    COALESCE(r.nk, rg.nk_g) AS target_nk,
    COALESCE(r.n_awards, rg.n_awards) AS n_awards,
    k.s AS detail
  FROM s1_keyed k
  LEFT JOIN reg r ON r.funder_id = k.funder_id AND r.nk = k.s_nk
  LEFT JOIN reg_g rg ON rg.funder_id = k.funder_id AND rg.nk_g = k.s_nk_g
  -- round-2 audit: the stripped residue matching a FOREIGN scheme also
  -- rescues ("DGE-1650116)." at NIH, "107337/Z/15/Z/" trailing slash)
  WHERE COALESCE(r.nk, rg.nk_g) IS NOT NULL OR (k.s_gram AND NOT k.s_weak)
     OR {foreign_cond_s}
     -- round-3: bare 7-8 digit residues are non-junk whole-string (NSF/NSFC
     -- forms), so a trailing-punctuation variant must not die either
     OR k.s rlike '^[0-9]{{6,8}}$'
),
-- S2: multi-id concat split
multi AS (
  SELECT funder_id, funder_award_id, _n FROM garbage
  WHERE {MULTI_DETECT}
),
parts0 AS (
  SELECT funder_id, funder_award_id, TRIM(p) AS p0
  FROM multi LATERAL VIEW EXPLODE(split(_n, '{MULTI_SPLIT}')) AS p
),
parts AS (
  SELECT funder_id, funder_award_id,
    regexp_replace(regexp_replace(regexp_replace(p0,
      '{DECOR_LEAD}', ''), '{DECOR_TRAIL}', ''), '{DECOR_TRAIL}', '') AS part
  FROM parts0 WHERE p0 <> ''
),
parts_keyed AS (
  SELECT funder_id, funder_award_id, part,
    {schema}.award_norm_key(funder_id, part, 'deposited') AS p_nk,
    {GENERIC.replace("award_id", "part")} AS p_nk_g,
    {schema}.award_id_is_weak(funder_id, part) AS p_weak,
    CASE
{gram_case_over('funder_id', 'part')}
    END AS p_gram
  FROM parts WHERE part <> ''
),
s2_scored AS (
  SELECT p.funder_id, p.funder_award_id,
    COUNT(*) AS n_parts,
    SUM(CASE WHEN COALESCE(r.nk, rg.nk_g) IS NOT NULL AND NOT p.p_weak THEN 1 ELSE 0 END) AS n_confirmed_parts,
    SUM(CASE WHEN COALESCE(r.nk, rg.nk_g) IS NULL AND p.p_gram AND NOT p.p_weak THEN 1 ELSE 0 END) AS n_plausible_parts
  FROM parts_keyed p
  LEFT JOIN reg r ON r.funder_id = p.funder_id AND r.nk = p.p_nk
  LEFT JOIN reg_g rg ON rg.funder_id = p.funder_id AND rg.nk_g = p.p_nk_g
  GROUP BY 1, 2
),
s2 AS (
  SELECT funder_id, funder_award_id, 'multi_id_split' AS action,
    funder_id AS target_funder_id, CAST(NULL AS STRING) AS target_nk,
    n_confirmed_parts AS n_awards,
    CONCAT('parts=', n_parts, ' confirmed=', n_confirmed_parts,
           ' plausible=', n_plausible_parts) AS detail
  FROM s2_scored
  WHERE n_confirmed_parts >= 1 OR n_plausible_parts >= 1
),
-- S3: wrong-funder detection. Letter-bearing ids only (bare numerics excluded
-- by the foreign-numeric control), and the claim requires the target funder's
-- STRONG cross-grammar (XGRAM): funders without a distinctive lettered token
-- structure (NSFC, SNSF) are not cross-targets at all — their generic `gram`
-- fields accept non-self-identifying strings and produced tens of thousands
-- of coincidental hits against dense numeric registries in the first build.
wf0 AS (
  -- candidates: letter-bearing, or the FAPESP numeric chassis (NN/NNNNN-N —
  -- structured punctuation, not a bare number). Chassis pattern comes from
  -- the S3_NUMERIC_CHASSIS constant: inlining it in the generator f-string
  -- turned the braces into Python tuples and emitted a never-matching regex
  -- (review pass-4 F1).
  SELECT g.funder_id, g.funder_award_id, g._n, t.t_fid
  FROM (SELECT * FROM garbage
        WHERE _n rlike '[A-Z]' OR _n rlike '{S3_NUMERIC_CHASSIS}') g
  CROSS JOIN (SELECT explode(array({xfids})) AS t_fid) t
  WHERE t.t_fid <> g.funder_id
),
wf_keyed AS (
  SELECT funder_id, funder_award_id, t_fid,
    {schema}.award_norm_key(t_fid, funder_award_id, 'deposited') AS f_nk,
    CASE
{xgram_case_over('t_fid', '_n')}
    END AS f_gram
  FROM wf0
),
s3 AS (
  SELECT k.funder_id, k.funder_award_id, 'wrong_funder' AS action,
    k.t_fid AS target_funder_id, r.nk AS target_nk, r.n_awards,
    CAST(NULL AS STRING) AS detail
  FROM (SELECT * FROM wf_keyed WHERE f_gram) k
  JOIN reg r ON r.funder_id = k.t_fid AND r.nk = k.f_nk
)
SELECT funder_id, funder_award_id, action, target_funder_id, target_nk,
       n_awards AS n_registry_awards, detail,
       current_timestamp() AS created_date
FROM (SELECT * FROM s1 UNION ALL SELECT * FROM s2 UNION ALL SELECT * FROM s3);

-- ============================================================================
-- Guard decision table — the single checkpoint the four covered ingest doors
-- (CreateCrossrefWorkFunders / CreateEuropePmcWorkFunders /
-- CreateDataCiteWorkFunders / CreateBackfillAwards) consume at mint time.
-- One row per scored (funder_id, funder_award_id). suppress = garbage with no
-- salvage rescue. Ids ABSENT from this table (new since the last scoring run)
-- mint fail-open.
-- ============================================================================
CREATE OR REPLACE TABLE {schema}.award_id_guard
USING delta
AS
-- DESIGN FLIP (recalibration round 1, 2026-08-03): suppression requires
-- POSITIVE junk classification. "Failed to verify" alone is NOT junk — the
-- random-327 audit measured 64.8% of failed-to-verify suppressions as real
-- grants in mangled dialects. Unclassifiable strings default to KEEP.
SELECT funder_id, funder_award_id, verdict,
  CASE WHEN verdict = 'garbage' AND actions IS NULL AND is_junk
       THEN 'suppress' ELSE 'mint' END AS decision,
  CASE WHEN verdict <> 'garbage' THEN verdict
       WHEN actions IS NOT NULL THEN CONCAT('salvaged:', actions)
       WHEN is_junk THEN 'junk_positive'
       ELSE 'unclassified_kept' END AS reason,
  current_timestamp() AS created_date
FROM (
SELECT v.funder_id, v.funder_award_id, v.verdict, s.actions,
  ({junk_cond}
   OR (v.funder_id = 4320306084 AND _n rlike '^[0-9]{{6}}$')  -- DOE-scoped: bare-6 = facility proposal ids (auditor-endorsed junk); global bare-6 carved out for SNSF/EC GA space
  ) AS is_junk
FROM (SELECT *, {NORM.format(c='funder_award_id')} AS _n FROM {schema}.award_id_verdicts) v
LEFT JOIN (
  SELECT funder_id, funder_award_id,
         array_join(array_sort(collect_set(action)), '+') AS actions
  FROM {schema}.award_id_salvage GROUP BY 1, 2
) s ON s.funder_id = v.funder_id AND s.funder_award_id = v.funder_award_id
);
"""


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="openalex.awards")
    a = ap.parse_args()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                       "notebooks", "awards", "AwardNormKey.sql")
    open(out, "w").write(gen(a.schema))
    print(f"wrote {os.path.normpath(out)} (schema={a.schema})")
