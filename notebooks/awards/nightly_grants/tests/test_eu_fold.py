"""Offline tests for EU_GRANT_FAMILY_FOLD (oxjob #690), the second family class in nih_fold.py.

Without EU pairs the build (build_awards + nih_fold, NIH pairs listed or not) and the NIH link gates render exactly as the committed
code at git HEAD does; with them, every hook the class needs is on, each held reason is in the proof, and the EU class never gets
the NIH bare-number exemption from the direct-target gates."""
import importlib.util
import pathlib
import re
import subprocess
import sys
import tempfile
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

import nih_fold
from test_nih_fold import RecordingC, Frame, load, four_tier_free

EU = "EU_GRANT_FAMILY_FOLD"
NIH = "NIH_PROJECT_YEAR_FOLD"


def _head_lib():
    """build_awards.py and nih_fold.py as committed at git HEAD (main's version while this change is uncommitted or on its branch's
    base), in a temp dir; None outside a git checkout."""
    try:
        top = pathlib.Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=ROOT, text=True).strip())
        base = subprocess.check_output(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, text=True).strip()
        d = pathlib.Path(tempfile.mkdtemp(prefix="eu_base_"))
        for f in ("build_awards.py", "nih_fold.py"):
            rel = (ROOT / "lib" / f).resolve().relative_to(top)
            (d / f).write_text(subprocess.check_output(["git", "show", f"{base}:{rel}"], cwd=ROOT, text=True))
        return d
    except Exception:
        return None


HEAD = _head_lib()
needs_head = pytest.mark.skipif(HEAD is None, reason="git base of this branch not found")


B = "B"   # sub-class B rows (a doi in evidence_json) listed: the probe nih_fold.eu_b_active makes


class C(RecordingC):
    """RecordingC whose manifest probe answers per class (B: EU rows with a doi)."""
    def __init__(self, listed=(), **kw):
        super().__init__(**kw)
        self.listed = set(listed)

    def sql(self, statement, params=None):
        if "'$.doi') IS NOT NULL" in statement and " LIMIT 1" in statement:
            self.statements.append(("sql", " ".join(statement.split())))
            return Frame([types.SimpleNamespace(x=1)] if B in self.listed else [])
        for cls in (NIH, EU):
            if f"evidence_class='{cls}' LIMIT 1" in statement:
                self.statements.append(("sql", " ".join(statement.split())))
                return Frame([types.SimpleNamespace(x=1)] if cls in self.listed else [])
        return super().sql(statement, params)


def _render(build_path, fold, listed=(), extra=("migration_manifest",)):
    """Render prepare/allocate/plan/project with build_awards bound to the given nih_fold module."""
    saved = sys.modules.get("nih_fold")
    sys.modules["nih_fold"] = fold
    try:
        mod = load("ba_eu_" + build_path.parent.name + "_" + fold.__name__, build_path)
    finally:
        sys.modules["nih_fold"] = saved
    c = C(listed=listed, extra=extra)
    mod.prepare(c); mod.allocate(c); mod.plan(c); mod.project(c)
    return c


NIH_AMOUNT_STATEMENTS = {"nih_obs", "nih_year_cost", "nih_span"}
# #2014's date guard owns the shell rule's retirement statements (proposals, the guard's three steps, the guarded rule, and the
# two manifest checks that read the proposals); test_retirement_dates.py pins each of them
DATE_GUARD_STATEMENTS = {"retirements_rule_proposed", "retirement_date_units", "retirement_date_papers", "retirement_date_held",
                         "retirements_rule", "MIGRATION_CONFLICT_WITH_RULE", "public_shell_held"}


def _probe_free(c):
    return [s for s in c.statements if "' LIMIT 1" not in s[1] or "evidence_class=" not in s[1]]


def _four_tier_free(c):
    return four_tier_free(_probe_free(c))   # four tiers' own statements: test_nih_fold.FOUR_TIER_STATEMENTS


def _head_fold():
    spec = importlib.util.spec_from_file_location("nih_fold_head", HEAD / "nih_fold.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


@needs_head
@pytest.mark.parametrize("listed,extra", [((), ()), ((), ("migration_manifest",)), ((NIH,), ("migration_manifest",))])
def test_without_eu_pairs_build_renders_as_head(listed, extra):
    """No EU pair listed: every statement of the build, NIH pairs or not, is the one main renders today (the probes aside)."""
    old = _render(HEAD / "build_awards.py", _head_fold(), listed, extra)
    new = _render(ROOT / "lib" / "build_awards.py", nih_fold, listed, extra)
    # #2326 (NIH project amounts) owns the sum's year-cost statements; test_funding_population and test_nih_fold pin them
    own = lambda c: [s for s in _four_tier_free(c) if s[0] not in NIH_AMOUNT_STATEMENTS | DATE_GUARD_STATEMENTS]
    assert own(new) == own(old)
    assert new.checks.keys() - {"DOI_TWIN_FUSE"} == old.checks.keys() - {"DOI_TWIN_FUSE"}   # four tiers' one new check


@needs_head
def test_without_eu_pairs_nih_link_gates_render_as_head():
    head = _head_fold()
    old, new = C(), C()
    old.nih = new.nih = True
    head.link_conservation(old); nih_fold.link_conservation(new)
    assert new.statements == old.statements and new.checks.keys() == old.checks.keys()


def eu_build(listed=(EU,)):
    return _render(ROOT / "lib" / "build_awards.py", nih_fold, listed)


def test_with_eu_pairs_every_hook_is_on():
    c = eu_build()
    q = dict(c.statements); names = [n for n, _ in c.statements]
    assert c.eu is True and c.nih is False
    # survivors: the class's targets keep their own (registry) funder; family checked per class
    assert "m.evidence_class family_class" in q["nih_survivors"] and f"WHERE m.evidence_class='{EU}'" in q["nih_survivors"]
    assert "WHEN 'EU_GRANT_FAMILY_FOLD' THEN pinned_funder_id IN (4320314745," in q["NIH_SURVIVOR_FUNDER_IN_FAMILY"]
    assert "WHEN 'EU_GRANT_FAMILY_FOLD' THEN f.canonical_funder_id IN" in q["NIH_SURVIVOR_BINDINGS_IN_FAMILY"]
    # the pre-pin plan of an EU survivor is its registry funder, not min() over a copy's binding
    assert f"WHEN s.family_class='{EU}' THEN s.pinned_funder_id" in q["ownership_plan_unpinned"]
    # exemptions and own-funder lookups are on with EU pairs alone
    assert "nih_s.stable_id=b.stable_id" in q["ENTITY_FUNDER_AMBIGUOUS"] and "nih_s.stable_id=o.stable_id" in q["DIRECT_CORRECTION_UNPROVEN"]
    assert "lookup_funder_id" in q["bound_observations"] and "n.funder_id <=> b.lookup_funder_id" in q["resolved_observations"]
    assert "nih_obs_funder nof" in q["shell_targets"]
    # class allowed, public losers allowed; NOT exempt from the direct-target gates (a survivor is a registry record)
    for gate in ("MIGRATION_EVIDENCE_CLASS", "MIGRATION_LOSER_NEVER_PUBLIC"):
        assert EU in q[gate], gate
    for gate in ("MIGRATION_TARGET_DIRECT", "RETIREMENT_DIRECT_TARGET"):
        assert EU not in q[gate], gate
    assert f"m.evidence_class<>'{EU}'" in q["MIGRATION_SAME_FUNDER"]
    assert f"nm.evidence_class='{EU}'" in q["RETIREMENT_SAME_FUNDER"]
    # proof right after the NIH filter's slot; the later gates read the proven set
    assert names.index("migration_proven") < names.index("eu_pairs") < names.index("eu_held") < names.index("migration_proven_eu")
    assert "migration_proven_eu" in q["MIGRATION_TARGET_ACTIVE"] and "migration_proven_eu" in q["MIGRATION_CONFLICT_WITH_RULE"]
    for gate in ("EU_LISTED_SHAPE", "EU_ONE_NUMBER_PER_TARGET", "EU_HELD_FUSE", "EU_SURVIVOR_FUNDER_PUBLISHED", "FAMILY_SURVIVOR_FUNDER_AS_LISTED",
                 "EU_SURVIVOR_REGISTRY_ONE_FUNDER", "EU_SURVIVOR_ROW_FUSE"):
        assert gate in c.checks, gate
    # no NIH projection on an EU-only night
    assert "awards_candidate_before_nih" not in q and "nih_choice" not in q
    for n in ("eu_survivors_published", "eu_survivors_off_registry", "eu_family_drift", "eu_pending_pairs", "eu_listed_pairs"):
        assert n in c.counts, n


def test_eu_pin_is_the_registry_observations_funder():
    """An EU survivor takes the canonical funder of its own direct erc/cordis observation, else its recorded funder made canonical;
    an NIH survivor still takes the list's institute."""
    q = dict(eu_build((NIH, EU)).statements)
    rf = q["eu_registry_funder"]
    assert "b.is_direct AND b.payload.provenance IN ('erc','cordis')" in rf and "f.canonical_funder_id" in rf
    s = q["nih_survivors"]
    assert f"CASE WHEN m.evidence_class='{EU}' THEN coalesce(rf.funder_id,fc.canonical_funder_id,e.funder_id)" in s
    assert "ELSE coalesce(CAST(get_json_object(m.evidence_json,'$.project_funder_id') AS BIGINT),e.funder_id) END pinned_funder_id" in s
    assert "eu_registry_funder rf ON rf.stable_id=m.target_id" in s and "funder_canon fc ON fc.funder_id=e.funder_id" in s
    assert "funders<>1" in q["EU_SURVIVOR_REGISTRY_ONE_FUNDER"]


def test_list_checks_and_fuse_read_the_whole_eu_list():
    """Applied rows count: the identity check covers them, and the fuse's denominator is every listed EU pair, so pairs still held
    after their batch applied (pending shrinks to them) never trip the fuse on the next night."""
    q = dict(eu_build().statements)
    assert "FROM migration_manifest_v WHERE evidence_class='EU_GRANT_FAMILY_FOLD'" in q["eu_listed"]
    assert "eu_listed GROUP BY target_id" in q["EU_ONE_NUMBER_PER_TARGET"] and "eu_listed WHERE" in q["EU_LISTED_SHAPE"]
    assert "ngr_s_migration_proven m" in q["eu_pairs"] and "migration_manifest_v" not in q["eu_pairs"]   # the proof: pending only


def test_published_row_is_the_listed_registry_row():
    q = dict(eu_build().statements)
    off = q["eu_survivors_off_registry"]
    assert "LEFT JOIN dev.lab.ngr_s_awards_candidate a" in off                       # a missing survivor counts as off
    assert "a.provenance=s.registry AND trim(a.funder_award_id)=s.number" in off     # the listed registry and number, not any
    assert "greatest(100,0.01*0)" in q["EU_SURVIVOR_ROW_FUSE"]


@pytest.mark.parametrize("reason", nih_fold.EU_HELD_REASONS)
def test_each_held_reason_is_in_the_proof(reason):
    held = dict(eu_build().statements)["eu_held"]
    assert f"'{reason}'" in held


def test_held_reasons_read_the_right_relations():
    q = dict(eu_build().statements)
    held = q["eu_held"]
    assert "trim(loser_number)=number AND trim(target_number)=number" in held
    assert "target_provenance=registry" in held
    assert "n.owners=1 AND n.owner_id=p.target_id" in held
    assert "retirements_rule_proposed t ON t.old_id=p.loser_id" in held and "retirements_rule t ON t.canonical_id=p.loser_id" in held
    assert "migration_pending x ON x.target_id=p.loser_id" in held
    assert "key_type='NATIVE'" in held
    # uniqueness counts direct erc/cordis observations under an EU funder, provenance read narrow from bindings
    owners = q["eu_number_owners"]
    assert "payload.provenance provenance FROM dev.lab.ngr_s_bindings WHERE is_direct" in owners
    assert "b.provenance IN ('erc','cordis')" in owners and "o.funder_id IN (4320314745," in owners
    assert "ownership_plan_unpinned ul" in q["eu_pair_numbers"] and "ta.provenance target_provenance" in q["eu_pair_numbers"]
    assert "HAVING n>greatest(100,0.01*0)" in q["EU_HELD_FUSE"]


def test_both_classes_listed():
    c = eu_build((NIH, EU))
    q = dict(c.statements); names = [n for n, _ in c.statements]
    assert f"m.evidence_class IN ('{NIH}','{EU}')" in q["nih_survivors"]
    assert f"m.evidence_class NOT IN ('{NIH}','{EU}')" in q["MIGRATION_SAME_FUNDER"]
    assert f"nm.evidence_class IN ('{NIH}','{EU}')" in q["RETIREMENT_SAME_FUNDER"]
    assert NIH in q["MIGRATION_TARGET_DIRECT"] and EU not in q["MIGRATION_TARGET_DIRECT"]
    assert names.index("migration_proven_nih") < names.index("eu_pairs") < names.index("migration_proven_eu")
    assert "migration_proven_nih" in q["eu_pairs"]
    assert "awards_candidate_before_nih" in q and "eu_survivors" in q


def test_link_conservation_per_class():
    c = C(); c.nih = False; c.eu = True
    nih_fold.link_conservation(c)
    q = dict(c.statements)
    assert f"evidence_class='{EU}'" in q["eu_applied"] and "nih_applied" not in q
    assert "EU_NO_LOSER_LINKS" in c.checks and "EU_MAPPED_LINKS_PRESERVED" in c.checks and "EU_TARGET_LINKS_PRESERVED" in c.checks
    assert "work_survivors" in q["eu_links_before"]
    # the survivor's own links: missing tonight only if no producer makes them any more (registry_twin's contract)
    assert "EXCEPT SELECT work_id,award_id FROM dev.lab.ngr_s_eu_candidate_edges" in q["eu_missing_target"]
    assert "work_matches wm" in q["eu_target_producers"] and "work_links_moved mv" in q["eu_target_producers"]
    both = C(); both.nih = True; both.eu = True
    nih_fold.link_conservation(both)
    assert {"NIH_NO_LOSER_LINKS", "EU_NO_LOSER_LINKS"} <= both.checks.keys() and "NIH_TARGET_LINKS_PRESERVED" not in both.checks


def test_eu_family_is_the_call_code_guards_family():
    fam = nih_fold.EU_FAMILY
    assert len(fam) == 86 and len(set(fam)) == 86 and list(fam) == sorted(fam)
    guard = (ROOT.parent / "tests" / "test_erc_call_code_guard.py").read_text()
    assert f"FAMILY = r'{nih_fold.EU_FAMILY_RLIKE}'" in guard
    for name, member in [("European Research Council", True), ("H2020 European Research Council", True), ("European Commission", True),
                         ("HORIZON EUROPE Health", True), ("European Resuscitation Council", False), ("Wellcome Trust", False)]:
        assert bool(re.search(nih_fold.EU_FAMILY_RLIKE, name)) is member
    assert not set(fam) & set(nih_fold.NIH_FAMILY)


@pytest.mark.parametrize("number,ok", [("101002724", True), ("609823", True), ("101000790", True), ("1010027245", False),
                                       ("101002724 RIDING", False), ("ERC-2021-ADG", False), ("12345", False), ("1012345", False),
                                       ("GA 101002724", False), ("H2020-101002724", False)])
def test_eu_grant_number_shape(number, ok):
    assert bool(re.match(nih_fold.EU_NUMBER, number)) is ok


def test_flag_reads_strict():
    """A stand-in context that answers any attribute (like RecordingC) never reads as EU pairs listed."""
    c = RecordingC()
    assert nih_fold._eu(c) is False and nih_fold.on(c) is False and nih_fold.with_class(c, ("X",)) == ("X",)


# ---------- sub-class B (oxjob #690 step 5): the EU's own grant-DOI record retires into its CORDIS record ----------
@needs_head
@pytest.mark.parametrize("listed", [(EU,), (NIH, EU)])
def test_without_b_rows_build_renders_as_head(listed):
    """EU rows listed, none with a doi (production since night 2026-11-24): every statement is the one main renders (the probes
    aside), so the B code changes nothing until a B batch loads."""
    old = _render(HEAD / "build_awards.py", _head_fold(), listed)
    new = _render(ROOT / "lib" / "build_awards.py", nih_fold, listed)
    own = lambda c: [s for s in _four_tier_free(c) if s[0] not in NIH_AMOUNT_STATEMENTS | DATE_GUARD_STATEMENTS]
    assert own(new) == own(old)
    assert new.checks.keys() == old.checks.keys() and new.eu_b is False


def test_b_probe_runs_only_with_eu_rows():
    probe = lambda c: [s for n, s in c.statements if n == "sql" and "'$.doi') IS NOT NULL" in s]
    assert probe(_render(ROOT / "lib" / "build_awards.py", nih_fold, ())) == []
    assert probe(_render(ROOT / "lib" / "build_awards.py", nih_fold, (NIH,))) == []
    assert len(probe(eu_build())) == 1 and f"evidence_class='{EU}' LIMIT 1" in probe(eu_build())[0]


def test_without_b_rows_no_b_statement():
    c = eu_build()
    q = dict(c.statements)
    assert "doi" not in q["eu_pairs"] and "doi" not in q["eu_listed"] and "target_doi" not in q["eu_pair_numbers"]
    assert "$.doi" not in q["MIGRATION_LOSER_NO_NATIVE"] and "eu_rule_into_survivor" not in q["RETIREMENT_SAME_FUNDER"]
    assert "eu_rule_into_survivor" not in q and "eu_b_pending_pairs" not in c.counts
    for reason in nih_fold.EU_B_HELD_REASONS:
        assert reason not in q["eu_held"]


def b_build(listed=(EU, B)):
    return _render(ROOT / "lib" / "build_awards.py", nih_fold, listed)


@pytest.mark.parametrize("reason", nih_fold.EU_HELD_REASONS + nih_fold.EU_B_HELD_REASONS)
def test_b_rows_every_held_reason_is_in_the_proof(reason):
    assert f"'{reason}'" in dict(b_build().statements)["eu_held"]


def test_b_rows_proof():
    c = b_build()
    q = dict(c.statements)
    assert c.eu_b is True
    assert "get_json_object(m.evidence_json,'$.doi') doi" in q["eu_pairs"]
    assert "get_json_object(evidence_json,'$.doi') doi FROM migration_manifest_v" in q["eu_listed"]
    # a B row's doi is 10.3030/<its number> and its registry is cordis; an A row (no doi) as before
    assert "(doi IS NULL OR (doi=concat('10.3030/',number) AND registry='cordis'))" in q["EU_LISTED_SHAPE"]
    held = q["eu_held"]
    # the paper-side-copy reasons are for A rows only
    assert "eu_direct d JOIN dev.lab.ngr_s_eu_pairs p ON p.loser_id=d.stable_id WHERE p.doi IS NULL" in held
    assert "k.key_type='NATIVE' WHERE p.doi IS NULL" in held
    # B: one NATIVE key, the crossref_grant DOI that is the listed doi (read from award_keys, any origin)
    assert "LEFT JOIN dev.lab.ngr_award_keys k ON k.stable_id=p.loser_id AND k.key_type='NATIVE'" in held
    assert "count(k.stable_id)=1 AND min(k.namespace)='crossref_grant'" in held and "=p.doi,false)" in held
    # B: nothing better than a grant DOI binds to the loser, so the registry's rank-1 row wins after the redirect
    assert "LEFT JOIN dev.lab.ngr_s_bindings b ON b.stable_id=p.loser_id" in held and "min(b.source_rank)>=2" in held
    # B: the target serves the listed doi
    assert "ta.doi target_doi" in q["eu_pair_numbers"] and "target_doi" in held
    assert "eu_b_pending_pairs" in c.counts


def test_b_rows_may_have_their_native_key():
    q = dict(b_build().statements)
    gate = q["MIGRATION_LOSER_NO_NATIVE"]
    assert ("AND NOT (m.evidence_class='EU_GRANT_FAMILY_FOLD' AND get_json_object(m.evidence_json,'$.doi') IS NOT NULL)") in gate
    # the other classes' gates are unchanged: DOI proof only for the DOI classes, the EU class still never public-shell exempt
    assert EU not in q["MIGRATION_DOI_PROOF"] and EU not in q["MIGRATION_TARGET_DIRECT"] and EU not in q["RETIREMENT_DIRECT_TARGET"]


def test_b_rows_new_shell_into_survivor_is_exempt_narrowly():
    c = b_build()
    q = dict(c.statements)
    gate = q["RETIREMENT_SAME_FUNDER"]
    assert "NOT EXISTS (SELECT 1 FROM dev.lab.ngr_s_eu_rule_into_survivor e WHERE e.old_id=t.old_id AND e.canonical_id=t.canonical_id)" in gate
    shells = q["eu_rule_shell_obs"]
    # only into an EU survivor, only a shell observation under another EU programme funder than the survivor's
    assert f"nih_survivors s ON s.stable_id=t.canonical_id AND s.family_class='{EU}'" in shells
    # only into a listed B row's target (its grant-DOI record's observation is the one that binds there), never any EU survivor
    assert (f"SELECT DISTINCT target_id FROM migration_manifest_v WHERE evidence_class='{EU}' AND "
            "get_json_object(evidence_json,'$.doi') IS NOT NULL) bt ON bt.target_id=t.canonical_id") in shells
    assert "NOT so.is_direct AND so.nk IS NOT NULL AND so.funder_id IN (4320314745," in shells
    assert "so.funder_id<>s.pinned_funder_id" in shells and "nih_obs_funder" in shells   # each observation's own funder
    # matched by a direct observation the survivor holds under that funder with that number; the join reads one earlier relation
    into = q["eu_rule_into_survivor"]
    assert "d.stable_id=x.canonical_id AND d.funder_id=x.funder_id AND d.nk=x.nk WHERE d.is_direct" in into
    assert "eu_rule_into_survivor" in c.counts


def test_b_rows_with_nih_rows():
    c = b_build((NIH, EU, B))
    q = dict(c.statements)
    assert f"nm.evidence_class IN ('{NIH}','{EU}')" in q["RETIREMENT_SAME_FUNDER"] and "eu_rule_into_survivor" in q["RETIREMENT_SAME_FUNDER"]
    assert "awards_candidate_before_nih" in q and "B_LOSER_KEYS" in q["eu_held"]
    # NIH proof untouched by B
    assert "doi" not in q["nih_pairs"]


def test_b_flag_reads_strict():
    c = RecordingC()
    assert nih_fold._eu_b(c) is False and nih_fold.native_loser_exempt(c, "m") == ""
    c.eu, c.eu_b = False, True
    assert nih_fold._eu_b(c) is False   # never without EU rows
