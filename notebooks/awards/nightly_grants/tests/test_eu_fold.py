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
from test_nih_fold import RecordingC, Frame, load

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


class C(RecordingC):
    """RecordingC whose manifest probe answers per class."""
    def __init__(self, listed=(), **kw):
        super().__init__(**kw)
        self.listed = set(listed)

    def sql(self, statement, params=None):
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


def _probe_free(c):
    return [s for s in c.statements if "' LIMIT 1" not in s[1] or "evidence_class=" not in s[1]]


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
    assert _probe_free(new) == _probe_free(old)
    assert new.checks.keys() == old.checks.keys()


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
    for gate in ("EU_LISTED_SHAPE", "EU_ONE_NUMBER_PER_TARGET", "EU_HELD_FUSE", "EU_SURVIVOR_FUNDER_PUBLISHED", "FAMILY_SURVIVOR_FUNDER_AS_LISTED"):
        assert gate in c.checks, gate
    # no NIH projection on an EU-only night
    assert "awards_candidate_before_nih" not in q and "nih_choice" not in q
    for n in ("eu_survivors_published", "eu_survivors_not_registry_row", "eu_family_drift", "eu_pending_pairs"):
        assert n in c.counts, n


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
    assert "retirements_rule t ON t.old_id=p.loser_id" in held and "retirements_rule t ON t.canonical_id=p.loser_id" in held
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
    assert "EU_NO_LOSER_LINKS" in c.checks and "EU_MAPPED_LINKS_PRESERVED" in c.checks
    assert "work_survivors" in q["eu_links_before"]
    both = C(); both.nih = True; both.eu = True
    nih_fold.link_conservation(both)
    assert {"NIH_NO_LOSER_LINKS", "EU_NO_LOSER_LINKS"} <= both.checks.keys()


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
