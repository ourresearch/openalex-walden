"""Offline tests for NIH_PROJECT_YEAR_FOLD: byte-identical build without NIH pairs; hooks, proof, pin and projection with them."""
import re
import importlib.util
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import nih_fold

def _base_lib():
    """The unpatched build_awards.py to compare against: $NIH_BASE_LIB if set, else the committed file at git HEAD (in a walden
    checkout with this change applied but not yet committed, HEAD holds main's version)."""
    import os, subprocess, tempfile
    if os.environ.get("NIH_BASE_LIB"):
        return pathlib.Path(os.environ["NIH_BASE_LIB"])
    try:
        rel = (ROOT / "lib" / "build_awards.py").resolve().relative_to(
            pathlib.Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=ROOT, text=True).strip()))
        text = subprocess.check_output(["git", "show", f"HEAD:{rel}"], cwd=ROOT, text=True)
    except Exception:
        return pathlib.Path("/nonexistent")
    d = pathlib.Path(tempfile.mkdtemp(prefix="nih_base_")); (d / "build_awards.py").write_text(text)
    return d


BASE = _base_lib()   # 6f's build_awards.py before this change


class Frame:
    def __init__(self, rows=(), cols=()):
        self.rows, self.columns = list(rows), list(cols)

    def collect(self):
        return self.rows


class RecordingC:
    """Runs a build module offline: records every statement; checks pass; the NIH probe answers per `nih`."""
    def __init__(self, extra=(), nih=False, config=None):
        self.p, self.r, self.extra, self.nih_listed = "dev.lab.ngr_", "dev.lab.ngr_s_", set(extra), nih
        self.config = dict(config or {}); self.params = {"rid": "run", "ts": "2026-10-01 00:00:00"}
        self.statements, self.checks, self.counts, self.previous_outputs, self.versions = [], {}, {}, {}, {}
        self.outputs = {"awards": "dev.lab.out_awards", "aliases": "dev.lab.out_aliases", "work_awards": "dev.lab.out_work_awards", "api": "dev.lab.out_api"}
        self.m = {"inputs": {}}
        self.nih = False                  # set by nih_fold.pin_ownership in plan(), as in the real runtime
        schema = types.SimpleNamespace(simpleString=lambda: "struct<id:bigint>", json=lambda: "{}", fields=[])
        self.spark = types.SimpleNamespace(table=lambda name: types.SimpleNamespace(schema=schema, columns=[]),
                                           sql=lambda s, *a: Frame())

    def __getattr__(self, name):          # any other runtime helper the build calls: record and return a relation name
        if name.startswith("__"):
            raise AttributeError(name)
        def helper(*a, **kw):
            self.statements.append(("call:" + name, repr(a)[:200])); return Frame()
        return helper

    def has_extra(self, name): return name in self.extra
    def require(self, cond, name):
        if not cond: raise RuntimeError(name)
    def guard(self): pass

    def sql(self, statement, params=None):
        self.statements.append(("sql", " ".join(statement.split())))
        if "evidence_class='NIH_PROJECT_YEAR_FOLD' LIMIT 1" in statement:
            return Frame([types.SimpleNamespace(x=1)] if self.nih_listed else [])
        if "WITH marks AS" in statement:
            return Frame([{"pairs": 0, "moved": 0, "deduplicated": 0}])
        if "count(*) n" in statement:
            return Frame([types.SimpleNamespace(n=0)])
        return Frame()

    def write(self, statement, params=None):
        self.statements.append(("write", " ".join(statement.split())))
        return Frame()

    def artifact(self, name, query, params=None):
        self.statements.append((name, " ".join(query.split()))); return self.r + name

    def zero(self, name, query):
        self.checks[name] = True; self.statements.append((name, " ".join(query.split())))

    def count(self, name, relation):
        self.counts[name] = 0; return 0

    def graph(self, source, label, **kw):
        self.statements.append(("graph:" + label, source)); return self.r + label


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path); mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod; spec.loader.exec_module(mod); return mod


def render(module_path, **kw):
    mod = load("ba_" + module_path.parent.parent.name, module_path)
    c = RecordingC(**kw)
    mod.prepare(c); mod.allocate(c); mod.plan(c); mod.project(c)
    return c


@pytest.mark.skipif(not (BASE / "build_awards.py").exists(), reason="unpatched build_awards.py not found (set NIH_BASE_LIB or run inside a git checkout)")
@pytest.mark.parametrize("extra", [(), ("migration_manifest",)])
def test_without_nih_pairs_build_is_byte_identical(extra):
    old = render(BASE / "build_awards.py", extra=extra)
    new = render(ROOT / "lib" / "build_awards.py", extra=extra)
    # the probe is the only statement the fold adds without NIH pairs; drop it on both sides (once the fold is on main, HEAD has it too)
    def strip(c): return [s for s in c.statements if "NIH_PROJECT_YEAR_FOLD' LIMIT 1" not in s[1]]
    assert strip(new) == strip(old)


def test_with_nih_pairs_every_hook_is_on():
    c = render(ROOT / "lib" / "build_awards.py", extra=("migration_manifest",), nih=True)
    names = [n for n, _ in c.statements]
    q = dict(c.statements)
    # pin comes right after ownership_plan and before the ownership gates
    assert names.index("ownership_plan_unpinned") < names.index("ENTITY_FUNDER_AMBIGUOUS") < names.index("DIRECT_CORRECTION_UNPROVEN")
    assert "nih_s.stable_id=b.stable_id" in q["ENTITY_FUNDER_AMBIGUOUS"] and "nih_s.stable_id=o.stable_id" in q["DIRECT_CORRECTION_UNPROVEN"]
    # the survivor's funder is the one the list gives (the institute), else its current one; the plan must equal it (Rohan 10-07, option B)
    assert "coalesce(CAST(get_json_object(m.evidence_json,'$.project_funder_id') AS BIGINT),e.funder_id) pinned_funder_id" in q["nih_survivors"]
    assert "NOT (o.funder_id <=> s.pinned_funder_id)" in q["FAMILY_SURVIVOR_FUNDER_AS_LISTED"]
    assert "pinned_funder_id NOT IN" in q["NIH_SURVIVOR_FUNDER_IN_FAMILY"] and "HAVING count(*)<>1" in q["NIH_SURVIVOR_ONE_FUNDER"]
    assert names.index("nih_survivors") < names.index("ownership_plan_unpinned") < names.index("FAMILY_SURVIVOR_FUNDER_AS_LISTED")
    # shell matching sees each observation's own funder, not the survivor's pinned one (fold rehearsal 10-09 N1)
    for side in ("WHERE is_direct)", "WHERE NOT is_direct)"):
        assert "coalesce(nof.funder_id,o.funder_id) funder_id FROM" in q["shell_targets"].split(side)[0].rsplit("AS (", 1)[1], side
    assert names.index("nih_obs_funder") < names.index("shell_targets")
    # a rule retirement touching a pinned survivor is checked on pre-pin funders (fold rehearsal 10-09 N2)
    assert "ua.funder_id=ub.funder_id" in q["nih_unpinned_same_funder"] and "ownership_plan_unpinned ua" in q["nih_unpinned_same_funder"]
    assert "nih_unpinned_same_funder x" in q["RETIREMENT_SAME_FUNDER"]
    assert names.index("retirements") < names.index("nih_unpinned_same_funder") < names.index("RETIREMENT_SAME_FUNDER")
    # number normalization uses the record's own funder for survivor-bound records
    assert "coalesce(nof.funder_id,e.funder_id)" in q["NORMALIZATION_COVERAGE"]
    assert "lookup_funder_id" in q["bound_observations"] and "n.funder_id <=> b.lookup_funder_id" in q["resolved_observations"]
    # class allowed, public losers allowed, shell target allowed, cross-funder only for the class
    for gate in ("MIGRATION_EVIDENCE_CLASS", "MIGRATION_LOSER_NEVER_PUBLIC", "MIGRATION_TARGET_DIRECT", "RETIREMENT_DIRECT_TARGET"):
        assert "NIH_PROJECT_YEAR_FOLD" in q[gate], gate
    assert "m.evidence_class<>'NIH_PROJECT_YEAR_FOLD'" in q["MIGRATION_SAME_FUNDER"]
    assert "nm.evidence_class='NIH_PROJECT_YEAR_FOLD'" in q["RETIREMENT_SAME_FUNDER"]
    # held-pair filter right after 6f's shell filter; list-level inconsistencies stop, pair failures are held (fuse on the count)
    assert names.index("migration_proven") < names.index("nih_pairs") < names.index("nih_held") < names.index("migration_proven_nih")
    held = q["nih_held"]
    for reason in ("'NUMBER'", "'TARGET_NOT_PROJECT_RECORD'", "'NOT_IN_FAMILY'", "'TARGET_RETIRING'", "'NOT_BOUND'", "'NOT_ACTIVE'"):
        assert reason in held, reason
    for gate in ("NIH_ONE_CHOICE_PER_TARGET", "NIH_PRIMARY_IS_PROJECT_FORM", "NIH_HELD_FUSE", "FAMILY_SURVIVOR_FUNDER_AS_LISTED", "NIH_SURVIVOR_ONE_FUNDER",
                 "NIH_SURVIVOR_BINDINGS_IN_FAMILY", "NIH_PROJECTION_SAME_ROWS", "NIH_PRIMARY_PUBLISHED", "NIH_SURVIVOR_FUNDER_PUBLISHED"):
        assert gate in c.checks, gate
    # the later retirement gates read the proven set (held NIH pairs never reach them)
    assert "migration_proven_nih" in q["MIGRATION_TARGET_ACTIVE"]
    assert names.index("awards_candidate_before_nih") < names.index("aliases_candidate")


def projection(**settings):
    c = RecordingC(extra=("migration_manifest",), nih=True, config={"nih_fold": settings} if settings else None)
    c.nih = True
    nih_fold.project(c)
    return dict(c.statements)["awards_candidate"]


def test_amount_switch():
    assert "THEN CAST(NULL AS DOUBLE) ELSE a.amount END amount" in projection()          # default: null until Kyle rules
    assert "sp.amount_sum" in projection(amount="sum")
    assert "THEN a.amount ELSE a.amount END amount" in projection(amount="elected")
    with pytest.raises(RuntimeError, match="NIH_FOLD_SETTINGS"):
        projection(amount="max")


def test_title_switch_and_number_always_primary():
    q = projection()
    assert "THEN t.display_name ELSE a.display_name END display_name" in q
    assert "THEN n.primary_award_id ELSE a.funder_award_id END funder_award_id" in q
    q = projection(title="elected")
    assert "a.display_name display_name" in q and "THEN n.primary_award_id" in q


def py_core(s):
    """Python mirror of nih_fold.core() (same steps as the SQL) for the parser vectors."""
    import re
    n = re.sub(r"\s", "", re.sub("[\u2010-\u2015]", "-", s.strip())).upper()
    m = re.match(r"^[1-9]?([A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{6})($|-)", n)
    if m:
        return m.group(1)
    a = re.sub("[^A-Z0-9]", "", n)
    if re.match(r"^[1-9]?[A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{1,6}$", a):
        return re.match(r"^[1-9]?([A-Z][A-Z0-9]{2}[A-Z]{2})[0-9]+$", a).group(1) + re.search("([0-9]+)$", a).group(1).zfill(6)
    return ""


def py_bare(s):
    import re
    n = re.sub(r"\s", "", re.sub("[\u2010-\u2015]", "-", s.strip())).upper()
    return not re.search(r"[0-9]{6}-[0-9]{2}", n) and not re.match("^[1-9]", re.sub("[^A-Z0-9]", "", n))


@pytest.mark.parametrize("number,core,bare", [
    ("5R01HL123456-03", "R01HL123456", False), ("R01HL123456", "R01HL123456", True), ("1ZIAMH002857-17", "ZIAMH002857", False),
    # the four shapes dev night 1 (09-30) rejected before the parse matched the list builder
    ("u01 dk63790", "U01DK063790", True), ("4uh3da050252\u201001", "UH3DA050252", False),
    ("u54ca 163313", "U54CA163313", True), ("t-32-am-07435", "T32AM007435", True),
    ("R01-HL-12345X", "", True)])
def test_project_number_parse(number, core, bare):
    assert py_core(number) == core and py_bare(number) == bare


def test_sql_parse_has_the_same_steps():
    sql = nih_fold.core("x")
    for step in ("x{2010}-\\\\x{2015}", "'\\\\s'", "[0-9]{6})($|-)", "[0-9]{1,6}$", "lpad("):
        assert step in sql, step
    assert "[0-9]{6}-[0-9]{2}" in nih_fold.bare("x")


@pytest.mark.parametrize("text,is_multi", [("ZIAMH002786–15, ZIAMH002778–17", True), ("ZIACL090034-09, ZIACL090035-08", True),
                                           ("zia/hl006223", False), ("5R01HL123456-03", False), ("R01 NR012726", False)])
def test_multi_number_text_is_held(text, is_multi):
    import re
    rx = re.search(r"'(\(\[A-Z\].*?\))'", nih_fold.multi("x")).group(1)
    assert (len(re.findall(rx, re.sub("[^A-Za-z0-9]", "", text).upper())) > 1) == is_multi


def test_multi_number_is_a_held_reason():
    c = render(ROOT / "lib" / "build_awards.py", extra=("migration_manifest",), nih=True)
    assert "'MULTI_NUMBER'" in dict(c.statements)["nih_held"]
    assert "'SUBPROJECT'" in dict(c.statements)["nih_held"]           # center sub-projects (number:subproject_id) never fold as years


def test_link_conservation_only_with_nih_and_strict():
    import nih_fold as nf
    off = RecordingC(); nf.link_conservation(off)
    assert off.statements == []                                   # no NIH rows: nothing runs
    on = RecordingC(extra=("migration_manifest",), nih=True); on.nih = True
    nf.link_conservation(on)
    assert "NIH_NO_LOSER_LINKS" in on.checks and "NIH_MAPPED_LINKS_PRESERVED" in on.checks
    q = dict(on.statements)
    assert "work_survivors" in q["nih_links_before"]                # papers merged tonight count as their surviving paper
    assert "evidence_class='NIH_PROJECT_YEAR_FOLD'" in q["nih_applied"]


def test_work_awards_calls_link_conservation_after_uk():
    text = (ROOT / "lib" / "work_awards.py").read_text()
    assert text.index("uk_lineage.link_conservation(c)") < text.index("nih_fold.link_conservation(c)")


def test_project_number_parse_is_pinned():
    """6f's class A load (twm_classA_v3, 10-01) embeds nih_fold.core() verbatim to keep NIH project numbers out of class A.
    If this hash changes, regenerate that load with the new parse before shipping either change."""
    import hashlib
    h = hashlib.sha256((nih_fold.core("x") + "|" + nih_fold.multi("x")).encode()).hexdigest()
    assert h == "ffdea852ece7c88bff0d61298f955ce96ddcf936dadabac04180e3dee5ff66c9", "nih_fold.core/multi changed: tell the grants-job session to regenerate the class A load"


@pytest.mark.parametrize("number,is_year_record", [("5R01CA123456-05", True), ("1R01CA123456-01A1", True), ("R01CA123456-05", True),
                                                   ("5R01CA123456-05S1", False), ("3R01CA123456-05S2A1", False), ("R01CA123456", False),
                                                   ("5P01CA012345-27:7001", False)])
def test_year_record_target(number, is_year_record):
    """10-07: the fold's survivor is the newest non-supplement year record; supplements never survive (bare is checked separately)."""
    pat = r"^[1-9]?[A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{6}-[0-9]{2}(A[0-9]+)*$"
    assert pat in nih_fold.year_record("x")
    assert bool(re.match(pat, re.sub(r"[^A-Z0-9-]", "", number.upper()))) == is_year_record


def test_matching_reads_prepin_funders():
    """The survivor pin is display-only (fold rehearsal 10-09): paper linking and shell matching read pre-pin funders."""
    text = (ROOT / "lib" / "work_awards.py").read_text()
    assert "{r}resolved_observations" not in text and text.count("nih_fold.unpinned(c,'resolved_observations')") == 3
    assert "nih_fold.owner_funder(c,'d')" in text and "nih_fold.unpinned_owner_join(c,'t')" in text
    off, on = types.SimpleNamespace(r="x.", nih=False), types.SimpleNamespace(r="x.", nih=True)
    assert nih_fold.unpinned(off, "resolved_observations") == "x.resolved_observations"
    assert "x.nih_obs_funder nof" in nih_fold.unpinned(on, "resolved_observations")
    assert nih_fold.owner_funder(off, "d") == "d.funder_id" and nih_fold.unpinned_owner_join(off, "t") == ""
    assert nih_fold.owner_funder(on, "d") == "coalesce(nih_u.funder_id,d.funder_id)"
    assert nih_fold.plan_for_matching(off) == "x.ownership_plan" and nih_fold.plan_for_matching(on) == "x.ownership_plan_unpinned"
