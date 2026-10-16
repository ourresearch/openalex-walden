"""Offline tests for pinned ADD / SUPPRESS link lists (list_legs.py, ce1014): config validation, pinned reads, size and
resolution fuses (fail closed), the number leg, the suppress arithmetic, conservation hooks and job wiring (no warehouse)."""
import json
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import list_legs
import work_awards
from nightly_runtime import Nightly

KAKEN = dict(name="kaken_direct_v1", relation="openalex.awards.list_kaken_direct_v1", version=0, expected_rows=363,
             source="KAKEN", filter={"source": "KAKEN"})
NIH = dict(name="nih_key_v1", kind="number", relation="openalex.awards.list_nih_key", version=2, expected_rows=10000, tolerance=0,
           columns={"funder_id": "target_funder_id", "number": "target_award_id"}, max_unresolved=50)
CLEAN = dict(name="cleanup_v1", relation="openalex.awards.list_cleanup", version=1, expected_rows=100, tolerance=5,
             columns={"reason": "decision"}, max_unmatched=3)


class Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def collect(self):
        return self.rows


class FakeC:
    """Answers count() from `counts` (by artifact name) and grouped status queries from `groups`."""
    def __init__(self, lists=None, counts=None, groups=None, distinct_removed=0, distinct_added=0, prev_rows=0, outputs=None):
        self.r = "dev.lab.ngr_s_"
        self.config = {"list_legs": lists or {}}
        self.counts, self.versions, self.checks = {}, {}, {}
        self.artifacts, self.views, self.zeros, self.statements = {}, {}, {}, []
        self.fake_counts, self.groups, self.distinct_removed = counts or {}, groups or [], distinct_removed
        self.distinct_added, self.prev_rows, self.prev_added = distinct_added, prev_rows, 0
        if outputs is not None:
            self.outputs = outputs

    def artifact(self, name, query, params=None):
        self.artifacts[name] = query
        return self.r + name

    def view(self, name, relation):
        self.views[name] = relation

    def zero(self, name, query):
        self.zeros[name] = query
        self.checks[name] = True

    def count(self, name, relation):
        n = self.fake_counts.get(relation.split(".")[-1].replace("ngr_s_", ""), 0)
        self.counts[name] = n
        return n

    def require(self, condition, name):
        if not condition:
            raise RuntimeError(name)

    def sql(self, statement, params=None):
        self.statements.append(statement)
        s = " ".join(statement.split())
        if "GROUP BY 1,2" in s:
            return Rows([types.SimpleNamespace(list_name=a, status=b, n=n) for a, b, n in self.groups])
        if "resolution IN ('UNRESOLVED','NO_DOCUMENT') GROUP BY 1" in s:
            return Rows([types.SimpleNamespace(list_name=a, n=n) for a, b, n in self.groups if b == "UNRESOLVED"])
        if "FROM prod.awards.audit" in s:
            return Rows([types.SimpleNamespace(n=self.prev_added if "status='ADDED'" in s else 0)])
        if "count(DISTINCT served_work_id,award_stable_id)" in s and "status='REMOVED'" in s:
            return Rows([types.SimpleNamespace(n=self.distinct_removed)])
        if "count(DISTINCT served_work_id,award_stable_id)" in s and "status='ADDED'" in s:
            return Rows([types.SimpleNamespace(n=self.distinct_added)])
        if s.startswith("SELECT count(*) n FROM prod."):
            return Rows([types.SimpleNamespace(n=self.prev_rows)])
        return Rows()


# ---------- config ----------
def test_empty_or_missing_config_is_no_lists():
    assert list_legs.specs({}) == [] and list_legs.specs({"list_legs": {"add": [], "suppress": []}}) == []


@pytest.mark.parametrize("bad", [
    dict(KAKEN, version=None), dict(KAKEN, version=True), dict(KAKEN, version=-1), {k: v for k, v in KAKEN.items() if k != "version"},
    {k: v for k, v in KAKEN.items() if k != "expected_rows"}, dict(KAKEN, name="Bad-Name"), dict(KAKEN, relation="x.y"),
    dict(KAKEN, relation="a.b.c; DROP TABLE x"), dict(KAKEN, kind="doi"), dict(KAKEN, filter={"source": ["KAKEN"]}),
    dict(KAKEN, filter={"1=1 OR source": "x"}), dict(KAKEN, columns={"award_id": "a b"}), dict(KAKEN, columns={"reason": "r"}),
    dict(KAKEN, unexpected=1), dict(KAKEN, priority=0), dict(KAKEN, tolerance=-1)])
def test_bad_add_specs_are_refused(bad):
    with pytest.raises(ValueError, match="LIST_CONFIG|Unsafe"):
        list_legs.specs({"list_legs": {"add": [bad]}})


def test_suppress_takes_award_ids_only_and_names_are_unique_across_actions():
    with pytest.raises(ValueError, match="kind"):
        list_legs.specs({"list_legs": {"suppress": [dict(CLEAN, kind="number")]}})
    with pytest.raises(ValueError, match="duplicate"):
        list_legs.specs({"list_legs": {"add": [KAKEN], "suppress": [dict(CLEAN, name=KAKEN["name"])]}})
    with pytest.raises(ValueError, match="only 'add' and 'suppress'"):
        list_legs.specs({"list_legs": {"remove": []}})


def test_defaults():
    s = list_legs.specs({"list_legs": {"add": [{k: v for k, v in KAKEN.items() if k != "source"}]}})[0]
    assert (s["kind"], s["source"], s["tolerance"], s["priority"], s["max_unresolved"]) == ("award_id", "kaken_direct_v1", 0, 3, 0)


# ---------- pinned read ----------
def test_read_is_pinned_filtered_projected_and_ignores_extra_columns():
    s = list_legs.specs({"list_legs": {"add": [KAKEN]}})[0]
    sql = list_legs.select_sql(s)
    assert "FROM openalex.awards.list_kaken_direct_v1 VERSION AS OF 0 WHERE source = 'KAKEN'" in sql
    assert "'KAKEN' source" in sql and "work_doi" not in sql and "route" not in sql and "SELECT *" not in sql
    assert "regexp_replace(CAST(award_id AS STRING),'^https://openalex.org/G','')" in sql


def test_number_and_suppress_column_mappings():
    n, c = list_legs.specs({"list_legs": {"add": [NIH], "suppress": [CLEAN]}})
    assert "CAST(target_funder_id AS BIGINT) funder_id" in list_legs.select_sql(n)
    assert "CAST(target_award_id AS STRING) number" in list_legs.select_sql(n)
    assert "VERSION AS OF 2 WHERE true" in list_legs.select_sql(n)
    assert "CAST(decision AS STRING) reason" in list_legs.select_sql(c)


# ---------- size fuse ----------
@pytest.mark.parametrize("rows,ok", [(100, True), (95, True), (105, True), (94, False), (106, False), (0, False)])
def test_size_fuse_is_expected_plus_minus_tolerance(rows, ok):
    s = list_legs.specs({"list_legs": {"suppress": [CLEAN]}})[0]
    if ok:
        list_legs.fuse(s, rows)
    else:
        with pytest.raises(RuntimeError, match="LIST_SIZE_FUSE: cleanup_v1"):
            list_legs.fuse(s, rows)


def test_bind_fails_closed_on_a_size_miss_and_records_versions():
    c = FakeC({"add": [KAKEN]}, counts={"list_in_kaken_direct_v1": 362})
    with pytest.raises(RuntimeError, match="LIST_SIZE_FUSE"):
        list_legs.bind(c)
    assert c.versions["list_kaken_direct_v1"]["version"] == 0 and c.versions["list_kaken_direct_v1"]["rows"] == 362


def test_bind_builds_the_three_views_and_null_key_checks():
    c = FakeC({"add": [KAKEN, NIH], "suppress": [CLEAN]},
              counts={"list_in_kaken_direct_v1": 363, "list_in_nih_key_v1": 10000, "list_in_cleanup_v1": 103})
    list_legs.bind(c)
    assert set(c.views) == {"list_add_award", list_legs.NUMBER_LEG, "list_suppress"}
    assert c.artifacts["list_add_award"] == "SELECT * FROM dev.lab.ngr_s_list_in_kaken_direct_v1"
    assert c.artifacts[list_legs.NUMBER_LEG] == "SELECT * FROM dev.lab.ngr_s_list_in_nih_key_v1"
    assert "work_id IS NULL OR award_id IS NULL" in c.zeros["LIST_NULL_KEYS_CLEANUP_V1"]
    assert "reason" not in c.zeros["LIST_NULL_KEYS_CLEANUP_V1"]


def test_no_lists_binds_typed_empty_views_so_the_night_is_unchanged():
    c = FakeC()
    list_legs.bind(c)
    assert all("WHERE false" in c.artifacts[v] for v in ("list_add_award", list_legs.NUMBER_LEG, "list_suppress"))
    assert not c.versions and not c.zeros


def test_rollback_is_an_empty_pinned_version():
    c = FakeC({"add": [dict(KAKEN, version=0, expected_rows=0, relation="openalex.awards.list_empty")]},
              counts={"list_in_kaken_direct_v1": 0})
    list_legs.bind(c)
    assert c.versions["list_kaken_direct_v1"]["rows"] == 0


# ---------- resolution fuses ----------
def test_add_resolves_through_the_registry_and_fuses_unresolved():
    c = FakeC({"add": [dict(KAKEN, max_unresolved=1)]}, groups=[("kaken_direct_v1", "UNRESOLVED", 2)])
    c.list_specs = {s["name"]: s for s in list_legs.specs(c.config)}
    with pytest.raises(RuntimeError, match="LIST_ADD_UNRESOLVED_FUSE: kaken_direct_v1 unresolved=2 max=1"):
        list_legs.resolve_adds(c)
    q = c.artifacts["list_add_resolved"]
    assert "x.owner_id=l.award_id AND x.status='ACTIVE'" in q and "'REDIRECTED'" in q
    c = FakeC({"add": [dict(KAKEN, max_unresolved=2)]}, groups=[("kaken_direct_v1", "UNRESOLVED", 2)])
    c.list_specs = {s["name"]: s for s in list_legs.specs(c.config)}
    list_legs.resolve_adds(c)
    assert c.counts["list_kaken_direct_v1_unresolved"] == 2


def apply_c(groups, before, after, removed, lists=None):
    c = FakeC(lists or {"suppress": [CLEAN]}, counts={"work_awards_listed": before, "work_awards_candidate": after},
              groups=groups, distinct_removed=removed)
    c.list_specs = {s["name"]: s for s in list_legs.specs(c.config)}
    return c


def test_suppress_arithmetic_and_audit():
    c = apply_c([("cleanup_v1", "REMOVED", 100), ("cleanup_v1", "NOT_SERVED", 3)], 1000, 900, 100)
    list_legs.apply(c, "dev.lab.ngr_s_work_survivors", True, "dev.lab.ngr_s_work_awards_listed")
    assert c.counts["list_cleanup_v1_removed"] == 100 and c.counts["list_suppressed_pairs"] == 100
    assert "LEFT ANTI JOIN dev.lab.ngr_s_list_suppress_keyed" in c.artifacts["work_awards_candidate"]
    assert {"LIST_ADD_SUPPRESS_CONFLICT", "LIST_SUPPRESSED_GONE"} <= set(c.zeros)
    with pytest.raises(RuntimeError, match="LIST_SUPPRESS_ARITHMETIC"):
        list_legs.apply(apply_c([("cleanup_v1", "REMOVED", 100)], 1000, 901, 100), "s", True, "u")


def test_suppress_unmatched_fuse_fails_closed():
    with pytest.raises(RuntimeError, match="LIST_SUPPRESS_UNMATCHED_FUSE: cleanup_v1 not_served=4 max=3"):
        list_legs.apply(apply_c([("cleanup_v1", "NOT_SERVED", 4)], 10, 10, 0), "s", True, "u")


def test_number_list_unmatched_fuse():
    with pytest.raises(RuntimeError, match="LIST_ADD_UNRESOLVED_FUSE: nih_key_v1"):
        list_legs.apply(apply_c([("nih_key_v1", "UNMATCHED", 51)], 10, 10, 0, {"add": [NIH]}), "s", True, "u")


def test_suppressed_link_keys_follow_merged_works_only_when_moves_are_on():
    c = apply_c([], 0, 0, 0)
    list_legs.apply(c, "S", False, "U")
    assert "LEFT JOIN S m ON false AND m.loser_work_id=s.work_id" in c.artifacts["list_suppress_keyed"]


# ---------- wiring ----------
def test_number_leg_is_a_registry_mode_leg_reading_funder_id():
    leg = [l for l in work_awards.LEGS if l[0] == list_legs.NUMBER_LEG]
    assert leg == [(list_legs.NUMBER_LEG, "work_id", "number", False, 4, True, True)]
    sql = work_awards.leg_sql()
    assert "x.funder_id source_funder_id,lower(x.number) award_key,4 priority,true sharp_eligible,true is_direct FROM list_add_number_v x" in sql


def test_work_awards_unions_adds_before_moves_and_suppresses_after_the_dedupe():
    src = (ROOT / "lib" / "work_awards.py").read_text()
    i_add, i_links, i_moved = src.index("list_legs.resolve_adds(c)"), src.index("c.artifact('work_links',"), src.index("c.artifact('work_links_moved'")
    i_listed, i_apply, i_cons = src.index("c.artifact('work_awards_listed'"), src.index("list_legs.apply("), src.index("nih_fold.link_conservation(c)")
    assert i_add < i_links < i_moved < i_listed < i_apply < i_cons
    assert "c.artifact('work_awards_candidate'" not in src


def test_conservation_gates_know_list_links():
    assert "list_legs.removed_pairs_sql(c)" in (ROOT / "lib" / "nih_fold.py").read_text()
    uk = (ROOT / "lib" / "uk_lineage.py").read_text()
    assert "list_legs.added_pairs_sql(c)" in uk and "list_legs.removed_pairs_sql(c)" in uk


def test_prod_config_has_no_lists_yet_and_the_audit_is_fenced_and_swapped():
    cfg = json.loads((ROOT / "config.prod.json").read_text())
    assert list_legs.specs(cfg) == []
    assert cfg["outputs"]["list_audit"] == "openalex.awards.work_award_list_audit"
    assert any(cfg["outputs"]["list_audit"].startswith(f) for f in cfg["write_fence"])
    nb = (ROOT / "NightlyGrants.py").read_text()
    assert 'o["list_audit"]: first' in nb
    rt = (ROOT / "lib" / "nightly_runtime.py").read_text()
    assert rt.index("list_legs.bind(self)") < rt.index("self.derive_inputs()\n\n")


def test_runtime_accepts_the_optional_audit_output():
    cfg = dict(registry_prefix="dev.lab.ngr_", scratch_prefix="dev.lab.ngr_s_", write_fence=["dev.lab.ngr_"], inputs={}, sync=dict(mode="none"),
               outputs=dict(awards="dev.lab.ngr_a", aliases="dev.lab.ngr_b", work_awards="dev.lab.ngr_c", api="dev.lab.ngr_d",
                            list_audit="dev.lab.ngr_e"))
    Nightly(object(), cfg)


def test_works_funders_keep_the_funder_of_a_suppressed_link():
    nb = json.loads((ROOT.parents[1] / "end2end" / "CreateWorksEnriched.ipynb").read_text())
    cell = next("".join(c["source"]) for c in nb["cells"] if "from_work_awards AS" in "".join(c["source"]))
    block = cell[cell.index("from_work_awards AS"):cell.index("from_funder_reported AS")]
    assert "openalex.awards.work_award_list_audit" in block and "action = 'SUPPRESS' AND status = 'REMOVED'" in block


def test_audit_ddl_matches_the_columns_the_night_writes():
    ddl = (ROOT / "sql" / "NIGHTLY-DDL.sql").read_text()
    assert "work_award_list_audit (action STRING, list_name STRING, list_version BIGINT, source STRING, work_id BIGINT," in ddl


def audit_columns():
    import re
    src = (ROOT / "lib" / "list_legs.py").read_text()
    head = src[src.index("SELECT 'ADD' action"):src.index("FROM {r}list_adds a LEFT JOIN other")]
    cols = set(re.findall(r"(?:\b| )([a-z_]+)(?=,|$| *\n)", head.replace("END status", "status")))
    return {"action", "list_name", "list_version", "source", "work_id", "served_work_id", "list_award", "award_stable_id", "status",
            "detail", "reason", "award", "release_id"}, src


def test_every_query_on_list_audit_names_real_columns():
    cols, src = audit_columns()
    import re
    # the first UNION branch of list_audit defines the columns; check them against the DDL and every reader
    ddl = (ROOT / "sql" / "NIGHTLY-DDL.sql").read_text()
    body = ddl[ddl.index("work_award_list_audit ("):]
    assert set(re.findall(r"(\w+) (?:STRING|BIGINT|STRUCT)", body[:body.index(");")])) == cols
    for q in re.findall(r"FROM \{(?:r|audit)\}?(?:list_audit)? ?WHERE[^\"]*", src):
        pass
    for m in re.finditer(r"count\(DISTINCT ([a-z_]+),([a-z_]+)\) n FROM \{(?:r\}list_audit|audit\})", src):
        assert {m.group(1), m.group(2)} <= cols, m.group(0)
    nb = (ROOT.parents[1] / "end2end" / "CreateWorksEnriched.ipynb").read_text()
    assert "SELECT served_work_id, award FROM openalex.awards.work_award_list_audit" in nb


def test_conflict_check_covers_both_add_kinds_and_dropped_is_gated():
    c = apply_c([], 0, 0, 0)
    list_legs.apply(c, "S", True, "U")
    assert "FROM dev.lab.ngr_s_list_adds a JOIN dev.lab.ngr_s_list_suppress_keyed" in c.zeros["LIST_ADD_SUPPRESS_CONFLICT"]
    assert "UNION ALL SELECT n.list_name" in c.artifacts["list_adds"]
    assert "status='DROPPED'" in c.zeros["LIST_ADD_DROPPED"]
    assert "LEFT SEMI JOIN works_v" in c.artifacts["list_audit"]


def test_no_work_counts_against_the_add_budget():
    with pytest.raises(RuntimeError, match="LIST_ADD_UNRESOLVED_FUSE: kaken_direct_v1"):
        list_legs.apply(apply_c([("kaken_direct_v1", "NO_WORK", 1)], 10, 10, 0, {"add": [KAKEN]}), "s", True, "u")


def test_link_accounting_excludes_declared_list_changes():
    out = {"work_awards": "prod.awards.work_awards", "list_audit": "prod.awards.audit"}
    # last night 1000 served (100 of them only via a list); tonight 1100 rows before suppress, 150 only via a list -> legs 950 vs 900
    c = apply_c([], 1100, 1100, 0); c.outputs = out; c.prev_rows = 1000; c.prev_added = 100; c.distinct_added = 150
    c.config["work_links_drop_fuse"] = 0.01
    list_legs.apply(c, "s", True, "dev.lab.ngr_s_work_awards_listed")
    assert (c.counts["work_links_base_now"], c.counts["work_links_base_prev"]) == (950, 900)
    # legs alone lost 5%: stops even though the lists hide it in the total
    c = apply_c([], 1100, 1100, 0); c.outputs = out; c.prev_rows = 1100; c.prev_added = 100; c.distinct_added = 150
    c.config["work_links_drop_fuse"] = 0.01
    with pytest.raises(RuntimeError, match="WORK_LINK_DROP_FUSE"):
        list_legs.apply(c, "s", True, "dev.lab.ngr_s_work_awards_listed")
    # without the config key it only reports
    c = apply_c([], 1100, 1100, 0); c.outputs = out; c.prev_rows = 1100; c.prev_added = 100; c.distinct_added = 150
    list_legs.apply(c, "s", True, "dev.lab.ngr_s_work_awards_listed")
    assert c.counts["work_links_base_change"] < 0


def test_lists_require_the_audit_output_and_it_swaps_before_work_awards():
    c = FakeC({"add": [KAKEN]}, counts={"list_in_kaken_direct_v1": 363}, outputs={"work_awards": "a.b.c"})
    with pytest.raises(RuntimeError, match="LIST_AUDIT_OUTPUT_REQUIRED"):
        list_legs.bind(c)
    nb = (ROOT / "NightlyGrants.py").read_text()
    assert nb.index('o["list_audit"]: first') < nb.index('o["work_awards"]: c.r + "work_awards_candidate"') < nb.index('c.swap({o["list_audit"]: c.r + "list_audit"})')
    c = FakeC(); c.outputs = {"list_audit": "prod.awards.audit"}
    assert list_legs.audit_superset(c) == "dev.lab.ngr_s_list_audit_superset"
    assert "FROM prod.awards.audit l LEFT ANTI JOIN dev.lab.ngr_s_list_audit t" in c.artifacts["list_audit_superset"]


def test_conservation_pairs_cover_both_work_id_bases():
    c = FakeC()
    assert "UNION SELECT work_id,award_url" in list_legs.removed_pairs_sql(c)
    assert "UNION SELECT served_work_id" in list_legs.added_pairs_sql(c) and "list_adds" in list_legs.added_pairs_sql(c)
    for f in ("nih_fold.py", "uk_lineage.py"):   # EXCEPT binds tighter than the UNION inside: always subtract the whole subquery
        src = (ROOT / "lib" / f).read_text()
        assert "EXCEPT {list_legs.removed_pairs_sql(c)}" not in src and "EXCEPT SELECT * FROM ({list_legs.removed_pairs_sql(c)})" in src
    assert "list_legs.removed_pairs_sql(c)" in (ROOT / "lib" / "uk_lineage.py").read_text().split("uk_missing_mapped")[1][:400]


# ---------- data-only activation (control table) ----------
from datetime import datetime, timezone


def binding(**kw):
    row = dict(list_name="kaken_direct_v1", action="add", kind=None, relation="openalex.awards.award_list_kaken_direct_v1", version=0,
               expected_rows=363, tolerance=None, source="KAKEN", columns_json=None, filter_json='{"source": "KAKEN"}', priority=3,
               max_unresolved=0, max_unmatched=None, enabled=True, active_from_night="2026-10-18", bound_at="x", bound_by="rohan", note=None)
    row.update(kw)
    return types.SimpleNamespace(asDict=lambda: row)


class BindC(FakeC):
    def __init__(self, rows, lists=None, **kw):
        super().__init__(lists, **kw)
        self.config["list_bindings"] = "openalex.awards.award_list_bindings"
        self.started = datetime(2026, 10, 20, 3, 0, tzinfo=timezone.utc)          # 20:00 PT Mon 10-19
        self.rows, self.bound = rows, []

    def bind(self, view, relation):
        self.bound.append((view, relation))

    def sql(self, statement, params=None):
        if "QUALIFY row_number() OVER (PARTITION BY list_name" in statement:
            self.statements.append(statement)
            return Rows(self.rows)
        return super().sql(statement, params)


def test_night_is_the_pacific_start_date():
    assert list_legs.night_of(datetime(2026, 10, 20, 3, 0, tzinfo=timezone.utc)) == "2026-10-19"
    assert list_legs.night_of(datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc)) == "2026-10-19"


def test_no_control_table_configured_reads_nothing():
    c = FakeC()
    assert list_legs.bindings(c) == {}


def test_bound_rows_become_lists_and_the_table_is_bound_once_at_start():
    c = BindC([binding(), binding(list_name="cleanup_b1", action="suppress", relation="openalex.awards.award_list_cleanup_b1",
                                  expected_rows=50000, tolerance=10, filter_json=None, columns_json='{"reason": "decision"}', max_unmatched=20)])
    got = list_legs.bindings(c)
    assert c.bound == [("list_bindings", "openalex.awards.award_list_bindings")]
    assert "active_from_night <= DATE '2026-10-19'" in c.statements[-1]
    assert "LIST_BINDING_AMBIGUOUS" in c.zeros
    s = {x["name"]: x for x in list_legs.specs(c.config, got)}
    assert s["kaken_direct_v1"]["filter"] == {"source": "KAKEN"} and s["kaken_direct_v1"]["tolerance"] == 0
    assert s["cleanup_b1"]["action"] == "suppress" and s["cleanup_b1"]["columns"] == {"reason": "decision"} and s["cleanup_b1"]["max_unmatched"] == 20


def test_disabled_row_is_the_rollback():
    c = BindC([binding(enabled=False)])
    assert list_legs.specs(c.config, list_legs.bindings(c)) == []


def test_a_name_both_in_config_and_bound_is_refused():
    c = BindC([binding()], lists={"add": [KAKEN]})
    with pytest.raises(ValueError, match="duplicate"):
        list_legs.specs(c.config, list_legs.bindings(c))


def test_bound_row_still_goes_through_every_validation():
    c = BindC([binding(relation="openalex_dev.x; DROP")])
    with pytest.raises(ValueError):
        list_legs.specs(c.config, list_legs.bindings(c))
    c = BindC([binding(action="delete")])
    with pytest.raises(ValueError, match="action"):
        list_legs.bindings(c)


def test_prod_config_names_the_control_table_and_ddl_creates_it_append_only():
    cfg = json.loads((ROOT / "config.prod.json").read_text())
    assert cfg["list_bindings"] == "openalex.awards.award_list_bindings"
    ddl = (ROOT / "sql" / "NIGHTLY-DDL.sql").read_text()
    body = ddl[ddl.index("award_list_bindings ("):]
    for col in list_legs.BINDING_COLUMNS:
        assert col + " " in body
    assert "'delta.appendOnly'='true'" in body
