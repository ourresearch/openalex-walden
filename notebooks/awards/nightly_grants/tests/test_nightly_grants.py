"""Offline tests: runtime guards, step order, search writer/publish order, and port drift (no warehouse, no network)."""
import ast
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
BUILD = ROOT.parent
sys.path.insert(0, str(ROOT / "lib"))

import nightly_runtime
import sync_awards
from nightly_runtime import Nightly


# ---------- fakes ----------
class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def collect(self):
        return self.rows

    def take(self, n):
        return self.rows[:n]

    def createOrReplaceTempView(self, name):
        pass


class FakeSpark:
    """Records statements; answers lock/run-row reads from a tiny in-memory state."""
    def __init__(self, holder=None, holder_status=None, holder_dbr=None):
        self.statements, self.holder, self.holder_status, self.holder_dbr = [], holder, holder_status, holder_dbr

    def sql(self, statement, args=None):
        self.statements.append((statement, dict(args or {})))
        s = " ".join(statement.split())
        if s.startswith("SELECT holder,databricks_run_id FROM"):
            return Result([types.SimpleNamespace(holder=self.holder, databricks_run_id=self.holder_dbr)])
        if s.startswith("SELECT status FROM") and "award_nightly_runs" in s:
            return Result([types.SimpleNamespace(status=self.holder_status)] if self.holder_status else [])
        if s.startswith("UPDATE") and "award_nightly_lock SET holder=:rid" in s:
            if (self.holder == args.get("old")):
                self.holder = args["rid"]
            return Result()
        if s.startswith("SELECT holder FROM"):
            return Result([types.SimpleNamespace(holder=self.holder)])
        return Result()


def config(**extra):
    cfg = dict(registry_prefix="dev.lab.ngr_", scratch_prefix="dev.lab.ngr_s_", write_fence=["dev.lab.ngr_"],
               outputs=dict(awards="dev.lab.ngr_out_awards", aliases="dev.lab.ngr_out_aliases",
                            work_awards="dev.lab.ngr_out_work_awards", api="dev.lab.ngr_out_api"),
               inputs={}, sync=dict(mode="none"))
    cfg.update(extra)
    return cfg


# ---------- runtime ----------
def test_write_outside_fence_is_refused():
    c = Nightly(FakeSpark(), config())
    with pytest.raises(RuntimeError, match="WRITE_OUTSIDE_FENCE"):
        c.write("CREATE OR REPLACE TABLE openalex.awards.openalex_awards AS SELECT 1")
    with pytest.raises(RuntimeError, match="UNSUPPORTED_WRITE"):
        c.write("DROP TABLE dev.lab.ngr_x")


def test_only_referenced_parameters_are_sent():
    spark = FakeSpark()
    c = Nightly(spark, config())
    c.sql("SELECT :rid x, 'https://openalex.org' y")
    c.sql("SELECT 1")
    assert spark.statements[0][1] == {"rid": c.run_id}
    assert spark.statements[1][1] == {}


def test_lock_free_is_taken():
    spark = FakeSpark(holder=None)
    c = Nightly(spark, config())
    c.start()
    assert spark.holder == c.run_id and c.locked


def test_lock_held_by_finished_run_is_taken_over_and_marked():
    spark = FakeSpark(holder="old-run", holder_status="FAILED")
    c = Nightly(spark, config())
    c.start()
    assert spark.holder == c.run_id
    assert any("SET status='ABANDONED'" in s for s, _ in spark.statements)


def test_lock_held_by_running_manual_holder_is_refused():
    spark = FakeSpark(holder="manual", holder_status="RUNNING", holder_dbr=None)
    c = Nightly(spark, config())
    with pytest.raises(RuntimeError, match="NIGHTLY_LOCK_HELD_BY_LIVE_RUN"):
        c.start()


def test_swap_requires_every_check_passed():
    c = Nightly(FakeSpark(), config())
    with pytest.raises(RuntimeError, match="CHECKS_NOT_PASSED"):
        c.swap({"dev.lab.ngr_out_awards": "dev.lab.ngr_s_awards_candidate"})
    c.checks = {"A": True, "B": False}
    with pytest.raises(RuntimeError, match="CHECKS_NOT_PASSED"):
        c.swap({"dev.lab.ngr_out_awards": "dev.lab.ngr_s_awards_candidate"})


def test_failed_zero_check_raises_and_records():
    class One(FakeSpark):
        def sql(self, statement, args=None):
            return Result([types.SimpleNamespace(x=1)])
    c = Nightly(One(), config())
    with pytest.raises(RuntimeError, match="SOME_GATE"):
        c.zero("SOME_GATE", "SELECT 1")
    assert c.checks["SOME_GATE"] is False


def test_input_set_is_exact():
    c = Nightly(FakeSpark(), config(inputs={"raw": "a.b.c"}))
    with pytest.raises(RuntimeError, match="INPUT_SET"):
        c.bind_inputs()


# ---------- notebook order ----------
def calls_in_order(path):
    tree = ast.parse(path.read_text())
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
                names.append((node.lineno, f"{f.value.id}.{f.attr}"))
    return [n for _, n in sorted(names)]


def test_notebook_orders_checks_before_state_change_and_swap_before_search():
    order = calls_in_order(ROOT / "NightlyGrants.py")
    idx = {name: order.index(name) for name in ("c.start", "c.bind_inputs", "build_awards.run", "work_awards.run", "create_api.run",
                                                "sync_awards.stage", "build_awards.apply", "c.swap", "sync_awards.publish")}
    assert idx["c.start"] < idx["c.bind_inputs"] < idx["build_awards.run"] < idx["work_awards.run"] < idx["create_api.run"]
    assert idx["create_api.run"] < idx["sync_awards.stage"] < idx["build_awards.apply"] < idx["c.swap"] < idx["sync_awards.publish"]


# ---------- search ----------
class FakeES:
    def __init__(self, fail_ids=(), status=500):
        self.fail_ids, self.status, self.actions = set(fail_ids), status, []
        outer = self

        class Elasticsearch:
            def __init__(self, **kwargs):
                pass

        class helpers:
            @staticmethod
            def streaming_bulk(client, actions, **kwargs):
                for action in actions:
                    outer.actions.append(action)
                    ok = action["_id"] not in outer.fail_ids
                    yield ok, {action["_op_type"]: {"status": 200 if ok else outer.status}}

        self.Elasticsearch, self.helpers = Elasticsearch, helpers


def row(**values):
    return types.SimpleNamespace(**values)


def test_es_actions_and_redirect_shape():
    fake = FakeES()
    sync_awards.partition_writer("h", "awards-v4", "awards", es=fake)([row(id="https://openalex.org/G1", document_json='{"a":1}')])
    sync_awards.partition_writer("h", "merge-awards", "redirects", es=fake)([row(old_id=2, canonical_id=3)])
    assert fake.actions[0] == {"_op_type": "index", "_index": "awards-v4", "_id": "https://openalex.org/G1", "_source": {"a": 1}}
    assert fake.actions[1]["_source"] == {"id": "https://openalex.org/G2", "merge_into_id": "https://openalex.org/G3"}


def test_bulk_failure_raises_but_delete_404_is_fine():
    with pytest.raises(RuntimeError, match="BULK_FAILED"):
        sync_awards.partition_writer("h", "i", "awards", es=FakeES(fail_ids={"x"}))([row(id="x", document_json="{}")])
    sync_awards.partition_writer("h", "i", "delete", deleting=True, es=FakeES(fail_ids={"x"}, status=404))([row(id="x")])


def test_publish_order_documents_redirects_deletes():
    events = []

    class FakeSearch:
        awards, redirects = "awards-v4", "merge-awards"

        def write(self, query, index, mode, deleting=False):
            events.append(("write", mode))

        def refresh(self, kind):
            events.append(("refresh", kind))

        def observed(self, kind):
            events.append(("observe", kind))
            return "dev.lab.ngr_s_observed_" + kind

    c = Nightly(FakeSpark(), config())
    sync_awards.publish(c, FakeSearch())
    writes = [e[1] for e in events if e[0] == "write"]
    assert writes == ["awards", "redirects", "delete"]
    # every write is followed by a refresh and an observation before the next write
    assert events.index(("write", "redirects")) > events.index(("observe", "awards"))
    assert events.index(("write", "delete")) > events.index(("observe", "redirects"))
    assert {"ACTIVE_AWARD_DOC_MISSING", "REDIRECT_MAPPING_MISMATCH", "DELETIONS_VERIFIED", "EXPECTED_AWARDS_VERIFIED"} <= set(c.checks)


def test_no_release_wrapper_left():
    for f in ("build_awards.py", "work_awards.py", "create_api.py", "sync_awards.py", "nightly_runtime.py"):
        text = (ROOT / "lib" / f).read_text()
        for bad in ("c.phase(", "c.inject(", "previous_outputs", "award_runs", "VERSION AS OF {int(pin", "works_barrier", "gtr_deferred"):
            assert bad not in text, (f, bad)


# ---------- review fixes (Codex r1) ----------
class HistorySpark(FakeSpark):
    def __init__(self, ops):
        super().__init__()
        self.ops = ops                                   # newest first: [(version, operation)]

    def sql(self, statement, args=None):
        self.statements.append((statement, dict(args or {})))
        if "DESCRIBE HISTORY" in statement:
            return Result([types.SimpleNamespace(version=v, timestamp="t", operation=o) for v, o in self.ops])
        return Result()

    def views(self):
        return [s for s, _ in self.statements if "VERSION AS OF" in s]


def bound_version(ops):
    spark = HistorySpark(ops)
    Nightly(spark, config()).bind("raw", "a.b.raw")
    return spark.views()[-1].rsplit(" ", 1)[-1]


def test_bind_skips_a_trailing_producer_delete():
    assert bound_version([(12, "DELETE"), (11, "WRITE"), (10, "DELETE")]) == "11"
    assert bound_version([(13, "OPTIMIZE"), (12, "DELETE"), (11, "WRITE")]) == "11"
    assert bound_version([(13, "OPTIMIZE"), (12, "WRITE"), (11, "DELETE")]) == "13"
    assert bound_version([(5, "MERGE")]) == "5"


def test_run_id_required_when_configured():
    with pytest.raises(RuntimeError, match="DATABRICKS_RUN_ID_REQUIRED"):
        Nightly(FakeSpark(), config(require_databricks_run_id=True))
    Nightly(FakeSpark(), config(require_databricks_run_id=True), databricks_run_id=42)


def test_legacy_lock_must_be_free():
    class Held(FakeSpark):
        def sql(self, statement, args=None):
            if "award_registry_lock" in statement:
                return Result([types.SimpleNamespace(holder="x")])
            return super().sql(statement, args)
    with pytest.raises(RuntimeError, match="LEGACY_REGISTRY_LOCK_HELD"):
        Nightly(Held(), config(legacy_lock="openalex.awards.award_registry_lock")).start()


def test_job_passes_run_id():
    text = (ROOT.parents[2] / "jobs" / "nightly_grants.yaml").read_text()
    assert 'databricks_run_id: "{{job.run_id}}"' in text
    assert "max_concurrent_runs: 1" in text and ("pause_status: PAUSED" in text or "pause_status: UNPAUSED" in text)


# ---------- per-source completeness on raw rows (F1, 10-01) ----------
class ReceiptSpark(FakeSpark):
    def __init__(self, last_run=None, unreadable=False):
        super().__init__()
        self.last_run, self.unreadable = last_run, unreadable

    def sql(self, statement, args=None):
        self.statements.append((statement, dict(args or {})))
        if "FROM dev.lab.ngr_award_nightly_runs WHERE status LIKE 'SUCCEEDED%'" in statement:
            return Result([types.SimpleNamespace(details_json=self.last_run)] if self.last_run else [])
        if statement.startswith("SELECT count(*) FROM (") and self.unreadable:
            raise Exception("DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION")
        return Result()


def receipts_sql(spark):
    return next(s for s, _ in spark.statements if "ngr_s_receipts" in s and s.startswith("CREATE"))


def test_receipts_count_raw_rows_against_last_runs_versions():
    import json
    run = json.dumps(dict(versions=dict(raw=dict(relation="a.b.raw", version=7), gtr=dict(relation="a.b.gtr", version=3))))
    spark = ReceiptSpark(last_run=run)
    c = Nightly(spark, config())
    c.family_receipts()
    q = receipts_sql(spark)
    assert "SELECT provenance family,count(*) cur_n FROM raw_v GROUP BY 1" in q
    assert "FROM a.b.raw VERSION AS OF 7 GROUP BY 1" in q and "FROM a.b.gtr VERSION AS OF 3 WHERE work_id IS NOT NULL" in q
    assert "observations o" not in q                      # never the deduplicated observation count
    assert "coalesce(cur_n,0)>=0.98*coalesce(prev_n,0) coverage_complete" in q
    assert c.counts["_source_baseline"] == "raw v7, gtr v3"


def test_no_baseline_skips_the_ratio_and_records_why():
    import json
    for last, unreadable, why in ((None, False, "none: no successful run yet"),
                                  (json.dumps(dict(versions=dict(raw=dict(relation="a.b.raw", version=7), gtr=dict(relation="a.b.gtr", version=3)))), True, "unavailable")):
        spark = ReceiptSpark(last_run=last, unreadable=unreadable)
        c = Nightly(spark, config())
        c.family_receipts()
        assert "true coverage_complete" in receipts_sql(spark)
        assert c.counts["_source_baseline"].startswith(why)

# ---------- new funders added after the basis (auto-add, 10-01) ----------
class FunderSpark(FakeSpark):
    """Basis has columns funder_id, merge_into_id, display_name, ror_id; live lacks ror_id."""
    def __init__(self):
        super().__init__()
        self.views = {}

    def table(self, name):
        cols = {"funders_v": ["funder_id", "merge_into_id", "display_name", "ror_id"],
                "live_funders_v": ["funder_id", "merge_into_id", "display_name"]}[name]
        return types.SimpleNamespace(schema=types.SimpleNamespace(fields=[types.SimpleNamespace(name=c) for c in cols]))

    def sql(self, statement, args=None):
        self.statements.append((statement, dict(args or {})))
        if "DESCRIBE HISTORY" in statement:
            return Result([types.SimpleNamespace(version=7, timestamp="t", operation="WRITE")])
        if statement.startswith("SELECT count(*) n"):
            return Result([types.SimpleNamespace(n=0)])
        view = self

        class Frame(Result):
            def createOrReplaceTempView(self, name):
                view.views[name] = statement
        return Frame()


def test_new_unmerged_funders_are_added_and_nothing_else():
    spark = FunderSpark()
    c = Nightly(spark, config(live_funders="openalex.funders.funders"))
    c.versions["funders"] = dict(relation="openalex.awards.award_funders_basis", version=34)
    c.add_new_funders()
    added = next(s for s, _ in spark.statements if "ngr_s_funder_additions" in s and s.startswith("CREATE"))
    assert "LEFT ANTI JOIN openalex.awards.award_funders_basis VERSION AS OF 34 b ON b.funder_id=l.funder_id" in added
    assert "WHERE l.merge_into_id IS NULL" in added           # a merged new row is never added (merges need review)
    assert "NULL AS ror_id" in added and "l.display_name" in added
    assert spark.views["funders_v"] == ("SELECT * FROM openalex.awards.award_funders_basis VERSION AS OF 34 "
                                        "UNION ALL SELECT * FROM dev.lab.ngr_s_funder_additions")
    assert "funders_auto_added" in c.counts and "funder_merges_pending_basis_update" in c.counts


def test_without_live_funders_the_basis_is_used_alone():
    import inspect
    src = inspect.getsource(Nightly.derive_inputs)
    assert 'if self.config.get("live_funders"):\n            self.add_new_funders()' in src
