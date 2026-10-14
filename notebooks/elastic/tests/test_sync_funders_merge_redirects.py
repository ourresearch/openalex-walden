"""Offline checks for the merge-funders redirect cell in notebooks/elastic/sync_funders.py.

Pure planner tests, plus the whole notebook executed against an in-memory Spark/ES fake, so the
ordering (redirect cell raises before the stale-doc delete) is exercised, not string-matched."""
import ast
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

HERE = Path(__file__).resolve().parent
SRC = (HERE.parent / "sync_funders.py").read_text()
F = "https://openalex.org/F"
FUNDERS, MERGE = "funders-v3", "merge-funders"


def _pairs():
    rows = [l.split("\t") for l in (HERE / "merged_funders_20261014.tsv").read_text().split("\n") if l]
    return {int(a): int(b) for a, b in rows}


def _load_pure():
    names = ("plan_merge_redirects", "check_redirect_plan", "protected_loser_urls")
    keep = [n for n in ast.parse(SRC).body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "F_URL" for t in n.targets))]
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), "sync_funders.py", "exec"), ns)
    return [ns[n] for n in names]


plan, check, protected = _load_pure()


# ---------------------------------------------------------------- in-memory Spark + ES
class FakeES:
    def __init__(self, store):
        self.store = store                     # {index: {doc_id: source}}
        self.ops = []                          # (op, index, id)
        self.indices = types.SimpleNamespace(exists=lambda index: index in self.store,
                                             refresh=lambda index: None)

    def mget(self, index, ids, _source=False):
        if getattr(self, "on_mget", None):
            self.on_mget(self)
        docs = self.store.get(index, {})
        return {"docs": [{"_id": i, "found": i in docs} for i in ids]}

    def get(self, index, id):
        return {"_id": id, "_source": self.store[index][id]}


def _run(merges, api_ids, store, cells=None, extra=None, on_mget=None):
    es = FakeES(store)
    es.on_mget = on_mget
    api_reads = list(api_ids) if isinstance(api_ids, list) else None

    def scan(client, index, query=None, _source=True):
        for k, v in list(es.store.get(index, {}).items()):
            yield {"_id": k, "_source": v}

    def bulk(client, actions, chunk_size=500, raise_on_error=True):
        n, errors = 0, []
        for a in actions:
            idx = es.store.setdefault(a["_index"], {})
            if a["_op_type"] == "create" and a["_id"] in idx:
                errors.append({"create": {"_id": a["_id"], "status": 409}})
                continue
            if a["_op_type"] in ("index", "create"):
                idx[a["_id"]] = a["_source"]
            else:
                idx.pop(a["_id"], None)
            es.ops.append((a["_op_type"], a["_index"], a["_id"])); n += 1
        return n, errors

    helpers = types.SimpleNamespace(scan=scan, bulk=bulk, parallel_bulk=lambda *a, **k: iter(()))
    esmod = types.SimpleNamespace(Elasticsearch=lambda **k: es, helpers=helpers)
    pyspark = types.ModuleType("pyspark"); sql = types.ModuleType("pyspark.sql")
    sql.functions = mock.MagicMock(); pyspark.sql = sql

    class Spark:
        def table(self, name):
            return mock.MagicMock()

        def sql(self, q):
            if "merge_into_id IS NOT NULL" in q:
                rows = [types.SimpleNamespace(funder_id=a, merge_into_id=b) for a, b in merges.items()]
            else:
                ids = api_reads.pop(0) if api_reads is not None else api_ids
                rows = [types.SimpleNamespace(id=i) for i in ids]
            return types.SimpleNamespace(collect=lambda: rows)

    g = {"spark": Spark(), "dbutils": types.SimpleNamespace(
        secrets=types.SimpleNamespace(get=lambda scope, key: "http://fake"))}
    g.update(extra or {})
    mods = {"elasticsearch": esmod, "elasticsearch.helpers": helpers,
            "pyspark": pyspark, "pyspark.sql": sql, "pyspark.sql.functions": sql.functions}
    parts = SRC.split("# COMMAND ----------")
    with mock.patch.dict(sys.modules, mods):
        for i, part in enumerate(parts):
            if cells is None or i in cells:
                exec(compile(part, f"sync_funders.py#cell{i}", "exec"), g)
    return es, g


def _store(funders, merge_docs=None):
    s = {FUNDERS: {f"{F}{i}": {"id": i} for i in funders}}
    if merge_docs is not None:
        s[MERGE] = dict(merge_docs)
    return s


WELLCOME = {F + "4320307874": {"id": F + "4320307874", "merge_into_id": F + "4320311904"}}


# ---------------------------------------------------------------- planner
def test_foster_foundation_redirects():
    _, up, conf, bad = plan({4320311391: 4320306339}, {4320306339}, {})
    assert up == {F + "4320311391": F + "4320306339"} and conf == {} and bad == {}


def test_chain_resolved_to_live_root():
    _, up, conf, bad = plan({1: 2, 2: 3}, {3}, {})
    assert up == {F + "1": F + "3", F + "2": F + "3"} and conf == {} and bad == {}


def test_unresolved_cases_publish_nothing():
    _, up, _, bad = plan({1: 2, 2: 1, 5: 6, 7: 8}, {7, 8}, {})
    assert up == {} and set(bad) == {1, 2, 5, 7}


def test_existing_doc_never_planned_for_change():
    _, up, conf, bad = plan({1: 2}, {2}, {F + "1": F + "9"})
    assert up == {} and conf == {F + "1": (F + "9", F + "2")}
    with pytest.raises(Exception, match="not overwritten"):
        check(bad, conf, {})
    assert protected(bad, conf, {}) == {F + "1"}


# ---------------------------------------------------------------- whole notebook
def test_backfill_65_keeps_wellcome_and_deletes_only_stale_losers():
    merges = _pairs()
    winners = set(merges.values())
    es, _ = _run(merges, winners, _store(winners | {4320311391}, WELLCOME))
    assert len(es.store[MERGE]) == 65
    assert es.store[MERGE][F + "4320307874"] == WELLCOME[F + "4320307874"]
    assert es.store[MERGE][F + "4320311391"]["merge_into_id"] == F + "4320306339"
    assert not [o for o in es.ops if o[1] == MERGE and o[0] != "create"]
    assert ("create", MERGE, F + "4320307874") not in es.ops          # Wellcome untouched
    assert ("delete", FUNDERS, F + "4320311391") in es.ops            # old loser doc removed after redirect


def test_empty_merge_read_deletes_no_redirects():
    docs = {F + str(a): {"id": F + str(a), "merge_into_id": F + str(b)} for a, b in _pairs().items()}
    es, _ = _run({}, {4320311904}, _store({4320311904}, docs))
    assert es.store[MERGE] == docs and not [o for o in es.ops if o[1] == MERGE]


def test_no_live_root_raises_before_stale_delete():
    store = _store({1, 3})                                            # F1 stale entity doc, F2 not live
    with pytest.raises(Exception, match="MERGE-FUNDERS REDIRECT SYNC FAILED.*root F2 not live"):
        _run({1: 2}, {3}, store)
    assert F + "1" in store[FUNDERS] and MERGE not in store           # loser doc kept (no 404), nothing written


def test_missing_winner_in_es_raises_and_writes_nothing():
    store = _store({1})                                               # F2 live in SQL but no ES doc
    with pytest.raises(Exception, match="winner https://openalex.org/F2 has no doc"):
        _run({1: 2}, {2}, store)
    assert MERGE not in store and F + "1" in store[FUNDERS]


def test_conflicting_existing_doc_raises_and_is_kept():
    store = _store({1, 2, 9}, {F + "1": {"id": F + "1", "merge_into_id": F + "9"}})
    with pytest.raises(Exception, match="not overwritten"):
        _run({1: 2}, {2, 9}, store)
    assert store[MERGE][F + "1"]["merge_into_id"] == F + "9"


def test_stale_delete_skips_protected_losers():
    parts = SRC.split("# COMMAND ----------")
    skip = {i for i, p in enumerate(parts) if "def plan_merge_redirects" in p}
    assert len(skip) == 1
    store = _store({1, 5, 7})                                         # run every cell but the redirect cell
    _run({}, {7}, store, cells=set(range(len(parts))) - skip,
         extra={"REDIRECT_PROTECTED": {F + "1"}, "API_SNAPSHOT_URLS": {F + "7"},
                "MERGED_LOSER_URLS": set(), "VERIFIED_LOSERS": set()})
    assert F + "1" in store[FUNDERS] and F + "5" not in store[FUNDERS]


def test_chain_extension_on_file_is_accepted_and_next_hop_written():
    # A=1 -> B=2 on file; B was merged into C=3 later: keep A->B, add B->C
    _, up, conf, bad = plan({1: 2, 2: 3}, {3}, {F + "1": F + "2"})
    assert conf == {} and bad == {} and up == {F + "2": F + "3"}
    store = _store({3}, {F + "1": {"id": F + "1", "merge_into_id": F + "2"}})
    es, _ = _run({1: 2, 2: 3}, {3}, store)
    assert store[MERGE][F + "1"]["merge_into_id"] == F + "2"         # kept, not rewritten
    assert store[MERGE][F + "2"]["merge_into_id"] == F + "3"         # second hop written
    assert ("create", MERGE, F + "1") not in es.ops


def test_on_file_target_with_a_different_root_still_raises():
    # 1 -> 2 on file, 2 -> 4 now, but 1 is merged into 3 (a different live root)
    _, up, conf, _ = plan({1: 3, 2: 4}, {3, 4}, {F + "1": F + "2"})
    assert conf == {F + "1": (F + "2", F + "3")}
    store = _store({3, 4}, {F + "1": {"id": F + "1", "merge_into_id": F + "2"}})
    with pytest.raises(Exception, match="not overwritten"):
        _run({1: 3, 2: 4}, {3, 4}, store)
    assert store[MERGE][F + "1"]["merge_into_id"] == F + "2"


# ---------------------------------------------------------------- Codex round-2 reproductions
def test_r2_1_existing_mapping_with_missing_winner_blocks_delete():
    # SQL {1:2}, API {2}, ES has F1 only, redirect F1->F2 on file: F2 has no doc -> raise, F1 kept
    store = _store({1}, {F + "1": {"id": F + "1", "merge_into_id": F + "2"}})
    with pytest.raises(Exception, match="winner https://openalex.org/F2 has no doc"):
        _run({1: 2}, {2}, store)
    assert F + "1" in store[FUNDERS]


def test_r2_2_stale_delete_uses_the_planner_snapshot():
    # planner read {1,2}, a later read would say {2}: F1 must not be deleted
    store = _store({1, 2})
    es, _ = _run({}, [{1, 2}, {2}], store)
    assert F + "1" in store[FUNDERS] and not [o for o in es.ops if o[0] == "delete"]


def test_r2_3_inconsistent_redirect_graph_is_a_conflict():
    # SQL {1:3, 2:3}; on file F1->F2 and F2->F1 (cycle): raise, both entity docs kept
    on_file = {F + "1": {"id": F + "1", "merge_into_id": F + "2"},
               F + "2": {"id": F + "2", "merge_into_id": F + "1"}}
    store = _store({1, 2, 3}, on_file)
    with pytest.raises(Exception, match="not overwritten"):
        _run({1: 3, 2: 3}, {3}, store)
    assert {F + "1", F + "2"} <= set(store[FUNDERS]) and store[MERGE] == on_file


def test_r2_4_doc_created_after_scan_is_not_overwritten():
    # F1->F9 appears after the scan (during the winner mget): create gets 409 -> re-read -> conflict
    def race(es):
        es.store.setdefault(MERGE, {})[F + "1"] = {"id": F + "1", "merge_into_id": F + "9"}
    store = _store({1, 2})
    with pytest.raises(Exception, match="not overwritten"):
        _run({1: 2}, {2}, store, on_mget=race)
    assert store[MERGE][F + "1"]["merge_into_id"] == F + "9" and F + "1" in store[FUNDERS]


def test_r2_4_same_doc_created_concurrently_is_fine():
    def race(es):
        es.store.setdefault(MERGE, {})[F + "1"] = {"id": F + "1", "merge_into_id": F + "2"}
    store = _store({1, 2})
    _run({1: 2}, {2}, store, on_mget=race)
    assert F + "1" not in store[FUNDERS]                              # verified loser removed


def test_r3_chain_through_a_live_intermediate_is_a_conflict():
    # SQL {1:3}; F2 and F3 live; on file F1->F2 and F2->F3: raise, F1's doc kept
    on_file = {F + "1": {"id": F + "1", "merge_into_id": F + "2"},
               F + "2": {"id": F + "2", "merge_into_id": F + "3"}}
    _, _, conf, _ = plan({1: 3}, {2, 3}, {k: d["merge_into_id"] for k, d in on_file.items()})
    assert conf == {F + "1": (F + "2", F + "3")}
    store = _store({1, 2, 3}, on_file)
    with pytest.raises(Exception, match="not overwritten"):
        _run({1: 3}, {2, 3}, store)
    assert F + "1" in store[FUNDERS] and store[MERGE] == on_file
