"""Offline tests for the sub-award step: input policy, gate order, projection, hash compatibility, job wiring (no warehouse)."""
import pathlib
import re
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import award_relations
import create_api


class Frame:
    def __init__(self, rows=(), cols=()):
        self.rows, self.columns = list(rows), list(cols)

    def collect(self):
        return self.rows


class FakeC:
    """Just enough of Nightly: records artifacts, checks and reads; answers counts with 0 and schema probes with `prev_cols`."""
    def __init__(self, extra=(), prev_cols=(), config=None):
        self.r, self.p, self.extra, self.prev_cols = "dev.lab.ngr_s_", "dev.lab.ngr_", set(extra), list(prev_cols)
        self.config = dict(package_root=str(ROOT), **(config or {}))
        self.artifacts, self.checks, self.counts, self.statements = [], {}, {}, []
        self.outputs = {}

    def guard(self): pass
    def has_extra(self, name): return name in self.extra
    def require(self, cond, name):
        if not cond: raise RuntimeError(name)

    def sql(self, statement, params=None):
        self.statements.append(statement)
        if "LIMIT 0" in statement:
            return Frame(cols=self.prev_cols)
        if statement.startswith("SELECT count(*) n"):
            return Frame([types.SimpleNamespace(n=0)])
        return Frame()

    def artifact(self, name, query, params=None):
        self.artifacts.append((name, query)); return self.r + name

    def zero(self, name, query):
        self.checks[name] = True; self.statements.append(query)

    def count(self, name, relation):
        self.counts[name] = 0; return 0


def test_no_input_before_first_links_runs_on_empty_relation():
    c = FakeC(prev_cols=["id", "display_name"])                   # yesterday's API predates the columns
    award_relations.run(c)
    names = [n for n, _ in c.artifacts]
    assert names[0] == "relations_raw_empty" and "RELATION_INPUT_REQUIRED" not in c.checks
    assert names[-1] == "awards_candidate"


def test_no_input_after_links_exist_is_checked():
    c = FakeC(prev_cols=["id", "parent_awards", "sub_awards_count"])
    award_relations.run(c)
    assert "RELATION_INPUT_REQUIRED" in c.checks


def test_configured_input_is_read_and_all_gates_precede_projection():
    c = FakeC(extra=["award_relations_raw"])
    award_relations.run(c)
    assert "relations_raw_empty" not in [n for n, _ in c.artifacts]
    assert any("award_relations_raw_v" in q for _, q in c.artifacts)
    for gate in ("RELATION_RAW_SHAPE", "RELATION_RAW_UNIQUE", "RELATION_BRIDGE_UNIQUE", "RELATION_LOOKUP_UNIQUE",
                 "RELATION_NATIVE_STAGING_DISAGREE", "RELATION_NO_LOSS", "RELATION_DROP_FUSE", "RELATION_TYPE_CONFLICT",
                 "RELATION_AWARDS_SAME_ROWS"):
        assert gate in c.checks, gate


def test_projection_caps_public_list_but_not_filters_or_count():
    c = FakeC(extra=["award_relations_raw"])
    award_relations.run(c)
    q = dict(c.artifacts)["awards_candidate"]
    assert "slice(coalesce(s.links,array()),1,100) sub_awards" in q
    assert "size(coalesce(s.links,array())) AS BIGINT) sub_awards_count" in q
    assert "coalesce(s.links,array()) sub_awards_full" in q
    assert "FROM dev.lab.ngr_s_awards_candidate_before_relations a" in q


def test_relations_never_touch_work_links():
    c = FakeC(extra=["award_relations_raw"])
    award_relations.run(c)
    assert not any("work_awards" in n or "work_awards" in q for n, q in c.artifacts)


def hash_queries(prev_cols):
    c = FakeC(prev_cols=prev_cols)
    c.sql = lambda st, params=None: Frame(cols=prev_cols) if "LIMIT 0" in st else Frame()
    create_api.run(c)
    return dict(c.artifacts)


def test_hash_unchanged_for_awards_without_links_and_old_previous_table():
    base = (ROOT / "sql/api_hash_expression.sql").read_text().strip()
    a = hash_queries(["id", "display_name"])
    assert a["previous_api_hash"] == f"SELECT id,updated_date,{base} content_hash FROM previous_api_v"
    assert a["new_api_hash"].startswith("SELECT id,CASE WHEN coalesce(size(parent_awards_full),0)=0 AND coalesce(size(sub_awards_full),0)=0 THEN " + base)
    b = hash_queries(["id", "parent_awards_full", "sub_awards_full"])
    assert "CASE WHEN coalesce(size(parent_awards_full),0)=0" in b["previous_api_hash"]


def test_job_runs_relations_between_work_links_and_api_and_swaps_output():
    text = (ROOT / "NightlyGrants.py").read_text()
    assert text.index("work_awards.run(c)") < text.index("award_relations.run(c)") < text.index("create_api.run(c)")
    assert 'o["relations"]: c.r + "relations_candidate"' in text
    assert text.index("award_relations.run(c)") < text.index('config.get("stop_before_apply")')


def test_payload_passes_relation_columns():
    sql = (ROOT / "sql/api_payload.sql").read_text()
    for col in ("parent_awards", "sub_awards", "sub_awards_count", "parent_awards_full", "sub_awards_full"):
        assert re.search(rf"oa\.{col},?\n", sql), col


class PublishedC(FakeC):
    """Last night published 1,000 component links; tonight's candidate has `now`."""
    def __init__(self, now, **kw):
        super().__init__(extra=["award_relations_raw"], **kw)
        self.outputs, self.now = {"relations": "dev.lab.ngr_out_relations"}, now

    def sql(self, statement, params=None):
        if "FROM dev.lab.ngr_out_relations GROUP BY 1" in statement:
            return Frame([types.SimpleNamespace(relation_type="component", n=1000)])
        if "FROM dev.lab.ngr_s_relations_candidate GROUP BY 1" in statement:
            return Frame([types.SimpleNamespace(relation_type="component", n=self.now)])
        return super().sql(statement, params)


def test_links_deleted_from_input_stop_the_night():
    with pytest.raises(RuntimeError, match="RELATION_COUNT_DROP_component: 1000 -> 0"):
        award_relations.run(PublishedC(now=0))


def test_small_drop_within_fuse_passes():
    c = PublishedC(now=990)
    award_relations.run(c)
    assert c.checks["RELATION_COUNT_DROP_component"]


def test_first_night_without_published_table_passes():
    class First(FakeC):
        def sql(self, statement, params=None):
            if "ngr_out_relations" in statement:
                self.probed = True
                raise Exception("[TABLE_OR_VIEW_NOT_FOUND] missing")
            return super().sql(statement, params)
    c = First(extra=["award_relations_raw"]); c.outputs = {"relations": "dev.lab.ngr_out_relations"}
    award_relations.run(c)
    assert c.probed and not any(k.startswith("RELATION_COUNT_DROP") for k in c.checks)
