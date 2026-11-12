"""Topics for awards with no text, from their papers (lib/papers_topics.py, oxjob #2125 part 2).

The behaviour test runs the production SQL itself (sqlglot Spark -> DuckDB) on a small fixture; it is skipped without them.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
import papers_topics as PT  # noqa: E402

SUB = {1: "S1", 2: "S1", 3: "S2", 4: "S3", 5: "S3"}
VOCAB = "SELECT tid, display_name, subfield, field, domain FROM vocab"


def topic(t, score):
    return {"id": f"https://openalex.org/T{t}", "score": score}


def text_row(t, nc=False):
    if nc:
        return []
    s = f"https://openalex.org/subfields/{SUB[t]}"
    return [{"id": f"https://openalex.org/T{t}", "display_name": f"T{t}", "score": 0.9,
                           "subfield": {"id": s, "display_name": SUB[t]}, "field": {"id": "F", "display_name": "F"},
                           "domain": {"id": "D", "display_name": "D"}}]


class Fixture:   # a funder with 60 text grants: both funder-wide 1-paper signals need >= 20 / >= 50 of them
    def __init__(self):
        duckdb = pytest.importorskip("duckdb")
        self.sqlglot = pytest.importorskip("sqlglot")
        self.db = db = duckdb.connect()
        st = "STRUCT(id VARCHAR, display_name VARCHAR)"
        db.execute(f"CREATE TABLE vocab (tid INT, display_name VARCHAR, subfield {st}, field {st}, domain {st})")
        for t, s in SUB.items():
            db.execute("INSERT INTO vocab VALUES (?, ?, {'id': ?, 'display_name': ?}, {'id': 'F', 'display_name': 'F'}, "
                       "{'id': 'D', 'display_name': 'D'})", [t, f"T{t}", f"https://openalex.org/subfields/{s}", s])
        db.execute("CREATE TABLE a (id BIGINT, funder_id BIGINT, funder_award_id VARCHAR)")
        db.execute(f"CREATE TABLE t (award_id BIGINT, topics STRUCT(id VARCHAR, display_name VARCHAR, score FLOAT, subfield {st}, "
                   f"field {st}, domain {st})[], not_classifiable BOOLEAN, source VARCHAR)")
        db.execute("CREATE TABLE wa (award STRUCT(id VARCHAR), work_id BIGINT)")
        db.execute("CREATE TABLE wt (work_id BIGINT, topics STRUCT(id VARCHAR, score DOUBLE)[])")

    def award(self, i, funder, number="X1", text=None, nc=False):
        self.db.execute("INSERT INTO a VALUES (?, ?, ?)", [i, funder, number])
        if text or nc:
            self.db.execute("INSERT INTO t VALUES (?, ?, ?, 'q8b_2m')", [i, text_row(text, nc), nc])

    def paper(self, w, topics, *awards):
        self.db.execute("INSERT INTO wt VALUES (?, ?)", [w, [topic(t, s) for t, s in topics]])
        for i in awards:
            self.db.execute("INSERT INTO wa VALUES ({'id': ?}, ?)", [f"https://openalex.org/G{i}", w])

    def served(self):
        q = lambda s: self.sqlglot.transpile(s, read="spark", write="duckdb")[0]
        self.db.execute(f"CREATE TABLE l AS {q(PT.links_sql('wa'))}")
        self.db.execute(f"CREATE TABLE p AS {q(PT.paper_topics_sql('wt', 'l'))}")
        rows = self.db.execute(f"SELECT award_id, rule, papers, share, topics FROM ({q(PT.served_sql('a', 't', 'l', 'p', vocab=VOCAB))})").fetchall()
        return {r[0]: {"rule": r[1], "papers": r[2], "share": r[3], "topics": [(x["id"].rsplit("T", 1)[1], round(x["score"], 4)) for x in r[4]]}
                for r in rows}


def build():
    f = Fixture()
    # funder 10: 60 text grants on topic 1, each with its own paper (topic 1 primary): both funder-wide signals hold for topic 1
    for i in range(60):
        f.award(1000 + i, 10, f"R01{i:04d}", text=1)
        f.paper(5000 + i, [(1, 0.9), (2, 0.1)], 1000 + i)
    f.award(100, 10); f.paper(1, [(1, 0.9), (3, 0.1)], 100); f.paper(2, [(1, 0.8), (2, 0.2)], 100)          # 2 papers, share .85
    f.award(101, 10); f.paper(3, [(1, 1.0)], 101); f.paper(4, [(3, 1.0)], 101)                               # 2 papers, .5 tie
    f.award(102, 10); f.paper(5, [(4, 1.0)], 102); f.paper(6, [(5, 1.0)], 102); f.paper(7, [(3, 1.0)], 102)  # spread: abstain
    f.award(103, 10, text=4); f.paper(8, [(1, 1.0)], 103); f.paper(9, [(1, 1.0)], 103)                        # text row wins
    f.award(104, 10, nc=True); f.paper(10, [(1, 1.0)], 104); f.paper(11, [(1, 1.0)], 104)                     # not classifiable too
    f.award(200, 10); f.paper(12, [(1, 0.7), (2, 0.3)], 200)                                                  # 1 paper, 2 signals
    f.award(3000, 20, text=4); f.award(201, 10); f.paper(13, [(1, 0.7), (2, 0.3)], 201, 3000)                 # 2 signals, vetoed
    f.award(3001, 30, text=5); f.award(202, 30); f.paper(14, [(5, 1.0)], 202, 3001)                           # co-grant only
    return f.served()


def test_papers_rule_serves_the_text_less_awards_it_should_and_nothing_else():
    s = build()
    assert set(s) == {100, 101, 200}, s
    assert s[100] == {"rule": "multi", "papers": 2, "share": 0.85, "topics": [("1", 0.85), ("2", 0.1), ("3", 0.05)]}
    assert s[101]["rule"] == "multi" and s[101]["topics"] == [("1", 0.5), ("3", 0.5)]   # a tie: the lower topic id first
    assert s[200] == {"rule": "one", "papers": 1, "share": 0.7, "topics": [("1", 0.7), ("2", 0.3)]}


def test_the_merge_never_touches_a_text_row_and_only_rewrites_changed_papers_rows():
    duckdb = pytest.importorskip("duckdb")
    sqlglot = pytest.importorskip("sqlglot")
    db = duckdb.connect()
    db.execute("CREATE TABLE target (award_id BIGINT, topics STRUCT(id VARCHAR)[], not_classifiable BOOLEAN, source VARCHAR, "
               "created_datetime TIMESTAMP, updated_datetime TIMESTAMP, text_key BIGINT)")
    old = "TIMESTAMP '2026-01-01 00:00:00'"
    db.execute(f"""INSERT INTO target VALUES (1, [{{'id': 'T1'}}], false, 'q8b_2m', {old}, {old}, 5),
        (2, [], true, 'q8b_2m', {old}, {old}, 6), (3, [{{'id': 'T3'}}], false, 'papers_v1', {old}, {old}, NULL),
        (4, [{{'id': 'T4'}}], false, 'papers_v1', {old}, {old}, NULL), (5, [{{'id': 'T5'}}], false, 'papers_v1', {old}, {old}, NULL)""")
    db.execute("CREATE TABLE served (award_id BIGINT, topics STRUCT(id VARCHAR)[])")
    # 1 and 2 are text rows (a served row for them is refused by the plan's over_text_rows guard; the MERGE leaves them anyway),
    # 3 unchanged, 4 changed, 5 no longer served, 6 new
    db.execute("INSERT INTO served VALUES (1, [{'id': 'T9'}]), (2, [{'id': 'T9'}]), (3, [{'id': 'T3'}]), (4, [{'id': 'T7'}]), (6, [{'id': 'T6'}])")
    db.execute(sqlglot.transpile(PT.merge_sql("target", "served"), read="spark", write="duckdb")[0])
    rows = {r[0]: (r[1], r[2], str(r[3])) for r in db.execute(
        "SELECT award_id, [x.id for x in topics], source, updated_datetime FROM target ORDER BY 1").fetchall()}
    assert rows[1] == (["T1"], "q8b_2m", "2026-01-01 00:00:00") and rows[2] == ([], "q8b_2m", "2026-01-01 00:00:00")
    assert rows[3] == (["T3"], "papers_v1", "2026-01-01 00:00:00")             # unchanged: not rewritten
    assert rows[4][:2] == (["T7"], "papers_v1") and rows[4][2] != "2026-01-01 00:00:00"
    assert 5 not in rows and rows[6][:2] == (["T6"], "papers_v1")
    db.execute("DELETE FROM served")                                             # an empty served set deletes papers rows only
    db.execute(sqlglot.transpile(PT.merge_sql("target", "served"), read="spark", write="duckdb")[0])
    assert {r[0] for r in db.execute("SELECT award_id FROM target").fetchall()} == {1, 2}


def test_the_rule_reads_only_the_staged_tables_and_the_plan_guards_exist():
    served = PT.served_sql("a", "target", "l", "p")
    assert "LEFT ANTI JOIN (SELECT award_id FROM target WHERE NOT (source <=> 'papers_v1'))" in served
    assert "CROSS JOIN" not in served and "work_awards" not in served and "LATERAL VIEW" not in served
    assert "sum(CAST(p.part AS DECIMAL(24, 12)))" in served                     # exact in any order: same rows every run
    plan = PT.plan_sql("target", "served")
    assert all(k in plan for k in ("over_text_rows", "bad_rows", "would_delete", "served_ids"))


def test_papers_task_runs_after_topics_and_stages_its_inputs():
    src = open(os.path.join(HERE, "..", "BuildAwardTopicsFromPapers.py")).read()
    assert src.index("PT.links_sql(") < src.index("PT.paper_topics_sql(") < src.index("PT.served_sql(") < src.index("PT.merge_sql(")
    assert src.index("if DRY:") < src.index("spark.sql(PT.merge_sql(")   # a dry run never writes the target
    # the staged tables can never be the target or an input, and the guards and the MERGE read one version of the served rows
    assert 'assert not {LINKS, PAPERS, SERVED} & {W["awards_table"], W["target"], W["work_awards_table"], W["work_topics_table"]}' in src
    assert src.index("assert not {LINKS, PAPERS, SERVED}") < src.index("CREATE OR REPLACE TABLE {LINKS}")
    assert 'PT.plan_sql(W["target"], SERVED_AT)' in src and 'PT.merge_sql(W["target"], SERVED_AT)' in src
    assert "raise RuntimeError(\"refusing to merge, NOTHING written to the target" in src
