"""CreateCrossrefWorkFunders, the evidence cell: which Crossref references carry their award ids into the junction.

Since the 10-16 Funders switch every new or changed reference entered with an empty array, so no new Crossref grant number
reached the Nightly. The cell now gives ROR / FundRef DOI references their usable ids, keeps name-resolved references as links
only (#1303), and appends to published pairs without ever removing an id.

The behaviour tests run the notebook's own cell in DuckDB via sqlglot (as the Nightly Grants tests do) and skip without them.
"""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
CELLS = [''.join(c['source']) for c in json.loads((ROOT / 'notebooks/awards/CreateCrossrefWorkFunders.ipynb').read_text())['cells']]
EVIDENCE = 'CREATE OR REPLACE TABLE openalex.awards.crossref_work_funder_evidence'
ARR = 'VARCHAR[]'


def cell(needle):
    found = [c for c in CELLS if needle in c]
    assert len(found) == 1, (needle, len(found))
    return found[0]


# ---------------------------------------------------------------- contract

def test_evidence_cell_shape():
    ev = cell(EVIDENCE)
    assert "CASE WHEN n.resolution_path IN ('ROR','FundRef DOI')\n  AND openalex.common.is_usable_award_id(aid) THEN aid END" in ev
    assert ev.count('resolution_path') == 1                             # ids by ROR / FundRef DOI only: no other path, no switch
    assert 'openalex.common.is_usable_award_id(aid)' in ev              # the junk filter the reader had before the switch
    assert 'LATERAL VIEW OUTER explode(n.award_ids) a AS aid' in ev     # OUTER: a reference with no ids still makes its link
    assert 'WHERE n.hold_reason IS NULL' in ev
    assert 'CAST(array() AS ARRAY<STRING>) award_ids' not in ev         # the 10-16 line that emptied every new reference
    assert 'array_sort(array_except(f.award_ids,coalesce(p.award_ids,array()))) new_ids' in ev   # appended ids in one order every run


# ---------------------------------------------------------------- behaviour

class Db:
    def __init__(self):
        self.duckdb = duckdb = pytest.importorskip('duckdb')
        self.sqlglot = pytest.importorskip('sqlglot')
        self.con = con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS openalex")
        con.execute('CREATE SCHEMA openalex.awards')
        con.execute('CREATE SCHEMA openalex.common')
        # stand-in for the UDF: the real one drops placeholders such as 'n/a'
        con.execute("CREATE MACRO openalex.common.is_usable_award_id(x) AS (x IS NOT NULL AND lower(trim(x)) NOT IN ('', 'n/a', 'none'))")
        for t in ('crossref_work_funders', 'crossref_no_doi_main_cut'):
            con.execute(f'CREATE TABLE openalex.awards.{t} (work_id BIGINT, funder_id BIGINT, award_ids {ARR})')
        con.execute(f'CREATE TABLE crossref_funder_name_candidates (work_id BIGINT, funder_id BIGINT, award_ids {ARR}, '
                    'resolution_path VARCHAR, hold_reason VARCHAR)')

    def published(self, work, funder, ids, table='crossref_work_funders'):
        self.con.execute(f'INSERT INTO openalex.awards.{table} VALUES (?, ?, ?::{ARR})', [work, funder, ids])

    def reference(self, work, funder, ids, path, hold=None):
        self.con.execute(f'INSERT INTO crossref_funder_name_candidates VALUES (?, ?, ?::{ARR}, ?, ?)', [work, funder, ids, path, hold])

    def run(self):
        """The evidence cell, then the junction as the notebook rebuilds it (no trap links in the fixture)."""
        self.before = {(w, f): ids for w, f, ids in self.con.execute(
            'SELECT * FROM openalex.awards.crossref_work_funders UNION ALL SELECT m.* FROM openalex.awards.crossref_no_doi_main_cut m '
            'ANTI JOIN openalex.awards.crossref_work_funders USING (work_id, funder_id)').fetchall()}
        sql = cell(EVIDENCE)[len('%sql\n'):].replace('USING delta AS', 'AS')
        out = self.sqlglot.transpile(sql, read='spark', write='duckdb')[0]
        outer = 'CROSS JOIN UNNEST(n.award_ids) AS a(aid)'                 # sqlglot drops OUTER: restore Spark's meaning
        assert out.count(outer) == 1
        out = out.replace(outer, 'LEFT JOIN UNNEST(n.award_ids) AS a(aid) ON true')
        try:
            self.con.execute(out)
        except self.duckdb.ParserException:                                # DuckDB before 1.3 writes a lambda as x -> ...
            self.con.execute(re.sub(r'LAMBDA (\w+) : ', r'\1 -> ', out))
        rows = self.con.execute('SELECT work_id, funder_id, award_ids, disposition FROM openalex.awards.crossref_work_funder_evidence').fetchall()
        assert len({r[:2] for r in rows}) == len(rows)                     # one row per pair
        got = {r[:2]: (self.as_spark(*r), r[3]) for r in rows}
        self.con.execute('DELETE FROM openalex.awards.crossref_work_funders')
        for (work, funder), (ids, _) in got.items():
            self.published(work, funder, ids)
        return got

    def as_spark(self, work, funder, ids, disposition):
        """Where the DuckDB rendering differs from Spark, put back here: collect_list over no ids is [] (ARRAY_AGG gives NULL)."""
        return ids or [] if disposition == 'accepted' else ids


def fixture():
    d = Db()
    d.published(1, 10, ['P1', 'P1', 'p1'])                 # published array, kept as it is (duplicates and case too)
    d.published(2, 10, ['Q1'])
    d.published(3, 10, None)                               # a NULL array stays NULL when nothing is added
    d.published(4, 10, [])                                 # e.g. a pair accepted with no ids since 10-16
    d.published(5, 10, ['Z9'])                             # no new reference
    d.published(6, 11, ['M1'], table='crossref_no_doi_main_cut')
    d.reference(1, 10, ['P1', 'P2', 'n/a'], 'FundRef DOI')         # changed reference on a published pair
    d.reference(2, 10, ['N1'], 'exact')                            # name path on a published pair
    d.reference(3, 10, None, 'FundRef DOI')
    d.reference(4, 10, ['B2', 'B1'], 'FundRef DOI')
    d.reference(4, 10, ['B1', 'B3'], 'ROR')
    d.reference(7, 10, ['A2', 'A1', 'A1', 'none'], 'FundRef DOI')  # new pair
    d.reference(8, 10, ['R1'], 'ROR')
    d.reference(9, 10, ['N2'], 'exact')                            # new pair, name path
    d.reference(9, 12, ['N3'], 'programme')
    d.reference(14, 10, ['N5'], 'hierarchy')
    d.reference(14, 11, ['N6'], 'reviewed')
    d.reference(14, 12, ['N7'], 'twin')
    d.reference(10, 10, [], 'FundRef DOI')                         # new pair, no ids deposited
    d.reference(11, 10, None, 'FundRef DOI')
    d.reference(12, 10, ['D1'], 'FundRef DOI')                     # one pair, both kinds of reference
    d.reference(12, 10, ['N4'], 'reviewed')
    d.reference(13, 10, ['H1'], 'exact', hold='global_name_ambiguity')   # held: no link at all
    d.reference(6, 11, ['M2'], 'FundRef DOI')                      # pair known only from the frozen main cut
    return d


def test_ror_and_doi_references_carry_ids_and_name_paths_do_not():
    got = fixture().run()
    assert got[(7, 10)] == (['A1', 'A2'], 'accepted')              # usable ids, distinct, sorted
    assert got[(8, 10)] == (['R1'], 'accepted')
    for pair in ((9, 10), (9, 12), (14, 10), (14, 11), (14, 12)):  # every name path: the link, no numbers (#1303)
        assert got[pair] == ([], 'accepted'), pair
    assert got[(10, 10)] == ([], 'accepted') and got[(11, 10)] == ([], 'accepted')
    assert got[(12, 10)] == (['D1'], 'accepted')
    assert (13, 10) not in got


def test_published_pairs_get_a_union_and_lose_nothing():
    d = fixture()
    got = d.run()
    before = d.before
    assert got[(1, 10)] == (['P1', 'P1', 'p1', 'P2'], 'preserved')  # old array first and untouched, then the new id
    assert got[(2, 10)] == (['Q1'], 'preserved')                    # a name-path reference adds nothing
    assert got[(3, 10)] == (None, 'preserved')
    assert got[(4, 10)] == (['B1', 'B2', 'B3'], 'preserved')
    assert got[(5, 10)] == (['Z9'], 'preserved')
    assert got[(6, 11)] == (['M1', 'M2'], 'preserved')
    for pair, old in before.items():                                # nothing is removed, order is kept
        assert got[pair][1] == 'preserved' and (got[pair][0] or [])[:len(old or [])] == (old or []), pair
    assert len(got) == len(before) + 10


def test_a_second_run_changes_nothing():
    d = fixture()
    first = d.run()
    second = d.run()
    assert {k: v[0] for k, v in first.items()} == {k: v[0] for k, v in second.items()}
    assert {v[1] for v in second.values()} == {'preserved'}         # last night's accepted pairs are published pairs tonight


def test_a_name_reference_never_adds_numbers_to_a_published_pair():
    d = fixture()
    first = d.run()
    assert first[(2, 10)] == (['Q1'], 'preserved') and first[(12, 10)] == (['D1'], 'accepted')
    second = d.run()                                                # the name references are still there on the next run
    assert second[(2, 10)][0] == ['Q1'] and second[(12, 10)][0] == ['D1'] and second[(9, 10)][0] == []
