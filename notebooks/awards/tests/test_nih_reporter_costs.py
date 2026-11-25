"""NIH RePORTER costs for ExPORTER's FY1985-1999 years (oxjob #2326, option C).

scripts/local/nih_reporter_costs_to_s3.py fetches RePORTER's cost for every FY1985-1999 ExPORTER application that has none.
The Awards job's nih_exporter mapping (notebooks/awards/sources/nih_exporter.sql), which replaced CreateNIHAwards and carries
the same rule, reads that file and uses it only where ExPORTER's own cost is blank. The mapping tests run its own join and
amount expressions in DuckDB (via sqlglot); the script tests use a fake RePORTER session.
"""
import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
MAP = (ROOT / 'notebooks/awards/sources/nih_exporter.sql').read_text()


def squash(s):
    return ' '.join(s.split())


def between(text, start, end):
    i = text.index(start)
    return text[i:text.index(end, i)]


# ---------------------------------------------------------------- mapping contract

def test_mapping_reads_the_reporter_file_and_only_a_positive_cost():
    s = squash(MAP)
    assert ("FROM read_files('s3://openalex-ingest/awards/nih/nih_reporter_costs.parquet', format => 'parquet') "
            "WHERE award_amount > 0") in s   # $0: not carried
    assert 'CAST(application_id AS STRING) AS application_id' in s and 'CAST(award_amount AS DOUBLE) AS award_amount' in s


def test_mapping_joins_by_application_id_never_for_a_sub_project():
    s = squash(MAP)
    assert ("LEFT JOIN nih_reporter_costs rc ON rc.application_id = CAST(r.application_id AS STRING) "
            "AND (r.subproject_id IS NULL OR TRIM(r.subproject_id) = '')") in s
    assert 'rc.award_amount as reporter_cost,' in s
    assert 'CAST(NULL AS DOUBLE) AS reporter_cost' in s          # the sub-project branch of parsed_grants carries none
    assert 'COALESCE(TRY_CAST(g.total_cost AS DOUBLE), g.reporter_cost)' in s


def test_the_hand_run_notebook_is_gone():
    assert not (ROOT / 'notebooks/awards/CreateNIHAwards.ipynb').exists()


# ---------------------------------------------------------------- mapping behaviour (the mapping's own SQL, in DuckDB)

def run_fill(rows, reporter):
    duckdb = pytest.importorskip('duckdb')
    sqlglot = pytest.importorskip('sqlglot')
    join = between(MAP, 'LEFT JOIN nih_reporter_costs rc', '\n),')
    amount = between(MAP, 'CAST(CASE WHEN g.subproject_id IS NULL', ' as amount,')
    spark = (f"WITH parsed_all AS (SELECT r.*, rc.award_amount as reporter_cost FROM nih_grants_raw r {join}) "
             f"SELECT g.application_id, {amount} AS amount FROM parsed_all g ORDER BY g.application_id")
    q = sqlglot.transpile(spark, read='spark', write='duckdb')[0]
    c = duckdb.connect()
    c.execute('CREATE TABLE nih_grants_raw(application_id VARCHAR, subproject_id VARCHAR, total_cost VARCHAR)')
    c.execute('CREATE TABLE nih_reporter_costs(application_id VARCHAR, award_amount DOUBLE)')
    c.executemany('INSERT INTO nih_grants_raw VALUES (?,?,?)', rows)
    c.executemany('INSERT INTO nih_reporter_costs VALUES (?,?)', reporter)
    return {a: amt for a, amt in c.execute(q).fetchall()}


def test_exporter_cost_first_reporter_only_where_blank_sub_projects_none():
    got = run_fill(
        rows=[('1001', None, '250000'),     # FY2005: ExPORTER's own cost
              ('1002', None, None),         # FY1995: no ExPORTER cost; RePORTER has one
              ('1003', None, None),         # FY1990 intramural: neither source
              ('1004', '0001', None),       # a sub-project row: never an amount
              ('1005', None, '100')],       # ExPORTER's cost wins even if RePORTER says otherwise
        reporter=[('1002', 161807.0), ('1004', 999.0), ('1005', 200.0)])
    assert got == {'1001': 250000.0, '1002': 161807.0, '1003': None, '1004': None, '1005': 100.0}, got


# ---------------------------------------------------------------- the fetch script

def load_script():
    pytest.importorskip('pandas'); pytest.importorskip('requests')
    spec = importlib.util.spec_from_file_location('nih_reporter_costs_to_s3', ROOT / 'scripts/local/nih_reporter_costs_to_s3.py')
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


def test_needed_ids_are_fy1985_1999_project_years_without_a_cost():
    import pandas as pd
    m = load_script()
    df = pd.DataFrame({'application_id': ['30', '4', '5', '6', '7', '8', '200'],
                       'fy': [1999.0, 1985.0, 2000.0, 1990.0, 1984.0, 1995.0, 1992.0],
                       'subproject_id': [None, '', None, '0002', None, None, None],
                       'total_cost': [None, None, None, None, None, '5000', '']})
    assert m.needed_application_ids(df) == ['4', '30', '200']   # numeric order; not FY2000, a sub-project, FY1984 or one with a cost


class FakeSession:
    def __init__(self, data):
        self.data, self.bodies = data, []

    def post(self, url, json, headers, timeout):
        self.bodies.append(json)
        assert 'User-Agent' in headers and '@' not in headers['User-Agent']
        res = [self.data[str(i)] for i in json['criteria']['appl_ids'] if str(i) in self.data]
        return type('R', (), {'raise_for_status': lambda s: None, 'json': lambda s: {'meta': {'total': len(res)}, 'results': res}})()


def test_fetch_is_spaced_batched_resumable_and_builds_one_row_per_application(tmp_path, monkeypatch):
    m = load_script()
    monkeypatch.setattr(m, 'BATCH', 2)
    rep = {i: {'appl_id': int(i), 'fiscal_year': 1995, 'project_num': f'5R01CA0000{i}-05', 'subproject_id': None, 'award_amount': 1000 * int(i)}
           for i in ('11', '12', '13')}
    rep['14'] = {'appl_id': 14, 'fiscal_year': 1995, 'project_num': '5R01CA000014-05', 'subproject_id': None, 'award_amount': None}
    s, t, waits = FakeSession(rep), [100.0], []
    clock = lambda: t[0]
    def sleep(x): waits.append(x); t[0] += x
    cache = tmp_path / 'cache.jsonl'
    ids = ['11', '12', '13', '14', '15']
    m.fetch(ids[:3], cache, session=s, sleep=sleep, clock=clock)
    m.fetch(ids, cache, session=s, sleep=sleep, clock=clock)             # resumes: asks only 14 and 15
    assert [b['criteria']['appl_ids'] for b in s.bodies] == [[11, 12], [13], [14, 15]]
    assert all(b['criteria']['exclude_subprojects'] for b in s.bodies)
    # the fake clock stands still between requests, so each request after the first of a run waits the full interval; the first
    # request of a run (a new process) does not wait
    assert waits == [m.MIN_INTERVAL_S], waits
    cache.write_text(cache.read_text() + '{"asked": ["16"], "res')                  # a run killed mid-write
    m.fetch(ids + ['16'], cache, session=s, sleep=sleep, clock=clock)
    assert s.bodies[-1]['criteria']['appl_ids'] == [16] and len(cache.read_text().splitlines()) == 4
    _, results = m.read_cache(cache)
    df = m.build_rows(ids, results + [rep['11']], '2026-11-21')          # a repeated, agreeing answer counts once
    assert df.application_id.tolist() == ['11', '12', '13'] and df.award_amount.tolist() == [11000.0, 12000.0, 13000.0]
    with pytest.raises(ValueError, match='two different costs'):
        m.build_rows(ids, results + [dict(rep['11'], award_amount=1)], '2026-11-21')
