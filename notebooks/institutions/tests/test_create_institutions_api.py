"""Offline checks for CreateInstitutionsApi's grants columns, awards_count and awards_api_url (no warehouse access).

The notebook's own SQL runs on DuckDB through sqlglot (Spark -> DuckDB), against a few hand-made awards.
Run: uv run --with pytest --with sqlglot --with duckdb python -m pytest -q notebooks/institutions/tests
"""
import json
import re
from pathlib import Path

import duckdb
import sqlglot

NB = Path(__file__).resolve().parents[1] / 'CreateInstitutionsApi.ipynb'
CELLS = [''.join(c['source']) for c in json.loads(NB.read_text(encoding='utf-8'))['cells']]
TERM = 'CASE WHEN COALESCE(awards_count, 0) > 0 THEN CAST(awards_count AS STRING) END'


def cell(tag):
    hits = [s for s in CELLS if tag in s]
    assert len(hits) == 1, tag
    return hits[0]


BUILD = cell('CREATE OR REPLACE TABLE openalex.institutions.institutions_api AS')
SNAPSHOT = cell('CREATE OR REPLACE TABLE openalex.institutions.institutions_api_hash AS')
COMPARE = cell('MERGE INTO openalex.institutions.institutions_api AS target')


def hashed_fields(s):
    """The CONCAT_WS arguments of a hash cell, comments and spacing removed."""
    body = s[s.index("CONCAT_WS('|',") + len("CONCAT_WS('|',"):s.index(')) AS content_hash')]
    body = re.sub(r'--[^\n]*', '', body)
    return [' '.join(x.split()) for x in body.split(',\n') if x.strip()]


def awards_agg_sql():
    start = BUILD.index('awards_agg AS (')
    end = BUILD.index('\n),', start) + 2
    return BUILD[start:end]


def served_columns(alias='aw'):
    """The two SELECT lines that serve the count and the link, and the join that feeds them."""
    lines = [l.strip().rstrip(',') for l in BUILD.splitlines() if l.strip().endswith(('AS awards_count,', 'AS awards_api_url,'))]
    join = [l.strip() for l in BUILD.splitlines() if l.strip().startswith(f'LEFT JOIN awards_agg {alias} ')]
    assert len(lines) == 2 and len(join) == 1, (lines, join)
    return lines, join[0]


def db_with_awards(rows):
    db = duckdb.connect()
    db.execute("ATTACH ':memory:' AS openalex")
    db.execute('CREATE SCHEMA openalex.awards')
    db.execute('CREATE TABLE openalex.awards.awards_api (id BIGINT, institution_awarded STRUCT(id VARCHAR, display_name VARCHAR)[])')
    for award_id, inst_ids in rows:
        arr = None if inst_ids is None else [{'id': f'https://openalex.org/I{i}', 'display_name': f'I{i}'} for i in inst_ids]
        db.execute('INSERT INTO openalex.awards.awards_api VALUES (?, ?)', [award_id, arr])
    return db


def duck(sql):
    return sqlglot.transpile(sql, read='spark', write='duckdb')[0]


def test_both_hash_cells_hash_the_same_fields():
    snap, comp = hashed_fields(SNAPSHOT), hashed_fields(COMPARE)
    assert snap == comp, (snap, comp)
    assert snap[-1] == TERM


def test_awards_count_counts_each_award_once_per_institution():
    db = db_with_awards([(1, [10, 20]), (2, [10]), (3, []), (4, None), (5, [10, 10])])
    got = db.execute(duck(f'WITH {awards_agg_sql()} SELECT institution_id, awards_count FROM awards_agg ORDER BY 1')).fetchall()
    assert got == [(10, 3), (20, 1)]


def test_served_columns_zero_and_link_for_every_institution():
    db = db_with_awards([(1, [10, 20]), (2, [10])])
    db.execute('CREATE TABLE institutions (id BIGINT)')
    db.execute('INSERT INTO institutions VALUES (10), (20), (30)')
    lines, join = served_columns()
    sql = f"WITH {awards_agg_sql()} SELECT i.id, {', '.join(lines)} FROM institutions i {join} ORDER BY 1"
    got = db.execute(duck(sql)).fetchall()
    url = 'https://api.openalex.org/awards?filter=institution_awarded.id:I'
    assert got == [(10, 2, f'{url}10'), (20, 1, f'{url}20'), (30, 0, f'{url}30')]


def test_institutions_without_awards_keep_their_hash():
    db = duckdb.connect()
    term = duck(f'SELECT {TERM} FROM t').split('SELECT ', 1)[1].rsplit(' FROM t', 1)[0]
    rows = db.execute(f"""SELECT awards_count, CONCAT_WS('|', 'I1', 'Harvard', {term}) = CONCAT_WS('|', 'I1', 'Harvard')
        FROM (VALUES (NULL), (0), (3)) v(awards_count) ORDER BY awards_count NULLS FIRST""").fetchall()
    assert rows == [(None, True), (0, True), (3, False)]


def test_the_link_filters_the_field_the_count_reads():
    lines, _ = served_columns()
    link = next(l for l in lines if l.endswith('AS awards_api_url'))
    key = re.search(r'awards\?filter=([a-z_.]+):I', link).group(1)
    assert key == 'institution_awarded.id'
    assert 'LATERAL VIEW EXPLODE(a.institution_awarded) t AS x' in awards_agg_sql() and 'x.id' in awards_agg_sql()
