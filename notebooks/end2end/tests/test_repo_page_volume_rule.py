#!/usr/bin/env python3
"""Offline checks for oxjob #1098 step 2c: a work no record gives a volume takes the one the landing pages of its
repository records state in their citation_volume / citation_issue meta tags (CreateWorksBase cell
openalex.works.repo_page_volumes), fill-only in the main statement's biblio. No warehouse, no network.

The SQL regexes are read from the notebook itself and run here with Python's re (same syntax for these patterns), so
the cell and these checks can't drift apart.

Run from any directory: python3 notebooks/end2end/tests/test_repo_page_volume_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorksBase.ipynb'
TABLE = 'openalex.works.repo_page_volumes'


def cells():
    return [''.join(c['source']) for c in json.loads(NB.read_text())['cells']]


def table_cell():
    return next(c for c in cells() if 'CREATE OR REPLACE TABLE ' + TABLE in c)


def sql_regexes(cell):
    """Every regex literal in the cell's REGEXP_EXTRACT / REGEXP_REPLACE / RLIKE calls, SQL escapes undone."""
    lits = re.findall(r"(?:REGEXP_EXTRACT|REGEXP_REPLACE)\([^']*'((?:[^'\\]|\\.)*)'", cell)
    lits += re.findall(r"RLIKE '((?:[^'\\]|\\.)*)'", cell)
    return [l.replace('\\\\', '\\') for l in lits]


CELL = table_cell()
RX = sql_regexes(CELL)
NAME_RX = next(r for r in RX if 'name|property' in r)
CONTENT_RX = next(r for r in RX if 'content' in r)
VOL_RX = next(r for r in RX if r.startswith('^(0*[1-9][0-9]{0,3})'))
ISS_RX = next(r for r in RX if r.startswith('^(0*[1-9][0-9]{0,2}(?:'))
ISS_ZEROS_RX = next(r for r in RX if r.startswith('(?<![0-9])0+'))


def extract(rx, s):
    m = re.search(rx, s)
    return m.group(1) if m else ''


def tag(line):
    """The cell's (name, content) for one raw <meta> line."""
    return extract(NAME_RX, line).strip().lower(), ' '.join(extract(CONTENT_RX, line).split())


def volume(v):
    """The cell's volume rule: 1-4 digits (leading zeros dropped), not a year 19xx/20xx. An empty content never gets
    here (the cell keeps only tags with t.c <> '')."""
    if not v or extract(VOL_RX, v) != v:
        return None
    v = re.sub('^0+', '', v)
    return None if len(v) == 4 and re.match('^(19|20)', v) else v


def issue(i):
    """The cell's issue rule: 1-3 digits or a combined issue (3-4, 1/2), leading zeros dropped."""
    if not i or extract(ISS_RX, i) != i:
        return None
    return re.sub(ISS_ZEROS_RX, '', i)


# Raw meta lines as openalex.landing_page.meta_tags stores them (22 Nov 2026 samples: OJS, CERN CDS, IRIS)
LINES = [
    ('<meta name="citation_volume" content="73" />', ('citation_volume', '73')),
    ('<meta content="46" name="citation_volume" />', ('citation_volume', '46')),   # cds.cern.ch: content first
    ("<meta name='citation_issue' content='3-4'>", ('citation_issue', '3-4')),
    ('<meta property="citation_volume" content=" 12 ">', ('citation_volume', '12')),
    ('<meta name="citation_title" content="Water-stable coatings">', ('citation_title', 'Water-stable coatings')),
]
VOLUMES = [('12', '12'), ('05', '5'), ('146', '146'), ('1', '1'), ('1899', '1899'),
           ('0', None), ('2019', None), ('2003', None), ('12345', None), ('vol. 15, no. 2', None),
           ('XXXIV', None), ('8(6)', None), ('54a', None), ('N° 45', None), ('', None)]
ISSUES = [('3', '3'), ('03', '3'), ('3-4', '3-4'), ('1/2', '1/2'), ('01-02', '1-2'), ('100', '100'),
          ('0', None), ('2010', None), ('5636', None), ('Suppl 7', None), ('1 Pt 2', None), ('S7', None), ('', None)]


def test_meta_lines():
    assert [tag(line) for line, _ in LINES] == [want for _, want in LINES]


def test_volume_rule():
    assert [(v, volume(v)) for v, _ in VOLUMES] == VOLUMES


def test_issue_rule():
    assert [(i, issue(i)) for i, _ in ISSUES] == ISSUES


def test_main_statement_fills_only():
    main = next(c for c in cells() if 'CREATE OR REPLACE TABLE openalex.works.openalex_works_base' in c
                or 'LEFT JOIN openalex.works.crossref_article_numbers crn' in c)
    assert 'LEFT JOIN ' + TABLE + ' rpv ON rpv.work_id = s.id' in main
    assert ('COALESCE(s.volume, IF(s.issue IS NULL OR rpv.issue IS NULL OR s.issue = rpv.issue, rpv.volume, NULL)) AS volume'
            in main)
    assert 'COALESCE(s.issue, IF(s.volume IS NULL, rpv.issue, NULL)) AS issue' in main
    # the table is built before the statement that reads it
    order = cells()
    assert order.index(CELL) < order.index(main)


def test_empty_content_is_no_value():
    assert "t.n = 'citation_volume' AND t.c <> ''" in CELL and "t.n = 'citation_issue' AND t.c <> ''" in CELL


def test_curations_win():
    assert "ARRAY_CONTAINS(curated_fields, 'biblio.volume') OR ARRAY_CONTAINS(curated_fields, 'biblio.issue')" in CELL
    assert 'LEFT ANTI JOIN' in CELL


def test_no_dollar_in_sql():
    # oxjob #1932: a job run blanks $1 / $name in notebook SQL
    assert '$' not in CELL


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn()
            print('ok', name)
