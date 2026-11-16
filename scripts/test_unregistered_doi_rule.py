#!/usr/bin/env python3
"""Offline checks for oxjob #1656: CreateLocationsMapped hides a DOI listed in openalex.works.unregistered_dois on
non-Crossref/DataCite records, and BuildUnregisteredDois compiles. No warehouse, no network.

Run from any directory: python3 scripts/test_unregistered_doi_rule.py   (also collected by pytest)
"""
import json
import py_compile
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / 'notebooks/end2end/CreateLocationsMapped.ipynb'
BUILD = ROOT / 'notebooks/maintenance/BuildUnregisteredDois.py'


def mapped_cell():
    return next(''.join(c['source']) for c in json.loads(NB.read_text())['cells']
                if 'CREATE OR REPLACE TABLE openalex.works.locations_mapped\n' in ''.join(c['source']))


def test_case_hides_listed_doi():
    cell = mapped_cell()
    i = cell.index("CASE WHEN r.work_id_source = 'oxjob1903_nodoi' THEN NULL")
    case_sql = cell[i:cell.index('END AS best_doi', i)]
    assert 'WHEN ud.doi IS NOT NULL THEN NULL -- oxjob #1656' in case_sql


def test_join_never_touches_registrations():
    cell = mapped_cell()
    m = re.search(r"LEFT JOIN \(SELECT doi FROM openalex\.works\.unregistered_dois\) ud[^\n]*\n(\s+--[^\n]*\n)?"
                  r"\s+ON\s+t\.provenance NOT IN \('crossref', 'datacite'\)\n\s+AND lower\(t\.best_doi\) = ud\.doi\n", cell)
    assert m, 'ud join with the provenance guard'
    assert cell.count('unregistered_dois) ud ') == 1 and cell.count('ud.doi') == 2  # one join; its ON clause and the CASE
    assert 'SELECT /*+ BROADCAST(ud) */' in cell  # the 3B-row side never shuffles on best_doi


def test_no_semicolon_in_new_lines():
    cell = mapped_cell()
    block = cell[cell.index('-- oxjob #1656: a DOI'):cell.index('END AS best_doi')]
    assert ';' not in block  # a literal semicolon would split the %sql cell


def test_build_notebook_compiles():
    py_compile.compile(str(BUILD), doraise=True)


if __name__ == '__main__':
    test_case_hides_listed_doi()
    test_join_never_touches_registrations()
    test_no_semicolon_in_new_lines()
    test_build_notebook_compiles()
    print('ok')
