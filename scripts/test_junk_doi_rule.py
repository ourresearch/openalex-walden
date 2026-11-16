#!/usr/bin/env python3
"""Offline checks for oxjob #1654: CreateLocationsMapped hides a DOI a record only carries when it names no work.
No warehouse, no network.

Run from any directory: python3 scripts/test_junk_doi_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / 'notebooks/end2end/CreateLocationsMapped.ipynb'
B = '\\'  # one backslash, as chr(92) is in the SQL

# The rule exactly as the notebook must carry it (Python mirror below; Java and Python regex agree on this subset).
RULE_LINES = [
    "     WHEN t.provenance NOT IN ('crossref', 'datacite')",
    "          AND (t.best_doi RLIKE '(?i)^10[.]13039/|%(?![0-9a-f]{2})'",
    "               OR (t.best_doi RLIKE '^10[.][0-9]+/10[.][0-9]+/?$'",
    "                   AND substring_index(t.best_doi, '/', 1) = regexp_extract(t.best_doi, '^10[.][0-9]+/(10[.][0-9]+)/?$', 1))",
    "               OR t.best_doi RLIKE concat(chr(92), chr(92), '[nt](?!(', chr(92), chr(92), '[nrt])*$)')) THEN NULL -- oxjob #1654",
]


def hides(doi):
    """Python mirror of the oxjob #1654 WHEN line, for a non-Crossref/DataCite record."""
    if re.search(r'(?i)^10[.]13039/|%(?![0-9a-f]{2})', doi):
        return True
    m = re.search(r'^10[.][0-9]+/(10[.][0-9]+)/?$', doi)
    if m and doi.split('/', 1)[0] == m.group(1):
        return True
    return re.search(B * 2 + '[nt](?!(' + B * 2 + '[nrt])*$)', doi) is not None


# Live examples, 16 Nov 2026 (#1654 README Log)
NOT_A_WORK = [
    '10.13039/501100010198',               # W2922135437, DIGITAL.CSIC dc:identifier (ZD 7352)
    '10.13039/501100000780',               # European Commission
    '10.5007/%x',                          # OJS placeholder
    '10.4102/safp.v%vi%i.6326',
    '10.2533/chimia.2024.%s',
    '10.30631/psej.6.1.%f',                # '%f' + end: one hex digit is not a pair
    '10.12739/10.12739',
    '10.1007/s11269-010-9632-x.' + B + 'n8.' + B + 't鄭嘉盈，1999',  # W6903033017 (ZD 8265): literal backslash escapes
    '10.1501/tite_0000000064&#13;' + B + 'nbaker',
    '10.1515/ijmr-2020-8106&#13;' + B + 'n&#13;' + B + 'n',
]
KEPT = [
    '10.1038/nature11247',
    '10.1121/10.0005480',                  # JASA: a real DOI whose suffix starts with 10.
    '10.1109/10.1373',                     # IEEE TBME: real
    '10.25185/10.5',
    '10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-x',
    '10.1000/abc%2fdef',                   # percent-encoded: a hex pair
    '10.12739/nwsa.2013.8.2.2c0085',
    '10.1017/s1755691021000153',
    '10.6028/jres.098.001' + B + 'n',      # lone trailing escape: the record's own DOI (suffix-strip step, not this rule)
    '10.9789/2175-5361.2014.v6i4.1525-1533' + B + 't',
    '10.1007/978-3-642-21254-3' + B + '_13',  # inline escape (HAL LaTeX): unescaping is the suffix-strip step
    '10.1097/01.rct.0000185385.35389' + B + '.cd',
]


def best_doi_case():
    cell = next(''.join(c['source']) for c in json.loads(NB.read_text())['cells']
                if 'CREATE OR REPLACE TABLE openalex.works.locations_mapped\n' in ''.join(c['source']))
    i = cell.index("CASE WHEN r.work_id_source = 'oxjob1903_nodoi' THEN NULL")
    return cell[i:cell.index('END AS best_doi', i)]


def test_notebook_carries_the_rule():
    case_sql = best_doi_case()
    for line in RULE_LINES:
        assert line in case_sql, line
    assert B not in case_sql  # no SQL escapes: chr(92) builds the backslash
    assert ';' not in case_sql  # a literal semicolon would split the %sql cell


def test_examples():
    for d in NOT_A_WORK:
        assert hides(d), d
    for d in KEPT:
        assert not hides(d), d


if __name__ == '__main__':
    test_notebook_carries_the_rule()
    test_examples()
    print('ok')
