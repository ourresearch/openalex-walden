#!/usr/bin/env python3
"""Offline checks for oxjob #1803: CreateWorkAuthorships drops a same-name pair's institution that a seat gets only from
bare-name pieces ("Northeastern University") when another string on the seat gives the other school of that name.
No warehouse, no network.

Run from any directory: python3 notebooks/end2end/tests/test_seat_namesake_bare_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorkAuthorships.ipynb'
DROPS = 'openalex.works.seat_namesake_bare_drops'

# Listed pairs (both directions): Northeastern Shenyang / Boston, Korea Kodaira / Seoul, Western Phnom Penh and
# Kanchanaburi / London, Ontario.
PAIRS = {(9224756, 12912129), (4210161052, 197347611), (4210116551, 125749732), (4405277831, 125749732)}


def norm(s):
    """Python mirror of the SQL bare-name key: lowercase, 'univ'/'univ.' -> 'university', letters only, single spaces."""
    s = re.sub(r'\buniv\b\.?', 'university', s.lower())
    return re.sub(r' +', ' ', re.sub(r'[^a-z]+', ' ', s).strip())


BARE = ['Northeastern University', 'northeastern Univ', 'Northeastern Univ.', '(Northeastern University),',
        'NORTHEASTERN UNIVERSITY', ' Northeastern University; ', 'Northeastern University 2',  # digits (footnote marks) drop
        'Korea University', 'Western University']
NOT_BARE = ['School of Metallurgy, Northeastern University, Shenyang, China', 'Northeastern University, Boston, MA',
            'Northeastern University (USA)', 'Northeastern University at Qinhuangdao', 'Korea University, Seoul',
            'Western University, Cambodia', 'Northeastern Universe', 'Northeastern University CHINA']


def cells():
    return [''.join(c['source']) for c in json.loads(NB.read_text())['cells']]


def test_examples():
    names = {'northeastern university', 'korea university', 'western university'}
    for s in BARE:
        assert norm(s) in names, s
    for s in NOT_BARE:
        assert norm(s) not in names, s


def test_notebook_carries_the_rule():
    cs = cells()
    i = next(k for k, c in enumerate(cs) if c.startswith('-- oxjob #1803') and f'CREATE OR REPLACE TABLE {DROPS} AS' in c)
    refresh = next(k for k, c in enumerate(cs) if 'REFRESH MATERIALIZED VIEW openalex.works.work_author_affiliations_mv' in c)
    main = next(k for k, c in enumerate(cs) if c.startswith('CREATE OR REPLACE TABLE openalex.works.work_authorships'))
    assert refresh < i < main  # reads the refreshed MV, written before the main statement reads it
    rule = cs[i]
    pairs = {tuple(int(x) for x in m) for m in re.findall(r'\((\d+), (\d+)\)', rule)}
    assert pairs == PAIRS | {(b, a) for a, b in PAIRS}  # every pair, both directions
    assert "'\\\\buniv\\\\b\\\\.?', 'university'" in rule and "'[^a-z]+', ' '" in rule
    assert 'WHERE a.only_bare AND NOT b.only_bare' in rule
    m = cs[main]
    assert m.count(DROPS) == 2
    assert f'LEFT ANTI JOIN {DROPS} nb' in m  # step 1: the seat's institutions
    assert 'CASE WHEN nb.work_id IS NULL THEN w0.institution_id END AS institution_id' in m  # step 3: the string's ids


if __name__ == '__main__':
    test_examples()
    test_notebook_carries_the_rule()
    print('ok')
