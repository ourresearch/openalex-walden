"""#690 ticket 7682: ERC call identifiers (ERC-2021-ADG) mint no award under the EU programme funders.

The Crossref and Europe PMC doors carry one shared predicate on their minting view. These tests pin that both doors carry
the same text, that it sits on the minting view (never the junction, which CreateWorksEnriched reads for work funders),
and what the two patterns accept, mirrored in Python (Spark RLIKE is Java regex; these patterns use nothing Python reads
differently).
"""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
CROSSREF = ROOT / 'notebooks/awards/CreateCrossrefWorkFunders.ipynb'
EPMC = ROOT / 'notebooks/awards/CreateEuropePmcWorkFunders.py'
BACKFILL = ROOT / 'notebooks/awards/CreateBackfillAwards.ipynb'

FAMILY = r'^(European Commission|European Research Council|Seventh Framework Programme|Horizon 2020( Framework Programme)?|(FP7|H2020|HORIZON EUROPE) .+)$'
CALL = r'^ERC[-_ ]?(19|20)[0-9]{2}[-_ ]?(STG|COG|ADG|SYG|POC[0-9]?|POC[-_ ]?(LS|SM)[0-9]?)$'


def predicate_lines(text):
    start = text.index('-- #690 ticket 7682')
    return [l for l in text[start:].split('\n')[:8]]


def crossref_minting_cell():
    cells = [''.join(c['source']) for c in json.loads(CROSSREF.read_text())['cells']]
    found = [c for c in cells if 'CREATE OR REPLACE TEMPORARY VIEW crossref_work_funder_awards' in c]
    assert len(found) == 1
    return found[0]


def epmc_minting_cell():
    text = EPMC.read_text()
    start = text.index('CREATE OR REPLACE TEMPORARY VIEW europepmc_work_funder_awards')
    end = text.index('# COMMAND ----------', start)
    return '\n'.join(l.removeprefix('# MAGIC ').removeprefix('# MAGIC') for l in text[start:end].split('\n'))


def test_both_doors_carry_the_same_predicate_on_the_minting_view():
    cr, ep = crossref_minting_cell(), epmc_minting_cell()
    assert predicate_lines(cr) == [l.rstrip(';') for l in predicate_lines(ep)]
    for cell in (cr, ep):
        assert cell.count('-- #690 ticket 7682') == 1
        assert f"coalesce(f.display_name, '') RLIKE '{FAMILY}'" in cell
        assert f"RLIKE '{CALL}')" in cell
        assert cell.index('JOIN funders f ON f.funder_id = e.funder_id') < cell.index('WHERE NOT (coalesce(f.display_name')


def backfill_insert_cell():
    cells = [''.join(c['source']) for c in json.loads(BACKFILL.read_text())['cells']]
    found = [c for c in cells if 'INSERT INTO openalex.awards.openalex_awards_raw' in c]
    assert len(found) == 1
    return found[0]


def test_backfill_door_carries_the_same_patterns():
    # the frozen crossref_work.grants door (no schedule) still held 203 call-code rows on 17 Nov; a rerun must not restore them
    cell = backfill_insert_cell()
    assert cell.count('-- #690 ticket 7682') == 1
    assert f"coalesce(f.display_name, '') RLIKE '{FAMILY}'" in cell
    assert f"RLIKE '{CALL}');" in cell
    assert "regexp_replace(b.funder_award_id, '[\\\\u2010-\\\\u2015\\\\u2212\\\\uFE58\\\\uFE63\\\\uFF0D]', '-')" in cell
    # a LEFT JOIN, so a funder missing from funders keeps its rows; the guard is ANDed after the verdict guard
    assert 'LEFT JOIN openalex.funders.funders f ON f.funder_id = b.funder_id' in cell
    assert cell.index("WHERE COALESCE(g.decision, 'mint') <> 'suppress'") < cell.index('AND NOT (coalesce(f.display_name')
    # the source-wide DELETE before the insert is unchanged
    assert "WHERE provenance = 'crossref_work.grants' AND priority IN (0, 2);" in cell


def test_junctions_are_untouched():
    # the junction tables feed CreateWorksEnriched (work -> funder); the call codes leave only the award minting
    assert 'ticket 7682' not in ''.join(c for c in EPMC.read_text().split('CREATE OR REPLACE TABLE openalex.awards.europepmc_work_funders')[1].split('# COMMAND ----------')[0])


def norm(s):
    s = re.sub('[‐-―−﹘﹣－]', '-', s)
    return re.sub(r'\s+', ' ', s.strip().upper())


@pytest.mark.parametrize('aid', ['ERC-2021-ADG', 'ERC-2015-AdG', 'erc-2017-stg', 'ERC‐2021‐ADG', 'ERC 2019 StG',
                                 'ERC-2018-SyG', 'ERC-2014-PoC', 'ERC-2022-POC2', 'ERC-2011-POC-LS', 'ERC_2016_COG', 'ERC2020COG'])
def test_call_codes_match(aid):
    assert re.search(CALL, norm(aid))


@pytest.mark.parametrize('aid', ['101052444', '725136', 'ERC', 'ERC-StG', 'ERC 101002724', 'ERC-2021-ADG 101052444',
                                 'H2020-ERC-2021-ADG', 'ERC-2021-ADG-101052444', 'GAP-101053352', '2021-ADG'])
def test_grant_numbers_and_mixed_strings_do_not_match(aid):
    assert not re.search(CALL, norm(aid))


@pytest.mark.parametrize('name,family', [('European Research Council', True), ('European Commission', True),
                                         ('H2020 European Research Council', True), ('HORIZON EUROPE Framework Programme', True),
                                         ('FP7 Ideas: European Research Council', True), ('Horizon 2020', True),
                                         ('Horizon 2020 Framework Programme', True), ('Seventh Framework Programme', True),
                                         ('European Resuscitation Council', False), ('European Regional Development Fund', False),
                                         ('Fonds Wetenschappelijk Onderzoek', False), ('National Science Foundation', False)])
def test_family(name, family):
    assert bool(re.search(FAMILY, name)) is family
