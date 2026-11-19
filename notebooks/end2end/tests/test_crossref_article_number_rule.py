#!/usr/bin/env python3
"""Offline checks for oxjob #1098: a work no record gives a page shows its Crossref article number as biblio.first_page.
No warehouse, no network.

Run from any directory: python3 notebooks/end2end/tests/test_crossref_article_number_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorksBase.ipynb'
TABLE = 'openalex.works.crossref_article_number_pages'


def cells():
    return [''.join(c['source']) for c in json.loads(NB.read_text())['cells']]


def kept(doi, article_number):
    """Python mirror of the cell's filter on one Crossref record that deposits no page."""
    an = (article_number or '').strip()
    if not an or len(an) > 15:
        return False
    key = lambda s: re.sub('[^a-z0-9]', '', s.lower())
    return key(an) not in (key(doi), key(doi[doi.index('/') + 1:]))


# Live examples, 19 Nov 2026 (#1098 README Log; paper trace run 5; bar on the publishers' own pages)
KEPT = [
    ('10.1002/aqc.70493', 'e70493'),              # W7225993773, Wiley's page citation_firstpage e70493
    ('10.1103/physrevd.102.094517', '094517'),    # W3016535260, APS
    ('10.1186/s40816-019-0108-0', '14'),          # W2924010296, BMC
    ('10.3389/frwa.2022.927113', '927113'),       # W4304166223, Frontiers
    ('10.1007/s43545-026-01901-y', '606'),        # W7224990638, Springer
    ('10.1002/qua.27401', 'e27401'),              # W4409485722, Wiley
    ('10.1128/spectrum.05292-22', 'e05292-22'),   # ASM's own form (PubMed: e0529222)
    ('10.1155/are/9932743', '9932743'),           # Hindawi article ID, the page's citation_firstpage
    ('10.1210/jendso/bvad114.2083', 'bvad114.2083'),  # J Endocr Soc supplement abstract, cited by this locator
    ('10.58530/2022/2842', '2842'),               # ISMRM program number
    ('10.1093/genetics/iyaf229', 'iyaf229'),      # OUP's page: schema.org pageStart iyaf229
    ('10.1093/petrology/egag059', 'egag059'),     # OUP's cite line: "Journal of Petrology, egag059"
]
SKIPPED = [
    ('10.1177/14634996261454512', '14634996261454512'),  # SAGE OnlineFirst: the DOI suffix again
    ('10.1002/chin.201352273', 'chin.201352273'),        # ChemInform: the DOI suffix again
    ('10.1002/pc.71836', 'pc.71836'),                    # Wiley early view: the DOI suffix again; the page never states it
    ('10.15252/embj.2021109958', 'EMBJ2021109958'),      # EMBO internal id = DOI suffix without the dot (PubMed: e109958)
    ('10.2337/dc24-2039', 'dc242039'),                   # ADA internal id (PubMed: 719-727)
    ('10.1128/msystems.00768-21', '10.1128/msystems.00768-21'),  # the whole DOI
    ('10.4271/2021-01-0082', '2021-01-0082'),            # SAE paper number = the DOI suffix: skipped (a known loss)
    ('10.1557/opl.2014.556', 'mrss14-1710-xx08-08'),     # over 15 characters: MRS internal id (page: 808)
    ('10.1063/pt.zthf.rvcc', '00000196470dd10fa1dff7aff6aa0000'),  # AIP hash
    ('10.1002/aqc.70493', '   '),                        # blank
]


def test_rule_on_examples():
    assert [d for d, an in KEPT if not kept(d, an)] == []
    assert [d for d, an in SKIPPED if kept(d, an)] == []


def test_cell_builds_the_table_with_the_rule():
    cell = next(c for c in cells() if f'CREATE OR REPLACE TABLE {TABLE}' in c)
    for line in [
        "  WHERE NULLIF(TRIM(page), '') IS NULL",
        "    AND NULLIF(TRIM(`article-number`), '') IS NOT NULL",
        "    AND LENGTH(TRIM(`article-number`)) <= 15",
        "    AND REGEXP_REPLACE(LOWER(TRIM(`article-number`)), '[^a-z0-9]', '') NOT IN (",
        "      REGEXP_REPLACE(LOWER(native_id), '[^a-z0-9]', ''),",
        "      REGEXP_REPLACE(LOWER(SUBSTR(native_id, INSTR(native_id, '/') + 1)), '[^a-z0-9]', ''))",
        "WHERE l.provenance = 'crossref' AND l.work_id IS NOT NULL",
        "GROUP BY l.work_id",
    ]:
        assert line in cell.split('\n'), line
    assert "ARRAY_CONTAINS(curated_fields, 'biblio.first_page')" in cell  # a page curation always wins
    assert 'LEFT ANTI JOIN' in cell


def test_main_statement_fills_only_an_empty_page():
    all_cells = cells()
    build = next(i for i, c in enumerate(all_cells) if f'CREATE OR REPLACE TABLE {TABLE}' in c)
    main = next(i for i, c in enumerate(all_cells) if 'openalex.works.openalex_works_base CLUSTER BY' in c)
    assert build < main  # the table is rebuilt before the main statement reads it
    src = all_cells[main]
    assert '      COALESCE(s.first_page, IF(s.last_page IS NULL, crn.article_number, NULL)) AS first_page, -- oxjob #1098' in src
    assert f'    LEFT JOIN {TABLE} crn ON crn.work_id = s.id' in src
    assert src.count('crn.') == 2  # read only by biblio.first_page (and the join)
    assert 'STRUCT(s.volume, s.issue, s.first_page, s.last_page) AS biblio' not in src


if __name__ == '__main__':
    test_rule_on_examples()
    test_cell_builds_the_table_with_the_rule()
    test_main_statement_fills_only_an_empty_page()
    print(f'ok: {len(KEPT)} kept, {len(SKIPPED)} skipped, notebook rule and fill in place')
