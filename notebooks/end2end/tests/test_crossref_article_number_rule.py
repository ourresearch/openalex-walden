#!/usr/bin/env python3
"""Offline checks for oxjob #1098: a work no record gives a page shows its Crossref article number as biblio.first_page,
from DOI prefixes whose numbers PubMed prints as the page. No warehouse, no network.

Run from any directory: python3 notebooks/end2end/tests/test_crossref_article_number_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorksBase.ipynb'
TABLE = 'openalex.works.crossref_article_numbers'


def cells():
    return [''.join(c['source']) for c in json.loads(NB.read_text())['cells']]


def repeats_doi(doi, article_number):
    """Python mirror of the cell's per-record skip: the number is the DOI or its suffix on letters and digits."""
    key = lambda s: re.sub('[^a-z0-9]', '', s.lower())
    return key(article_number.strip()) in (key(doi), key(doi[doi.index('/') + 1:]))


def same_number(pubmed_page, article_number):
    """Python mirror of the PubMed comparison: letters and digits only, leading letters and zeros dropped."""
    key = lambda s: re.sub('^[a-z]*0*', '', re.sub('[^a-z0-9]', '', s.lower()))
    return key(pubmed_page) == key(article_number)


# Live examples, 19 Nov 2026 (#1098 README Log; paper trace run 5; bar on the publishers' own pages)
KEPT = [
    ('10.1002/aqc.70493', 'e70493'),              # W7225993773, Wiley's page citation_firstpage e70493
    ('10.1103/physrevd.102.094517', '094517'),    # W3016535260, APS
    ('10.3389/frwa.2022.927113', '927113'),       # W4304166223, Frontiers
    ('10.1007/s43545-026-01901-y', '606'),        # W7224990638, Springer
    ('10.1155/are/9932743', '9932743'),           # Hindawi article ID, the page's citation_firstpage
    ('10.1093/genetics/iyaf229', 'iyaf229'),      # OUP's page: schema.org pageStart iyaf229
    ('10.1128/genomea.01111-13', 'e01111-13'),    # ASM's own form
]
REPEATS = [
    ('10.1177/14634996261454512', '14634996261454512'),  # SAGE OnlineFirst: the DOI suffix again
    ('10.1002/chin.201352273', 'chin.201352273'),        # ChemInform
    ('10.1002/pc.71836', 'pc.71836'),                    # Wiley early view; the page never states it
    ('10.15252/embj.2021109958', 'EMBJ2021109958'),      # EMBO internal id = DOI suffix without the dot
    ('10.1128/msystems.00768-21', '10.1128/msystems.00768-21'),  # the whole DOI
]
# PubMed's page vs the Crossref number on the same work: same number in another form, and a different one
SAME = [('e2400819', '2400819'), ('e0111113', 'e01111-13'), ('3000605241281874', '03000605241281874'), ('E40', 'e40')]
DIFFERENT = [('3906', 'jeb.085399'), ('141', '130208'), ('549', '14')]


def test_doi_repeats_are_skipped():
    assert [d for d, an in KEPT if repeats_doi(d, an)] == []
    assert [d for d, an in REPEATS if not repeats_doi(d, an)] == []


def test_pubmed_comparison():
    assert all(same_number(p, an) for p, an in SAME)
    assert not any(same_number(p, an) for p, an in DIFFERENT)


def test_cell_builds_the_table_with_the_rule():
    cell = next(c for c in cells() if f'CREATE OR REPLACE TABLE {TABLE}' in c)
    lines = cell.split('\n')
    for line in [
        "  JOIN openalex.works.locations_mapped_curated l ON l.provenance = 'crossref' AND l.native_id = c.native_id",
        "    ON p.work_id = n.work_id AND p.provenance = 'pubmed' AND NULLIF(TRIM(p.first_page), '') IS NOT NULL",
        "  HAVING COUNT(*) >= 50",
        "    AND AVG(IF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(p.first_page), '[^a-z0-9]', ''), '^[a-z]*0*', '')",
        "             = REGEXP_REPLACE(REGEXP_REPLACE(LOWER(n.article_number), '[^a-z0-9]', ''), '^[a-z]*0*', ''), 1, 0)) >= 0.8",
        "JOIN pubmed_vouched v ON v.prefix = n.prefix",
        "WHERE n.page IS NULL",
        "  AND REGEXP_REPLACE(LOWER(n.article_number), '[^a-z0-9]', '') NOT IN (",
        "GROUP BY n.work_id",
    ]:
        assert line in lines, line
    assert "ARRAY_CONTAINS(curated_fields, 'biblio.first_page')" in cell  # a page curation always wins


def test_main_statement_fills_only_an_empty_page():
    all_cells = cells()
    build = next(i for i, c in enumerate(all_cells) if f'CREATE OR REPLACE TABLE {TABLE}' in c)
    main = next(i for i, c in enumerate(all_cells) if 'openalex.works.openalex_works_base CLUSTER BY' in c)
    assert build < main  # the table is rebuilt before the main statement reads it
    src = all_cells[main]
    assert '      COALESCE(s.first_page, IF(s.last_page IS NULL, crn.article_number, NULL)) AS first_page, -- oxjob #1098' in src
    assert f'    LEFT JOIN {TABLE} crn ON crn.work_id = s.id' in src
    assert src.count('crn.') == 2  # read only by biblio.first_page (and the join)


if __name__ == '__main__':
    test_doi_repeats_are_skipped()
    test_pubmed_comparison()
    test_cell_builds_the_table_with_the_rule()
    test_main_statement_fills_only_an_empty_page()
    print(f'ok: {len(KEPT)} kept, {len(REPEATS)} DOI repeats, PubMed comparison, notebook rule and fill in place')
