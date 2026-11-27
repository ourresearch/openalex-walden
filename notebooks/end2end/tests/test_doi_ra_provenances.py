#!/usr/bin/env python3
"""Offline checks for oxjob #1776 Step 0 (C1): one list of DOI registration-agency provenances.

Every quoted Crossref/DataCite pair in notebooks/ and libraries/ is marked as DOI_RA_PROVENANCES
(notebooks/utils/doi_ra_provenances.py) or listed there as Crossref/DataCite only, and every marked site holds exactly the
list, so adding an RA (mEDRA, JaLC, KISTI) is one edit plus `--write`. No warehouse, no network.

Run from any directory: python3 notebooks/end2end/tests/test_doi_ra_provenances.py   (also collected by pytest)
"""
import importlib.util
import sys
from pathlib import Path

MOD = Path(__file__).resolve().parents[2] / "utils" / "doi_ra_provenances.py"
spec = importlib.util.spec_from_file_location("doi_ra_provenances", MOD)
D = importlib.util.module_from_spec(spec)
spec.loader.exec_module(D)


def test_every_pair_marked_or_listed():
    assert D.check() == []


def test_marked_sites_exist():
    marked = [s for p in D.files() for s in D.sites(p.read_text(encoding="utf-8")) if s[1]]
    assert len(marked) >= 30, len(marked)


def test_rewrite_is_a_noop_today():
    for p in D.files():
        text = p.read_text(encoding="utf-8")
        assert D.rewrite(text) == text, p


def test_rewrite_adds_an_ra_and_keeps_tails():
    old = D.DOI_RA_PROVENANCES
    D.DOI_RA_PROVENANCES = ("crossref", "datacite", "medra")
    try:
        cases = {
            "WHERE l.provenance IN (/*DOI_RA*/ 'crossref', 'datacite') AND x":
                "WHERE l.provenance IN (/*DOI_RA*/ 'crossref', 'datacite', 'medra') AND x",
            "MIN(CASE WHEN provenance NOT IN (/*DOI_RA*/ 'crossref', 'datacite', 'pubmed', 'mag')":
                "MIN(CASE WHEN provenance NOT IN (/*DOI_RA*/ 'crossref', 'datacite', 'medra', 'pubmed', 'mag')",
            '    return (p, n.lower() if p in ("crossref", "datacite") else n)  # DOI_RA':
                '    return (p, n.lower() if p in ("crossref", "datacite", "medra") else n)  # DOI_RA',
            'F.col("provenance").isin("crossref", "datacite") & (F.col("ns") == "doi"),  # DOI_RA':
                'F.col("provenance").isin("crossref", "datacite", "medra") & (F.col("ns") == "doi"),  # DOI_RA',
            "AND a.provenance IN ('crossref', 'datacite')": "AND a.provenance IN ('crossref', 'datacite')",  # unmarked: untouched
        }
        for before, after in cases.items():
            assert D.rewrite(before) == after, before
            assert D.rewrite(after) == after, after  # idempotent
    finally:
        D.DOI_RA_PROVENANCES = old


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
    print("ok")
    sys.exit(0)
