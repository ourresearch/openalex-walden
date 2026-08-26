"""Tests for extract_ids (oxjob #881 extraction, oxjob #880 P3 doi: rule)."""

from openalex.dlt.repo_ids import extract_ids


def _ids(results, ns):
    return [r["id"] for r in results if r["namespace"] == ns]


def test_doi_prefixed_native_id_is_read_as_self_doi():
    # oxjob #880 P3 -- the Open MIND shape
    r = extract_ids([], "doi:10.57451/lhd.a.cxsmap7_nustar.101329.1")
    assert _ids(r, "doi") == ["10.57451/lhd.a.cxsmap7_nustar.101329.1"]
    d = [x for x in r if x["namespace"] == "doi"][0]
    assert d["relationship"] == "self"
    # native_id still recorded as pmh self alongside
    assert _ids(r, "pmh") == ["doi:10.57451/lhd.a.cxsmap7_nustar.101329.1"]


def test_doi_rule_is_anchored_not_a_substring_match():
    # a doi buried mid-string must NOT be read -- the reference-DOI trap (evidence/q40)
    r = extract_ids([], "oai:site.org:something/doi:10.1016/j.fake.1")
    assert _ids(r, "doi") == []


def test_dc_identifier_doi_wins_no_duplicate():
    # 444,188 records carry the DOI in dc:identifier too -- must not end up with two doi entries
    r = extract_ids(["https://doi.org/10.57451/xyz.1"], "doi:10.57451/xyz.1")
    assert _ids(r, "doi") == ["10.57451/xyz.1"]


def test_arxiv_native_id_does_not_mint_a_doi():
    # the rule removed 2025-04-23 fabricated 10.48550/arxiv.* DOIs -- must stay dead
    r = extract_ids([], "oai:arXiv.org:2101.00001")
    assert _ids(r, "doi") == []
    assert _ids(r, "pmh") == ["oai:arXiv.org:2101.00001"]


def test_arxiv_abs_url_still_extracts_with_self_relationship():
    r = extract_ids(["https://arxiv.org/abs/2101.00001"], "oai:arXiv.org:2101.00001")
    a = [x for x in r if x["namespace"] == "arxiv"]
    assert a and a[0]["id"] == "arXiv:2101.00001" and a[0]["relationship"] == "self"


def test_pmid_and_pmcid_patterns_present():
    # RepoBackfill's drifted copy had lost these -- the shared module restores them there
    r = extract_ids(["https://www.ncbi.nlm.nih.gov/pubmed/12345",
                     "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC67890"], "oai:x:1")
    assert _ids(r, "pmid") == ["12345"]
    assert _ids(r, "pmcid") == ["PMC67890"]


def test_null_inputs_are_safe():
    assert extract_ids(None, None) == []
    assert extract_ids([], None) == [] or extract_ids([], None)[0]["namespace"] == "pmh"
