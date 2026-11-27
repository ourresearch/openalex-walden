"""Regression tests for the shared repo type vocabulary (oxjob #881).

These lock in the behaviour that was previously copy-pasted between Repo.py and
RepoBackfill.py. The maps stayed in sync by luck; the filters built on them drifted. If someone
edits one of these lists again, this is what catches it.
"""

import pytest

from openalex.dlt import repo_types as rt


def test_types_to_delete_is_lowercase_and_deduped():
    # the filter compares against LOWER(raw_native_type); an uppercase entry silently never matches
    assert all(t == t.lower() for t in rt.TYPES_TO_DELETE)
    assert len(rt.TYPES_TO_DELETE) == len(set(rt.TYPES_TO_DELETE))


def test_types_to_delete_keeps_the_known_members():
    for t in ("person", "image", "newspaper", "photograph", "photographs", "still image"):
        assert t in rt.TYPES_TO_DELETE


def test_best_type_is_order_independent():
    """oxjob #537: pick the most informative element, not the first.

    repo_works_backfill still carries first-element types because it was built before this
    landed -- 84.3% of its multi-valued dc:type rows (oxjob #881 q59).
    """
    a = rt.best_raw_and_type(["Text", "info:eu-repo/semantics/article"])
    b = rt.best_raw_and_type(["info:eu-repo/semantics/article", "Text"])
    assert a == b
    assert a[0] == "info:eu-repo/semantics/article"


def test_best_type_handles_empty_and_null():
    assert rt.best_raw_and_type(None) == (None, "other")
    assert rt.best_raw_and_type([]) == (None, "other")


def test_specific_beats_generic():
    raw, mapped = rt.best_raw_and_type(["Text", "doctoral thesis"])
    assert mapped == "dissertation"


@pytest.mark.parametrize("value,expected", [("Text", "article"), ("image", "other")])
def test_single_element_mapping(value, expected):
    assert rt.best_raw_and_type([value])[1] == expected


# oxjob #758: a COAR resource type must not lose to the record's COAR version marker. UARM's DSpace 7 feed sends
# [bachelor thesis, version of record] on its theses; c_7a1f was missing from COAR_RESOURCE_TYPE_MAP, so it ranked
# 'other', the version URI ('article') won, and the theses were typed article downstream.
VOR = "http://purl.org/coar/version/c_970fb48d4fbd8a85"


@pytest.mark.parametrize("code,expected", [
    ("c_7a1f", "dissertation"),   # bachelor thesis
    ("c_bdcc", "dissertation"),   # master thesis
    ("c_db06", "dissertation"),   # doctoral thesis
    ("c_93fc", "report"),         # report
    ("c_18ws", "report"),         # research report
    ("c_18gh", "report"),         # technical report
])
def test_coar_resource_type_beats_version_marker(code, expected):
    rtype = f"http://purl.org/coar/resource_type/{code}"
    for arr in ([rtype, VOR], [VOR, rtype]):
        assert rt.best_raw_and_type(arr) == (rtype, expected)


def test_review_article_code_does_not_decide():
    # Oxford ORA sends COAR 'review article' on ~108,000 ordinary journal articles: the text type decides there
    raw, mapped = rt.best_raw_and_type(["Journal article", "http://purl.org/coar/resource_type/c_dcae04bc", "submittedVersion"])
    assert mapped == "article"
    assert "c_dcae04bc" not in raw


def test_uarm_bachelor_thesis_record():
    # repositorio.uarm.edu.pe hdl 20.500.12833/1941's dc:type, as harvested 25 Nov 2026
    raw, mapped = rt.best_raw_and_type(["http://purl.org/coar/resource_type/c_7a1f", VOR])
    assert raw == "http://purl.org/coar/resource_type/c_7a1f"
    assert mapped == "dissertation"


def test_version_marker_alone_still_article():
    assert rt.best_raw_and_type([VOR]) == (VOR, "article")


def test_coar_article_with_version_keeps_article():
    # a journal article with its version marker: unchanged, the resource type wins the article tie on source quality
    raw, mapped = rt.best_raw_and_type(["http://purl.org/coar/resource_type/c_6501", VOR])
    assert (raw, mapped) == ("http://purl.org/coar/resource_type/c_6501", "article")


def test_padded_coar_string_does_not_beat_clean_eu_repo_tie():
    # dict_map matches the raw string whole: a padded COAR string must not take a tie from the eu-repo thesis type
    padded = "http://purl.org/coar/resource_type/c_7a1f\xa0"
    raw, mapped = rt.best_raw_and_type([padded, "info:eu-repo/semantics/bachelorThesis"])
    assert (raw, mapped) == ("info:eu-repo/semantics/bachelorThesis", "dissertation")
    # alone with a version marker it still wins on rank
    assert rt.best_raw_and_type([padded, VOR]) == (padded, "dissertation")


def test_clean_coar_wins_tie_over_eu_repo():
    raw, _ = rt.best_raw_and_type(["info:eu-repo/semantics/bachelorThesis", "http://purl.org/coar/resource_type/c_7a1f"])
    assert raw == "http://purl.org/coar/resource_type/c_7a1f"
