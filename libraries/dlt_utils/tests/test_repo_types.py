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
