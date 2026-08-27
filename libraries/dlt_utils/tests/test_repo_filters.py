"""Tests for the repo ingest policy filters (oxjob #881).

The filter itself needs a real SparkSession, so what is asserted here is the contract that
drifted: the constant, and that the rules are sourced from one vocabulary rather than a
hand-copied list.
"""

from openalex.dlt import repo_filters as rf
from openalex.dlt import repo_types as rt


def test_min_title_length_matches_the_rule_it_replaced():
    # Repo.py used the literal 5 inline; RepoBackfill.py had no title rule at all
    assert rf.MIN_TITLE_LENGTH == 5


def test_policy_filter_is_exported():
    # the point of the extraction: one callable both notebooks import
    assert callable(rf.apply_repo_policy_filters)


def test_filter_reads_the_shared_vocabulary():
    # not a hand-copied list -- if these ever stop being the same object, the drift is back
    assert rf.TYPES_TO_DELETE is rt.TYPES_TO_DELETE


def test_filter_signature_allows_column_overrides():
    # RepoBackfill and Repo name these columns identically today, but the filter should not
    # assume it
    import inspect
    params = inspect.signature(rf.apply_repo_policy_filters).parameters
    assert {"title_col", "type_col", "native_id_col"} <= set(params)


def test_endpoint_denylist_shape():
    # oxjob #881 round 2: 67 tier-A endpoints + pt.cision.com. Every id is a non-empty string;
    # the count is asserted so an accidental paste-truncation fails loudly.
    assert len(rf.ENDPOINTS_TO_DELETE) == 68
    assert all(isinstance(e, str) and e for e in rf.ENDPOINTS_TO_DELETE)
    # the four individually-adjudicated verdicts (ENDPOINT_VERDICTS.md) are present
    assert {
        "2b57dfbd43207095dbc",  # pt.cision.com -- EDP media monitoring
        "05b312337ed8746c780",  # UBC ContentDM
        "db7d86c1ad782e15c55",  # server15795 ContentDM
        "215bbea87561a3ab940",  # CDAEA
    } <= rf.ENDPOINTS_TO_DELETE


def test_gallica_is_carved_not_denylisted():
    # the carve keeps monographies/manuscrits/periodiques:titres -- whole-endpoint removal
    # of Gallica would be wrong, so it must never appear in both structures
    assert "b6f3a90f96528af2baa" in rf.ENDPOINT_SETSPEC_DELETE
    assert "b6f3a90f96528af2baa" not in rf.ENDPOINTS_TO_DELETE
    prefixes = rf.ENDPOINT_SETSPEC_DELETE["b6f3a90f96528af2baa"]
    assert "gallica:typedoc:periodiques:fascicules" in prefixes
    # keep-classes must not be carved, even by prefix overlap
    for keep in ("gallica:typedoc:monographies", "gallica:typedoc:manuscrits",
                 "gallica:typedoc:periodiques:titres"):
        assert not any(keep.startswith(p) for p in prefixes)


def test_no_endpoint_in_both_structures():
    assert not set(rf.ENDPOINT_SETSPEC_DELETE) & rf.ENDPOINTS_TO_DELETE


def test_endpoint_filter_is_exported():
    import inspect
    assert callable(rf.apply_endpoint_filters)
    params = inspect.signature(rf.apply_endpoint_filters).parameters
    assert {"endpoint_col", "set_spec_col", "keep_when"} <= set(params)
