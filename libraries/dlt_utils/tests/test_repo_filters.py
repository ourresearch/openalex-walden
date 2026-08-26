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
