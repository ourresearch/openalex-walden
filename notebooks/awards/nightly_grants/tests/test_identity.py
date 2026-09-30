"""Offline identity cases adapted from the pure r1 reference tests."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lib"))

import pytest
from stable_award_ids import component_roots, native_root, resolve_graph, shell_target, staging_root


def test_native_key_connects_changed_staging_triples():
    observations = [
        dict(observation_key="a", staging_id=1, source_funder_id=10, award_key="x", namespace="crossref_grant", source_record_id="10.1/a", existing_id=None),
        dict(observation_key="b", staging_id=2, source_funder_id=20, award_key="y", namespace="crossref_grant", source_record_id="10.1/a", existing_id=None),
    ]
    roots = component_roots(observations)
    assert roots["a"] == roots["b"]
    assert roots["a"][0] == native_root("crossref_grant", "10.1/a")


def test_plain_staging_key_includes_all_three_fields():
    assert staging_root(1, 2, "x") != staging_root(1, 3, "x")
    assert staging_root(1, 2, "x") != staging_root(1, 2, "y")


def test_component_refuses_two_permanent_owners():
    rows = [
        dict(observation_key="a", staging_id=1, source_funder_id=2, award_key="x", existing_id=7),
        dict(observation_key="b", staging_id=1, source_funder_id=2, award_key="x", existing_id=8),
    ]
    with pytest.raises(ValueError, match="COMPONENT_OWNER_CONFLICT"):
        component_roots(rows)


def test_redirects_flatten_and_cycles_fail():
    assert resolve_graph({1: ("REDIRECTED", 2), 2: ("REDIRECTED", 3), 3: ("ACTIVE", None)})[1] == (3, "ACTIVE")
    with pytest.raises(ValueError, match="BROKEN_ENTITY_GRAPH"):
        resolve_graph({1: ("REDIRECTED", 2), 2: ("REDIRECTED", 1)})


def test_shell_election_generic_before_sharp_and_only_unique():
    assert shell_target([7], [8]) == 7
    assert shell_target([], [8]) == 8
    assert shell_target([7, 8], [9]) is None
