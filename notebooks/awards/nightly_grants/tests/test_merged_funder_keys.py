"""Offline checks for the merged-funder key hit: an unknown staging key takes the one live grant keyed with its number in the same merged family."""
import pathlib

from test_nih_fold import render

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_unknown_key_takes_the_single_grant_of_a_merged_family():
    c = render(ROOT / "lib" / "build_awards.py")
    names = [n for n, _ in c.statements]
    q = dict(c.statements)
    assert names.index("funder_canon") < names.index("merged_funder_keys") < names.index("lookup") < names.index("NATIVE_STAGING_DISAGREE")
    keys = q["merged_funder_keys"]
    assert "HAVING count(*)>1" in keys, "only funders that share a canonical funder with another"
    assert "x.status='ACTIVE'" in keys and "HAVING count(DISTINCT x.stable_id)=1" in keys, "one live grant, never a withdrawn one, never a choice"
    lookup = q["lookup"]
    hit = "CASE WHEN ks.stable_id IS NULL AND o.namespace IS NULL AND NOT o.is_direct THEN mk.stable_id END"
    assert f"coalesce(rn.stable_id,rs.stable_id,{hit}) existing_id" in lookup, "exact keys first; registry records never take the family path"
    assert "mk.canonical_funder_id=fo.canonical_funder_id AND mk.award_key=o.award_key" in lookup
