"""one_namesake (oxjob #1803): run with `python -m pytest utils/affiliation_matcher/tests` from the repo root."""
import pytest

from utils.affiliation_matcher.nightly import NAMESAKE_PAIRS, _folded, one_namesake

SEOUL, TOKYO = 197347611, 4210161052
ONTARIO, CAMBODIA, THAILAND = 125749732, 4210116551, 4405277831


@pytest.mark.parametrize("s, ids, want", [
    ("Department of Chemistry, Korea University", [SEOUL, TOKYO], [SEOUL]),
    ("Dept X, Korea University", [TOKYO, 5, SEOUL], [5, SEOUL]),                   # order of the rest kept
    ("Korea University, Kodaira, Tokyo 187, Japan", [SEOUL, TOKYO], [SEOUL, TOKYO]),
    ("Korea University, Tōkyō", [SEOUL, TOKYO], [SEOUL, TOKYO]),                  # accents
    ("Korea University, KODAIRA-SHI", [SEOUL, TOKYO], [SEOUL, TOKYO]),
    ("kim@korea-u.ac.jp, Korea University", [SEOUL, TOKYO], [SEOUL, TOKYO]),
    ("朝鮮大学校", [SEOUL, TOKYO], [SEOUL, TOKYO]),
    ("University of Tokyo; Korea University", [SEOUL, TOKYO], [SEOUL, TOKYO]),     # doubt keeps both
    ("Korea University", [TOKYO], [TOKYO]),                                        # smaller alone: untouched
    ("Korea University", [SEOUL], [SEOUL]),
    ("Schulich School of Medicine, Western University", [ONTARIO, CAMBODIA], [ONTARIO]),
    ("Western University, Phnom-Penh", [ONTARIO, CAMBODIA], [ONTARIO, CAMBODIA]),
    ("Faculty of Nursing, Western University, Pathum Thani", [ONTARIO, THAILAND], [ONTARIO, THAILAND]),
    ("Faculty of Dentistry, Western University", [ONTARIO, THAILAND, CAMBODIA], [ONTARIO]),
    (None, [SEOUL, TOKYO], [SEOUL]),
    ("", [], []),
    ("Department of Chemistry, Korea University", ["197347611", "4210161052"], ["197347611"]),  # string ids
])
def test_one_namesake(s, ids, want):
    assert one_namesake(s, ids) == want


def test_no_word_list_matches_a_bare_name():
    for small, (big, words) in NAMESAKE_PAIRS.items():
        for s in ("", "Korea University", "Western University", "Department of Chemistry", "University"):
            assert not words.search(s) and not words.search(_folded(s)), (small, s)
