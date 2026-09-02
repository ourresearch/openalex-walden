"""Tests for normalize_title (oxjob #880 problem 1: digits are identity)."""

from openalex.dlt.normalize import normalize_title


def test_digits_are_kept():
    # the mega-work shape: scan runs, catalogs, serials -- digits are the only discriminator
    assert normalize_title("Folder 1983: 1829: Scan 29") == "folder19831829scan29"
    assert normalize_title("Folder 1983: 1829: Scan 56") == "folder19831829scan56"
    assert normalize_title("Nelson_Daily_News_1902_11_23_001") == "nelsondailynews19021123001"
    assert normalize_title("Graduate Catalog, 2009-2011") != normalize_title("Graduate Catalog, 2012-2014")


def test_digit_free_titles_keep_their_old_key():
    # the 122M keys with no digit must not move -- the work_id_map seed relies on this
    assert normalize_title("The Politics of Austerity") == "politicsausterity"
    assert normalize_title("Université Laval: a history") == "universitelavalhistory"


def test_isbn_only_title_no_longer_normalizes_to_empty():
    # 221,114 repo rows collapsed onto the empty string (evidence: PLAN § Problem 1)
    assert normalize_title("9789632671260") == "9789632671260"


def test_existing_behaviour_unchanged():
    assert normalize_title("") == ""
    assert normalize_title(None) == ""
    assert normalize_title("<i>The Title of a Paper") == "titlepaper"
    assert normalize_title("Ca2+ channels") == "ca2channels"
