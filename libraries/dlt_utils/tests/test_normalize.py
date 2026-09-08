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


"""Tests for normalize_license (oxjob #314: CC URLs were dropped to None)."""

from openalex.dlt.normalize import normalize_license


def test_cc_url_flavors_survive():
    # the bug: hyphen-stripping turns .../licenses/by-nc-nd/ into .../licenses/byncnd/,
    # which no "ccby*" key can match. 8.55M Crossref rows sat at None because of it.
    assert normalize_license("http://creativecommons.org/licenses/by-nc-nd/3.0") == "cc-by-nc-nd"
    assert normalize_license("https://creativecommons.org/licenses/by-nc/4.0") == "cc-by-nc"
    assert normalize_license("https://creativecommons.org/licenses/by-sa/4.0/") == "cc-by-sa"
    assert normalize_license("https://creativecommons.org/licenses/by-nc-sa/4.0/") == "cc-by-nc-sa"
    assert normalize_license("https://creativecommons.org/licenses/by-nd/4.0/") == "cc-by-nd"
    assert normalize_license("https://creativecommons.org/licenses/by/4.0") == "cc-by"


def test_url_suffixes_and_case():
    assert normalize_license("https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode") == "cc-by-nc-nd"
    assert normalize_license("HTTP://CreativeCommons.org/licenses/BY-NC-ND/3.0/") == "cc-by-nc-nd"


def test_text_form_still_matches():
    # repo/OAI metadata spells these out; that path was never broken and must stay working
    assert normalize_license("CC BY-NC-ND 4.0") == "cc-by-nc-nd"
    assert normalize_license("cc-by-sa") == "cc-by-sa"
    assert normalize_license("CC BY") == "cc-by"


def test_longest_flavor_wins():
    # a shorter key must never swallow a longer one -- by-nc-nd is not by-nc, and not by
    assert normalize_license("creativecommons.org/licenses/by-nc-nd/4.0/") != "cc-by"
    assert normalize_license("creativecommons.org/licenses/by-nc-sa/4.0/") != "cc-by-nc"


def test_malformed_flavor_does_not_fall_back_to_the_permissive_answer():
    # 334 records carry junk in the flavor slot; reading them as cc-by is the exact
    # failure #314 is about, so they stay None
    assert normalize_license("https://creativecommons.org/licenses/by-n345667c-nd/4.0/deed.es_AR") is None


def test_non_cc_behaviour_unchanged():
    assert normalize_license(None) is None
    assert normalize_license("") is None
    assert normalize_license("https://creativecommons.org/publicdomain/zero/1.0/") == "public-domain"
    assert normalize_license("https://www.elsevier.com/tdm/userlicense/1.0/") is None
    assert normalize_license("info:eu-repo/semantics/openAccess") == "other-oa"
