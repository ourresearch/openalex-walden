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


"""Tests for the merge_key id filter (oxjob #1864: a related id is never the record's own)."""

from openalex.dlt.normalize import MERGE_KEY_IDENTITY_RELATIONS, merge_key_id_filter


def test_identity_relations_feed_merge_key():
    for rel in ("self", "IsIdenticalTo", "IsVariantFormOf", "IsOriginalFormOf", "IsVersionOf", "HasVersion",
                "IsNewVersionOf", "IsPreviousVersionOf"):
        assert rel.lower() in MERGE_KEY_IDENTITY_RELATIONS


def test_related_ids_never_feed_merge_key():
    # live 2026-10-26: Cites put 6,352 DataCite rows on the arXiv paper they cite (BERT, Adam, PPO, GPT-4 ...);
    # IsDerivedFrom put 45,816 datasets onto the PubMed article they came from
    for rel in ("Cites", "References", "IsCitedBy", "IsReferencedBy", "IsSupplementTo", "IsSupplementedBy",
                "IsDescribedBy", "Describes", "IsDerivedFrom", "IsSourceOf", "IsPublishedIn", "IsPartOf", "HasPart",
                "Requires", "IsRequiredBy", "IsDocumentedBy", "Documents", "IsReviewedBy", "Reviews", "Continues",
                "HasMetadata", "IsMetadataFor", "IsCompiledBy", "Compiles", "Obsoletes", "IsObsoletedBy"):
        assert rel.lower() not in MERGE_KEY_IDENTITY_RELATIONS


def test_filter_expression_shape():
    sql = merge_key_id_filter("arxiv")
    assert sql.startswith("filter(ids, x -> x.namespace = 'arxiv' and x.id is not null and (")
    # sources that tag no relation (Crossref, PubMed, repo, MAG; PDF writes 'None') keep their ids
    assert "x.relationship is null or lower(x.relationship) in (" in sql
    assert "'cites'" not in sql and "'self'" in sql
    assert merge_key_id_filter("pmid").startswith("filter(ids, x -> x.namespace = 'pmid'")


def test_arxiv_id_the_record_declares_a_version_of_still_counts():
    # Dagstuhl tags a paper's own full version `Cites arXiv:X` beside `IsVersionOf 10.48550/arXiv.X`
    # (checked on Databricks 26 Oct: own-preprint and v2-suffix cases keep the key, a plain citation does not)
    arxiv = merge_key_id_filter("arxiv")
    assert "exists(ids, y -> y.namespace = 'doi'" in arxiv
    assert "concat('10.48550/arxiv.', lower(regexp_replace(x.id, '(?i)^arxiv:|v[0-9]+$', '')))" in arxiv
    assert "10.48550" not in merge_key_id_filter("pmid")


"""Tests for the arXiv key taken from arXiv's own DOI (oxjob #1940)."""

import re

from openalex.dlt.normalize import ARXIV_DOI_PATTERN, arxiv_key_from_own_doi


def _key(doi):
    # the SQL's own logic, in Python: lower + trim, full-pattern match, `arXiv:` + group 1
    d = doi.strip().lower()
    m = re.match(ARXIV_DOI_PATTERN, d)
    return f"arXiv:{m.group(1)}" if m else None


def test_arxiv_doi_gives_the_oai_records_key():
    # arXiv's OAI records carry `arXiv:<id>`, the id lowercase (3,230,513 of 3,230,513 on 30 Oct)
    assert _key("10.48550/arxiv.2610.13100") == "arXiv:2610.13100"
    assert _key("10.48550/arXiv.1810.04805") == "arXiv:1810.04805"
    assert _key(" 10.48550/ARXIV.0704.0001 ") == "arXiv:0704.0001"
    assert _key("10.48550/arxiv.hep-th/9711200") == "arXiv:hep-th/9711200"
    assert _key("10.48550/arxiv.math.ag/0309136") == "arXiv:math.ag/0309136"


def test_other_dois_give_no_key():
    for doi in ("10.5281/zenodo.23236157", "10.1023/a:1026654312961", "10.48550/arxiv.", "10.48550/arxiv.2101.00001v2",
                "10.48550/arxiv.2101.00001/x", "10.48551/arxiv.2101.00001", "x10.48550/arxiv.2101.00001"):
        assert _key(doi) is None, doi


def test_sql_shape():
    sql = arxiv_key_from_own_doi("d")
    assert sql == (f"CASE WHEN lower(trim(d)) RLIKE '{ARXIV_DOI_PATTERN}' "
                   f"THEN concat('arXiv:', regexp_extract(lower(trim(d)), '{ARXIV_DOI_PATTERN}', 1)) END")
    assert "\\" not in sql and "'" not in ARXIV_DOI_PATTERN
