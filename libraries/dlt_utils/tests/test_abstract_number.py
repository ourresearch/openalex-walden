"""Tests for the conference abstract number normalizer (oxjob #1162, ruling D7).

The titles are abstracts from the measured series (fresh draws of 18 Nov 2026); the guard cases are the errors
found while measuring the scope on openalex_works (draws A-C).
"""

from openalex.dlt.abstract_number import (
    abstract_number_form, abstract_number_prefix, strip_abstract_number, form_sql, prefix_sql, guard_sql, series_sql,
)


def test_series_titles_lose_their_number():
    cases = [
        ("#164 Impact of enhanced recovery after surgery (ERAS) in the return to intended oncologic treatment",
         2023, "Impact of enhanced recovery after surgery (ERAS) in the return to intended oncologic treatment"),
        ("2022-RA-1163-ESGO Metastatic adenoid cystic carcinoma gland bartholini: a case report",
         2022, "Metastatic adenoid cystic carcinoma gland bartholini: a case report"),
        ("2022-LBA-1618-ESGO A pilot study of interval cytoreductive surgery",
         2022, "A pilot study of interval cytoreductive surgery"),
        ("818 Primary Colon Cancer Diagnosed Concurrently With Metastatic Ovarian Cancer",
         2004, "Primary Colon Cancer Diagnosed Concurrently With Metastatic Ovarian Cancer"),
        ("112: EFFECTIVENESS OF A NEW METHOD OF PALS RECERTIFICATION", 2012,
         "EFFECTIVENESS OF A NEW METHOD OF PALS RECERTIFICATION"),
        ("1751: ACUTE EMPHYSEMATOUS GASTRITIS AND MULTIORGAN DYSFUNCTION", 2016,
         "ACUTE EMPHYSEMATOUS GASTRITIS AND MULTIORGAN DYSFUNCTION"),
        # capitals stay as printed
        ("657: PENETRATION OF DAPTOMYCIN INTO PLEURAL SPACE", 2017, "PENETRATION OF DAPTOMYCIN INTO PLEURAL SPACE"),
    ]
    all_forms = ("code", "hash", "num_colon", "num_dot", "num_space")
    for title, year, want in cases:
        assert strip_abstract_number(title, year, series_forms=all_forms) == want, title


def test_separator_variants():
    assert abstract_number_prefix("#245 : The Clinical Outcome", 2023) == ("hash", "#245 : ")
    assert abstract_number_prefix("#30 - SPONTANEOUS BACTERAEMIA", 2026) == ("hash", "#30 - ")
    assert abstract_number_prefix("# 57. Avaliação da utilização", 2014) == ("hash", "# 57. ")
    assert abstract_number_prefix("1341. Development of a Series", 2019) == ("num_dot", "1341. ")
    assert abstract_number_prefix("0402 Effectiveness Of Ultra-brief CBTi", 2018) == ("num_space", "0402 ")


def test_out_of_scope_title_is_unchanged():
    # a lone "1848: ..." in a history journal, or a chapter number, is never in a series
    title = "1848: A Crisis for Multi-Nationalism?"
    assert strip_abstract_number(title, 2010) == title
    # a source-year whose series is "N: " doesn't lose a "#N " number, and the other way round
    assert strip_abstract_number("#164 Impact of enhanced recovery", 2023, series_forms=("num_colon",)) == \
        "#164 Impact of enhanced recovery"
    assert strip_abstract_number("#164 Impact of enhanced recovery", 2023, series_forms=("hash",)) == \
        "Impact of enhanced recovery"


def test_bare_number_needs_a_capital_next():
    assert abstract_number_form("3 cases of rare disease") is None
    assert abstract_number_form("818 Primary Colon Cancer") == "num_space"
    assert abstract_number_form("54. ĐÁNH GIÁ") == "num_dot"
    assert abstract_number_form("286 CaO-CaCl_2 系溶融フラックス") == "num_space"
    assert abstract_number_form("1153 FNNとIAWQ ASM1") == "num_space"
    assert abstract_number_form("23 の検討") is None


def test_units_and_quantities_keep_their_number():
    # draw B: Optical Fiber Communication Conference titles start with a data rate
    assert abstract_number_prefix("400 Gb/s transmission (40 Ch.×10 Gb/s) over 544 km", 2002) is None
    assert abstract_number_prefix("20 Gbit/s Directly Modulated 1310 Ridge Laser", 2001) is None
    assert abstract_number_prefix("14 CO in Antarctic Glacial Ice as a Tracer", 2017) is None
    assert abstract_number_prefix("3 Reasons Chromebooks Are Shining in Education", 2015) is None
    assert abstract_number_prefix("24 Hour ambulatory blood pressure in children", 2012) is None
    assert abstract_number_prefix("100 Years of Radiology", 1995) is None
    assert abstract_number_prefix("5 T MRI of the knee", 2010) is None


def test_article_a_and_t_cell_are_not_guarded():
    assert abstract_number_prefix("123 A phase II study of docetaxel", 2003) == ("num_space", "123 ")
    assert abstract_number_prefix("771 Characterization of tumor infiltrating immune cells", 2020) == ("num_space", "771 ")


def test_year_guard():
    # draw C: Neurosurgery's "2018 AT-A-GLANCE" in a 2018 abstract supplement
    assert abstract_number_prefix("2018 AT-A-GLANCE", 2018) is None
    assert abstract_number_prefix("2017 IEEE Aerospace Membership Application", 2017) is None
    # abstract number 2018 in another year is still an abstract number
    assert abstract_number_prefix("2018 Endoscopic management of", 2012) == ("num_space", "2018 ")
    # the code form starts with the meeting year on purpose
    assert abstract_number_prefix("2022-RA-701-ESGO Elucidating mechanisms", 2022)[0] == "code"


def test_no_prefix():
    assert abstract_number_prefix("", 2020) is None
    assert abstract_number_prefix(None, 2020) is None
    assert abstract_number_prefix("Impact of enhanced recovery after surgery", 2023) is None
    assert abstract_number_prefix("3D printing of scaffolds", 2023) is None
    assert abstract_number_prefix("COVID-19 and the ICU", 2021) is None
    # a number with nothing after it is not a prefix
    assert abstract_number_prefix("1751: ", 2016) is None


def test_sql_builders_render():
    f = form_sql("title")
    assert f.startswith("CASE WHEN title RLIKE '^[0-9]{4}-") and f.endswith("END")
    assert "\\\\p{Lu}" in f  # Java regex escape survives as a SQL string literal
    assert "regexp_extract(title, '^([0-9]{1,6}:\\\\s+)', 1)" in prefix_sql("title", "form")
    g = guard_sql("title", "form", "prefix", "year")
    assert "BETWEEN year - 1 AND year + 1" in g and g.startswith("coalesce(")
    s = series_sql("openalex.works.openalex_works")
    assert "HAVING count(*) >= 20" in s and "S4210172488" in s
