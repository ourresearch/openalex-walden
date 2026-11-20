"""Tests for the conference abstract number normalizer (oxjob #1162, ruling D7).

The titles are abstracts from the measured series (fresh draws of 18 Nov 2026); the guard cases are the errors
found while measuring the scope on openalex_works (draws A-C).
"""

from openalex.dlt.abstract_number import (
    abstract_number_form, abstract_number_prefix, strip_abstract_number, form_sql, prefix_sql, guard_sql, series_sql,
    series_key, series_key_sql, letter_code_shape,
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
    assert "0.9 * count(*)" in s and "series_key" in s
    assert "'letter_code'" in g and "'IL'" in g
    assert series_key_sql("form", "prefix").startswith("CASE WHEN form = 'letter_code' THEN concat('letter_code:'")


# Letter codes (D7 step 1b): titles from draw B of 18 Nov 2026, the guard cases from the targeted molecule check.

def test_letter_codes_are_found_with_their_separator():
    assert abstract_number_prefix("P095. Immune compromised mice develop colitis", 2016) == ("letter_code", "P095. ")
    assert abstract_number_prefix("Mo1428 Hemoclip Application Using CAP-Fitted Forward Endoscopy", 2015) == \
        ("letter_code", "Mo1428 ")
    assert abstract_number_prefix("MP63-19 THE CHANGING FACE OF READMISSIONS", 2016) == ("letter_code", "MP63-19 ")
    assert abstract_number_prefix("EP-1667: MR-Guided Radiotherapy of Head and Neck Cancers", 2017) == \
        ("letter_code", "EP-1667: ")
    assert abstract_number_prefix("P1.24 Mutations in ANO5 represent a common cause", 2012) == ("letter_code", "P1.24 ")
    assert abstract_number_prefix("PO-04-230 DRUG REPURPOSING", 2026) == ("letter_code", "PO-04-230 ")
    assert abstract_number_prefix("S180 – Change of Mucosal Immunity Due to Radiotherapy", 2008) == \
        ("letter_code", "S180 – ")
    # the meeting year inside a code is not a year guard
    assert abstract_number_prefix("IDF2022-1156 Cognitive dysfunction in older adults", 2023)[0] == "letter_code"
    assert abstract_number_prefix("WCN25-1894 CHRONIC KIDNEY DISEASE", 2025) == ("letter_code", "WCN25-1894 ")


def test_letter_code_shape_and_series():
    assert letter_code_shape("MP63-19 ") == "MP9-9"
    assert letter_code_shape("P095. ") == "P9"
    assert letter_code_shape("Mo1428 ") == "Mo9"
    assert letter_code_shape("EP-1667: ") == "EP-9"
    assert series_key("letter_code", "MP63-19 ") == "letter_code:MP9-9"
    assert series_key("num_colon", "46: ") == "num_colon"
    title = "MP63-19 THE CHANGING FACE OF READMISSIONS"
    assert strip_abstract_number(title, 2016, series_forms=("letter_code:MP9-9",)) == "THE CHANGING FACE OF READMISSIONS"
    # another shape in the same source-year is another series
    assert strip_abstract_number(title, 2016, series_forms=("letter_code:MP9",)) == title
    assert strip_abstract_number(title, 2016, series_forms=("letter_code",)) == title


def test_molecule_names_keep_their_letters():
    # series of these held in PLoS ONE, Blood and J Immunol before the guard
    assert abstract_number_prefix("IL-10 Can Enhance FcγR-mediated Mast Cell Activation", 2010) is None
    assert abstract_number_prefix("CD4 Cell Levels during Treatment for Tuberculosis", 2014) is None
    assert abstract_number_prefix("MiR-21 Enhances Melanoma Invasiveness", 2013) is None
    assert abstract_number_prefix("COVID-19 Vaccination Rates Among Adults", 2021) is None
    assert abstract_number_prefix("SU5416 Inhibits Angiogenesis", 2001) is None
    # a Sunday poster code is not the drug prefix
    assert abstract_number_prefix("Su634 IMPLICATIONS OF NEAR-INFRARED IMAGING", 2021) == ("letter_code", "Su634 ")


def test_letter_code_needs_a_capital_next():
    assert abstract_number_form("Tu2243 STUMP APPENDICITIS") == "letter_code"
    assert abstract_number_form("H2 production by algae") is None
    assert abstract_number_form("Phase 3 Trial") is None
    assert abstract_number_form("D-7-42 ICAを用いた脳電位解析法の検討") == "letter_code"
    assert abstract_number_form("OP-121-1 腹腔鏡下") is None


# D7 step 3 (19 Nov 2026): the frozen series table, its exclusion list and the series rules that keep chapters,
# session headings, years and dates out (oxjob #1162 work/d7_step3_series_2026-11-19.md).

from openalex.dlt.abstract_number import (  # noqa: E402
    EXCLUDED_SOURCES, EXCLUDED_SOURCE_IDS, RULE_VERSION, _SERIES_COLUMNS, series_table_ddl, excluded_table_ddl,
    excluded_seed_sql, series_append_sql,
)


def test_a_day_of_the_month_keeps_its_number():
    # Homily Service and The Expository Times title their items by date
    assert abstract_number_prefix("24 October 2004", 2004) is None
    assert abstract_number_prefix("10 December 2006 • Second Sunday in Advent", 2006) is None
    assert abstract_number_prefix("24 June: Proper 7", 2001) is None
    # ticket examples (ZD 26225, ZD 339) still lose theirs
    assert abstract_number_prefix("511 Obturative nerve complication in laparoscopic treatment", 2021) == \
        ("num_space", "511 ")
    assert abstract_number_prefix("1402: STANDARDIZING WEANING OF HIGH-FLOW NASAL CANNULA", 2019) == \
        ("num_colon", "1402: ")
    assert abstract_number_prefix("46: THE “JULY EFFECT” IN THE ICU REVISITED", 2015) == ("num_colon", "46: ")


def test_exclusion_seed_list():
    assert len(EXCLUDED_SOURCE_IDS) == len(set(EXCLUDED_SOURCE_IDS)) == len(EXCLUDED_SOURCES) == 12
    assert all(s.startswith("https://openalex.org/S") and reason for s, reason in EXCLUDED_SOURCES)
    seed = excluded_seed_sql("t.x")
    assert seed.startswith("INSERT INTO t.x") and "LEFT ANTI JOIN t.x" in seed
    assert all(s in seed for s in EXCLUDED_SOURCE_IDS)
    assert "\\'" in seed  # a quote inside a reason is escaped


def test_series_rules_render():
    s = series_sql()
    assert "count_if(NOT guarded) >= 20" in s
    assert "BETWEEN 1800 AND year + 1" in s and "'hash'" in s
    assert "size(split(rest, '\\\\s+')) <= 4" in s
    assert "coalesce(a.median_pages, 0) >= 10" in s
    assert "NOT IN ('https://openalex.org/S4210172488'" in s and "\nAND NOT (a.n_pass_latin" in s
    t = series_sql(excluded_table="x.excl")
    assert "LEFT ANTI JOIN x.excl e ON e.source_id = a.source_id" in t and "\nWHERE NOT (a.n_pass_latin" in t
    assert "NOT IN ('https://openalex.org/S4210172488'" not in t
    d = series_sql(prefixes_table="x.pre")
    assert "LEFT JOIN x.pre n ON n.work_id = w.id" in d and "concat(coalesce(n.prefix, ''), w.title) AS title" in d


def test_series_table_and_append_agree():
    ddl = series_table_ddl("x.series")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS x.series")
    ddl_cols = [c.strip().split()[0] for c in ddl.split("(", 1)[1].split(")\nCOMMENT")[0].split(",")]
    assert ddl_cols == list(_SERIES_COLUMNS) + ["added_on", "rule_version"]
    a = series_append_sql("x.series", "x.excl", "x.works", "x.pre")
    assert a.startswith("INSERT INTO x.series (source_id, year, form, series_key")
    assert f"'{RULE_VERSION}'" in a and "LEFT ANTI JOIN x.series t ON t.source_id <=> s.source_id" in a
    assert "FROM x.works w LEFT JOIN x.pre n" in a and "LEFT ANTI JOIN x.excl e" in a
    b = series_append_sql("x.series", "x.excl", "x.build")  # step 4: titles as deposited, no prefixes table
    assert "FROM x.build w\n" in b and "LEFT JOIN" not in b.split("SELECT a.*")[0].split("FROM x.build w")[1][:5]
    assert excluded_table_ddl("x.excl").startswith("CREATE TABLE IF NOT EXISTS x.excl (\n  source_id STRING NOT NULL")


def test_builder_notebook_is_generated_from_the_module():
    from pathlib import Path
    from openalex.dlt.abstract_number import BUILDER_NOTEBOOK, builder_notebook_sql
    notebook = Path(__file__).resolve().parents[3] / BUILDER_NOTEBOOK
    assert notebook.read_text(encoding="utf-8") == builder_notebook_sql(), \
        f"{BUILDER_NOTEBOOK} differs from abstract_number.builder_notebook_sql(): regenerate it (see its docstring)"


def test_display_title_twins():
    from openalex.dlt.abstract_number import display_title, display_title_sql
    assert display_title("#303 Risk-reducing bilateral oophorectomy", "#303 ") == "Risk-reducing bilateral oophorectomy"
    assert display_title("46: THE JULY EFFECT IN THE ICU REVISITED", "46: ") == "THE JULY EFFECT IN THE ICU REVISITED"
    assert display_title("A title that changed since", "#303 ") == "A title that changed since"  # stale prefix: whole
    assert display_title("#303 Risk", None) == "#303 Risk" and display_title(None, "#303 ") is None
    sql = display_title_sql()
    assert sql.startswith("CASE WHEN title_abstract_number IS NOT NULL AND substr(title, 1, length(title_abstract_number))")
    assert sql.endswith("ELSE title END")


def test_work_prefixes_sql_renders():
    from openalex.dlt.abstract_number import work_prefixes_sql
    s = work_prefixes_sql("x.build", "x.series", "x.excl", "x.cur")
    assert "FROM x.build w" in s and "LEFT SEMI JOIN x.series s ON s.source_id = g.source_id" in s
    assert "LEFT ANTI JOIN x.excl e ON e.source_id = g.source_id" in s
    assert "SELECT work_id FROM x.cur WHERE title IS NOT NULL" in s and s.endswith("WHERE NOT g.guarded")
    assert "s.series_key = g.series_key" in s  # a letter_code series only strips its own code shape


def test_strip_cell_switch():
    from openalex.dlt.abstract_number import enriched_strip_cell_sql, STRIP_SWITCH_ON
    assert STRIP_SWITCH_ON is True  # D7 step 6 flipped it; revert = False + regenerate the cell
    off, on = enriched_strip_cell_sql(False), enriched_strip_cell_sql(True)
    # off: one SELECT, nothing written; no scripting block for the push night to parse
    assert "BEGIN" not in off and "MERGE" not in off and "INSERT" not in off and "CREATE" not in off
    assert off.rstrip().endswith("AS d7_title_strip") and off.count(";") == 0
    from openalex.dlt.abstract_number import enriched_column_cell_sql
    assert enriched_column_cell_sql().count(";") == 0 and enriched_column_cell_sql().count("ALTER TABLE") == 1
    # on: the scripting block
    assert on.count("BEGIN") == 2 and on.rstrip().endswith("END")
    assert "INSERT INTO openalex.works.title_abstract_number_series" in on
    assert "FROM openalex.works.openalex_works_build w" in on  # series and prefixes from tonight's build
    assert "UPDATE SET t.title_abstract_number = s.prefix" in on
    assert "UPDATE SET t.title_abstract_number = p.title_abstract_number" in on  # fallback: last night's
    assert "t.title =" not in on  # title itself is never written


def test_enriched_notebook_cells_are_generated_from_the_module():
    from openalex.dlt.abstract_number import (enriched_cells_in_notebook, enriched_column_cell_sql,
                                              enriched_strip_cell_sql, ENRICHED_NOTEBOOK)
    column, strip = enriched_cells_in_notebook()
    assert column == enriched_column_cell_sql() and strip == enriched_strip_cell_sql(), \
        f"{ENRICHED_NOTEBOOK}'s D7 cells differ from the module: regenerate (enriched_strip_cell_sql's docstring)"


def test_enriched_notebook_order_and_hash():
    import json
    from pathlib import Path
    from openalex.dlt.abstract_number import ENRICHED_NOTEBOOK, ENRICHED_COLUMN_CELL, ENRICHED_STRIP_CELL
    nb = json.loads((Path(__file__).resolve().parents[3] / ENRICHED_NOTEBOOK).read_text(encoding="utf-8"))
    srcs = ["".join(c["source"]) for c in nb["cells"]]
    first = lambda prefix: next(i for i, s in enumerate(srcs) if s.startswith(prefix))  # noqa: E731
    column, strip = first(ENRICHED_COLUMN_CELL), first(ENRICHED_STRIP_CELL)
    alt_merge = first("-- oxjob #1610: before the hash cell")
    fulltext = first("-------- Merge related_works + fulltext")
    hashed = first("-- Per-run scratch (recreated every run")
    assert alt_merge < column < strip < fulltext < hashed  # type is final, the fulltext MERGE stays last, then the hash
    assert "CASE WHEN title_abstract_number IS NOT NULL THEN CONCAT('title_abstract_number:', title_abstract_number) END" \
        in srcs[hashed]


def test_publishers_serve_the_display_title():
    from pathlib import Path
    from openalex.dlt.abstract_number import PUBLISH_FILES, display_title_sql
    root = Path(__file__).resolve().parents[3]
    for f in PUBLISH_FILES:
        text = (root / f).read_text(encoding="utf-8")
        assert display_title_sql().replace('"', '\\"') in text or display_title_sql() in text, f
        assert "title_abstract_number" in text, f
