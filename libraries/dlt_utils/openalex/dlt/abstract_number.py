"""Conference abstract numbers at the start of a work title (oxjob #1162, ruling D7).

Ruling D7 (16 Nov 2026, oxjobs working/policy-decisions/RULINGS.md § 3, Work titles): drop a conference abstract's
leading program number or submission code from its title ("#164 ", "112: ", "2022-RA-1163-ESGO "), keep capitals as
printed. Conditions: one shared normalizer, used by the displayed title and by every title-based merge or match key,
applied to the whole corpus in one night; the bar includes "no existing merge or match is lost".

This module is that normalizer's definition. Since step 4 (19 Nov 2026) CreateWorksEnriched holds its two generated
cells and the publishers its display expression, behind STRIP_SWITCH_ON (off): until the switch-over night (oxjob #1162
PLAN.md, D7 build, step 6) it changes no title and re-stamps no work.

Two parts:

1. ``abstract_number_prefix(title, publication_year)``: the title's leading number in one of six forms, or None.
   A pure function of the title, plus the work's year for one guard. The forms:
     code         "2022-RA-1163-ESGO "  (a submission code: year-track-number-meeting)
     hash         "#164 ", "#245 : ", "#30 - "
     num_colon    "112: "
     num_dot      "1341. " followed by a capital letter
     num_space    "818 " followed by a capital letter
     letter_code  "P095. ", "Mo1428 ", "MP63-19 ", "EP-1667: ", "WCN25-1894 " followed by a capital letter
   Guards (all but code and letter_code): the number is not the work's publication year (+/- 1), and for the three
   bare-number forms the next word is not a unit, a chemical symbol or a quantity word ("400 Gb/s ...", "14 CO ...",
   "3 Reasons ...", "24 Hour ..."). letter_code: the letters are not a gene, protein, virus or drug-code family
   ("IL-10 Can Enhance ...", "CD4 Cell Levels ...", "MiR-21 Enhances ...", "SU5416 ...").

2. The scope: a prefix is dropped only when the work's source and publication year hold an abstract series of that
   form. ``series_sql()`` builds that table from a works table: a source of type journal or conference, works of
   the allowed types, >= 20 titles of the form in that source-year, at least half of their leading numbers distinct
   (except code), and for num_dot at least half of them on supplement pages or issues or typed
   conference-abstract. A leading number on a lone title ("1848: A Crisis for Multi-Nationalism?") or in a book
   ("3: The Beginnings of ...", a chapter number) is never dropped. A letter_code series is one code shape (the
   code with each digit run written 9, case kept: "P9", "MP9-9", "Mo9"), >= 20 titles, >= 90% distinct codes.

Measured on openalex_works (night 2026-11-18), 18 Nov 2026: 539,073 works in 2,156 source-years of 525 sources
(num_space 408,928; num_colon 63,396; num_dot 55,243; hash 10,475; code 1,031); 73% of them are typed article.
Precision on a fresh random draw of 100 changed titles: 100/100 abstract or program numbers (Wilson 95% lower bound
96.3%). The guards and the excluded source were chosen on three earlier draws (A: num_dot needs a supplement signal;
B: units, quantity words, distinct numbers; C: the year guard and Zorg en Financiering's item numbers).
Receipts: oxjobs working/garbled-and-missing-work-titles/work/d7_scope_2026-11-18.md.

letter_code, measured the same way on 18 Nov 2026: 718,688 works in 4,757 source-year shapes of 523 sources. The
shape, distinct-code share and molecule-family guards were chosen on draw A (100) plus a targeted check of titles
that start with a gene or protein name (IL-, CD, miR-: series in PLoS ONE, Blood, J Immunol); a fresh draw B of 100
was 100/100 program codes. Receipts: work/d7_letter_codes_2026-11-18.md in the same oxjob.

Merge and match keys keep the title as deposited (oxjob #1162, D7 step 2): putting the stripped title inside the
title_author keys would push 3,637 works' keys under the 20-character minimum or over the 3-works cap and give ~34K
abstracts the key of another work (often their later full paper). So the strip changes the displayed title only.

Where the strip happens (D7 step 4, 19 Nov 2026): at publish, never in ``openalex_works.title``. CreateWorksEnriched
stores the number a work's displayed title drops in the build column TITLE_PREFIX_COLUMN (``title_abstract_number``,
NULL on every other work; ``enriched_strip_cell_sql``, behind STRIP_SWITCH_ON), hashes it so the work re-stamps, and the
publishers (ES ``sync_works``, Lakebase single-GET docs, the snapshot) serve ``display_title_sql`` as ``title`` and
``display_name``. Every other reader of ``openalex_works.title`` (71 walden files: matchers, merge keys, topics,
keywords, embeddings, judges) keeps the deposited title, so no key, match, topic or model input changes. Weighed
against stripping ``openalex_works.title`` itself: oxjob #1162 work/d7_step4_weigh_2026-11-19.md.

The frozen series table (D7 step 3, 19 Nov 2026): SERIES_TABLE, built and topped up only by ``series_append_sql``
(append-only, from titles as deposited), with EXCLUDED_TABLE for taking a series out; the rules for both are in the
comment above SERIES_TABLE. The SQL notebook BUILDER_NOTEBOOK runs it (generated by ``builder_notebook_sql``; no job
runs it yet). Step 3 also found series the step 1 rules let in that are not conference abstracts, all
small (20-100 titles), so a random draw of works never met them; a draw of SERIES did (76-77 of 80 abstract series):
chapters of review issues and monographs ("17 DNA Replication Accessory Proteins", Cold Spring Harbor Monograph
Archive; "To improve the academy"), session headings and catalogue entries ("9. Breast Imaging", AJR; "2. DRAPERY",
BICS 2013), titles that start with a year ("1959: La dolce vita") or a date ("24 October 2004"), quantities ("100 GBd
Intensity Modulation ...") and item, review or hymn numbers. Four series rules (>= 20 titles past the guards; not
mostly <= 4 words; not mostly year numbers; not chapter-length items) and a month-name guard take out 162 series,
and 12 sources in the exclusion list 31 more (5,443 works in all). Then: 6,738 series, 1,252,251 works
(letter_code 717,539; num_space 405,978; num_colon 62,754; num_dot 54,499; hash 10,450; code 1,031), all 20 ticket
examples (ZD 26225, 339) in. Fresh
draws: 58/60 series (both misses now excluded sources) and 60/60 works. The module's SQL gave exactly the hand-built
set (0 differ either way); the e-locator worry from step 1b was checked and dropped: the 20 num_dot series that
pass only on "e123" pages are all meeting abstracts (Clinical Neurophysiology, Brain Behavior and Immunity ...).
Receipts: work/d7_step3_series_2026-11-19.md in the same oxjob.

The Python function and the SQL built here must agree. Checked 18 Nov 2026: the SQL reproduced the measured 539,073
works exactly, and Python and SQL gave the same form, prefix and guard on 1,500 random candidate titles. letter_code:
the SQL reproduced the measured 718,688 works exactly, and Python and SQL gave the same prefix and series key on 23
edge-case titles (separators, trailing letters, dotted and multi-part codes, non-Latin text after the code).
tests/test_abstract_number.py covers the Python side and checks the generated cells and publisher copies. Step 4, on
Databricks (19 Nov 2026): work_prefixes_sql over openalex_works gave the frozen scope exactly (1,252,240 works = the
1,252,251 of step 3 minus 11 whose title is curated), and on a 14,468-work clone of the build the cells' effect
(column NULL, then set) changed 0 content hashes switched off and exactly the 5,028 prefixed works' switched on, with
`title` byte-identical and display_title_sql equal to the stripped titles (oxjob #1162 work/d7_step4_*).
"""

import re
import unicodedata

FORMS = ("code", "hash", "num_colon", "num_dot", "num_space", "letter_code")

# Works of these types never lose a leading number (books and chapters number chapters; datasets and reference
# entries number items; editorials, errata and paratext are not abstracts).
EXCLUDED_WORK_TYPES = ("book", "book-chapter", "dataset", "reference-entry", "dissertation", "book-review",
                       "editorial", "paratext", "erratum", "retraction")
SOURCE_TYPES = ("journal", "conference")

# Sources whose numbered titles are not conference abstract numbers: item, review, document, hymn or episode numbers,
# or chapters that the page rule below can't see (no pages). The seed rows of the exclusion table (EXCLUDED_TABLE):
# Zorg en Financiering found in draw C (18 Nov 2026), the rest while freezing the series table (19 Nov 2026, D7 step 3).
EXCLUDED_SOURCES = (
    ("https://openalex.org/S4210172488", "Zorg en Financiering: numbered news items ('182 Beklonken')"),
    ("https://openalex.org/S157670870", "IEEE Trans Computers: review numbers ('R68-36 Dataless Programming')"),
    ("https://openalex.org/S60436882", "IEEE Trans Electronic Computers: review numbers ('R65-42 ...')"),
    ("https://openalex.org/S188551233", "J Stat Comput Simul: literature item numbers ('C376. ...')"),
    ("https://openalex.org/S152305454", "Food Cosmet Toxicol: abstract-service item numbers ('950 Triton ...')"),
    ("https://openalex.org/S4210201994", "Black Sacred Music: hymn numbers ('60 The Crown of Life')"),
    ("https://openalex.org/S96872871", "Chinese Law & Government: document numbers ('9: General Office of ...')"),
    ("https://openalex.org/S195740279", "Clinics in Dermatology: chapter numbers ('21 Selected dermatoses of ...')"),
    ("https://openalex.org/S90857023", "Int Migration Review: chapter and document numbers ('57 Sectarian ...')"),
    ("https://openalex.org/S5406931023", "merz | medien + erziehung: episode numbers ('#28 - Sprache in den Medien')"),
    ("https://openalex.org/S57227388", "Nursing Research: abstract-service item numbers ('427: The treatment ...')"),
    ("https://openalex.org/S4306545868", "Guowai yixue (mianyixue): article sequence numbers ('032 KIR研究进展')"),
)
EXCLUDED_SOURCE_IDS = tuple(source_id for source_id, _ in EXCLUDED_SOURCES)

MIN_SERIES_TITLES = 20
MIN_DISTINCT_SHARE = 0.5
MIN_LETTER_CODE_DISTINCT_SHARE = 0.9  # gene-name "series" (IL-6, IL-10, IL-17 ...) repeat codes; program codes don't
MIN_NUM_DOT_SUPPLEMENT_SHARE = 0.5

# Series that look like abstract series but aren't (D7 step 3, 19 Nov 2026; receipts in oxjob #1162
# work/d7_step3_series_2026-11-19.md). Each rule reads only the titles whose number passes the guards.
SHORT_TITLE_WORDS = 4  # session headings, catalogue entries, report sections: "9. Breast Imaging", "2. DRAPERY"
MAX_SHORT_TITLE_SHARE = 0.5  # of the Latin-script stripped titles
FIRST_YEAR_NUMBER = 1800  # "1959: La dolce vita", "1955: Then and Now": the number is a year (bare-number forms)
MAX_YEAR_NUMBER_SHARE = 0.5
MIN_CHAPTER_PAGES = 10  # median pages per item: chapters of a monograph or review issue, not abstracts
MIN_CHAPTER_DISTINCT_FIRST_PAGE_SHARE = 0.5  # abstract supplements that give each abstract the whole range don't count

# One body per form, the source of truth for both twins. The SQL form test adds what must follow the prefix (any
# non-space, or for the bare-number forms an upper-case letter: Java's \p{Lu}, Python's category "Lu" check).
_FORM_BODIES = {
    "code": r"[0-9]{4}-[A-Z]{2,4}-[0-9]{1,5}-[A-Z]{2,8}\s+",
    "hash": r"#\s?[0-9]{1,6}(\s*[:.–—-]\s*|\s+)",
    "num_colon": r"[0-9]{1,6}:\s+",
    "num_dot": r"[0-9]{1,5}\.\s+",
    "num_space": r"[0-9]{1,5}\s+",
    "letter_code": r"[A-Za-z]{1,6}[0-9]{0,2}-?[0-9]{1,5}(?:[.-][0-9]{1,4})?[A-Za-z]?\s*[:.–—-]?\s+",
}
_NEEDS_CAPITAL = ("num_dot", "num_space", "letter_code")
_NO_YEAR_GUARD = ("code", "letter_code")  # their digits may hold the meeting year on purpose ("IDF2022-1156 ")

# Letters that start gene, protein, virus, chemical and drug-code names (compared upper-cased). A few also start
# program codes somewhere ("CO-001 " oral communications, "PM12 "): those series keep their numbers, precision over
# recall. "SU" is excluded only in capitals: "Su634 " is a Sunday poster, "SU5416 " a drug.
MOLECULE_CODE_LETTERS = frozenset((
    "IL", "CD", "MIR", "MIRNA", "LNC", "LINC", "TLR", "MMP", "HLA", "CCL", "CXCL", "CCR", "CXCR", "CX", "TGF", "FGF",
    "IGF", "IGFBP", "BMP", "WNT", "SOX", "FOX", "FOXP", "HOX", "KLF", "STAT", "JAK", "CDK", "PARP", "HDAC", "SIRT",
    "ATG", "LC", "GLP", "SGLT", "DPP", "CYP", "UGT", "ABCB", "ABCG", "ABCC", "SLC", "KIF", "TRPV", "TRPM", "TRPC",
    "TRPA", "GPR", "ADAM", "ADAMTS", "MUC", "KRT", "COL", "HSP", "HSPA", "ERK", "MEK", "MAPK", "AKT", "NOX", "COVID",
    "SARS", "MERS", "HIV", "HPV", "HER", "BRCA", "APOE", "APO", "IGG", "IGE", "IGM", "IGA", "TNF", "IFN", "VEGF",
    "VEGFR", "EGF", "EGFR", "PDGF", "PDGFR", "NPM", "FLT", "BCL", "MYC", "KRAS", "NRAS", "EBV", "CMV", "HBV", "HCV",
    "HSV", "HHV", "HTLV", "SIV", "ZIKV", "DENV", "PM", "CO", "NO", "SO", "CH", "MK", "BMS", "GSK", "AZD", "LY", "ABT",
    "PF", "JNJ", "BAY", "AMG", "TAK", "ONO", "NVP", "OSI", "CGP", "SCH", "WAY", "GW", "ARQ", "BIBF", "CI", "SB", "SR",
    "KU", "PI", "GDC", "INCB", "RG", "RO", "MLN", "AG", "PCI", "ZD", "NSC", "AMD", "PKC", "PLK", "CK", "MCL", "NF",
    "ATP", "ADP", "AMP", "GTP", "DNA", "RNA", "IRF", "NLRP", "NOD", "CASP", "GSDM", "PPAR", "GABA", "NMDA", "AMPA",
    "DRD", "HTR", "ADRB", "TRK", "NTRK", "ALK", "ROS", "RET", "MET", "FGFR", "ERBB", "PTEN", "MDM", "CDKN", "PAX",
    "GATA", "RUNX", "NOTCH", "GLI", "CTLA", "PDL", "TIM", "LAG", "KIR", "NKG", "FCGR", "TCR", "MHC", "EPO",
))
CASE_SENSITIVE_EXCLUDED_LETTERS = frozenset(("SU",))
_BARE_NUMBER_FORMS = ("num_colon", "num_dot", "num_space")

SQL_FORM_PATTERNS = tuple(
    (f, "^" + _FORM_BODIES[f] + (r"\p{Lu}" if f in _NEEDS_CAPITAL else r"\S")) for f in FORMS)
SQL_PREFIX_PATTERNS = {f: "^(" + _FORM_BODIES[f] + ")" for f in FORMS}
_PY_PREFIX = {f: re.compile("^(" + _FORM_BODIES[f] + r")(?=\S)") for f in FORMS}

# What may follow a bare leading number when the number belongs to the title. Two spellings of one pattern: Java
# takes the (?i) flag after ^, Python only at the very start.
QUANTITY_WORDS = (
    "years?", "yrs?", "months?", "weeks?", "days?", "hours?", "hrs?", "h", "minutes?", "min", "seconds?", "sec", "s",
    "reasons", "ways", "tips", "things", "steps", "questions", "lessons", "rules", "myths", "ideas", "keys", "facts",
    "million", "millions", "billion", "thousand", "hundred", "percent", "per", "x", "×", "times", "fold",
    "of", "to", "and", "or", r"vs\.?", "versus",
    # a day of the month: "24 October 2004", "24 June: Proper 7" (Homily Service, Expository Times; 19 Nov 2026)
    "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november",
    "december",
)
_SQL_QUANTITY = r"^(?i)(" + "|".join(QUANTITY_WORDS) + r")\b"
_PY_QUANTITY = re.compile(r"^(?:" + "|".join(QUANTITY_WORDS) + r")\b", re.IGNORECASE)

# Units (with an SI prefix) and chemical symbols, case-sensitive, as the whole next token. Same text in both twins.
_UNIT = (r"^(?:[GMTPkmµn]?(?:b|B|bit|bits|bps|Bps|byte|bytes|Hz|W|V|eV|Pa|J|N|Ω|K|T|g|m|l|L|M|bar|Gy|Sv|Bq|"
         r"Ci|D|G)(?:/s)?|C|O|F|P|S|CO|CO2|NO|Tc|Ga|Fe|Cu|Zn|Ca|Na|Cl|Br|Co|Xe|Rb|Sr|Lu|Ra|Th|U|Pu|Am)"
         r"(?=[\s,.:;)\-]|$)")
_PY_UNIT = re.compile(_UNIT)
_DIGITS = re.compile(r"[0-9]+")
_LETTERS = re.compile(r"[A-Za-z]+")
_CODE_END = r"[\s:.–—-]+$"  # what follows a letter code inside its prefix (a code ends in a digit or a letter)
_PY_CODE_END = re.compile(_CODE_END)


def _form_of(title):
    first = title[0]
    if first in "#0123456789":
        forms = FORMS[:-1]
    elif "A" <= first <= "Z" or "a" <= first <= "z":  # only letter_code starts with a letter: skip the rest
        forms = ("letter_code",)
    else:
        return None
    for form in forms:
        m = _PY_PREFIX[form].match(title)
        if m and (form not in _NEEDS_CAPITAL or unicodedata.category(title[m.end()]) == "Lu"):
            return form, m.group(1)
    return None


def letter_code_shape(prefix):
    """A letter_code prefix's shape: the code with each digit run written 9, case kept ("MP63-19 " -> "MP9-9")."""
    return _DIGITS.sub("9", _PY_CODE_END.sub("", prefix))


def series_key(form, prefix):
    """The series-table key a prefix needs: its form, and for letter_code also its shape ("letter_code:MP9-9")."""
    return f"letter_code:{letter_code_shape(prefix)}" if form == "letter_code" else form


def abstract_number_form(title):
    """The leading-number form of a title (no guards, no scope), or None. Counts toward a source-year's series."""
    found = _form_of(title) if title else None
    return found[0] if found else None


def abstract_number_prefix(title, publication_year=None):
    """(form, prefix) when the title starts with a droppable abstract number, else None.

    The caller still checks the scope (the work's source-year holds a series of this form, see series_sql); this
    function only reads the title.
    """
    found = _form_of(title) if title else None
    if not found:
        return None
    form, prefix = found
    if form == "letter_code":
        letters = _LETTERS.match(prefix).group(0)
        if letters.upper() in MOLECULE_CODE_LETTERS or letters in CASE_SENSITIVE_EXCLUDED_LETTERS:
            return None
        return form, prefix
    if form not in _NO_YEAR_GUARD and publication_year is not None:
        number = int(_DIGITS.search(prefix).group(0))
        if publication_year - 1 <= number <= publication_year + 1:
            return None
    if form in _BARE_NUMBER_FORMS:
        rest = title[len(prefix):]  # starts with a non-space: the prefix pattern's lookahead
        if "/" in rest.split(None, 1)[0] or _PY_QUANTITY.match(rest) or _PY_UNIT.match(rest):
            return None
    return form, prefix


def strip_abstract_number(title, publication_year=None, series_forms=()):
    """The title without its abstract number when its series key (see series_key) is one of series_forms (the
    series the work's source-year holds, from the series table); otherwise the title unchanged."""
    found = abstract_number_prefix(title, publication_year) if series_forms else None
    return title[len(found[1]):] if found and series_key(*found) in series_forms else title


def _sql_str(pattern):
    """A Java regex or plain value as a Spark SQL string literal (backslashes doubled, quotes escaped)."""
    return "'" + pattern.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_list(values):
    return ", ".join(_sql_str(v) for v in values)


def form_sql(title_col):
    """SQL: the leading-number form of title_col, or NULL (abstract_number_form's twin)."""
    whens = " ".join(f"WHEN {title_col} RLIKE {_sql_str(p)} THEN '{f}'" for f, p in SQL_FORM_PATTERNS)
    return f"CASE {whens} END"


def prefix_sql(title_col, form_col):
    """SQL: the prefix of title_col for a form already computed (the matched text, guards not applied)."""
    whens = " ".join(f"WHEN '{f}' THEN regexp_extract({title_col}, {_sql_str(p)}, 1)"
                     for f, p in SQL_PREFIX_PATTERNS.items())
    return f"CASE {form_col} {whens} END"


def guard_sql(title_col, form_col, prefix_col, year_col):
    """SQL: TRUE when abstract_number_prefix's guards keep the number (it belongs to the title).

    Pass column names, not expressions: each is written into the SQL several times."""
    rest = f"substr({title_col}, length({prefix_col}) + 1)"
    number = f"CAST(regexp_extract({prefix_col}, '([0-9]+)', 1) AS INT)"
    first_token = _sql_str(r"^(\S+)")  # no backslash inside f-string braces: Python < 3.12 refuses it
    letters = f"regexp_extract({prefix_col}, '^([A-Za-z]+)', 1)"
    year_guard = (f"({form_col} NOT IN ({_sql_list(_NO_YEAR_GUARD)}) "
                  f"AND {number} BETWEEN {year_col} - 1 AND {year_col} + 1)")
    letter_guard = (f"({form_col} = 'letter_code' AND (upper({letters}) IN ({_sql_list(sorted(MOLECULE_CODE_LETTERS))}) "
                    f"OR {letters} IN ({_sql_list(sorted(CASE_SENSITIVE_EXCLUDED_LETTERS))})))")
    word_guard = (f"({form_col} IN ({_sql_list(_BARE_NUMBER_FORMS)}) AND ("
                  f"regexp_extract({rest}, {first_token}, 1) RLIKE '/' "
                  f"OR {rest} RLIKE {_sql_str(_SQL_QUANTITY)} "
                  f"OR {rest} RLIKE {_sql_str(_UNIT)}))")
    return f"coalesce({year_guard} OR {word_guard} OR {letter_guard}, false)"


def series_key_sql(form_col, prefix_col):
    """SQL: series_key's twin (the form, or for letter_code 'letter_code:<shape>')."""
    shape = f"regexp_replace(regexp_replace({prefix_col}, {_sql_str(_CODE_END)}, ''), '[0-9]+', '9')"
    return f"CASE WHEN {form_col} = 'letter_code' THEN concat('letter_code:', {shape}) ELSE {form_col} END"


def series_sql(works_table="openalex.works.openalex_works", excluded_table=None, prefixes_table=None):
    """SQL: one row per (source_id, year, series_key) that holds an abstract series, from a works table shaped like
    openalex_works. A work's leading number is dropped only when its (source, year, series_key) is here.

    A series: a journal or conference source-year with >= 20 titles of one form (series key), most of their numbers
    distinct, num_dot only with a supplement or conference-abstract signal (step 1), and, among the titles whose
    number passes the guards (step 3, 19 Nov 2026): at least 20 of them; not mostly short (<= 4 words after the
    number: session headings, catalogue entries, report sections); for the bare-number forms, not mostly numbers
    from 1800 to the year after the work's year (titles that start with a year); not chapters (median >= 10 pages
    per item, on distinct first pages). Sources in the exclusion list never hold a series: excluded_table (rows
    source_id, year or NULL for all years, series_key or NULL for all) when given, else EXCLUDED_SOURCE_IDS.

    Read titles AS DEPOSITED. openalex_works.title always is (the strip happens at publish, step 4); prefixes_table
    (work_id, prefix) is only for a works table whose titles were stripped, and puts the number back.
    """
    supplement = "fp RLIKE '^[A-Za-z]' OR lower(coalesce(issue, '')) RLIKE 'sup|s[0-9]|[0-9]s'"
    conference_abstract = "type = 'conference-abstract'"
    if prefixes_table:
        title = "concat(coalesce(n.prefix, ''), w.title)"
        source = f"{works_table} w LEFT JOIN {prefixes_table} n ON n.work_id = w.id"
    else:
        title, source = "w.title", f"{works_table} w"
    latin = _sql_str(r"^[\p{IsLatin}\p{P}\p{S}\s0-9]+$")
    span_ok = (_sql_str(r"^[A-Za-z]*[0-9]{1,6}$"))
    page_letters = "'^([A-Za-z]*)'"
    page_number = "'([0-9]+)$'"
    if excluded_table:
        excluded = (f"LEFT ANTI JOIN {excluded_table} e ON e.source_id = a.source_id"
                    f" AND (e.year IS NULL OR e.year = a.year) AND (e.series_key IS NULL OR e.series_key = a.series_key)")
    else:
        excluded = f"WHERE a.source_id NOT IN ({_sql_list(EXCLUDED_SOURCE_IDS)})"
    return f"""
WITH w AS (
  SELECT w.primary_location.source.id AS source_id, w.publication_year AS year, w.type, {title} AS title,
         w.biblio.first_page AS fp, w.biblio.last_page AS lp, w.biblio.issue AS issue
  FROM {source}
  WHERE w.primary_location.source.type IN ({_sql_list(SOURCE_TYPES)})
    AND w.type NOT IN ({_sql_list(EXCLUDED_WORK_TYPES)})
    AND ({title} RLIKE '^[#0-9]' OR substr({title}, 1, 12) RLIKE '^[A-Za-z]{{1,6}}[0-9]{{0,2}}-?[0-9]')
), f AS (
  SELECT *, {form_sql('title')} AS form FROM w
), p AS (
  SELECT *, {prefix_sql('title', 'form')} AS prefix FROM f WHERE form IS NOT NULL
), k AS (
  SELECT *, {series_key_sql('form', 'prefix')} AS series_key,
    CASE WHEN form = 'letter_code' THEN regexp_replace(prefix, {_sql_str(_CODE_END)}, '')
         ELSE CAST(CAST(regexp_extract(title, '([0-9]+)', 1) AS INT) AS STRING) END AS num,
    trim(substr(title, length(prefix) + 1)) AS rest,
    CASE WHEN regexp_extract(fp, {page_letters}, 1) = regexp_extract(lp, {page_letters}, 1)
          AND fp RLIKE {span_ok} AND lp RLIKE {span_ok}
          AND CAST(regexp_extract(lp, {page_number}, 1) AS INT) >= CAST(regexp_extract(fp, {page_number}, 1) AS INT)
         THEN CAST(regexp_extract(lp, {page_number}, 1) AS INT) - CAST(regexp_extract(fp, {page_number}, 1) AS INT) + 1
    END AS span
  FROM p
), g AS (
  SELECT *, {guard_sql('title', 'form', 'prefix', 'year')} AS guarded FROM k
), a AS (
  SELECT source_id, year, form, series_key, count(*) AS n_titles, count(DISTINCT num) AS n_distinct,
    count_if({supplement}) AS n_supplement,
    count_if({conference_abstract}) AS n_conference_abstract,
    count_if(NOT guarded) AS n_pass,
    count_if(NOT guarded AND rest RLIKE {latin}) AS n_pass_latin,
    count_if(NOT guarded AND rest RLIKE {latin} AND size(split(rest, '\\\\s+')) <= {SHORT_TITLE_WORDS}) AS n_pass_short,
    count_if(NOT guarded AND form IN ({_sql_list(_BARE_NUMBER_FORMS + ("hash",))})
             AND CAST(regexp_extract(prefix, '([0-9]+)', 1) AS INT) BETWEEN {FIRST_YEAR_NUMBER} AND year + 1)
      AS n_pass_year_number,
    count_if(NOT guarded AND span IS NOT NULL) AS n_pass_paged,
    percentile_approx(CASE WHEN NOT guarded THEN span END, 0.5) AS median_pages,
    count(DISTINCT CASE WHEN NOT guarded AND span IS NOT NULL THEN fp END) AS n_pass_paged_first_pages
  FROM g
  GROUP BY source_id, year, form, series_key
  HAVING count(*) >= {MIN_SERIES_TITLES}
    AND (form = 'code'
         OR (form = 'letter_code' AND count(DISTINCT num) >= {MIN_LETTER_CODE_DISTINCT_SHARE} * count(*))
         OR (form NOT IN ('code', 'letter_code') AND count(DISTINCT num) >= {MIN_DISTINCT_SHARE} * count(*)))
    AND (form <> 'num_dot'
         OR count_if({supplement}) >= {MIN_NUM_DOT_SUPPLEMENT_SHARE} * count(*)
         OR count_if({conference_abstract}) >= {MIN_NUM_DOT_SUPPLEMENT_SHARE} * count(*))
    AND count_if(NOT guarded) >= {MIN_SERIES_TITLES}
)
SELECT a.* FROM a
{excluded}
{"AND" if not excluded_table else "WHERE"} NOT (a.n_pass_latin >= {MAX_SHORT_TITLE_SHARE} * a.n_pass
       AND a.n_pass_short >= {MAX_SHORT_TITLE_SHARE} * a.n_pass_latin)
  AND a.n_pass_year_number < {MAX_YEAR_NUMBER_SHARE} * a.n_pass
  AND NOT (a.n_pass_paged >= 0.5 * a.n_pass AND coalesce(a.median_pages, 0) >= {MIN_CHAPTER_PAGES}
           AND a.n_pass_paged_first_pages >= {MIN_CHAPTER_DISTINCT_FIRST_PAGE_SHARE} * a.n_pass_paged)
""".strip()


# The frozen series table and its exclusion list (D7 step 3, 19 Nov 2026). How a series gets in, and out:
#
# - In: series_append_sql adds the (source_id, year, series_key) rows series_sql finds in titles AS DEPOSITED that the
#   table doesn't hold yet, stamped with added_on and RULE_VERSION. It never changes or deletes a row, so a series
#   once in stays in (a work's title doesn't flip back and forth as its source-year gains or loses works), and a new
#   meeting's supplement joins the night its 20th numbered abstract arrives. Run it before the strip, each night.
# - Out: a row in EXCLUDED_TABLE (source_id; year and series_key NULL for all), with the reason and the oxjob. The
#   strip anti-joins the exclusion table when it reads the series table, so the titles get their numbers back the
#   next night; the series row stays as the record of what the rules found. Never delete series rows by hand.
# - New rules: change the constants above and RULE_VERSION. They only decide what new rows come in; to take out a
#   series the new rules would refuse, add exclusion rows.
SERIES_TABLE = "openalex.works.title_abstract_number_series"
EXCLUDED_TABLE = "openalex.works.title_abstract_number_excluded"
RULE_VERSION = "2026-11-19"  # step 1 + 1b + 3 rules
_SERIES_COLUMNS = ("source_id", "year", "form", "series_key", "n_titles", "n_distinct", "n_supplement",
                   "n_conference_abstract", "n_pass", "n_pass_latin", "n_pass_short", "n_pass_year_number",
                   "n_pass_paged", "median_pages", "n_pass_paged_first_pages")


def series_table_ddl(table=SERIES_TABLE):
    """SQL: create the series table if it isn't there (columns: series_sql's, plus added_on and rule_version)."""
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  source_id STRING, year INT, form STRING, series_key STRING, n_titles BIGINT, n_distinct BIGINT,
  n_supplement BIGINT, n_conference_abstract BIGINT, n_pass BIGINT, n_pass_latin BIGINT, n_pass_short BIGINT,
  n_pass_year_number BIGINT, n_pass_paged BIGINT, median_pages INT, n_pass_paged_first_pages BIGINT,
  added_on DATE, rule_version STRING)
COMMENT 'oxjob #1162 (ruling D7): source-years whose leading conference abstract numbers the displayed title drops. Append-only (openalex.dlt.abstract_number.series_append_sql); take a series out with a row in {EXCLUDED_TABLE}.'
""".strip()


def excluded_table_ddl(table=EXCLUDED_TABLE):
    """SQL: create the exclusion table if it isn't there."""
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  source_id STRING NOT NULL, year INT, series_key STRING, reason STRING, oxjob STRING, added_on DATE)
COMMENT 'oxjob #1162 (ruling D7): sources (or source-years, or one series) whose leading numbers are never dropped from a title, whatever {SERIES_TABLE} says. year / series_key NULL = all.'
""".strip()


def excluded_seed_sql(table=EXCLUDED_TABLE, added_on="2026-11-19"):
    """SQL: insert EXCLUDED_SOURCES as whole-source rows, skipping any the table already holds."""
    values = ", ".join(f"({_sql_str(source_id)}, {_sql_str(reason)})" for source_id, reason in EXCLUDED_SOURCES)
    return f"""
INSERT INTO {table} (source_id, year, series_key, reason, oxjob, added_on)
SELECT v.source_id, CAST(NULL AS INT), CAST(NULL AS STRING), v.reason, '1162', DATE'{added_on}'
FROM VALUES {values} AS v(source_id, reason)
LEFT ANTI JOIN {table} t ON t.source_id = v.source_id AND t.year IS NULL AND t.series_key IS NULL
""".strip()


def series_append_sql(series_table=SERIES_TABLE, excluded_table=EXCLUDED_TABLE,
                      works_table="openalex.works.openalex_works", prefixes_table=None):
    """SQL: append the series series_sql finds (titles as deposited, exclusions applied) that series_table lacks."""
    cols = ", ".join(f"s.{c}" for c in _SERIES_COLUMNS)
    return f"""
INSERT INTO {series_table} ({", ".join(_SERIES_COLUMNS)}, added_on, rule_version)
SELECT {cols}, current_date(), '{RULE_VERSION}'
FROM ({series_sql(works_table, excluded_table, prefixes_table)}) s
LEFT ANTI JOIN {series_table} t ON t.source_id <=> s.source_id AND t.year <=> s.year AND t.series_key <=> s.series_key
""".strip()


BUILDER_NOTEBOOK = "notebooks/maintenance/BuildTitleAbstractNumberSeries.sql"  # from the walden repo root


def builder_notebook_sql(series_table=SERIES_TABLE, excluded_table=EXCLUDED_TABLE,
                         works_table="openalex.works.openalex_works"):
    """The Databricks SQL notebook that creates the tables and appends new series (BUILDER_NOTEBOOK is this text;
    tests/test_abstract_number.py fails when they differ). Regenerate from libraries/dlt_utils:
    python3 -c "from openalex.dlt.abstract_number import builder_notebook_sql as b; print(b(), end='')" \\
      > ../../notebooks/maintenance/BuildTitleAbstractNumberSeries.sql
    """
    cells = [
        f"""-- oxjob #1162, ruling D7 (16 Nov 2026), step 3: the frozen series table for conference abstract numbers.
-- GENERATED from libraries/dlt_utils/openalex/dlt/abstract_number.py (builder_notebook_sql, rules {RULE_VERSION}):
-- edit the module, then regenerate (its docstring says how); a test fails when this file and the module differ.
--
-- A conference abstract's leading program number or code leaves its displayed title ("#303 Risk-reducing ...",
-- "46: THE JULY EFFECT ...", "Mo1428 Hemoclip ...") only when the work's source-year holds a series of that form:
-- {series_table} holds those series, {excluded_table} takes one out.
-- Append-only: each run adds the series the rules find in titles AS DEPOSITED (openalex_works.title is never
-- stripped: the strip happens at publish) that the table lacks, dated, and never changes or deletes a row.
-- Take a series out with a row in {excluded_table} (source_id; year and series_key NULL = all), never a DELETE.
-- CreateWorksEnriched runs the same statements each night once the strip is switched on (#1162 PLAN, D7 steps 4
-- and 6); run this notebook by hand only to create and inspect the tables before that.
-- Measured 19 Nov 2026 on openalex_works: 6,738 series, 1,252,251 works (#1162 work/d7_step3_series_2026-11-19.md;
-- this SQL built openalex.oxlane.oxjob1162_d7_series_frozen). Revert: DROP the two tables (no reader until the switch).""",
        excluded_table_ddl(excluded_table) + ";",
        excluded_seed_sql(excluded_table) + ";",
        series_table_ddl(series_table) + ";",
        series_append_sql(series_table, excluded_table, works_table) + ";",
        f"SELECT rule_version, added_on, count(*) AS series, sum(n_pass) AS works\nFROM {series_table}\n"
        "GROUP BY rule_version, added_on ORDER BY added_on;",
    ]
    return "-- Databricks notebook source\n" + "\n\n-- COMMAND ----------\n\n".join(cells) + "\n"


# D7 step 4 (19 Nov 2026): the strip, at publish. Weighed in oxjob #1162 work/d7_step4_weigh_2026-11-19.md.
#
# CreateWorksEnriched adds TITLE_PREFIX_COLUMN to tonight's build (ENRICHED_COLUMN_CELL, always) and, with the switch on,
# sets it on the works whose leading number their series drops (ENRICHED_STRIP_CELL), before the content hash cell,
# which hashes the column when it is set: with the switch off it is NULL everywhere and no work's hash changes; switched
# on, exactly the works that carry a prefix re-stamp, once (and later a work whose prefix starts or stops). `title` is
# never changed. The publishers serve display_title_sql() as the work's title and display_name (PUBLISH_FILES hold
# copies of it; a test checks each). STRIP_SWITCH_ON is the switch: flip it, regenerate the cell (see
# enriched_strip_cell_sql), push before the night's cut-off with the night's pre-clear (D7 step 6).
TITLE_PREFIX_COLUMN = "title_abstract_number"
BUILD_TABLE = "openalex.works.openalex_works_build"
PUBLISHED_TABLE = "openalex.works.openalex_works"
CURATED_FIELDS_TABLE = "openalex.works.work_curated_fields"  # a curated title (users-api, oxjob #1162) is never stripped
STRIP_SWITCH_ON = False
ENRICHED_NOTEBOOK = "notebooks/end2end/CreateWorksEnriched.ipynb"
ENRICHED_COLUMN_CELL = "-- oxjob #1162 (ruling D7, step 4): openalex_works_base does not carry title_abstract_number"
ENRICHED_STRIP_CELL = "-- oxjob #1162, ruling D7 (16 Nov 2026), step 4: the conference abstract number a work's displayed title drops."
PUBLISH_FILES = (  # from the walden repo root; each holds display_title_sql() verbatim
    "notebooks/elastic/sync_works.ipynb",
    "notebooks/lakebase/BuildLakebaseWorksDocs.py",
    "notebooks/snapshot/export_works.ipynb",
    "notebooks/elastic/IndexRebuild.py",
)


def display_title(title, prefix):
    """The title a work is served with: without its abstract number when one is stored (display_title_sql's twin)."""
    return title[len(prefix):] if prefix and title and title.startswith(prefix) else title


def display_title_sql(title_col="title", prefix_col=TITLE_PREFIX_COLUMN):
    """SQL: the served title. Drops the stored prefix only when the title still starts with it (a title that changed
    since the prefix was stored is served whole)."""
    return (f"CASE WHEN {prefix_col} IS NOT NULL AND substr({title_col}, 1, length({prefix_col})) = {prefix_col} "
            f"THEN substr({title_col}, length({prefix_col}) + 1) ELSE {title_col} END")


def work_prefixes_sql(works_table=BUILD_TABLE, series_table=SERIES_TABLE, excluded_table=EXCLUDED_TABLE,
                      curated_table=CURATED_FIELDS_TABLE):
    """SQL: one row per work whose displayed title drops its leading number (id, prefix, form, series_key): the title
    (as deposited) has a prefix that passes the guards, of a series key its source-year holds in series_table, not
    taken out in excluded_table, and the work's title is not curated. The same filters as series_sql's input."""
    return f"""
WITH w AS (
  SELECT w.id, w.title, w.type, w.publication_year AS year, w.primary_location.source.id AS source_id
  FROM {works_table} w
  WHERE w.primary_location.source.type IN ({_sql_list(SOURCE_TYPES)})
    AND w.type NOT IN ({_sql_list(EXCLUDED_WORK_TYPES)})
    AND (w.title RLIKE '^[#0-9]' OR substr(w.title, 1, 12) RLIKE '^[A-Za-z]{{1,6}}[0-9]{{0,2}}-?[0-9]')
), f AS (
  SELECT *, {form_sql('title')} AS form FROM w
), p AS (
  SELECT *, {prefix_sql('title', 'form')} AS prefix FROM f WHERE form IS NOT NULL
), k AS (
  SELECT *, {series_key_sql('form', 'prefix')} AS series_key FROM p
), g AS (
  SELECT *, {guard_sql('title', 'form', 'prefix', 'year')} AS guarded FROM k
)
SELECT g.id, g.prefix, g.form, g.series_key FROM g
LEFT SEMI JOIN {series_table} s ON s.source_id = g.source_id AND s.year = g.year AND s.series_key = g.series_key
LEFT ANTI JOIN {excluded_table} e ON e.source_id = g.source_id
  AND (e.year IS NULL OR e.year = g.year) AND (e.series_key IS NULL OR e.series_key = g.series_key)
LEFT ANTI JOIN (SELECT work_id FROM {curated_table} WHERE title IS NOT NULL) c ON c.work_id = g.id
WHERE NOT g.guarded
""".strip()


def enriched_column_cell_sql(build_table=BUILD_TABLE):
    """The CreateWorksEnriched cell that adds the column to tonight's build (runs whatever the switch says)."""
    return f"""{ENRICHED_COLUMN_CELL}: it is added to
-- the build here (appended last, after #1610's alternate_titles) and stays NULL on every work whose displayed title keeps
-- its whole title, which is every work while the strip is switched off (the next cell). GENERATED from
-- libraries/dlt_utils/openalex/dlt/abstract_number.py (enriched_column_cell_sql), and a test fails when they differ.
ALTER TABLE {build_table} ADD COLUMNS (
  {TITLE_PREFIX_COLUMN} STRING COMMENT 'oxjob #1162 (ruling D7): the conference abstract number the served title drops (internal, not an API field), NULL when none, title keeps it.'
)"""


def enriched_strip_cell_sql(switch_on=None, build_table=BUILD_TABLE, published_table=PUBLISHED_TABLE,
                            series_table=SERIES_TABLE, excluded_table=EXCLUDED_TABLE,
                            curated_table=CURATED_FIELDS_TABLE):
    """The CreateWorksEnriched cell that sets the column (CreateWorksEnriched.ipynb holds this text; a test fails when
    they differ). Switched off it is one SELECT that changes nothing; switched on, a SQL scripting block. Regenerate
    from libraries/dlt_utils after changing the module or the switch:
    python3 -c "from openalex.dlt.abstract_number import write_enriched_cells as w; w()"
    """
    on = STRIP_SWITCH_ON if switch_on is None else switch_on
    col = TITLE_PREFIX_COLUMN
    header = f"""{ENRICHED_STRIP_CELL}
-- GENERATED from libraries/dlt_utils/openalex/dlt/abstract_number.py (enriched_strip_cell_sql, rules {RULE_VERSION},
-- switch STRIP_SWITCH_ON = {on}): edit the module, then regenerate (a test fails when this cell and the module differ).
-- Ruling D7 (oxjobs RULINGS.md, section 3, Work titles): a conference abstract's leading program number or code leaves
-- its displayed title ("#303 Risk-reducing ...", "46: THE JULY EFFECT ...", "2022-RA-1046-ESGO ..."). Here it is
-- stored, not stripped: {col} holds the number and `title` keeps it, so every key, matcher, topic and
-- model input reads the title as deposited. sync_works, BuildLakebaseWorksDocs and export_works serve the title without
-- it (abstract_number.display_title_sql). Before the hash cell, which hashes the column when set: a work re-stamps when
-- its prefix starts or stops."""
    if not on:
        return header + f"""
-- Switched off: this cell changes nothing, {col} stays NULL on every work and no content hash changes.
-- Switched on (oxjob #1162 D7 step 6), it (1) creates the series and exclusion tables if missing and tops the series up
-- from tonight's build, append-only, and (2) sets {col} on the works whose number their series drops
-- (1,252,240 on 19 Nov 2026), keeping last night's values on any error.
SELECT 'D7 title strip: switched off (abstract_number.STRIP_SWITCH_ON), {col} stays NULL' AS d7_title_strip"""
    schema, table = published_table.split(".")[1:]
    prefixes = work_prefixes_sql(build_table, series_table, excluded_table, curated_table).replace("\n", "\n    ")
    append = series_append_sql(series_table, excluded_table, build_table).replace("\n", "\n  ")
    seed = excluded_seed_sql(excluded_table).replace("\n", "\n  ")
    return header + f"""
-- Switched on: (1) the series and exclusion tables are created if missing and topped up from tonight's build (append-
-- only; a series leaves only by a row in {excluded_table}); (2) the works whose number their series
-- drops get it in {col}. Any error: last night's values are kept and the night publishes as usual.
-- Revert: STRIP_SWITCH_ON = False and regenerate (the next night re-stamps the works back to their whole titles).
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
      SELECT 'D7 title strip: the strip and its fallback both failed; no {col} tonight' AS d7_title_strip;
    IF (SELECT count(*) FROM openalex.information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{col}') > 0 THEN
      MERGE INTO {build_table} AS t
      USING (SELECT id, {col} FROM {published_table} WHERE {col} IS NOT NULL) AS p
      ON t.id = p.id
      WHEN MATCHED THEN UPDATE SET t.{col} = p.{col};
    END IF;
    SELECT 'D7 title strip failed; last night''s {col} kept' AS d7_title_strip;
  END;
  {excluded_table_ddl(excluded_table).replace(chr(10), chr(10) + "  ")};
  {seed};
  {series_table_ddl(series_table).replace(chr(10), chr(10) + "  ")};
  {append};
  MERGE INTO {build_table} AS t
  USING (
    {prefixes}
  ) AS s
  ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET t.{col} = s.prefix;
END"""


def _enriched_notebook_path():
    from pathlib import Path
    return Path(__file__).resolve().parents[4] / ENRICHED_NOTEBOOK


def enriched_cells_in_notebook(path=None):
    """(column cell, strip cell) source text as CreateWorksEnriched.ipynb holds them (None when missing)."""
    import json
    nb = json.loads((path or _enriched_notebook_path()).read_text(encoding="utf-8"))
    found = {}
    for cell in nb["cells"]:
        src = "".join(cell["source"])
        for key in (ENRICHED_COLUMN_CELL, ENRICHED_STRIP_CELL):
            if src.startswith(key):
                found[key] = src
    return found.get(ENRICHED_COLUMN_CELL), found.get(ENRICHED_STRIP_CELL)


def write_enriched_cells(path=None):
    """Rewrite the two cells in CreateWorksEnriched.ipynb from the module (keeps every other cell byte for byte)."""
    import json
    path = path or _enriched_notebook_path()
    text = path.read_text(encoding="utf-8")
    nb = json.loads(text)
    want = {ENRICHED_COLUMN_CELL: enriched_column_cell_sql(), ENRICHED_STRIP_CELL: enriched_strip_cell_sql()}
    n = 0
    for cell in nb["cells"]:
        src = "".join(cell["source"])
        for key, new in want.items():
            if src.startswith(key):
                cell["source"] = new if isinstance(cell["source"], str) else new.splitlines(keepends=True)
                n += 1
    if n != 2:
        raise ValueError(f"expected the two D7 cells in {path}, found {n}")
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
