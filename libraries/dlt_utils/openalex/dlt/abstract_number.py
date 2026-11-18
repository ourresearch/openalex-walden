"""Conference abstract numbers at the start of a work title (oxjob #1162, ruling D7).

Ruling D7 (16 Nov 2026, oxjobs working/policy-decisions/RULINGS.md § 3, Work titles): drop a conference abstract's
leading program number or submission code from its title ("#164 ", "112: ", "2022-RA-1163-ESGO "), keep capitals as
printed. Conditions: one shared normalizer, used by the displayed title and by every title-based merge or match key,
applied to the whole corpus in one night; the bar includes "no existing merge or match is lost".

This module is that normalizer's definition. NOTHING CALLS IT YET: the switch-over is one night for the whole
corpus (oxjob #1162 PLAN.md, D7 build, last step). Until then it changes no data.

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

The Python function and the SQL built here must agree. Checked 18 Nov 2026: the SQL reproduced the measured 539,073
works exactly, and Python and SQL gave the same form, prefix and guard on 1,500 random candidate titles. letter_code:
the SQL reproduced the measured 718,688 works exactly, and Python and SQL gave the same prefix and series key on 23
edge-case titles (separators, trailing letters, dotted and multi-part codes, non-Latin text after the code).
tests/test_abstract_number.py covers the Python side; a Spark parity test belongs with the wiring step.
"""

import re
import unicodedata

FORMS = ("code", "hash", "num_colon", "num_dot", "num_space", "letter_code")

# Works of these types never lose a leading number (books and chapters number chapters; datasets and reference
# entries number items; editorials, errata and paratext are not abstracts).
EXCLUDED_WORK_TYPES = ("book", "book-chapter", "dataset", "reference-entry", "dissertation", "book-review",
                       "editorial", "paratext", "erratum", "retraction")
SOURCE_TYPES = ("journal", "conference")

# Sources whose numbered titles are a magazine's item numbers, not abstract numbers (found in draw C, 18 Nov 2026).
EXCLUDED_SOURCE_IDS = (
    "https://openalex.org/S4210172488",  # Zorg en Financiering: numbered news items ("182 Beklonken")
)

MIN_SERIES_TITLES = 20
MIN_DISTINCT_SHARE = 0.5
MIN_LETTER_CODE_DISTINCT_SHARE = 0.9  # gene-name "series" (IL-6, IL-10, IL-17 ...) repeat codes; program codes don't
MIN_NUM_DOT_SUPPLEMENT_SHARE = 0.5

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


def series_sql(works_table="openalex.works.openalex_works"):
    """SQL: one row per (source_id, year, series_key) that holds an abstract series, from a works table shaped like
    openalex_works. A work's leading number is dropped only when its (source, year, series_key) is here.

    Build it from titles as deposited and keep it frozen (a dated table): once the switch-over strips the displayed
    titles, openalex_works no longer shows the series, and a rebuild from it would find none.
    """
    supplement = "fp RLIKE '^[A-Za-z]' OR lower(coalesce(issue, '')) RLIKE 'sup|s[0-9]|[0-9]s'"
    conference_abstract = "type = 'conference-abstract'"
    return f"""
WITH w AS (
  SELECT primary_location.source.id AS source_id, publication_year AS year, type, title,
         biblio.first_page AS fp, biblio.issue AS issue
  FROM {works_table}
  WHERE primary_location.source.type IN ({_sql_list(SOURCE_TYPES)})
    AND type NOT IN ({_sql_list(EXCLUDED_WORK_TYPES)})
    AND (title RLIKE '^[#0-9]' OR substr(title, 1, 12) RLIKE '^[A-Za-z]{{1,6}}[0-9]{{0,2}}-?[0-9]')
), f AS (
  SELECT *, {form_sql('title')} AS form FROM w
), p AS (
  SELECT *, {prefix_sql('title', 'form')} AS prefix FROM f WHERE form IS NOT NULL
), k AS (
  SELECT *, {series_key_sql('form', 'prefix')} AS series_key,
    CASE WHEN form = 'letter_code' THEN regexp_replace(prefix, {_sql_str(_CODE_END)}, '')
         ELSE CAST(CAST(regexp_extract(title, '([0-9]+)', 1) AS INT) AS STRING) END AS num
  FROM p
)
SELECT source_id, year, form, series_key, count(*) AS n_titles, count(DISTINCT num) AS n_distinct,
  count_if({supplement}) AS n_supplement,
  count_if({conference_abstract}) AS n_conference_abstract
FROM k
WHERE source_id NOT IN ({_sql_list(EXCLUDED_SOURCE_IDS)})
GROUP BY source_id, year, form, series_key
HAVING count(*) >= {MIN_SERIES_TITLES}
  AND (form = 'code'
       OR (form = 'letter_code' AND count(DISTINCT num) >= {MIN_LETTER_CODE_DISTINCT_SHARE} * count(*))
       OR (form NOT IN ('code', 'letter_code') AND count(DISTINCT num) >= {MIN_DISTINCT_SHARE} * count(*)))
  AND (form <> 'num_dot'
       OR count_if({supplement}) >= {MIN_NUM_DOT_SUPPLEMENT_SHARE} * count(*)
       OR count_if({conference_abstract}) >= {MIN_NUM_DOT_SUPPLEMENT_SHARE} * count(*))
""".strip()
