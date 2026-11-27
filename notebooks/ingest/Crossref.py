# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.50-py3-none-any.whl

# COMMAND ----------

import dlt
import re
import unicodedata
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

from openalex.dlt.normalize import normalize_title, normalize_license, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000
unallowed_types = ["component", "grant"]
# oxjob #1135: a Protein Data Bank entry (10.2210/pdbXXXX/pdb) is a whole dataset record, not a piece of an article.
# wwPDB re-deposits each entry as Crossref type `dataset` when it revises it (every deposit since May 2025), and those
# are works (228K on Worldwide Protein Data Bank, S7407063902). The 80,025 entries not revised since still carry the old
# type `component` and were dropped with the figures (ZD 1129: 10.2210/pdb8ehb/pdb, 10.2210/pdb100d/pdb). Read them as
# the dataset deposit: type `dataset` here, and the source named from `publisher` below (their deposit has no container
# title). Untitled records (withdrawn entries, 5,354) and the old alternate DOIs of the same entries (/pdbx, /mmcif: 969)
# stay out. Records the stream already dropped come back by a one-time re-feed (oxjob #1135 PLAN.md). Revert: git revert.
PDB_COMPONENT_SQL = ("coalesce(type = 'component' AND lower(native_id) RLIKE '^10[.]2210/pdb[0-9a-z]{4}/pdb$' "
                     "AND exists(title, x -> trim(x) <> ''), false)")
# oxjob #1722: notices that carry the notice word as a trailing title element (Cureus: ["<article title>",
# "Correction"]). Keeping only title[0] gave the notice its article's exact title, so the type cascade
# typed it article and title keys joined it to the article. Lowercase, exact match on the trimmed element.
NOTICE_TITLE_ELEMENTS = ["correction", "retraction", "expression of concern"]
# oxjob #2160: J-STAGE registrants (JSME, JSRT, Japanese Urological Association, ...) deposit the only title in
# original-title and leave title empty or [""]: 506,419 records, 503,329 visible works served no title (11 Nov 2026).
# When no title element has text, the first original-title element with text is the title. A record with any
# non-blank title element is unchanged, and one with neither keeps title[0] (dry run over 180.1M records: 506,419
# change, all to their original-title; 0 lose a title).
TITLE_BLANK_ORIGINAL_TITLE_SET_SQL = (
    "NOT coalesce(exists(title, x -> trim(x) <> ''), false) AND exists(`original-title`, x -> trim(x) <> '')")
ORIGINAL_TITLE_SQL = "get(filter(`original-title`, x -> trim(x) <> ''), 0)"
# oxjob #2187: an entry with no DOI, no text and no article-title gets its raw text from its fields; bare keys are dropped.
# Entries with an article-title keep raw NULL: #2054 carries their title matches by coalesce(raw, title).
REFERENCES_SQL = """filter(transform(reference, r -> CASE
  WHEN coalesce(nullif(trim(r.DOI), ''), nullif(trim(r.unstructured), ''), nullif(trim(r.`article-title`), '')) IS NOT NULL
  THEN named_struct('doi', r.DOI, 'pmid', CAST(NULL AS STRING), 'arxiv', CAST(NULL AS STRING),
    'title', r.`article-title`, 'authors', r.author, 'year', r.year, 'raw', r.unstructured)
  ELSE named_struct('doi', r.DOI, 'pmid', CAST(NULL AS STRING), 'arxiv', CAST(NULL AS STRING),
    'title', coalesce(nullif(trim(r.`volume-title`), ''), nullif(trim(r.`series-title`), '')),
    'authors', r.author, 'year', r.year,
    'raw', nullif(concat_ws('. ', nullif(trim(TRAILING '.' FROM trim(r.author)), ''), nullif(trim(r.year), ''),
      nullif(trim(TRAILING '.' FROM trim(r.`volume-title`)), ''), nullif(trim(TRAILING '.' FROM trim(r.`series-title`)), ''),
      nullif(concat_ws(', ', nullif(concat_ws(' ', nullif(trim(r.`journal-title`), ''),
        nullif(concat(coalesce(trim(r.volume), ''), coalesce('(' || nullif(trim(r.issue), '') || ')', '')), '')), ''),
        nullif(trim(r.`first-page`), '')), '')), ''))
  END), x -> coalesce(nullif(trim(x.doi), ''), nullif(trim(x.title), ''), x.raw) IS NOT NULL)"""

# oxjob #2240: page strings read the way NCBI reads them (its StartPage / EndPage). MEDLINE writes ranges short
# ("358-60" is 358-360, "C845-53" is C845-C853), lists as "27, 31-2, 34-7" (27-37) or "1001-6; discussion 1006-7"
# (1001-1007), and supplements as "Suppl 4:165-6" (165-166); Crossref deposits chapter pages as "17-1-17-28".
# Splitting at the first hyphen served last_page "60" for 358-360 on ~5.4M works.
# first_page = the first item's start; last_page = the end of the last item that reads as a page or range (leading
# words like "author reply" and a trailing "passim" / "contd" set aside; "2 pp." is a page count and doesn't read), its dropped leading digits put back and the
# start's letter prefix carried. A string whose first item doesn't read, or whose end would run backwards ("358-12"),
# keeps the old split at the first hyphen: never invent a page. Pure SQL, so the same text backfills existing rows.
# PubMed.py holds the same builder (the test checks the copies match). Tests: notebooks/ingest/tests/test_page_ranges.py.
_PG_ITEM = "^[A-Za-z]*[0-9]+(-[A-Za-z]*[0-9]+)?$"
_PG_RANGE = "^([A-Za-z]*)([0-9]+)-([A-Za-z]*)([0-9]+)$"
_PG_COMPOUND = "^([A-Za-z]*[0-9]+-[0-9]+)-([A-Za-z]*[0-9]+-[0-9]+)$"


def page_range_sql(page):
    """SQL for named_struct(first_page, last_page) read from a page-string SQL expression."""
    def end(x):  # last page of a readable item x; NULL when restoring the dropped digits would run it backwards
        p1, n1, p2, n2 = (f"regexp_extract({x}, '{_PG_RANGE}', {i})" for i in (1, 2, 3, 4))
        full = (f"CASE WHEN length({n2}) < length({n1}) "
                f"THEN concat(substr({n1}, 1, length({n1}) - length({n2})), {n2}) ELSE {n2} END")
        return (f"CASE WHEN {x} NOT RLIKE '{_PG_RANGE}' THEN {x} "
                f"WHEN length({n2}) < length({n1}) AND {full} < {n1} THEN NULL "
                f"ELSE concat(CASE WHEN {p2} <> '' THEN {p2} ELSE {p1} END, {full}) END")
    items = ("transform(split(regexp_replace(pg_s, '^[Ss][Uu][Pp][Pp][^:]*: *', ''), '[,;]'), (pg_x, pg_n) -> "
             "regexp_replace(CASE WHEN pg_n > 0 THEN regexp_replace(trim(pg_x), '^[A-Za-z][A-Za-z ]*[A-Za-z] +', '') "
             "ELSE trim(pg_x) END, '(?i) +(passim|contd|cont|continued)[.]?$', ''))")
    read = (f"get(transform(array({items}), pg_i -> CASE WHEN get(pg_i, 0) RLIKE '{_PG_ITEM}' THEN "
            f"get(transform(array(try_element_at(filter(pg_i, pg_x -> pg_x RLIKE '{_PG_ITEM}'), -1)), pg_l -> "
            f"CASE WHEN {end('pg_l')} IS NOT NULL THEN named_struct('first_page', "
            f"regexp_extract(get(pg_i, 0), '^([A-Za-z]*[0-9]+)', 1), 'last_page', {end('pg_l')}) END), 0) END), 0)")
    compound = (f"CASE WHEN pg_s RLIKE '{_PG_COMPOUND}' THEN named_struct("
                f"'first_page', regexp_extract(pg_s, '{_PG_COMPOUND}', 1), "
                f"'last_page', regexp_extract(pg_s, '{_PG_COMPOUND}', 2)) END")
    old_first = f"get(split({page}, '-'), 0)"
    old = f"named_struct('first_page', {old_first}, 'last_page', coalesce(get(split({page}, '-'), 1), {old_first}))"
    return f"get(transform(array(trim({page})), pg_s -> coalesce({compound}, {read}, {old})), 0)"


CROSSREF_PAGES_SQL = page_range_sql("page")

# Affiliation-as-Author Fix - Publishers excluded from filtering
# These publishers have < 0.1% affected rate and should not have their records modified
AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS = [
    "ACM",
    "Acoustical Society of America (ASA)",
    "AIP Publishing",
    "American Chemical Society (ACS)",
    "American Geophysical Union (AGU)",
    "American Institute of Aeronautics & Astronautics",
    "American Society of Civil Engineers (ASCE)",
    "American Society of Mechanical Engineers",
    "Association for Computing Machinery (ACM)",
    "ASTM International",
    "Atlantis Press",
    "Bentham Science Publishers Ltd.",
    "BRILL",
    "CAIRN",
    "Cambridge University Press",
    "Center for Open Science",
    "CRC Press",
    "De Gruyter",
    "Duke University Press",
    "Edward Elgar Publishing",
    "Egyptian Knowledge Bank",
    "Egypts Presidential Specialized Council",
    "Elsevier",
    "Emerald",
    "ENCODE Data Coordination Center",
    "Georg Thieme Verlag KG",
    "H1 Connect",
    "Hans Publishers",
    "IEEE",
    "IGI Global",
    "Inderscience Publishers",
    "Informa UK Limited",
    "Institute of Electrical and Electronics Engineers (IEEE)",
    "Institution of Engineering and Technology (IET)",
    "International Union of Crystallography (IUCr)",
    "IUCN",
    "Japan Society of Mechanical Engineers",
    "Nomos Verlagsgesellschaft mbH & Co. KG",
    "OpenEdition",
    "Oxford University Press",
    "PERSEE Program",
    "Project MUSE",
    "Research Square Platform LLC",
    "Routledge",
    "Royal Society of Chemistry (RSC)",
    "SAE International",
    "Sciencedomain International",
    "Scientific Research Publishing, Inc.",
    "SPIE",
    "Springer Berlin Heidelberg",
    "Springer International Publishing",
    "Springer Nature Singapore",
    "Springer Nature Switzerland",
    "The Conversation",
    "The Electrochemical Society",
    "The Royal Society",
    "Trans Tech Publications, Ltd.",
    "transcript Verlag",
    "Universidade de Sao Paulo",
    "University of Chicago Press",
    "VS Verlag fur Sozialwissenschaften",
    "Walter de Gruyter GmbH",
    "World Scientific Pub Co Pte Lt",
]

# Institution keywords regex for detecting affiliation-as-author entries (case-insensitive)
# Uses a hybrid approach to avoid false positives from surnames containing institution keywords:
# - Long institution keywords: NO word boundaries (catches concatenated forms like "KazanUniversity")
# - Short corporate keywords: WITH word boundaries (avoids false positives like "Vincent" containing "Inc")
# - School/Center: Special handling in is_valid_author() to avoid surnames like Schooler, Centerwall

# Pattern for keywords EXCLUDING School and Center (used for family field)
INSTITUTION_KEYWORDS_PATTERN_NO_SCHOOL_CENTER = (
    r"(?i)("
    # Long institution keywords - no word boundaries needed
    r"University|Institute|College|Hospital|Department|Centre|"
    r"Laboratory|Faculty|Academy|"
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    r"Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|"
    r"Clinic|Museum|Library|Foundation|Polytechnic"
    r")"
    r"|"
    # Short corporate keywords - WITH word boundaries
    r"\b(Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Medical|Research)\b"
)

# Full pattern including School and Center (used for given field and name-only pattern)
INSTITUTION_KEYWORDS_PATTERN = (
    r"(?i)("
    r"University|Institute|College|Hospital|Department|School|Center|Centre|"
    r"Laboratory|Faculty|Academy|"
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    r"Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|"
    r"Clinic|Museum|Library|Foundation|Polytechnic"
    r")"
    r"|"
    r"\b(Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Medical|Research)\b"
)

@F.pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@F.pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

# COMMAND ----------

def affiliation_string(aff):
    """oxjob #1242: the raw affiliation string is the publisher's whole affiliation, `name` then its `place` values.

    Crossref affiliation objects can carry the city / country in `place`, apart from `name` (ACS on ~88% of its records
    since mid-2025; also Taylor & Francis, OUP, IEEE, Emerald, Frontiers, PNAS, AIP ...: ~16% of 2025-26 records with
    affiliations, sample of 600 on 16 Nov 2026). Reading only `name` served "University of South Carolina" for
    name "University of South Carolina" + place ["Columbia, United States"], and the matcher lost the country.
    Format: name, then each place value not already in the name, joined by ", " (the "name, city, country" form of
    the matcher's own institution names, utils/affiliation_matcher/nightly.py names_for_dense). Trailing separators
    on the name (ACS: "Dresden University of Technology , , Nöthnitzer Str. 61 , ,") go before the join. A name
    with no letter, or no new place, keeps its bytes, so records without `place` are unchanged.
    Revert: return aff["name"] (then re-feed the same DOIs).
    """
    name = aff["name"]
    places = F.transform(
        F.filter(F.coalesce(aff["place"], F.array().cast("array<string>")), lambda p: F.trim(p) != ""),
        lambda p: F.trim(p))
    new_places = F.filter(places, lambda p: ~F.lower(name).contains(F.lower(p)))
    return F.when(
        name.rlike(r"\p{L}") & (F.size(new_places) > 0),
        F.concat_ws(", ", F.regexp_replace(name, r"[\s,;]+$", ""), F.array_join(new_places, ", "))
    ).otherwise(name)

def create_author_struct(author):
    """Transform raw Crossref author to normalized author struct."""
    return F.struct(
        F.substring(author["given"], 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
        F.substring(author["family"], 0, MAX_AUTHOR_NAME_LENGTH).alias("family"),
        F.substring(author["name"], 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
        F.regexp_extract(
            author["ORCID"], r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])", 1
        ).alias("orcid"),
        F.transform(
            author["affiliation"],
            lambda aff: F.struct(
                F.substring(affiliation_string(aff), 0, MAX_AFFILIATION_STRING_LENGTH).alias("name"),  # oxjob #1242
                F.substring(F.get(aff["department"], 0), 0, MAX_AFFILIATION_STRING_LENGTH).alias("department"),
                F.when(
                    F.get(aff["id"]["id-type"], 0) == "ROR",
                    F.get(aff["id"]["id"], 0)
                ).alias("ror_id")
            )
        ).alias("affiliations")
    )

def is_valid_author(author):
    """
    Returns True if the author is NOT an affiliation-as-author entry.

    Detects two patterns:
    1. Institution keywords in given OR family fields (e.g., given="Kazan", family="University")
    2. Institution keywords in name field when given/family are empty
       (e.g., name="University of Professional Studies, Accra, Ghana")

    Special handling for School and Center keywords in family field to avoid
    false positives from surnames like Schooler, Schooling, Centerwall:
    - School: only matches if family is exactly "School" or contains "Schoolof"
    - Center: only matches if family ends with "Center" or contains "Centerof"
    """
    family = F.coalesce(author["family"], F.lit(""))
    given = F.coalesce(author["given"], F.lit(""))
    name = F.coalesce(author["name"], F.lit(""))

    # Pattern 1a: Non-School/Center keywords in family
    has_other_institution_in_family = family.rlike(INSTITUTION_KEYWORDS_PATTERN_NO_SCHOOL_CENTER)

    # Pattern 1b: School in family - only if exactly "School" or contains "Schoolof"
    # This avoids surnames like Schooler, Schooling, Schoolfield, etc.
    has_school_in_family = (
        family.rlike(r"(?i)^School$") |  # Exactly "School"
        family.rlike(r"(?i)Schoolof")     # Contains "Schoolof" (e.g., SchoolofMedicine)
    )

    # Pattern 1c: Center in family - only if ends with "Center" or contains "Centerof"
    # This avoids surnames like Centers, Centerwall
    has_center_in_family = (
        family.rlike(r"(?i)Center$") |    # Ends with "Center" (e.g., ResearchCenter)
        family.rlike(r"(?i)Centerof")     # Contains "Centerof" (e.g., CenterofRAS)
    )

    # Pattern 1d: Any institution keyword in given field (use full pattern)
    # School/Center in given is typically part of institution name, not surname
    has_institution_in_given = given.rlike(INSTITUTION_KEYWORDS_PATTERN)

    # Combined Pattern 1: institution in given or family
    has_institution_in_given_family = (
        has_other_institution_in_family |
        has_school_in_family |
        has_center_in_family |
        has_institution_in_given
    )

    # Pattern 2: Institution in name field when given/family are empty
    has_institution_in_name_only = (
        (F.trim(given) == "") &
        (F.trim(family) == "") &
        name.rlike(INSTITUTION_KEYWORDS_PATTERN)
    )

    return ~(has_institution_in_given_family | has_institution_in_name_only)

# oxjob #2183: name-only contributors (no given/family) that are affiliation or address lines, in any language.
# Publishers deposit affiliations as Crossref organisation contributors ("name" only); the English keyword list above
# missed ~97% of them (~500K seats on ~310K works, e.g. W4214559618: "中国科学院地理科学与资源研究所,...,北京100101").
# Applied ON TOP of is_valid_author (a contributor is kept only if both keep it), and only to name-only entries:
# widening the given/family branch dropped real people (Vietnamese "Trường", surnames Pusat/Balai/Hospedales).
# Words that are also surnames (AMBIGUOUS_LATIN) drop only a string that is not shaped like a person's name
# ("Jacqueline R. Center", "S. Centro" stay); CJK/Hangul words need 5+ characters (person names are 2-4).
# Tested read-only on crossref_parsed 13 and 25 Nov 2026: 507,575 drops on 311,981 records, 0 people in 450 Opus-judged
# drops (oxjob #2183 evidence/FILTER_DRAFT.md, evidence/SHIP.md). Revert: use is_valid_author alone in `authors`.
AFF_STRONG_LATIN = (
    r"(?i)("
    r"Universit|Universidad|Universidade|Universiteit|Universitet|Univerzit|Uniwersytet|Üniversite|"
    r"Institut|Instytut|Enstitü|Laborator|Laboratoire|Faculty|Facult|Fakult|Fakültesi|Department|Departament|"
    r"Département|Departemen|Hochschule|Klinikum|Krankenhaus|Politecnic|Politechnika|Polytechn|Politeknik|"
    r"Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|Ministry|Ministerio|"
    r"Ministère|Kementerian|Akademi|Academia|Hôpital|Hospitais|Sekolah Tinggi|Program Studi|"
    r"Jurusan|Wydział|Katedra|Bölümü|Egyetem|Polyclinic|Đại học|Học viện|Viện Nghiên cứu|Viện Khoa học|"
    r"Pusat Penelitian|Pusat Studi|Balai Besar|Balai Penelitian|Lembaga Ilmu|Lembaga Penelitian"
    r")"
    r"|"
    r"\b(Inc|LLC|Ltd|Corp|Corporation|GmbH|Ospedale|Ústav|Fakulta|Unidad|Hastanesi|Viện|Szpital|Sveučilište|"
    r"Division of)\b"
    r"|"
    r"\b(Univ|Dept)\."
)
AFF_AMBIGUOUS_LATIN = (
    r"(?i)\b(Center|Centre|Centro|Centrum|Hospital|College|School|Escola|Academy|Clinic|Klinik|Klinika|Museum|"
    r"Library|Foundation|Company|Medical|Research)\b"
)
AFF_NON_LATIN = (
    r"(大学|大學|学院|學院|研究所|研究院|研究中心|实验室|實驗室|医院|醫院|科学院|科學院|有限公司|股份|"
    r"管理局|气象局|研究室|学校|学部|株式会社|대학교|대학원|연구소|연구원|병원|학과|센터|대학|"
    r"มหาวิทยาลัย|คณะ|สถาบัน|โรงพยาบาล|جامعة|كلية|معهد|مستشفى|دانشگاه|دانشکده|پژوهشگاه|"
    r"Πανεπιστήμιο|Τμήμα|אוניברסיטת|विश्वविद्यालय|संस्थान)"
)
AFF_CYRILLIC = (
    r"(?iU)(университет|інститут|институт|кафедр|лаборатор|академи|факультет|больниц|филиал|"
    r"\bцентр(а|е|у|ом)?\b)"
)
AFF_AMB_WORDS = r"(?i:Center|Centre|Centro|Centrum|Hospital|College|School|Escola|Academy|Clinic|Klinik|Klinika|Museum|Library|Foundation|Company|Medical|Research)"
AFF_PERSON_SHAPED = r"^\s*((?!" + AFF_AMB_WORDS + r"\b)\p{Lu}[\p{L}\x27\-]*\.?\s+){0,3}\p{Lu}[\p{L}\x27\-]*\s*$"
AFF_POSTCODE = r"[0-9]{4,6}"
AFF_CHAIN = r"[,;，；、]"


def is_not_affiliation_line(author):
    """oxjob #2183: False for a name-only contributor that is an affiliation or address line (see the patterns above)."""
    given = F.coalesce(author["given"], F.lit(""))
    family = F.coalesce(author["family"], F.lit(""))
    name = F.coalesce(author["name"], F.lit(""))
    name_only = (F.trim(given) == "") & (F.trim(family) == "")
    strong = (name.rlike(AFF_STRONG_LATIN) | name.rlike(AFF_CYRILLIC)
              | (name.rlike(AFF_NON_LATIN) & (F.length(name) >= 5)))
    address = name.rlike(AFF_POSTCODE) & name.rlike(AFF_CHAIN) & (F.length(name) >= 12)
    ambiguous = name.rlike(AFF_AMBIGUOUS_LATIN) & ~name.rlike(AFF_PERSON_SHAPED)
    return ~(name_only & (strong | address | ambiguous))


def filter_valid_authors(authors):
    """Contributors both filters keep; a work whose every contributor the #2183 rule drops keeps is_valid_author's
    result instead (an organisation as sole creator: IMF departments, court decisions, institutes issuing reports;
    16,333 works would otherwise lose all authors)."""
    both = F.filter(authors, lambda a: is_valid_author(a) & is_not_affiliation_line(a))
    return F.when(F.size(both) > 0, both).otherwise(F.filter(authors, is_valid_author))

# COMMAND ----------

# COMMAND ----------

# Raw data in single column as items table
@dlt.table(
  name="crossref_items",
  comment = "Reading in files from s3://openalex-ingest/crossref/",
  table_properties={'quality': 'bronze'}
)
@dlt.expect("rescued_data_null", "_rescued_data IS NULL")
def crossref_items():
  # use fixed schema - pipeline breaks with errors to replace structs with strings otherwise
  current_schema = spark.table("openalex.crossref.crossref_items").schema

  return ( spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .schema(current_schema)  # Explicitly use fixed schema to avoid errors and drift (it is 6k lines long)
    # Discovery via UC managed file events on the openalex-ingest external location (oxjob #585)
    .option("cloudFiles.useManagedFileEvents", "true")
    .load("s3a://openalex-ingest/crossref/")
    .withColumn("ingested_at", F.current_timestamp())
  )

# COMMAND ----------

# Exploded data to multiple columns
@dlt.table(
  name="crossref_exploded",
  cluster_by = ["native_id"],
  table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "quality": "silver"
  }
)
def crossref_snapshots_exploded():
  return (dlt.read_stream("crossref_items")
    .select(F.explode(F.col("items")).alias("record"), F.col("ingested_at"))
    .select("record.*", "ingested_at")
    .withColumn("indexed_date", F.col("indexed.date-time"))
    .withColumnRenamed("DOI", "native_id")
    .withColumn("updated_date", F.make_date(
        F.get(F.col("indexed.`date-parts`")[0], 0),
        F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 1), F.lit(1)),
        F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 2), F.lit(1))
    ))
    .withColumn("type", F.when(F.expr(PDB_COMPONENT_SQL), F.lit("dataset")).otherwise(F.col("type")))  # oxjob #1135
    .filter(~F.col("type").isin(unallowed_types))
  )

# This table will be the target for your merge operation
dlt.create_streaming_table(
    name = "crossref_deduplicated",
    comment = "Deduplicated crossref data, keeping only the latest version.",
    cluster_by_auto = True,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "silver"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_deduplicated",
    source="crossref_exploded", # Use the name of the new table
    keys=["native_id"],
    sequence_by="updated_date"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### `crossref_parsed`

# COMMAND ----------

# oxjob #2241: a date Crossref states only as a year (issued/published/published-print [2022], as Springer deposits its
# chapters) was padded to 1 January and, being the earliest date, beat the full online date later that same year:
# 3,283,097 visible works served a 1 January no source states (W4312963213: print [2022], online 2022-11-19, DOI created
# 2022-11-18, served 2022-01-01). Each publication date keeps its precision (its number of date-parts), and a year-only
# date is dropped when another publication date of the record states a month or a day in the same year. Then the
# earliest of the kept dates, created and deposited, exactly as before: no year changes, no source gains precedence, and
# a record whose only publication date is a year keeps 1 January (#1154 / #1709 own that case). try_to_date: an invalid
# online or print date (Feb 30) is NULL, never an error. The same strings run the backfill (oxjobs #2241 work/gen_sql.py).
# Dry run over all 180.2M records (14 Nov 2026): 3,439,694 change, 0 change year, 0 move earlier; every other record keeps
# its date. Revert: git revert, then #2241 work/10_revert.sql.
# oxjob #1157 (card D6, Jason 17 Nov 2026; RULINGS § 3 "Work dates"): a work's date is the earliest date the publisher
# states (issued, published, online, print, approved, posted). The DOI registration day (created) and the deposit day count
# only when the record states no online and no posted date: then they stand in for when the work went online (Elsevier and
# others deposit only a print date). Before, they always joined the least(), so a DOI registered months or years ahead of
# its stated online date dated the work (MIT Press 10.1162/rest_a_01152: registered 2021-12-22, online 2024-03-19). No
# month cutoff (Jason). The same strings run the backfill (oxjobs #1157 work/). Revert: git revert, then #1157's revert SQL.
PUB_DATE_PARTS_FIELDS = ["issued", "published", "published-online", "published-print", "approved", "posted"]


def _padded_date_sql(field):
    parts = f"get(`{field}`.`date-parts`, 0)"
    return (f"named_struct('d', try_to_date(format_string('%04d-%02d-%02d', get({parts}, 0), coalesce(get({parts}, 1), 1), "
            f"coalesce(get({parts}, 2), 1))), 'p', size({parts}))")


PUB_DATES_SQL = "array(" + ", ".join(_padded_date_sql(f) for f in PUB_DATE_PARTS_FIELDS) + ")"
PUBLISHED_DATE_SQL = ("least(array_min(transform(filter(_pub_dates, c -> c.d IS NOT NULL AND NOT coalesce(c.p = 1 AND "
                      "exists(_pub_dates, f -> f.d IS NOT NULL AND f.p >= 2 AND year(f.d) = year(c.d)), false)), c -> c.d)), "
                      "CASE WHEN _pub_dates[2].d IS NULL AND _pub_dates[5].d IS NULL THEN least(created_date, deposited) END)")

# Transformed Table
@dlt.view(
    name="crossref_parsed_source",
    comment="Source of the crossref transformations to parse the data"
)
def crossref_parsed():
    def extract_issn_id_by_type(id_type):
        return F.expr(f"filter(`issn-type`, x -> x.type = '{id_type}').value")

    def extract_isbn_id_by_type(id_type):
        return F.expr(f"filter(`isbn-type`, x -> x.type = '{id_type}').value")

    def extract_license_by_type(content_version):
        return F.expr(
            f"filter(license, x -> x.`content-version` != '{content_version}').URL"
        )

    def extract_url_by_type():
        return F.expr(
            f"filter(link, x -> !contains(x.`content-type`, 'plain') and !contains(x.`content-type`, 'xml'))"
        )

    def elsevier_oa_started(content_version):
        # Elsevier's Open Archive (vor) and Open Manuscript (am) programmes are
        # registered as elsevier.com/open-access/userlicense with a start date;
        # the file is free from that date. normalize_license maps the URL to None
        # (no reuse rights), so OA comes from the element itself. The host gate
        # drops journals that left Elsevier with the element still registered.
        # Oxjob #1118.
        return F.expr(
            "coalesce(resource.primary.URL like '%linkinghub.elsevier.com/%'"
            " and exists(license, x -> x.URL like '%elsevier.com/open-access/userlicense%'"
            f" and x.`content-version` = '{content_version}'"
            " and to_timestamp(x.start.`date-time`) <= current_timestamp()), false)"
        )

    def preprint_relation_ids():
        # Declared preprint <-> published links as ids entries; relationship is
        # Crossref's own key, verbatim. Appended AFTER the self DOI: merge_key.doi
        # is the first doi entry in ids. DOI-typed links only. Oxjob #1099.
        return F.array_distinct(F.concat(*[
            F.expr(
                rf"""filter(transform(filter(coalesce(relation.`{rel}`, array()), r -> r.`id-type` = 'doi'),
                    r -> named_struct('id', lower(regexp_replace(trim(r.id), '^https?://(dx\\.)?doi\\.org/', '')),
                                      'namespace', 'doi', 'relationship', '{rel}')),
                    x -> x.id rlike '^10\\.[0-9]{{4,}}/.+' and x.id <> native_id)"""
            )
            for rel in ("is-preprint-of", "has-preprint")
        ]))

    return (
        # spark.readStream
        # .option("skipChangeCommits", "true") 
        # .table("openalex.crossref.crossref_deduplicated")
        # dlt.read("crossref_deduplicated")
        spark.readStream
            .option("readChangeFeed", "true")
            .table("LIVE.crossref_deduplicated")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))        
        .filter(~F.col("type").isin(unallowed_types))
        .withColumn("native_id_namespace", F.lit("doi"))
        .withColumn("title", F.substring(
            F.when((F.size("title") > 1) & F.lower(F.trim(F.get(F.col("title"), F.size("title") - 1))).isin(*NOTICE_TITLE_ELEMENTS),
                   F.concat_ws(": ", F.trim(F.get(F.col("title"), F.size("title") - 1)), F.get(F.col("title"), 0)))
             .when(F.expr(TITLE_BLANK_ORIGINAL_TITLE_SET_SQL), F.expr(ORIGINAL_TITLE_SQL))  # oxjob #2160
             .otherwise(F.get(F.col("title"), 0)), 0, MAX_TITLE_LENGTH))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn(
            "authors",
            F.when(
                F.col("publisher").isin(AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS),
                # Excluded publishers: keep all authors unchanged
                F.transform("author", create_author_struct)
            ).otherwise(
                # Other publishers: filter out affiliation-as-author entries, then transform
                F.transform(
                    filter_valid_authors(F.col("author")),  # oxjob #2183
                    create_author_struct
                )
            )
        )
        .withColumn(
            "ids",
            F.concat(F.filter(
                F.array(
                    F.struct(
                        F.when(F.isnull(F.get(extract_issn_id_by_type("print"), 0)), None)
                        .otherwise(F.get(extract_issn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.get(extract_issn_id_by_type("electronic"), 0)), None)
                            .otherwise(F.get(extract_issn_id_by_type("electronic"), 0))
                            .alias("id"),
                        F.lit("eissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.get(extract_isbn_id_by_type("print"), 0)), None)
                        .otherwise(F.get(extract_isbn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_isbn_id_by_type("electronic"), 0)),
                            None
                        )
                        .otherwise(F.get(extract_isbn_id_by_type("electronic"), 0))
                        .alias("id"),
                        F.lit("eisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.col("native_id")), None).otherwise(F.col("native_id")).alias("id"),
                        F.lit("doi").alias("namespace"),
                        F.lit("self").alias("relationship")
                    )
                ),
                lambda x: x.id != ""
            ), preprint_relation_ids())  # Filter out empty ids
        )
        .withColumn("elsevier_open_archive", elsevier_oa_started("vor"))
        .withColumn("elsevier_open_manuscript", elsevier_oa_started("am"))
        .withColumn("version", 
            # oxjob #2094: posted content (preprints, working papers, posters, abstracts, blog posts) is posted, not
            # formally published: submittedVersion, as Unpaywall always served it. CreateLocationsMapped applies the
            # same rule to rows parsed before this line changed.
            F.when(F.col("type") == "posted-content", F.lit("submittedVersion"))
            # only the accepted manuscript is free (Open Manuscript)
            .when(
                F.col("elsevier_open_manuscript")
                & ~F.col("elsevier_open_archive")
                & ~F.expr("exists(license, x -> x.URL like '%creativecommons.org%')"),
                F.lit("acceptedVersion"),
            )
            .otherwise(F.lit("publishedVersion"))
        )
        .withColumn("raw_type", F.col("type"))
        # ingest no longer assigns type; the work-type cascade owns it (raw_type kept as evidence)
        .withColumn("type", F.lit(None).cast("string"))
        .withColumn("raw_license",  
            # Filter out content-version = "tdm", then prioritize the url containing "creativecommons.org", otherwise select the first url
            F.when(
                F.isnull(
                    F.get(
                        F.filter(
                            extract_license_by_type("tdm"),
                            lambda x: x.contains("creativecommons.org"),
                        ),
                        0,
                    )
                ),
                F.get(extract_license_by_type("tdm"), 0),
            ).otherwise(
                F.get(
                    F.filter(
                        extract_license_by_type("tdm"),
                        lambda x: x.contains("creativecommons.org"),
                    ),
                    0
                )
            )
        )
        .withColumn("license", normalize_license_udf(F.col("raw_license")))
        .withColumn("language", F.col("language"))
        .withColumn("created_date", F.to_date(F.col("created.`date-time`")))
        .withColumn("deposited", F.to_date(F.col("deposited.`date-time`")))
        # oxjob #2241: publication dates with their precision; a year-only date never beats a finer date in its own year
        .withColumn("_pub_dates", F.expr(PUB_DATES_SQL))
        .withColumn("published_date", F.expr(PUBLISHED_DATE_SQL))
        .withColumn("issue", F.col("issue"))
        .withColumn("volume", F.col("volume"))
        .withColumn("first_page", F.expr(CROSSREF_PAGES_SQL).getField("first_page"))  # oxjob #2240: "17-1-17-28", "253-62"
        .withColumn("last_page", F.expr(CROSSREF_PAGES_SQL).getField("last_page"))
        # oxjob #1500 (Kyle 2026-10-02): a retraction notice is not retracted. A notice's own update-to names the paper it
        # retracts, so update-to flags a record only when it names the record itself (a self-stamp); updated-by (the
        # retracted paper's side, which Crossref and Retraction Watch fill) flags as before.
        .withColumn("is_retracted",
            F.coalesce(F.exists(F.col("updated-by"), lambda x: F.lower(x["label"]) == "retraction"), F.lit(False)) |
            F.coalesce(F.exists(F.col("update-to"), lambda x: (F.lower(x["label"]) == "retraction")
                                & (F.lower(x["DOI"]) == F.lower(F.col("native_id")))), F.lit(False))
        )
        .withColumn("abstract", F.substring(F.col("abstract"), 0, MAX_ABSTRACT_LENGTH))
        .withColumn("source_name", F.coalesce(F.col("`container-title`")[0], F.col("institution")[0]["name"],
            # oxjob #1135: wwPDB's old deposits name no container; its dataset deposits name "Worldwide Protein Data Bank",
            # which is also the deposits' publisher
            F.when(F.lower(F.col("native_id")).rlike("^10[.]2210/pdb[0-9a-z]{4}/pdb$"), F.col("publisher"))))
        .withColumn("publisher", F.col("publisher"))
        .withColumn("funders",
            F.transform(
                "funder",
                lambda fun: F.struct(
                    fun["DOI"].alias("doi"),
                    F.lit(None).cast("string").alias("ror"),
                    fun["name"].alias("name"),
                    fun["award"].alias("awards")
                )
            )
        )  # funders: no ror in crossref?
        .withColumn("references", F.expr(REFERENCES_SQL))
        # NULL, not []: CreateSuperLocations skips only a NULL list, so an empty one outranks the record's PDF list
        .withColumn("references", F.when(F.size("references") > 0, F.col("references")))
        .withColumn(
            "urls",
            F.array_union(
                F.array(
                    F.struct(
                        F.col("URL").alias("url"), F.lit("html").alias("content_type")
                    )
                ),  # url from items.url
                F.coalesce(
                    F.transform(
                        extract_url_by_type(),
                        lambda link: F.struct(
                            link["URL"].alias("url"),
                            F.when(
                                link["URL"].contains("pdf")
                                | link["content-type"].contains("pdf"),
                                F.lit("pdf"),
                            )
                            .otherwise(F.lit("html"))
                            .alias("content_type"),
                        ),
                    ),
                    F.array(
                        F.struct(
                            F.col("URL").alias("url"),
                            F.lit("html").alias("content_type"),
                        )
                    )
                )  # rest of the urls from items.link, except those that have content_type plain or xml. Coalesce used to preserve schema. array_union prevents duplicates.
            )
        )
        .withColumn("mesh", F.lit(None).cast("string"))
        # Does this col need to be same struct type as pubmed?
        .withColumn(
            "is_oa",
            F.when(
                F.lower(F.col("license")).startswith("cc")
                | F.lower(F.col("license")).contains("other-oa")
                | F.lower(F.col("license")).contains("public-domain")
                | (F.lower(F.col("publisher")) == "iucn")
                | F.col("elsevier_open_archive")
                | F.col("elsevier_open_manuscript"),
                F.lit(True),
            ).otherwise(F.lit(False))
        )
        .select(
            "native_id",
            "native_id_namespace",
            "title",
            "normalized_title",
            "authors",
            "ids",
            "raw_type",
            "type",
            "version",
            "license",
            "language",
            "published_date",
            "created_date",
            "updated_date",
            "issue",
            "volume",
            "first_page",
            "last_page",
            "is_retracted",
            "abstract",
            "source_name",
            "publisher",
            "funders",
            "references",
            "urls",
            "mesh",
            "is_oa",
            "ingested_at",
            "_change_type",
            "_commit_version",
            "_commit_timestamp"
        )
    )

    # This table will be the target for your merge operation
dlt.create_streaming_table(
    name="crossref_parsed",
    comment="Parsed crossref data, executing transforms after de-duplication",
    cluster_by = ["native_id"],
    cluster_by_auto = True,
    table_properties = {
        #"delta.enableRowTracking": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "silver"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_parsed",
    source="crossref_parsed_source", # Use the name of the new table
    keys=["native_id"],
    sequence_by="updated_date",
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    apply_as_deletes="lower(_change_type) = 'delete'",  # 👈 Enable DELETE detection
    # oxjob #1918: every update is a whole Crossref record, so the newest deposit replaces the row.
    # With True, a field the registrant removed (NULL in the new parse) kept its old value forever:
    # 12,088 Springer reference entries kept bulk contributor lists Springer deleted on 13-14 Oct 2026
    # (933,872 authorships), plus 2,358 withdrawn abstracts. Same rule as #994 for landing pages.
    ignore_null_updates=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `crossref_processed`

# COMMAND ----------

# Step 1
@dlt.view(name="crossref_processed",
           comment="Crossref data after initial schema application and processing.")
def crossref_processed():
    # Define schema with raw_type for Crossref
    walden_works_with_raw_type_schema = StructType([
        StructField("provenance", StringType(), True), StructField("native_id", StringType(), True),
        StructField("native_id_namespace", StringType(), True), StructField("title", StringType(), True),
        StructField("normalized_title", StringType(), True),
        StructField("authors", ArrayType(StructType([
            StructField("given", StringType(), True), StructField("family", StringType(), True),
            StructField("name", StringType(), True), StructField("orcid", StringType(), True),
            StructField("affiliations", ArrayType(StructType([
                StructField("name", StringType(), True), StructField("department", StringType(), True),
                StructField("ror_id", StringType(), True)])), True),
            StructField("is_corresponding", BooleanType(), True)
        ])), True),
        StructField("ids", ArrayType(StructType([
            StructField("id", StringType(), True), StructField("namespace", StringType(), True),
            StructField("relationship", StringType(), True)])), True),
        StructField("raw_type", StringType(), True), StructField("type", StringType(), True), StructField("version", StringType(), True),
        StructField("license", StringType(), True), StructField("language", StringType(), True),
        StructField("published_date", DateType(), True), StructField("created_date", DateType(), True),
        StructField("updated_date", DateType(), True), StructField("issue", StringType(), True),
        StructField("volume", StringType(), True), StructField("first_page", StringType(), True),
        StructField("last_page", StringType(), True), StructField("is_retracted", BooleanType(), True),
        StructField("abstract", StringType(), True), StructField("source_name", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("funders", ArrayType(StructType([
            StructField("doi", StringType(), True), StructField("ror", StringType(), True),
            StructField("name", StringType(), True), StructField("awards", ArrayType(StringType(), True), True)
        ])), True),
        StructField("references", ArrayType(StructType([
            StructField("doi", StringType(), True), StructField("pmid", StringType(), True),
            StructField("arxiv", StringType(), True), StructField("title", StringType(), True),
            StructField("authors", StringType(), True), StructField("year", StringType(), True),
            StructField("raw", StringType(), True)
        ])), True),
        StructField("urls", ArrayType(StructType([
            StructField("url", StringType(), True), StructField("content_type", StringType(), True)
        ])), True),
        StructField("mesh", StringType(), True), StructField("is_oa", BooleanType(), True),
        StructField("ingested_at", TimestampType(), True)
    ])

    df_parsed_input = (spark.readStream
        .option("readChangeFeed", "true").table("LIVE.crossref_parsed")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )
    return apply_initial_processing(df_parsed_input, "crossref", walden_works_with_raw_type_schema)

# Step 2: Isolate the expensive enrichment step into its own table.
@dlt.view(name="crossref_features_and_authors",
           comment="Applies expensive author and feature enrichment UDFs.")
def crossref_features_and_authors():
    df_processed = dlt.read_stream("crossref_processed")
    # enrich_with_features_and_author_keys is the expensive operation
    return enrich_with_features_and_author_keys(df_processed)

# oxjob #837: total-order dedup sequence — ties on the lead value alone pick
# refresh-dependent winners. Same shape as DataCite/PubMed.
def _with_total_order_sequence(df, lead_col):
    hash_cols = [c for c, t in df.dtypes if not t.startswith("map") and not c.startswith("_")]
    return df.withColumn("_sequence", F.struct(
        lead_col.alias("lead"),
        F.coalesce(F.col("ingested_at"), F.lit("1970-01-01").cast("timestamp")).alias("harvested_at"),
        F.xxhash64(*[F.col(f"`{c}`") for c in hash_cols]).alias("content_hash"),
    ))

# Step 3: The final, quick step.
@dlt.view(name="crossref_works_source",
           comment="Applies merge_key and final filtering")
# oxjob #1099: ids also carries related DOIs; the record's own must stay merge_key.doi
@dlt.expect("merge_key_doi_is_self", "merge_key.doi = native_id")
def crossref_works_source():
    df_processed = dlt.read_stream("crossref_features_and_authors")
    return _with_total_order_sequence(
        apply_final_merge_key_and_filter(df_processed),
        F.col("updated_date").cast("timestamp"))

dlt.create_streaming_table(
    name = "crossref_works", # Final PUBLISHED table name
    comment = "Final Crossref works data including merge_key and filtering.",
    cluster_by = ["native_id"],
    cluster_by_auto = True,
    table_properties = {
        "delta.enableRowTracking": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_works",
    source="crossref_works_source", # Use the name of the new table
    keys=["native_id"],
    sequence_by="_sequence",
    except_column_list=["_sequence"]
)
