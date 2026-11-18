# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.41-py3-none-any.whl

# COMMAND ----------

import sys, os
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

import re
import unicodedata
from functools import reduce
import pandas as pd

from openalex.dlt.normalize import normalize_title_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

# COMMAND ----------

# Define a UDF to consolidate award_id's per agency
def consolidate_awards(records):

    if records is not None:
        consolidated = {}

        for record in records:
            agency = record["Agency"]
            grant_id = record["GrantID"]

            if agency not in consolidated:
                consolidated[agency] = []

            consolidated[agency].append(grant_id)

        result = [{"doi": None, "ror": None, "name": agency, "awards": awards} for agency, awards in consolidated.items()]
        return result
    
# Register the UDF
consolidate_awards_udf = F.udf(consolidate_awards, ArrayType(StructType([
    StructField("doi", StringType(), True),
    StructField("ror", StringType(), True),
    StructField("name", StringType(), True),
    StructField("awards", ArrayType(StringType()), True)
]), True))

# COMMAND ----------

# Define a UDF to convert languages from 3-letter to 2-letter

def convert_language_code(three_letter_code):
    """
    Convert ISO 639-2/B (bibliographic) three-letter language codes to ISO 639-1 two-letter codes.
    Returns None for undefined or unknown codes.
    
    Based on the ISO 639-2 standard: https://www.loc.gov/standards/iso639-2/
    """
    lang_map = {
        'aar': 'aa', 'abk': 'ab', 'afr': 'af', 'aka': 'ak', 'alb': 'sq', 'amh': 'am',
        'ara': 'ar', 'arg': 'an', 'arm': 'hy', 'asm': 'as', 'ava': 'av', 'ave': 'ae',
        'aym': 'ay', 'aze': 'az', 'bak': 'ba', 'bam': 'bm', 'baq': 'eu', 'bel': 'be',
        'ben': 'bn', 'bih': 'bh', 'bis': 'bi', 'bod': 'bo', 'bos': 'bs', 'bre': 'br',
        'bul': 'bg', 'bur': 'my', 'cat': 'ca', 'ces': 'cs', 'cha': 'ch', 'che': 'ce',
        'chi': 'zh', 'chu': 'cu', 'chv': 'cv', 'cor': 'kw', 'cos': 'co', 'cre': 'cr',
        'cym': 'cy', 'cze': 'cs', 'dan': 'da', 'deu': 'de', 'div': 'dv', 'dut': 'nl',
        'dzo': 'dz', 'ell': 'el', 'eng': 'en', 'epo': 'eo', 'est': 'et', 'eus': 'eu',
        'ewe': 'ee', 'fao': 'fo', 'fas': 'fa', 'fij': 'fj', 'fin': 'fi', 'fra': 'fr',
        'fre': 'fr', 'fry': 'fy', 'ful': 'ff', 'geo': 'ka', 'ger': 'de', 'gla': 'gd',
        'gle': 'ga', 'glg': 'gl', 'glv': 'gv', 'gre': 'el', 'grn': 'gn', 'guj': 'gu',
        'hat': 'ht', 'hau': 'ha', 'heb': 'he', 'her': 'hz', 'hin': 'hi', 'hmo': 'ho',
        'hrv': 'hr', 'hun': 'hu', 'hye': 'hy', 'ibo': 'ig', 'ice': 'is', 'ido': 'io',
        'iii': 'ii', 'iku': 'iu', 'ile': 'ie', 'ina': 'ia', 'ind': 'id', 'ipk': 'ik',
        'isl': 'is', 'ita': 'it', 'jav': 'jv', 'jpn': 'ja', 'kal': 'kl', 'kan': 'kn',
        'kas': 'ks', 'kat': 'ka', 'kau': 'kr', 'kaz': 'kk', 'khm': 'km', 'kik': 'ki',
        'kin': 'rw', 'kir': 'ky', 'kom': 'kv', 'kon': 'kg', 'kor': 'ko', 'kua': 'kj',
        'kur': 'ku', 'lao': 'lo', 'lat': 'la', 'lav': 'lv', 'lim': 'li', 'lin': 'ln',
        'lit': 'lt', 'ltz': 'lb', 'lub': 'lu', 'lug': 'lg', 'mac': 'mk', 'mah': 'mh',
        'mal': 'ml', 'mao': 'mi', 'mar': 'mr', 'may': 'ms', 'mkd': 'mk', 'mlg': 'mg',
        'mlt': 'mt', 'mon': 'mn', 'mri': 'mi', 'msa': 'ms', 'mya': 'my', 'nau': 'na',
        'nav': 'nv', 'nbl': 'nr', 'nde': 'nd', 'ndo': 'ng', 'nep': 'ne', 'nld': 'nl',
        'nno': 'nn', 'nob': 'nb', 'nor': 'no', 'nya': 'ny', 'oci': 'oc', 'oji': 'oj',
        'ori': 'or', 'orm': 'om', 'oss': 'os', 'pan': 'pa', 'per': 'fa', 'pli': 'pi',
        'pol': 'pl', 'por': 'pt', 'pus': 'ps', 'que': 'qu', 'roh': 'rm', 'ron': 'ro',
        'rum': 'ro', 'run': 'rn', 'rus': 'ru', 'sag': 'sg', 'san': 'sa', 'sin': 'si',
        'slk': 'sk', 'slo': 'sk', 'slv': 'sl', 'sme': 'se', 'smo': 'sm', 'sna': 'sn',
        'snd': 'sd', 'som': 'so', 'sot': 'st', 'spa': 'es', 'sqi': 'sq', 'srd': 'sc',
        'srp': 'sr', 'ssw': 'ss', 'sun': 'su', 'swa': 'sw', 'swe': 'sv', 'tah': 'ty',
        'tam': 'ta', 'tat': 'tt', 'tel': 'te', 'tgk': 'tg', 'tgl': 'tl', 'tha': 'th',
        'tib': 'bo', 'tir': 'ti', 'ton': 'to', 'tsn': 'tn', 'tso': 'ts', 'tuk': 'tk',
        'tur': 'tr', 'twi': 'tw', 'uig': 'ug', 'ukr': 'uk', 'urd': 'ur', 'uzb': 'uz',
        'ven': 've', 'vie': 'vi', 'vol': 'vo', 'wel': 'cy', 'wln': 'wa', 'wol': 'wo',
        'xho': 'xh', 'yid': 'yi', 'yor': 'yo', 'zha': 'za', 'zho': 'zh', 'zul': 'zu',
        'und': 'und'
    }
    if not three_letter_code:
        return None
    
    return lang_map.get(three_letter_code.lower()) if three_letter_code.lower() != 'und' else None
  
convert_language_code_udf = F.udf(convert_language_code, StringType())

# COMMAND ----------

# Items table
@dlt.table(
  name="pubmed_items",
  table_properties={'quality': 'bronze'}
)
@dlt.expect("rescued_data_null", "_rescued_data IS NULL")
def pubmed_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "xml")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", "/pubmed/schemas/ingest")
      .option("cloudFiles.schemaEvolutionMode", "rescue")
      .option("cloudFiles.schemaHints", "MedlineCitation.Article.Abstract.AbstractText ARRAY<STRUCT<_Label:STRING,_NlmCategory:STRING,_VALUE:STRING,sub:ARRAY<STRING>,sup:ARRAY<STRING>,i:ARRAY<STRING>,b:ARRAY<STRING>,u:ARRAY<STRING>>>, MedlineCitation.Article.ArticleTitle STRING, MedlineCitation.OtherAbstract ARRAY<MAP<STRING,STRING>>, MedlineCitation.Article.VernacularTitle STRING, MedlineCitation.KeywordList ARRAY<STRING>")
      .option("maxFilesPerTrigger", "10")
      .option("rowTag", "PubmedArticle")
      .option("inferSchema", "true")
      .option("sampleSize", "1")
      .option("mergeSchema", "true")
      .option("compression", "gzip")
      .option("ignorMissingFiles", "true")
      .option("ignoreCorruptFiles", "true")
      .option("mode", "PERMISSIVE")
      # Discovery via UC managed file events on the openalex-ingest external location (oxjob #585)
      .option("cloudFiles.useManagedFileEvents", "true")
      .load("s3a://openalex-ingest/pubmed/")
      .withColumn("ingested_at", F.current_timestamp())
  )

# COMMAND ----------

# Exploded Table
@dlt.table(
    name="pubmed_exploded",
    comment="Accumulated PubMed data with unique PMID and additional metadata",
)
def pubmed_exploded():
  # Return data stream
  return (dlt.read_stream("pubmed_items")
    .withColumn("revised_date",F.to_date(
      F.concat_ws(
          "-",
          F.col("MedlineCitation.DateRevised.Year"),
          F.col("MedlineCitation.DateRevised.Month"),
          F.col("MedlineCitation.DateRevised.Day"),
      ))
    )
    .withColumn("pmid", F.col("MedlineCitation.PMID._VALUE"))
    .dropDuplicates(["pmid", "revised_date"])
  )

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000 

# COMMAND ----------

# oxjob #1863: PubMed's issue date when PubDate has no Year. Some records give the issue date only as a
# PubDate.MedlineDate string ("1950 Oct-Dec", "1996 Mar-Apr", "2011-2012 Winter", "1945 Dec 22-29"; ~2.5M PMIDs).
# The old code fell back to created_date, i.e. MedlineCitation.DateCompleted: the day NLM finished indexing the
# record, which put OLDMEDLINE papers from the 1940s-60s in 1998-2004. DateCompleted is never a publication date.
# Rule (data-bugs charter, "What right means", 2026-10-26): PubDate Year/Month/Day; else the first year in
# MedlineDate, with its first month name and, when the day follows that month directly ("1945 Dec 22-29"), the
# day; seasons and quarters keep the year only (month 1, as PubDate Year+Season always has); else the
# PubMedPubDate PubStatus="pubmed" history date; else NULL. Pure SQL, so the same text backfills existing rows.
# shortcut: month names match by prefix after a word start ("Marine" would read as March); only the month can be
# wrong that way, never the year. Tests: notebooks/ingest/tests/test_pubmed_medline_date.py.
_MEDLINE_MONTHS = {
    "jan": 1, "ene": 1, "feb": 2, "fev": 2, "fév": 2, "mar": 3, "mär": 3, "mrz": 3, "apr": 4, "abr": 4, "avr": 4,
    "may": 5, "mai": 5, "jun": 6, "juin": 6, "jul": 7, "juil": 7, "aug": 8, "ago": 8, "aout": 8, "août": 8,
    "sep": 9, "set": 9, "oct": 10, "okt": 10, "out": 10, "nov": 11, "dec": 12, "déc": 12, "dic": 12, "dez": 12,
}


def medline_date_fallback_sql(medline_date, history):
    """SQL for a DATE from a PubDate.MedlineDate string column, else the 'pubmed' history date, else NULL.

    medline_date: SQL expression of the MedlineDate string; history: SQL expression of the
    PubmedData.History.PubMedPubDate array (structs with Year, Month, Day, _PubStatus).
    """
    tokens = "|".join(sorted(_MEDLINE_MONTHS, key=len, reverse=True))
    year = (f"try_cast(nullif(regexp_extract({medline_date}, "
            f"'(?<![0-9])(1[5-9][0-9][0-9]|20[0-9][0-9])(?![0-9])', 1), '') AS INT)")
    month_map = "map(" + ", ".join(f"'{t}', {m}" for t, m in _MEDLINE_MONTHS.items()) + ")"
    month = (f"try_element_at({month_map}, "
             f"lower(regexp_extract({medline_date}, '(?i)(?<![a-zà-ÿ])({tokens})', 1)))")
    day = (f"try_cast(nullif(regexp_extract({medline_date}, "
           f"'(?i)^\\\\s*[12][0-9][0-9][0-9]\\\\s+({tokens})[a-zà-ÿ]*\\\\.?,?\\\\s+([0-9]{{1,2}})(?![0-9])', 2), '') AS INT)")
    medline = (f"CASE WHEN {year} IS NOT NULL THEN make_date({year}, coalesce({month}, 1), "
               f"CASE WHEN {month} IS NOT NULL AND {day} BETWEEN 1 AND day(last_day(make_date({year}, {month}, 1))) "
               f"THEN {day} ELSE 1 END) END")
    hist = f"get(filter({history}, x -> x._PubStatus = 'pubmed'), 0)"
    history_date = (f"CASE WHEN {hist}.Year BETWEEN 1500 AND 2100 AND {hist}.Month BETWEEN 1 AND 12 "
                    f"THEN date_add(make_date(CAST({hist}.Year AS INT), CAST({hist}.Month AS INT), 1), "
                    f"CAST(least(greatest(coalesce({hist}.Day, 1), 1), 31) AS INT) - 1) END")
    return f"coalesce({medline}, {history_date})"


PUBMED_DATE_FALLBACK_SQL = medline_date_fallback_sql(
    "MedlineCitation.Article.Journal.JournalIssue.PubDate.MedlineDate", "PubmedData.History.PubMedPubDate")

# oxjob #2240: page strings read the way NCBI reads them (its StartPage / EndPage). MEDLINE writes ranges short
# ("358-60" is 358-360, "C845-53" is C845-C853), lists as "27, 31-2, 34-7" (27-37) or "1001-6; discussion 1006-7"
# (1001-1007), and supplements as "Suppl 4:165-6" (165-166); Crossref deposits chapter pages as "17-1-17-28".
# Splitting at the first hyphen served last_page "60" for 358-360 on ~5.4M works.
# first_page = the first item's start; last_page = the end of the last item that reads as a page or range (leading
# words like "author reply" and a trailing "passim" / "contd" set aside; "2 pp." is a page count and doesn't read), its dropped leading digits put back and the
# start's letter prefix carried. A string whose first item doesn't read, or whose end would run backwards ("358-12"),
# keeps the old split at the first hyphen: never invent a page. Pure SQL, so the same text backfills existing rows.
# Crossref.py holds the same builder (the test checks the copies match). Tests: notebooks/ingest/tests/test_page_ranges.py.
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


PUBMED_PAGES_SQL = page_range_sql("MedlineCitation.Article.Pagination.MedlinePgn")

# COMMAND ----------

# Parsed Table
@dlt.table(
    name="pubmed_parsed",
    comment="Parsed Medline and PubMed data",
)
def pubmed_parsed():
  def extract_id_by_type(id_type):
      return F.expr(
          f"filter(PubmedData.ArticleIdList.ArticleId, x -> x._IdType = '{id_type}')[0]._VALUE"
      )

  def extract_ids_by_type(id_type):
      return F.expr(
          f"filter(PubmedData.ArticleIdList.ArticleId, x -> x._IdType = '{id_type}')._VALUE"
      )

  def extract_issn_by_type(issn_type):
      return F.expr(
          f"filter(MedlineCitation.Article.Journal.ISSN, x -> x._IssnType = '{issn_type}')._VALUE"
      )

  def create_date_column(year_col, month_col, day_col):
      return F.to_date(
          F.concat_ws("-", F.col(year_col), F.col(month_col), F.col(day_col))
      )

  return (dlt.read_stream("pubmed_exploded").withColumns(
      {
            "native_id": extract_id_by_type("pubmed"),
            "native_id_namespace": F.lit("pmid"),
            "title": F.substring(F.col("MedlineCitation.Article.ArticleTitle"), 0, MAX_TITLE_LENGTH),
            "normalized_title": normalize_title_udf(F.col("title")),
            "authors": F.transform(
                F.col("MedlineCitation.Article.AuthorList.Author"),
                lambda auth: F.struct(
                    F.substring(auth["ForeName"], 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
                    F.substring(auth["LastName"], 0, MAX_AUTHOR_NAME_LENGTH).alias("family"),
                    F.lit(None).cast("string").alias("name"),
                    F.lit(None).cast("string").alias("orcid"),
                    F.transform(
                        auth["AffiliationInfo"]["Affiliation"],
                        lambda aff: F.struct(
                            F.substring(F.get(aff["_VALUE"], 0), 0, MAX_AFFILIATION_STRING_LENGTH).alias("name"),
                            F.lit(None).cast("string").alias("department"),
                            F.lit(None).cast("string").alias("ror_id"),
                        ),
                    ).alias("affiliations"),
                ),
            ),
            "ids": F.filter(
                F.array(
                    F.struct(
                        F.get(extract_ids_by_type("doi"), 0).alias("id"),
                        F.lit("doi").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                    F.struct(
                        F.get(extract_ids_by_type("pubmed"), 0).alias("id"),
                        F.lit("pmid").alias("namespace"),
                        F.lit("self").alias("relationship"),
                    ),
                    F.struct(
                        F.get(extract_ids_by_type("pmc"), 0).alias("id"),
                        F.lit("pmcid").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                    F.struct(
                        F.get(extract_ids_by_type("pii"), 0).alias("id"),
                        F.lit("pii").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                    F.struct(
                        F.get(extract_ids_by_type("mid"), 0).alias("id"),
                        F.lit("mid").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                    F.struct(
                        F.col("MedlineCitation.Article.Journal.ISSN._VALUE").alias(
                            "id"
                        ),
                        F.lit("eissn").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                    F.struct(
                        F.col("MedlineCitation.MedlineJournalInfo.ISSNLinking").alias(
                            "id"
                        ),
                        F.lit("lissn").alias("namespace"),
                        F.lit(None).alias("relationship"),
                    ),
                ),
                lambda x: x.id != "",
            ),  # Filter out empty ids
            # ingest no longer assigns type; the work-type cascade owns it.
            # raw_type = the same PublicationType element the old mapping consumed (evidence for the cascade dict).
            "raw_type": F.get(
                F.col(
                    "MedlineCitation.Article.PublicationTypelist.PublicationType._VALUE"
                ),
                0,
            ),
            "type": F.lit(None).cast("string"),
            # oxjob #2143: a record NLM types "Preprint" (bioRxiv, medRxiv, Research Square, arXiv... from the NIH preprint
            # pilot) is the submitted version; every other PubMed record is the published one.
            "version": F.when(
                F.array_contains(F.col("MedlineCitation.Article.PublicationTypelist.PublicationType._VALUE"), "Preprint"),
                F.lit('submittedVersion'),
            ).otherwise(F.lit('publishedVersion')),
            "license": F.lit(None).cast("string"),
            "language":  convert_language_code_udf(F.get(F.col("MedlineCitation.Article.Language"), 0)),
            "created_date": F.when(
                F.col("MedlineCitation.DateCompleted").isNull(), None
            ).otherwise(
                create_date_column(
                    "MedlineCitation.DateCompleted.Year",
                    "MedlineCitation.DateCompleted.Month",
                    "MedlineCitation.DateCompleted.Day",
                )
            ),
            "updated_date": create_date_column(
                "MedlineCitation.DateRevised.Year",
                "MedlineCitation.DateRevised.Month",
                "MedlineCitation.DateRevised.Day",
            ),
            "published_date": F.when(
                F.col(
                    "MedlineCitation.Article.Journal.JournalIssue.PubDate.Year"
                ).isNull(),
                # oxjob #1863: MedlineDate, else the 'pubmed' history date; never created_date (DateCompleted)
                F.expr(PUBMED_DATE_FALLBACK_SQL)
            ).otherwise(
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col(
                            "MedlineCitation.Article.Journal.JournalIssue.PubDate.Year"
                        ),
                        F.coalesce(
                            F.when(
                                F.length(
                                    F.col(
                                        "MedlineCitation.Article.Journal.JournalIssue.PubDate.Month"
                                    )
                                )
                                == 3,
                                F.date_format(
                                    F.to_date(
                                        F.col(
                                            "MedlineCitation.Article.Journal.JournalIssue.PubDate.Month"
                                        ),
                                        "MMM",
                                    ),
                                    "MM",
                                ),
                            ).otherwise(
                                F.col(
                                    "MedlineCitation.Article.Journal.JournalIssue.PubDate.Month"
                                )
                            ),
                            F.lit("1"),
                        ),
                        F.coalesce(
                            F.col(
                                "MedlineCitation.Article.Journal.JournalIssue.PubDate.Day"
                            ),
                            F.lit("1"),
                        ),
                    )
                )
            ),
            "issue": F.col("MedlineCitation.Article.Journal.JournalIssue.Issue"),
            "volume": F.col("MedlineCitation.Article.Journal.JournalIssue.Volume"),
            # oxjob #2240: "358-60" serves 358 / 360 (page_range_sql above)
            "first_page": F.expr(PUBMED_PAGES_SQL).getField("first_page"),
            "last_page": F.expr(PUBMED_PAGES_SQL).getField("last_page"),
            "is_retracted": F.coalesce(
                F.array_contains(
                    F.col(
                        "MedlineCitation.CommentsCorrectionsList.CommentsCorrections._RefType"
                    ),
                    "RetractionIn",
                ),
                F.lit(False),
            ),
            "abstract": F.substring(
                F.expr(
                    "nullif(concat_ws(' ', transform(MedlineCitation.Article.Abstract.AbstractText, "
                    "x -> CASE "
                    "WHEN x._Label IS NOT NULL AND x._Label != '' "
                    "  THEN concat(x._Label, ': ', x._VALUE) "
                    "WHEN x._NlmCategory IS NOT NULL AND x._NlmCategory != '' AND x._NlmCategory != 'UNASSIGNED' "
                    "  THEN concat(x._NlmCategory, ': ', x._VALUE) "
                    "ELSE x._VALUE END)), '')"
                ),
                0, MAX_ABSTRACT_LENGTH,
            ),
            "source_name": F.col("MedlineCitation.Article.Journal.Title"),
            "publisher": F.lit(None).cast("string"),
            "funders" : consolidate_awards_udf(F.col("MedlineCitation.Article.GrantList.Grant")),
            "references": F.zip_with(
                F.expr(
                    "transform(filter(PubmedData.ReferenceList.Reference[0], x -> x.ArticleIdList.ArticleId[0]._IdType = 'pubmed').ArticleIdList.ArticleId._VALUE, x -> struct(get(x,0) as pmid))"
                ),
                F.expr(
                    "transform(PubmedData.ReferenceList.Reference.Citation[0]._VALUE, x -> struct(get(x,0) as raw))"
                ),
                lambda x, y: F.struct(
                    F.lit(None).cast("string").alias("doi"), 
                    x.pmid, 
                    F.lit(None).cast("string").alias("arxiv"), 
                    F.lit(None).cast("string").alias("title"), 
                    F.lit(None).cast("string").alias("authors"), 
                    F.lit(None).cast("string").alias("year"), 
                    y.raw
                ),
            ),
            "urls": F.array(
                F.struct(
                    F.concat(
                        F.lit("https://pubmed.ncbi.nlm.nih.gov/"),
                        extract_id_by_type("pubmed"),
                        F.lit("/"),
                    ).alias("url"),
                    F.lit("html").alias("content_type"),
                ),
            ),
            "mesh": F.col("MedlineCitation.MeshHeadingList").cast("string"), # casting this as a string to help with the append_flow. parse it later when needed.
        }
    ).select(
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
        "first_page",  # "720-6" now serves 720 / 726 (oxjob #2240)
        "last_page",
        "is_retracted",
        "abstract",
        "source_name",
        "publisher",
        "funders",
        "references",
        "urls",
        "mesh",
        "ingested_at",
    ))

# COMMAND ----------

# oxjob #1104: PubMed's book records (PubmedBookArticle: NCBI Bookshelf parts such as StatPearls and GeneReviews
# chapters, and whole books). NLM's baseline and update files carry none of them (17 Nov 2026: rowTag
# PubmedBookArticle over every file in s3://openalex-ingest/pubmed/ = 0 rows), so ~40K PubMed records never became
# works. The PubMed Books job (jobs/pubmed_books.yaml, notebooks/ingest/PubMedBooksHarvest.py) fetches all of them
# through E-utilities into s3://openalex-ingest/pubmed_books/. They get their own stream, parse and CDC flow into
# pubmed_works, keyed by PMID like every PubMed record (a PMID names one record, article or book).
# The parse is pure SQL over an explicit schema (PUBMED_BOOK_SCHEMA, PUBMED_BOOK_COLUMNS), so the dry run and the
# test read the same text: notebooks/ingest/tests/test_pubmed_books.py (`--sql PATH` prints the dry run over any files).
# ArticleTitle and AbstractText are read as STRING, which keeps their inline markup (<b>, <i>, <sup>) as text; the
# tags are stripped below. Read as structs, spark-xml's _VALUE keeps only the text after the last inline tag (#1052).
PUBMED_BOOKS_PATH = "s3a://openalex-ingest/pubmed_books/"
PUBMED_BOOK_SCHEMA = """BookDocument STRUCT<
    PMID: STRUCT<_VALUE: STRING, _Version: STRING>,
    ArticleIdList: STRUCT<ArticleId: ARRAY<STRUCT<_VALUE: STRING, _IdType: STRING>>>,
    Book: STRUCT<
      Publisher: STRUCT<PublisherName: STRING, PublisherLocation: STRING>,
      BookTitle: STRING,
      PubDate: STRUCT<Year: STRING, Month: STRING, Day: STRING, MedlineDate: STRING>,
      BeginningDate: STRUCT<Year: STRING, Month: STRING>,
      Isbn: ARRAY<STRING>,
      ELocationID: ARRAY<STRUCT<_VALUE: STRING, _EIdType: STRING>>>,
    ArticleTitle: STRING,
    Language: ARRAY<STRING>,
    AuthorList: ARRAY<STRUCT<_Type: STRING, Author: ARRAY<STRUCT<LastName: STRING, ForeName: STRING,
      Initials: STRING, CollectiveName: STRING, AffiliationInfo: ARRAY<STRUCT<Affiliation: STRING>>,
      Identifier: ARRAY<STRUCT<_VALUE: STRING, _Source: STRING>>>>>>,
    PublicationType: ARRAY<STRUCT<_VALUE: STRING, _UI: STRING>>,
    Abstract: STRUCT<AbstractText: ARRAY<STRING>>,
    ContributionDate: STRUCT<Year: STRING, Month: STRING, Day: STRING>,
    DateRevised: STRUCT<Year: STRING, Month: STRING, Day: STRING>,
    ELocationID: ARRAY<STRUCT<_VALUE: STRING, _EIdType: STRING>>,
    ReferenceList: ARRAY<STRUCT<Reference: ARRAY<STRUCT<Citation: STRING,
      ArticleIdList: STRUCT<ArticleId: ARRAY<STRUCT<_VALUE: STRING, _IdType: STRING>>>>>>>>,
  PubmedBookData STRUCT<
    History: STRUCT<PubMedPubDate: ARRAY<STRUCT<Year: STRING, Month: STRING, Day: STRING, _PubStatus: STRING>>>,
    PublicationStatus: STRING,
    ArticleIdList: STRUCT<ArticleId: ARRAY<STRUCT<_VALUE: STRING, _IdType: STRING>>>>"""


def _book_date_sql(d):
    """SQL DATE from a {Year, Month, Day} struct expression (month '06' or 'Jun'); month and day default to 1; NULL
    when the year is missing or the date doesn't exist."""
    ymd = f"concat_ws('-', {d}.Year, coalesce({d}.Month, '1'), coalesce({d}.Day, '1'))"
    return f"CASE WHEN {d}.Year IS NOT NULL THEN coalesce(try_to_date({ymd}, 'y-M-d'), try_to_date({ymd}, 'y-MMM-d')) END"


def _strip_tags_sql(x, tag=""):
    """Markup out ('GeneReviews<sup>®</sup>' -> 'GeneReviews®'); tag=' ' where a tag separates words (abstract headings)."""
    return f"nullif(trim(regexp_replace(regexp_replace({x}, '<[^>]+>', '{tag}'), '\\\\s+', ' ')), '')"


_BD = "BookDocument"
_BOOK_DOI = (f"coalesce(get(filter({_BD}.ArticleIdList.ArticleId, x -> x._IdType = 'doi'), 0)._VALUE, "
             f"get(filter({_BD}.ELocationID, e -> e._EIdType = 'doi'), 0)._VALUE, "
             f"get(filter({_BD}.Book.ELocationID, e -> e._EIdType = 'doi'), 0)._VALUE)")
_BOOK_PMID = f"coalesce(get(filter(PubmedBookData.ArticleIdList.ArticleId, x -> x._IdType = 'pubmed'), 0)._VALUE, {_BD}.PMID._VALUE)"
_BOOK_NBK = f"get(filter({_BD}.ArticleIdList.ArticleId, x -> x._IdType = 'bookaccession'), 0)._VALUE"
_BOOK_HISTORY_PUBMED = "get(filter(PubmedBookData.History.PubMedPubDate, x -> x._PubStatus = 'pubmed'), 0)"

# One SQL expression per pubmed_parsed column, in pubmed_parsed's order.
PUBMED_BOOK_COLUMNS = {
    "native_id": _BOOK_PMID,
    "native_id_namespace": "'pmid'",
    # a part's own title; a whole-book record has only the book's
    "title": f"substr({_strip_tags_sql(f'coalesce({_BD}.ArticleTitle, {_BD}.Book.BookTitle)')}, 1, {MAX_TITLE_LENGTH})",
    # book authors only: AuthorList Type="editors" are the book's editors, not this part's authors
    "authors": (f"transform(flatten(transform(filter({_BD}.AuthorList, l -> coalesce(l._Type, 'authors') = 'authors'), "
                f"l -> l.Author)), a -> named_struct("
                f"'given', substr(a.ForeName, 1, {MAX_AUTHOR_NAME_LENGTH}), "
                f"'family', substr(coalesce(a.LastName, a.CollectiveName), 1, {MAX_AUTHOR_NAME_LENGTH}), "
                f"'name', CAST(NULL AS STRING), "
                f"'orcid', get(filter(a.Identifier, i -> i._Source = 'ORCID'), 0)._VALUE, "
                f"'affiliations', transform(a.AffiliationInfo, f -> named_struct("
                f"'name', substr(f.Affiliation, 1, {MAX_AFFILIATION_STRING_LENGTH}), "
                f"'department', CAST(NULL AS STRING), 'ror_id', CAST(NULL AS STRING)))))"),
    "ids": (f"filter(array("
            f"named_struct('id', {_BOOK_DOI}, 'namespace', 'doi', 'relationship', CAST(NULL AS STRING)), "
            f"named_struct('id', {_BOOK_PMID}, 'namespace', 'pmid', 'relationship', 'self')), "
            f"x -> x.id IS NOT NULL AND x.id != '')"),
    # The work-type cascade owns type; raw_type says what the record is, in the cascade's own words ('book-chapter',
    # 'book': CreateLocationsWithTypes types them so, and source matching's book guards read them). NLM's
    # PublicationType on book records ('Study Guide', 'Review') describes the content, not that it is a book part.
    "raw_type": f"CASE WHEN {_BD}.ArticleTitle IS NOT NULL THEN 'book-chapter' ELSE 'book' END",
    "type": "CAST(NULL AS STRING)",
    "version": "'publishedVersion'",
    "license": "CAST(NULL AS STRING)",
    "language": f"get({_BD}.Language, 0)",
    # The date this part was posted (ContributionDate: GeneReviews' initial posting, StatPearls' current edition),
    # else the book's own date, else the record's PubMed date.
    "published_date": (f"coalesce({_book_date_sql(f'{_BD}.ContributionDate')}, "
                       f"{_book_date_sql(f'{_BD}.Book.PubDate')}, {_book_date_sql(_BOOK_HISTORY_PUBMED)})"),
    "created_date": _book_date_sql(_BOOK_HISTORY_PUBMED),
    "updated_date": (f"coalesce({_book_date_sql(f'{_BD}.DateRevised')}, {_book_date_sql(f'{_BD}.ContributionDate')}, "
                     f"{_book_date_sql(_BOOK_HISTORY_PUBMED)})"),
    "issue": "CAST(NULL AS STRING)",
    "volume": "CAST(NULL AS STRING)",
    "first_page": "CAST(NULL AS STRING)",
    "last_page": "CAST(NULL AS STRING)",
    "is_retracted": "false",
    "abstract": f"substr({_strip_tags_sql(f'array_join({_BD}.Abstract.AbstractText, chr(10))', ' ')}, 1, {MAX_ABSTRACT_LENGTH})",
    "source_name": _strip_tags_sql(f"{_BD}.Book.BookTitle"),
    "publisher": f"{_BD}.Book.Publisher.PublisherName",
    "funders": ("CAST(array() AS ARRAY<STRUCT<doi: STRING, ror: STRING, name: STRING, awards: ARRAY<STRING>>>)"),
    "references": (f"transform(flatten(coalesce({_BD}.ReferenceList.Reference, array())), r -> named_struct("
                   f"'doi', CAST(NULL AS STRING), "
                   f"'pmid', get(filter(r.ArticleIdList.ArticleId, x -> x._IdType = 'pubmed'), 0)._VALUE, "
                   f"'arxiv', CAST(NULL AS STRING), 'title', CAST(NULL AS STRING), 'authors', CAST(NULL AS STRING), "
                   f"'year', CAST(NULL AS STRING), 'raw', r.Citation))"),
    # PubMed's page for the record, then the full text on NCBI Bookshelf (free to read)
    "urls": (f"filter(array("
             f"named_struct('url', concat('https://pubmed.ncbi.nlm.nih.gov/', {_BOOK_PMID}, '/'), 'content_type', 'html'), "
             f"named_struct('url', concat('https://www.ncbi.nlm.nih.gov/books/', {_BOOK_NBK}, '/'), 'content_type', 'html')), "
             f"u -> u.url IS NOT NULL)"),
    "mesh": "CAST(NULL AS STRING)",
}


@dlt.table(name="pubmed_book_items", table_properties={'quality': 'bronze'})
def pubmed_book_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "xml")
      .option("rowTag", "PubmedBookArticle")
      .option("compression", "gzip")
      .option("mode", "PERMISSIVE")
      .option("cloudFiles.useManagedFileEvents", "true")
      .schema(PUBMED_BOOK_SCHEMA)
      .load(PUBMED_BOOKS_PATH)
      .withColumn("ingested_at", F.current_timestamp())
  )


@dlt.table(name="pubmed_book_parsed", comment="PubMed book records in pubmed_parsed's shape (oxjob #1104)")
def pubmed_book_parsed():
  cols = dict(PUBMED_BOOK_COLUMNS)
  df = dlt.read_stream("pubmed_book_items").selectExpr(*[f"{v} AS {k}" for k, v in cols.items()], "ingested_at")
  return (df
    .withColumn("language", convert_language_code_udf(F.col("language")))
    .withColumn("normalized_title", normalize_title_udf(F.col("title")))
    .select("native_id", "native_id_namespace", "title", "normalized_title", "authors", "ids", "raw_type", "type",
            "version", "license", "language", "published_date", "created_date", "updated_date", "issue", "volume",
            "first_page", "last_page", "is_retracted", "abstract", "source_name", "publisher", "funders",
            "references", "urls", "mesh", "ingested_at")
    # 9 of 37,578 records (17 Nov 2026) have an empty BookTitle and no part title: nothing to name a work by
    .filter("native_id IS NOT NULL AND title IS NOT NULL"))

# COMMAND ----------

# oxjob #837: total-order dedup sequence — ties on the lead value alone pick
# refresh-dependent winners. PubMed's DateRevised is date-only (no finer publisher
# timestamp exists), so updated_date stays the lead and the tiebreakers do the work.
def _with_total_order_sequence(df, lead_col):
    hash_cols = [c for c, t in df.dtypes if not t.startswith("map") and not c.startswith("_")]
    seq = F.struct(
        lead_col.alias("lead"),
        F.coalesce(F.col("ingested_at"), F.lit("1970-01-01").cast("timestamp")).alias("harvested_at"),
        F.xxhash64(*[F.col(f"`{c}`") for c in hash_cols]).alias("content_hash"),
    )
    # oxjob #1104: every flow into pubmed_works must carry one identical sequence type, nullability included, or DLT
    # refuses the update; a table read back from storage is all-nullable while one built in the same update is not
    # (Repo.py, oxjobs #1786 / #1828). So every field is nullable on every path.
    seq_type = df.select(seq.alias("s")).schema["s"].dataType
    return df.withColumn("_sequence", seq.cast(StructType([StructField(f.name, f.dataType, True) for f in seq_type.fields])))

def enrich_pubmed(df_parsed_input):
    """pubmed_parsed-shaped rows -> pubmed_enriched rows. Shared by the article flow and the book flow (oxjob #1104)."""
    # Same walden works schema plus raw_type (mirrors Crossref/DataCite; shared schema lacks it)
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

    df_walden_works_schema = apply_initial_processing(df_parsed_input, "pubmed", walden_works_with_raw_type_schema)

    # enrich_with_features_and_author_keys is imported from your openalex.dlt.transform
    # It applies udf_last_name_only (Pandas UDF) and udf_f_generate_inverted_index (Pandas UDF)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return _with_total_order_sequence(
        apply_final_merge_key_and_filter(df_enriched), F.col("updated_date")
    )


@dlt.table(name="pubmed_enriched",
           comment="PubMed data after full parsing and author/feature enrichment.")
def pubmed_enriched():
    return enrich_pubmed(dlt.read_stream("pubmed_parsed"))


@dlt.table(name="pubmed_book_enriched",
           comment="PubMed book records after enrichment (oxjob #1104).")
def pubmed_book_enriched():
    return enrich_pubmed(dlt.read_stream("pubmed_book_parsed"))

dlt.create_streaming_table(
    name="pubmed_works",
    comment="Final pubmed works table with unique identifiers",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    },
    cluster_by=["native_id"]
)

# One set of CDC settings for every flow into pubmed_works (oxjob #1104: flows must agree on keys, sequence and columns).
PUBMED_WORKS_CDC = dict(
    target="pubmed_works",
    keys=["native_id"],
    sequence_by="_sequence",
    except_column_list=["_sequence"]
)
dlt.apply_changes(source="pubmed_enriched", **PUBMED_WORKS_CDC)

# oxjob #1104: the second flow, PubMed's book records (named; the flow above keeps its default name, "pubmed_works").
dlt.create_auto_cdc_flow(source="pubmed_book_enriched", name="pubmed_works_books", **PUBMED_WORKS_CDC)
