# Databricks notebook source
# MAGIC %pip install nameparser

# COMMAND ----------

import sys, os
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

import re
import unicodedata
from functools import reduce
import pandas as pd

def clean_html(raw_html):
    cleanr = re.compile('<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
    
def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from|\n)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()
    
@F.pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

# COMMAND ----------

def get_openalex_type_from_pubmed(pubmed_type):
    """
    Convert PubMed publication types to OpenAlex types.
    """
    pubmed_to_openalex = {
        # article
        "Journal Article": "article",
        "Clinical Study": "article",
        "Clinical Trial": "article",
        "Clinical Trial, Phase I": "article",
        "Clinical Trial, Phase II": "article",
        "Clinical Trial, Phase III": "article",
        "Clinical Trial, Phase IV": "article",
        "Controlled Clinical Trial": "article",
        "Adaptive Clinical Trial": "article",
        "Pragmatic Clinical Trial": "article",
        "Randomized Controlled Trial": "article",
        "Observational Study": "article",
        "Case Reports": "article",
        "Comparative Study": "article",
        "Validation Study": "article",
        "Multicenter Study": "article",
        "Evaluation Study": "article",
        
        # review
        "Review": "review",
        "Systematic Review": "review",
        "Meta-Analysis": "review",
        "Scientific Integrity Review": "review",
        
        # letter
        "Letter": "letter",
        "Comment": "letter",
        
        # editorial
        "Editorial": "editorial",
        
        # dataset
        "Dataset": "dataset",
        
        # report
        "Technical Report": "report",
        
        # erratum
        "Published Erratum": "erratum",
        "Corrected and Republished Article": "erratum",
        
        # retraction
        "Retraction of Publication": "retraction",
        "Retracted Publication": "retraction",
        
        # preprint
        "Preprint": "preprint",
        
        # paratext (supplementary/administrative content)
        "Bibliography": "paratext",
        "Congress": "paratext",
        "Dictionary": "paratext",
        "Directory": "paratext",
        "Periodical Index": "paratext",
        
        # supplementary-materials
        "Electronic Supplementary Materials": "supplementary-materials",
        
        # peer-review
        "Peer Review": "peer-review",

        # grant
        "Research Support, American Recovery and Reinvestment Act": "grant",
        "Research Support, N.I.H., Extramural": "grant",
        "Research Support, N.I.H., Intramural": "grant",
        "Research Support, Non-U.S. Gov't": "grant",
        "Research Support, U.S. Gov't, Non-P.H.S.": "grant",
        "Research Support, U.S. Gov't, P.H.S.": "grant",
        
        # everything else maps to "other"
        "Address": "other",
        "Autobiography": "other",
        "Biography": "other",
        "Classical Article": "other",
        "Clinical Conference": "other",
        "Collected Work": "other",
        "Consensus Development Conference": "other",
        "Duplicate Publication": "other",
        "English Abstract": "other",
        "Expression of Concern": "other",
        "Festschrift": "other",
        "Government Publication": "other",
        "Guideline": "other",
        "Historical Article": "other",
        "Interactive Tutorial": "other",
        "Interview": "other",
        "Introductory Journal Article": "other",
        "Lecture": "other",
        "Legal Case": "other",
        "Legislation": "other",
        "News": "other",
        "Newspaper Article": "other",
        "Overall": "other",
        "Patient Education Handout": "other",
        "Personal Narrative": "other",
        "Portrait": "other",
        "Practice Guideline": "other",
        "Video-Audio Media": "other",
        "Webcast": "other",
    }
    
    return pubmed_to_openalex.get(pubmed_type, "other")

@F.pandas_udf(StringType())
def get_openalex_type_from_pubmed_udf(series: pd.Series) -> pd.Series:
    return series.apply(get_openalex_type_from_pubmed)

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
def pubmed_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "xml")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", "/pubmed/schemas/ingest")
      .option("cloudFiles.schemaHints", "MedlineCitation.Article.Abstract.AbstractText STRING, MedlineCitation.Article.ArticleTitle STRING, MedlineCitation.OtherAbstract ARRAY<MAP<STRING,STRING>>, MedlineCitation.Article.VernacularTitle STRING, MedlineCitation.KeywordList ARRAY<STRING>")
      .option("maxFilesPerTrigger", "10")
      .option("rowTag", "PubmedArticle")
      .option("inferSchema", "true")
      .option("sampleSize", "1")
      .option("mergeSchema", "true")
      .option("compression", "gzip")
      .option("ignorMissingFiles", "true")
      .option("ignoreCorruptFiles", "true")
      .option("mode", "PERMISSIVE")
      .load("s3a://openalex-ingest/pubmed/")
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
            "type": get_openalex_type_from_pubmed_udf(
                F.get(
                    F.col(
                        "MedlineCitation.Article.PublicationTypelist.PublicationType._VALUE"
                    ),
                    0,
                )
            ),
            "version": F.col("PubmedData.PublicationStatus"),
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
                F.when(
                    F.col("created_date").isNotNull(),
                    F.col("created_date")  # Use created_date if published_date is null
                ).otherwise(F.lit(None))
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
            "first_page": F.get(
                F.split(F.col("MedlineCitation.Article.Pagination.MedlinePgn"), "-"), 0
            ),
            "last_page": F.coalesce(
                F.get(
                    F.split(
                        F.col("MedlineCitation.Article.Pagination.MedlinePgn"), "-"
                    ),
                    1,
                ),
                F.col("first_page"),
            ),
            "is_retracted": F.coalesce(
                F.array_contains(
                    F.col(
                        "MedlineCitation.CommentsCorrectionsList.CommentsCorrections._RefType"
                    ),
                    "RetractionIn",
                ),
                F.lit(False),
            ),
            "abstract": F.substring(F.col("MedlineCitation.Article.Abstract.AbstractText"), 0, MAX_ABSTRACT_LENGTH),
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
        "type",
        "version",
        "license",
        "language",
        "published_date",
        "created_date",
        "updated_date",
        "issue",
        "volume",
        "first_page",  # Sometimes looks like first_page = 720, last_page = 6, because input is "720-6". Is that ok?
        "last_page",
        "is_retracted",
        "abstract",
        "source_name",
        "publisher",
        "funders",
        "references",
        "urls",
        "mesh",
    ))

# COMMAND ----------

dlt.create_target_table(
    name="pubmed_works",
    comment="Final pubmed works table with unique identifiers",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="pubmed_works",
    source="pubmed_parsed",
    keys=["native_id"],
    sequence_by="updated_date"
)
