# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.20-py3-none-any.whl

# COMMAND ----------

import sys, os
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

import re
import unicodedata
from functools import reduce
import pandas as pd

from openalex.utils.environment import *
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
            "version": F.lit('publishedVersion'),
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
        "ingested_at",
    ))

# COMMAND ----------

# oxjob #837: total-order dedup sequence — ties on the lead value alone pick
# refresh-dependent winners. PubMed's DateRevised is date-only (no finer publisher
# timestamp exists), so updated_date stays the lead and the tiebreakers do the work.
def _with_total_order_sequence(df, lead_col):
    hash_cols = [c for c, t in df.dtypes if not t.startswith("map") and not c.startswith("_")]
    return df.withColumn("_sequence", F.struct(
        lead_col.alias("lead"),
        F.coalesce(F.col("ingested_at"), F.lit("1970-01-01").cast("timestamp")).alias("harvested_at"),
        F.xxhash64(*[F.col(f"`{c}`") for c in hash_cols]).alias("content_hash"),
    ))

@dlt.table(name="pubmed_enriched",
           comment="PubMed data after full parsing and author/feature enrichment.")
def pubmed_enriched():
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

    df_parsed_input = dlt.read_stream("pubmed_parsed")
    df_walden_works_schema = apply_initial_processing(df_parsed_input, "pubmed", walden_works_with_raw_type_schema)

    # enrich_with_features_and_author_keys is imported from your openalex.dlt.transform
    # It applies udf_last_name_only (Pandas UDF) and udf_f_generate_inverted_index (Pandas UDF)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return _with_total_order_sequence(
        apply_final_merge_key_and_filter(df_enriched), F.col("updated_date")
    )

dlt.create_streaming_table(
    name="pubmed_works",
    comment=f"Final pubmed works table with unique identifiers in {ENV.upper()} environment",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    },
    cluster_by=["native_id"]
)

dlt.apply_changes(
    target="pubmed_works",
    source="pubmed_enriched",
    keys=["native_id"],
    sequence_by="_sequence",
    except_column_list=["_sequence"]
)
