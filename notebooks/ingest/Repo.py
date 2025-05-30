# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.1.9-py3-none-any.whl

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

import re
import unicodedata
from functools import reduce
import pandas as pd

from openalex.utils.environment import *
from openalex.dlt.normalize import normalize_title_udf, normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

def get_openalex_type_from_repo(input_type):
    """
    Convert various publication type formats to OpenAlex types.
    """
    if not input_type or not isinstance(input_type, str):
        return "other"

    input_type = input_type.strip().lower()
    
    # handle URI formats
    if "info:eu-repo/semantics/" in input_type:
        input_type = input_type.split("info:eu-repo/semantics/")[-1]
    elif "purl.org/coar/resource_type/" in input_type:
        coar_to_type = {
            "c_93fc": "article",    # contribution to journal
            "c_6501": "article",    # journal article
            "c_bdcc": "article",    # research article
            "c_db06": "dataset",    # dataset
            "r60j-j5bd": "article", # article
            "c_5794": "article",    # journal article
            "c_2f33": "article",    # article
        }
        coar_id = input_type.split("/")[-1]
        return coar_to_type.get(coar_id.lower(), "other")
    elif "purl.org/coar/version/" in input_type:
        return "article"
    
    input_variations = {
        # article
        "journal article": "article",
        "journal articles": "article",
        "article": "article",
        "publishedversion": "article",
        "contributiontoperiodical": "article",
        
        # book
        "book": "book",
        "books": "book",
        "book sections": "book-chapter",
        "bookpart": "book-chapter",
        
        # conference materials
        "conference papers": "article",
        "conferencepaper": "article",
        "conferenceobject": "article",
        "conferenceposter": "article",
        "conferenceproceedings": "article",
        "conferencecontribution": "article",
        "conferenceitem": "article",
        
        # thesis/dissertation
        "thesis": "dissertation",
        "theses": "dissertation",
        "masterthesis": "dissertation",
        "bachelorthesis": "dissertation",
        "doctoralthesis": "dissertation",
        "doctor of philosophy": "dissertation",
        "phd": "dissertation",
        "dissertation": "dissertation",
        "undergraduate senior honors thesis": "dissertation",
        
        # report
        "report": "report",
        "reports": "report",
        "technical report": "report",
        "workingpaper": "report",
        "working paper": "report",
        "reportpart": "report",
        "technical documentation": "report",
        
        # review
        "review": "review",
        "peerreviewed": "peer-review",
        "peer-review": "peer-review",
        
        # preprint
        "preprint": "preprint",
        "preprints, working papers, ...": "preprint",
        
        # image
        "image": "other",
        "chemical structures": "other",
        
        # text
        "text": "article",
        "manuscript": "article",
        
        # Other specific types
        "lecture": "other",
        "patent": "other",
        "dataset": "dataset",
        "creative project": "other",
        
        # general catch-alls
        "other": "other",
        "null": "other"
    }
    return input_variations.get(input_type, "other")

def normalize_language_code(lang_code):
    """
    Normalize language codes to ISO 639-1 two-letter lowercase format.
    Handles ISO 639-1 (two-letter), ISO 639-2/T (three-letter), and common variations.
    """
    if not lang_code or not isinstance(lang_code, str):
        return None

    lang_code = lang_code.strip().lower()
    
    # remove any [[iso]] prefix
    if "[[iso]]" in lang_code:
        lang_code = lang_code.replace("[[iso]]", "")
        
    # handle special cases
    if lang_code in ["null", "und", "other"]:
        return None
    
    # handle codes with regional variants
    if "_" in lang_code:
        lang_code = lang_code.split("_")[0]
        
    # handle multiple codes (e.g., "tr; en")
    if ";" in lang_code:
        lang_code = lang_code.split(";")[0].strip()

    # full names to codes mapping
    names_to_codes = {
        "english": "en",
        "spanish": "es",
        "french": "fr",
        "german": "de",
        "chinese": "zh",
        "russian": "ru",
        "japanese": "ja",
        "arabic": "ar",
        "portuguese": "pt",
        "italian": "it",
    }

    # ISO 639-2 to ISO 639-1 mapping
    three_to_two = {
        # most common languages first for quick matches
        "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "rus": "ru",
        "zho": "zh", "jpn": "ja", "ara": "ar", "por": "pt", "ita": "it",
        # additional languages
        "abk": "ab", "aar": "aa", "afr": "af", "aka": "ak", "alb": "sq",
        "amh": "am", "arg": "an", "arm": "hy", "asm": "as", "ava": "av",
        "ave": "ae", "aym": "ay", "aze": "az", "bam": "bm", "bak": "ba",
        "baq": "eu", "bel": "be", "ben": "bn", "bih": "bh", "bis": "bi",
        "bos": "bs", "bre": "br", "bul": "bg", "bur": "my", "cat": "ca",
        "cha": "ch", "che": "ce", "nya": "ny", "chi": "zh", "chu": "cu",
        "chv": "cv", "cor": "kw", "cos": "co", "cre": "cr", "hrv": "hr",
        "cze": "cs", "dan": "da", "div": "dv", "dut": "nl", "dzo": "dz",
        "epo": "eo", "est": "et", "ewe": "ee", "fao": "fo", "fij": "fj",
        "fin": "fi", "fre": "fr", "fry": "fy", "ful": "ff", "geo": "ka",
        "ger": "de", "gla": "gd", "gle": "ga", "glg": "gl", "glv": "gv",
        "gre": "el", "grn": "gn", "guj": "gu", "hat": "ht", "hau": "ha",
        "heb": "he", "her": "hz", "hin": "hi", "hmo": "ho", "hun": "hu",
        "ice": "is", "ido": "io", "iii": "ii", "iku": "iu", "ile": "ie",
        "ina": "ia", "ind": "id", "ipk": "ik", "isl": "is", "jav": "jv",
        "kan": "kn", "kau": "kr", "kas": "ks", "kaz": "kk", "khm": "km",
        "kik": "ki", "kin": "rw", "kir": "ky", "kom": "kv", "kon": "kg",
        "kor": "ko", "kua": "kj", "kur": "ku", "lao": "lo", "lat": "la",
        "lav": "lv", "lim": "li", "lin": "ln", "lit": "lt", "ltz": "lb",
        "lub": "lu", "lug": "lg", "mac": "mk", "mah": "mh", "mal": "ml",
        "mao": "mi", "mar": "mr", "may": "ms", "mlg": "mg", "mlt": "mt",
        "mon": "mn", "nau": "na", "nav": "nv", "nbl": "nr", "nde": "nd",
        "ndo": "ng", "nep": "ne", "nno": "nn", "nob": "nb", "nor": "no",
        "oji": "oj", "ori": "or", "orm": "om", "oss": "os", "pan": "pa",
        "per": "fa", "pli": "pi", "pol": "pl", "pus": "ps", "que": "qu",
        "roh": "rm", "rum": "ro", "run": "rn", "sag": "sg", "san": "sa",
        "sin": "si", "slo": "sk", "slv": "sl", "sme": "se", "smo": "sm",
        "sna": "sn", "snd": "sd", "som": "so", "sot": "st", "srd": "sc",
        "srp": "sr", "ssw": "ss", "sun": "su", "swa": "sw", "swe": "sv",
        "tah": "ty", "tam": "ta", "tat": "tt", "tel": "te", "tgk": "tg",
        "tgl": "tl", "tha": "th", "tib": "bo", "tir": "ti", "ton": "to",
        "tsn": "tn", "tso": "ts", "tuk": "tk", "tur": "tr", "twi": "tw",
        "uig": "ug", "ukr": "uk", "urd": "ur", "uzb": "uz", "ven": "ve",
        "vie": "vi", "vol": "vo", "wel": "cy", "wln": "wa", "wol": "wo",
        "xho": "xh", "yid": "yi", "yor": "yo", "zha": "za", "zul": "zu"
    }
    
    # check if it's already a valid two-letter code
    if len(lang_code) == 2:
        return lang_code
        
    # check full names
    if lang_code in names_to_codes:
        return names_to_codes[lang_code]
        
    # check three-letter codes
    if len(lang_code) == 3:
        return three_to_two.get(lang_code)
        
    return None

def has_oa_domain(native_id):
    oa_domains = ["arxiv", "osti", "pubmedcentral", "biorxiv", "medrxiv", "zenodo", "figshare"]
    if native_id is None:
        return False
    
    parts = native_id.lower().split(":")
    if len(parts) >= 2:
        domain_part = parts[1]
        for domain in oa_domains:
            if domain in domain_part:
                return True
    return False

@F.pandas_udf(StringType())
def get_openalex_type_from_repo_udf(repo_type_series: pd.Series) -> pd.Series:
    return repo_type_series.apply(get_openalex_type_from_repo)

@F.pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@F.pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

# COMMAND ----------

# IDs UDF

url_pattern = r"(https?://\S+|www\.\S+)"

id_struct_type = StructType([
    StructField("id", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("relationship", StringType(), True)
])

def extract_ids(identifiers, native_id):
   try:
       if identifiers is None:
           return []
       if not isinstance(identifiers, list):
           identifiers = [identifiers]
       if native_id is None:
           native_id = ""
           
       patterns = {
           'arxiv': (r"https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}|[a-z\-]+/\d+)", 1),
           'arxiv_native': (r"oai:arXiv\.org:([^/\s]+/\d+|\d+\.\d+)", 1),
           'doi': (r"\b10\.\d{4,9}/\S+\b", 0),
           'issn': (r"\b\d{4}-\d{3}[0-9X]\b", 0),
           'hal': (r"\bhal-\d+\b", 0),
           'handle': (r"https?://hdl\.handle\.net/([^/\s]+/[^/\s]+)", 1),
           'pmid': (r"/pubmed/(\d+)", 1),
           'pmcid': (r"/pmc/articles/(PMC\d+)", 1)
       }
       
       results = []
       arxiv_id_from_native = None
       
       # extract arxiv ID from native_id and normalize it
       try:
           if isinstance(native_id, str):
               match = re.search(patterns['arxiv_native'][0], native_id)
               if match:
                   arxiv_id_from_native = match.group(1)
       except Exception:
           pass
       
       # process each identifier
       for identifier in identifiers:
           if not identifier or not isinstance(identifier, str):
               continue
               
           try:
               for namespace, (pattern, group) in patterns.items():
                   match = re.search(pattern, identifier)
                   if match:
                       try:
                           relationship = None
                           
                           if namespace.startswith('arxiv'):
                               id_value = "arXiv:" + match.group(group)  # prepend arXiv:
                               
                               # check if this is an arxiv ID and compare with native_id
                               if arxiv_id_from_native:
                                   if id_value == f"arXiv:{arxiv_id_from_native}" or f"oai:arXiv.org:{match.group(group)}" == native_id:
                                       relationship = 'self'
                           else:
                               id_value = match.group(group)
                           
                           results.append({
                               "id": id_value,
                               "namespace": namespace.split('_')[0],
                               "relationship": relationship
                           })
                           break
                       except Exception:
                           continue
           except Exception:
               continue
       
       # add native_id
       if native_id:
           results.append({
               "id": native_id,
               "namespace": "pmh",
               "relationship": "self"
           })

    # removed 4/23/2025 causing matching issues with arxiv due to doi
       # add doi if available
    #    if "doi" not in identifiers and native_id.lower().startswith("oai:arxiv.org:"):
    #        doi = native_id.lower().replace("oai:arxiv.org:", "10.48550/arxiv.")
    #        results.append({
    #            "id": doi,
    #            "namespace": "doi",
    #            "relationship": "self"
    #        })
       
       # deduplicate
       seen = set()
       unique_results = []
       for r in results:
           try:
               key = (r['id'], r['namespace'], r['relationship'])
               if key not in seen:
                   seen.add(key)
                   unique_results.append(r)
           except Exception:
               continue
       
       return unique_results
       
   except Exception as e:
       print(f"Error in extract_ids: {str(e)}")
       return []

extract_ids_udf = F.udf(extract_ids, ArrayType(id_struct_type))

# COMMAND ----------

repository_schema = StructType([
    StructField("ns0:header", StructType([
        # StructField("@status", StringType(), True),
        StructField("ns0:identifier", StringType(), True),
        StructField("ns0:datestamp", TimestampType(), True),
        StructField("ns0:setSpec", ArrayType(StringType()), True)
    ]), True),
    StructField("ns0:metadata", StructType([
        StructField("ns1:dc", StructType([
            StructField("dc:title", StringType(), True),
            StructField("dc:creator", ArrayType(StringType()), True),
            StructField("dc:contributor", ArrayType(StringType()), True),
            StructField("dc:subject", ArrayType(StringType()), True),
            StructField("dc:description", ArrayType(StringType()), True),
            StructField("dc:source", StringType(), True),
            StructField("dc:date", ArrayType(StringType()), True),
            StructField("dc:type", StringType(), True),
            StructField("dc:identifier", ArrayType(StringType()), True),
            StructField("dc:language", StringType(), True),
            StructField("dc:format", ArrayType(StringType()), True),
            StructField("dc:publisher", StringType(), True),
            StructField("dc:rights", ArrayType(StringType()), True)
        ]), True)
    ]), True)
])

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

# COMMAND ----------

# Items table
@dlt.table(
  name="repo_items",
  table_properties={'quality': 'bronze'}
)
#@dlt.expect("rescued_data_null", "_rescued_data IS NULL")
def repo_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "xml")
      .option("rowTag", "ns0:record")
      .option("compression", "gzip")
      .option("ignorMissingFiles", "true")
      .schema(repository_schema)
      .option("cloudFiles.schemaLocation", "dbfs:/pipelines/repo/schema")
      .load("s3a://openalex-ingest/repositories/")
  )

# COMMAND ----------

@dlt.table(
  name="repo_parsed"
)
def repo_parsed():
  return (dlt.read_stream("repo_items")
    .filter(F.col("ns0:metadata").isNotNull())  # doaj deleted articles have no metadata
    .withColumn("native_id", F.col("`ns0:header`.`ns0:identifier`"))
    .withColumn("updated_date", F.col("`ns0:header`.`ns0:datestamp`"))
    .dropDuplicates(["native_id", "updated_date"])
    .withColumn("native_id_namespace", F.lit("pmh"))
    .withColumn("title", F.substring(F.col("`ns0:metadata`.`ns1:dc`.`dc:title`"), 0, MAX_TITLE_LENGTH))
    .withColumn("normalized_title", normalize_title_udf(F.col("title")))
    .withColumn(
        "authors",
        F.transform(
            F.col("`ns0:metadata`.`ns1:dc`.`dc:creator`"),
            lambda auth: F.struct(
                F.lit(None).cast("string").alias("given"),
                F.lit(None).cast("string").alias("family"), 
                F.substring(auth, 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
                F.lit(None).cast("string").alias("orcid"),
                F.array(
                    F.struct(
                        F.lit(None).cast("string").alias("name"),
                        F.lit(None).cast("string").alias("department"),
                        F.lit(None).cast("string").alias("ror_id"),
                    )
                ).alias("affiliations"),
            ),
        ),
    )
    .withColumn(
        "ids",
        extract_ids_udf(
            F.col("`ns0:metadata`.`ns1:dc`.`dc:identifier`"),
            F.col("native_id")
        )
    )
    .withColumn("type", get_openalex_type_from_repo_udf(F.col("`ns0:metadata`.`ns1:dc`.`dc:type`")))
    .withColumn("version", F.lit(None))
    .withColumn(
        "raw_license",
        F.when(
            F.expr(
                "size(filter(`ns0:metadata`.`ns1:dc`.`dc:rights`, x -> x like '%creativecommons.org%')) > 0"
            ),
            F.expr(
                "filter(`ns0:metadata`.`ns1:dc`.`dc:rights`, x -> x like '%creativecommons.org%')[0]"
            ),
        ).otherwise(F.col("`ns0:metadata`.`ns1:dc`.`dc:rights`")[0]),
    )
    .withColumn("license", normalize_license_udf(F.col("raw_license")))
    .withColumn("language", normalize_language_code_udf(F.col("`ns0:metadata`.`ns1:dc`.`dc:language`")))
    .withColumn(
        "published_date_raw", F.element_at(F.col("ns0:metadata.ns1:dc.dc:date"), 1)
    )
    .withColumn(
        "published_date",
        F.coalesce(
            # ISO format with timezone
            F.to_date(
                F.to_timestamp(F.col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ),
            # ISO format without timezone
            F.to_date(
                F.to_timestamp(F.col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss")
            ),
            # Regular date
            F.to_date(F.col("published_date_raw"), "yyyy-MM-dd"),
            # Month and year
            F.to_date(F.col("published_date_raw"), "yyyy-MM"),
            # Year only
            F.to_date(
                F.when(
                    F.length(F.trim(F.col("published_date_raw"))) == 4,
                    F.concat(F.col("published_date_raw"), F.lit("-01-01")),
                ),
                "yyyy-MM-dd",
            ),
        ),
    )
    .withColumn("created_date", F.col("published_date"))
    .withColumn(
        "updated_date", F.col("updated_date").cast("date")
    )  # cast updated_date from timestamp to date
    .withColumn("version", F.lit(None).cast("string"))
    .withColumn("issue", F.lit(None).cast("string"))
    .withColumn("volume", F.lit(None).cast("string"))
    .withColumn("first_page", F.lit(None).cast("string"))
    .withColumn("last_page", F.lit(None).cast("string"))
    .withColumn("is_retracted", F.lit(None).cast("boolean"))
    .withColumn(
        "abstract",
        F.substring(F.col("`ns0:metadata`.`ns1:dc`.`dc:description`").getItem(0), 0, MAX_ABSTRACT_LENGTH)
    )
    .withColumn("source_name", F.col("`ns0:metadata`.`ns1:dc`.`dc:source`"))
    .withColumn("publisher", F.col("`ns0:metadata`.`ns1:dc`.`dc:publisher`"))
    .withColumn(
        "funders",
        F.array(
            F.struct(
                F.lit(None).cast("string").alias("doi"),
                F.lit(None).cast("string").alias("ror"),
                F.lit(None).cast("string").alias("name"),
                F.array(F.lit(None).cast("string")).alias("awards"),
            )
        ),
    )
    .withColumn("references", F.array(
        F.struct(
            F.lit(None).cast("string").alias("doi"),
            F.lit(None).cast("string").alias("pmid"),
            F.lit(None).cast("string").alias("arxiv"),
            F.lit(None).cast("string").alias("title"),
            F.lit(None).cast("string").alias("authors"),
            F.lit(None).cast("string").alias("year"),
            F.lit(None).cast("string").alias("raw"),
        )
    ))
    .withColumn(
        "has_pmcid",
        F.expr("exists(ids, id -> id.namespace = 'pmcid')")
    )
    .withColumn(
        "pmcid",
        F.expr("filter(ids, id -> id.namespace = 'pmcid')[0].id")
    )
    .withColumn(
        "urls",
        F.when(
            F.col("has_pmcid"),
            F.array(
                F.struct(
                    F.regexp_replace(
                        F.concat(F.lit("https://www.ncbi.nlm.nih.gov/pmc/articles/"), F.col("pmcid")),
                        "PMC(\\d+)", "$1"
                    ).alias("url"),
                    F.lit("html").alias("content_type")
                )
            )
        ).otherwise(
            F.filter(
                F.transform(
                    F.col("`ns0:metadata`.`ns1:dc`.`dc:identifier`"),
                    lambda x: F.struct(
                        F.regexp_extract(x, url_pattern, 0).alias("url"),  # extract URL
                        # set content_type based on whether 'pdf' appears in the URL
                        F.when(x.rlike("(?i)pdf"), F.lit("pdf"))
                        .otherwise(F.lit("html"))
                        .alias("content_type"),
                    ),
                ),
                lambda x: x["url"] != "",  # Filter out entries with no matched URL
            )
        )
    )
    .drop("has_pmcid", "pmcid")
    .withColumn("mesh", F.lit(None).cast("string"))
    .withColumn(
        "is_oa",
        F.when(
            F.lower(F.col("license")).startswith("cc") | 
            F.lower(F.col("license")).contains("other-oa") |
            F.lower(F.col("license")).contains("public-domain") |
            has_oa_domain_udf(F.col("native_id")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .select(
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
        "is_oa"
    )
)

# COMMAND ----------


@dlt.table(
    name="repo_parsed_backfill",
    temporary=True,
    comment="Streaming read of repo backfill using CDF (automatically incremental in DLT)"
)
def repo_parsed_backfill():
    repo_schema = spark.table("openalex.repo.repo_works_backfill").schema
    return (
        spark.readStream
            .option("readChangeFeed", "true")
            .schema(repo_schema)
            .table("openalex.repo.repo_works_backfill")
            .filter(F.col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


@dlt.table(name="repo_enriched",
           comment="repo data after full parsing and author/feature enrichment.")
def repo_enriched():
    df_parsed_backfill = dlt.read_stream("repo_parsed_backfill")
    df_parsed_input = dlt.read_stream("repo_parsed")
    
    # Apply consistent schema and transformations
    df_walden_works = apply_initial_processing(df_parsed_input, "repo", walden_works_schema)
    df_backfill_walden_works = apply_initial_processing(df_parsed_backfill, "repo_backfill", walden_works_schema)
    
    # Combine both
    combined_df = df_walden_works.union(df_backfill_walden_works)
    
    # Apply enrichment (with fast Pandas UDFs)
    df_enriched = enrich_with_features_and_author_keys(combined_df)
    
    return apply_final_merge_key_and_filter(df_enriched)

dlt.create_streaming_table(
    name="repo_works",
    comment="Final repository works table with unique identifiers",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

dlt.apply_changes(
    target="repo_works",
    source="repo_enriched",
    keys=["native_id"],
    sequence_by="updated_date"
)
