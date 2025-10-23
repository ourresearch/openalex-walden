# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.1-py3-none-any.whl

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

# Types to filter out (DELETE from CSV mapping) - lowercase normalized
TYPES_TO_DELETE = [
    "person", "image", "newspaper", "info:eu-repo/semantics/lecture", "photograph",
    "bildband", "dvd-video", "video", "fotografia", "cd", "sound recording",
    "text and image", "moving image", "photographs", "cd-rom",
    "blu-ray-disc", "stillimage", "image; text", "image;stillimage", "still image",
    "image;", "ilustraciones y fotos", "fotografie", "fotografía"
]

# Comprehensive type mappings - combines original logic + CSV mappings (all lowercase keys)
REPO_TYPE_MAPPING = {
    # Articles and journal content
    "article": "article",
    "journal article": "article",
    "journal articles": "article",
    "publishedversion": "article",
    "contributiontoperiodical": "article",
    "conference papers": "article",
    "conferencepaper": "article",
    "conferenceobject": "article",
    "conferenceposter": "article",
    "conferenceproceedings": "article",
    "conferencecontribution": "article",
    "conferenceitem": "article",
    "text": "article",
    "manuscript": "article",
    "peerreviewed": "article",  # CSV
    "peer reviewed": "article",  # CSV
    "submittedversion": "article",  # CSV (extracted from URI)
    "acceptedversion": "article",  # CSV
    "review": "article",  # CSV (extracted from URI)
    "zeitschrift": "article",  # CSV
    "conference paper": "article",  # CSV
    "konferenzschrift": "article",  # CSV
    "article / letter to editor": "article",  # CSV
    "inproceedings": "article",  # CSV
    "text (article)": "article",  # CSV
    "info:ulb-repo/semantics/openurl/article": "article",  # CSV (different prefix)
    "vor": "article",  # CSV
    "journal contribution": "article",  # CSV
    "artigo de jornal": "article",  # CSV
    "doc-type:article": "article",  # CSV
    "letter": "article",  # CSV
    
    # Books and book content
    "book": "book",
    "books": "book",
    "book sections": "book-chapter",
    "bookpart": "book-chapter",
    "monografische reihe": "book",  # CSV
    "volume": "book",  # CSV
    "monograph": "book",  # CSV
    "book article": "book-chapter",  # CSV
    "book part": "book-chapter",  # CSV
    "chapter, part of book": "book-chapter",  # CSV
    "part of book or chapter of book": "book-chapter",  # CSV
    "libros": "book",  # CSV
    "doc-type:bookpart": "book-chapter",  # CSV
    
    # Theses and dissertations
    "thesis": "dissertation",
    "theses": "dissertation",
    "masterthesis": "dissertation",
    "bachelorthesis": "dissertation",
    "doctoralthesis": "dissertation",
    "doctor of philosophy": "dissertation",
    "phd": "dissertation",
    "dissertation": "dissertation",
    "undergraduate senior honors thesis": "dissertation",
    "hochschulschrift": "dissertation",  # CSV
    "phdthesis": "dissertation",  # CSV
    "master thesis": "dissertation",  # CSV
    "diplomová práce": "dissertation",  # CSV
    "doctoral thesis": "dissertation",  # CSV
    "doc-type:doctoralthesis": "dissertation",  # CSV
    "masters paper": "dissertation",  # CSV
    "masters thesis": "dissertation",  # CSV
    "dissertação": "dissertation",  # CSV
    "doctoral_dissertation": "dissertation",  # CSV
    "tesi doctoral": "dissertation",  # CSV
    "thesis or dissertation": "dissertation",  # CSV
    "thèse": "dissertation",  # CSV
    "electronic dissertation": "dissertation",  # CSV
    "dissertation-reproduction (electronic)": "dissertation",  # CSV
    "thesis-reproduction (electronic)": "dissertation",  # CSV
    
    # Reports
    "report": "report",
    "reports": "report",
    "technical report": "report",
    "workingpaper": "report",
    "working paper": "report",
    "reportpart": "report",
    "technical documentation": "report",
    
    # Reviews
    "review": "review",
    "peer-review": "peer-review",
    
    # Preprints
    "preprint": "preprint",
    "preprints, working papers, ...": "preprint",
    
    # Datasets
    "dataset": "dataset",
    "dataset/mass spectrometry": "dataset",  # CSV
    
    # Awards
    "award/grant": "award",  # CSV
    
    # Software
    "software": "software",  # CSV
    
    # Images (mapped to other)
    "image": "other",
    "chemical structures": "other",
    
    # Other types
    "lecture": "other",
    "patent": "other",
    "creative project": "other",
    "other": "other",
    "null": "other",
}

def get_openalex_type_from_repo(input_type):
    """
    Convert various publication type formats to OpenAlex types.
    """
    if not input_type or not isinstance(input_type, str):
        return "other"

    input_type = input_type.strip()
    input_type_lower = input_type.lower()
    
    # Handle URI formats first
    if "info:eu-repo/semantics/" in input_type_lower:
        input_type_lower = input_type_lower.split("info:eu-repo/semantics/")[-1]
    elif "purl.org/coar/resource_type/" in input_type_lower:
        coar_to_type = {
            "c_93fc": "article",    # contribution to journal
            "c_6501": "article",    # journal article
            "c_bdcc": "article",    # research article
            "c_db06": "dissertation",    # dissertation (corrected from CSV)
            "r60j-j5bd": "article", # article
            "c_5794": "article",    # journal article
            "c_2f33": "article",    # article
        }
        coar_id = input_type_lower.split("/")[-1]
        return coar_to_type.get(coar_id, "other")
    elif "purl.org/coar/version/" in input_type_lower:
        return "article"
    
    # Check comprehensive mapping dictionary
    return REPO_TYPE_MAPPING.get(input_type_lower, "other")

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

def detect_version_from_metadata(metadata_string, native_id):
    """
    Detect version from stringified metadata and native_id based on regex patterns
    Returns 'acceptedVersion', 'publishedVersion', or 'submittedVersion'
    """
    
    ACCEPTED_VERSION_REPOS = [
        "oai:catalog.lib.kyushu-u.ac.jp",
        "oai:cronfa.swan.ac.uk",
        "oai:dora",
        "oai:e-space.mmu.ac.uk",
        "oai:hrcak.srce.hr",
        "oai:infocom.co.jp",
        "oai:library.wur.nl",
        "oai:lirias2repo.kuleuven.be",
        "oai:mro.massey.ac.nz",
        "oai:raumplan.iaus.ac.rs",
        "oai:repository.arizona.edu",
        "oai:repository.cardiffmet.ac.uk",
        "oai:researchbank.swinburne.edu.au",
        "oai:researchonline.gcu.ac.uk",
        "oai:rke.abertay.ac.uk",
        "oai:shura.shu.ac.uk",
        "oai:taju.uniarts.fi"
    ]
    
    if native_id:
        native_id_str = str(native_id)
        for repo in ACCEPTED_VERSION_REPOS:
            if native_id_str.startswith(repo + ":"):
                return "acceptedVersion"
    
    if not metadata_string:
        return "submittedVersion"
    
    search_text = str(metadata_string).lower()
    
    accepted_patterns = [
        r"accepted.?version",
        r"version.?accepted", 
        r"accepted.?manuscript",
        r"peer.?reviewed",
        r"refereed/peer-reviewed"
    ]
    
    for pattern in accepted_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "acceptedVersion"
    
    published_patterns = [
        r"publishedversion",
        r"published.*version",
        r"version.*published"
    ]
    
    for pattern in published_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "publishedVersion"
    
    return "submittedVersion"

@F.pandas_udf(StringType())
def get_openalex_type_from_repo_udf(repo_type_series: pd.Series) -> pd.Series:
    return repo_type_series.apply(get_openalex_type_from_repo)

@F.pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@F.pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

@F.pandas_udf(StringType())
def detect_version_udf(metadata_series: pd.Series, native_id_series: pd.Series) -> pd.Series:
    return pd.Series([
        detect_version_from_metadata(metadata, native_id) 
        for metadata, native_id in zip(metadata_series, native_id_series)
    ])

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

# @TODO Convert to Pandas UDF - repo_parsed is slow
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
    .withColumn("raw_native_type", F.col("`ns0:metadata`.`ns1:dc`.`dc:type`"))
    .withColumn("type", get_openalex_type_from_repo_udf(F.col("raw_native_type")))
    .filter(~F.lower(F.col("raw_native_type")).isin(TYPES_TO_DELETE))  # Filter out records marked for deletion (case-insensitive)
    .withColumn("metadata_string", F.col("`ns0:metadata`").cast("string"))
    .withColumn("version", detect_version_udf(
        F.col("metadata_string"), 
        F.col("native_id")
    ))
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
        "raw_native_type",
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
    
    walden_works_schema_with_raw_type = StructType([
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
        StructField("raw_native_type", StringType(), True), StructField("type", StringType(), True), StructField("version", StringType(), True),
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
        StructField("mesh", StringType(), True), StructField("is_oa", BooleanType(), True)
    ])

    # Apply consistent schema and transformations
    df_walden_works = apply_initial_processing(df_parsed_input, "repo", walden_works_schema_with_raw_type)
    df_backfill_walden_works = apply_initial_processing(df_parsed_backfill, "repo_backfill", walden_works_schema_with_raw_type)
    
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
