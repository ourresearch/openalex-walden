# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.22-py3-none-any.whl

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
import pandas as pd

import re
import unicodedata
from functools import reduce

from openalex.utils.environment import *
from openalex.dlt.normalize import normalize_title_udf, normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys
from openalex.dlt.repo_types import best_type_udf
from openalex.dlt.repo_filters import apply_repo_policy_filters, apply_endpoint_filters
from openalex.dlt.repo_ids import extract_ids_udf



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

normalize_language_code_udf = F.udf(normalize_language_code, StringType())
detect_version_udf = F.udf(detect_version_from_metadata, StringType())

# COMMAND ----------

# IDs UDF

url_pattern = r"(https?://\S+|www\.\S+)"


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
            StructField("dc:type", ArrayType(StringType()), True),
            StructField("dc:identifier", ArrayType(StringType()), True),
            StructField("dc:language", StringType(), True),
            StructField("dc:format", ArrayType(StringType()), True),
            StructField("dc:publisher", StringType(), True),
            StructField("dc:rights", ArrayType(StringType()), True),
            StructField("dc:relation", ArrayType(StringType()), True)
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
      # Discovery via UC managed file events on the openalex-ingest external location
      # (millions of tiny per-record gzips make directory listing take hours).
      .option("cloudFiles.useManagedFileEvents", "true")
      # oxjob #911: the default rate limit (maxFilesPerTrigger=1000) caps ingest at
      # ~600 files/s regardless of cluster size -- micro-batches complete every ~1.5-2s,
      # so 1000-file batches leave the executors idle (measured: identical ~12K rows/s
      # on 20 and 60 nodes). 50K-file batches amortize the per-batch overhead; steady
      # state nightly volume never approaches this, so it only matters for refreshes.
      .option("cloudFiles.maxFilesPerTrigger", "50000")
      .load("s3a://openalex-ingest/repositories/")
      # Named repository_id historically; renamed to endpoint_id in repo_parsed.
      # Kept here to avoid re-ingesting the entire streaming table.
      .withColumn("repository_id",
          F.regexp_extract(F.col("_metadata.file_path"), r"repositories/([^/]+)/", 1))
      # oxjob #911: S3 object mtime = when the harvester delivered the file, not when this
      # stream read it. Stable across re-reads, so a full refresh no longer restamps the
      # corpus (the #542 taxicab hazard). Rows before the cutover carry pipeline-read times.
      .withColumn("ingested_at", F.col("_metadata.file_modification_time"))
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
        .withColumn("raw_native_types", F.col("`ns0:metadata`.`ns1:dc`.`dc:type`"))
    # oxjob #881: the repository's OWN classification of the record ("photographs", "theses",
    # "datasets") and the MIME type. Both were already parsed into repo_items and thrown away
    # here. setSpec is populated on 84.14% of 1,136,117,802 repo_items rows, dc:format on 47.71%.
    # They are dropped again before repo_works (see except_column_list) so they never enter the
    # cross-source union, where repo_works is the canonical schema donor.
    # oxjob #881: repo_items is not a CDF source, so every live record is an upsert. The column
    # exists so the three-way union is uniform and apply_as_deletes has something to test.
    .withColumn("_change_type", F.lit("upsert"))
    .withColumn("set_spec", F.col("`ns0:header`.`ns0:setSpec`"))
    .withColumn("dc_format", F.col("`ns0:metadata`.`ns1:dc`.`dc:format`"))
    # oxjob #537: choose the best element across the full dc:type array (not just the first).
    # raw_native_type = the winning element's ORIGINAL full string; type = its mapped value.
    .withColumn("_best", best_type_udf(F.col("raw_native_types")))
    .withColumn("raw_native_type", F.col("_best.raw_native_type"))
    # ingest no longer assigns type; the work-type cascade owns it (raw_native_type kept as evidence)
    .withColumn("type", F.lit(None).cast("string"))
    .drop("_best")
    # oxjob #881: one implementation, in openalex.dlt.repo_filters. Applied again on the union in
    # repo_enriched() -- idempotent -- so backfill and irdb get the same rules. Kept here too so
    # repo_parsed stays lean rather than materialising rows the union would drop.
    .transform(apply_repo_policy_filters)
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
        "published_date",
        F.expr("""
            array_min(
                filter(
                    transform(
                        `ns0:metadata`.`ns1:dc`.`dc:date`,
                        date_str -> coalesce(
                            -- ISO format with timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss'Z'")),
                            -- ISO format without timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss")),
                            -- Regular date
                            to_date(date_str, "yyyy-MM-dd"),
                            -- Month and year
                            to_date(date_str, "yyyy-MM"),
                            -- Year only
                            to_date(
                                if(length(trim(date_str)) = 4, concat(date_str, "-01-01"), null),
                                "yyyy-MM-dd"
                            )
                        )
                    ),
                    d -> d is not null and year(d) >= 1900
                )
            )
        """)
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
    # oxjob #945: the two halves of the OAI identifier, used to test whether a dc:relation
    # URL is this record's own page. oai:eprints.lancs.ac.uk:11007 -> host, local id.
    .withColumn("_oai_host", F.regexp_extract(F.col("native_id"), r"^oai:([^:]+):", 1))
    .withColumn("_oai_local", F.regexp_extract(F.col("native_id"), r"([^:]+)$", 1))
    .withColumn(
        "_identifier_urls",
        F.filter(
            F.transform(
                F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`dc:identifier`"), F.array()),
                lambda x: F.struct(
                    F.when(
                        F.regexp_extract(x, url_pattern, 0).startswith("www."),
                        F.concat(F.lit("https://"), F.regexp_extract(x, url_pattern, 0))
                    ).otherwise(F.regexp_extract(x, url_pattern, 0)).alias("url"),
                    F.when(x.rlike("(?i)pdf"), F.lit("pdf"))
                    .otherwise(F.lit("html"))
                    .alias("content_type"),
                ),
            ),
            lambda x: x["url"] != "",
        )
    )
    .withColumn(
        "_relation_urls",
        F.filter(
            F.transform(
                F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`dc:relation`"), F.array()),
                lambda x: F.struct(
                    F.when(
                        F.regexp_extract(x, url_pattern, 0).startswith("www."),
                        F.concat(F.lit("https://"), F.regexp_extract(x, url_pattern, 0))
                    ).otherwise(F.regexp_extract(x, url_pattern, 0)).alias("url"),
                    F.when(x.rlike("(?i)pdf"), F.lit("pdf"))
                    .otherwise(F.lit("html"))
                    .alias("content_type"),
                ),
            ),
            # oxjob #945: dc:relation is Dublin Core for a *related* resource -- cited
            # references, project pages, instrument manuals -- so it is only this record's
            # landing page when it identifies this record. EPrints-family repos legitimately
            # put the item URL here (4.5M records), so the field is not banned; the URL has to
            # carry the OAI local id or the OAI host. doi.org is excluded outright: those
            # landing pages are the crossref/datacite lane's job (100% of both, vs 2.4% of
            # repo). The length guards matter -- contains("") is true for every string.
            lambda x: (x["url"] != "")
            & (~F.lower(x["url"]).contains("doi.org/"))
            & (
                ((F.length("_oai_local") > 0)
                 & F.lower(x["url"]).contains(F.lower(F.col("_oai_local"))))
                | ((F.length("_oai_host") > 0)
                   & F.lower(x["url"]).contains(F.lower(F.col("_oai_host"))))
            ),
        )
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
            F.when(
                F.size(F.col("_identifier_urls")) > 0,
                F.col("_identifier_urls")
            ).otherwise(F.col("_relation_urls"))
        )
    )
    .drop("has_pmcid", "pmcid", "_identifier_urls", "_relation_urls", "_oai_host", "_oai_local")
    .filter(F.size(F.col("urls")) > 0)
    .filter(F.size(F.filter(F.col("urls"), lambda x: ~x.url.contains("doi.org"))) > 0)
    .withColumn("mesh", F.lit(None).cast("string"))
    .withColumn(
        "is_oa",
        F.when(
            F.lower(F.col("license")).startswith("cc") | 
            F.lower(F.col("license")).contains("other-oa") |
            F.lower(F.col("license")).contains("public-domain") |
            (
                (F.size(F.split(F.col("native_id"), ":")) >= 2) &
                F.lower(F.split(F.col("native_id"), ":")[1]).rlike("arxiv|osti|pubmedcentral|biorxiv|medrxiv|zenodo|figshare|open-science\\.canada")
            ),
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
        "raw_native_types",
        "raw_native_type",
        "set_spec",
        "dc_format",
        "_change_type",
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
        F.col("repository_id").alias("endpoint_id"),
        "ingested_at"
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
            # oxjob #881: 'delete' is now FORWARDED, not dropped. Without it a record removed
            # from the source could never leave repo_works -- the pipeline had no delete path at
            # all. update_preimage stays excluded (it is the stale half of an update).
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .drop("_commit_version", "_commit_timestamp")
    )


@dlt.table(
    name="repo_parsed_irdb",
    temporary=True,
    comment="Streaming read of IRDB parsed records using CDF (automatically incremental in DLT)"
)
def repo_parsed_irdb():
    irdb_schema = spark.table("openalex.repo.irdb_parsed").schema
    return (
        spark.readStream
            .option("readChangeFeed", "true")
            .schema(irdb_schema)
            .table("openalex.repo.irdb_parsed")
            # oxjob #881: 'delete' is now FORWARDED, not dropped. Without it a record removed
            # from the source could never leave repo_works -- the pipeline had no delete path at
            # all. update_preimage stays excluded (it is the stale half of an update).
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .drop("_commit_version", "_commit_timestamp")
    )


@dlt.table(name="repo_enriched",
           comment="repo data after full parsing and author/feature enrichment.")
def repo_enriched():
    df_parsed_backfill = dlt.read_stream("repo_parsed_backfill")
    df_parsed_input = dlt.read_stream("repo_parsed")
    df_parsed_irdb = dlt.read_stream("repo_parsed_irdb")
    
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
        StructField("mesh", StringType(), True), StructField("is_oa", BooleanType(), True),
        StructField("endpoint_id", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
        # oxjob #881: repo-local only. This is NOT the shared walden schema -- crossref, datacite,
        # pubmed and the rest are untouched. Sources that lack these get typed NULLs from
        # apply_walden_schema (transform.py:132), so backfill and irdb need no change until
        # RepoBackfill.py is re-run.
        StructField("set_spec", ArrayType(StringType()), True),
        StructField("dc_format", ArrayType(StringType()), True),
        # oxjob #881: drives apply_as_deletes below; dropped before repo_works
        StructField("_change_type", StringType(), True)
    ])

    # Apply consistent schema and transformations
    df_walden_works = apply_initial_processing(df_parsed_input, "repo", walden_works_schema_with_raw_type)
    df_backfill_walden_works = apply_initial_processing(df_parsed_backfill, "repo_backfill", walden_works_schema_with_raw_type)
    df_irdb_walden_works = apply_initial_processing(df_parsed_irdb, "repo", walden_works_schema_with_raw_type)

    # Combine all three streams
    combined_df = (
        df_walden_works
        .unionByName(df_backfill_walden_works, allowMissingColumns=True)
        .unionByName(df_irdb_walden_works, allowMissingColumns=True)
    )

    # oxjob #881: THE gate. repo_parsed_backfill and repo_parsed_irdb are raw CDF passthroughs
    # with no filtering of their own, so 20,874,419 records reached repo_works past rules we had
    # already agreed to -- 20,872,994 of them (99.99%) from backfill. Applying it on the union
    # covers all three streams and any stream added later.
    # oxjob #881: _IS_DELETE bypasses every filter between here and apply_changes. A delete event
    # carries the pre-image of a record we are removing *because* it is junk, so it fails these
    # very rules -- filter it and the deletion silently never happens.
    _IS_DELETE = F.col("_change_type") == "delete"
    combined_df = apply_repo_policy_filters(combined_df, keep_when=_IS_DELETE)
    # oxjob #881 round 2: endpoint denylist + setSpec carves. Union-only -- this is the one
    # place every stream carries endpoint_id and set_spec.
    combined_df = apply_endpoint_filters(combined_df, keep_when=_IS_DELETE)

    # a record with no usable URL can never become a location (no scrape seed, no landing page)
    combined_df = combined_df.filter(
        _IS_DELETE | F.expr("exists(urls, x -> x.url IS NOT NULL)"))

    for c in ["published_date", "updated_date", "created_date"]:
        combined_df = combined_df.withColumn(
            c,
            F.when(
                (F.col(c) < F.lit("1500-01-01").cast("date")) |
                (F.col(c) > F.date_add(F.current_date(), 30)),
                F.lit(None).cast("date")
            ).otherwise(F.col(c))
        )

    # Tiebreaker: updated_date first, then repo over backfill, then latest ingested_at,
    # then stable content hash — backfill has no ingested_at, leaving 110M rows fully
    # tied; without a total order every full refresh picks different winners
    combined_df = combined_df.withColumn(
        "_sequence",
        F.struct(
            F.col("updated_date"),
            F.when(F.col("provenance") == "repo", F.lit(1)).otherwise(F.lit(0)),
            F.coalesce(F.col("ingested_at"), F.lit("1970-01-01").cast("timestamp")),
            F.xxhash64(*[F.col(c) for c in combined_df.columns])
        )
    )

    # backfill feed keeps its identity through _sequence above; the published
    # value is always repo. Restamped BEFORE merge-key computation ON PURPOSE
    # (Casey 08-07): short/bad-title fallback keys converge to the clean
    # native_id+'repo' form; existing locations_mapped keys were renamed in
    # place to match, preserving work_ids. (First attempt at this restamp
    # re-minted 14.7M works because locations_mapped still had the old keys.)
    combined_df = combined_df.withColumn("provenance", F.lit("repo"))

    # Apply enrichment (with fast Pandas UDFs)
    df_enriched = enrich_with_features_and_author_keys(combined_df)

    return apply_final_merge_key_and_filter(df_enriched, keep_when=_IS_DELETE)

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

# Content-keyed dedup (same endpoint + identical URL set) was tried here and
# REVERTED 08-06: repo_enriched streams every historical version of a record,
# and URL-format drift between harvests (http->https, &amp; re-encoding, domain
# moves) gives old versions their own content keys — 33M stale variants
# resurrected. DLT cannot chain apply_changes (targets can't be streamed), so
# same-URL dedup lives in the batch layer (CreateSuperLocations), not here.
dlt.apply_changes(
    target="repo_works",
    source="repo_enriched",
    keys=["native_id"],
    sequence_by="_sequence",
    # oxjob #881: the pipeline can finally express "this record went away". Previously deletes
    # were dropped in three separate places, so a record could never leave repo_works.
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_sequence", "set_spec", "dc_format", "_change_type"]
)
