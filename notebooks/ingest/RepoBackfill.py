# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.22-py3-none-any.whl

# COMMAND ----------

import re
import unicodedata
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# oxjob #881: the work-type vocabulary and the policy filters used to be copy-pasted here from
# Repo.py (~1,240 lines). The maps stayed in sync; the filters built on them did not.
from openalex.dlt.repo_types import best_type_udf
from openalex.dlt.repo_filters import apply_repo_policy_filters
from openalex.dlt.sequencing import dedupe_by_sequence
from openalex.dlt.repo_ids import extract_ids_udf
# oxjob #880: the title normalizer had drifted into a local copy here; one definition, in the wheel.
from openalex.dlt.normalize import normalize_title_udf

# COMMAND ----------

# oxjob #881 -- ONE-TIME REBUILD SWITCH. Default false; leave it false.
#
# false (normal): MERGE with whenNotMatchedBySourceDelete. Incremental, deletes flow through CDF
#   to repo_works, no DLT refresh needed. This is the steady state.
#
# true (exceptional): overwrite the table outright. Needed once, to clear damage the MERGE cannot
#   reach -- as of 2026-08-26 that is 82,121,583 duplicate native_ids left by write paths that
#   predate dedupe_by_sequence. The MERGE cannot remove them: whenNotMatchedBySourceDelete only
#   deletes rows whose key is ABSENT from the source, and a duplicate's key is present, so every
#   copy matches and is updated in place.
#
#   Deleting them by hand is NOT a safe alternative now that the delete path is live: a DELETE of
#   duplicate rows emits delete CDF for keys that should REMAIN, and apply_changes would take the
#   last event per key and drop the record from repo_works entirely.
#
#   AFTER a rebuild run you MUST full-refresh the Repo DLT pipeline. An overwrite is not a
#   row-level change feed, so repo_parsed_backfill's readChangeFeed stream fails with "Detected a
#   data update/delete in the source table" until it is refreshed. Set the widget back to false
#   immediately afterwards.
# Text, not dropdown: a dropdown widget raises if a job supplies a value outside its choice list,
# and this is driven by the job's base_parameters (jobs/repo_backfill.yaml).
dbutils.widgets.text("rebuild", "false", "Full rebuild (overwrite)")
REBUILD = dbutils.widgets.get("rebuild").strip().lower() in ("true", "1", "yes")
print(f"RepoBackfill mode: {'REBUILD (overwrite)' if REBUILD else 'MERGE (incremental)'}")

# COMMAND ----------

# first run
# df = (
#     spark.read
#     .parquet("s3a://openalex-ingest/parquet_output/")
# )

# save to table so we don't need to query s3 next time
# df.write.format("delta").mode("overwrite").saveAsTable("openalex.repo.repo_items_backfill")

# second run
df = spark.table("openalex.repo.repo_items_backfill")

# COMMAND ----------

clean_df = df.withColumn("cleaned_xml",
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("api_raw"), 
                        r'^"{3}', ''
                    ),
                    r'"{3}$', ''
                ),
                r'\\\"', '"'
            ),
            r'""', '"'
        )
    )
).drop("api_raw")

# COMMAND ----------

# udfs
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

url_pattern = r"(https?://\S+|www\.\S+)"

   
def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),
        
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", "other-oa"),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "other-oa"),
        ("arxiv.orgperpetual", "other-oa"),
        ("arxiv.orgnonexclusive", "other-oa"),
        
        # creative Commons licenses
        ("ccbyncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("ccby", "cc-by"),
        ("creativecommons.org/licenses/by/", "cc-by"),
        
        # public domain
        ("publicdomain", "public-domain"),
        
        # software/Dataset licenses
        ("mit ", "mit"),
        ("gpl3", "gpl-3"),
        ("gpl2", "gpl-2"),
        ("gpl", "gpl"),
        ("apache2", "apache-2.0")
    ]

    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license

    return None

def has_oa_domain(native_id):
    oa_domains = ["arxiv", "osti", "pubmedcentral", "biorxiv", "medrxiv", "zenodo", "figshare", "open-science.canada"]
    if native_id is None:
        return False
    
    parts = native_id.lower().split(":")
    if len(parts) >= 2:
        domain_part = parts[1]
        for domain in oa_domains:
            if domain in domain_part:
                return True
    return False

def detect_version_from_xml(cleaned_xml, native_id):
    """
    Detect version from XML content and native_id based on regex patterns
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
    
    # Check if native_id starts with any of the accepted repo prefixes
    if native_id:
        native_id_str = str(native_id)
        for repo in ACCEPTED_VERSION_REPOS:
            if native_id_str.startswith(repo + ":"):
                return "acceptedVersion"
    
    if not cleaned_xml:
        return "submittedVersion"
    
    search_text = str(cleaned_xml).lower()
    
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


@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

@pandas_udf(StringType())
def detect_version_udf(metadata_series: pd.Series, native_id_series: pd.Series) -> pd.Series:
    return pd.Series([
        detect_version_from_xml(metadata, native_id) 
        for metadata, native_id in zip(metadata_series, native_id_series)
    ])


# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MIN_ABSTRACT_LENGTH = 100
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")
# oxjob #881: whenMatchedUpdateAll does NOT evolve the target schema on its own, and the
# mergeSchema option below only applies to the first-run branch. Without this the new
# set_spec/dc_format columns are silently dropped on every MERGE.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# oxjob #881: set_spec / dc_format below are the repository's OWN classification of the record
# ("photographs", "theses", "datasets") and its MIME type. Repo.py reads them from its parsed
# struct; here the raw XML is regexed, and <setSpec> is confirmed BARE -- zero namespaced
# occurrences across a 2M-row sample of repo_items_backfill.api_raw (oxjob #881 evidence/q60).
# Both are dropped again before repo_works via except_column_list in Repo.py, so they never
# enter the cross-source union where repo_works is the canonical schema donor.

parsed_df = clean_df \
    .withColumn("native_id", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
    .withColumn("native_id_namespace", lit("pmh")) \
    .withColumn("title", substring(regexp_extract(col("cleaned_xml"), r"<dc:title.*?>(.*?)</dc:title>", 1), 0, MAX_TITLE_LENGTH)) \
    .withColumn("normalized_title", normalize_title_udf(col("title"))) \
    .withColumn("authors", 
        expr(f"""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:creator>(.*?)</dc:creator>'),
                x -> struct(
                    cast(null as string) as given,
                    cast(null as string) as family,
                    substring(x, 0, {MAX_AUTHOR_NAME_LENGTH}) as name,
                    cast(null as string) as orcid,
                    array(struct(
                        cast(null as string) as name,
                        cast(null as string) as department,
                        cast(null as string) as ror_id
                    )) as affiliations
                )
            )
        """)) \
    .withColumn("raw_native_types", expr("regexp_extract_all(cleaned_xml, '<dc:type.*?>(.*?)</dc:type>', 1)")) \
    .withColumn("set_spec", expr("regexp_extract_all(cleaned_xml, '<setSpec>(.*?)</setSpec>', 1)")) \
    .withColumn("dc_format", expr("regexp_extract_all(cleaned_xml, '<dc:format.*?>(.*?)</dc:format>', 1)")) \
    .withColumn("_best", best_type_udf(col("raw_native_types"))) \
    .withColumn("raw_native_type", col("_best.raw_native_type")) \
    .withColumn("type", lit(None).cast("string")) \
    .drop("_best") \
    .transform(apply_repo_policy_filters) \
    .withColumn("identifiers", 
        expr("""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:identifier>(.*?)</dc:identifier>'),
                x -> trim(x)
            )
        """)) \
    .withColumn("ids",
        extract_ids_udf(
            col("identifiers"),
            col("native_id")
        )) \
    .withColumn("version", detect_version_udf(col("cleaned_xml"), col("native_id"))) \
    .withColumn("language", normalize_language_code_udf(regexp_extract(col("cleaned_xml"), r"<dc:language.*?>(.*?)</dc:language>", 1))) \
    .withColumn("published_date",
        expr("""
            array_min(
                filter(
                    transform(
                        regexp_extract_all(cleaned_xml, '<dc:date.*?>(.*?)</dc:date>'),
                        date_str -> coalesce(
                            -- ISO format with timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss'Z'")),
                            -- ISO format without timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss")),
                            -- Regular date
                            to_date(date_str, "yyyy-MM-dd"),
                            -- Month and year
                            to_date(date_str, "yyyy-MM"),
                            -- Period-separated format
                            to_date(regexp_replace(date_str, '\\.', '-'), "yyyy-MM-dd"),
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
        """)) \
    .withColumn("created_date", col("published_date")) \
    .withColumn("updated_date", to_date(
        regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
    .withColumn("abstract_raw", 
        element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1)) \
    .withColumn("abstract",
        when(length(col("abstract_raw")) >= MIN_ABSTRACT_LENGTH, 
            substring(col("abstract_raw"), 0, MAX_ABSTRACT_LENGTH))
        .otherwise(lit(None))) \
    .withColumn("source_name", 
        regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
    .withColumn("publisher", 
        regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
    .withColumn("urls",
        expr("""
            transform(
                case
                    when size(regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')) > 0
                    then regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')
                    else regexp_extract_all(cleaned_xml, '<dc:relation>(http.*?)</dc:relation>')
                end,
                x -> struct(
                    x as url,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content-type`,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content_type`
                )
            )
        """)) \
    .withColumn("raw_license",
        expr("""
            case 
                when size(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>')) > 0 
                then element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>'), 1)
                else element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?)</dc:rights>'), 1)
            end
        """)) \
    .withColumn("license", normalize_license_udf(col("raw_license"))) \
    .withColumn("issue", lit(None).cast("string")) \
    .withColumn("volume", lit(None).cast("string")) \
    .withColumn("first_page", lit(None).cast("string")) \
    .withColumn("last_page", lit(None).cast("string")) \
    .withColumn("is_retracted", lit(None).cast("boolean")) \
    .withColumn("funders", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("ror"),
            lit(None).cast("string").alias("name"),
            array(lit(None).cast("string")).alias("awards")
        )
    )) \
    .withColumn("references", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("pmid"),
            lit(None).cast("string").alias("arxiv"),
            lit(None).cast("string").alias("title"),
            lit(None).cast("string").alias("authors"),
            lit(None).cast("string").alias("year"),
            lit(None).cast("string").alias("raw")
        )
    )) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn(
    "is_oa",
        when(
            lower(col("license")).startswith("cc") |
            lower(col("license")).contains("other-oa") |
            lower(col("license")).contains("public-domain") |
            has_oa_domain_udf(col("native_id")),
            lit(True)
        ).otherwise(lit(False))
    ) \
    \
    .filter(size(col("urls")) > 0) \
    .filter(size(filter(col("urls"), lambda x: ~x.url.contains("doi.org"))) > 0)

# Select final columns in the same order as DLT
parsed_df = parsed_df.select(
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
    "is_oa",
    "endpoint_id",
    "set_spec",
    "dc_format"
)

# oxjob #911: the whole backfill corpus arrived in S3 as one parquet_output export
# (607 chunks, all mtime 2025-01-16 -- verified via s3 ls). ingested_at means "when the
# source file arrived in our S3", so every backfill row carries that batch timestamp;
# stamped physically on the table 2026-08-28, re-stamped here so MERGE/rebuild runs
# keep the column populated.
parsed_df = parsed_df.withColumn(
    "ingested_at", lit("2025-01-16 15:52:00").cast("timestamp"))

# Keep the newest record per native_id. oxjob #837/#881: this WAS
#   parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])
# which only looks deterministic -- dropDuplicates does not promise to keep the first row of a
# preceding sort, so reruns could pick a different winner for the same key. That matters here:
# repo_works_backfill holds 82,121,583 duplicate native_ids over 228,564,455 distinct keys, so
# which row survives is decided for 82M keys. 3,932,463 rows are additionally tied on a NULL
# updated_date (though 99.99% of those carry no usable URL and never reach repo_works anyway).
# dedupe_by_sequence appends a content hash, giving a total order.
parsed_df = dedupe_by_sequence(
    parsed_df,
    keys=["native_id"],
    order_by=[col("updated_date").desc_nulls_last()],
)

# oxjob #881: MERGE that also DELETES (Casey, 2026-08-26).
#
# This used to be a MERGE with whenMatchedUpdateAll/whenNotMatchedInsertAll and NO delete clause,
# so a record the parse stopped producing was never removed from the target. That is why
# tightening the filters upstream did nothing to what was already stored: 14,945,709 untyped
# archive.org rows, ~16.4M denylisted types, ~11.5M short titles and 3,932,145 rows with no usable
# URL all survived filters meant to exclude them, plus 82,121,583 duplicate native_ids.
#
# whenNotMatchedBySourceDelete makes the target a true mirror of the parse. Deletions emit
# row-level CDF, which Repo.py's repo_parsed_backfill now forwards, and dlt.apply_changes turns
# into real deletes in repo_works via apply_as_deletes.
#
# A rebuild (mode overwrite) was tried first and reverted: an overwrite is not a row-level change
# feed, so it breaks the readChangeFeed stream and forces a DLT full refresh on EVERY run. This
# keeps the pipeline incremental.
target_table = "openalex.repo.repo_works_backfill"

from delta.tables import DeltaTable

# GUARD -- whenNotMatchedBySourceDelete deletes every target row the source does not contain, so
# a short or partial parse would silently wipe the corpus. If the source is unexpectedly small
# relative to the target, stop rather than delete.
#
# Compare against the target's DISTINCT native_id count, not its row count. parsed_df is already
# deduped on native_id (dedupe_by_sequence above), while the target still carries duplicates from
# write paths that predate that dedup -- 82,121,583 of them as of 2026-08-26. Comparing a deduped
# source against a duplicated target understates the ratio badly and trips this guard on a
# perfectly healthy parse: on the 2026-08-26 run it read 60.3% against rows where the real figure
# was 81.9% against distinct ids. native_id is the MERGE key, so distinct is the honest baseline.
# After the first successful run the target is unique and the two measures converge.
MIN_KEEP_RATIO = 0.75

# The guard applies to BOTH modes -- an overwrite with a short parse destroys just as much as a
# delete-by-absence does.
# Persist before the guard: its count() materializes the full parse, and without a cache Spark
# recomputes the entire 310M-row regex parse a second time for the write. On the 4-worker run the
# parse dominates wall-clock, so this is close to a 2x on the whole job.
from pyspark import StorageLevel
parsed_df = parsed_df.persist(StorageLevel.MEMORY_AND_DISK)

if spark.catalog.tableExists(target_table):
    _src = parsed_df.count()
    _tgt_rows = spark.table(target_table).count()
    _tgt_keys = spark.table(target_table).select("native_id").distinct().count()
    _ratio = (_src / _tgt_keys) if _tgt_keys else 1.0
    if _tgt_keys > 0 and _ratio < MIN_KEEP_RATIO:
        raise ValueError(
            f"RepoBackfill aborted: source has {_src:,} rows vs {_tgt_keys:,} distinct native_ids "
            f"in the target ({_ratio:.1%}), below MIN_KEEP_RATIO={MIN_KEEP_RATIO:.0%}. "
            f"(Target holds {_tgt_rows:,} rows, so {_tgt_rows - _tgt_keys:,} are duplicate keys.) "
            "The difference would be destroyed -- deleted by whenNotMatchedBySourceDelete in "
            "MERGE mode, or simply not written in REBUILD mode. Investigate the parse before "
            "re-running; do not lower the ratio to get past this."
        )
    _dropped_keys = _tgt_keys - _src if _tgt_keys > _src else 0
    print(f"RepoBackfill: source {_src:,} rows vs {_tgt_keys:,} distinct target keys "
          f"({_ratio:.1%}); target holds {_tgt_rows:,} rows across those keys.")
    if REBUILD:
        # the overwrite is the only path that collapses duplicates -- see the switch comment
        print(f"  REBUILD: table becomes exactly the {_src:,} parsed rows. "
              f"{_tgt_rows - _src:,} rows go away: {_tgt_rows - _tgt_keys:,} duplicate keys "
              f"collapsed plus {_dropped_keys:,} keys filtered out.")
    else:
        # MERGE does NOT collapse duplicate keys: whenNotMatchedBySourceDelete removes only rows
        # whose native_id is ABSENT from the source, and a duplicate's key is present, so every
        # copy matches and is updated in place. They stay as CDF noise (~82M redundant update
        # events per run) -- not a correctness problem, since repo_works keys on native_id via
        # apply_changes and collapses them anyway.
        print(f"  MERGE: {_dropped_keys:,} keys are absent from the source and their rows will "
              f"be DELETED. Remaining keys are updated in place, duplicates included -- "
              f"{_tgt_rows - _tgt_keys:,} duplicate rows survive (use REBUILD to clear them).")

if not spark.catalog.tableExists(target_table):
    (parsed_df.write.format("delta")
        .option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table))

elif REBUILD:
    print(f"REBUILD: overwriting {target_table} -- collapsing duplicate keys and applying "
          f"filters in one pass. FULL-REFRESH the Repo DLT pipeline afterwards, then set the "
          f"rebuild widget back to false.")
    (parsed_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

else:
    (DeltaTable.forName(spark, target_table).alias("target")
        .merge(parsed_df.alias("source"), "target.native_id = source.native_id")
        # NULL-safe: 3,932,463 rows carry a NULL updated_date and NULL >= NULL is NULL, which
        # MERGE treats as false -- those rows could never be refreshed (oxjob #881 REPOBACKFILL
        # CHANGES section C).
        .whenMatchedUpdateAll(
            condition="source.updated_date IS NULL OR target.updated_date IS NULL "
                      "OR source.updated_date >= target.updated_date")
        .whenNotMatchedInsertAll()
        # the whole point: rows the parse no longer produces are removed, not stranded
        .whenNotMatchedBySourceDelete()
        .execute())
