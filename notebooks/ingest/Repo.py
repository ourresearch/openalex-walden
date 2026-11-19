# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.43-py3-none-any.whl

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
import pandas as pd

import re
import unicodedata
from functools import reduce

from openalex.dlt.normalize import normalize_title_udf, normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys
from openalex.dlt.repo_types import best_type_udf
from openalex.dlt.repo_filters import apply_repo_policy_filters, apply_endpoint_filters
from openalex.dlt.repo_ids import extract_ids_udf
from openalex.dlt.oai_ids import read_endpoint_hosts, with_rekeyed_native_id

# oxjob #933: repositories that host only open content attest OA on their own. OSTI is
# the origin of its reports (10.2172) and DOE data-centre DOIs, but a record carrying a
# publisher's DOI is a pointer at a paywalled article; those get OA only from a PDF the
# PDF pipeline actually fetched (composed in CreateSuperLocations).
OSTI_ORIGIN_DOI_PREFIXES = "2172|25582|17188|11578|5439|18429|15121|21947|25585|17190|17182|15485|18141|15473|34664|7910"
# oxjob #1407: reads oai_identifier, the RAW header id. native_id may be re-keyed from a placeholder
# host to the endpoint's own host, and an unanchored RLIKE on that host ('osti', 'zenodo') must not
# flip is_oa. Keep byte-identical with RepoBackfill.py.
TRUSTED_HOST_IS_OA_EXPR = f"""
    size(split(oai_identifier, ':')) >= 2 AND (
      lower(split(oai_identifier, ':')[1]) RLIKE 'arxiv|pubmedcentral|europepmc|biorxiv|medrxiv|zenodo|open-science\\\\.canada'
      OR (
        lower(split(oai_identifier, ':')[1]) RLIKE 'osti'
        AND NOT exists(ids, x -> x.namespace = 'doi'
                             AND NOT lower(x.id) RLIKE '(^|doi\\\\.org/)10\\\\.({OSTI_ORIGIN_DOI_PREFIXES})/')
      )
    )
"""
# oxjob #1344: PubMed Central / Europe PMC version from the record's own OAI setSpec. PMC oai_dc
# never states a version, so the regex in detect_version_* fell through to submittedVersion for
# ~2.9M works. The set names the kind of deposit: funder author-manuscript sets (>=99.6% carry a
# PubMed NIHMS/UKMS/CAMS/HALMS manuscript id; gatesmanu 63/110), preprint sets, or a journal set
# (publisher deposit). NULL for non-PMC records and PMC records with no setSpec -> the regex
# decides, as before. Keep byte-identical with the other ingest notebook. Reads the RAW id (oxjob #1407).
PMC_VERSION_FROM_SET_SPEC_EXPR = """
    CASE WHEN (lower(oai_identifier) LIKE 'oai:pubmedcentral.nih.gov:%' OR lower(oai_identifier) LIKE 'oai:europepmc.org:%')
              AND size(set_spec) > 0 THEN
      CASE WHEN arrays_overlap(set_spec, array('nihpa','wtpa','hhspa','epapa','nistpa','vapa','capmc','nasapa',
                                                'hhmipa','hal','hrams','dhspa','asms','gatesmanu','manusctipt'))
             THEN 'acceptedVersion'
           WHEN arrays_overlap(set_spec, array('biorxiv','medrxiv','ressq','arxiv','ssrn','chemrxiv'))
             THEN 'submittedVersion'
           ELSE 'publishedVersion'
      END
    END
"""



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

# oxjob #227: prefixes the oai_dc wrapper gets besides ns1 (why: repository_schema below), in coalesce order.
DC_WRAPPER_ALIASES = ("ns2:dc", "ns3:dc", "dc:dc")

# The Dublin Core record's fields, the same under every wrapper prefix.
_dc_record_struct = StructType([
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
            # Licence carried outside dc:rights. The harvester re-serialises records with
            # ElementTree, which names unregistered namespaces ns0, ns1, ... in order of first
            # use, so this matches the SERIALISED PREFIX, not the namespace URI: dcterms lands
            # as ns4 in every harvested file that carries accessRights/license (Persee:
            # dcterms:accessRights = "Content available online under CC BY-NC-SA 3.0 (...)"
            # behind a copyright line in dc:rights; Pure: dcterms:license). Files that bind
            # dcterms to another nsN are not covered (3 of 8,485 sampled files, isPartOf only).
            # Text only: an element's attributes (DOAB's licenseCondition uri=...) are dropped
            # by StringType, the row is not corrupted.
            StructField("ns4:accessRights", ArrayType(StringType()), True),
            StructField("ns4:license", ArrayType(StringType()), True),
            StructField("dc:relation", ArrayType(StringType()), True)
        ])

repository_schema = StructType([
    StructField("ns0:header", StructType([
        # StructField("@status", StringType(), True),
        StructField("ns0:identifier", StringType(), True),
        StructField("ns0:datestamp", TimestampType(), True),
        StructField("ns0:setSpec", ArrayType(StringType()), True)
    ]), True),
    StructField("ns0:metadata", StructType([
        StructField("ns1:dc", _dc_record_struct, True),
        # oxjob #227: the same record under the other prefixes its wrapper gets in a harvested file. ElementTree names
        # a namespace ns<count of namespaces seen so far, registered ones included>, so the oai_dc wrapper is ns1 only
        # when nothing else came first: xsi:schemaLocation on <metadata> (WestminsterResearch, EPrints) makes it ns2,
        # dc and xsi both make it ns3. A wrapper in the dc-elements namespace itself, <dc xmlns="http://purl.org/dc/
        # elements/1.1/"> instead of <oai_dc:dc> (IRUA, UAntwerp: every record since at least Jan 2025), is stored as
        # <dc:dc>, "dc" being ElementTree's registered prefix. ns1:dc came back NULL for all of them: no title, so
        # repo_parsed dropped the record (19,253 IRUA rows, 0 parsed). Read only here: fold_dc_wrapper() moves the
        # one present into ns1:dc before anything is stored, so no table gains a column.
        *[StructField(name, _dc_record_struct, True) for name in DC_WRAPPER_ALIASES],
    ]), True)
])


def fold_dc_wrapper(df):
  """oxjob #227: `ns0:metadata` as stored, struct<ns1:dc>: the oai_dc wrapper under whichever prefix the file gave it
  (ns1:dc, else DC_WRAPPER_ALIASES in order). Applied where the raw files are read (repo_items, RepoReparse.py), so
  tables keep their schema and every reader of `ns0:metadata`.`ns1:dc` sees these records too. A record with no
  <metadata> (deleted) stays NULL."""
  return df.withColumn(
      "ns0:metadata",
      F.when(
          F.col("`ns0:metadata`").isNotNull(),
          F.struct(F.coalesce(*[F.col(f"`ns0:metadata`.`{name}`") for name in ("ns1:dc", *DC_WRAPPER_ALIASES)])
                   .alias("ns1:dc")),
      ),
  )

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

# CC0 named as text in dc:rights: the normaliser knows the CC0 URL and the words "public domain"
# only. Whole-value forms seen in the corpus (whitespace collapsed, lower-cased, trailing period
# stripped); a bare token would also match revocations such as "In Copyright - The CC0 1.0
# Universal license was removed from this item". The value is checked as a whole, so a
# sentence that merely mentions CC0 does not count.
CC0_TEXT_PATTERNS = (
    r"^(creative commons |cc |licen[cs]e:? ?)?cc0([- ]1\.0)?( universal| universell| uniwersalna| παγκόσμια| public domain dedication)?( \(cc0([- ]1\.0)?\))?$"
    r"|^cc-?zero$|^creative commons \(cc0\)$|^creative commons zero v?1\.0 universal( \(cc0[- ]1\.0\))?$"
    r"|^cc0:? creative commons zero( 1\.0)?( universal)?$|^cc0[- ]?(1\.0 )?no rights reserved$|^no rights reserved \(cc0( 1\.0)?\)$"
    r"|^https?://(spdx\.org|choosealicense\.com)/licenses/cc0-1\.0(\.html)?/?$|^https?://api\.github\.com/licenses/cc0-1\.0$"
    r"|^https?://(www\.)?opendefinition\.org/licenses/cc-zero/?$"
    r"|^dedicaci[óo]n de dominio p[úu]blico 1\.0 universal\.? \(cc0\)$|^uznanie cc0 1\.0 universal \(cc0 1\.0\) przekazanie do domeny publicznej$"
)
# Whole values only. The full CC0 legal-code text, the deed sentence ("To the extent possible under
# law, X has waived ...") and per-item statements ("Items in this record are available as ...") are
# deliberately not matched: an open-ended prefix would also accept a negated or qualified sentence.
# A dc:rights value that restricts access blocks every fallback below (not the dc:rights pick
# itself, whose behaviour is unchanged). Tested on the normalised form (spaces/hyphens removed).
RIGHTS_RESTRICTED_PATTERN = r"restrictedaccess|closedaccess|embargoedaccess|rightsstatements\.org/vocab/inc|incopyright"

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
      .transform(fold_dc_wrapper)  # oxjob #227: the oai_dc wrapper under any prefix read as ns1:dc; stored shape unchanged
  )

# COMMAND ----------

def parse_repo_records(df, id_hosts, abstract=None, issue=None, volume=None, first_page=None, last_page=None,
                       dedupe=True, pdf_urls=None):
  """repo_parsed's column rules for any stream of oai_dc-shaped records (`ns0:header`, `ns0:metadata`.`ns1:dc`,
  repository_id, ingested_at): repo_items (the live harvest) and hal_tei_parsed (HAL rebuilt from its TEI, oxjob
  #1786). The keyword columns replace what oai_dc can't carry; with the defaults the plan is exactly repo_parsed's.
  dedupe=False skips the (native_id, datestamp) dedup, whose streaming state keeps the FIRST copy forever: the HAL
  TEI stream lets every copy through so the latest harvest (and a re-emitted CDF row, via _commit_version) wins
  in repo_works' _sequence instead. pdf_urls (oxjob #2429): a column of URLs the record itself says are PDFs; those
  urls are typed pdf whatever their text says."""
  return (df
    .filter(F.col("ns0:metadata").isNotNull())  # doaj deleted articles have no metadata
    # oxjob #1407: native_id is the OAI header id, except that a placeholder host is replaced by the
    # endpoint's own host: oai:ojs.pkp.sfu.ca:article/1 -> oai:et.ippt.pan.pl:article/1. Without it,
    # journals that never set their repository identifier share one id space and apply_changes keeps
    # one record per id. Done BEFORE dropDuplicates so two journals' records with the same datestamp
    # don't drop each other. The raw id stays in oai_identifier for everything that parses the OAI
    # host; it is not carried past this table.
    .withColumn("oai_identifier", F.col("`ns0:header`.`ns0:identifier`"))
    .transform(lambda df: with_rekeyed_native_id(df, id_hosts, "repository_id"))
    .withColumn("updated_date", F.col("`ns0:header`.`ns0:datestamp`"))
    .transform(lambda d: d.dropDuplicates(["native_id", "updated_date"]) if dedupe else d)
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
    .withColumn("version", F.coalesce(
        F.expr(PMC_VERSION_FROM_SET_SPEC_EXPR),
        detect_version_udf(F.col("metadata_string"), F.col("oai_identifier"))  # raw id, oxjob #1407
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
    # Fallbacks when the dc:rights pick normalises to nothing (e.g. a copyright line): the
    # alternative fields with the same creativecommons.org-first selection rule, then CC0 named
    # as text. Both are blocked by a restricting dc:rights value.
    .withColumn(
        "_alt_rights",
        F.concat(
            F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`ns4:accessRights`"), F.array().cast("array<string>")),
            F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`ns4:license`"), F.array().cast("array<string>")),
        ),
    )
    .withColumn(
        "raw_alt_license",
        F.when(
            F.expr("size(filter(_alt_rights, x -> x like '%creativecommons.org%')) > 0"),
            F.expr("filter(_alt_rights, x -> x like '%creativecommons.org%')[0]"),
        ).otherwise(F.expr("get(_alt_rights, 0)")),  # get(): null on an empty array under ANSI
    )
    .withColumn(
        "_rights_restricted",
        F.exists(
            F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`dc:rights`"), F.array().cast("array<string>")),
            # coalesce: exists() over a null element would return null, and ~null would then
            # block the fallbacks for a record that restricts nothing.
            lambda x: F.coalesce(F.regexp_replace(F.lower(x), "[ -]", "").rlike(RIGHTS_RESTRICTED_PATTERN), F.lit(False)),
        ),
    )
    .withColumn(
        "_cc0_text",
        F.exists(
            F.coalesce(F.col("`ns0:metadata`.`ns1:dc`.`dc:rights`"), F.array().cast("array<string>")),
            lambda x: F.coalesce(F.regexp_replace(F.regexp_replace(F.lower(F.trim(x)), r"\s+", " "), r"\.$", "").rlike(CC0_TEXT_PATTERNS), F.lit(False)),
        ),
    )
    .withColumn(
        "license",
        F.coalesce(
            normalize_license_udf(F.col("raw_license")),
            F.when(
                ~F.col("_rights_restricted"),
                F.coalesce(
                    normalize_license_udf(F.col("raw_alt_license")),
                    F.when(F.col("_cc0_text"), F.lit("public-domain")),
                ),
            ),
        ),
    )
    .drop("_alt_rights", "raw_alt_license", "_rights_restricted", "_cc0_text")
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
    .withColumn("issue", F.lit(None).cast("string") if issue is None else issue)
    .withColumn("volume", F.lit(None).cast("string") if volume is None else volume)
    .withColumn("first_page", F.lit(None).cast("string") if first_page is None else first_page)
    .withColumn("last_page", F.lit(None).cast("string") if last_page is None else last_page)
    .withColumn("is_retracted", F.lit(None).cast("boolean"))
    .withColumn(
        "abstract",
        F.substring(F.col("`ns0:metadata`.`ns1:dc`.`dc:description`").getItem(0) if abstract is None else abstract,
                    0, MAX_ABSTRACT_LENGTH)
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
    # oxjob #1700: OAI-PMH Dublin Core carries no references, so the list is NULL, typed as IRDB.py does. It was a one-entry
    # all-NULL list, which outranked the record's PDF list in CreateSuperLocations and made parse_work_references treat the
    # work as read (203M repository locations on 3 Nov 2026).
    .withColumn("references", F.lit(None).cast(ArrayType(StructType([
        StructField(n, StringType(), True) for n in ("doi", "pmid", "arxiv", "title", "authors", "year", "raw")]))))
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
    # oxjob #1407: from the RAW id, so a re-keyed record's dc:relation handling is unchanged.
    .withColumn("_oai_host", F.regexp_extract(F.col("oai_identifier"), r"^oai:([^:]+):", 1))
    .withColumn("_oai_local", F.regexp_extract(F.col("oai_identifier"), r"([^:]+)$", 1))
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
    # oxjob #2266: Zenodo's oai_dc (since its 2023 move to InvenioRDM) lists only https://doi.org/<doi>, so the
    # doi.org filter below dropped 7.6M of Zenodo's 8.8M records. Those with Zenodo's own DOI (10.5281/zenodo.*) reach
    # their DataCite work as a Zenodo location already and stay out. The ~1.22M with a publisher's or funder's DOI were
    # the only Zenodo copy of their work: their record page, built from the OAI id, is the landing page. A second host
    # that lists only doi.org is the cue to move this into a per-host record-URL table.
    .withColumn(
        "urls",
        F.when(
            (F.col("_oai_host") == "zenodo.org")
            & F.col("_oai_local").rlike(r"^[0-9]+$")
            & ~F.exists("urls", lambda x: ~x["url"].contains("doi.org"))
            & ~F.exists("ids", lambda x: (x["namespace"] == "doi") & F.lower(x["id"]).startswith("10.5281/zenodo")),
            F.array(F.struct(
                F.concat(F.lit("https://zenodo.org/records/"), F.col("_oai_local")).alias("url"),
                F.lit("html").alias("content_type"),
            )),
        ).otherwise(F.col("urls"))
    )
    # oxjob #2429: an oai_openaire record names its files' mime types, so its PDFs are typed pdf even when the URL
    # doesn't say so (Wageningen's https://edepot.wur.nl/215208). Other streams skip this step: their plan is unchanged.
    .transform(lambda d: d if pdf_urls is None else d.withColumn("urls", F.transform(
        "urls", lambda u: F.struct(
            u["url"].alias("url"),
            F.when(F.array_contains(F.coalesce(pdf_urls, F.array().cast("array<string>")), u["url"]), F.lit("pdf"))
             .otherwise(u["content_type"]).alias("content_type")))))
    .drop("has_pmcid", "pmcid", "_identifier_urls", "_relation_urls", "_oai_host", "_oai_local")
    .filter(F.size(F.col("urls")) > 0)
    .filter(F.size(F.filter(F.col("urls"), lambda x: ~x.url.contains("doi.org"))) > 0)
    .withColumn("mesh", F.lit(None).cast("string"))
    .withColumn(
        "is_oa",
        # oxjob #2266: Zenodo is a trusted host (#933), but a Zenodo record can say its files are restricted, closed or
        # embargoed (dc:rights, beside a licence that applies once access is granted): not an open copy, whatever its
        # licence. A null test falls through to the rules below.
        F.when(
            F.col("oai_identifier").startswith("oai:zenodo.org:") & F.exists(
                "`ns0:metadata`.`ns1:dc`.`dc:rights`",
                lambda x: F.lower(x).rlike(r"info:eu-repo/semantics/(restricted|closed|embargoed)access")),
            F.lit(False)
        ).when(
            F.lower(F.col("license")).startswith("cc") |
            F.lower(F.col("license")).contains("other-oa") |
            F.lower(F.col("license")).contains("public-domain") |
            F.expr(TRUSTED_HOST_IS_OA_EXPR),
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
        "ingested_at",
        *(["_commit_version"] if "_commit_version" in df.columns else []),  # CDF sources only (oxjob #1786)
    )
  )


@dlt.table(
  name="repo_parsed"
)
def repo_parsed():
  # oxjob #1407: placeholder-host ids (ojs.pkp.sfu.ca, generic.eprints.org, localhost, ojs.localhost)
  # are keyed on the endpoint's own pmh_url host, from the endpoint registry read once per update and
  # inlined as a literal map (no join, so no stateful-plan change). Endpoints whose placeholder
  # records were already stored are excluded until Phase B moves them (oai_ids_excluded).
  id_hosts = read_endpoint_hosts(spark)
  return parse_repo_records(dlt.read_stream("repo_items"), id_hosts)


# oxjob #1786: HAL from its TEI. openalex.repo.hal_tei_parsed (HAL TEI pipeline, HalTei.py) holds, per harvested
# record, the oai_dc record HAL would serve for it, rebuilt from the TEI, so every derived column (ids, type, version,
# licence, dates, URLs) comes from the same rules as repo_parsed. Three columns come from the TEI instead: the abstract
# (the real one in the document's language; oai_dc led with HAL's 'International audience' flag, #1635) and the
# volume/issue/pages oai_dc has no field for. Same native_id (oai:HAL:<id>v<N>) and endpoint_id as the oai_dc row it
# replaces, so W-ids and sources stay put. Read through CDF like repo_parsed_irdb.
HAL_TEI_PARSED_TABLE = "openalex.repo.hal_tei_parsed"


@dlt.table(
    name="repo_parsed_hal_tei",
    temporary=True,
    comment="HAL records rebuilt from TEI (oxjob #1786), parsed with repo_parsed's rules"
)
def repo_parsed_hal_tei():
    return parse_hal_tei_records(
        spark.readStream
            .option("readChangeFeed", "true")
            .table(HAL_TEI_PARSED_TABLE)
            .filter(F.col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_timestamp")  # _commit_version feeds _sequence, as for backfill/IRDB
    )


def parse_hal_tei_records(df):
    """hal_tei_parsed rows -> repo_parsed rows. Shared with notebooks/maintenance/HalTeiCutoverDryRun.py. No endpoint
    registry ({}): it only re-keys placeholder hosts (ojs.pkp.sfu.ca, localhost, ...), never oai:HAL:."""
    return parse_repo_records(
        df, {}, dedupe=False,
        abstract=F.col("tei_abstract"),
        issue=F.col("tei_issue"),
        volume=F.col("tei_volume"),
        first_page=F.col("tei_first_page"),
        last_page=F.col("tei_last_page"),
    )


# oxjob #1828: raw files re-read with today's schema, for a field repo_items never stored (why and how to run it:
# notebooks/ingest/RepoReparse.py). Same shape as the HAL TEI flow: its own stream, no dedup, rank 2 in _sequence,
# its own CDC flow into repo_works. Deletes in the side table are not forwarded: a re-parse only replaces a record.
REPO_REPARSE_ITEMS_TABLE = "openalex.repo.repo_reparse_items"


@dlt.table(
    name="repo_parsed_reparse",
    temporary=True,
    comment="Raw repository files re-read with today's schema (oxjob #1828), parsed with repo_parsed's rules"
)
def repo_parsed_reparse():
    return parse_reparse_records(
        spark.readStream
            .option("readChangeFeed", "true")
            .table(REPO_REPARSE_ITEMS_TABLE)
            .filter(F.col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_timestamp"),  # _commit_version feeds _sequence: a re-run re-emits and wins
        read_endpoint_hosts(spark),
    )


def parse_reparse_records(df, id_hosts):
    """repo_reparse_items rows -> repo_parsed rows. Shared with RepoReparse.py's dry run. Same endpoint re-key as
    repo_parsed, so a record keeps its native_id."""
    return parse_repo_records(df, id_hosts, dedupe=False)


# COMMAND ----------

# oxjob #2429: repositories harvested in oai_openaire (OpenAIRE Guidelines for Literature Repositories v4). Many put no
# access statement and no file link in oai_dc (Wageningen's dc:rights names only the institution), so their open files
# read closed; the same record in oai_openaire carries the COAR access right and the file URLs. An endpoint switched to
# metadata_prefix oai_openaire keeps its id and lands in s3://openalex-ingest/openaire/<endpoint id>/ (harvester
# PREFIX_S3_FOLDER), not repositories/, which repo_items reads with a fixed oai_dc schema. Each record is rebuilt as the
# oai_dc record repo_parsed reads (same native_id, endpoint and rules; the access right written as the eu-repo statement
# those rules already read), then flows into repo_works like HAL TEI: its own stream, no dedup, rank 2 in _sequence, so
# on the same datestamp it replaces the oai_dc row harvested before the switch.
# The parser block is copied verbatim from oxjobs todo/openaire-format-repository-feeds/work/openaire_dc.py, where it
# is tested against both formats of the same records; edit both.
OPENAIRE_S3_PATH = "s3a://openalex-ingest/openaire/"
# Endpoints whose open records count as open copies without a file link: feeds that never list files, switched only
# after their landing pages pass the bar (oxjob #2429 ACCEPTANCE Test 1). Everyone else needs an open file.
OPENAIRE_OPEN_WITHOUT_FILE = set()

# --- openaire_dc: begin (verbatim from oxjob #2429 work/openaire_dc.py) ---
import datetime
import gzip
import io
import xml.etree.ElementTree as ET

OAI = "{http://www.openarchives.org/OAI/2.0/}"
OAIRE = "{http://namespace.openaire.eu/schema/oaire/}"
DATACITE = "{http://datacite.org/schema/kernel-4}"
DATACITE3 = "{http://datacite.org/schema/kernel-3}"  # a few DSpace crosswalks still bind kernel-3
DC = "{http://purl.org/dc/elements/1.1/}"
DCTERMS = "{http://purl.org/dc/terms/}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

COAR_ACCESS = {  # http://purl.org/coar/access_right/<code>
    "c_abf2": "open",
    "c_f1cf": "embargoed",
    "c_16ec": "restricted",
    "c_14cb": "closed",  # metadata only access
}
ACCESS_LABEL = {"open access": "open", "embargoed access": "embargoed", "restricted access": "restricted",
                "metadata only access": "closed", "closed access": "closed"}
EUREPO_ACCESS = {"open": "info:eu-repo/semantics/openAccess", "embargoed": "info:eu-repo/semantics/embargoedAccess",
                 "restricted": "info:eu-repo/semantics/restrictedAccess", "closed": "info:eu-repo/semantics/closedAccess"}
COAR_VERSION = {  # http://purl.org/coar/version/<code> -> Repo.py's version vocabulary
    "c_970fb48d4fbd8a85": "publishedVersion",   # VoR
    "c_ab4af688f83e57aa": "acceptedVersion",    # AM
    "c_e19f295774971610": "acceptedVersion",    # P (proof)
    "c_71e4c1898caa6e32": "submittedVersion",   # SMUR
    "c_b1a7d7d4d402bcce": "submittedVersion",   # AO (author's original)
    "c_dc82b40f9837b551": "publishedVersion",   # CVoR (corrected VoR)
    "c_be7fb7dd8ff6fe43": "publishedVersion",   # EVoR (enhanced VoR)
}
VERSION_LABEL = {"vor": "publishedVersion", "am": "acceptedVersion", "smur": "submittedVersion",
                 "ao": "submittedVersion", "p": "acceptedVersion", "cvor": "publishedVersion", "evor": "publishedVersion",
                 "publishedversion": "publishedVersion", "acceptedversion": "acceptedVersion",
                 "submittedversion": "submittedVersion"}


def _t(el):
    if el is None:
        return None
    return re.sub(r"\s+", " ", "".join(el.itertext())).strip() or None


def _dc(res, local):
    """Elements named `local` under either DataCite namespace."""
    return res.findall(f".//{DATACITE}{local}") + res.findall(f".//{DATACITE3}{local}")


def _coar_code(uri):
    m = re.search(r"/(c_[0-9a-f]+)\s*$", uri or "")
    return m.group(1) if m else None


SAME_ITEM_RELATIONS = {"IsIdenticalTo", "IsVersionOf", "IsVariantFormOf", "IsOriginalFormOf"}
EUREPO_TEXT = {"openaccess": "open", "embargoedaccess": "embargoed", "restrictedaccess": "restricted",
               "closedaccess": "closed"}


def _access(res):
    """(access, source): the record's COAR access right (datacite:rights; a few feeds put the eu-repo statement in
    dc:rights instead), else the most open of its files'."""
    for r in _dc(res, "rights"):
        a = COAR_ACCESS.get(_coar_code(r.get("rightsURI"))) or ACCESS_LABEL.get((_t(r) or "").lower())
        if a:
            return a, "rights"
    for r in res.findall(f"{DC}rights"):
        m = re.search(r"info:eu-repo/semantics/(\w+access)", _t(r) or "", re.I)
        if m and m.group(1).lower() in EUREPO_TEXT:
            return EUREPO_TEXT[m.group(1).lower()], "dc_rights"
    order = ["open", "embargoed", "restricted", "closed"]
    files = [COAR_ACCESS.get(_coar_code(f.get("accessRightsURI"))) for f in res.findall(f"{OAIRE}file")]
    files = [f for f in files if f]
    if files:
        return min(files, key=order.index), "file"
    return None, None


def openaire_to_dc(res, today=None, open_needs_file=True):
    """One <oaire:resource> element -> dict: `dc` (Repo.py's ns1:dc fields) plus access, version, file URLs, ORCIDs.

    open_needs_file: an open record counts as an open copy only when it lists a file that is itself open (or carries no
    access right of its own). A record that is "open" with no file is usually a link to the publisher's copy or a
    news clipping (Wageningen, 6 of 45 open records sampled 19 Nov); feeds that never list files (TNO) are switched
    with it False only after their landing pages pass the bar."""
    today = today or datetime.date.today().isoformat()

    titles = [(t.get("titleType"), _t(t)) for t in _dc(res, "title")]
    main = next((t for ty, t in titles if not ty and t), None) or next((t for _, t in titles if t), None)
    sub = next((t for ty, t in titles if ty == "Subtitle" and t), None)
    title = f"{main}: {sub}" if main and sub and sub.lower() not in main.lower() else main

    creators, orcids = [], []
    for c in _dc(res, "creator"):
        names = [x for x in (_t(n) for n in c.findall(f"{DATACITE}creatorName") + c.findall(f"{DATACITE3}creatorName")) if x]
        family = _t(c.find(f"{DATACITE}familyName")) or _t(c.find(f"{DATACITE3}familyName"))
        given = _t(c.find(f"{DATACITE}givenName")) or _t(c.find(f"{DATACITE3}givenName"))
        # "Family, Given" as oai_dc writes it, when the display name isn't (CWI: "A.C. Kik (Annette)" with family Kik)
        if len(names) == 1 and family and given and "," not in names[0]:
            names = [f"{family}, {given}"]
        oid = next((_t(n) for n in c.findall(f"{DATACITE}nameIdentifier") + c.findall(f"{DATACITE3}nameIdentifier")
                    if (n.get("nameIdentifierScheme") or "").upper() == "ORCID"), None)
        m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", oid or "")
        for name in names:  # some feeds (TNO) put every author's creatorName in one creator element
            creators.append(name)
            orcids.append(m.group(1) if m and len(names) == 1 else None)
    contributors = [x for x in (_t(c.find(f"{DATACITE}contributorName")) for c in _dc(res, "contributor")) if x]

    dates = {}
    for d in _dc(res, "date"):
        dates.setdefault(d.get("dateType"), _t(d))
    issued = dates.get("Issued") or dates.get("Accepted") or dates.get("Created") or _t(res.find(f"{DC}date"))

    access, access_from = _access(res)
    available = dates.get("Available")
    # An embargo that has run out: the file is free now, whatever the record still says (HalTei.py does the same).
    embargo_ended = bool(available and re.match(r"\d{4}-\d{2}-\d{2}", available) and available[:10] <= today)
    if access == "embargoed" and embargo_ended:
        access, access_from = "open", access_from + "+embargo_ended"

    files, pdf_urls, files_seen = [], [], False
    for f in res.findall(f"{OAIRE}file"):
        u = _t(f)
        if not (u and re.match(r"https?://", u)):
            continue
        files_seen = True
        code = COAR_ACCESS.get(_coar_code(f.get("accessRightsURI")))
        if code is None or code == "open" or (code == "embargoed" and embargo_ended):
            if u not in files:
                files.append(u)
                if "pdf" in (f.get("mimeType") or "").lower():
                    pdf_urls.append(u)
    # Rights in Repo.py's terms: a CC licence first only on an open copy (Repo.py picks the first creativecommons.org
    # value), then the eu-repo access statement (openAccess -> other-oa; the others restrict and block the fallbacks).
    # A closed copy's licence is left out, or Repo.py's licence fallback would reopen it.
    lic = [x for x in (l.get("uri") or _t(l) for l in res.findall(f"{OAIRE}licenseCondition")) if x]
    # An open record with a CC licence and no file stays open, as its oai_dc licence already made it (Repo.py's
    # licence rule; Wageningen dry run 19 Nov: 68 of 16,984 records).
    has_cc = any("creativecommons.org" in x for x in lic)
    is_open = access == "open" and (bool(files) or has_cc or not open_needs_file)
    if access == "open" and not is_open:
        access_from += "+no_open_file" if files_seen else "+no_file"
    if is_open:
        rights = [x for x in lic if "creativecommons.org" in x] + [EUREPO_ACCESS["open"]]
    else:
        rights = [EUREPO_ACCESS[access]] if access in ("embargoed", "restricted", "closed") else []

    identifier = []
    for i in _dc(res, "identifier") + _dc(res, "alternateIdentifier"):
        typ = (i.get("identifierType") or i.get("alternateIdentifierType") or "").upper()
        val = _t(i)
        if not val:
            continue
        if typ == "DOI":
            d = re.search(r"10\.\d{4,9}/\S+", val)
            val = f"doi:{d.group(0)}" if d else None
        elif typ == "HANDLE" and not val.startswith("http"):
            val = f"http://hdl.handle.net/{val.removeprefix('hdl:')}"
        elif typ in ("PMID",):
            val = None  # a bare PMID matches nothing in extract_ids; skipped rather than faked into a URL
        if val and val not in identifier:
            identifier.append(val)
    # DSpace 7's crosswalk puts the item's own DOI in a relatedIdentifier (IsIdenticalTo; IsVersionOf for the published
    # version of an accepted manuscript), where its oai_dc has it in dc:identifier. Other relations
    # (IsPartOf, IsSupplementTo, References ...) name other things and stay out.
    for i in _dc(res, "relatedIdentifier"):
        if (i.get("relatedIdentifierType") or "").upper() == "DOI" and i.get("relationType") in SAME_ITEM_RELATIONS:
            d = re.search(r"10\.\d{4,9}/\S+", _t(i) or "")
            if d and f"doi:{d.group(0)}" not in identifier:
                identifier.append(f"doi:{d.group(0)}")
    identifier += [f for f in files if f not in identifier]  # open files only: a restricted file is no copy

    vers = res.find(f"{OAIRE}version")
    version = None
    if vers is not None:
        version = COAR_VERSION.get(_coar_code(vers.get("uri"))) or VERSION_LABEL.get((_t(vers) or "").lower().replace(" ", ""))
    # The eu-repo version goes where only Repo.py's version rule reads it (the text of the whole record): in dc:type,
    # where DSpace's oai_dc puts it, best_type_udf ranks it as an article and it became raw_native_type (dry run 19 Nov).
    rtype = res.find(f"{OAIRE}resourceType")
    dc_type = [x for x in [_t(rtype)] if x]
    subjects = [x for x in (_t(s) for s in _dc(res, "subject")) if x] + ([f"info:eu-repo/semantics/{version}"] if version else [])

    def texts(path):
        return [x for x in (_t(e) for e in res.findall(path)) if x]

    cit = {k: _t(res.find(f"{OAIRE}citation{k}")) for k in ("Title", "Volume", "Issue", "StartPage", "EndPage")}
    publisher = texts(f"{DC}publisher")
    lang = texts(f"{DC}language")
    return {
        "dc": {
            "dc:title": title,
            "dc:creator": creators,
            "dc:contributor": contributors,
            "dc:subject": subjects,
            "dc:description": texts(f"{DC}description"),
            "dc:source": cit["Title"],
            "dc:date": [issued] if issued else [],
            "dc:type": dc_type,
            "dc:identifier": identifier,
            "dc:language": lang[0] if lang else None,
            "dc:format": texts(f"{DC}format") or None,
            "dc:publisher": publisher[0] if publisher else None,
            "dc:rights": rights or None,
            "ns4:accessRights": None,
            "ns4:license": (lic or None) if is_open else None,
            "dc:relation": None,
        },
        "access": access,
        "access_from": access_from,
        "is_open": is_open,
        "version": version,
        "file_urls": files,
        "pdf_urls": pdf_urls,
        "creator_orcids": orcids,
        "volume": cit["Volume"], "issue": cit["Issue"], "first_page": cit["StartPage"], "last_page": cit["EndPage"],
    }


def parse_record(rec, today=None, open_needs_file=True):
    """One OAI <record> element -> dict with header fields and `p` (openaire_to_dc) or None."""
    h = rec.find(OAI + "header")
    out = {"oai_id": _t(h.find(OAI + "identifier")), "datestamp": _t(h.find(OAI + "datestamp")),
           "set_spec": [_t(s) for s in h.findall(OAI + "setSpec")], "is_deleted": h.get("status") == "deleted",
           "p": None}
    md = rec.find(OAI + "metadata")
    res = md[0] if md is not None and len(md) else None
    # local name only: a few feeds (TNO) write <resource> with no default namespace, its children still prefixed
    if out["is_deleted"] or res is None or res.tag.rsplit("}", 1)[-1] != "resource":
        return out
    out["p"] = openaire_to_dc(res, today, open_needs_file)
    return out
# --- openaire_dc: end ---


def parse_openaire_file(data, path):
    """Harvester file bytes (gzip or not) -> one row per record, streamed record by record as HalTei.py does. A file
    that fails to parse becomes one error row (counted by the expectation), never a failed update."""
    m = re.search(r"openaire/([^/]+)/", path or "")
    needs_file = not (m and m.group(1) in OPENAIRE_OPEN_WITHOUT_FILE)
    try:
        stream = gzip.GzipFile(fileobj=io.BytesIO(data)) if data[:2] == b"\x1f\x8b" else io.BytesIO(data)
        out = []
        for _, el in ET.iterparse(stream, events=("end",)):
            if el.tag == OAI + "record":
                r = parse_record(el, open_needs_file=needs_file)
                p = r.pop("p") or {}
                out.append({**r, **{k: p.get(k) for k in ("dc", "access", "access_from", "is_open", "version", "pdf_urls",
                                                          "creator_orcids", "volume", "issue", "first_page", "last_page")}})
                el.clear()
        return out
    except Exception as e:  # noqa: BLE001 -- surfaced as a row, never swallowed
        return [{"is_deleted": False, "parse_error": f"{type(e).__name__}: {e}"[:1000]}]


_openaire_row_type = StructType([
    StructField("oai_id", StringType()), StructField("datestamp", StringType()),
    StructField("set_spec", ArrayType(StringType())), StructField("is_deleted", BooleanType()),
    StructField("dc", _dc_record_struct),  # repository_schema's ns1:dc: same names and types
    StructField("access", StringType()), StructField("access_from", StringType()), StructField("is_open", BooleanType()),
    StructField("version", StringType()), StructField("pdf_urls", ArrayType(StringType())),
    StructField("creator_orcids", ArrayType(StringType())),
    StructField("volume", StringType()), StructField("issue", StringType()),
    StructField("first_page", StringType()), StructField("last_page", StringType()),
    StructField("parse_error", StringType()),
])
parse_openaire_file_udf = F.udf(parse_openaire_file, ArrayType(_openaire_row_type))


@dlt.table(
    name="repo_items_openaire",
    table_properties={"quality": "bronze"},
    comment="oai_openaire records as harvested (oxjob #2429), one row per record per file, rebuilt as oai_dc + access",
)
@dlt.expect("file_parsed", "parse_error IS NULL")
def repo_items_openaire():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xml.gz")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/repo/openaire_schema")
        .option("cloudFiles.useManagedFileEvents", "true")  # as repo_items
        .option("cloudFiles.maxFilesPerTrigger", "2000")
        .load(OPENAIRE_S3_PATH)
        .select(
            F.explode(parse_openaire_file_udf(F.col("content"), F.col("path"))).alias("r"),
            F.regexp_extract(F.col("path"), r"openaire/([^/]+)/", 1).alias("repository_id"),
            F.col("path").alias("file_path"),
            F.col("modificationTime").alias("ingested_at"),  # S3 object mtime: when the harvester delivered it (#911)
        )
        .select("r.*", "repository_id", "file_path", "ingested_at")
    )


def openaire_items_as_repo_items(df):
    """repo_items_openaire rows -> repo_items' shape (`ns0:header`, `ns0:metadata`.`ns1:dc`) plus their own columns.
    Shared with the dry run (notebooks/maintenance/OpenAireDryRun.py)."""
    return df.select(
        F.struct(
            F.col("oai_id").alias("ns0:identifier"),
            F.col("datestamp").cast("timestamp").alias("ns0:datestamp"),
            F.col("set_spec").alias("ns0:setSpec"),
        ).alias("ns0:header"),
        F.when(~F.col("is_deleted") & F.col("dc").isNotNull(), F.struct(F.col("dc").alias("ns1:dc"))).alias("ns0:metadata"),
        "repository_id", "ingested_at", "pdf_urls", "volume", "issue", "first_page", "last_page",
    )


def parse_openaire_records(df, id_hosts):
    """repo_items_openaire rows -> repo_parsed rows. Shared with the dry run."""
    return parse_repo_records(
        openaire_items_as_repo_items(df.filter(F.col("parse_error").isNull())), id_hosts, dedupe=False,
        volume=F.col("volume"), issue=F.col("issue"), first_page=F.col("first_page"), last_page=F.col("last_page"),
        pdf_urls=F.col("pdf_urls"),
    )


@dlt.table(
    name="repo_parsed_openaire",
    temporary=True,
    comment="oai_openaire records (oxjob #2429) parsed with repo_parsed's rules"
)
def repo_parsed_openaire():
    return parse_openaire_records(dlt.read_stream("repo_items_openaire"), read_endpoint_hosts(spark))

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
            .drop("_commit_timestamp")  # _commit_version feeds _sequence (oxjob #837/#880)
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
            .drop("_commit_timestamp")  # _commit_version feeds _sequence (oxjob #837/#880)
    )


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
        StructField("_change_type", StringType(), True),
        # oxjob #837/#880: CDF commit version of the backfill/IRDB source row (NULL -> 0 for
        # repo_parsed); a re-emitted row must outrank the stored one. Dropped before repo_works.
        StructField("_commit_version", LongType(), True)
])


def finish_repo_enriched(combined_df, source_rank=None):
    """repo_enriched's rules after the union: policy and endpoint filters, the date guard, _sequence, the provenance
    restamp, enrichment and merge keys. Shared with repo_enriched_hal_tei (oxjob #1786). source_rank replaces the
    repo-over-backfill rank in _sequence, for a stream that must win ties on the same datestamp."""
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
    # oxjob #880: a backfill/IRDB row re-emitted through CDF with only derived columns changed
    # (normalized_title, merge_key) used to tie on the first three terms and win or lose on the
    # content hash -- ~50% of re-keys were silently rejected by apply_changes. The source commit
    # version now ranks ahead of the hash, so a re-emit always outranks its stored version; the
    # hash stays the tiebreak for genuine duplicates. Same design as locations_parsed (#837 Phase 3).
    _hash_cols = [c for c in combined_df.columns if c != "_commit_version"]
    _seq_fields = [
        F.col("updated_date"),
        F.when(F.col("provenance") == "repo", F.lit(1)).otherwise(F.lit(0)),
        F.coalesce(F.col("ingested_at"), F.lit("1970-01-01").cast("timestamp")),
        F.coalesce(F.col("_commit_version"), F.lit(0)).cast("long"),
        F.xxhash64(*[F.col(c) for c in _hash_cols])
    ]
    if source_rank is not None:
        # oxjob #1786: Spark names the struct's fields after their expressions, and every CDC flow into repo_works must
        # carry one identical sequence type, so the replacement rank takes the default's field name.
        _names = combined_df.select(F.struct(*_seq_fields).alias("s")).schema["s"].dataType.fieldNames()
        _seq_fields = [c.alias(n) for c, n in zip(_seq_fields[:1] + [source_rank] + _seq_fields[2:], _names)]
    # Nullability is part of the type, and every CDC flow into repo_works must carry one identical sequence type, or
    # DLT refuses the update ("different sequencing types": #1786, 23 Oct 16:07 CT). The literals and coalesces above
    # are non-nullable. In an incremental update repo_works reads repo_enriched's struct back from the stored table,
    # all-nullable; when repo_enriched is rebuilt in the same update (a selective full refresh) it keeps the query's
    # non-nullable fields, and the refresh failed at analysis (oxjob #1828, 26 Oct). So cast on BOTH paths: the same
    # fields, every one nullable.
    _seq_type = combined_df.select(F.struct(*_seq_fields).alias("s")).schema["s"].dataType
    _nullable = StructType([StructField(f.name, f.dataType, True) for f in _seq_type.fields])
    combined_df = combined_df.withColumn("_sequence", F.struct(*_seq_fields).cast(_nullable))

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


@dlt.table(name="repo_enriched",
           comment="repo data after full parsing and author/feature enrichment.")
def repo_enriched():
    df_parsed_backfill = dlt.read_stream("repo_parsed_backfill")
    df_parsed_input = dlt.read_stream("repo_parsed")
    df_parsed_irdb = dlt.read_stream("repo_parsed_irdb")

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
    return finish_repo_enriched(combined_df)


# oxjob #1786: HAL rebuilt from TEI gets its own stream and its own CDC flow into repo_works (below), not a fourth branch
# of the union above: adding a source to repo_enriched's streaming query would invalidate its checkpoint. Rank 2 in
# _sequence: on the same datestamp the TEI row beats the oai_dc row (rank 1) for the same native_id, so the oai_dc HAL
# harvest can run alongside until its endpoints are retired; a newer oai_dc datestamp still wins until the TEI
# harvest delivers that datestamp too (the next day).
@dlt.table(name="repo_enriched_hal_tei",
           comment="HAL records rebuilt from TEI (oxjob #1786) after repo_enriched's rules; second CDC source of repo_works")
def repo_enriched_hal_tei():
    return enrich_hal_tei(dlt.read_stream("repo_parsed_hal_tei"))


def enrich_hal_tei(df):
    """repo_parsed_hal_tei rows -> repo_enriched_hal_tei rows. Shared with HalTeiCutoverDryRun.py."""
    return finish_repo_enriched(apply_initial_processing(df, "repo", walden_works_schema_with_raw_type),
                                source_rank=F.lit(2))


# oxjob #1828: the re-parse stream, rank 2 like HAL TEI: on the same datestamp the re-parsed row beats the stored
# repo_items row (rank 1) and the backfill row (rank 0) for the same native_id. A newer datestamp still wins whatever
# its rank, so a later harvest replaces it (and carries the new field itself); an older re-parsed copy changes nothing.
# So once re-parsed, a record no longer takes a refresh of repo_parsed / repo_enriched: refresh the two *_reparse tables
# with them (jobs/repo_pipeline.yaml).
@dlt.table(name="repo_enriched_reparse",
           comment="Re-parsed raw repository files (oxjob #1828) after repo_enriched's rules; third CDC source of repo_works")
def repo_enriched_reparse():
    return enrich_reparse(dlt.read_stream("repo_parsed_reparse"))


# The same rules and rank as HAL TEI, one body: every flow into repo_works must carry one identical _sequence type.
enrich_reparse = enrich_hal_tei  # shared with RepoReparse.py's dry run


# oxjob #2429: oai_openaire records, rank 2 like HAL TEI and the re-parse: on the same datestamp the openaire row beats the
# oai_dc row harvested before the endpoint was switched (same native_id); after the switch only this stream delivers.
@dlt.table(name="repo_enriched_openaire",
           comment="oai_openaire records (oxjob #2429) after repo_enriched's rules; fourth CDC source of repo_works")
def repo_enriched_openaire():
    return enrich_hal_tei(dlt.read_stream("repo_parsed_openaire"))

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
# One set of CDC settings for every flow into repo_works (oxjob #1786: two flows must agree on keys, sequence and
# columns, or they break or drift apart).
REPO_WORKS_CDC = dict(
    target="repo_works",
    keys=["native_id"],
    sequence_by="_sequence",
    # oxjob #881: the pipeline can finally express "this record went away". Previously deletes
    # were dropped in three separate places, so a record could never leave repo_works.
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_sequence", "set_spec", "dc_format", "_change_type", "_commit_version"],
)
dlt.apply_changes(source="repo_enriched", **REPO_WORKS_CDC)

# oxjob #1786: the second flow, HAL from TEI (named; the flow above keeps its default name, "repo_works").
dlt.create_auto_cdc_flow(source="repo_enriched_hal_tei", name="repo_works_hal_tei", **REPO_WORKS_CDC)

# oxjob #1828: the third flow, raw files re-parsed with today's schema.
dlt.create_auto_cdc_flow(source="repo_enriched_reparse", name="repo_works_reparse", **REPO_WORKS_CDC)

# oxjob #2429: the fourth flow, repositories harvested in oai_openaire.
dlt.create_auto_cdc_flow(source="repo_enriched_openaire", name="repo_works_openaire", **REPO_WORKS_CDC)
