# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.1-py3-none-any.whl

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

import re

from openalex.utils.environment import *
from openalex.dlt.normalize import normalize_title_udf, normalize_license_udf

# Language normalization (not in shared lib — copied from Repo.py)
def normalize_language_code(lang_code):
    if not lang_code or not isinstance(lang_code, str):
        return None
    lang_code = lang_code.strip().lower()
    if "[[iso]]" in lang_code:
        lang_code = lang_code.replace("[[iso]]", "")
    if lang_code in ["null", "und", "other"]:
        return None
    if "_" in lang_code:
        lang_code = lang_code.split("_")[0]
    if ";" in lang_code:
        lang_code = lang_code.split(";")[0].strip()

    names_to_codes = {
        "english": "en", "spanish": "es", "french": "fr", "german": "de",
        "chinese": "zh", "russian": "ru", "japanese": "ja", "arabic": "ar",
        "portuguese": "pt", "italian": "it",
    }
    three_to_two = {
        "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "rus": "ru",
        "zho": "zh", "jpn": "ja", "ara": "ar", "por": "pt", "ita": "it",
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
        "xho": "xh", "yid": "yi", "yor": "yo", "zha": "za", "zul": "zu",
    }
    if len(lang_code) == 2:
        return lang_code
    if lang_code in names_to_codes:
        return names_to_codes[lang_code]
    if len(lang_code) == 3:
        return three_to_two.get(lang_code)
    return None

normalize_language_code_udf = F.udf(normalize_language_code, StringType())

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500

# COMMAND ----------

# COAR resource type → OpenAlex type
# JPCOAR 2.0 uses COAR URIs like http://purl.org/coar/resource_type/c_6501
IRDB_COAR_TYPE_MAP = F.create_map(
    F.lit("c_6501"), F.lit("article"),       # journal article
    F.lit("c_bdcc"), F.lit("article"),       # research article
    F.lit("c_93fc"), F.lit("article"),       # contribution to journal
    F.lit("c_186u"), F.lit("article"),       # departmental bulletin paper (very common in IRDB)
    F.lit("c_18cf"), F.lit("article"),       # text
    F.lit("c_18co"), F.lit("article"),       # conference paper
    F.lit("c_18op"), F.lit("article"),       # conference proceedings
    F.lit("c_efa0"), F.lit("article"),       # conference poster
    F.lit("c_5794"), F.lit("article"),       # conference paper (alt)
    F.lit("c_2f33"), F.lit("article"),       # book review
    F.lit("r60j-j5bd"), F.lit("article"),    # article (alt code)
    F.lit("c_db06"), F.lit("dissertation"),  # doctoral thesis
    F.lit("c_46ec"), F.lit("dissertation"),  # thesis
    F.lit("c_bdcc"), F.lit("article"),       # research article
    F.lit("c_816b"), F.lit("preprint"),      # preprint
    F.lit("c_8042"), F.lit("report"),        # working paper
    F.lit("c_18gh"), F.lit("report"),        # technical report
    F.lit("c_18ws"), F.lit("report"),        # report
    F.lit("c_beb9"), F.lit("dataset"),       # research data
    F.lit("c_2f33"), F.lit("book"),          # book
    F.lit("c_3248"), F.lit("book-chapter"),  # book part
    F.lit("c_ba08"), F.lit("review"),        # review article
    F.lit("c_dcae04bc"), F.lit("review"),    # review
)

# Text-based type fallback for records without COAR URIs
IRDB_TEXT_TYPE_MAP = F.create_map(
    F.lit("article"), F.lit("article"),
    F.lit("journal article"), F.lit("article"),
    F.lit("departmental bulletin paper"), F.lit("article"),
    F.lit("conference paper"), F.lit("article"),
    F.lit("conference object"), F.lit("article"),
    F.lit("text"), F.lit("article"),
    F.lit("thesis"), F.lit("dissertation"),
    F.lit("doctoral thesis"), F.lit("dissertation"),
    F.lit("master thesis"), F.lit("dissertation"),
    F.lit("bachelor thesis"), F.lit("dissertation"),
    F.lit("book"), F.lit("book"),
    F.lit("book part"), F.lit("book-chapter"),
    F.lit("report"), F.lit("report"),
    F.lit("technical report"), F.lit("report"),
    F.lit("working paper"), F.lit("report"),
    F.lit("preprint"), F.lit("preprint"),
    F.lit("research data"), F.lit("dataset"),
    F.lit("dataset"), F.lit("dataset"),
    F.lit("review"), F.lit("review"),
    F.lit("review article"), F.lit("review"),
    F.lit("software"), F.lit("software"),
    F.lit("conference proceedings"), F.lit("article"),
    F.lit("conference output"), F.lit("article"),
    F.lit("conference presentation"), F.lit("article"),
    F.lit("research paper"), F.lit("article"),
    F.lit("manuscript"), F.lit("article"),
    F.lit("learning object"), F.lit("other"),
    F.lit("lecture"), F.lit("other"),
    F.lit("presentation"), F.lit("other"),
    F.lit("other"), F.lit("other"),
)

# COAR version URI → walden version string
COAR_VERSION_MAP = F.create_map(
    F.lit("c_b1a7d7d4d402bcce"), F.lit("submittedVersion"),   # AO (Author's Original)
    F.lit("c_ab4af688f83e57aa"), F.lit("acceptedVersion"),     # AM (Accepted Manuscript)
    F.lit("c_970fb48d4fbd8a85"), F.lit("publishedVersion"),    # VoR (Version of Record)
    F.lit("c_e19f295774971610"), F.lit("publishedVersion"),    # CVoR (Corrected VoR)
    F.lit("c_dc82b40f9837b551"), F.lit("publishedVersion"),    # EVoR (Enhanced VoR)
    F.lit("c_fa2f19c6d8db04a2"), F.lit("submittedVersion"),    # NA (Not Applicable)
)

# COMMAND ----------

# ID extraction UDF — structured extraction from JPCOAR typed identifiers
id_struct_type = StructType([
    StructField("id", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("relationship", StringType(), True),
])

def extract_jpcoar_ids(identifiers, source_identifiers, native_id):
    try:
        results = []
        seen = set()

        if identifiers:
            for ident in identifiers:
                if not ident:
                    continue
                id_type = ident["_identifierType"]
                value = ident["_VALUE"]
                if not value or not id_type:
                    continue

                if id_type == "DOI":
                    # Strip DOI URL prefixes to get bare DOI
                    doi = re.sub(r"^https?://doi\.org/", "", value.strip())
                    doi = re.sub(r"^https?://dx\.doi\.org/", "", doi)
                    if doi.startswith("10."):
                        key = ("doi", doi)
                        if key not in seen:
                            seen.add(key)
                            results.append({"id": doi, "namespace": "doi", "relationship": None})

                elif id_type == "HDL":
                    handle = re.sub(r"^https?://hdl\.handle\.net/", "", value.strip())
                    key = ("handle", handle)
                    if key not in seen:
                        seen.add(key)
                        results.append({"id": handle, "namespace": "handle", "relationship": None})

        if source_identifiers:
            for si in source_identifiers:
                if not si:
                    continue
                si_type = si["_identifierType"]
                si_value = si["_VALUE"]
                if not si_value or not si_type:
                    continue

                if si_type in ("ISSN", "PISSN", "EISSN"):
                    issn = si_value.strip()
                    # Insert dash if missing (e.g., 0286861X → 0286-861X)
                    if len(issn) == 8 and "-" not in issn:
                        issn = issn[:4] + "-" + issn[4:]
                    key = ("issn", issn)
                    if key not in seen:
                        seen.add(key)
                        results.append({"id": issn, "namespace": "issn", "relationship": None})

        # Always add native_id as pmh self-reference
        if native_id:
            results.append({"id": native_id, "namespace": "pmh", "relationship": "self"})

        return results
    except Exception:
        if native_id:
            return [{"id": native_id, "namespace": "pmh", "relationship": "self"}]
        return []

extract_jpcoar_ids_udf = F.udf(extract_jpcoar_ids, ArrayType(id_struct_type))

# COMMAND ----------

# JPCOAR 2.0 XML schema for Spark XML reader
#
# Namespace prefix mapping (confirmed from actual S3 data):
#   ns0: = OAI-PMH (stable across all files)
#   ns1: = JPCOAR 2.0 (stable)
#   dc:  = Dublin Core (stable)
#   ns3: = DC Terms (stable)
#   rdf: = RDF (stable)
#
# DataCite (date, description) and OpenAIRE (version) have VARIABLE prefixes
# (ns5/ns6/ns8/ns9 depending on file). These are extracted via regex from
# the metadata string instead.

jpcoar_schema = StructType([
    StructField("ns0:header", StructType([
        StructField("_status", StringType(), True),
        StructField("ns0:identifier", StringType(), True),
        StructField("ns0:datestamp", StringType(), True),
    ]), True),
    StructField("ns0:metadata", StructType([
        StructField("ns1:jpcoar", StructType([
            # Titles — multiple with xml:lang attribute
            StructField("dc:title", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_xml:lang", StringType(), True),
            ])), True),
            # Creators — array of complex structs
            StructField("ns1:creator", ArrayType(StructType([
                StructField("ns1:creatorName", ArrayType(StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_xml:lang", StringType(), True),
                ])), True),
                StructField("ns1:nameIdentifier", ArrayType(StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_nameIdentifierScheme", StringType(), True),
                ])), True),
                StructField("ns1:affiliation", ArrayType(StructType([
                    StructField("ns1:affiliationName", ArrayType(StructType([
                        StructField("_VALUE", StringType(), True),
                        StructField("_xml:lang", StringType(), True),
                    ])), True),
                ])), True),
            ])), True),
            # Typed identifiers (DOI, HDL, URI)
            StructField("ns1:identifier", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_identifierType", StringType(), True),
            ])), True),
            # Source identifiers (ISSN)
            StructField("ns1:sourceIdentifier", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_identifierType", StringType(), True),
            ])), True),
            # Type with COAR resource type URI
            StructField("dc:type", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
            # Simple fields
            StructField("dc:language", StringType(), True),
            StructField("ns1:volume", StringType(), True),
            StructField("ns1:issue", StringType(), True),
            StructField("ns1:pageStart", StringType(), True),
            StructField("ns1:pageEnd", StringType(), True),
            # Source title — multiple with xml:lang
            StructField("ns1:sourceTitle", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_xml:lang", StringType(), True),
            ])), True),
            # Publisher — multiple with lang attribute
            StructField("dc:publisher", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_lang", StringType(), True),
            ])), True),
            # Rights/license
            StructField("dc:rights", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ])), True),
            # Access rights (DC Terms)
            StructField("ns3:accessRights", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
            # Funding references
            StructField("ns1:fundingReference", ArrayType(StructType([
                StructField("ns1:funderIdentifier", StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_funderIdentifierType", StringType(), True),
                ]), True),
                StructField("ns1:funderName", ArrayType(StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_xml:lang", StringType(), True),
                ])), True),
                StructField("ns1:awardNumber", StructType([
                    StructField("_VALUE", StringType(), True),
                ]), True),
            ])), True),
            # File URLs
            StructField("ns1:file", ArrayType(StructType([
                StructField("ns1:URI", StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_objectType", StringType(), True),
                ]), True),
                StructField("ns1:mimeType", StringType(), True),
            ])), True),
            # --- Variable-namespace fields ---
            # DataCite and OpenAIRE use different ns prefixes across files because
            # Python ElementTree assigns ns3/ns5/ns6/ns8/ns9 based on encounter order.
            # Include all observed variants; coalesce later.
            # DataCite date (observed as ns3, ns5, ns6)
            StructField("ns3:date", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_dateType", StringType(), True),
            ])), True),
            StructField("ns5:date", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_dateType", StringType(), True),
            ])), True),
            StructField("ns6:date", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_dateType", StringType(), True),
            ])), True),
            # DataCite description (observed as ns3, ns5, ns6)
            StructField("ns3:description", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_descriptionType", StringType(), True),
                StructField("_xml:lang", StringType(), True),
            ])), True),
            StructField("ns5:description", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_descriptionType", StringType(), True),
                StructField("_xml:lang", StringType(), True),
            ])), True),
            StructField("ns6:description", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_descriptionType", StringType(), True),
                StructField("_xml:lang", StringType(), True),
            ])), True),
            # OpenAIRE version (observed as ns3, ns6, ns8, ns9)
            StructField("ns3:version", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
            StructField("ns6:version", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
            StructField("ns8:version", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
            StructField("ns9:version", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_rdf:resource", StringType(), True),
            ]), True),
        ]), True),
    ]), True),
])

# COMMAND ----------

@dlt.table(
    name="irdb_items",
    table_properties={"quality": "bronze"},
    comment="Raw IRDB JPCOAR 2.0 records from S3"
)
def irdb_items():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", "ns0:record")
        .option("compression", "gzip")
        .option("ignoreMissingFiles", "true")
        .schema(jpcoar_schema)
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/irdb/schema")
        .load("s3a://openalex-ingest/irdb/")
        .withColumn("ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# Helper: coalesce variable-namespace date arrays and filter by dateType
def _coalesce_dates(md):
    """Coalesce ns3/ns5/ns6 date arrays, prefer Issued, fallback to Created."""
    all_dates = F.concat(
        F.coalesce(F.col(f"{md}.`ns3:date`"), F.array()),
        F.coalesce(F.col(f"{md}.`ns5:date`"), F.array()),
        F.coalesce(F.col(f"{md}.`ns6:date`"), F.array()),
    )
    return F.coalesce(
        F.filter(all_dates, lambda d: d["_dateType"] == "Issued")[0]["_VALUE"],
        F.filter(all_dates, lambda d: d["_dateType"] == "Created")[0]["_VALUE"],
    )

# Helper: coalesce variable-namespace description arrays
def _coalesce_descriptions(md):
    """Coalesce ns3/ns5/ns6 description arrays.
    Prefer Abstract type, fallback to long Other descriptions (100+ chars)."""
    all_descs = F.concat(
        F.coalesce(F.col(f"{md}.`ns3:description`"), F.array()),
        F.coalesce(F.col(f"{md}.`ns5:description`"), F.array()),
        F.coalesce(F.col(f"{md}.`ns6:description`"), F.array()),
    )
    abstracts = F.filter(
        all_descs,
        lambda d: F.lower(d["_descriptionType"]) == "abstract",
    )
    # Fallback: "Other" descriptions that are long enough to be abstracts
    long_others = F.filter(
        all_descs,
        lambda d: (d["_descriptionType"] == "Other") & (F.length(d["_VALUE"]) >= 100),
    )
    return F.coalesce(
        F.filter(abstracts, lambda d: d["_xml:lang"] == "en")[0]["_VALUE"],
        abstracts[0]["_VALUE"],
        F.filter(long_others, lambda d: d["_xml:lang"] == "en")[0]["_VALUE"],
        long_others[0]["_VALUE"],
    )

# Helper: coalesce variable-namespace version fields
def _coalesce_version(md):
    """Coalesce ns3/ns6/ns8/ns9 version rdf:resource attributes."""
    return F.coalesce(
        F.col(f"{md}.`ns3:version`.`_rdf:resource`"),
        F.col(f"{md}.`ns6:version`.`_rdf:resource`"),
        F.col(f"{md}.`ns8:version`.`_rdf:resource`"),
        F.col(f"{md}.`ns9:version`.`_rdf:resource`"),
    )

# COMMAND ----------

@dlt.table(
    name="irdb_parsed",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    comment="Parsed IRDB records in walden_works schema"
)
@dlt.expect("has_native_id", "native_id IS NOT NULL")
@dlt.expect_or_drop("has_title", "title IS NOT NULL AND length(trim(title)) >= 5")
def irdb_parsed():
    md = "`ns0:metadata`.`ns1:jpcoar`"

    return (
        dlt.read_stream("irdb_items")
        # Filter deleted records and records with no metadata
        .filter(
            F.col("`ns0:header`.`_status`").isNull()
            | (F.col("`ns0:header`.`_status`") != "deleted")
        )
        .filter(F.col("`ns0:metadata`").isNotNull())
        # === native_id ===
        .withColumn("native_id", F.col("`ns0:header`.`ns0:identifier`"))
        .withColumn("native_id_namespace", F.lit("pmh"))
        # === updated_date (from OAI header datestamp) ===
        .withColumn(
            "updated_date",
            F.to_date(F.col("`ns0:header`.`ns0:datestamp`").substr(1, 10), "yyyy-MM-dd"),
        )
        # Dedup on native_id + updated_date
        .dropDuplicates(["native_id", "updated_date"])
        # === title (prefer Japanese, then English, then first) ===
        .withColumn(
            "title",
            F.substring(
                F.coalesce(
                    F.filter(F.col(f"{md}.`dc:title`"), lambda x: x["_xml:lang"] == "ja")[0][
                        "_VALUE"
                    ],
                    F.filter(F.col(f"{md}.`dc:title`"), lambda x: x["_xml:lang"] == "en")[0][
                        "_VALUE"
                    ],
                    F.col(f"{md}.`dc:title`")[0]["_VALUE"],
                ),
                0,
                MAX_TITLE_LENGTH,
            ),
        )
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        # === authors ===
        .withColumn(
            "authors",
            F.transform(
                F.coalesce(F.col(f"{md}.`ns1:creator`"), F.array()),
                lambda creator: F.struct(
                    F.lit(None).cast("string").alias("given"),
                    F.lit(None).cast("string").alias("family"),
                    F.substring(
                        F.coalesce(
                            F.filter(
                                F.coalesce(creator["ns1:creatorName"], F.array()),
                                lambda n: n["_xml:lang"] == "en",
                            )[0]["_VALUE"],
                            F.coalesce(creator["ns1:creatorName"], F.array())[0]["_VALUE"],
                        ),
                        0,
                        MAX_AUTHOR_NAME_LENGTH,
                    ).alias("name"),
                    # ORCID from nameIdentifier where scheme = "ORCID"
                    F.filter(
                        F.coalesce(creator["ns1:nameIdentifier"], F.array()),
                        lambda ni: ni["_nameIdentifierScheme"] == "ORCID",
                    )[0]["_VALUE"].alias("orcid"),
                    # Affiliations
                    F.transform(
                        F.coalesce(creator["ns1:affiliation"], F.array()),
                        lambda aff: F.struct(
                            F.coalesce(
                                F.filter(
                                    F.coalesce(aff["ns1:affiliationName"], F.array()),
                                    lambda a: a["_xml:lang"] == "en",
                                )[0]["_VALUE"],
                                F.coalesce(aff["ns1:affiliationName"], F.array())[0]["_VALUE"],
                            ).alias("name"),
                            F.lit(None).cast("string").alias("department"),
                            F.lit(None).cast("string").alias("ror_id"),
                        ),
                    ).alias("affiliations"),
                    F.lit(None).cast("boolean").alias("is_corresponding"),
                ),
            ),
        )
        # === ids ===
        .withColumn(
            "ids",
            extract_jpcoar_ids_udf(
                F.col(f"{md}.`ns1:identifier`"),
                F.col(f"{md}.`ns1:sourceIdentifier`"),
                F.col("native_id"),
            ),
        )
        # === type (COAR URI lookup, with text fallback) ===
        .withColumn("raw_native_type", F.col(f"{md}.`dc:type`.`_VALUE`"))
        .withColumn(
            "_coar_code",
            F.element_at(
                F.split(F.col(f"{md}.`dc:type`.`_rdf:resource`"), "/"), -1
            ),
        )
        .withColumn(
            "type",
            F.coalesce(
                IRDB_COAR_TYPE_MAP[F.col("_coar_code")],
                IRDB_TEXT_TYPE_MAP[F.lower(F.trim(F.col("raw_native_type")))],
                F.lit("other"),
            ),
        )
        .drop("_coar_code")
        # === version (from variable-namespace COAR version URI) ===
        .withColumn("_version_uri", _coalesce_version(md))
        .withColumn(
            "_version_code",
            F.element_at(F.split(F.col("_version_uri"), "/"), -1),
        )
        .withColumn(
            "version",
            F.coalesce(COAR_VERSION_MAP[F.col("_version_code")], F.lit("submittedVersion")),
        )
        .drop("_version_uri", "_version_code")
        # === license ===
        .withColumn(
            "raw_license",
            F.coalesce(
                # Prefer CC URL from rdf:resource attribute
                F.filter(
                    F.coalesce(F.col(f"{md}.`dc:rights`"), F.array()),
                    lambda r: r["_rdf:resource"].contains("creativecommons.org"),
                )[0]["_rdf:resource"],
                # Fallback to first rights rdf:resource
                F.col(f"{md}.`dc:rights`")[0]["_rdf:resource"],
                # Fallback to rights text value
                F.col(f"{md}.`dc:rights`")[0]["_VALUE"],
            ),
        )
        .withColumn("license", normalize_license_udf(F.col("raw_license")))
        # === language ===
        .withColumn(
            "language",
            normalize_language_code_udf(F.col(f"{md}.`dc:language`")),
        )
        # === published_date (from variable-namespace DataCite date) ===
        # Route by format to avoid ANSI mode errors from mismatched patterns
        .withColumn("_date_str", F.trim(_coalesce_dates(md)))
        .withColumn(
            "published_date",
            F.when(
                F.col("_date_str").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                F.to_date(F.col("_date_str"), "yyyy-MM-dd"),
            ).when(
                F.col("_date_str").rlike(r"^\d{4}-\d{2}$"),
                F.to_date(F.concat(F.col("_date_str"), F.lit("-01")), "yyyy-MM-dd"),
            ).when(
                F.col("_date_str").rlike(r"^\d{4}$"),
                F.to_date(F.concat(F.col("_date_str"), F.lit("-01-01")), "yyyy-MM-dd"),
            ),
        )
        .drop("_date_str")
        .withColumn("created_date", F.col("published_date"))
        # === bibliographic fields (direct from stable ns1: prefix) ===
        .withColumn("volume", F.col(f"{md}.`ns1:volume`"))
        .withColumn("issue", F.col(f"{md}.`ns1:issue`"))
        .withColumn("first_page", F.col(f"{md}.`ns1:pageStart`"))
        .withColumn("last_page", F.col(f"{md}.`ns1:pageEnd`"))
        .withColumn("is_retracted", F.lit(None).cast("boolean"))
        # === abstract (from variable-namespace DataCite description) ===
        .withColumn(
            "abstract",
            F.substring(_coalesce_descriptions(md), 0, MAX_ABSTRACT_LENGTH),
        )
        # === source_name (prefer English) ===
        .withColumn(
            "source_name",
            F.coalesce(
                F.filter(
                    F.coalesce(F.col(f"{md}.`ns1:sourceTitle`"), F.array()),
                    lambda x: x["_xml:lang"] == "en",
                )[0]["_VALUE"],
                F.col(f"{md}.`ns1:sourceTitle`")[0]["_VALUE"],
            ),
        )
        # === publisher (prefer English) ===
        .withColumn(
            "publisher",
            F.coalesce(
                F.filter(
                    F.coalesce(F.col(f"{md}.`dc:publisher`"), F.array()),
                    lambda x: x["_lang"] == "en",
                )[0]["_VALUE"],
                F.col(f"{md}.`dc:publisher`")[0]["_VALUE"],
            ),
        )
        # === funders ===
        .withColumn(
            "funders",
            F.transform(
                F.coalesce(F.col(f"{md}.`ns1:fundingReference`"), F.array()),
                lambda fr: F.struct(
                    # Funder DOI from funderIdentifier (Crossref Funder type)
                    F.when(
                        F.coalesce(fr["ns1:funderIdentifier"]["_funderIdentifierType"], F.lit(""))
                        == "Crossref Funder",
                        F.regexp_extract(
                            F.coalesce(fr["ns1:funderIdentifier"]["_VALUE"], F.lit("")),
                            r"10\.\d{4,9}/\S+",
                            0,
                        ),
                    )
                    .otherwise(F.lit(None).cast("string"))
                    .alias("doi"),
                    F.lit(None).cast("string").alias("ror"),
                    # Funder name (prefer English)
                    F.coalesce(
                        F.filter(
                            F.coalesce(fr["ns1:funderName"], F.array()),
                            lambda fn: fn["_xml:lang"] == "en",
                        )[0]["_VALUE"],
                        F.coalesce(fr["ns1:funderName"], F.array())[0]["_VALUE"],
                    ).alias("name"),
                    F.when(
                        fr["ns1:awardNumber"]["_VALUE"].isNotNull(),
                        F.array(fr["ns1:awardNumber"]["_VALUE"]),
                    )
                    .otherwise(F.array(F.lit(None).cast("string")))
                    .alias("awards"),
                ),
            ),
        )
        # === references (not available in JPCOAR — empty array) ===
        .withColumn(
            "references",
            F.lit(None).cast(
                ArrayType(
                    StructType([
                        StructField("doi", StringType(), True),
                        StructField("pmid", StringType(), True),
                        StructField("arxiv", StringType(), True),
                        StructField("title", StringType(), True),
                        StructField("authors", StringType(), True),
                        StructField("year", StringType(), True),
                        StructField("raw", StringType(), True),
                    ])
                )
            ),
        )
        # === urls (landing page from identifier + PDF from file) ===
        # Landing page URL from ns1:identifier[@identifierType="URI"]
        .withColumn(
            "_landing_page_url",
            F.filter(
                F.coalesce(F.col(f"{md}.`ns1:identifier`"), F.array()),
                lambda i: i["_identifierType"] == "URI",
            )[0]["_VALUE"],
        )
        .withColumn(
            "_landing_page_arr",
            F.when(
                F.col("_landing_page_url").isNotNull(),
                F.array(
                    F.struct(
                        F.col("_landing_page_url").alias("url"),
                        F.lit("html").alias("content_type"),
                    )
                ),
            ).otherwise(F.array()),
        )
        # PDF/file URLs from ns1:file (prefer fulltext)
        .withColumn(
            "_all_files",
            F.coalesce(F.col(f"{md}.`ns1:file`"), F.array()),
        )
        .withColumn(
            "_fulltext_files",
            F.filter(F.col("_all_files"), lambda f: f["ns1:URI"]["_objectType"] == "fulltext"),
        )
        .withColumn(
            "_file_urls",
            F.transform(
                F.when(F.size(F.col("_fulltext_files")) > 0, F.col("_fulltext_files")).otherwise(
                    F.col("_all_files")
                ),
                lambda f: F.struct(
                    f["ns1:URI"]["_VALUE"].alias("url"),
                    F.when(
                        F.coalesce(f["ns1:mimeType"], F.lit("")).contains("pdf")
                        | F.lower(F.coalesce(f["ns1:URI"]["_VALUE"], F.lit(""))).contains("pdf"),
                        F.lit("pdf"),
                    )
                    .otherwise(F.lit("html"))
                    .alias("content_type"),
                ),
            ),
        )
        # Combine: file URLs first (PDFs), then landing page
        .withColumn("urls", F.concat(F.col("_file_urls"), F.col("_landing_page_arr")))
        .drop("_landing_page_url", "_landing_page_arr", "_all_files", "_fulltext_files", "_file_urls")
        .withColumn("mesh", F.lit(None).cast("string"))
        # === is_oa (trust PDFs from IRDB repos as open access) ===
        .withColumn(
            "is_oa",
            F.when(
                F.lower(F.coalesce(F.col("license"), F.lit(""))).startswith("cc")
                | F.lower(F.coalesce(F.col("license"), F.lit(""))).contains("public-domain")
                | F.coalesce(
                    F.col(f"{md}.`ns3:accessRights`.`_rdf:resource`"), F.lit("")
                ).contains("open_access")
                | F.exists(F.col("urls"), lambda u: u["content_type"] == "pdf"),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        # === endpoint_id (single IRDB endpoint) ===
        .withColumn("endpoint_id", F.lit("irdb_nii_ac_jp"))
        # Filter: must have at least one URL
        .filter(F.size(F.col("urls")) > 0)
        # Filter: exclude non-scholarly types (newspaper, still image, image)
        .filter(~F.lower(F.col("raw_native_type")).isin(
            "newspaper", "still image", "image", "photograph"
        ))
        # Filter: clamp out-of-range dates to null (year 0000, 9999, etc.)
        .withColumn(
            "published_date",
            F.when(
                (F.col("published_date") >= F.lit("1800-01-01").cast("date"))
                & (F.col("published_date") <= F.date_add(F.current_date(), 365)),
                F.col("published_date"),
            ),
        )
        .withColumn("created_date", F.col("published_date"))
        # Final column selection (walden_works schema order)
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
            "is_oa",
            "endpoint_id",
            "ingested_at",
        )
        .drop("raw_license")
    )
