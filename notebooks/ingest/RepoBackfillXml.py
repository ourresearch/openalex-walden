# Databricks notebook source
import re
import unicodedata
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Repo Backfill using from_xml
# MAGIC
# MAGIC This notebook processes repository backfill data using Spark's `from_xml` function
# MAGIC instead of regex for more efficient and structured XML parsing.

# COMMAND ----------

# Load data from table
df = spark.table("openalex.repo.repo_items_backfill")

# COMMAND ----------

# Clean XML by removing escaped quotes and triple quotes
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

# MAGIC %md
# MAGIC ## Define XML Schema for from_xml parsing
# MAGIC
# MAGIC Example XML structure:
# MAGIC ```xml
# MAGIC <record>
# MAGIC   <header>
# MAGIC     <identifier>oai:aleph.bib-bvb.de:BVB01-005639317</identifier>
# MAGIC     <datestamp>2021-12-20T16:32:39Z</datestamp>
# MAGIC     <setSpec>OpenData</setSpec>
# MAGIC   </header>
# MAGIC   <metadata>
# MAGIC     <dc>
# MAGIC       <dc:title>...</dc:title>
# MAGIC       <dc:creator>Pander, Klaus</dc:creator>
# MAGIC       <dc:type>text</dc:type>
# MAGIC       <dc:type>Kunstführer</dc:type>  <!-- Can be multiple! -->
# MAGIC       <dc:identifier>URN:ISBN:3770112261</dc:identifier>
# MAGIC       <dc:identifier>http://...</dc:identifier>
# MAGIC       ...
# MAGIC     </dc>
# MAGIC   </metadata>
# MAGIC </record>
# MAGIC ```

# COMMAND ----------

# Define the OAI-PMH + Dublin Core XML schema
# Based on real XML structure where many DC fields can be multi-valued
oai_schema = StructType([
    StructField("header", StructType([
        StructField("identifier", StringType(), True),
        StructField("datestamp", StringType(), True),
        StructField("setSpec", ArrayType(StringType()), True)
    ]), True),
    StructField("metadata", StructType([
        StructField("dc", StructType([
            StructField("title", StringType(), True),
            StructField("creator", ArrayType(StringType()), True),
            StructField("subject", ArrayType(StringType()), True),
            StructField("description", ArrayType(StringType()), True),
            StructField("publisher", StringType(), True),
            StructField("contributor", ArrayType(StringType()), True),
            StructField("date", ArrayType(StringType()), True),
            StructField("type", ArrayType(StringType()), True),  # FIXED: Can be multiple values
            StructField("format", ArrayType(StringType()), True),
            StructField("identifier", ArrayType(StringType()), True),
            StructField("source", StringType(), True),
            StructField("language", StringType(), True),
            StructField("relation", ArrayType(StringType()), True),
            StructField("coverage", ArrayType(StringType()), True),  # FIXED: Can be multiple values
            StructField("rights", ArrayType(StringType()), True)
        ]), True)
    ]), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions and UDFs

# COMMAND ----------

# Utility functions
def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
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
    text = remove_accents(text)
    text = clean_html(text)
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)
    text = remove_everything_but_alphas(text)
    return text.strip()

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
    "article": "article", "journal article": "article", "journal articles": "article",
    "publishedversion": "article", "contributiontoperiodical": "article",
    "conference papers": "article", "conferencepaper": "article", "conferenceobject": "article",
    "conferenceposter": "article", "conferenceproceedings": "article",
    "conferencecontribution": "article", "conferenceitem": "article",
    "text": "article", "manuscript": "article", "peerreviewed": "article",
    "peer reviewed": "article", "submittedversion": "article", "acceptedversion": "article",
    "review": "article", "zeitschrift": "article", "conference paper": "article",
    "konferenzschrift": "article", "article / letter to editor": "article",
    "inproceedings": "article", "text (article)": "article",
    "info:ulb-repo/semantics/openurl/article": "article", "vor": "article",
    "journal contribution": "article", "artigo de jornal": "article",
    "doc-type:article": "article", "letter": "article",
    "http://purl.org/coar/resource_type/c_6501": "article",
    "http://purl.org/coar/resource_type/c_5794": "article",
    
    # Books and book content
    "book": "book", "books": "book", "book sections": "book-chapter", "bookpart": "book-chapter",
    "monografische reihe": "book", "volume": "book", "monograph": "book",
    "book article": "book-chapter", "book part": "book-chapter",
    "chapter, part of book": "book-chapter", "part of book or chapter of book": "book-chapter",
    "libros": "book", "doc-type:bookpart": "book-chapter",
    
    # Theses and dissertations
    "thesis": "dissertation", "theses": "dissertation", "masterthesis": "dissertation",
    "bachelorthesis": "dissertation", "doctoralthesis": "dissertation",
    "doctor of philosophy": "dissertation", "phd": "dissertation", "dissertation": "dissertation",
    "undergraduate senior honors thesis": "dissertation", "hochschulschrift": "dissertation",
    "phdthesis": "dissertation", "master thesis": "dissertation", "diplomová práce": "dissertation",
    "doctoral thesis": "dissertation", "doc-type:doctoralthesis": "dissertation",
    "masters paper": "dissertation", "masters thesis": "dissertation", "dissertação": "dissertation",
    "doctoral_dissertation": "dissertation", "tesi doctoral": "dissertation",
    "thesis or dissertation": "dissertation", "thèse": "dissertation",
    "electronic dissertation": "dissertation",
    "dissertation-reproduction (electronic)": "dissertation",
    "thesis-reproduction (electronic)": "dissertation",
    "http://purl.org/coar/resource_type/c_db06": "dissertation",
    
    # Reports
    "report": "report", "reports": "report", "technical report": "report",
    "workingpaper": "report", "working paper": "report", "reportpart": "report",
    "technical documentation": "report",
    
    # Reviews
    "review": "review", "peer-review": "peer-review",
    
    # Preprints
    "preprint": "preprint", "preprints, working papers, ...": "preprint",
    
    # Datasets
    "dataset": "dataset", "dataset/mass spectrometry": "dataset",
    
    # Awards
    "award/grant": "award",
    
    # Software
    "software": "software",
    
    # Images (mapped to other)
    "image": "other", "chemical structures": "other",
    
    # Other types
    "lecture": "other", "patent": "other", "creative project": "other",
    "other": "other", "null": "other",
}

def get_openalex_type_from_repo(input_type):
    """Convert various publication type formats to OpenAlex types."""
    if not input_type or not isinstance(input_type, str):
        return "other"
    input_type = input_type.strip()
    input_type_lower = input_type.lower()
    
    # Handle URI formats first
    if "info:eu-repo/semantics/" in input_type_lower:
        input_type_lower = input_type_lower.split("info:eu-repo/semantics/")[-1]
    elif "purl.org/coar/resource_type/" in input_type_lower:
        coar_to_type = {
            "c_93fc": "article", "c_6501": "article", "c_bdcc": "article",
            "c_db06": "dissertation", "r60j-j5bd": "article",
            "c_5794": "article", "c_2f33": "article",
        }
        coar_id = input_type_lower.split("/")[-1]
        return coar_to_type.get(coar_id, "other")
    elif "purl.org/coar/version/" in input_type_lower:
        return "article"
    
    return REPO_TYPE_MAPPING.get(input_type_lower, "other")

def normalize_language_code(lang_code):
    """Normalize language codes to ISO 639-1 two-letter lowercase format."""
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
    
    # ISO 639-2 to ISO 639-1 mapping (abbreviated)
    three_to_two = {
        "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "rus": "ru",
        "zho": "zh", "jpn": "ja", "ara": "ar", "por": "pt", "ita": "it",
    }
    
    if len(lang_code) == 2:
        return lang_code
    if len(lang_code) == 3:
        return three_to_two.get(lang_code)
    return None

def normalize_license(text):
    """Normalize license text to standard formats."""
    if not text:
        return None
    normalized_text = text.replace(" ", "").replace("-", "").lower()
    
    license_lookups = [
        ("elsevier.com/openaccess/userlicense", None),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "publisher-specific-oa"),
        ("ccbyncnd", "cc-by-nc-nd"), ("ccbyncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"), ("ccbysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"), ("ccby", "cc-by"),
        ("creativecommons.org/licenses/by/", "cc-by"),
        ("publicdomain", "public-domain"), ("openaccess", "other-oa"),
    ]
    
    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license
    return None

def has_oa_domain(native_id):
    """Check if native_id contains known OA domain."""
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

# Pandas UDFs
@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

@pandas_udf(StringType())
def get_openalex_type_from_repo_udf(repo_type_series: pd.Series) -> pd.Series:
    return repo_type_series.apply(get_openalex_type_from_repo)

@pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MIN_ABSTRACT_LENGTH = 100
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse XML using from_xml

# COMMAND ----------

# Parse XML using from_xml function
parsed_xml_df = clean_df.withColumn("parsed", 
    from_xml(col("cleaned_xml"), oai_schema, map("mode", "PERMISSIVE"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Transform Fields

# COMMAND ----------

# Extract fields from parsed XML structure
parsed_df = parsed_xml_df \
    .withColumn("native_id", col("parsed.header.identifier")) \
    .withColumn("native_id_namespace", lit("pmh")) \
    .withColumn("title", substring(col("parsed.metadata.dc.title"), 0, MAX_TITLE_LENGTH)) \
    .withColumn("normalized_title", normalize_title_udf(col("title"))) \
    .withColumn("authors", 
        transform(
            col("parsed.metadata.dc.creator"),
            lambda x: struct(
                lit(None).cast("string").alias("given"),
                lit(None).cast("string").alias("family"),
                substring(x, 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
                lit(None).cast("string").alias("orcid"),
                array(struct(
                    lit(None).cast("string").alias("name"),
                    lit(None).cast("string").alias("department"),
                    lit(None).cast("string").alias("ror_id")
                )).alias("affiliations")
            )
        )
    ) \
    .withColumn("raw_native_type", element_at(col("parsed.metadata.dc.type"), 1)) \
    .withColumn("type", get_openalex_type_from_repo_udf(col("raw_native_type"))) \
    .filter(~lower(col("raw_native_type")).isin(TYPES_TO_DELETE)) \
    .withColumn("identifiers", col("parsed.metadata.dc.identifier")) \
    .withColumn("version", lit(None).cast("string")) \
    .withColumn("language", normalize_language_code_udf(col("parsed.metadata.dc.language"))) \
    .withColumn("published_date_raw", element_at(col("parsed.metadata.dc.date"), 1)) \
    .withColumn("published_date",
        coalesce(
            to_date(to_timestamp(col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'")),
            to_date(to_timestamp(col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss")),
            to_date(col("published_date_raw"), "yyyy-MM-dd"),
            to_date(col("published_date_raw"), "yyyy-MM"),
            to_date(regexp_replace(col("published_date_raw"), r'\.', '-'), "yyyy-MM-dd"),
            to_date(
                when(length(trim(col("published_date_raw"))) == 4,
                    concat(col("published_date_raw"), lit("-01-01"))),
                "yyyy-MM-dd"
            )
        )) \
    .withColumn("created_date", col("published_date")) \
    .withColumn("updated_date", to_date(col("parsed.header.datestamp"))) \
    .withColumn("abstract_raw", element_at(col("parsed.metadata.dc.description"), 1)) \
    .withColumn("abstract",
        when(length(col("abstract_raw")) >= MIN_ABSTRACT_LENGTH, 
            substring(col("abstract_raw"), 0, MAX_ABSTRACT_LENGTH))
        .otherwise(lit(None))) \
    .withColumn("source_name", col("parsed.metadata.dc.source")) \
    .withColumn("publisher", col("parsed.metadata.dc.publisher")) \
    .withColumn("urls",
        transform(
            filter(col("identifiers"), lambda x: x.rlike("^http")),
            lambda x: struct(
                x.alias("url"),
                when(lower(x).contains("pdf"), lit("pdf")).otherwise(lit("html")).alias("content_type")
            )
        )) \
    .withColumn("raw_license",
        when(
            size(filter(col("parsed.metadata.dc.rights"), lambda x: x.contains("creativecommons.org"))) > 0,
            element_at(filter(col("parsed.metadata.dc.rights"), lambda x: x.contains("creativecommons.org")), 1)
        ).otherwise(element_at(col("parsed.metadata.dc.rights"), 1))) \
    .withColumn("license", normalize_license_udf(col("raw_license"))) \
    .withColumn("issue", lit(None).cast("string")) \
    .withColumn("volume", lit(None).cast("string")) \
    .withColumn("first_page", lit(None).cast("string")) \
    .withColumn("last_page", lit(None).cast("string")) \
    .withColumn("is_retracted", lit(None).cast("boolean")) \
    .withColumn("funders", array(struct(
        lit(None).cast("string").alias("doi"),
        lit(None).cast("string").alias("ror"),
        lit(None).cast("string").alias("name"),
        array(lit(None).cast("string")).alias("awards")
    ))) \
    .withColumn("references", array(struct(
        lit(None).cast("string").alias("doi"),
        lit(None).cast("string").alias("pmid"),
        lit(None).cast("string").alias("arxiv"),
        lit(None).cast("string").alias("title"),
        lit(None).cast("string").alias("authors"),
        lit(None).cast("string").alias("year"),
        lit(None).cast("string").alias("raw")
    ))) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn("is_oa",
        when(
            lower(col("license")).startswith("cc") |
            lower(col("license")).contains("other-oa") |
            lower(col("license")).contains("public-domain") |
            has_oa_domain_udf(col("native_id")),
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn("ids", 
        # Note: This still needs extract_ids UDF - simplified version for now
        array(struct(
            col("native_id").alias("id"),
            lit("pmh").alias("namespace"),
            lit("self").alias("relationship")
        ))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Final Columns

# COMMAND ----------

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
    "is_oa"
)

# COMMAND ----------

# Deduplicate based on native_id and most recent updated date
parsed_df = parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Delta Table

# COMMAND ----------

parsed_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("openalex.repo.repo_works_backfill_xml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison
# MAGIC
# MAGIC **Advantages of from_xml:**
# MAGIC - More efficient XML parsing (C++ based)
# MAGIC - Better handling of malformed XML
# MAGIC - Cleaner, more maintainable code
# MAGIC - Type safety with defined schema
# MAGIC - Easier to debug with structured data
# MAGIC
# MAGIC **Trade-offs:**
# MAGIC - Requires schema definition upfront
# MAGIC - May need fallback to regex for edge cases
# MAGIC - Schema must match XML structure exactly

