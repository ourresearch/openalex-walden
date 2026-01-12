# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.3-py3-none-any.whl

# COMMAND ----------

import dlt
import re
import unicodedata
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

from openalex.utils.environment import *
from openalex.dlt.normalize import normalize_title, normalize_license, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000
unallowed_types = ["component", "grant"]

# Affiliation-as-Author Fix - Publishers excluded from filtering
# These publishers have < 0.1% affected rate and should not have their records modified
AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS = [
    "ACM",
    "Acoustical Society of America (ASA)",
    "AIP Publishing",
    "American Chemical Society (ACS)",
    "American Geophysical Union (AGU)",
    "American Institute of Aeronautics & Astronautics",
    "American Society of Civil Engineers (ASCE)",
    "American Society of Mechanical Engineers",
    "Association for Computing Machinery (ACM)",
    "ASTM International",
    "Atlantis Press",
    "Bentham Science Publishers Ltd.",
    "BRILL",
    "CAIRN",
    "Cambridge University Press",
    "Center for Open Science",
    "CRC Press",
    "De Gruyter",
    "Duke University Press",
    "Edward Elgar Publishing",
    "Egyptian Knowledge Bank",
    "Egypts Presidential Specialized Council",
    "Elsevier",
    "Emerald",
    "ENCODE Data Coordination Center",
    "Georg Thieme Verlag KG",
    "H1 Connect",
    "Hans Publishers",
    "IEEE",
    "IGI Global",
    "Inderscience Publishers",
    "Informa UK Limited",
    "Institute of Electrical and Electronics Engineers (IEEE)",
    "Institution of Engineering and Technology (IET)",
    "International Union of Crystallography (IUCr)",
    "IUCN",
    "Japan Society of Mechanical Engineers",
    "Nomos Verlagsgesellschaft mbH & Co. KG",
    "OpenEdition",
    "Oxford University Press",
    "PERSEE Program",
    "Project MUSE",
    "Research Square Platform LLC",
    "Routledge",
    "Royal Society of Chemistry (RSC)",
    "SAE International",
    "Sciencedomain International",
    "Scientific Research Publishing, Inc.",
    "SPIE",
    "Springer Berlin Heidelberg",
    "Springer International Publishing",
    "Springer Nature Singapore",
    "Springer Nature Switzerland",
    "The Conversation",
    "The Electrochemical Society",
    "The Royal Society",
    "Trans Tech Publications, Ltd.",
    "transcript Verlag",
    "Universidade de Sao Paulo",
    "University of Chicago Press",
    "VS Verlag fur Sozialwissenschaften",
    "Walter de Gruyter GmbH",
    "World Scientific Pub Co Pte Lt",
]

# Institution keywords regex for detecting affiliation-as-author entries (case-insensitive)
INSTITUTION_KEYWORDS_PATTERN = (
    r"(?i)\b("
    # English institution keywords
    r"University|Institute|College|Hospital|Department|School|Center|Centre|"
    r"Laboratory|Faculty|Academy|"
    # Non-English institution keywords
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    # Corporate/organization patterns
    r"Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Consortium|Association|"
    r"Collaboration|Committee|Council|Organization|Organisation|"
    # Additional keywords
    r"Clinic|Medical|Research|Museum|Library|Foundation|Polytechnic"
    r")\b"
)

@F.pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@F.pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

# COMMAND ----------

def create_author_struct(author):
    """Transform raw Crossref author to normalized author struct."""
    return F.struct(
        F.substring(author["given"], 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
        F.substring(author["family"], 0, MAX_AUTHOR_NAME_LENGTH).alias("family"),
        F.substring(author["name"], 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
        F.regexp_extract(
            author["ORCID"], r"(\d{4}-\d{4}-\d{4}-\d{4})", 1
        ).alias("orcid"),
        F.transform(
            author["affiliation"],
            lambda aff: F.struct(
                F.substring(aff["name"], 0, MAX_AFFILIATION_STRING_LENGTH).alias("name"),
                F.substring(F.get(aff["department"], 0), 0, MAX_AFFILIATION_STRING_LENGTH).alias("department"),
                F.when(
                    F.get(aff["id"]["id-type"], 0) == "ROR",
                    F.get(aff["id"]["id"], 0)
                ).alias("ror_id")
            )
        ).alias("affiliations")
    )

def is_valid_author(author):
    """
    Returns True if the author is NOT an affiliation-as-author entry.

    Detects two patterns:
    1. Institution keywords in given OR family fields (e.g., given="Kazan", family="University")
    2. Institution keywords in name field when given/family are empty
       (e.g., name="University of Professional Studies, Accra, Ghana")
    """
    family = F.coalesce(author["family"], F.lit(""))
    given = F.coalesce(author["given"], F.lit(""))
    name = F.coalesce(author["name"], F.lit(""))

    # Pattern 1: Institution keywords in given or family
    has_institution_in_given_family = (
        family.rlike(INSTITUTION_KEYWORDS_PATTERN) |
        given.rlike(INSTITUTION_KEYWORDS_PATTERN)
    )

    # Pattern 2: Institution in name field when given/family are empty
    has_institution_in_name_only = (
        (F.trim(given) == "") &
        (F.trim(family) == "") &
        name.rlike(INSTITUTION_KEYWORDS_PATTERN)
    )

    return ~(has_institution_in_given_family | has_institution_in_name_only)

# COMMAND ----------

def get_openalex_type(crossref_type):
    """
    Convert Crossref types to OpenAlex types based on mappings.
    """
    crossref_to_openalex = {
        # article
        "journal-article": "article",
        "proceedings-article": "article",
        "posted-content": "article",
        
        # book
        "book-set": "book",
        "edited-book": "book",
        "monograph": "book",
        "reference-book": "book",
        
        # book-chapter
        "book-part": "book-chapter",
        
        # paratext
        "book-series": "paratext",
        "component": "paratext",
        "journal": "paratext", 
        "journal-issue": "paratext",
        "journal-volume": "paratext",
        "proceedings": "paratext",
        "proceedings-series": "paratext",
        "report-series": "paratext",

        # pass-through types
        "dataset": "dataset",
        "dissertation": "dissertation",
        "standard": "standard",
        "peer-review": "peer-review",
        "report": "report",
        "other": "other"
    }
    
    return crossref_to_openalex.get(crossref_type, crossref_type)

@F.pandas_udf(StringType())
def get_openalex_type_udf(series: pd.Series) -> pd.Series:
    return series.apply(get_openalex_type)

# COMMAND ----------

# Raw data in single column as items table
@dlt.table(
  name="crossref_items",
  comment = f"Reading in files from s3://openalex-ingest/crossref/ in {ENV.upper()}",
  table_properties={'quality': 'bronze'}
)
@dlt.expect("rescued_data_null", "_rescued_data IS NULL")
def crossref_items():
  # use fixed schema - pipeline breaks with errors to replace structs with strings otherwise
  current_schema = spark.table("openalex.crossref.crossref_items").schema

  return ( spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .schema(current_schema)  # Explicitly use fixed schema to avoid errors and drift (it is 6k lines long)
    .load("s3a://openalex-ingest/crossref/")
  )

# COMMAND ----------

# Exploded data to multiple columns
@dlt.table(
  name="crossref_exploded",
  cluster_by = ["native_id"],
  table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "quality": "silver"
  }
)
def crossref_snapshots_exploded():
  return (dlt.read_stream("crossref_items")
    .select(F.explode(F.col("items")).alias("record"))
    .select("record.*")
    .withColumn("indexed_date", F.col("indexed.date-time"))
    .withColumnRenamed("DOI", "native_id")
    .withColumn("updated_date", F.make_date(
        F.get(F.col("indexed.`date-parts`")[0], 0),
        F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 1), F.lit(1)),
        F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 2), F.lit(1))
    ))
    .filter(~F.col("type").isin(unallowed_types))
  )

# This table will be the target for your merge operation
dlt.create_streaming_table(
    name = "crossref_deduplicated",
    comment = "Deduplicated crossref data, keeping only the latest version.",
    cluster_by_auto = True,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "silver"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_deduplicated",
    source="crossref_exploded", # Use the name of the new table
    keys=["native_id"],
    sequence_by="updated_date"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### `crossref_parsed`

# COMMAND ----------

# Transformed Table
@dlt.view(
    name="crossref_parsed_source",
    comment="Source of the crossref transformations to parse the data"
)
def crossref_parsed():
    def extract_issn_id_by_type(id_type):
        return F.expr(f"filter(`issn-type`, x -> x.type = '{id_type}').value")

    def extract_isbn_id_by_type(id_type):
        return F.expr(f"filter(`isbn-type`, x -> x.type = '{id_type}').value")

    def extract_license_by_type(content_version):
        return F.expr(
            f"filter(license, x -> x.`content-version` != '{content_version}').URL"
        )

    def extract_url_by_type():
        return F.expr(
            f"filter(link, x -> !contains(x.`content-type`, 'plain') and !contains(x.`content-type`, 'xml'))"
        )

    return (
        # spark.readStream
        # .option("skipChangeCommits", "true") 
        # .table("openalex.crossref.crossref_deduplicated")
        # dlt.read("crossref_deduplicated")
        spark.readStream
            .option("readChangeFeed", "true")
            .table("LIVE.crossref_deduplicated")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))        
        .filter(~F.col("type").isin(unallowed_types))
        .withColumn("native_id_namespace", F.lit("doi"))
        .withColumn("title", F.substring(F.get(F.col("title"), 0), 0, MAX_TITLE_LENGTH))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn(
            "authors",
            F.when(
                F.col("publisher").isin(AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS),
                # Excluded publishers: keep all authors unchanged
                F.transform("author", create_author_struct)
            ).otherwise(
                # Other publishers: filter out affiliation-as-author entries, then transform
                F.transform(
                    F.filter(F.col("author"), is_valid_author),
                    create_author_struct
                )
            )
        )
        .withColumn(
            "ids",
            F.filter(
                F.array(
                    F.struct(
                        F.when(F.isnull(F.get(extract_issn_id_by_type("print"), 0)), None)
                        .otherwise(F.get(extract_issn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.get(extract_issn_id_by_type("electronic"), 0)), None)
                            .otherwise(F.get(extract_issn_id_by_type("electronic"), 0))
                            .alias("id"),
                        F.lit("eissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.get(extract_isbn_id_by_type("print"), 0)), None)
                        .otherwise(F.get(extract_isbn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_isbn_id_by_type("electronic"), 0)),
                            None
                        )
                        .otherwise(F.get(extract_isbn_id_by_type("electronic"), 0))
                        .alias("id"),
                        F.lit("eisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship")
                    ),
                    F.struct(
                        F.when(F.isnull(F.col("native_id")), None).otherwise(F.col("native_id")).alias("id"),
                        F.lit("doi").alias("namespace"),
                        F.lit("self").alias("relationship")
                    )
                ),
                lambda x: x.id != ""
            )  # Filter out empty ids
        )
        .withColumn("version", 
            F.when(F.col("type") == "posted-content", F.lit("acceptedVersion"))
            .otherwise(F.lit("publishedVersion"))
        )
        .withColumn("raw_type", F.col("type"))
        .withColumn("type", get_openalex_type_udf(F.col("type")))
        .withColumn("raw_license",  
            # Filter out content-version = "tdm", then prioritize the url containing "creativecommons.org", otherwise select the first url
            F.when(
                F.isnull(
                    F.get(
                        F.filter(
                            extract_license_by_type("tdm"),
                            lambda x: x.contains("creativecommons.org"),
                        ),
                        0,
                    )
                ),
                F.get(extract_license_by_type("tdm"), 0),
            ).otherwise(
                F.get(
                    F.filter(
                        extract_license_by_type("tdm"),
                        lambda x: x.contains("creativecommons.org"),
                    ),
                    0
                )
            )
        )
        .withColumn("license", normalize_license_udf(F.col("raw_license")))
        .withColumn("language", F.col("language"))
        .withColumn("created_date", F.to_date(F.col("created.`date-time`")))
        .withColumn("deposited", F.to_date(F.col("deposited.`date-time`")))
        .withColumn(
            "issued",
            F.make_date(
                F.get(F.col("issued.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("issued.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("issued.`date-parts`")[0], 2), F.lit(1))
            )
        )
        .withColumn(
            "published",
            F.make_date(
                F.get(F.col("published.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("published.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("published.`date-parts`")[0], 2), F.lit(1))
            )
        )
        .withColumn(
            "approved",
            F.make_date(
                F.get(F.col("approved.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("approved.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("approved.`date-parts`")[0], 2), F.lit(1))
            )
        )
        .withColumn(
            "published_date",
            F.least(
                F.col("issued"),
                F.col("created_date"),
                F.col("published"),
                F.col("approved"),
                F.col("deposited")
            )
        )
        .withColumn("issue", F.col("issue"))
        .withColumn("volume", F.col("volume"))
        .withColumn("first_page", F.get(F.split("page", "-"), 0))
        .withColumn("last_page", F.coalesce(F.get(F.split("page", "-"), 1), "first_page"))
        .withColumn("is_retracted",
            F.exists(F.col("update-to"), lambda x: F.lower(x["label"]) == "retraction") |
            F.exists(F.col("updated-by"), lambda x: F.lower(x["label"]) == "retraction")
        )
        .withColumn("abstract", F.substring(F.col("abstract"), 0, MAX_ABSTRACT_LENGTH))
        .withColumn("source_name", F.col("`container-title`")[0])
        .withColumn("publisher", F.col("publisher"))
        .withColumn("funders",
            F.transform(
                "funder",
                lambda fun: F.struct(
                    fun["DOI"].alias("doi"),
                    F.lit(None).cast("string").alias("ror"),
                    fun["name"].alias("name"),
                    fun["award"].alias("awards")
                )
            )
        )  # funders: no ror in crossref?
        .withColumn(
            "references",
            F.transform(
                "reference",
                lambda ref: F.struct(
                    ref["DOI"].alias(
                        "doi"
                    ),  # sometimes null and doi is not available to parse from the unstructured/raw field
                    F.lit(None).cast("string").alias("pmid"),
                    F.lit(None).cast("string").alias("arxiv"),
                    ref["article-title"].alias("title"),  # always null
                    ref["author"].alias("authors"),  # always null
                    ref["year"].alias("year"),  #  always null
                    ref["unstructured"].alias("raw")
                )
            )
        )
        .withColumn(
            "urls",
            F.array_union(
                F.array(
                    F.struct(
                        F.col("URL").alias("url"), F.lit("html").alias("content_type")
                    )
                ),  # url from items.url
                F.coalesce(
                    F.transform(
                        extract_url_by_type(),
                        lambda link: F.struct(
                            link["URL"].alias("url"),
                            F.when(
                                link["URL"].contains("pdf")
                                | link["content-type"].contains("pdf"),
                                F.lit("pdf"),
                            )
                            .otherwise(F.lit("html"))
                            .alias("content_type"),
                        ),
                    ),
                    F.array(
                        F.struct(
                            F.col("URL").alias("url"),
                            F.lit("html").alias("content_type"),
                        )
                    )
                )  # rest of the urls from items.link, except those that have content_type plain or xml. Coalesce used to preserve schema. array_union prevents duplicates.
            )
        )
        .withColumn("mesh", F.lit(None).cast("string"))
        # Does this col need to be same struct type as pubmed?
        .withColumn(
            "is_oa",
            F.when(
                F.lower(F.col("license")).startswith("cc")
                | F.lower(F.col("license")).contains("other-oa")
                | F.lower(F.col("license")).contains("public-domain")
                | (F.lower(F.col("publisher")) == "iucn"),
                F.lit(True),
            ).otherwise(F.lit(False))
        )
        .select(
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
            "_change_type", 
            "_commit_version", 
            "_commit_timestamp"
        )
    )

    # This table will be the target for your merge operation
dlt.create_streaming_table(
    name="crossref_parsed",
    comment="Parsed crossref data, executing transforms after de-duplication",
    cluster_by = ["native_id"],
    cluster_by_auto = True,
    table_properties = {
        #"delta.enableRowTracking": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "silver"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_parsed",
    source="crossref_parsed_source", # Use the name of the new table
    keys=["native_id"],
    sequence_by="updated_date",
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    apply_as_deletes="lower(_change_type) = 'delete'",  # 👈 Enable DELETE detection
    ignore_null_updates=True                            # 👈 Optional safety for sparse updates
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `crossref_processed`

# COMMAND ----------

# Step 1
@dlt.view(name="crossref_processed",
           comment="Crossref data after initial schema application and processing.")
def crossref_processed():
    # Define schema with raw_type for Crossref
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
        StructField("mesh", StringType(), True), StructField("is_oa", BooleanType(), True)
    ])
    
    df_parsed_input = (spark.readStream
        .option("readChangeFeed", "true").table("LIVE.crossref_parsed")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))        
    )
    return apply_initial_processing(df_parsed_input, "crossref", walden_works_with_raw_type_schema)

# Step 2: Isolate the expensive enrichment step into its own table.
@dlt.view(name="crossref_features_and_authors",
           comment="Applies expensive author and feature enrichment UDFs.")
def crossref_features_and_authors():
    df_processed = dlt.read_stream("crossref_processed")
    # enrich_with_features_and_author_keys is the expensive operation
    return enrich_with_features_and_author_keys(df_processed)

# Step 3: The final, quick step.
@dlt.view(name="crossref_works_source",
           comment="Applies merge_key and final filtering")
def crossref_works_source():
    df_processed = dlt.read_stream("crossref_features_and_authors")
    # enrich_with_features_and_author_keys is the expensive operation
    return apply_final_merge_key_and_filter(df_processed)

dlt.create_streaming_table(
    name = "crossref_works", # Final PUBLISHED table name
    comment = "Final Crossref works data including merge_key and filtering.",
    cluster_by = ["native_id"],
    cluster_by_auto = True,
    table_properties = {
        "delta.enableRowTracking": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

# NOW, apply_changes reads from the new intermediate table by its string name.
dlt.create_auto_cdc_flow(
    target="crossref_works",
    source="crossref_works_source", # Use the name of the new table
    keys=["native_id"],
    sequence_by="updated_date"
)
