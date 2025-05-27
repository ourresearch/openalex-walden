# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.1.5-py3-none-any.whl

# COMMAND ----------

import dlt
import re
import unicodedata
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

from openalex.dlt.normalize import normalize_title_udf, normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

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
    # This Pandas UDF calls your original 'get_openalex_type_from_datacite' Python function
    return series.apply(get_openalex_type)

# COMMAND ----------

# Raw data in single column as items table
@dlt.table(
  name="crossref_items",
  table_properties={'quality': 'bronze'}
)
def crossref_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true") 
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("cloudFiles.maxFilesPerTrigger", 50000)
      .option("cloudFiles.maxBytesPerTrigger", "10gb")
      .option("cloudFiles.fetchParallelism", 64)
      .option("multiLine", "true") 
      .load("s3a://openalex-ingest/crossref/")
  )

# COMMAND ----------

# Exploded data to multiple columns
@dlt.table(
  name="crossref_exploded",
  table_properties={'quality': 'silver'}
)
def crossref_snapshots_exploded():
  return (dlt.read_stream("crossref_items")
      .select(F.explode(F.col("items")).alias("record"))
      .select("record.*")
      .withColumn("indexed_date", F.col("indexed.date-time"))#.drop("indexed_date")
      #.dropDuplicates(["DOI", "indexed_date"]).drop("indexed_date") - apply_changed will de-dup these records DOI == native_id
  )

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000 
unallowed_types = ["component"]

# COMMAND ----------

# Transformed Table
@dlt.table(
    name="crossref_parsed",
    comment="Crossref data transformed to a denormalized, Walden schema",
    table_properties={"quality": "silver"}
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
        dlt.read_stream("crossref_exploded")
        .filter(~F.col("type").isin(unallowed_types))
        .withColumn("native_id", F.regexp_replace(F.col("DOI"), "", ""))
        .withColumn("native_id_namespace", F.lit("doi"))
        .withColumn("title", F.substring(F.get(F.col("title"), 0), 0, MAX_TITLE_LENGTH))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn(
            "authors",
            F.transform(
                "author",
                lambda author: F.struct(
                    F.substring(author["given"], 0, MAX_AUTHOR_NAME_LENGTH).alias(
                        "given"
                    ),
                    F.substring(author["family"], 0, MAX_AUTHOR_NAME_LENGTH).alias(
                        "family"
                    ),
                    F.substring(author["name"], 0, MAX_AUTHOR_NAME_LENGTH).alias(
                        "name"
                    ),
                    F.regexp_extract(
                        author["ORCID"], r"(\d{4}-\d{4}-\d{4}-\d{4})", 1
                    ).alias("orcid"),
                    F.transform(
                        author["affiliation"],
                        lambda aff: F.struct(
                            F.substring(
                                aff["name"], 0, MAX_AFFILIATION_STRING_LENGTH
                            ).alias("name"),
                            F.substring(
                                F.get(aff["department"], 0),
                                0,
                                MAX_AFFILIATION_STRING_LENGTH,
                            ).alias("department"),
                            F.when(
                                F.get(aff["id"]["id-type"], 0) == "ROR",
                                F.get(aff["id"]["id"], 0),
                            ).alias("ror_id"),
                        ),
                    ).alias("affiliations"),
                ),
            ),
        )
        .withColumn(
            "ids",
            F.filter(
                F.array(
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_issn_id_by_type("print"), 0)), None
                        )
                        .otherwise(F.get(extract_issn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship"),
                    ),
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_issn_id_by_type("electronic"), 0)),
                            None,
                        )
                        .otherwise(F.get(extract_issn_id_by_type("electronic"), 0))
                        .alias("id"),
                        F.lit("eissn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship"),
                    ),
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_isbn_id_by_type("print"), 0)), None
                        )
                        .otherwise(F.get(extract_isbn_id_by_type("print"), 0))
                        .alias("id"),
                        F.lit("pisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship"),
                    ),
                    F.struct(
                        F.when(
                            F.isnull(F.get(extract_isbn_id_by_type("electronic"), 0)),
                            None,
                        )
                        .otherwise(F.get(extract_isbn_id_by_type("electronic"), 0))
                        .alias("id"),
                        F.lit("eisbn").alias("namespace"),
                        F.lit(None).cast("string").alias("relationship"),
                    ),
                    F.struct(
                        F.when(F.isnull(F.col("DOI")), None)
                        .otherwise(F.col("DOI"))
                        .alias("id"),
                        F.lit("doi").alias("namespace"),
                        F.lit("self").alias("relationship"),
                    ),
                ),
                lambda x: x.id != "",
            ),  # Filter out empty ids
        )
        .withColumn("version", 
            F.when(F.col("type") == "posted-content", F.lit("acceptedVersion"))
            .otherwise(F.lit("publishedVersion"))
        )
        .withColumn("type", get_openalex_type_udf(F.col("type")))
        .withColumn(
            "raw_license",  # Filter out content-version = "tdm", then prioritize the url containing "creativecommons.org", otherwise select the first url
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
                    0,
                )
            ),
        )
        .withColumn("license",
            F.when(
                normalize_license_udf(F.col("raw_license")).isNotNull() & 
                (normalize_license_udf(F.col("raw_license")) != ""),
                normalize_license_udf(F.col("raw_license"))
            ).otherwise(
                normalize_license_udf(F.col("raw_license"))
            )
        )
        .withColumn("language", F.col("language"))
        .withColumn("created_date", F.to_date(F.col("created.`date-time`")))
        .withColumn("deposited", F.to_date(F.col("deposited.`date-time`")))
        .withColumn(
            "issued",
            F.make_date(
                F.get(F.col("issued.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("issued.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("issued.`date-parts`")[0], 2), F.lit(1)),
            ),
        )
        .withColumn(
            "published",
            F.make_date(
                F.get(F.col("published.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("published.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("published.`date-parts`")[0], 2), F.lit(1)),
            ),
        )
        .withColumn(
            "approved",
            F.make_date(
                F.get(F.col("approved.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("approved.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("approved.`date-parts`")[0], 2), F.lit(1)),
            ),
        )
        .withColumn(
            "published_date",
            F.least(
                F.col("issued"),
                F.col("created_date"),
                F.col("published"),
                F.col("approved"),
                F.col("deposited"),
            ),
        )
        .withColumn(
            "updated_date",
            F.make_date(
                F.get(F.col("indexed.`date-parts`")[0], 0),
                F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 1), F.lit(1)),
                F.coalesce(F.get(F.col("indexed.`date-parts`")[0], 2), F.lit(1)),
            ),
        )
        .withColumn("issue", F.col("issue"))
        .withColumn("volume", F.col("volume"))
        .withColumn("first_page", F.get(F.split("page", "-"), 0))
        .withColumn(
            "last_page", F.coalesce(F.get(F.split("page", "-"), 1), "first_page")
        )
        .withColumn("is_retracted", F.col("update-to.label")[0] == "Retraction")
        .withColumn("abstract", F.substring(F.col("abstract"), 0, MAX_ABSTRACT_LENGTH))
        .withColumn("source_name", F.col("`container-title`")[0])
        .withColumn("publisher", F.col("publisher"))
        .withColumn(
            "funders",
            F.transform(
                "funder",
                lambda fun: F.struct(
                    fun["DOI"].alias("doi"),
                    F.lit(None).cast("string").alias("ror"),
                    fun["name"].alias("name"),
                    fun["award"].alias("awards"),
                ),
            ),
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
                    ref["unstructured"].alias("raw"),
                ),
            ),
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
                    ),
                ),  # rest of the urls from items.link, except those that have content_type plain or xml. Coalesce used to preserve schema. array_union prevents duplicates.
            ),
        )
        .withColumn(
            "mesh", F.lit(None).cast("string")
        )  # Does this col need to be same struct type as pubmed?
        .withColumn(
            "is_oa",
            F.when(
                F.lower(F.col("license")).startswith("cc")
                | F.lower(F.col("license")).contains("other-oa")
                | F.lower(F.col("license")).contains("public-domain")
                | (F.lower(F.col("publisher")) == "iucn"),
                F.lit(True),
            ).otherwise(F.lit(False)),
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
            "is_oa",
        )
    )

# COMMAND ----------

@dlt.table(name="crossref_enriched", temporary=True, 
           comment="Crossref data after full parsing and author/feature enrichment.")
def crossref_enriched():
    df_parsed_input = dlt.read_stream("crossref_parsed")
    df_walden_works_schema = apply_initial_processing(df_parsed_input, "crossref", walden_works_schema)

    # enrich_with_features_and_author_keys is imported from your openalex.dlt.transform
    # It applies udf_last_name_only (Pandas UDF) and udf_f_generate_inverted_index (Pandas UDF)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return apply_final_merge_key_and_filter(df_enriched)

# COMMAND ----------

dlt.create_target_table(
    name="crossref_works", # Final PUBLISHED table name
    comment="Final Crossref works data including merge_key, filtered, and managed by APPLY CHANGES.",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="crossref_works",
    source="crossref_enriched",
    keys=["native_id"],
    sequence_by="updated_date"
)
