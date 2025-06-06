# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.1-py3-none-any.whl

# COMMAND ----------

import dlt
import re
import unicodedata
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

from openalex.dlt.normalize import normalize_title_udf, normalize_license, normalize_license_udf, walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

# COMMAND ----------


"""
Convert DataCite resource types to OpenAlex types.
Returns 'other' for types that don't map to OpenAlex types.
"""
datacite_to_openalex = {
    # article types
    "JournalArticle": "article",
    "ConferencePaper": "article",
    "DataPaper": "article",
    "Text": "article",
    
    # book types
    "Book": "book",
    "BookChapter": "book-chapter",
    
    # dataset
    "Dataset": "dataset",
    "Model": "dataset",
    "DatasetOutputManagementPlan": "dataset",
    
    # dissertation
    "Dissertation": "dissertation",
    
    # preprint
    "Preprint": "preprint",
    
    # report
    "Report": "report",
    "ProjectReport": "report",
    
    # standard
    "Standard": "standard",
    
    # peer review
    "PeerReview": "peer-review",

    # map everything else to other - AK, we don't *have* to do that - "other" is default anyway
    "Audiovisual": "other",
    "Award": "other",
    "Collection": "other",
    "ComputationalNotebook": "other",
    "ConferenceProceeding": "other",
    "Event": "other",
    "Image": "other",
    "InteractiveResource": "other",
    "Instrument": "other",
    "Journal": "other",
    "ModelOutput": "other",
    "PhysicalObject": "other",
    "Service": "other",
    "Software": "other",
    "Sound": "other",
    "StudyRegistration": "other",
    "Workflow": "other",
    "Other": "other"
}
openalex_type_from_datacite_mapping_expr = F.create_map([F.lit(x) for pair in datacite_to_openalex.items() for x in pair])

@pandas_udf(StringType())
def normalize_license_udf_vectorized(license_series: pd.Series) -> pd.Series:
    normalized = (
        license_series.fillna("")
        .str.replace(r"[\s\-]", "", regex=True)
        .str.lower()
    )

    extracted = normalized.str.extract(master_pattern)

    # Vectorized version using idxmax
    first_match = extracted.notna().idxmax(axis=1)
    has_match = extracted.notna().any(axis=1)

    # Restore formatting from label
    result = first_match.where(has_match).str.replace("_", "-").str.replace("2-0", "2.0")

    return result

# COMMAND ----------

@dlt.table(
  name="datacite_items",
  comment="Datacite ingest from s3",
  table_properties={'quality': 'bronze'}
)
@dlt.expect("rescued_data_null", "_rescued_data IS NULL")
def datacite_items():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("inferSchema", "true")
      .option("sampleSize", "10000000")
      .option("mergeSchema", "true")
      .option("compression", "gzip")
      .option("pathGlobFilter", "*.json*.gz")
      .load("s3a://openalex-ingest/datacite/")
  )

# COMMAND ----------

# @TODO This seems to be entirely unnecessary. id is native_id and will get deduped in apply_changes by updated_date
# which may be more accurate (i.e. you can have 2021 record with multiple 2025 updates and 2022 record with no updates and the deduplication below would keep the latter one)
# @dlt.table(
#   name="datacite_exploded",
#   comment="Datacite deduped on created_date and id column",
#   table_properties={"quality": "silver"}
# )
# def datacite_exploded():
#   return (dlt.read_stream("datacite_items")
#     .withColumn("created_date", F.to_date(F.col("attributes.updated")))
#     .dropDuplicates(["id", "created_date"]).drop("created_date")
#   )

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

@dlt.table(
    name="datacite_parsed",
    comment="Datacite works table in the Walden schema",
    table_properties={"quality": "silver"}
)
def datacite_parsed():
    return (
        dlt.read_stream("datacite_items").repartition(2048)
        .withColumn("native_id", F.col("id"))
        .withColumn("native_id_namespace",
            F.when(F.col("type") == "dois", "doi").otherwise(F.col("type")))
        .withColumn("title", F.substring(F.get(F.col("attributes.titles"), 0)["title"], 0, MAX_TITLE_LENGTH))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn(
            "authors", F.transform(
                "attributes.creators",
                lambda author: F.struct(
                    F.substring(author["givenName"], 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
                    F.substring(author["familyName"], 0, MAX_AUTHOR_NAME_LENGTH).alias("family"), 
                    F.substring(author["name"], 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
                    F.when(
                        F.get(author["nameIdentifiers"]["nameIdentifierScheme"], 0) == "ORCID",
                        F.regexp_extract(
                            F.get(author["nameIdentifiers"], 0)["nameIdentifier"],
                            r"(\d{4}-\d{4}-\d{4}-\d{4})",
                            1,
                        ),
                    ).alias("orcid"),
                    F.transform(
                        author["affiliation"],
                        lambda aff: F.struct(
                            F.substring(aff["name"], 0, MAX_AFFILIATION_STRING_LENGTH).alias("name"),
                            F.lit(None).cast("string").alias("department"),
                            F.when(
                                F.lower(aff["affiliationIdentifierScheme"]) == "ror",
                                aff["affiliationIdentifier"],
                            ).alias("ror_id"),
                        ),
                    ).alias("affiliations"),
                ),
            ),
        )
        # replaced array_union with caoncat+array_distinct to avoid duplicates and improve performane
        # per thread dumps, array_union appears to get stuck for hours - this transform on 20K 100MB tasks can take 3 hours
        .withColumn(
            "ids",
            F.array_distinct(
                F.concat(
                    F.array(F.struct(
                        F.col("id").alias("id"),
                        F.lit("doi").alias("namespace"),
                        F.lit("self").alias("relationship")
                    )),
                    F.filter(
                        F.transform(
                            F.col("attributes.relatedIdentifiers"),
                            lambda ids: F.struct(
                                ids["relatedIdentifier"].alias("id"),
                                F.lower(ids["relatedIdentifierType"]).alias("namespace"),
                                ids["relationType"].alias("relationship")
                            )
                        ),
                        lambda x: ~F.lower(x.namespace).isin("url", "references")
                    )
                )
            )
        )
        # Skip UDF usage entirely and pre-build a MapType to use in lookup
        .withColumn("type", F.coalesce(
                openalex_type_from_datacite_mapping_expr[F.col("attributes.types.resourceTypeGeneral")], F.lit("other"))
        )
        .withColumn("version", F.lit(None).cast("string"))
        .withColumn(
            "raw_license",
            F.when(
                F.isnull(
                    F.get(
                        F.filter(
                            F.col("attributes.rightslist.rightsUri"),
                            lambda x: x.contains("creativecommons.org"),
                        ),
                        0,
                    )
                ),
                F.get(F.col("attributes.rightslist.rightsUri"), 0),
            ).otherwise(
                F.get(
                    F.filter(
                        F.col("attributes.rightslist.rightsUri"),
                        lambda x: x.contains("creativecommons.org"),
                    ),
                    0,
                )
            ),         # If url with "creativecommons.org" exists, use that one, otherwise grab the first rightsUri
        )
        .withColumn("license", normalize_license_udf(F.col("raw_license")))
        .withColumn("language", F.col("attributes.language"))
        .withColumn(
            "published_date",
            F.least(
                F.to_date(F.col("attributes.registered")),
                F.to_date(F.col("attributes.created")),
            ),
        )
        .withColumn("created_date", F.to_date(F.col("attributes.created")))
        .withColumn("updated_date", F.to_date(F.col("attributes.updated")))
        .withColumn("issue", F.lit(None).cast("string"))
        .withColumn("volume", F.lit(None).cast("string"))
        .withColumn("first_page", F.lit(None).cast("string"))
        .withColumn("last_page", F.lit(None).cast("string"))
        .withColumn("is_retracted", F.lit(None).cast("boolean"))
        .withColumn(
            "abstract", F.substring(
            F.get(
                F.filter(
                    F.col("attributes.descriptions"),
                    lambda x: x.descriptionType == "Abstract",
                ),
                0,
            )["description"], 0, MAX_ABSTRACT_LENGTH)
        )
        .withColumn("source_name", F.lit(None).cast("string"))
        .withColumn("publisher", F.col("attributes.publisher.name"))
        .withColumn(
            "funders",
                F.transform(
                    "attributes.fundingReferences",
                    lambda fun: F.struct(
                        F.regexp_extract(
                            fun["funderIdentifier"], r"https://doi\.org/(.*)", 1
                        ).alias(
                            "doi"
                        ),  # Parse the doi from the doi url
                        F.lit(None).cast("string").alias("ror"),
                        fun["funderName"].alias("name"),
                        F.array(fun["awardNumber"]).alias("awards"),
                    ),
                ),  
            )
        .withColumn("funders", F.when(F.size(F.col("funders"))==0, None).otherwise(F.col("funders")))
        .withColumn(
            "references",
            F.transform(
                F.filter(
                    F.col("attributes.relatedIdentifiers"),
                    lambda x: F.lower(x.relationType) == "references",
                ),
                lambda ref: F.struct(
                    F.when(
                        ref["relatedIdentifierType"] == "DOI", ref["relatedIdentifier"]
                    )
                    .otherwise(F.lit(None))
                    .alias("doi"),
                    F.lit(None).cast("string").alias("pmid"),
                    F.lit(None).cast("string").alias("arxiv"),
                    F.lit(None).cast("string").alias("title"),
                    F.lit(None).cast("string").alias("authors"),
                    F.lit(None).cast("string").alias("year"),
                    F.when(
                        ref["relatedIdentifierType"] != "DOI", ref["relatedIdentifier"]
                    )
                    .otherwise(F.lit(None))
                    .alias("raw"),
                ),
            ),
        )
        .withColumn("references", F.when(F.size(F.col("references"))==0, None).otherwise(F.col("references")))
        .withColumn(
            "urls",
            F.transform(F.filter(
                F.array(
                    F.array(
                        F.struct(
                            F.col("attributes.url").alias("url"),
                            F.lit("html").alias(
                                "content_type"
                            ),  # url from attributes.url
                        )
                    ),
                    F.transform(
                        F.filter(
                            F.col("attributes.identifiers"),
                            lambda x: F.contains(x.identifier, F.lit(".pdf")),
                        ),
                        lambda url: F.struct(
                            F.regexp_extract(
                                url["identifier"], r"(https?://[^\s]+\.pdf)", 1
                            ).alias(
                                "url"
                            ),  # pdfs from attributes.identifiers
                            F.lit("pdf").alias("content_type"),
                        ),
                    ),
                    F.transform(
                        F.filter(
                            F.col("attributes.identifiers"),
                            lambda x: x.identifierType.isin(
                                ["uri", "URL"]
                            ),  # other htmls from attributes.identifiers with identifierType = uri or URL
                        ),
                        lambda url: F.struct(
                            url["identifier"].alias("url"),
                            F.lit("html").alias("content_type"),
                        )
                    ),
                ),
                lambda x: F.get(x, 0).isNotNull(),
            ), lambda x: F.get(x, 0))
        )
        .withColumn("mesh", F.lit(None).cast("string"))
        .withColumn("is_oa", F.lit(True))
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

@dlt.table(name="datacite_enriched",
           comment="DataCite data after full parsing and author/feature enrichment.")
def datacite_enriched():
    df_parsed_input = dlt.read_stream("datacite_parsed")
    df_walden_works_schema = apply_initial_processing(df_parsed_input, "datacite", walden_works_schema)

    # enrich_with_features_and_author_keys is imported from your openalex.dlt.transform
    # It applies udf_last_name_only (Pandas UDF) and udf_f_generate_inverted_index (Pandas UDF)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return apply_final_merge_key_and_filter(df_enriched)

dlt.create_streaming_table(
    name="datacite_works",
    comment="Final datacite works table with unique identifiers and in the Walden schema",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

dlt.apply_changes(
    target="datacite_works",
    source="datacite_enriched",
    keys=["native_id"],
    sequence_by="updated_date"
)
