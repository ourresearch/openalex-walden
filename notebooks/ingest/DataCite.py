# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.3-py3-none-any.whl

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
Convert DataCite resource types to OpenAlex types (oxjob #539).

Tier 1 below maps the *controlled* `resourceTypeGeneral` vocabulary. Only SPECIFIC values are keys:
the generic buckets (Text / Other / Collection) are deliberately OMITTED so the Tier-1 lookup returns
null and `datacite_type_cascade` falls through to the citeproc (Tier 2) and resourceType (Tier 3)
signals. Unmapped / unknown values default to 'other'. Full rationale + sizing in the job's MAPPING.md.
"""
datacite_to_openalex = {
    # article
    "JournalArticle": "article",

    # conference (#539: ConferencePaper/ConferenceProceeding were article/other; Poster was other)
    "ConferencePaper": "conference-paper",
    "ConferenceProceeding": "conference-paper",
    "Poster": "conference-abstract",

    # data-paper (#539: was article)
    "DataPaper": "data-paper",

    # book
    "Book": "book",
    "BookChapter": "book-chapter",

    # dataset
    "Dataset": "dataset",
    "Model": "dataset",

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

    # software (#539: Software/ComputationalNotebook were other)
    "Software": "software",
    "ComputationalNotebook": "software",

    # explicit other (media / non-scholarly artifacts — NOT refined by the cascade)
    "Audiovisual": "other",
    "Award": "other",
    "Event": "other",
    "Image": "other",
    "InteractiveResource": "other",
    "Instrument": "other",
    "Journal": "other",
    "ModelOutput": "other",
    "PhysicalObject": "other",
    "Service": "other",
    "Sound": "other",
    "StudyRegistration": "other",
    "Workflow": "other",
    # NOTE (#539): "Text", "Other", "Collection" intentionally absent -> handled by the cascade.
    # Removed dead key "DatasetOutputManagementPlan" (real value is "OutputManagementPlan",
    # which correctly falls to the 'other' default).
}
openalex_type_from_datacite_mapping_expr = F.create_map([F.lit(x) for pair in datacite_to_openalex.items() for x in pair])

# Tier 2: citeproc value, used ONLY when Tier 1 is a generic bucket (Text/Other/Collection/null).
# GUARDRAIL: citeproc='article' is DataCite's "no usable type" placeholder (image files, research
# cruises, seismic events) and is intentionally NOT a key here -> it never becomes 'article'. Only the
# genuine 'article-journal' is rescued. bibtex/schemaOrg are NOT cross-checked: they are lossier
# co-derived vocabularies, so requiring agreement only drops real records (oxjob #539; e.g. it would
# discard ~102K real reports typed bibtex='article' and ~96K datasets typed schemaOrg='DataDownload').
citeproc_to_openalex = {
    "book": "book",
    "thesis": "dissertation",
    "report": "report",
    "chapter": "book-chapter",
    "dataset": "dataset",
    "article-journal": "article",
}
citeproc_type_from_datacite_mapping_expr = F.create_map([F.lit(x) for pair in citeproc_to_openalex.items() for x in pair])


def datacite_type_cascade(rtg_col, citeproc_col, resource_type_col):
    """oxjob #539: 3-tier DataCite work-type resolution (first match wins).

    Tier 1  resourceTypeGeneral (controlled map above) — specific values win outright.
    Tier 3  resourceType free-text allowlist — only for generic buckets; evaluated BEFORE Tier 2
            because posts/posters carry citeproc='article-journal'.
    Tier 2  citeproc value — only for generic buckets; the 'article' placeholder is excluded by
            construction (see GUARDRAIL above).
    Default Text historically -> article; everything else -> other.
    See the job's MAPPING.md for sizing and the full rationale.
    """
    is_generic = rtg_col.isNull() | rtg_col.isin("Text", "Other", "Collection")
    sub = F.lower(resource_type_col)
    return F.coalesce(
        openalex_type_from_datacite_mapping_expr[rtg_col],
        F.when(is_generic & (sub == "post"), F.lit("other"))
         .when(is_generic & sub.like("%poster%"), F.lit("conference-abstract")),
        F.when(is_generic, citeproc_type_from_datacite_mapping_expr[citeproc_col]),
        F.when(rtg_col == "Text", F.lit("article")).otherwise(F.lit("other")),
    )

@F.pandas_udf(StringType())
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
      .withColumn("ingested_at", F.current_timestamp())
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

# CDL/EZID Kernel Metadata Reserved Codes used as sentinels for unavailable values
# (e.g. "(:unav)", "(:Unkn) Unknown"). See notebooks/authors/CleanPlaceholderAuthorNames.ipynb.
PLACEHOLDER_NAME_REGEX = r"^\s*\(:un[a-z]{2,3}\)(\s*(unknown( author)?|unassigned))?\s*$"

def _null_if_placeholder(col):
    return F.when(F.lower(col).rlike(PLACEHOLDER_NAME_REGEX), F.lit(None).cast("string")).otherwise(col)

def _is_placeholder_or_empty(col):
    return col.isNull() | (F.length(F.trim(col)) == 0) | F.lower(col).rlike(PLACEHOLDER_NAME_REGEX)

@dlt.table(
    name="datacite_parsed",
    comment="Datacite works table in the Walden schema",
    table_properties={"quality": "silver"}
)
def datacite_parsed():
    return (
        dlt.read_stream("datacite_items")
        .repartition(2000)
        .withColumn("native_id", F.col("id"))
        .withColumn("native_id_namespace",
            F.when(F.col("type") == "dois", "doi").otherwise(F.col("type")))
        .withColumn("title", F.substring(F.get(F.col("attributes.titles"), 0)["title"], 0, MAX_TITLE_LENGTH))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn(
            "authors", F.transform(
                F.filter(
                    "attributes.creators",
                    lambda a: ~(
                        _is_placeholder_or_empty(a["name"])
                        & _is_placeholder_or_empty(a["givenName"])
                        & _is_placeholder_or_empty(a["familyName"])
                    ),
                ),
                lambda author: F.struct(
                    F.substring(_null_if_placeholder(author["givenName"]), 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
                    F.substring(_null_if_placeholder(author["familyName"]), 0, MAX_AUTHOR_NAME_LENGTH).alias("family"),
                    F.substring(_null_if_placeholder(author["name"]), 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
                    F.when(
                        F.get(author["nameIdentifiers"]["nameIdentifierScheme"], 0) == "ORCID",
                        F.regexp_extract(
                            F.get(author["nameIdentifiers"], 0)["nameIdentifier"],
                            r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])",
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
                    ),
                    F.array(F.struct(
                        F.col("relationships.client.data.id").alias("id"),
                        F.lit("datacite_client").alias("namespace"),
                        F.lit("self").alias("relationship")
                    ))
                )
            )
        )
        # Skip UDF usage entirely and pre-build a MapType to use in lookup
        .withColumn("raw_type", F.col("attributes.types.citeproc"))
        # oxjob #539: 3-tier resolution (resourceTypeGeneral -> resourceType allowlist -> citeproc)
        .withColumn("type", datacite_type_cascade(
                F.col("attributes.types.resourceTypeGeneral"),
                F.col("attributes.types.citeproc"),
                F.col("attributes.types.resourceType"),
        ))
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
            F.coalesce(
                F.to_date(F.expr("array_min(filter(attributes.dates, d -> lower(d.dateType) = 'submitted').date)")),
                F.to_date(F.expr("array_min(attributes.dates.date)")),
                F.least(
                    F.to_date(F.col("attributes.registered")),
                    F.to_date(F.col("attributes.created")),
                )
            ),
        )
        .withColumn(
            "published_date",
            F.when(F.year(F.col("published_date")) >= 1900, F.col("published_date")).otherwise(F.lit(None))
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
        "ingested_at"
    )
)

# COMMAND ----------

@dlt.table(name="datacite_enriched",
           comment="DataCite data after full parsing and author/feature enrichment.")
def datacite_enriched():
    # Define schema with raw_type for DataCite
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
        StructField("mesh", StringType(), True), StructField("is_oa", BooleanType(), True),
        StructField("ingested_at", TimestampType(), True)
    ])

    df_parsed_input = dlt.read_stream("datacite_parsed")
    df_walden_works_schema = apply_initial_processing(df_parsed_input, "datacite", walden_works_with_raw_type_schema)

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
