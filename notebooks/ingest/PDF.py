# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.1-py3-none-any.whl

# COMMAND ----------

import dlt
import re
from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import *
import xml.etree.ElementTree as ET

import unicodedata
from functools import reduce
import pandas as pd

from openalex.dlt.normalize import walden_works_schema
from openalex.dlt.transform import apply_initial_processing, apply_final_merge_key_and_filter, enrich_with_features_and_author_keys

# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000
MAX_FULLTEXT_LENGTH = 200000

# define the schema for extracted fields
fields_schema = StructType([
    StructField("title", StringType(), True),
    StructField("language", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("authors", ArrayType(
        StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True),
            StructField("name", StringType(), True),
            StructField("orcid", StringType(), True),
            StructField("affiliations", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("department", StringType(), True),
                    StructField("ror_id", StringType(), True)
                ])
            ))
        ])
    ), True),
    StructField("source_name", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("issue", StringType(), True),
    StructField("first_page", StringType(), True),
    StructField("last_page", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("published_date", DateType(), True),
    StructField("references", ArrayType(
        StructType([
            StructField("raw", StringType(), True)
        ])
    ), True),
    StructField("funders", ArrayType(
        StructType([
            StructField("doi", StringType(), True),
            StructField("ror", StringType(), True),
            StructField("name", StringType(), True),
            StructField("awards", ArrayType(StringType()), True)
        ])
    ), True),
    StructField("fulltext", StringType(), True)
])

# COMMAND ----------

def clean_xml(xml_content):
    """Clean invalid characters in the XML content."""
    if not xml_content:
        return None
    return re.sub(r'[^\x09\x0A\x0D\x20-\x7E]', '', xml_content)

def extract_fields(xml_content):
    """Extract multiple fields from the XML."""
    try:
        xml_content = clean_xml(xml_content)
        if not xml_content:
            return {
                "title": None,
                "language": None,
                "abstract": None,
                "authors": [],
                "source_name": None,
                "volume": None,
                "issue": None,
                "first_page": None,
                "last_page": None,
                "publisher": None,
                "published_date": pd.NaT,
                "references": [],
                "funders": [],
                "fulltext": None
            }

        root = ET.fromstring(xml_content)
        ns = {"tei": "http://www.tei-c.org/ns/1.0"}

        # title
        title = None
        title_element = root.find(".//tei:titleStmt/tei:title", namespaces=ns)
        if title_element is not None and title_element.text:
            title = title_element.text.strip()

        # language
        language = None
        tei_header = root.find(".//tei:teiHeader", namespaces=ns)
        if tei_header is not None:
            language = tei_header.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", None)

        # abstract
        abstract = None
        abstract_element = root.find(".//tei:profileDesc/tei:abstract", namespaces=ns)
        if abstract_element is not None:
            abstract = ''.join(abstract_element.itertext()).strip()

        # authors
        authors = []
        author_elements = root.findall(".//tei:author", namespaces=ns)
        for author in author_elements:
            given_name = author.find(".//tei:persName/tei:forename[@type='first']", namespaces=ns)
            family_name = author.find(".//tei:persName/tei:surname", namespaces=ns)
            orcid = author.find(".//tei:idno[@type='ORCID']", namespaces=ns)

            affiliations = []
            affiliation_elements = author.findall(".//tei:affiliation", namespaces=ns)
            for affiliation in affiliation_elements:
                name = affiliation.find(".//tei:note[@type='raw_affiliation']", namespaces=ns)
                affiliations.append({
                    "name": name.text.strip() if name is not None else None,
                    "department": None,
                    "ror_id": None
                })

            authors.append({
                "given": given_name.text.strip() if given_name is not None else None,
                "family": family_name.text.strip() if family_name is not None else None,
                "name": None,
                "orcid": orcid.text.strip() if orcid is not None else None,
                "affiliations": affiliations
            })

        # source_name
        source_name = None
        source_element = root.find(".//tei:biblStruct/tei:monogr/tei:title", namespaces=ns)
        if source_element is not None:
            source_name = source_element.text.strip()

        # publisher
        publisher = None
        publisher_element = root.find(".//tei:imprint/tei:publisher", namespaces=ns)
        if publisher_element is not None and publisher_element.text:
            publisher = publisher_element.text.strip()

        # issue
        issue = None
        issue_element = root.find(".//tei:biblScope[@unit='issue']", namespaces=ns)
        if issue_element is not None:
            issue = issue_element.text.strip()

        # volume
        volume = None
        volume_element = root.find(".//tei:biblScope[@unit='volume']", namespaces=ns)
        if volume_element is not None:
            volume = volume_element.text.strip()

        # first_page and last_page
        first_page = None
        last_page = None
        extent_element = root.find(".//tei:sourceDesc/tei:biblStruct/tei:monogr/tei:imprint/tei:extent/tei:measure[@unit='page']", namespaces=ns)
        if extent_element is not None:
            first_page = extent_element.attrib.get("from", None)
            last_page = extent_element.attrib.get("to", None)

        # references
        references = []
        reference_elements = root.findall(".//tei:listBibl/tei:biblStruct", namespaces=ns)
        for ref in reference_elements:
            raw = ref.find(".//tei:note[@type='raw_reference']", namespaces=ns)
            references.append({
                "raw": raw.text.strip() if raw is not None else None
            })

        # funders
        funders = []
        funder_elements = root.findall(".//tei:funder", namespaces=ns)
        for funder in funder_elements:
            # Extract the funder name
            org_name = funder.find(".//tei:orgName", namespaces=ns)

            # Extract awards associated with the funder
            awards = []
            award_elements = funder.findall(".//tei:idno[@type='award']", namespaces=ns)
            for award in award_elements:
                if award.text:
                    awards.append(award.text.strip())

            funders.append({
                "doi": None,
                "ror": None,
                "name": org_name.text.strip() if org_name is not None else None,
                "awards": awards if awards else None
            })

        # fulltext
        fulltext = None
        text_element = root.find(".//tei:text", namespaces=ns)
        if text_element is not None:
            fulltext = ''.join(text_element.itertext()).strip()
            
            # clean and truncate
            if fulltext:
                # remove XML/HTML tags
                fulltext = re.sub(r'<[^>]+>', '', fulltext)
                # normalize whitespace
                fulltext = ' '.join(fulltext.split())
                # remove empty lines and normalize line breaks
                fulltext = '\n'.join([line.strip() for line in fulltext.splitlines() if line.strip()])
                # truncate to 200k characters
                if len(fulltext) > MAX_FULLTEXT_LENGTH:
                    fulltext = fulltext[:MAX_FULLTEXT_LENGTH]
                
        return {
            "title": title,
            "language": language,
            "abstract": abstract,
            "authors": authors,
            "source_name": source_name,
            "volume": volume,
            "issue": issue,
            "first_page": first_page,
            "last_page": last_page,
            "publisher": publisher,
            "published_date": pd.NaT,
            "references": references,
            "funders": funders,
            "fulltext": fulltext
        }
    except Exception:
        return {
            "title": None,
            "language": None,
            "abstract": None,
            "authors": [],
            "source_name": None,
            "volume": None,
            "issue": None,
            "first_page": None,
            "last_page": None,
            "publisher": None,
            "published_date": pd.NaT,
            "references": [],
            "funders": [],
            "fulltext": None
        }

# @TODO figure out if we can use pandas_udf here
# @udf(fields_schema)
# def extract_fields_udf(xml_content):
#     return extract_fields(xml_content)

@pandas_udf(fields_schema)
def extract_fields_udf(xml_series: pd.Series) -> pd.DataFrame:
    results = [extract_fields(xml) for xml in xml_series]
    return pd.DataFrame(results)

# COMMAND ----------

@dlt.view
def grobid_raw():
    return (
        spark.readStream
        .format("delta")
        .table("openalex.pdf.grobid_processing_results")
        .where("source_pdf_id IS NOT NULL")
        .withColumn("native_id", 
            when(
                col("native_id").startswith("https://doi.org/"),
                expr("substring(native_id, length('https://doi.org/') + 1)")
            )
            .otherwise(col("native_id"))
        )  # just in case, as some native_id came in as https://doi.org/ before and it affects matching
    )

@dlt.table
def pdf_parse():
   df = dlt.read_stream("grobid_raw")
   parsed_df = df.withColumn("fields", extract_fields_udf(col("xml_content")))

   version_column = when(
        (col("native_id_namespace") == "pmh") &
        (
            lower(col("xml_content")).contains("accepted version") |
            lower(col("xml_content")).contains("manuscript")
        ),
        "acceptedVersion"
    ).when(
        (col("native_id_namespace") == "pmh"),
        "submittedVersion"
    ).when(
        (col("native_id_namespace") == "doi") &
        (
            lower(col("native_id")).contains("arxiv") |
            lower(col("native_id")).contains("zenodo")
        ),
        "acceptedVersion"
    ).when(
        (col("native_id_namespace") == "doi"),
        "publishedVersion"
    ).otherwise(None)

   return parsed_df.select(
       col("url").alias("native_id"),
       lit("url").alias("native_id_namespace"),
       array(
            struct(
                col("url").alias("id"),
                lit("url").alias("namespace"),
                lit("self").alias("relationship")
            ),
            struct(
                col("native_id").alias("id"),
                col("native_id_namespace").alias("namespace"),
                lit("None").alias("relationship")
            ),
            struct(
                concat(col("source_pdf_id"), lit(".pdf")).alias("id"),
                lit("docs.pdf").alias("namespace"),
                lit(None).alias("relationship")
            ),
            struct(
                concat(col("id"), lit(".xml.gz")).alias("id"),
                lit("docs.parsed-pdf").alias("namespace"),
                lit(None).alias("relationship")
            )
        ).alias("ids"),
       substring(col("fields.title"), 0, MAX_TITLE_LENGTH).alias("title"),
       expr(f"""
           transform(fields.authors, a -> struct(
               substring(a.given, 0, {MAX_AUTHOR_NAME_LENGTH}) as given,
               substring(a.family, 0, {MAX_AUTHOR_NAME_LENGTH}) as family,
               substring(a.name, 0, {MAX_AUTHOR_NAME_LENGTH}) as name,
               a.orcid as orcid,
               transform(
                   a.affiliations,
                   aff -> struct(
                       substring(aff.name, 0, {MAX_AFFILIATION_STRING_LENGTH}) as name,
                       aff.department as department,
                       aff.ror_id as ror_id
                   )
               ) as affiliations
           ))
       """).alias("authors"),
       col("fields.language").alias("language"),
       substring(col("fields.abstract"), 0, MAX_ABSTRACT_LENGTH).alias("abstract"),
       col("fields.source_name").alias("source_name"),
       col("fields.volume").alias("volume"),
       col("fields.issue").alias("issue"),
       col("fields.first_page").alias("first_page"),
       col("fields.last_page").alias("last_page"),
       col("fields.publisher").alias("publisher"),
       col("fields.references").alias("references"),
       col("fields.funders").alias("funders"),
       col("created_date").alias("created_date"),
       current_timestamp().alias("updated_date"),
       lit(True).alias("is_oa"),
       version_column.alias("version"),
       col("fields.fulltext").alias("fulltext")
   )

# COMMAND ----------

@dlt.view
def pdf_backfill():
    return (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .table("openalex.pdf.pdf_works_grobid_backfill")
            .drop("urls")
            .drop("_change_type", "_commit_version", "_commit_timestamp")
            .withColumn("created_date", to_timestamp(col("created_date")))
    )

@dlt.table
def pdf_combined():
    return dlt.read_stream("pdf_parse").unionByName(dlt.read_stream("pdf_backfill"))

# COMMAND ----------

@dlt.table(name="pdf_enriched",
           comment="PDF data after full parsing and author/feature enrichment.")
def pdf_enriched():
    #pdf_combined = dlt.read_stream("pdf_combined")
    df_parsed_input = (
        spark.readStream
            .option("readChangeFeed", "true")
            .table("LIVE.pdf_combined")
            # figure out how to handle deletes at some point
            .filter(col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_version", "_commit_timestamp")  # ✅ Required
    )

    df_walden_works_schema = apply_initial_processing(df_parsed_input, "pdf", walden_works_schema)
    df_enriched = enrich_with_features_and_author_keys(df_walden_works_schema)
    return apply_final_merge_key_and_filter(df_enriched)

# @dlt.view(name="pdf_enriched_append_only")
# def pdf_enriched_append()
#     return spark.readStream.option("skipChangeCommits", "true").table("pdf_enriched")

dlt.create_streaming_table(
    name="pdf_works",
    comment="Final PDF works table with unique identifiers and in the Walden schema",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

dlt.apply_changes(
    target="pdf_works",
    source="pdf_enriched",
    keys=["native_id"],
    sequence_by="updated_date",
)
