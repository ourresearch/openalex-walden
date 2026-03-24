# Databricks notebook source
import re
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

# schema definition for DOAJ journal records
journal_schema = StructType([
    StructField("ns0:header", StructType([
        StructField("ns0:identifier", StringType(), True),
        StructField("ns0:datestamp", TimestampType(), True),
        StructField("ns0:setSpec", ArrayType(StringType()), True)
    ]), True),
    StructField("ns0:metadata", StructType([
        StructField("ns1:dc", StructType([
            StructField("dc:title", StringType(), True),
            StructField("dc:identifier", ArrayType(StringType()), True),
            StructField("dc:language", ArrayType(StringType()), True),
            StructField("dc:rights", ArrayType(StringType()), True),
            StructField("dc:publisher", StringType(), True),
            StructField("dc:relation", ArrayType(StringType()), True),
            StructField("dc:date", StringType(), True),
            StructField("dc:type", StringType(), True),
            StructField("dc:subject", ArrayType(StringType()), True)
        ]), True)
    ]), True)
])

# function to extract ISSNs
def extract_issns(identifiers):
    if not identifiers:
        return []
    issns = []
    for identifier in identifiers:
        if re.match(r'^\d{4}-\d{3}[\dX]$', identifier):
            issns.append(identifier)
    return issns

extract_issns_udf = F.udf(extract_issns, ArrayType(StringType()))

@dlt.table(
    name="doaj_journals_raw",
    table_properties={'quality': 'bronze'}
)
def doaj_journals_raw():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", "ns0:record")
        .option("compression", "gzip")
        .schema(journal_schema)
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/doaj/schema")
        .load("s3a://openalex-ingest/doaj/journals/")
    )


@dlt.table(
    name="doaj_journals_parsed"
)
def doaj_journals_parsed():
    return (dlt.read_stream("doaj_journals_raw")
        .filter(F.col("ns0:metadata").isNotNull())
        .withColumn("doaj_id", F.col("`ns0:header`.`ns0:identifier`"))
        .withColumn("updated_date", F.col("`ns0:header`.`ns0:datestamp`"))
        .withColumn("title", F.col("`ns0:metadata`.`ns1:dc`.`dc:title`"))
        .withColumn("issns", extract_issns_udf(F.col("`ns0:metadata`.`ns1:dc`.`dc:identifier`")))
        .withColumn("languages", F.col("`ns0:metadata`.`ns1:dc`.`dc:language`"))
        .withColumn("license", F.col("`ns0:metadata`.`ns1:dc`.`dc:rights`")[0])
        .withColumn("publisher", F.col("`ns0:metadata`.`ns1:dc`.`dc:publisher`"))
        .withColumn("subjects", F.col("`ns0:metadata`.`ns1:dc`.`dc:subject`"))
        .withColumn("website", F.expr("filter(`ns0:metadata`.`ns1:dc`.`dc:relation`, x -> x like 'http%')[0]"))
        .withColumn("doaj_url", F.expr("filter(`ns0:metadata`.`ns1:dc`.`dc:identifier`, x -> x like '%doaj.org/toc/%')[0]"))
        .select(
            "doaj_id",
            "title",
            "issns",
            "languages",
            "license",
            "publisher",
            "subjects",
            "website",
            "doaj_url",
            "updated_date"
        )
    )

dlt.create_target_table(
    name="doaj_journals",
    comment="Final DOAJ journals table with change tracking"
)

dlt.apply_changes(
    target="doaj_journals",
    source="doaj_journals_parsed",
    keys=["doaj_id"],
    sequence_by="updated_date"
)
