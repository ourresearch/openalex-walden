# Databricks notebook source
import dlt
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

@dlt.table(
  name="ror_raw",
  table_properties={'quality': 'bronze'}
)
def ror_raw():
  return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", "s3a://openalex-ingest/ror/schema")
      .option("mergeSchema", "true")
      .load("s3a://openalex-ingest/ror/json")
  )

# COMMAND ----------

@dlt.table(
    name="ror_parsed",
    table_properties={'quality': 'silver'}
)
def ror_parsed():
    return (
        dlt.read_stream("ror_raw")
        .withColumn("created_date", F.to_date(F.col("admin.created.date"), "yyyy-MM-dd"))
        .withColumn("updated_date", F.to_date(F.col("admin.last_modified.date"), "yyyy-MM-dd"))
        .select(
            "id",
            "names",
            "locations",
            "links",
            "external_ids",
            "relationships",
            "types",
            "status",
            "established",
            "created_date",
            "updated_date"
        )
    )

# COMMAND ----------

dlt.create_target_table(
    name="ror",
    comment="Final ror table with unique IDs",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="ror",
    source="ror_parsed",
    keys=["id"],
    sequence_by="updated_date"
)
