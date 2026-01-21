# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(
    name="base_institutions",
    table_properties={"quality": "bronze"}
)
def base_institutions():
    return (spark.table("openalex.institutions.institutions_from_postgres")
        .drop("is_in_doaj")
        .withColumn(
            "ror_id",
            concat(lit("https://ror.org/"), col("ror_id"))
        )
    )

@dlt.table(
    name="new_ror_institutions",
    table_properties={"quality": "bronze", "skipChangeCommits": "true"}
)
def new_ror_institutions():
    base_batch = spark.table("openalex.institutions.institutions_from_postgres").drop("is_in_doaj")
    ror_df = spark.table("openalex.institutions.ror")
    
    # get the maximum ID from existing institutions
    max_id = base_batch.agg(max("id")).collect()[0][0]
    
    # find ROR records that don't have matching institutions
    existing_ror_ids = (base_batch
        .withColumn("ror_id", concat(lit("https://ror.org/"), col("ror_id")))
        .select("ror_id")
    )
    
    return (
        ror_df
        .join(existing_ror_ids, ror_df.id == existing_ror_ids.ror_id, "left_anti")
        .withColumn("row_number", row_number().over(Window.orderBy("id")))
        .withColumn("new_id", lit(max_id) + col("row_number"))
        .select(
            col("new_id").alias("id"),
            
            # extract display name from names array (prefer ror_display, fallback to label)
            coalesce(
                expr("filter(names, x -> array_contains(x.types, 'ror_display'))[0].value"),
                expr("filter(names, x -> array_contains(x.types, 'label'))[0].value"),
                col("names")[0]["value"]
            ).alias("display_name"),
            
            # extract website from links array
            expr("filter(links, x -> x.type = 'website')[0].value").alias("official_page"),
            
            # extract Wikipedia from links array
            expr("filter(links, x -> x.type = 'wikipedia')[0].value").alias("wiki_page"),
            
            # extract country code from locations array
            col("locations")[0]["geonames_details"]["country_code"].alias("iso3166_code"),
            
            # extract coordinates from locations array
            col("locations")[0]["geonames_details"]["lat"].alias("latitude"),
            col("locations")[0]["geonames_details"]["lng"].alias("longitude"),
            
            # extract GRID ID from external_ids array
            expr("filter(external_ids, x -> x.type = 'grid')[0].preferred").alias("grid_id"),
            
            # ROR ID is the main ID
            col("id").alias("ror_id"),
            
            # Extract Wikidata ID from external_ids array
            concat(
                lit("https://www.wikidata.org/wiki/"),
                expr("filter(external_ids, x -> x.type = 'wikidata')[0].all[0]")
            ).alias("wikidata_id"),
            
            # No merge info for new records
            lit(None).cast("long").alias("merge_into_id"),
            
            # Extract location details
            col("locations")[0]["geonames_details"]["name"].alias("city"),
            col("locations")[0]["geonames_details"]["country_subdivision_name"].alias("region"),
            col("locations")[0]["geonames_details"]["country_name"].alias("country"),
            col("locations")[0]["geonames_id"].alias("geonames_city_id"),
            
            # No image data in ROR
            lit(None).cast("string").alias("image_url"),
            lit(None).cast("string").alias("image_thumbnail_url"),
            
            # Default merge date
            lit("-").alias("merge_into_date"),

            # Add ROR type field (first value from types array)
            col("types")[0].alias("type"),

            # Extract acronyms from names array
            expr("transform(filter(names, x -> array_contains(x.types, 'acronym')), y -> y.value)").alias("display_name_acronyms"),
            
            # Extract alternative names (aliases)
            expr("transform(filter(names, x -> array_contains(x.types, 'alias')), y -> y.value)").alias("display_name_alternatives"),
            
            # Timestamps
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        )
    )

@dlt.table(
    name="institutions_with_ror",
    table_properties={"quality": "silver"}
)
def institutions_with_ror():
    base_df = dlt.read_stream("base_institutions")
    ror_df = spark.table("openalex.institutions.ror")
    
    return (
        base_df
        .join(ror_df, base_df.ror_id == ror_df.id, "left")
        .select(
            base_df["*"],
            ror_df["types"][0].alias("type"),
            # Extract acronyms from names array
            expr("transform(filter(names, x -> array_contains(x.types, 'acronym')), y -> y.value)").alias("display_name_acronyms"),
            
            # Extract alternative names (aliases)
            expr("transform(filter(names, x -> array_contains(x.types, 'alias')), y -> y.value)").alias("display_name_alternatives"),
        )
    )

@dlt.table(
    name="institutions",
    table_properties={"quality": "gold"}
)
def institutions():
    existing_institutions = dlt.read_stream("institutions_with_ror")
    new_institutions = dlt.read_stream("new_ror_institutions")
    
    return existing_institutions.unionByName(new_institutions, allowMissingColumns=True)
