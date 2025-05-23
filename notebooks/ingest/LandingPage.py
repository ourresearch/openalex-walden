# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import time
from datetime import datetime
from time import sleep

import requests
from requests.exceptions import Timeout
import json

# COMMAND ----------

author_schema = StructType([
    StructField("name", StringType(), True),
    StructField("is_corresponding", BooleanType(), True),
    StructField("affiliations", ArrayType(
        StructType([
            StructField("name", StringType(), True)
        ])
    ), True)
])

url_schema = StructType([
    StructField("url", StringType(), True),
    StructField("content_type", StringType(), True)
])

response_schema = StructType([
    StructField("authors", ArrayType(author_schema), True),
    StructField("urls", ArrayType(url_schema), True),
    StructField("license", StringType(), True),
    StructField("version", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("had_error", BooleanType(), True)
])

# COMMAND ----------

MAX_ABSTRACT_LENGTH = 65535
MAX_AUTHOR_NAME_LENGTH = 1000
MAX_AFFILIATION_STRING_LENGTH = 1000

# COMMAND ----------

def call_parser_single(id, max_retries=3, timeout=20):
    """Individual parser call function with increased retries"""
    retries = 0
    while retries <= max_retries:
        try:
            parser_url = "http://parseland-load-balancer-667160048.us-east-1.elb.amazonaws.com/parseland/"
            response = requests.get(f"{parser_url}/{id}", timeout=timeout)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 504:
                print(f"Got 504 for {id}, retrying after delay...")
                sleep(1 * (retries + 1))
                retries += 1
                continue
            else:
                print(f"Parser returned status code {response.status_code} for {id}")
                return None
                
        except Timeout:
            print(f"Timeout for {id}, retry {retries}/{max_retries}")
            sleep(2 * (retries + 1))
            retries += 1
            continue
        except Exception as e:
            print(f"Error calling parser for {id}: {str(e)}")
            return None
    
    print(f"Max retries reached for {id}")
    return None

def process_batch_with_threadpool(ids, max_workers=30):
    """Process a batch of IDs using ThreadPool"""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {executor.submit(call_parser_single, id): id for id in ids}
        
        for future in as_completed(future_to_id):
            id = future_to_id[future]
            try:
                result = future.result(timeout=10)
                results[id] = result
            except TimeoutError:
                print(f"Task timed out for ID: {id}")
                results[id] = None
            except Exception as e:
                print(f"Error processing ID {id}: {str(e)}")
                results[id] = None
    
    return [results.get(id) for id in ids]

@F.pandas_udf(response_schema)
def parser_udf(ids: pd.Series) -> pd.Series:
    """Vectorized pandas UDF for parallel processing"""
    results = process_batch_with_threadpool(ids.tolist())
    
    processed_results = []
    for result in results:
        if result is None:
            processed_results.append({
                'authors': [],
                'urls': [],
                'license': str(''),
                'version': str(''),
                'abstract': str(''),
                'had_error': True
            })
        else:
            processed_results.append({
                'authors': result.get('authors', []) or [],
                'urls': result.get('urls', []) or [],
                'license': str(result.get('license', '')) if result.get('license') is not None else '',
                'version': str(result.get('version', '')) if result.get('version') is not None else '',
                'abstract': str(result.get('abstract', '')) if result.get('abstract') is not None else '',
                'had_error': False
            })
    
    return pd.DataFrame(processed_results)

# COMMAND ----------

@dlt.table(
    comment="Filtered taxicab data for processing",
    partition_cols=["native_id_namespace"]
)
def taxicab_filtered_new():
    return (
        spark.readStream
            .format("delta")
            .option("skipChangeCommits", "true")
            .table("openalex.taxicab.taxicab_results")
            .filter(F.col("taxicab_id").isNotNull() & F.col("content_type").contains("html"))
    )

@dlt.table(
    comment="Taxicab data enriched with parser results - permanent table to avoid re-calling API",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["native_id_namespace"]
)
def taxicab_enriched_new():
    return (
        dlt.read_stream("taxicab_filtered_new")
        .withColumn("parser_response", parser_udf(F.col("taxicab_id")))
    )

@dlt.table(
    name="landing_page_works_staged_new",
    comment="Intermediate staging table for landing page works",
    temporary=True
)
def landing_page_works_staged_new():
    return (
        dlt.read_stream("taxicab_enriched_new")
        .select(
            F.col("url").alias("native_id"),
            F.lit("url").alias("native_id_namespace"),
            F.col("parser_response.authors").alias("authors"),
            F.array(
                F.struct(
                    F.col("url").alias("id"),
                    F.lit("url").alias("namespace"),
                    F.lit("self").alias("relationship")
                ),
                F.struct(
                    F.col("native_id").alias("id"),
                    F.col("native_id_namespace").alias("namespace"),
                    F.lit(None).alias("relationship")
                ),
                F.struct(
                    F.concat(F.col("taxicab_id"), F.lit(".html.gz")).alias("id"),
                    F.lit("docs.html").alias("namespace"),
                    F.lit(None).alias("relationship")
                )
            ).alias("ids"),
            F.col("parser_response.version").alias("version"),
            F.when(
                F.col("parser_response.license") == "other-oa",
                F.lit(None)  # need to set other-oa to null due to parseland issue of too broad detection
            ).otherwise(F.col("parser_response.license")).alias("license"),
            F.when(
                F.length(F.col("parser_response.abstract")) > MAX_ABSTRACT_LENGTH, 
                F.substring(F.col("parser_response.abstract"), 1, MAX_ABSTRACT_LENGTH)
            ).otherwise(F.col("parser_response.abstract")).alias("abstract"),
            F.expr("""
                array_distinct(
                    array_union(
                        coalesce(parser_response.urls, array()),
                        array_union(
                            CASE WHEN url IS NOT NULL THEN 
                                array(struct(url as url, 'html' as content_type))
                            ELSE array()
                            END,
                            CASE WHEN resolved_url IS NOT NULL THEN 
                                array(struct(resolved_url as url, 'html' as content_type))
                            ELSE array()
                            END
                        )
                    )
                )
            """).alias("urls"),
            F.when(
                F.col("parser_response.license").isNotNull() & 
                F.lower(F.col("parser_response.license")).like("%cc%"),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("is_oa"),
            F.current_timestamp().alias("updated_date"),
            F.current_timestamp().alias("created_date"),
            F.col("parser_response.had_error").alias("had_error"),
        )
    )

# COMMAND ----------

@dlt.view(
    name="landing_page_backfill",
    comment="Landing page backfill view"
)
def landing_page_backfill():
    return (
        spark.readStream
        .format("delta")
        .option("skipChangeCommits", "true")
        .table("openalex.landing_page.landing_page_works_backfill")
        .withColumn("license", 
            F.when(
                F.col("license") == "other-oa",
                F.lit(None)
            ).otherwise(F.col("license"))
        )
    )

@dlt.table(
    name="landing_page_combined_new",
    comment="Combined data from staged and backfill sources"
)
def landing_page_combined_new():
    staged_data = dlt.read_stream("landing_page_works_staged_new")
    backfill_data = dlt.read_stream("landing_page_backfill")
    return staged_data.unionByName(backfill_data, allowMissingColumns=True)

# COMMAND ----------

dlt.create_target_table(
    name="landing_page_works",
    comment="Final landing page works table with unique records",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "gold"
    }
)

dlt.apply_changes(
    target="landing_page_works",
    source="landing_page_combined_new",
    keys=["native_id"],
    sequence_by="updated_date",
    ignore_null_updates=True
)
