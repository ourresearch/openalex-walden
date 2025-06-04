# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.2.1-py3-none-any.whl

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *
from nameparser import HumanName # Will be installed via pipeline libraries
import re
import unicodedata
from functools import reduce
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------
# Final output table name, equivalent to current 'openalex.works.sources_combined'
# or your chosen name for the parallel run (e.g., 'locations_streamed')
FINAL_OUTPUT_TABLE_NAME = "locations_parsed" # Or "locations_streamed"

UPSTREAM_SOURCES = {
    "crossref": "openalex.crossref.crossref_works",
    "datacite": "openalex.datacite.datacite_works",
    "pdf": "openalex.pdf.pdf_works",
    "pubmed": "openalex.pubmed.pubmed_works",
    "repo": "openalex.repo.repo_works",
    "repo_backfill": "openalex.repo.repo_works_backfill",
    "mag": "openalex.mag.mag_works",
    "landing_page": "openalex.landing_page.landing_page_works"
}
MERGE_COLUMN_NAME = "merge_key"

# Walden Schema definition (as per your Locations.py)
# This is the target schema for the first major normalization step.
# It does NOT initially contain authors_exist or authors.author_key as per your notebook.
# These will be added in subsequent transformations.
walden_schema = StructType([
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
    StructField("type", StringType(), True), StructField("version", StringType(), True),
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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions

# COMMAND ----------

# --- last_name_only UDF and its helpers (VERBATIM) ---
def remove_latin_characters(author):
    if author and any("\u0080" <= c <= "\u02AF" for c in author):
        author = (
            unicodedata.normalize("NFKD", author)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    return author

def remove_author_prefixes(author):
    if not author: return ""
    prefixes = ["None ", "Array "]
    for prefix in prefixes:
        if author.startswith(prefix):
            author = author.replace(prefix, "")
    return author

def clean_author_name(author_name):
    if not author_name: return ""
    return re.sub(r"[ \-‐.'' ́>]", "", author_name).strip()

def last_name_only(author): 
    if not author:
        return ["", "", ""] 
    author = remove_latin_characters(author)
    author = remove_author_prefixes(author)
    author_name_obj = HumanName(author) 
    first_name = clean_author_name(author_name_obj.first)
    last_name = clean_author_name(author_name_obj.last)
    first_initial = first_name[0] if first_name else ""
    return [ f"{last_name};{first_initial}", f"{first_name}", f"{last_name}" ]

# Schema for the enriched author struct (output of the author processing Pandas UDF)
# This MUST match the structure of the dictionaries returned by the Pandas UDF's internal logic
# AND the walden_schema's authors element type plus author_key.
enriched_author_struct_type = StructType([
    StructField("given", StringType(), True),
    StructField("family", StringType(), True),
    StructField("name", StringType(), True),
    StructField("orcid", StringType(), True),
    StructField("affiliations", walden_schema["authors"].dataType.elementType["affiliations"].dataType, True), 
    StructField("is_corresponding", BooleanType(), True), 
    StructField("author_key", StringType(), True)
])

@F.pandas_udf(ArrayType(enriched_author_struct_type))
def udf_last_name_only(authors_arrays_series: pd.Series) -> pd.Series: # Name matches your original UDF variable
    results = []
    for author_list_for_single_record in authors_arrays_series:
        if author_list_for_single_record is None:
            results.append(None)
            continue
        
        processed_author_list = []
        for author_dict in author_list_for_single_record:
            if author_dict is None:
                processed_author_list.append(None) 
                continue

            name_str_for_parser = author_dict.get("name")
            if not name_str_for_parser:
                given_original = author_dict.get("given", "") or "" 
                family_original = author_dict.get("family", "") or ""
                name_str_for_parser = f"{given_original} {family_original}".strip()
            
            parsed_name_components = ["", "", ""] 
            if name_str_for_parser:
                try:
                    # Calling YOUR original last_name_only Python function
                    parsed_name_components = last_name_only(name_str_for_parser) 
                except Exception: 
                    pass 

            new_given = parsed_name_components[1]
            new_family = parsed_name_components[2]
            reconstructed_name = author_dict.get("name")
            if not reconstructed_name and (new_given or new_family):
                reconstructed_name = f"{new_given or ''} {new_family or ''}".strip()
            
            is_corresponding_val = author_dict.get("is_corresponding")
            is_corresponding_bool = None
            if isinstance(is_corresponding_val, str):
                is_corresponding_bool = is_corresponding_val.lower() == 'true'
            elif isinstance(is_corresponding_val, bool):
                is_corresponding_bool = is_corresponding_val
            
            processed_author_struct = {
                "given": new_given, "family": new_family, "name": reconstructed_name,
                "orcid": author_dict.get("orcid"), 
                "affiliations": author_dict.get("affiliations"), 
                "is_corresponding": is_corresponding_bool,
                "author_key": parsed_name_components[0].lower() if parsed_name_components and parsed_name_components[0] else None
            }
            processed_author_list.append(processed_author_struct)
        results.append(processed_author_list)
    return pd.Series(results)


def f_generate_inverted_index(abstract_string_input): 
    import re 
    import json 
    from collections import OrderedDict 
    
    if not abstract_string_input or not isinstance(abstract_string_input, str): 
        return None
    
    abstract_s = abstract_string_input
    
    # MODIFIED: Combined regex pattern directly in the re.sub call (inline)
    # Replaces newlines, tabs, JATS opening/closing tags, <p>, </p> tags with a single space.
    # The \b replacement was removed as its intent was unclear and potentially problematic.
    abstract_s = re.sub(
        r"\n|\t|<jats:[^>]*?>|</jats:[^>]*?>|<p>|</p>", # Inline regex string
        " ", 
        abstract_s
    )
    
    # Consolidate multiple spaces and strip
    abstract_s = " ".join(abstract_s.split()).strip()

    if not abstract_s: 
        return None

    invertedIndex = OrderedDict()
    words = abstract_s.split()
    for i, word in enumerate(words):
        if word not in invertedIndex: 
            invertedIndex[word] = []
        invertedIndex[word].append(i)
    
    return json.dumps(invertedIndex, ensure_ascii=False) if invertedIndex else None

@F.pandas_udf(StringType())
def udf_f_generate_inverted_index(abstract_series: pd.Series) -> pd.Series: # Name matches your original UDF variable
    # This Pandas UDF calls your 'f_generate_inverted_index' Python function
    return abstract_series.apply(f_generate_inverted_index)

def transform_struct(col_name, source_struct, target_struct):
    target_fields = {f.name: f for f in target_struct.fields}
    source_fields = {f.name: f for f in source_struct.fields}
    expressions = []
    for field_name, field in target_fields.items():
        if field_name in source_fields:
            if isinstance(field.dataType, StructType):
                expressions.append(
                    transform_struct(f"{col_name}.{field_name}", source_fields[field_name].dataType, field.dataType)
                )
            else:
                expressions.append(F.col(f"{col_name}.{field_name}").cast(field.dataType).alias(field_name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field_name))
    return F.struct(*expressions).alias(col_name)

def transform_array_of_structs(col_name, source_array, target_array):
    target_fields = target_array.elementType.fields
    source_fields = {f.name: f for f in source_array.elementType.fields}
    struct_fields_expr = []
    for field in target_fields:
        if field.name in source_fields:
            if isinstance(field.dataType, StructType):
                nested_expr = transform_struct("x." + field.name, source_fields[field.name].dataType, field.dataType)
                struct_fields_expr.append(f"{nested_expr} AS {field.name}")
            else:
                struct_fields_expr.append(f"x.{field.name} AS {field.name}")
        else:
            struct_fields_expr.append(f"CAST(NULL AS STRING) AS {field.name}")
    struct_expr = f"STRUCT({', '.join(struct_fields_expr)})"
    return F.expr(f"TRANSFORM({col_name}, x -> {struct_expr})").alias(col_name)


def align_column(col_name, source_type, target_type):
    if isinstance(target_type, StructType) and isinstance(source_type, StructType):
        return transform_struct(col_name, source_type, target_type)
    elif isinstance(target_type, ArrayType) and isinstance(source_type, ArrayType):
        if isinstance(target_type.elementType, StructType) and isinstance(source_type.elementType, StructType):
            return transform_array_of_structs(col_name, source_type, target_type)
        else:
            return F.col(col_name).cast(target_type)
    else:
        return F.col(col_name).cast(target_type)

def apply_walden_schema(df, schema):
    schema_fields = {field.name: field for field in schema.fields}
    source_schema_fields = {field.name: field for field in df.schema.fields}
    aligned_columns = []
    for col_name, target_field in schema_fields.items():
        if col_name in source_schema_fields:
            aligned_columns.append(align_column(col_name, source_schema_fields[col_name].dataType, target_field.dataType))
        else:
            aligned_columns.append(F.lit(None).cast(target_field.dataType).alias(col_name))
    return df.select(*aligned_columns)

# --- DOI Normalization (Spark native version for DLT) ---
def normalize_doi_spark_col(doi_string_col_expr):
    return F.regexp_replace(
        F.regexp_extract(
            F.lower(F.trim(F.regexp_replace(doi_string_col_expr, " ", ""))),
            r"(10\.\d+/[^\s]+)", 1),
        r"\u0000", "")

# --- create_merge_column and clean_native_id (VERBATIM from your "Locations Parsed" DLT snippet) ---
def clean_native_id(df, column_name="native_id"):
    return (
        df.withColumn("true_native_id", F.col(column_name))
        .withColumn(column_name, F.regexp_replace(F.col(column_name), r"https?://", ""))
        .withColumn(column_name, F.regexp_replace(F.col(column_name), r"/+$", ""))
        .withColumn(column_name, F.regexp_replace(F.col(column_name), r"[^a-zA-Z0-9./:]", ""))
        .withColumn(column_name, F.lower(F.col(column_name))) )

def create_merge_column(df): 
    df_cleaned = clean_native_id(df, "native_id") 
    df_cleaned = df_cleaned.withColumn("title_cleaned_newline", F.regexp_replace(F.col("title"), "\n", " "))
    return df_cleaned.withColumn(MERGE_COLUMN_NAME,
        F.struct(
            F.element_at(F.expr("filter(ids, x -> x.namespace = 'doi' and x.id is not null)"), 1).getField("id").alias("doi"),
            F.element_at(F.expr("filter(ids, x -> x.namespace = 'pmid' and x.id is not null)"), 1).getField("id").alias("pmid"),
            F.element_at(F.expr("filter(ids, x -> x.namespace = 'arxiv' and x.id is not null)"), 1).getField("id").alias("arxiv"),
            F.when(
                (F.expr(f"title_cleaned_newline in (select title from openalex.system.bad_titles)")) |
                (F.length(F.col("title_cleaned_newline")) < 19) |
                (F.col("title_cleaned_newline").isNull()),
                F.concat(F.col("native_id"), F.col("provenance")) 
            ).when(F.col("authors_exist") == False, F.col("normalized_title")
            ).otherwise(F.concat_ws("_", F.col("normalized_title"), F.col("authors").getItem(0).getField("author_key"))
            ).alias("title_author")
        )).drop("title_cleaned_newline")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Pipeline Definition

# COMMAND ----------



# COMMAND ----------

# DLT Pipeline Definition using Correctly Named Pandas UDFs
# This section assumes all OTHER helper Python functions (apply_walden_schema, create_merge_column, etc.)
# and configuration variables (UPSTREAM_SOURCES_TABLES, FINAL_OUTPUT_TABLE_NAME, 
# MERGE_COLUMN_NAME, BAD_TITLES_TABLE, walden_schema) are defined in preceding cells VERBATIM.
# The Pandas UDFs 'udf_last_name_only' and 'udf_f_generate_inverted_index' 
# are defined as in Window 1 above.

# --- Factory function to define the DLT flow for a single source ---
def define_full_dlt_flow_for_source(source_key, source_table_fqn):

    # --- Stage 1: Walden Schema Alignment & Initial DOI Normalization (Temporary) ---
    def factory_stage1_initial_processing():
        @dlt.table(
            name=f"TEMP_{source_key}_walden_doi_normalized", 
            temporary=True, 
            comment=f"Walden aligned & DOI normalized data for {source_key}"
        )
        def initial_processed_stream_for_source(): 
            df = spark.readStream.option("skipChangeCommits", "true").table(source_table_fqn)
            df = df.withColumn("provenance", F.lit(source_key))

            if source_key == "repo_backfill":
                df = df.withColumn("ids", F.when(
                    (F.col("native_id").like("oai:arXiv.org:%")) & (F.expr("exists(ids, x -> x['namespace'] = 'doi')") == False),
                    F.array_union(F.col("ids"), F.array(F.struct(
                        F.regexp_replace(F.col("native_id"), "oai:arXiv.org:", "10.48550/arxiv.").alias("id"),
                        F.lit("doi").alias("namespace"), F.lit("self").alias("relationship"))))
                ).otherwise(F.col("ids")))
            df_aligned = apply_walden_schema(df, walden_schema)

            if source_key == "landing_page":
                df_aligned = df_aligned.withColumn("authors",
                    F.transform( F.col("authors"), lambda auth_s: F.struct(
                        auth_s.getItem("given").alias("given"), auth_s.getItem("family").alias("family"),
                        auth_s.getItem("name").alias("name"), auth_s.getItem("orcid").alias("orcid"),
                        F.transform( auth_s.getItem("affiliations"), lambda aff_s: F.struct(
                            aff_s.getItem("name").alias("name"), 
                            F.lit(None).cast("string").alias("department"), 
                            F.lit(None).cast("string").alias("ror_id"))
                        ).alias("affiliations"), 
                        auth_s.getItem("is_corresponding").cast(BooleanType()).alias("is_corresponding"))))
            df_doi_normalized = df_aligned.withColumns({
                "native_id": F.when(F.col("native_id_namespace") == "doi", normalize_doi_spark_col(F.col("native_id"))).otherwise(F.col("native_id")),
                "ids": F.transform(F.col("ids"), lambda x: F.struct(
                    F.when(x.getItem("namespace") == "doi", normalize_doi_spark_col(x.getItem("id"))).otherwise(x.getItem("id")).alias("id"),
                    x.getItem("namespace").alias("namespace"), x.getItem("relationship").alias("relationship")))})
        
            return df_doi_normalized
        
        return initial_processed_stream_for_source
    
    factory_stage1_initial_processing()

    # --- Stage 2: Locations Enriched (Published) - USING PANDAS UDFs ---
    def factory_stage2_locations_enriched():
        @dlt.table(name=f"{source_key}_locations_enriched", 
                   temporary=True, 
                   comment=f"Feature-enriched locations for {source_key} using Pandas UDFs")
        def locations_enriched_for_source():
            df_normalized_stream = dlt.read_stream(f"TEMP_{source_key}_walden_doi_normalized")
            
            # Apply the Pandas UDF udf_last_name_only (which processes the whole authors array)
            df_with_enriched_authors = df_normalized_stream.withColumn(
                "authors", 
                udf_last_name_only(F.col("authors")) # USING THE PANDAS UDF (same name as old UDF)
            )
            
            df_final_enriched = df_with_enriched_authors.withColumns({
                # Apply the Pandas UDF udf_f_generate_inverted_index
                "abstract_inverted_index": udf_f_generate_inverted_index(F.col("abstract")), # USING THE PANDAS UDF
                "authors_exist": F.expr("concat_ws('', authors[0].given, authors[0].family, authors[0].name) != ''"),
                "affiliations_exist": F.expr("exists(authors, author -> nvl(size(author.affiliations), 0) > 0 AND exists(author.affiliations, aff -> aff.name is not null or aff.department is not null or aff.ror_id is not null))"),
                "is_corresponding_exists": F.exists(F.col("authors"), lambda x: x.getItem("is_corresponding") == True), 
                "best_doi": F.when(F.col("native_id_namespace") == "doi", F.col("native_id")).otherwise(F.element_at(F.expr("filter(ids, id_struct -> id_struct.namespace == 'doi')"), 1).getField("id"))})
            return df_final_enriched
        return locations_enriched_for_source
    factory_stage2_locations_enriched()

    # --- Stage 3: Create <source>_with_keys (Published) ---
    def factory_stage3_with_keys():
        @dlt.table(name=f"{source_key}_with_keys",
                   temporary=True,
                   comment=f"Locations for {source_key} with {MERGE_COLUMN_NAME} generated.")
        def with_keys_for_source():
            df_enriched = dlt.read_stream(f"{source_key}_locations_enriched")
            return create_merge_column(df_enriched)
        return with_keys_for_source
    factory_stage3_with_keys()

    # --- Stage 4: Create <source>_filtered (Published) ---
    def factory_stage4_filtered():
        @dlt.table(name=f"{source_key}_filtered",
                   temporary=True,
                   comment=f"Filtered locations for {source_key}.")
        def filtered_for_source():
            df_with_keys = dlt.read_stream(f"{source_key}_with_keys")
            return df_with_keys.filter(F.expr( 
                f"({MERGE_COLUMN_NAME}.doi is not null) or ({MERGE_COLUMN_NAME}.pmid is not null) or " +
                f"({MERGE_COLUMN_NAME}.arxiv is not null) or (({MERGE_COLUMN_NAME}.title_author is not null and {MERGE_COLUMN_NAME}.title_author <> ''))"))
        return filtered_for_source
    
    factory_stage4_filtered()

# COMMAND ----------

# --- Call the main factory for each source to define all its DLT tables ---
for source_key_main_loop, source_table_fqn_main_loop in UPSTREAM_SOURCES.items():
    define_full_dlt_flow_for_source(source_key_main_loop, source_table_fqn_main_loop)

# COMMAND ----------

# --- Final Union Stage (Published) ---
@dlt.table(name=FINAL_OUTPUT_TABLE_NAME, comment="Final unified locations table.")
def final_union_step():
    streams_to_union = [dlt.read_stream(f"{key}_filtered") for key in UPSTREAM_SOURCES.keys()]
    if not streams_to_union: raise ValueError("No DLT streams for union.")
    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=False), streams_to_union)
