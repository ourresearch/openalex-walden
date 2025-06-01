from .normalize import *

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

def apply_initial_processing(df_input, source_key, target_walden_schema):
    """
    Applies Walden schema alignment, source-specific fixes, and DOI normalization.
    Relies on apply_walden_schema and normalize_doi_spark_col from utils.
    """
    df = df_input.withColumn("provenance", F.lit(source_key))

    if source_key == "repo_backfill":
        df = df.withColumn("ids", F.when(
            (F.col("native_id").like("oai:arXiv.org:%")) & (F.expr("exists(ids, x -> x['namespace'] = 'doi')") == False),
            F.array_union(F.col("ids"), F.array(F.struct(
                F.regexp_replace(F.col("native_id"), "oai:arXiv.org:", "10.48550/arxiv.").alias("id"),
                F.lit("doi").alias("namespace"), F.lit("self").alias("relationship"))))
        ).otherwise(F.col("ids")))
    
    df_aligned = apply_walden_schema(df, target_walden_schema) 

    if source_key == "landing_page": 
        df_aligned = df_aligned.withColumn("authors",
            F.transform( F.col("authors"), lambda auth_s: F.struct(
                auth_s.getItem("given").alias("given"), auth_s.getItem("family").alias("family"),
                auth_s.getItem("name").alias("name"), auth_s.getItem("orcid").alias("orcid"),
                F.transform( auth_s.getItem("affiliations"), lambda aff_s: F.struct(
                    aff_s.getItem("name").alias("name"), 
                    F.lit(None).cast(StringType()).alias("department"), 
                    F.lit(None).cast(StringType()).alias("ror_id"))
                ).alias("affiliations"), 
                auth_s.getItem("is_corresponding").cast(BooleanType()).alias("is_corresponding"))))
    
    df_doi_normalized = df_aligned.withColumns({
        "native_id": F.when(F.col("native_id_namespace") == "doi", normalize_doi_spark_col(F.col("native_id"))).otherwise(F.col("native_id")),
        "ids": F.transform(F.col("ids"), lambda x: F.struct(
            F.when(x.getItem("namespace") == "doi", normalize_doi_spark_col(x.getItem("id"))).otherwise(x.getItem("id")).alias("id"),
            x.getItem("namespace").alias("namespace"), x.getItem("relationship").alias("relationship")))})
    return df_doi_normalized


def enrich_with_features_and_author_keys(df_input, 
                                         # Assuming Pandas UDFs are defined in normalize.py or parsing.py and imported
                                         # For example, if udf_last_name_only is the Pandas UDF for author arrays
                                         # and udf_f_generate_inverted_index is the Pandas UDF for abstracts
                                         ):
    """
    Enriches the DataFrame with author keys (using a Pandas UDF for author name parsing)
    and other derived features like abstract_inverted_index, authors_exist, etc.
    """
    # This function now expects that the udf_last_name_only Pandas UDF is defined
    # (e.g., in utils.normalize or utils.parsing) and imported into this transform module.
    # Or, the DLT notebook itself defines the Pandas UDFs and passes them as arguments here.
    # For simplicity, let's assume they are imported and available.
    
    df_with_enriched_authors = df_input.withColumn(
        "authors", 
        udf_last_name_only(F.col("authors")) # This udf_last_name_only is THE PANDAS UDF
    )
    
    df_final_enriched = df_with_enriched_authors.withColumns({
        "abstract_inverted_index": udf_f_generate_inverted_index(F.col("abstract")), # PANDAS UDF
        "authors_exist": F.expr("concat_ws('', authors[0].given, authors[0].family, authors[0].name) != ''"),
        "affiliations_exist": F.expr("exists(authors, author -> nvl(size(author.affiliations), 0) > 0 AND exists(author.affiliations, aff -> aff.name is not null or aff.department is not null or aff.ror_id is not null))"),
        "is_corresponding_exists": F.exists(F.col("authors"), lambda x: x.getItem("is_corresponding") == True), 
        "best_doi": F.when(F.col("native_id_namespace") == "doi", F.col("native_id")).otherwise(F.element_at(F.expr("filter(ids, id_struct -> id_struct.namespace == 'doi')"), 1).getField("id"))
    })
    return df_final_enriched


def apply_final_merge_key_and_filter(df_enriched_input):
    MERGE_COL = "merge_key"

    """
    Applies the create_merge_column logic and the final filtering logic.
    Assumes create_merge_column is available (e.g. imported from utils.dataframe or defined in normalize).
    """
    df_with_merge_key = create_merge_column(df_enriched_input) 

    return df_with_merge_key.filter(F.expr( 
        f"({MERGE_COL}.doi is not null) or " +
        f"({MERGE_COL}.pmid is not null) or " +
        f"({MERGE_COL}.arxiv is not null) or " +
        f"(({MERGE_COL}.title_author is not null and {MERGE_COL}.title_author <> ''))"
    ))
