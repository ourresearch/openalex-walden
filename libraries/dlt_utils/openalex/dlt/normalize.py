import pyspark.sql.functions as F
from pyspark.sql.types import *
from nameparser import HumanName # Will be installed via pipeline libraries

import re
import unicodedata
from functools import reduce
import pandas as pd

# Walden Schema definition (as per your Locations.py)
# This is the target schema for the first major normalization step.
# It does NOT initially contain authors_exist or authors.author_key as per your notebook.
# These will be added in subsequent transformations.
walden_works_schema = StructType([
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
# AND the walden_work_schema's authors element type plus author_key.
enriched_author_struct_type = StructType([
    StructField("given", StringType(), True),
    StructField("family", StringType(), True),
    StructField("name", StringType(), True),
    StructField("orcid", StringType(), True),
    StructField("affiliations", walden_works_schema["authors"].dataType.elementType["affiliations"].dataType, True), 
    StructField("is_corresponding", BooleanType(), True), 
    StructField("author_key", StringType(), True)
])

# --- create_merge_column and clean_native_id ---
def clean_native_id(df, column_name="native_id"):
    return (
            df.withColumn(column_name, F.regexp_replace(F.col(column_name), r"https?://", ""))
            .withColumn(column_name, F.regexp_replace(F.col(column_name), r"/+$", ""))
            .withColumn(column_name, F.regexp_replace(F.col(column_name), r"[^a-zA-Z0-9./:-]", ""))
            .withColumn(column_name, F.lower(F.col(column_name)))
    )

def create_merge_column(df, MERGE_COLUMN_NAME="merge_key"):
    return ( 
        df
            # decided together with Casey to keep only one native_id
            # can apply more deduplication cleaning if needed in later steps
            .withColumn("native_id", F.trim(F.lower(F.col("native_id"))))
            .withColumn("title_cleaned_newline", F.trim(F.regexp_replace(F.col("title"), "\n", " ")))
            .withColumn(MERGE_COLUMN_NAME,
                F.struct(
                    F.element_at(F.expr("filter(ids, x -> x.namespace = 'doi' and x.id is not null)"), 1).getField("id").alias("doi"),
                    F.element_at(F.expr("filter(ids, x -> x.namespace = 'pmid' and x.id is not null)"), 1).getField("id").alias("pmid"),
                    F.element_at(F.expr("filter(ids, x -> x.namespace = 'arxiv' and x.id is not null)"), 1).getField("id").alias("arxiv"),
                    F.when(
                        (F.expr(f"title_cleaned_newline in (select trim(title) from openalex.system.bad_titles)")) |
                        (F.length(F.col("title_cleaned_newline")) < 19) |
                        (F.col("title_cleaned_newline").isNull()),
                        F.concat(F.col("native_id"), F.col("provenance")) 
                    ).when(F.col("authors_exist") == False, F.col("normalized_title")
                    ).otherwise(F.concat_ws("_", F.col("normalized_title"), F.col("authors").getItem(0).getField("author_key"))
                    ).alias("title_author")
                )
            ).drop("title_cleaned_newline")
    )
# normalize title and types UDFs

def clean_html(raw_html):
    cleanr = re.compile('<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from|\n)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()

def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),
        
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", "publisher-specific-oa"),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "publisher-specific-oa"),
        ("arxiv.orgperpetual", "publisher-specific-oa"),
        ("arxiv.orgnonexclusive", "publisher-specific-oa"),
        
        # creative Commons licenses
        ("ccbyncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("ccby", "cc-by"),
        ("creativecommons.org/licenses/by/", "cc-by"),
        
        # public domain
        ("publicdomain", "public-domain"),
        
        # software/Dataset licenses
        ("mit ", "mit"),
        ("gpl3", "gpl-3"),
        ("gpl2", "gpl-2"),
        ("gpl", "gpl"),
        ("apache2", "apache-2.0")
    ]

    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license
    return None

# --- DOI Normalization (Spark native version for DLT - no UDF needed) ---
def normalize_doi_spark_col(doi_string_col_expr):
    return F.regexp_replace(
        F.regexp_extract(
            F.lower(F.trim(F.regexp_replace(doi_string_col_expr, " ", ""))),
            r"(10\.\d+/[^\s]+)", 1),
        r"\u0000", "")
    
@F.pandas_udf(ArrayType(enriched_author_struct_type))
def udf_last_name_only(authors_arrays_series: pd.Series) -> pd.Series:
    results = []

    for author_list in authors_arrays_series:
        if author_list is None:
            results.append(None)
            continue

        processed_list = []
        for author in author_list:
            if author is None:
                processed_list.append(None)
                continue

            # Select raw name string
            raw_name = author.get("name")
            if not raw_name:
                raw_given = author.get("given", "") or ""
                raw_family = author.get("family", "") or ""
                raw_name = f"{raw_given} {raw_family}".strip()

            # Parse components
            try:
                parsed_components = last_name_only(raw_name)
                author_key = parsed_components[0].lower() if parsed_components[0] else None
                given_clean = parsed_components[1]
                family_clean = parsed_components[2]
            except Exception:
                author_key = None
                given_clean = author.get("given", "")
                family_clean = author.get("family", "")

            # Reconstruct display name: prioritize original "name" if present
            display_name = author.get("name")
            if not display_name:
                display_name = f"{raw_given} {raw_family}".strip()

            # Normalize is_corresponding to boolean
            is_corr = author.get("is_corresponding")
            if isinstance(is_corr, str):
                is_corr = is_corr.lower() == "true"
            elif not isinstance(is_corr, bool):
                is_corr = None

            processed_author = {
                "given": given_clean,
                "family": family_clean,
                "name": display_name,
                "orcid": author.get("orcid"),
                "affiliations": author.get("affiliations"),
                "is_corresponding": is_corr,
                "author_key": author_key,
            }

            processed_list.append(processed_author)

        results.append(processed_list)

    return pd.Series(results)


@F.pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    # This Pandas UDF calls your original 'normalize_license' Python function
    return license_series.apply(normalize_license)

@F.pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    # This Pandas UDF calls your original 'normalize_title' Python function
    return title_series.apply(normalize_title)