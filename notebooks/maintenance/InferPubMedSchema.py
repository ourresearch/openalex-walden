# Databricks notebook source
# MAGIC %md
# MAGIC # Capture canonical PubMed XML schema (one-off)
# MAGIC
# MAGIC Produces a single JSON file capturing the full schema for `pubmed_items`,
# MAGIC with the `AbstractText` field patched to the correct
# MAGIC `ARRAY<STRUCT<_Label, _NlmCategory, _VALUE, sub, sup, i, b, u>>` shape.
# MAGIC
# MAGIC The output is committed to `libraries/dlt_utils/openalex/dlt/schemas/pubmed_items.json`
# MAGIC and used as a fixed schema by `notebooks/ingest/PubMed.py`, replacing the
# MAGIC fragile `schemaHints` + `schemaLocation` block (oxjob #142).
# MAGIC
# MAGIC ## Approach
# MAGIC
# MAGIC By default this notebook reads the existing `openalex.pubmed.pubmed_items`
# MAGIC schema and patches just the broken `AbstractText` field. This is fast (no
# MAGIC S3 reads) and captures every field Auto Loader has ever inferred over the
# MAGIC pipeline's lifetime — far more comprehensive than any one-shot inference.
# MAGIC
# MAGIC There's an optional second mode (`MODE = "infer"`) that re-infers from a
# MAGIC fresh S3 sample. Use that only if you suspect the existing table schema is
# MAGIC stale or missing recent fields.
# MAGIC
# MAGIC ## How to run
# MAGIC
# MAGIC 1. Attach to any cluster that can read Unity Catalog tables and S3.
# MAGIC 2. Run all cells.
# MAGIC 3. Copy the printed JSON (cell 5) into
# MAGIC    `libraries/dlt_utils/openalex/dlt/schemas/pubmed_items.json` in the repo,
# MAGIC    or download from the Volume path.

# COMMAND ----------

import json
from pyspark.sql.types import (
    StructType, StructField, ArrayType, StringType, DataType,
)

# "patch" (default) = read existing pubmed_items schema and rewrite AbstractText.
# "infer" = re-infer from a fresh S3 sample (slower, less comprehensive).
MODE = "patch"

# How many S3 files to sample if MODE = "infer". 100 is a reasonable default.
INFER_SAMPLE_SIZE = 100

# Output path for the JSON schema.
OUTPUT_VOLUME_PATH = "/Volumes/openalex/default/schemas/pubmed_items_schema.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load source schema

# COMMAND ----------

if MODE == "patch":
    src_schema = spark.table("openalex.pubmed.pubmed_items").schema
    print(f"Loaded schema from openalex.pubmed.pubmed_items "
          f"({len(src_schema.fields)} top-level fields)")

elif MODE == "infer":
    all_files = [f.path for f in dbutils.fs.ls("s3a://openalex-ingest/pubmed/")
                 if f.path.endswith(".xml.gz")]
    step = max(len(all_files) // INFER_SAMPLE_SIZE, 1)
    sample = all_files[::step][:INFER_SAMPLE_SIZE]
    print(f"Inferring from {len(sample)} of {len(all_files)} files...")
    df = (
        spark.read
        .format("xml")
        .option("rowTag", "PubmedArticle")
        .option("compression", "gzip")
        .option("samplingRatio", "1.0")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(sample)
    )
    src_schema = df.schema
    print(f"Inferred from {df.count():,} records "
          f"({len(src_schema.fields)} top-level fields)")
else:
    raise ValueError(f"Unknown MODE: {MODE!r} — choose 'patch' or 'infer'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Patch AbstractText to the correct shape
# MAGIC
# MAGIC The known bug: `AbstractText` is currently typed as `STRING`, which causes
# MAGIC spark-xml to keep only the LAST element when a record has multiple
# MAGIC `<AbstractText Label="...">` children (structured abstracts).
# MAGIC
# MAGIC The corrected type captures attributes (`_Label`, `_NlmCategory`), the
# MAGIC text content (`_VALUE`), and common inline NLM markup (`sub`, `sup`, `i`,
# MAGIC `b`, `u`).

# COMMAND ----------

ABSTRACT_TEXT_TYPE = ArrayType(StructType([
    StructField("_Label", StringType(), True),
    StructField("_NlmCategory", StringType(), True),
    StructField("_VALUE", StringType(), True),
    StructField("sub", ArrayType(StringType()), True),
    StructField("sup", ArrayType(StringType()), True),
    StructField("i", ArrayType(StringType()), True),
    StructField("b", ArrayType(StringType()), True),
    StructField("u", ArrayType(StringType()), True),
]))


def patch_field(schema: StructType, path: list, new_type: DataType) -> StructType:
    """
    Walk a dotted path through nested StructTypes and replace the target
    field's dataType with new_type. All intermediate path components must
    be StructType (not ArrayType — we're patching field values, not array
    elements).
    """
    head, *rest = path
    new_fields = []
    found = False
    for f in schema.fields:
        if f.name != head:
            new_fields.append(f)
            continue
        found = True
        if not rest:
            # Replace this field's type
            new_fields.append(StructField(f.name, new_type, f.nullable, f.metadata))
        else:
            if not isinstance(f.dataType, StructType):
                raise TypeError(
                    f"Cannot recurse into {head}: type is {type(f.dataType).__name__}"
                )
            new_fields.append(StructField(
                f.name,
                patch_field(f.dataType, rest, new_type),
                f.nullable,
                f.metadata,
            ))
    if not found:
        raise KeyError(f"Field {head!r} not found at this level. "
                       f"Available: {[x.name for x in schema.fields]}")
    return StructType(new_fields)


target_path = ["MedlineCitation", "Article", "Abstract", "AbstractText"]
patched_schema = patch_field(src_schema, target_path, ABSTRACT_TEXT_TYPE)

print("BEFORE:")
print(" ", src_schema["MedlineCitation"].dataType["Article"].dataType["Abstract"]
      .dataType["AbstractText"].dataType.simpleString())
print()
print("AFTER:")
print(" ", patched_schema["MedlineCitation"].dataType["Article"].dataType["Abstract"]
      .dataType["AbstractText"].dataType.simpleString())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Round-trip check
# MAGIC
# MAGIC Confirm the JSON serialization is loadable back into a Spark schema —
# MAGIC i.e., the file we'd commit will work in `.schema(...)`.

# COMMAND ----------

schema_json = patched_schema.json()
schema_pretty = json.dumps(json.loads(schema_json), indent=2)

reloaded = StructType.fromJson(json.loads(schema_pretty))
assert reloaded == patched_schema, "Round-trip mismatch"
print(f"Round-trip OK. JSON size: {len(schema_pretty):,} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Volume

# COMMAND ----------

import os
dbutils.fs.mkdirs(os.path.dirname(OUTPUT_VOLUME_PATH))
dbutils.fs.put(OUTPUT_VOLUME_PATH, schema_pretty, overwrite=True)
print(f"Wrote {len(schema_pretty):,} chars to {OUTPUT_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Print JSON for copy-paste into the repo
# MAGIC
# MAGIC Save the output of the next cell as
# MAGIC `libraries/dlt_utils/openalex/dlt/schemas/pubmed_items.json` in the repo.

# COMMAND ----------

print(schema_pretty)
