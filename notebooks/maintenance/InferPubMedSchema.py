# Databricks notebook source
# MAGIC %md
# MAGIC # Infer PubMed XML Schema (one-off)
# MAGIC
# MAGIC Reads a sample of PubMed XML files directly from S3 to derive a comprehensive
# MAGIC inferred schema. Writes the result to JSON so it can be committed to the repo
# MAGIC and used as a fixed schema by `notebooks/ingest/PubMed.py` (replacing the
# MAGIC fragile schemaHints + schemaLocation approach).
# MAGIC
# MAGIC ## Why
# MAGIC
# MAGIC The schemaHints approach has produced three production stream-failure
# MAGIC iterations (see oxjob #142) because every undeclared nested attribute or
# MAGIC inline element causes Auto Loader to fail. A pinned schema, sourced from a
# MAGIC broad sample of real files, captures everything in one shot.
# MAGIC
# MAGIC ## How to run
# MAGIC
# MAGIC 1. Attach to any cluster with Spark XML support (DBR 14+ or any cluster the
# MAGIC    PubMed pipeline uses today).
# MAGIC 2. Set `SAMPLE_SIZE` below — `None` means all 1400+ files (slow but
# MAGIC    comprehensive). Recommended first pass: 100 files (covers wide variation
# MAGIC    in 3-5 minutes).
# MAGIC 3. Run all cells. The schema JSON is printed AND written to the Volume.
# MAGIC 4. Download `/Volumes/openalex/default/schemas/pubmed_items_schema.json`
# MAGIC    (or copy from the cell output) and commit it to
# MAGIC    `libraries/dlt_utils/openalex/dlt/schemas/pubmed_items.json` in the repo.

# COMMAND ----------

import json
from pyspark.sql.types import StructType

# How many S3 files to sample for inference. None = all files.
# 100 is fast and usually catches all common variation. Bump up to 500+ if
# you suspect missing rare elements; the schema will be a UNION across files.
SAMPLE_SIZE = 100

# Output destination for the inferred schema JSON.
OUTPUT_VOLUME_PATH = "/Volumes/openalex/default/schemas/pubmed_items_schema.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pick files

# COMMAND ----------

# List all PubMed XML files in S3 and pick a sample.
all_files = [f.path for f in dbutils.fs.ls("s3a://openalex-ingest/pubmed/")
             if f.path.endswith(".xml.gz")]
print(f"Total files in s3a://openalex-ingest/pubmed/: {len(all_files)}")

if SAMPLE_SIZE is None or SAMPLE_SIZE >= len(all_files):
    sample_files = all_files
    print(f"Using ALL {len(sample_files)} files for inference")
else:
    # Stratified pick: spread sample across the file index range to capture
    # both old (24n0001) and new (26n14xx) variants.
    step = len(all_files) // SAMPLE_SIZE
    sample_files = all_files[::max(step, 1)][:SAMPLE_SIZE]
    print(f"Using {len(sample_files)} of {len(all_files)} files (stratified by index)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the sample with full inference (no schema hints)

# COMMAND ----------

# spark-xml in batch read mode — infers the full schema across the union
# of all sampled files. No schemaHints, no schemaLocation, no Auto Loader.
df = (
    spark.read
    .format("xml")
    .option("rowTag", "PubmedArticle")
    .option("compression", "gzip")
    .option("samplingRatio", "1.0")  # use 100% of rows in each sampled file
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(sample_files)
)

print(f"Inferred from {df.count():,} PubmedArticle records")
print()
print("Top-level fields:")
for f in df.schema.fields:
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect the bug-relevant subschema

# COMMAND ----------

# Drill down to the AbstractText struct so we can verify it's correctly typed
# as ARRAY<STRUCT<...>> with all the inline-markup elements present.
abs_field = (df.select("MedlineCitation.Article.Abstract.AbstractText")
               .schema["AbstractText"])
print("AbstractText type:")
print(abs_field.dataType.simpleString())
print()
print("AbstractText element fields (if struct array):")
elem_type = abs_field.dataType.elementType
for f in elem_type.fields:
    print(f"  {f.name:30s} {f.dataType.simpleString()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write the schema JSON to the Volume

# COMMAND ----------

schema_json = df.schema.json()

# Pretty-print version for human review (committed file should use this)
schema_pretty = json.dumps(json.loads(schema_json), indent=2)

# Ensure the volume directory exists
import os
volume_dir = os.path.dirname(OUTPUT_VOLUME_PATH)
dbutils.fs.mkdirs(volume_dir)

# Write the pretty JSON
dbutils.fs.put(OUTPUT_VOLUME_PATH, schema_pretty, overwrite=True)
print(f"Wrote {len(schema_pretty):,} chars to {OUTPUT_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Print the JSON for copy-paste into the repo
# MAGIC
# MAGIC Save the output of the next cell as
# MAGIC `libraries/dlt_utils/openalex/dlt/schemas/pubmed_items.json` in the repo.

# COMMAND ----------

print(schema_pretty)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sanity round-trip
# MAGIC
# MAGIC Confirm the JSON we'd commit is loadable back into a Spark schema —
# MAGIC i.e., reproducible.

# COMMAND ----------

reloaded = StructType.fromJson(json.loads(schema_pretty))
print("Round-trip successful.")
print(f"Top-level fields: {len(reloaded.fields)}")
abs_after = reloaded["MedlineCitation"].dataType["Article"].dataType["Abstract"].dataType["AbstractText"]
print(f"AbstractText round-trips to: {abs_after.dataType.simpleString()}")
