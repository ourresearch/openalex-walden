# Databricks notebook source
# MAGIC %md
# MAGIC # Clean stored location abstracts through the current shared cleaner (oxjob #2689 phase 2)
# MAGIC
# MAGIC A source's `*_works` table keeps each record's abstract as the cleaner of its ingest day left it. New records
# MAGIC get dlt_utils 0.3.50 (`text_clean.space_sentence_joins`: 'response.Starting' -> 'response. Starting'), but the
# MAGIC stock only changes at a full refresh. This notebook builds, for ONE source, the rows whose stored abstract the
# MAGIC current cleaner changes, with the new abstract and inverted index from `udf_abstract_features` (the function the
# MAGIC ingest's enrich step calls, so text and index derive from byte-identical input, oxjob #191.1). It writes only its
# MAGIC own build tables; a separate, guarded MERGE (oxjob #2689 work/) applies them to the source table.
# MAGIC
# MAGIC Output `{build_table}`: native_id, old_h (xxhash64 of the stored abstract), old_len, old_aii_h, abstract,
# MAGIC abstract_inverted_index. `{build_table}_stage`: the gated rows with their stored abstract, inverted index and
# MAGIC updated_date (the revert copy). `{build_table}_chunks`: the ledger (a killed run resumes).

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.50-py3-none-any.whl

# COMMAND ----------

import time

from pyspark.sql import functions as F

from openalex.dlt.transform import udf_abstract_features

SOURCES = {
    "pdf": "openalex.pdf.pdf_works",
    "landing_page": "openalex.landing_page.landing_page_works",
    "repo": "openalex.repo.repo_works",
    "crossref": "openalex.crossref.crossref_works",
    "datacite": "openalex.datacite.datacite_works",
    "pubmed": "openalex.pubmed.pubmed_works",
}
dbutils.widgets.dropdown("source", "pdf", list(SOURCES), "Source")
dbutils.widgets.text("build_table", "", "Build table (required)")
dbutils.widgets.text("chunks", "8", "Chunks")
SOURCE = dbutils.widgets.get("source")
SOURCE_TABLE = SOURCES[SOURCE]
BUILD_TABLE = dbutils.widgets.get("build_table").strip()
assert BUILD_TABLE.startswith("openalex.transient."), "build into openalex.transient only"
STAGE_TABLE, LEDGER_TABLE = f"{BUILD_TABLE}_stage", f"{BUILD_TABLE}_chunks"
N_CHUNKS = int(dbutils.widgets.get("chunks"))

# The gate of notebooks/maintenance/CleanAbstractsBackfill, byte for byte (copied 28 Nov 2026 from walden 3daa4d9c): broad
# on purpose; the cleaner decides, and only rows whose text changes are kept.
GATE_ENTITY = r"&[A-Za-z][A-Za-z0-9]*;|&#[0-9]+;|&#[xX][0-9A-Fa-f]+;"
GATE_TAG = r"<(/[a-zA-Z]|[a-zA-Z])[^>]*>"
GATE_MOJIBAKE = (
    "[\\u00c2-\\u00df][\\u0080-\\u00bf]"
    "|[\\u00e0-\\u00ef][\\u0080-\\u00bf]{2}"
    "|[\\u00f0-\\u00f4][\\u0080-\\u00bf]{3}"
)
GATE_HEADING = r"^\s*(?i:a ?bstract)"
GATE_WRAP = r"\n[\s\p{Z}\x{85}\x{1C}-\x{1F}]*[,:;.?!)\]]|[(\[][\s\p{Z}\x{85}\x{1C}-\x{1F}]*\n"
GATE_GLUE = (
    r'[\p{L}\p{N})\]%"”’»]\p{M}*[.?!]["”’»)]?\p{Lu}'
    r'|(?iu:Abstract|Background|Backgrounds|Objective|Objectives|Aim|Aims|Purpose|Purposes|Method|Methods|Methodology|Result|Results|Conclusion|Conclusions|Introduction|Discussion|Findings|Design|Setting|Settings|Participants|Patients|Intervention|Interventions|Measurements|Significance|Limitations|Implications|Context|Highlights|Summary|Keywords|Resumen|Resumo|Objetivo|Objetivos|Metodología|Metodologia|Método|Métodos|Resultados|Conclusión|Conclusiones|Conclusão|Conclusões|Introducción|Introdução|Contexte|Méthodes|Résultats|Hintergrund|Methoden|Ergebnisse|Schlussfolgerung|Schlussfolgerungen|Zusammenfassung|Fazit|Résumé)\p{M}*:\p{L}'
    r'|(Background|Objectives?|Aims?|Purpose|Methods|Methodology|Results|Conclusions?|Introduction|Discussion|Findings|Highlights)[A-Z][a-z]'
)
GATE = f"({GATE_ENTITY})|({GATE_TAG})|({GATE_MOJIBAKE})|({GATE_HEADING})|({GATE_WRAP})|({GATE_GLUE})"
print(f"source {SOURCE_TABLE} -> {BUILD_TABLE} ({N_CHUNKS} chunks)")

# COMMAND ----------

# Step 1: one gated scan of the source into the stage (the revert copy: the stored abstract, index and updated_date).
if spark.catalog.tableExists(STAGE_TABLE):
    print(f"stage exists: {spark.table(STAGE_TABLE).count():,} rows, skipping the scan")
else:
    t0 = time.time()
    (
        spark.table(SOURCE_TABLE)
        .filter(F.col("abstract").isNotNull() & F.col("abstract").rlike(GATE))
        .select("native_id", "abstract", "abstract_inverted_index", "updated_date")
        .write.format("delta").mode("overwrite").saveAsTable(STAGE_TABLE)
    )
    print(f"staged {spark.table(STAGE_TABLE).count():,} rows in {time.time() - t0:,.0f}s")

# COMMAND ----------

# Step 2: clean each chunk through the shipped library; keep rows whose text changes, with a non-null text and index.
spark.sql(f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} "
          "(chunk INT, rows_in BIGINT, rows_changed BIGINT, seconds DOUBLE, finished_at TIMESTAMP)")
done = {r.chunk for r in spark.table(LEDGER_TABLE).select("chunk").collect()}
for chunk in range(N_CHUNKS):
    if chunk in done:
        print(f"[chunk {chunk + 1}/{N_CHUNKS}] done, skipping")
        continue
    t0 = time.time()
    part = spark.table(STAGE_TABLE).filter(F.pmod(F.xxhash64("native_id"), F.lit(N_CHUNKS)) == chunk)
    rows_in = part.count()
    (
        part.withColumn("_f", udf_abstract_features(F.col("abstract")))
        .select(
            "native_id",
            F.xxhash64("abstract").alias("old_h"),
            F.length("abstract").alias("old_len"),
            F.xxhash64("abstract_inverted_index").alias("old_aii_h"),
            F.col("abstract").alias("_old"),
            F.col("_f.abstract").alias("abstract"),
            F.col("_f.abstract_inverted_index").alias("abstract_inverted_index"),
        )
        .filter(F.col("abstract").isNotNull() & F.col("abstract_inverted_index").isNotNull()
                & (F.col("abstract") != F.col("_old")))
        .drop("_old")
        .withColumn("chunk", F.lit(chunk))
        .write.format("delta")
        .mode("append" if spark.catalog.tableExists(BUILD_TABLE) else "overwrite")
        .saveAsTable(BUILD_TABLE)
    )
    rows_changed = spark.table(BUILD_TABLE).filter(F.col("chunk") == chunk).count()
    seconds = time.time() - t0
    spark.sql(f"INSERT INTO {LEDGER_TABLE} VALUES ({chunk}, {rows_in}, {rows_changed}, {seconds}, current_timestamp())")
    print(f"[chunk {chunk + 1}/{N_CHUNKS}] in={rows_in:,} changed={rows_changed:,} ({seconds:,.0f}s)")

# COMMAND ----------

staged = spark.table(STAGE_TABLE).count()
built = spark.table(BUILD_TABLE).count()
keys = spark.table(BUILD_TABLE).select("native_id").distinct().count()
print(f"staged {staged:,}; changed {built:,} ({100.0 * built / max(staged, 1):.1f}%); distinct native_id {keys:,}")
assert keys == built, "a native_id appears twice in the build"
