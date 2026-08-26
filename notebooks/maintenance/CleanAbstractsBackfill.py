# Databricks notebook source
# MAGIC %md
# MAGIC # Clean the abstracts_backfill legacy table (oxjob #807)
# MAGIC
# MAGIC `CreateWorksBase` falls back to `openalex.abstracts.abstracts_backfill` (priority 997)
# MAGIC whenever no provenance supplies an abstract. **104.9M works** currently publish their
# MAGIC abstract from that table, and **3.79M of them are corrupted** (2.79M HTML entities,
# MAGIC 1.10M HTML/JATS tags, 103K mojibake — measured 2026-08-18).
# MAGIC
# MAGIC That table is a static legacy snapshot with no producer in this repo, so **no pipeline
# MAGIC refresh ever cleans it** — the #807 at-ingest cleaner and the Silver reparse both miss it
# MAGIC entirely. This notebook closes that gap.
# MAGIC
# MAGIC **Approach — a repair OVERLAY, not an in-place rewrite.** We build a small
# MAGIC `abstracts_backfill_repair` table holding only the rows whose text actually changed
# MAGIC (work_id -> cleaned abstract + rebuilt inverted index), and `CreateWorksBase` LEFT JOINs
# MAGIC it over the backfill CTE. This mirrors the `openalex.institutions.affiliation_strings_repair`
# MAGIC overlay from oxjob #801 (see walden CLAUDE.md) and buys three things a rewrite would not:
# MAGIC
# MAGIC 1. **Non-destructive** — the 273 GB / 150.6M-row source table is never touched; reverting
# MAGIC    is a one-line change in the CreateWorksBase CTE.
# MAGIC 2. **Zero logic drift** — the cleaning runs through the *actual* shipped library
# MAGIC    (`openalex.dlt.transform.udf_abstract_features`), not a hand-copied UC UDF that would
# MAGIC    then have to be kept byte-identical with `text_clean.py` forever.
# MAGIC 3. **No recurring cost** — one-time build; the nightly CreateWorksBase pays only a join
# MAGIC    against a few-million-row table.
# MAGIC
# MAGIC The abstract and its inverted index are produced by a **single** call to
# MAGIC `udf_abstract_features`, so they derive from byte-identical text (oxjob #191.1 invariant),
# MAGIC and the overlay row carries both or neither.
# MAGIC
# MAGIC **MANUAL TRIGGER ONLY.** Re-runnable, with two different reasons to redo work:
# MAGIC `rebuild=false` (default) resumes, skipping chunks the ledger already records;
# MAGIC `rebuild=true` re-cleans every staged row but **keeps** the staging table (use after a
# MAGIC `text_clean.py` change — the gate is unchanged, so the staged rows are still the right
# MAGIC rows); `rescan=true` additionally drops staging and re-scans the 273 GB source (only
# MAGIC needed if the GATE or the source table changed).

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.10-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

import time

from pyspark.sql import functions as F

from openalex.dlt.transform import udf_abstract_features

dbutils.widgets.text("env_suffix", "", "Environment suffix (e.g. _dev)")
dbutils.widgets.dropdown("rebuild", "false", ["false", "true"], "Re-clean everything (keeps staging)")
dbutils.widgets.dropdown("rescan", "false", ["false", "true"], "Also re-scan the source (drops staging)")

ENV_SUFFIX = dbutils.widgets.get("env_suffix")
REBUILD = dbutils.widgets.get("rebuild") == "true"
# The two reasons to re-run are different and cost wildly different amounts:
#   text_clean.py changed  -> the GATE is unchanged, so the staged rows are still exactly the
#                             right rows. Re-clean them. `rebuild=true`. Cheap.
#   the GATE changed, or   -> the staged set itself is wrong. Re-scan the 273 GB source.
#   the source changed        `rescan=true`. Expensive (~12 min of pure scan).
# Defaulting rebuild to keep staging is what makes a cleaner-logic fix cheap to redo.
RESCAN = dbutils.widgets.get("rescan") == "true"

SOURCE_TABLE = "openalex.abstracts.abstracts_backfill"
STAGE_TABLE = f"openalex{ENV_SUFFIX}.abstracts.abstracts_backfill_corrupt_stage"
LEDGER_TABLE = f"openalex{ENV_SUFFIX}.abstracts.abstracts_backfill_repair_chunks"
TARGET_TABLE = f"openalex{ENV_SUFFIX}.abstracts.abstracts_backfill_repair"

# Number of work_id-modulo chunks. Each is an independent clean+append with a ledger row,
# so a killed run resumes without redoing finished chunks (workspace batch-job rule 2).
N_CHUNKS = 8

# Corruption gate. MUST stay byte-identical with the detector used in ACCEPTANCE.md and in the
# #807 before/after verification queries — a gate that disagrees between call sites is what
# splits a record across two answers (walden CLAUDE.md). Written with explicit \u escapes so
# the mojibake ranges survive copy/paste between notebooks, SQL cells, and job configs.
GATE_ENTITY = r"&[A-Za-z][A-Za-z0-9]*;|&#[0-9]+;|&#[xX][0-9A-Fa-f]+;"
GATE_TAG = r"<(/[a-zA-Z]|[a-zA-Z])[^>]*>"
GATE_MOJIBAKE = (
    "[\\u00c2-\\u00df][\\u0080-\\u00bf]"
    "|[\\u00e0-\\u00ef][\\u0080-\\u00bf]{2}"
    "|[\\u00f0-\\u00f4][\\u0080-\\u00bf]{3}"
)
GATE = f"({GATE_ENTITY})|({GATE_TAG})|({GATE_MOJIBAKE})"

print(f"source  : {SOURCE_TABLE}")
print(f"stage   : {STAGE_TABLE}")
print(f"ledger  : {LEDGER_TABLE}")
print(f"target  : {TARGET_TABLE}")
print(f"rebuild : {REBUILD}")

# COMMAND ----------

if REBUILD or RESCAN:
    # TRUNCATE, not DROP, for the target: CreateWorksBase LEFT JOINs it on every nightly run,
    # so the table must never stop existing — a dropped table turns a rebuild window into a
    # broken end2end. The ledger has no readers, so dropping it is fine.
    if spark.catalog.tableExists(TARGET_TABLE):
        spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
        print(f"truncated {TARGET_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {LEDGER_TABLE}")
    print(f"dropped {LEDGER_TABLE}")

if RESCAN:
    spark.sql(f"DROP TABLE IF EXISTS {STAGE_TABLE}")
    print(f"dropped {STAGE_TABLE} — the source will be re-scanned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — stage the corrupted rows (one full scan of the 273 GB source)
# MAGIC
# MAGIC The gated scan is the expensive part, so it happens exactly once and lands in a staging
# MAGIC table. Every later step reads the (few-million-row) staging table instead.

# COMMAND ----------

if spark.catalog.tableExists(STAGE_TABLE):
    staged = spark.table(STAGE_TABLE).count()
    print(f"staging already exists: {staged:,} rows — skipping scan")
else:
    t0 = time.time()
    (
        spark.table(SOURCE_TABLE)
        .filter(F.col("abstract").isNotNull())
        .filter(F.col("abstract").rlike(GATE))
        .select("work_id", "abstract")
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(STAGE_TABLE)
    )
    staged = spark.table(STAGE_TABLE).count()
    print(f"staged {staged:,} corrupted rows in {time.time() - t0:,.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — clean each chunk through the shipped library, append to the overlay
# MAGIC
# MAGIC Only rows whose text actually *changed* are kept: the gate is deliberately broad (the #801
# MAGIC mojibake detector is FP-heavy on French typographic NBSP) while `clean_text` is
# MAGIC conservative and returns its input untouched when it can't repair confidently. Filtering
# MAGIC on `cleaned <> original` is what turns the broad gate into a precise overlay.

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} "
    "(chunk INT, rows_in BIGINT, rows_changed BIGINT, seconds DOUBLE, finished_at TIMESTAMP)"
)

done = {r.chunk for r in spark.table(LEDGER_TABLE).select("chunk").collect()}
print(f"chunks already done: {sorted(done) or 'none'}")

for chunk in range(N_CHUNKS):
    if chunk in done:
        print(f"[chunk {chunk + 1}/{N_CHUNKS}] already done — skipping")
        continue

    t0 = time.time()
    part = spark.table(STAGE_TABLE).filter(F.pmod(F.col("work_id"), F.lit(N_CHUNKS)) == chunk)
    rows_in = part.count()

    cleaned = (
        part.withColumnRenamed("abstract", "abstract_original")
        .withColumn("_f", udf_abstract_features(F.col("abstract_original")))
        .select(
            "work_id",
            "abstract_original",
            F.col("_f.abstract").alias("abstract"),
            F.col("_f.abstract_inverted_index").alias("abstract_inverted_index"),
        )
        # Carry both fields or neither: CreateWorksBase swaps them as a pair, so an overlay row
        # with a cleaned abstract but a NULL index would publish a work whose searched text and
        # inverted index disagree (oxjob #191.1).
        .filter(
            F.col("abstract").isNotNull()
            & F.col("abstract_inverted_index").isNotNull()
            & (F.col("abstract") != F.col("abstract_original"))
        )
        .drop("abstract_original")
    )

    (
        cleaned.write.format("delta")
        .mode("overwrite" if not spark.catalog.tableExists(TARGET_TABLE) else "append")
        .option("mergeSchema", "false")
        .saveAsTable(TARGET_TABLE)
    )

    rows_changed = (
        spark.table(TARGET_TABLE)
        .filter(F.pmod(F.col("work_id"), F.lit(N_CHUNKS)) == chunk)
        .count()
    )
    seconds = time.time() - t0

    spark.sql(
        f"INSERT INTO {LEDGER_TABLE} VALUES "
        f"({chunk}, {rows_in}, {rows_changed}, {seconds}, current_timestamp())"
    )
    rate = rows_in / seconds if seconds else 0
    remaining = N_CHUNKS - chunk - 1
    print(
        f"[chunk {chunk + 1}/{N_CHUNKS}] in={rows_in:,} changed={rows_changed:,} "
        f"({seconds:,.0f}s, {rate:,.0f} rows/s) — ETA {remaining * seconds / 60:,.1f} min"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — verify

# COMMAND ----------

total_in = spark.table(STAGE_TABLE).count()
total_out = spark.table(TARGET_TABLE).count()
print(f"gated rows        : {total_in:,}")
print(f"overlay rows      : {total_out:,}  ({100.0 * total_out / total_in:.1f}% of gated)")

residue = spark.table(TARGET_TABLE).filter(F.col("abstract").rlike(GATE)).count()
print(f"overlay rows still matching the gate: {residue:,}")
print("  (expected: a small conservative residue of rare mojibake leads and legit literal")
print("   ampersand-sequences, exactly as in #801 — NOT expected to be zero)")

display(spark.table(LEDGER_TABLE).orderBy("chunk"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — spot-check a sample by eye

# COMMAND ----------

display(
    spark.table(STAGE_TABLE).alias("s")
    .join(spark.table(TARGET_TABLE).alias("t"), "work_id")
    .select(
        "work_id",
        F.substring(F.col("s.abstract"), 1, 300).alias("before"),
        F.substring(F.col("t.abstract"), 1, 300).alias("after"),
    )
    .limit(50)
)
