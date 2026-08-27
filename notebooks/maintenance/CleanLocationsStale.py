# Databricks notebook source
# MAGIC %md
# MAGIC # Clean titles + abstracts in `locations_stale` in place (oxjob #807)
# MAGIC
# MAGIC `CreateLocationsMapped` rebuilds `locations_mapped` every night as
# MAGIC `live (locations_w_types) UNION stale (locations_stale)`. The #807 at-ingest cleaner and the
# MAGIC Silver reparse fixed every LIVE row, but `locations_stale` is a frozen snapshot with no
# MAGIC producer in this repo (it is only ever DELETED from, by the #765 drains), so its text never
# MAGIC gets cleaned: after the 2026-08-22 publish it still held ~3.9M rows (~3.2M works) with
# MAGIC undecoded HTML entities, HTML/JATS tags or mojibake in `title`/`abstract` — essentially ALL
# MAGIC of the garbled text an API user can still see.
# MAGIC
# MAGIC This notebook applies the SAME cleaning (the shipped `openalex.dlt.transform` UDFs, not a
# MAGIC hand-copied UC UDF) to those rows and MERGEs the result back into `locations_stale`.
# MAGIC The next nightly `CreateLocationsMapped` then sees a different payload hash, bumps
# MAGIC `openalex_updated_dt`, and the change flows to works_base → works_enriched → ES/Lakebase
# MAGIC through the normal pipeline. Nothing else needs to change.
# MAGIC
# MAGIC Only `title`, `abstract`, `abstract_inverted_index` are touched. `normalized_title` and
# MAGIC `merge_key` (work identity) are NOT — zero W-id churn, same rule as the live cleaner.
# MAGIC
# MAGIC **Reversible:** the repair table keeps `*_original` for every row it changed; reverting is
# MAGIC the same MERGE with the original columns.
# MAGIC
# MAGIC **MANUAL TRIGGER ONLY.** Steps: stage (one gated scan) → clean (chunked, ledgered, resumable)
# MAGIC → apply (one idempotent MERGE) → verify. `limit>0` runs the whole thing on a small sample
# MAGIC into `_smoke`-suffixed tables (serial-first rule) — it still MERGEs those rows into the real
# MAGIC `locations_stale`, which is the point of the smoke test.

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.18-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from openalex.dlt.transform import udf_abstract_features, udf_clean_title

dbutils.widgets.text("env_suffix", "", "Environment suffix (e.g. _dev)")
dbutils.widgets.text("limit", "0", "Smoke test: stage only N gated rows (0 = all)")
dbutils.widgets.dropdown("rebuild", "false", ["false", "true"], "Drop stage/ledger/repair and redo")
dbutils.widgets.dropdown("apply", "true", ["false", "true"], "MERGE the repair into locations_stale")

ENV_SUFFIX = dbutils.widgets.get("env_suffix")
LIMIT = int(dbutils.widgets.get("limit") or "0")
REBUILD = dbutils.widgets.get("rebuild") == "true"
APPLY = dbutils.widgets.get("apply") == "true"

SUFFIX = f"_smoke{LIMIT}" if LIMIT > 0 else ""
TARGET_TABLE = f"openalex{ENV_SUFFIX}.works.locations_stale"
STAGE_TABLE = f"openalex{ENV_SUFFIX}.works.locations_stale_dirty_stage{SUFFIX}"
LEDGER_TABLE = f"openalex{ENV_SUFFIX}.works.locations_stale_repair_chunks{SUFFIX}"
REPAIR_TABLE = f"openalex{ENV_SUFFIX}.works.locations_stale_repair{SUFFIX}"

N_CHUNKS = 8 if LIMIT == 0 else 1

# Corruption gate — byte-identical with CleanAbstractsBackfill and the #807 ACCEPTANCE detectors.
GATE_ENTITY = r"&[A-Za-z][A-Za-z0-9]*;|&#[0-9]+;|&#[xX][0-9A-Fa-f]+;"
GATE_TAG = r"<(/[a-zA-Z]|[a-zA-Z])[^>]*>"
GATE_MOJIBAKE = (
    "[\\u00c2-\\u00df][\\u0080-\\u00bf]"
    "|[\\u00e0-\\u00ef][\\u0080-\\u00bf]{2}"
    "|[\\u00f0-\\u00f4][\\u0080-\\u00bf]{3}"
)
GATE = f"({GATE_ENTITY})|({GATE_TAG})|({GATE_MOJIBAKE})"

KEYS = ["provenance", "native_id_namespace", "native_id", "work_id"]

print(f"target : {TARGET_TABLE}")
print(f"stage  : {STAGE_TABLE}")
print(f"ledger : {LEDGER_TABLE}")
print(f"repair : {REPAIR_TABLE}")
print(f"limit  : {LIMIT}   rebuild: {REBUILD}   apply: {APPLY}   chunks: {N_CHUNKS}")

# COMMAND ----------

if REBUILD:
    for t in (STAGE_TABLE, LEDGER_TABLE, REPAIR_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"dropped {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — stage the gated rows (one scan of locations_stale)

# COMMAND ----------

if spark.catalog.tableExists(STAGE_TABLE):
    staged = spark.table(STAGE_TABLE).count()
    print(f"staging already exists: {staged:,} rows — skipping scan")
else:
    t0 = time.time()
    src = (
        spark.table(TARGET_TABLE)
        .filter(F.col("title").rlike(GATE) | F.col("abstract").rlike(GATE))
        .select(*KEYS, "title", "abstract", "abstract_inverted_index")
    )
    if LIMIT > 0:
        src = src.limit(LIMIT)
    src.write.format("delta").mode("overwrite").saveAsTable(STAGE_TABLE)
    staged = spark.table(STAGE_TABLE).count()
    print(f"staged {staged:,} gated rows in {time.time() - t0:,.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — clean each chunk through the shipped library → repair table (keeps originals)
# MAGIC
# MAGIC A row lands in the repair table only if its title OR abstract actually changed. For a changed
# MAGIC abstract, `abstract` and `abstract_inverted_index` come from ONE `udf_abstract_features` call
# MAGIC (byte-identical text, oxjob #191.1 invariant); an unchanged abstract keeps its original index.

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
    part = spark.table(STAGE_TABLE).filter(
        F.pmod(F.coalesce(F.col("work_id"), F.lit(0)), F.lit(N_CHUNKS)) == chunk
    )
    rows_in = part.count()

    cleaned = (
        part.withColumnRenamed("title", "title_original")
        .withColumnRenamed("abstract", "abstract_original")
        .withColumnRenamed("abstract_inverted_index", "abstract_inverted_index_original")
        .withColumn("_t", udf_clean_title(F.col("title_original")))
        .withColumn(
            "_f",
            F.when(F.col("abstract_original").isNotNull(), udf_abstract_features(F.col("abstract_original"))),
        )
        .withColumn(
            "title_changed",
            F.col("_t").isNotNull() & (F.col("_t") != F.col("title_original")),
        )
        .withColumn(
            "abstract_changed",
            F.col("_f.abstract").isNotNull()
            & F.col("_f.abstract_inverted_index").isNotNull()
            & (F.col("_f.abstract") != F.col("abstract_original")),
        )
        .filter(F.col("title_changed") | F.col("abstract_changed"))
        .select(
            *KEYS,
            "title_original",
            F.when(F.col("title_changed"), F.col("_t")).otherwise(F.col("title_original")).alias("title"),
            "abstract_original",
            F.when(F.col("abstract_changed"), F.col("_f.abstract")).otherwise(F.col("abstract_original")).alias("abstract"),
            "abstract_inverted_index_original",
            F.when(F.col("abstract_changed"), F.col("_f.abstract_inverted_index"))
            .otherwise(F.col("abstract_inverted_index_original"))
            .alias("abstract_inverted_index"),
            "title_changed",
            "abstract_changed",
            F.lit(chunk).alias("chunk"),
        )
    )

    (
        cleaned.write.format("delta")
        .mode("overwrite" if not spark.catalog.tableExists(REPAIR_TABLE) else "append")
        .option("mergeSchema", "false")
        .saveAsTable(REPAIR_TABLE)
    )
    rows_changed = spark.table(REPAIR_TABLE).filter(F.col("chunk") == chunk).count()
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
# MAGIC ## Step 3 — look before applying

# COMMAND ----------

rep = spark.table(REPAIR_TABLE)
print(f"repair rows: {rep.count():,}  (title changed: {rep.filter('title_changed').count():,}, "
      f"abstract changed: {rep.filter('abstract_changed').count():,})")
print(f"repair rows still matching the gate — title: {rep.filter(F.col('title').rlike(GATE)).count():,}, "
      f"abstract: {rep.filter(F.col('abstract').rlike(GATE)).count():,}  (small literal residue expected, not 0)")
display(
    rep.select(
        "work_id", "provenance",
        F.substring("title_original", 1, 120).alias("title_before"),
        F.substring("title", 1, 120).alias("title_after"),
        F.substring("abstract_original", 1, 200).alias("abstract_before"),
        F.substring("abstract", 1, 200).alias("abstract_after"),
    ).limit(40)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — apply: one idempotent MERGE into locations_stale
# MAGIC
# MAGIC Keys are `(provenance, native_id_namespace, native_id, work_id)` — the same key
# MAGIC `CreateLocationsMapped` uses for its payload hash. The repair side is deduped on that key so
# MAGIC MERGE can never see two source rows for one target row.

# COMMAND ----------

if not APPLY:
    print("apply=false — repair table built, locations_stale NOT modified")
else:
    t0 = time.time()
    w = Window.partitionBy(*KEYS).orderBy(F.col("chunk"))
    dedup = rep.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    dedup.createOrReplaceTempView("_stale_repair_src")
    on = " AND ".join(f"s.{k} <=> r.{k}" for k in KEYS)
    res = spark.sql(f"""
        MERGE INTO {TARGET_TABLE} s
        USING _stale_repair_src r
        ON {on}
        WHEN MATCHED AND (s.title <=> r.title_original OR s.abstract <=> r.abstract_original) THEN UPDATE SET
          s.title = r.title,
          s.abstract = r.abstract,
          s.abstract_inverted_index = r.abstract_inverted_index
    """)
    display(res)
    print(f"MERGE done in {time.time() - t0:,.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — verify on the target

# COMMAND ----------

tgt = spark.table(TARGET_TABLE)
if LIMIT > 0:
    tgt = tgt.join(spark.table(STAGE_TABLE).select(*KEYS), KEYS, "inner")
print(f"target rows still matching the gate — title: {tgt.filter(F.col('title').rlike(GATE)).count():,}, "
      f"abstract: {tgt.filter(F.col('abstract').rlike(GATE)).count():,}")
display(spark.table(LEDGER_TABLE).orderBy("chunk"))
