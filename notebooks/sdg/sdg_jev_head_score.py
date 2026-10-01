# Databricks notebook source
# MAGIC %md
# MAGIC # SDG head (Jev-trained) over the Qwen3 work vectors → `openalex.works.works_sdg_jev` (oxjob #1300)
# MAGIC
# MAGIC Scores rows of `openalex.vector_search.work_embeddings_qwen3` with a 17-output logistic head trained on
# MAGIC 200K Jev labels (17 SDG Nouls, oxjobs #1298/#1300), isotonic-calibrated per goal, and writes one row per
# MAGIC work: the 17 calibrated scores, the goals at or above their threshold (0.4 on every goal) in the
# MAGIC `{id, display_name, score}` shape of `sustainable_development_goals` (`score` = the calibrated score, the
# MAGIC classifier's confidence; sorted by score, highest first), the head version, the vector's `embedded_at`
# MAGIC and a timestamp.
# MAGIC
# MAGIC **This is the source of `openalex_works.sustainable_development_goals`** since the head replaced Aurora
# MAGIC (Jason, 2026-10-01): `CreateWorksEnriched` merges `sdgs` into the field every night; a work with no row
# MAGIC here (no vector yet) or no goal over threshold gets an empty list, never Aurora. Aurora's last tags live
# MAGIC on, deprecated, in `sustainable_development_goals_aurora` (`notebooks/sdg/freeze_aurora_sdgs`).
# MAGIC
# MAGIC The head weights are `HEAD_FILE` beside this notebook (git); to ship a new head, add its JSON beside this
# MAGIC one, change `HEAD_FILE`, and run `mode=full` (incremental refuses to mix versions). The `head_path` widget
# MAGIC overrides the file for a `sample` look at a candidate head.
# MAGIC
# MAGIC Modes:
# MAGIC - `full` rebuilds the table (~474M rows, one pass, single-digit dollars). Run once before the replacement
# MAGIC   ships, and again on every head change.
# MAGIC - `incremental` (nightly, task `sdg_score` of `jobs/embed_qwen3_nightly.yaml`, right after the Qwen3 embed
# MAGIC   task): scores the vectors embedded since the table's newest `embedded_at` (minus `overlap_hours`, so a
# MAGIC   late append is never skipped) and MERGEs them by `work_id`. New works and works whose text changed get a
# MAGIC   new vector with a new `embedded_at` (EmbedQwen3Incremental deletes and re-appends), so both are rescored.
# MAGIC   Refuses to run if the table is missing or holds another head version: run `full` first.
# MAGIC - `sample` scores `sample_limit` rows into `<target_table>_sample` for a look before a real run.
# MAGIC
# MAGIC Nothing here touches `openalex_works`, the content hash or Aurora's tables.

# COMMAND ----------

# The head artefact: ONE constant. v2 = v1 with goal 15 retrained under the narrow SDG 15 reading
# (oxjob #1300 README Decision 6, EXPLORE § 16); v1 stays in the repo for comparison.
HEAD_FILE = "sdg_jev_head_v2.json"
HEAD_VOLUME_DIR = "/Volumes/openalex/works/models/sdg_jev"   # fallback copy if the git file is not reachable

dbutils.widgets.text("mode", "incremental")          # full | incremental | sample
dbutils.widgets.text("sample_limit", "100000")
dbutils.widgets.text("overlap_hours", "48")          # incremental: rescore this far behind the watermark
dbutils.widgets.text("head_path", "")                # default: HEAD_FILE beside this notebook (sample runs only)
dbutils.widgets.text("target_table", "openalex.works.works_sdg_jev")
dbutils.widgets.text("source_table", "openalex.vector_search.work_embeddings_qwen3")

MODE = dbutils.widgets.get("mode").strip().lower()
SAMPLE_LIMIT = int(dbutils.widgets.get("sample_limit"))
OVERLAP_HOURS = int(dbutils.widgets.get("overlap_hours"))
HEAD_PATH = dbutils.widgets.get("head_path").strip()
TARGET = dbutils.widgets.get("target_table").strip()
SOURCE = dbutils.widgets.get("source_table").strip()
if MODE not in ("full", "incremental", "sample"):
    raise ValueError(f"mode must be full | incremental | sample, got {MODE!r}")
if HEAD_PATH and MODE != "sample":
    raise ValueError("head_path overrides HEAD_FILE for sample runs only; to ship a new head, change HEAD_FILE")
if MODE == "sample":
    TARGET = TARGET + "_sample"
print(f"mode={MODE} source={SOURCE} target={TARGET}")

# COMMAND ----------

import json, os
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.window import Window

if not HEAD_PATH:
    HEAD_PATH = os.path.join(os.getcwd(), HEAD_FILE)
    if not os.path.exists(HEAD_PATH):
        HEAD_PATH = os.path.join(HEAD_VOLUME_DIR, HEAD_FILE)
if not os.path.exists(HEAD_PATH):
    raise FileNotFoundError(f"{HEAD_FILE} not beside this notebook (cwd {os.getcwd()}) nor in {HEAD_VOLUME_DIR}")
head = json.load(open(HEAD_PATH))
MODEL_VERSION = head["model_version"]
W = np.asarray(head["W"], dtype=np.float32)            # (17, 1024)
B = np.asarray(head["b"], dtype=np.float32)            # (17,)
ISO_X = [np.asarray(g["iso_x"], dtype=np.float32) for g in head["goals"]]
ISO_Y = [np.asarray(g["iso_y"], dtype=np.float32) for g in head["goals"]]
THRESHOLDS = [float(g["threshold"]) for g in head["goals"]]
GOAL_IDS = [g["id"] for g in head["goals"]]
GOAL_NAMES = [g["display_name"] for g in head["goals"]]
assert W.shape == (17, head["input_dim"]) and len(THRESHOLDS) == 17
assert GOAL_IDS == [f"https://metadata.un.org/sdg/{k}" for k in range(1, 18)], "goals must be in SDG 1..17 order"
print(f"head {MODEL_VERSION} from {HEAD_PATH}: trained {head['trained_at']}, input_dim {head['input_dim']}")
print("thresholds:", dict(zip(range(1, 18), THRESHOLDS)))


# The head (~70 KB) travels inside the UDF's closure: no SparkContext broadcast, which serverless does not have.
@pandas_udf(ArrayType(FloatType()))
def sdg_probs(emb: pd.Series) -> pd.Series:
    """17 calibrated scores per work: sigmoid(W·x + b), then the per-goal isotonic map
    (np.interp on the fitted breakpoints, clipped at both ends == sklearn out_of_bounds='clip')."""
    X = np.stack(emb.values).astype(np.float32)
    raw = 1.0 / (1.0 + np.exp(-(X @ W.T + B)))
    out = np.empty_like(raw)
    for j in range(17):
        out[:, j] = np.interp(raw[:, j], ISO_X[j], ISO_Y[j])
    return pd.Series([row.tolist() for row in np.round(out, 4)])


# COMMAND ----------

src = spark.table(SOURCE).select(
    F.col("work_id").cast("bigint").alias("work_id"), "embedding", "embedded_at"
).where(F.col("work_id").isNotNull() & (F.size("embedding") == head["input_dim"]))

target_exists = spark.catalog.tableExists(TARGET)
if MODE == "sample":
    src = src.limit(SAMPLE_LIMIT)
elif MODE == "incremental":
    if not target_exists:
        raise RuntimeError(f"{TARGET} does not exist: run this notebook once with mode=full (it is the source of "
                           "sustainable_development_goals; an unattended nightly must not build it from scratch)")
    versions = [r.model_version for r in spark.table(TARGET).select("model_version").distinct().collect()]
    if versions != [MODEL_VERSION]:
        raise RuntimeError(f"{TARGET} holds head version(s) {versions}, this notebook loads {MODEL_VERSION}: "
                           "run mode=full to rescore the corpus with the new head")
    watermark = spark.table(TARGET).agg(F.max("embedded_at")).first()[0]
    print(f"incremental: vectors embedded after {watermark} minus {OVERLAP_HOURS} h")
    src = src.where(F.col("embedded_at") > F.lit(watermark) - F.expr(f"INTERVAL {OVERLAP_HOURS} HOURS"))

ids_lit = F.array(*[F.lit(x) for x in GOAL_IDS])
names_lit = F.array(*[F.lit(x) for x in GOAL_NAMES])
thr_lit = F.array(*[F.lit(x) for x in THRESHOLDS])

# Goals at or above their threshold in the struct of sustainable_development_goals, score = calibrated score,
# highest score first (as Aurora's list was), ties in goal order.
SDGS_EXPR = """
transform(
  array_sort(
    transform(filter(sequence(0, 16), i -> probs[i] >= thr[i]),
              i -> named_struct('g', i, 'id', ids[i], 'display_name', names[i], 'score', probs[i])),
    (l, r) -> CASE WHEN l.score > r.score THEN -1 WHEN l.score < r.score THEN 1
                   WHEN l.g < r.g THEN -1 WHEN l.g > r.g THEN 1 ELSE 0 END),
  s -> named_struct('id', s.id, 'display_name', s.display_name, 'score', s.score))
"""

# One row per work: the vector table can hold more than one row for a work; the latest vector wins. Deduped
# after scoring, on the small rows, so the 1024-d vectors are never shuffled.
latest = Window.partitionBy("work_id").orderBy(F.col("embedded_at").desc_nulls_last())
scored = (src
    .withColumn("ids", ids_lit).withColumn("names", names_lit).withColumn("thr", thr_lit)
    .withColumn("probs", sdg_probs("embedding"))
    .withColumn("sdgs", F.expr(SDGS_EXPR))
    .select("work_id", "probs", "sdgs", "embedded_at")
    .withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
    .withColumn("model_version", F.lit(MODEL_VERSION))
    .withColumn("scored_at", F.current_timestamp())
    .select("work_id", "probs", "sdgs", "model_version", "embedded_at", "scored_at")
)
scored.createOrReplaceTempView("sdg_jev_scored")

# COMMAND ----------

if MODE == "incremental":
    n_new = spark.table("sdg_jev_scored").count()
    print(f"incremental: {n_new:,} works to (re)score")
    if n_new:
        spark.sql(f"""
            MERGE INTO {TARGET} AS t
            USING sdg_jev_scored AS s
            ON t.work_id = s.work_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
else:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TARGET} CLUSTER BY (work_id)
        COMMENT 'SDG tags from the Jev-trained head on Qwen3 vectors (oxjob #1300); source of openalex_works.sustainable_development_goals'
        AS SELECT * FROM sdg_jev_scored
    """)
    print(f"{MODE}: wrote {TARGET}")

# COMMAND ----------

# Sanity: row count, label rate, per-goal positive shares. Compare with EXPLORE § 13's expectations
# (about 55-60% of abstract-bearing works get at least one goal; SDG 3 the largest).
stats = spark.sql(f"""
    SELECT COUNT(*) AS n,
           COUNT(DISTINCT work_id) AS works,
           AVG(CASE WHEN SIZE(sdgs) > 0 THEN 1 ELSE 0 END) AS tagged_share,
           AVG(SIZE(sdgs)) AS labels_per_work,
           MAX(embedded_at) AS newest_vector,
           MAX(scored_at) AS last_scored,
           COUNT(DISTINCT model_version) AS n_versions
    FROM {TARGET}
""").collect()[0]
print(dict(stats.asDict()))
if stats["n"] != stats["works"]:
    raise RuntimeError(f"{TARGET} has {stats['n'] - stats['works']:,} duplicate work_ids: CreateWorksEnriched's MERGE would fail")
display(spark.sql(f"""
    SELECT s.id, s.display_name, COUNT(*) AS works, ROUND(AVG(s.score), 3) AS mean_score
    FROM {TARGET} LATERAL VIEW explode(sdgs) t AS s
    GROUP BY 1, 2 ORDER BY works DESC
"""))
