# Databricks notebook source
# MAGIC %md
# MAGIC # SDG head (Jev-trained) over the Qwen3 work vectors → `openalex.works.works_sdg_jev` (oxjob #1300)
# MAGIC
# MAGIC Scores every row of `openalex.vector_search.work_embeddings_qwen3` with a 17-output logistic head
# MAGIC trained on 200K Jev labels (17 SDG Nouls, oxjobs #1298/#1300), isotonic-calibrated per goal, and
# MAGIC writes one row per work: the 17 calibrated probabilities, the goals at or above their per-goal
# MAGIC thresholds in the same `{id, display_name, score}` shape as `sustainable_development_goals`
# MAGIC (`score` = the calibrated probability), the head version and a timestamp.
# MAGIC
# MAGIC `CreateWorksEnriched` merges `sdgs` into the shadow works column `x_sdgs` (hash-excluded, so this
# MAGIC never re-stamps a work); the manual `Sync All Works to Elasticsearch` job's `x_sdgs` mode pushes the
# MAGIC first fill to ES. The head weights live in `sdg_jev_head_v1.json` beside this notebook (git);
# MAGIC `head_path` overrides that (e.g. a Volume copy).
# MAGIC
# MAGIC Modes: `full` rebuilds the table (~474M rows, one pass, single-digit dollars); `incremental` scores
# MAGIC only works with no row yet, a newer `embedded_at` than their `scored_at`, or a different
# MAGIC `model_version` (the nightly shape, on the embedding loop); `sample` scores `sample_limit` rows
# MAGIC into `<target_table>_sample` for a look before the real run.
# MAGIC
# MAGIC Nothing here touches `openalex_works`, the content hash or Aurora's tables.

# COMMAND ----------

dbutils.widgets.text("mode", "incremental")          # full | incremental | sample
dbutils.widgets.text("sample_limit", "100000")
dbutils.widgets.text("head_path", "")                # default: sdg_jev_head_v1.json beside this notebook
dbutils.widgets.text("target_table", "openalex.works.works_sdg_jev")
dbutils.widgets.text("source_table", "openalex.vector_search.work_embeddings_qwen3")

MODE = dbutils.widgets.get("mode").strip().lower()
SAMPLE_LIMIT = int(dbutils.widgets.get("sample_limit"))
HEAD_PATH = dbutils.widgets.get("head_path").strip()
TARGET = dbutils.widgets.get("target_table").strip()
SOURCE = dbutils.widgets.get("source_table").strip()
if MODE not in ("full", "incremental", "sample"):
    raise ValueError(f"mode must be full | incremental | sample, got {MODE!r}")
if MODE == "sample":
    TARGET = TARGET + "_sample"
print(f"mode={MODE} source={SOURCE} target={TARGET}")

# COMMAND ----------

import json, os, datetime
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql.functions import pandas_udf

if not HEAD_PATH:
    HEAD_PATH = os.path.join(os.getcwd(), "sdg_jev_head_v1.json")
    if not os.path.exists(HEAD_PATH):
        HEAD_PATH = "/Volumes/openalex/works/models/sdg_jev/sdg_jev_head_v1.json"
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
print(f"head {MODEL_VERSION} from {HEAD_PATH}: trained {head['trained_at']}, input_dim {head['input_dim']}")
print("thresholds:", dict(zip(range(1, 18), THRESHOLDS)))

bc = spark.sparkContext.broadcast({"W": W, "B": B, "ISO_X": ISO_X, "ISO_Y": ISO_Y})


@pandas_udf(ArrayType(FloatType()))
def sdg_probs(emb: pd.Series) -> pd.Series:
    """17 calibrated probabilities per work: sigmoid(W·x + b), then the per-goal isotonic map
    (np.interp on the fitted breakpoints, clipped at both ends == sklearn out_of_bounds='clip')."""
    h = bc.value
    X = np.stack(emb.values).astype(np.float32)
    raw = 1.0 / (1.0 + np.exp(-(X @ h["W"].T + h["B"])))
    out = np.empty_like(raw)
    for j in range(17):
        out[:, j] = np.interp(raw[:, j], h["ISO_X"][j], h["ISO_Y"][j])
    return pd.Series([row.tolist() for row in np.round(out, 4)])


# COMMAND ----------

src = spark.table(SOURCE).select(
    F.col("work_id").cast("bigint").alias("work_id"), "embedding", "embedded_at"
).where(F.col("work_id").isNotNull() & (F.size("embedding") == head["input_dim"]))

if MODE == "sample":
    src = src.limit(SAMPLE_LIMIT)
elif MODE == "incremental" and spark.catalog.tableExists(TARGET):
    prev = spark.table(TARGET).select("work_id", F.col("scored_at").alias("_scored_at"), F.col("model_version").alias("_mv"))
    src = (src.join(prev, "work_id", "left")
              .where(F.col("_scored_at").isNull() | (F.col("embedded_at") > F.col("_scored_at")) | (F.col("_mv") != F.lit(MODEL_VERSION)))
              .drop("_scored_at", "_mv"))
elif MODE == "incremental":
    print(f"{TARGET} does not exist yet: incremental falls back to a full build")

ids_lit = F.array(*[F.lit(x) for x in GOAL_IDS])
names_lit = F.array(*[F.lit(x) for x in GOAL_NAMES])
thr_lit = F.array(*[F.lit(x) for x in THRESHOLDS])

# goals at or above their threshold, score = calibrated p, same struct as sustainable_development_goals
scored = (src
    .withColumn("ids", ids_lit).withColumn("names", names_lit).withColumn("thr", thr_lit)
    .withColumn("probs", sdg_probs("embedding"))
    .withColumn("sdgs", F.expr(
        "filter(transform(sequence(0, 16), i -> struct(ids[i] AS id, names[i] AS display_name, probs[i] AS score)), "
        "s -> s.score >= thr[int(substring_index(s.id, '/', -1)) - 1])"))
    .withColumn("model_version", F.lit(MODEL_VERSION))
    .withColumn("scored_at", F.current_timestamp())
    .select("work_id", "probs", "sdgs", "model_version", "scored_at")
)

# COMMAND ----------

if MODE == "incremental" and spark.catalog.tableExists(TARGET):
    scored.createOrReplaceTempView("sdg_jev_new")
    n_new = spark.table("sdg_jev_new").count()
    print(f"incremental: {n_new:,} works to (re)score")
    if n_new:
        spark.sql(f"""
            MERGE INTO {TARGET} AS t
            USING sdg_jev_new AS s
            ON t.work_id = s.work_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
else:
    (scored.write.mode("overwrite").option("overwriteSchema", "true")
           .clusterBy("work_id").saveAsTable(TARGET))
    print(f"{MODE}: wrote {TARGET}")

# COMMAND ----------

# Sanity: row count, label rate, per-goal positive shares. Compare with EXPLORE § 13's expectations
# (about 55-60% of abstract-bearing works get at least one goal; SDG 3 the largest).
stats = spark.sql(f"""
    SELECT COUNT(*) AS n,
           AVG(CASE WHEN SIZE(sdgs) > 0 THEN 1 ELSE 0 END) AS tagged_share,
           AVG(SIZE(sdgs)) AS labels_per_work,
           MAX(scored_at) AS last_scored,
           COUNT(DISTINCT model_version) AS n_versions
    FROM {TARGET}
""").collect()[0]
print(dict(stats.asDict()))
display(spark.sql(f"""
    SELECT s.id, s.display_name, COUNT(*) AS works, ROUND(AVG(s.score), 3) AS mean_score
    FROM {TARGET} LATERAL VIEW explode(sdgs) t AS s
    GROUP BY 1, 2 ORDER BY works DESC
"""))
