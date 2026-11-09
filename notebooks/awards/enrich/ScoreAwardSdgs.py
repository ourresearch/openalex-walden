# Databricks notebook source
# MAGIC %md
# MAGIC # Awards enrich 4/4: SDGs as works get them, into `award_sdgs`
# MAGIC
# MAGIC Embeds each queued award with `ai_query('databricks-qwen3-embedding-0-6b', text)` (the works vectors' endpoint; text =
# MAGIC `award_text.EMBED_TEXT`: "Title: ...\n\nAbstract: ..." cut at 2,000 characters, documents bare as for works), then scores
# MAGIC the 1024-d vector with the works head (`notebooks/sdg/sdg_jev_head_v2.json` via `lib/sdg_head.py`: sigmoid, per-goal isotonic,
# MAGIC threshold 0.4), and MERGEs one row per award into `target`: the 17 scores, the served goals in the shape of works'
# MAGIC `sustainable_development_goals`, the head version, the text_key. Vectors are not kept.
# MAGIC No GPU and no Modal: serverless + the Databricks embedding endpoint (~$3.4 per 1M texts, EmbedQwen3Incremental's measure).
# MAGIC Measured 2026-11-08 on the 10k sample, read-only: 15 s for 10,000 awards (665 / s), endpoint ~$0.03; the head on the laptop
# MAGIC matched served works_sdg_jev goals on 24 of 24 works (max score difference 0.034).
# MAGIC `dry_run = true` (the default): counts and estimate only.

# COMMAND ----------

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import award_text as T  # noqa: E402
import sdg_head as H  # noqa: E402

for name, default in [("state_prefix", "openalex_dev.rohan_lab.awt_"), ("target", "openalex_dev.rohan_lab.awt_award_sdgs"),
                      ("awards_table", "openalex.awards.awards_api"), ("max_usd", "5"), ("chunk_shards", "10"), ("parallel", "4"),
                      ("dry_run", "true")]:
    dbutils.widgets.text(name, default)
P, TARGET = dbutils.widgets.get("state_prefix").strip(), dbutils.widgets.get("target").strip()
AWARDS = dbutils.widgets.get("awards_table").strip()
MAX_USD = float(dbutils.widgets.get("max_usd"))
CHUNK = max(1, int(dbutils.widgets.get("chunk_shards")))   # queue shards per ai_query pass (10 x 25,000 = 250k, as EmbedQwen3Incremental)
PAR = max(1, int(dbutils.widgets.get("parallel")))         # passes at once (EmbedQwen3Incremental: ~2,370 texts / s at 4)
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE = f"{P}queue"
USD_PER_M = 3.4


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

def planned():
    """(awards, shards, build_id) for a dry run, from build_queue's task values (its dry run writes no queue). Run by hand
    (no job), they fall back to the queue table if one exists, else 0."""
    tv = lambda k, d: dbutils.jobs.taskValues.get(taskKey="build_queue", key=k, default=d, debugValue=d)
    n = int(tv("queued", -1))
    if n < 0 and spark.catalog.tableExists(QUEUE):
        r = spark.sql(f"SELECT count(*) AS n, count(DISTINCT shard) AS s, max(build_id) AS b FROM {QUEUE}").collect()[0]
        return int(r.n), int(r.s), r.b or "awt-dry"
    return max(n, 0), int(tv("shards", 1)), tv("build_id", "awt-dry")

# COMMAND ----------

head = H.load()
if DRY:
    n, _, build_id = planned()
else:
    q = spark.sql(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}").collect()[0]
    n, build_id = int(q.n), q.b
est = n / 1e6 * USD_PER_M
log(f"build {build_id}: {n:,} awards; head {head['version']} ({head['path']}); endpoint estimate ${est:.2f} (max_usd {MAX_USD})")
if n == 0 or DRY:
    dbutils.notebook.exit("empty queue" if n == 0 else f"dry run: {n:,} awards, ~${est:.2f}")
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.functions import pandas_udf  # noqa: E402
from pyspark.sql.types import ArrayType, FloatType  # noqa: E402


HW, HB, IX, IY = head["W"], head["B"], head["iso_x"], head["iso_y"]   # plain arrays in the closure: workers need no import of lib/


@pandas_udf(ArrayType(FloatType()))
def sdg_probs(emb: pd.Series) -> pd.Series:   # = sdg_jev_head_score.sdg_probs = sdg_head.probs
    if len(emb) == 0:
        return pd.Series([], dtype=object)
    X = np.stack(emb.values).astype(np.float32)
    raw = 1.0 / (1.0 + np.exp(-(X @ HW.T + HB)))
    out = np.empty_like(raw)
    for j in range(17):
        out[:, j] = np.interp(raw[:, j], IX[j], IY[j])
    return pd.Series([row.tolist() for row in np.round(out, 4)])


spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (award_id BIGINT, probs ARRAY<FLOAT>,
  sdgs ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT>>, text_key BIGINT, model_version STRING, scored_at TIMESTAMP)
  CLUSTER BY (award_id) COMMENT 'Awards enrich: SDGs from the works head (oxjob #1300) on Qwen3 award vectors; shape of works sustainable_development_goals'""")
ids_lit = F.array(*[F.lit(x) for x in head["ids"]])
names_lit = F.array(*[F.lit(x) for x in head["names"]])
thr_lit = F.array(*[F.lit(x) for x in head["thr"]])
STAGE = f"{P}sdg_stage"     # append-only: parallel passes only append (no concurrent MERGE); one MERGE into the target at the end
PENDING = f"{P}sdg_pending"  # written once before the passes, so a pass reads it and never the stage it appends to (blind appends)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {STAGE} (award_id BIGINT, probs ARRAY<FLOAT>, sdgs ARRAY<STRUCT<id: STRING, display_name: STRING,
  score: FLOAT>>, text_key BIGINT, model_version STRING, scored_at TIMESTAMP, build_id STRING) CLUSTER BY (build_id)
  COMMENT 'Awards enrich: SDG rows per build, staged by parallel ai_query passes before one MERGE into the target'""")


# Awards still to score, fixed before any pass starts: not served on today's text by today's head, not staged for this build
# (a retry of the task resumes here).
spark.sql(f"""CREATE OR REPLACE TABLE {PENDING} CLUSTER BY (shard) AS
SELECT q.shard, q.award_id, q.text_key, q.title AS display_name, q.abstract AS description FROM {QUEUE} q
LEFT ANTI JOIN (SELECT award_id, text_key FROM {TARGET} WHERE model_version = '{head["version"]}') d
  ON d.award_id = q.award_id AND d.text_key <=> q.text_key
LEFT ANTI JOIN (SELECT award_id, text_key FROM {STAGE} WHERE build_id = '{build_id}' AND model_version = '{head["version"]}') g
  ON g.award_id = q.award_id AND g.text_key <=> q.text_key""")


def score_chunk(part):
    """One ai_query pass over a few queue shards of the pending table, appended to the stage. One failed row fails this pass only."""
    where = ", ".join(str(x) for x in part)
    vec = spark.sql(f"""SELECT award_id, text_key, ai_query('{T.EMBED_ENDPOINT}', {T.EMBED_TEXT}) AS embedding
      FROM {PENDING} WHERE shard IN ({where})""")
    (vec.where(F.size("embedding") == head["input_dim"])
        .withColumn("ids", ids_lit).withColumn("names", names_lit).withColumn("thr", thr_lit).withColumn("probs", sdg_probs("embedding"))
        .withColumn("sdgs", F.expr(H.SDGS_EXPR)).select("award_id", "probs", "sdgs", "text_key")
        .withColumn("model_version", F.lit(head["version"])).withColumn("scored_at", F.current_timestamp())
        .withColumn("build_id", F.lit(build_id))
        .write.mode("append").saveAsTable(STAGE))
    log(f"shards {part[0]}..{part[-1]} staged")


import concurrent.futures as cf  # noqa: E402

shards = [int(r.shard) for r in spark.sql(f"SELECT DISTINCT shard FROM {PENDING} ORDER BY shard").collect()]
with cf.ThreadPoolExecutor(PAR) as ex:
    list(ex.map(score_chunk, [shards[i:i + CHUNK] for i in range(0, len(shards), CHUNK)]))   # list(): re-raises a failed pass
m = spark.sql(f"""MERGE INTO {TARGET} t USING (
    SELECT g.award_id, g.probs, g.sdgs, g.text_key, g.model_version, g.scored_at FROM {STAGE} g
    JOIN {QUEUE} q ON q.award_id = g.award_id AND q.text_key <=> g.text_key
    WHERE g.build_id = '{build_id}' AND g.model_version = '{head["version"]}'
    QUALIFY row_number() OVER (PARTITION BY g.award_id ORDER BY g.scored_at DESC) = 1) s ON t.award_id = s.award_id
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""").collect()[0]
log(f"merged {m.num_affected_rows:,} staged rows of build {build_id} into {TARGET}")
c = spark.sql(f"""SELECT count(*) AS n, count_if(size(t.sdgs) > 0) AS tagged
  FROM {TARGET} t JOIN {QUEUE} q ON q.award_id = t.award_id AND q.text_key <=> t.text_key
  WHERE t.model_version = '{head["version"]}'""").collect()[0]
if c.n != n:
    raise RuntimeError(f"{TARGET}: {c.n:,} of {n:,} queued awards served on today's text by {head['version']} (an embedding came back empty?)")
log(f"build {build_id}: {n:,} awards in {TARGET}; {c.tagged / n:.1%} with at least one goal")
