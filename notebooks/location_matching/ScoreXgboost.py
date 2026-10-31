# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 4/6: XGBoost (oxjob #1668)
# MAGIC
# MAGIC Scores every (seed, candidate) pair with `openalex.works.location_work_matcher@challenger` (raw score, pinned training
# MAGIC categories). Hard rules first: pairs on a vetoed type boundary (`location_work_match_type_veto`, Casey 2026-10-15: matching
# MAGIC trusts the record's type) and undeclared Kaggle versions (both DOIs `10.34740/kaggle/dsv/`) are never scored. Writes
# MAGIC `location_work_match_xgb`: every scored pair with its raw score and its rank within the record. A record's top score
# MAGIC < 0.01 mints; >= 0.01 goes to Qwen. Scores in Spark through the model's UDF (a 705K-record night ran the driver out of
# MAGIC memory when the pairs went through pandas, 2026-10-30).

# COMMAND ----------

import json
import time

import mlflow
from pyspark.sql import Window, functions as F

NAME = "openalex.works.location_work_matcher"
T = "openalex.works.location_work_match_"
CATS = ["prov_a", "type_a", "type_b", "version_a", "prov_b", "version_b"]
KEEP = ["part", "seed_id", "cid", "decision", "stratum_prov", "weight", "raw_type_a", "dc_client"]
T0 = time.time()

pairs = spark.sql(f"""
  WITH kag AS (
    SELECT DISTINCT r.id AS seed_id, w.id AS cid FROM {T}ftok r JOIN {T}ftok w
      ON r.side = 'r' AND w.side = 'w' AND r.kind = 'do' AND w.kind = 'do'
     AND r.t LIKE '10.34740/kaggle/dsv/%' AND w.t LIKE '10.34740/kaggle/dsv/%'
  )
  SELECT f.* FROM {T}features f
  LEFT ANTI JOIN {T}type_veto v ON v.type_a = f.type_a AND v.type_b = f.type_b
  LEFT ANTI JOIN kag k ON k.seed_id = f.seed_id AND k.cid = f.cid
""")
n_all = spark.table(f"{T}features").count()

mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(NAME, "challenger")
score = mlflow.pyfunc.spark_udf(spark, f"models:/{NAME}/{mv.version}", result_type="double", env_manager="local")
feats = [c for c in pairs.columns if c not in KEEP]
inp = F.struct(*[(F.col(c).cast("string") if c in CATS else F.col(c).cast("double")).alias(c) for c in feats])
out = (pairs.withColumn("p_raw", score(inp))
       .withColumn("rank", F.row_number().over(Window.partitionBy("seed_id").orderBy(F.desc("p_raw"), "cid")))
       .select("seed_id", "cid", "p_raw", "rank", "is_bound", "ch_emb", "decision", "stratum_prov", "weight",
               F.lit(int(mv.version)).alias("model_version")))
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{T}xgb")
s = spark.sql(f"""SELECT count(*) AS scored, count_if(rank = 1) AS records, count_if(rank = 1 AND p_raw >= 0.9156) AS top_ge_tau_bind,
  count_if(rank = 1 AND p_raw < 0.01) AS top_lt_tau_mint FROM {T}xgb""").collect()[0]
summary = {"pairs": n_all, "scored": s.scored, "vetoed": n_all - s.scored, "records": s.records,
           "top_ge_tau_bind": s.top_ge_tau_bind, "top_lt_tau_mint": s.top_lt_tau_mint, "model": f"{NAME} v{mv.version}"}
print(summary)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit(json.dumps(summary))
