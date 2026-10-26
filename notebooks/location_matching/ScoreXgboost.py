# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 4/6: XGBoost (oxjob #1668)
# MAGIC
# MAGIC Scores every (seed, candidate) pair with `openalex.works.location_work_matcher@challenger` (raw score, pinned training
# MAGIC categories). Hard rules first: pairs on a vetoed type boundary (`location_work_match_type_veto`, Casey 2026-10-15: matching
# MAGIC trusts the record's type) and undeclared Kaggle versions (both DOIs `10.34740/kaggle/dsv/`) are never scored. Writes
# MAGIC `location_work_match_xgb`: every scored pair with its raw score and its rank within the record. A record's top score
# MAGIC < 0.01 mints; >= 0.01 goes to Qwen; XGBoost alone binds at >= 0.9156 when Qwen is unavailable.

# COMMAND ----------

import json
import time

import mlflow

NAME = "openalex.works.location_work_matcher"
T = "openalex.works.location_work_match_"
T0 = time.time()

df = spark.sql(f"""
  WITH kag AS (
    SELECT DISTINCT r.id AS seed_id, w.id AS cid FROM {T}ftok r JOIN {T}ftok w
      ON r.side = 'r' AND w.side = 'w' AND r.kind = 'do' AND w.kind = 'do'
     AND r.t LIKE '10.34740/kaggle/dsv/%' AND w.t LIKE '10.34740/kaggle/dsv/%'
  )
  SELECT f.* FROM {T}features f
  LEFT ANTI JOIN {T}type_veto v ON v.type_a = f.type_a AND v.type_b = f.type_b
  LEFT ANTI JOIN kag k ON k.seed_id = f.seed_id AND k.cid = f.cid
""").toPandas()
n_all = spark.table(f"{T}features").count()

mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(NAME, "challenger")
model = mlflow.pyfunc.load_model(f"models:/{NAME}@challenger")
CATS = ["prov_a", "type_a", "type_b", "version_a", "prov_b", "version_b"]
inp = df.drop(columns=["part", "seed_id", "cid", "decision", "stratum_prov", "weight", "raw_type_a", "dc_client"])
num = [c for c in inp.columns if c not in CATS]
inp[num] = inp[num].astype("float64")
df["p_raw"] = model.predict(inp).p_raw.values
df["rank"] = df.groupby("seed_id").p_raw.rank(ascending=False, method="first").astype(int)
out = df[["seed_id", "cid", "p_raw", "rank", "is_bound", "ch_emb", "decision", "stratum_prov", "weight"]].assign(model_version=int(mv.version))
spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{T}xgb")
top = out[out["rank"] == 1]
summary = {"pairs": n_all, "scored": len(out), "vetoed": n_all - len(out), "records": int(top.seed_id.nunique()),
           "top_ge_tau_bind": int((top.p_raw >= 0.9156).sum()), "top_lt_tau_mint": int((top.p_raw < 0.01).sum()),
           "model": f"{NAME} v{mv.version}"}
print(summary)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit(json.dumps(summary))
