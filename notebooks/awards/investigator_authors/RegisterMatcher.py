# Databricks notebook source
# MAGIC %md
# MAGIC # Investigator authors: register the model (oxjob #1847). Manual, never scheduled.
# MAGIC
# MAGIC Registers the XGBoost model JSON as a new version of `model` in Unity Catalog, with its feature names as the signature and its
# MAGIC thresholds as tags, and points `alias` at it. Writes only the registry; decide.sql fails a run scored by a version whose tags
# MAGIC differ from its own thresholds.

# COMMAND ----------

import json

import mlflow
import pandas as pd
import xgboost as xgb
from mlflow.models import infer_signature

for k, v in [("model_json", ""), ("meta_json", ""), ("key", "gr1102_nojev"), ("model", "openalex.awards.investigator_author_matcher"),
             ("alias", "champion"), ("experiment", "/Shared/investigator_author_matcher")]:
    dbutils.widgets.text(k, v)
w = {k: dbutils.widgets.get(k).strip() for k in ("model_json", "meta_json", "key", "model", "alias", "experiment")}
meta = json.load(open(w["meta_json"]))["models"][w["key"]]
booster = xgb.Booster()
booster.load_model(w["model_json"])
booster.feature_names = meta["features"]
x = pd.DataFrame([[0.0] * len(meta["features"])], columns=meta["features"])
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(w["experiment"])
with mlflow.start_run(run_name=f"register {w['key']}"):
    info = mlflow.xgboost.log_model(booster, artifact_path="model", signature=infer_signature(x, booster.predict(xgb.DMatrix(x))),
                                    registered_model_name=w["model"])
version = info.registered_model_version
client = mlflow.MlflowClient()
for k in ("th99", "th_4_6", "guard_second", "max_cands", "heldout_gold_precision", "heldout_gold_wilson_lo", "heldout_recall_1_6"):
    client.set_model_version_tag(w["model"], version, k, str(meta[k]))
client.set_model_version_tag(w["model"], version, "source", f"{w['key']} ({meta['label']})")
client.set_registered_model_alias(w["model"], w["alias"], version)
print(f"{w['model']} version {version} -> @{w['alias']}")
