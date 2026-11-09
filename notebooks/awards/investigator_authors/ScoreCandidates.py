# Databricks notebook source
# MAGIC %md
# MAGIC # Investigator authors: score the candidates (oxjob #1847)
# MAGIC
# MAGIC Scores every row of `<prefix>features` with the registered model `model@alias` (Unity Catalog) into `<prefix>scores`, with the
# MAGIC version's threshold tags, which decide.sql checks against its own. A missing model, alias, tag or feature column, or an empty
# MAGIC feature table, fails the task. CPU, no model API.

# COMMAND ----------

import os
import sys

import mlflow
from pyspark.sql import functions as F

sys.path.insert(0, os.getcwd())
import iaa  # noqa: E402

p = iaa.params(dbutils)
P = p["prefix"]
mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(p["model"], p["alias"])   # raises when the model or the alias is missing
uri = f"models:/{p['model']}/{mv.version}"
tags = {k: float(mv.tags[k]) for k in ("th99", "th_4_6", "guard_second")}       # KeyError = a version registered without them
cols = mlflow.models.get_model_info(uri).signature.inputs.input_names()
feats = spark.table(P + "features")
missing = [c for c in cols if c not in feats.columns]
if missing or feats.isEmpty():
    raise RuntimeError(f"{uri}: features missing {missing}" if missing else f"{P}features is empty")
score = mlflow.pyfunc.spark_udf(spark, uri, result_type="double", env_manager="local")
out = feats.select("entry_id", "author_id", score(F.struct(*[F.col(c).cast("double").alias(c) for c in cols])).alias("s"),
                   F.lit(int(mv.version)).alias("model_version"), *[F.lit(v).alias(k) for k, v in tags.items()])
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(P + "scores")
iaa.log(f"scored by {uri} {tags}")
