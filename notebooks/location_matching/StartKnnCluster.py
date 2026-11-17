# Databricks notebook source
# MAGIC %md
# MAGIC # Start the knn GPU cluster (oxjob #2198)
# MAGIC
# MAGIC Runs on `knn_gpu` at the start of Map Work Ids, so the cluster (7-13 min to start) is up when `knn` reaches it instead of
# MAGIC starting only then. A shared job cluster stays up until the last task that uses it finishes. Does nothing else.

# COMMAND ----------

print(f"knn_gpu up: {spark.sparkContext.defaultParallelism} cores")
