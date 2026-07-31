# Databricks notebook source
# MAGIC %md
# MAGIC oxjob #709 — parity + throughput of the `topic-classifier` serving endpoint
# MAGIC against the current GPU-cluster method. Run on a small CPU cluster; the GPU
# MAGIC work happens on the endpoint.

# COMMAND ----------

dbutils.widgets.text("sample_size", "2000")
dbutils.widgets.text("endpoint", "topic-classifier")
dbutils.widgets.text("request_batch", "25")
dbutils.widgets.text("concurrency", "8")

SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))
ENDPOINT = dbutils.widgets.get("endpoint")
REQUEST_BATCH = int(dbutils.widgets.get("request_batch"))
CONCURRENCY = int(dbutils.widgets.get("concurrency"))

# COMMAND ----------

import sys

sys.path.append("/Workspace/Shared/openalex-walden/notebooks/topics")
from topic_predictor import clean_title, clean_abstract, is_heavily_stripped

# COMMAND ----------

baseline = spark.sql(
    f"""
    SELECT work_id, title, abstract, lm_topics
    FROM openalex.works.work_topics_lm_output
    WHERE created_timestamp >= current_date() - INTERVAL 1 DAY
      AND lm_topics IS NOT NULL
      AND SIZE(lm_topics) = 3
    ORDER BY RANDOM()
    LIMIT {SAMPLE_SIZE}
    """
).toPandas()

print(f"baseline rows: {len(baseline):,}")

# COMMAND ----------


def build_text(row):
    title = clean_title(row["title"]) or ""
    abstract = clean_abstract(row["abstract"]) or ""
    if is_heavily_stripped(row["title"], title) and (
        is_heavily_stripped(row["abstract"], abstract) or not row["abstract"]
    ):
        return None
    return f"[CLS]<TITLE> {title.strip()} <ABSTRACT> {abstract.strip()} [SEP]"


baseline["text"] = baseline.apply(build_text, axis=1)
skipped = baseline["text"].isna().sum()
baseline = baseline[baseline["text"].notna()].reset_index(drop=True)
print(f"skipped by cleaning rule: {skipped} | scoring: {len(baseline):,}")

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

texts = baseline["text"].tolist()
batches = [texts[i : i + REQUEST_BATCH] for i in range(0, len(texts), REQUEST_BATCH)]


def score(batch):
    resp = w.serving_endpoints.query(
        name=ENDPOINT,
        dataframe_records=[{"text": t} for t in batch],
    )
    return resp.predictions


start = time.time()
with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
    results = list(pool.map(score, batches))
elapsed = time.time() - start

predictions = [p for batch in results for p in batch]
print(f"scored {len(predictions):,} in {elapsed:.1f}s -> {len(predictions)/elapsed:.1f} rows/sec")
print(f"concurrency={CONCURRENCY} request_batch={REQUEST_BATCH}")

# COMMAND ----------


def as_pairs(topics):
    if isinstance(topics, dict):
        topics = topics.get("topics", topics)
    return [(int(t["topic_id"]), float(t["score"])) for t in topics]


rows = []
for i, pred in enumerate(predictions):
    old = as_pairs(baseline.loc[i, "lm_topics"])
    new = as_pairs(pred if not isinstance(pred, dict) else pred["topics"])
    old_ids = [t[0] for t in old]
    new_ids = [t[0] for t in new]
    rows.append(
        {
            "work_id": baseline.loc[i, "work_id"],
            "top1_match": old_ids[0] == new_ids[0],
            "top3_set_match": set(old_ids) == set(new_ids),
            "top3_exact_match": old_ids == new_ids,
            "max_score_delta": max(
                (abs(o[1] - n[1]) for o, n in zip(old, new) if o[0] == n[0]), default=None
            ),
            "old_ids": old_ids,
            "new_ids": new_ids,
        }
    )

import pandas as pd

cmp = pd.DataFrame(rows)
n = len(cmp)
print(f"n                = {n:,}")
print(f"top-1 match      = {cmp['top1_match'].mean():.4%}")
print(f"top-3 set match  = {cmp['top3_set_match'].mean():.4%}")
print(f"top-3 exact      = {cmp['top3_exact_match'].mean():.4%}")
print(f"max score delta  = {cmp['max_score_delta'].max():.6f}")

# COMMAND ----------

display(cmp[~cmp["top3_set_match"]].head(50))

# COMMAND ----------

spark.createDataFrame(cmp.astype({"work_id": "str"})).write.mode("overwrite").saveAsTable(
    "openalex.works.qa_oxjob709_endpoint_parity"
)
