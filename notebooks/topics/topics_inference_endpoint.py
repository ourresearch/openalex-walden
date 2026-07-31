# Databricks notebook source
dbutils.widgets.text("endpoint", "topic-classifier")
dbutils.widgets.text("concurrency", "24")
dbutils.widgets.text("request_batch", "25")
dbutils.widgets.text("max_rows", "3840000")

ENDPOINT = dbutils.widgets.get("endpoint")
CONCURRENCY = int(dbutils.widgets.get("concurrency"))
REQUEST_BATCH = int(dbutils.widgets.get("request_batch"))
MAX_ROWS = int(dbutils.widgets.get("max_rows"))

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

pdf = (
    spark.table("openalex.works.work_topics_input")
    .select("work_id", "title", "abstract", "journal_name")
    .limit(MAX_ROWS)
    .toPandas()
)
print(f"input rows: {len(pdf):,}")

# COMMAND ----------

w = WorkspaceClient()
records = pdf[["title", "abstract"]].where(pdf[["title", "abstract"]].notna(), None).to_dict("records")
batches = [records[i : i + REQUEST_BATCH] for i in range(0, len(records), REQUEST_BATCH)]


def score(batch):
    for attempt in range(5):
        try:
            return w.serving_endpoints.query(name=ENDPOINT, dataframe_records=batch).predictions
        except Exception as e:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
    return None


start = time.time()
with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
    results = list(pool.map(score, batches))
elapsed = time.time() - start

predictions = [p for b in results for p in b]
assert len(predictions) == len(pdf), f"got {len(predictions)} predictions for {len(pdf)} rows"
print(f"scored {len(predictions):,} in {elapsed:.1f}s -> {len(predictions)/max(elapsed,1):.1f} rows/sec")

# COMMAND ----------

topic_struct = StructType(
    [
        StructField("topic_id", IntegerType(), True),
        StructField("score", FloatType(), True),
    ]
)
output_schema = StructType(
    [
        StructField("work_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("journal_name", StringType(), True),
        StructField("lm_topics", ArrayType(topic_struct), True),
    ]
)


def as_topics(pred):
    topics = pred.get("topics") if isinstance(pred, dict) else pred
    if topics is None or len(topics) == 0:
        return None
    return [(int(t["topic_id"]), float(t["score"])) for t in topics]


rows = [
    (
        str(pdf.iloc[i]["work_id"]),
        pdf.iloc[i]["title"],
        pdf.iloc[i]["abstract"],
        pdf.iloc[i]["journal_name"],
        as_topics(pred),
    )
    for i, pred in enumerate(predictions)
]
declined = sum(1 for r in rows if r[4] is None)
print(f"declined (NULL lm_topics): {declined:,} of {len(rows):,}")

res_df = (
    spark.createDataFrame(rows, output_schema)
    .withColumn("lm_primary_topic", col("lm_topics")[0])
    .withColumn("source", lit("bert_lm"))
    .withColumn("created_timestamp", current_timestamp())
).cache()
print(f"output rows: {res_df.count():,}")

res_df.write.mode("append").saveAsTable("openalex.works.work_topics_lm_output")

# COMMAND ----------

res_df.select("work_id").createOrReplaceTempView("res_df_temp")
spark.sql(
    """
    DELETE FROM openalex.works.work_topics_input
    WHERE CAST(work_id AS STRING) IN (SELECT work_id FROM res_df_temp)
    """
)
print("removed processed work_ids from work_topics_input")
