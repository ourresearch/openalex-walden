# Databricks notebook source
dbutils.widgets.text("endpoint", "topic-classifier")
dbutils.widgets.text("concurrency", "24")
dbutils.widgets.text("request_batch", "25")
dbutils.widgets.text("max_rows", "3840000")
dbutils.widgets.text("chunk_works", "100000")

ENDPOINT = dbutils.widgets.get("endpoint")
CONCURRENCY = int(dbutils.widgets.get("concurrency"))
REQUEST_BATCH = int(dbutils.widgets.get("request_batch"))
MAX_ROWS = int(dbutils.widgets.get("max_rows"))
CHUNK_WORKS = int(dbutils.widgets.get("chunk_works"))

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
work_ids = pdf["work_id"].astype(str).tolist()
titles = pdf["title"].tolist()
abstracts = pdf["abstract"].tolist()
journals = pdf["journal_name"].tolist()
del pdf


def score(batch):
    for attempt in range(5):
        try:
            return w.serving_endpoints.query(name=ENDPOINT, dataframe_records=batch).predictions
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
    return None


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


# COMMAND ----------

# Score -> build -> append -> dequeue in chunks: bounds driver memory, writes
# incrementally, and because processed work_ids leave work_topics_input per
# chunk, a retried run resumes where it stopped instead of re-scoring the wave.
if records:
    warm = time.time()
    score(records[:1])
    print(f"warm-up request: {time.time() - warm:.1f}s")

total_written = 0
total_declined = 0
overall_start = time.time()

for chunk_start in range(0, len(records), CHUNK_WORKS):
    chunk_records = records[chunk_start : chunk_start + CHUNK_WORKS]
    batches = [
        chunk_records[i : i + REQUEST_BATCH]
        for i in range(0, len(chunk_records), REQUEST_BATCH)
    ]
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        results = list(pool.map(score, batches))
    preds = [p for b in results for p in b]
    assert len(preds) == len(chunk_records), (
        f"got {len(preds)} predictions for {len(chunk_records)} rows"
    )

    rows = [
        (
            work_ids[chunk_start + i],
            titles[chunk_start + i],
            abstracts[chunk_start + i],
            journals[chunk_start + i],
            as_topics(pred),
        )
        for i, pred in enumerate(preds)
    ]
    declined = sum(1 for r in rows if r[4] is None)

    res_df = (
        spark.createDataFrame(rows, output_schema)
        .withColumn("lm_primary_topic", col("lm_topics")[0])
        .withColumn("source", lit("bert_lm"))
        .withColumn("created_timestamp", current_timestamp())
    )
    res_df.write.mode("append").saveAsTable("openalex.works.work_topics_lm_output")

    res_df.select("work_id").createOrReplaceTempView("res_df_temp")
    spark.sql(
        """
        DELETE FROM openalex.works.work_topics_input
        WHERE CAST(work_id AS STRING) IN (SELECT work_id FROM res_df_temp)
        """
    )

    total_written += len(rows)
    total_declined += declined
    rate = total_written / max(time.time() - overall_start, 1)
    print(
        f"{total_written:,}/{len(records):,} written "
        f"(declined {total_declined:,}) at {rate:.0f} rows/sec",
        flush=True,
    )

print(f"done: {total_written:,} written, {total_declined:,} declined (NULL lm_topics)")
