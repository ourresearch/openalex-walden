# Databricks notebook source
dbutils.widgets.text("endpoint", "sdg-classifier")
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
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

pdf = (
    spark.table("openalex.works.works_sdg_frontfill_input")
    .select("work_id", "title", "abstract")
    .limit(MAX_ROWS)
    .toPandas()
)
print(f"input rows: {len(pdf):,}")

# COMMAND ----------

w = WorkspaceClient()
records = pdf[["title", "abstract"]].where(pdf[["title", "abstract"]].notna(), None).to_dict("records")
work_ids = pdf["work_id"].astype(str).tolist()
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


sdg_struct = StructType(
    [
        StructField("id", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("score", FloatType(), True),
    ]
)
output_schema = StructType(
    [
        StructField("work_id", StringType(), nullable=False),
        StructField("sdg", ArrayType(sdg_struct), nullable=True),
    ]
)


def as_sdg(pred):
    sdg = pred.get("sdg") if isinstance(pred, dict) else pred
    if sdg is None:
        return []
    return [(s["id"], s["display_name"], float(s["score"])) for s in sdg]


# COMMAND ----------

# Score -> build -> merge -> dequeue in chunks: bounds driver memory, writes
# incrementally, and because processed work_ids leave the input table per
# chunk, a retried run resumes where it stopped instead of re-scoring the wave.
if records:
    warm = time.time()
    score(records[:1])
    print(f"warm-up request: {time.time() - warm:.1f}s")

target_table = DeltaTable.forName(spark, "openalex.works.works_sdg_frontfill")
total_written = 0
total_empty = 0
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
        (work_ids[chunk_start + i], as_sdg(pred)) for i, pred in enumerate(preds)
    ]
    empty = sum(1 for r in rows if not r[1])

    inferred_sdg_df = spark.createDataFrame(rows, output_schema)
    output_df = inferred_sdg_df.withColumn(
        "created_timestamp", current_timestamp()
    ).select("work_id", "sdg", "created_timestamp")

    (
        target_table.alias("target")
        .merge(output_df.alias("source"), "target.work_id = source.work_id")
        .whenMatchedUpdate(
            set={"sdg": "source.sdg", "created_timestamp": "source.created_timestamp"}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    inferred_sdg_df.select("work_id").createOrReplaceTempView("processed_ids")
    spark.sql(
        """
        DELETE FROM openalex.works.works_sdg_frontfill_input
        WHERE CAST(work_id AS STRING) IN (SELECT work_id FROM processed_ids)
        """
    )

    total_written += len(rows)
    total_empty += empty
    rate = total_written / max(time.time() - overall_start, 1)
    print(
        f"{total_written:,}/{len(records):,} merged "
        f"(empty sdg {total_empty:,}) at {rate:.0f} rows/sec",
        flush=True,
    )

print(f"done: {total_written:,} merged, {total_empty:,} empty sdg")
