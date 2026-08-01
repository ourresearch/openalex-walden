# Databricks notebook source
dbutils.widgets.text("endpoint", "sdg-classifier")
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
batches = [records[i : i + REQUEST_BATCH] for i in range(0, len(records), REQUEST_BATCH)]


def score(batch):
    for attempt in range(5):
        try:
            return w.serving_endpoints.query(name=ENDPOINT, dataframe_records=batch).predictions
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
    return None


if batches:
    warm = time.time()
    score(batches[0][:1])
    print(f"warm-up request: {time.time() - warm:.1f}s")

start = time.time()
with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
    results = list(pool.map(score, batches))
elapsed = time.time() - start

predictions = [p for b in results for p in b]
assert len(predictions) == len(pdf), f"got {len(predictions)} predictions for {len(pdf)} rows"
print(f"scored {len(predictions):,} in {elapsed:.1f}s -> {len(predictions)/max(elapsed,1):.1f} rows/sec")

# COMMAND ----------

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


rows = [
    (str(pdf.iloc[i]["work_id"]), as_sdg(pred)) for i, pred in enumerate(predictions)
]
print(f"empty sdg: {sum(1 for r in rows if not r[1]):,} of {len(rows):,}")

inferred_sdg_df = spark.createDataFrame(rows, output_schema).cache()
print(f"output rows: {inferred_sdg_df.count():,}")

# COMMAND ----------

output_df = inferred_sdg_df.withColumn("created_timestamp", current_timestamp()).select(
    "work_id", "sdg", "created_timestamp"
)

target_table = DeltaTable.forName(spark, "openalex.works.works_sdg_frontfill")
(
    target_table.alias("target")
    .merge(output_df.alias("source"), "target.work_id = source.work_id")
    .whenMatchedUpdate(
        set={"sdg": "source.sdg", "created_timestamp": "source.created_timestamp"}
    )
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"merged {output_df.count():,} rows to works_sdg_frontfill")

# COMMAND ----------

inferred_sdg_df.select("work_id").createOrReplaceTempView("processed_ids")
spark.sql(
    """
    DELETE FROM openalex.works.works_sdg_frontfill_input
    WHERE CAST(work_id AS STRING) IN (SELECT work_id FROM processed_ids)
    """
)
print("removed processed work_ids from works_sdg_frontfill_input")
