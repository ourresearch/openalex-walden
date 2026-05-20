# Databricks notebook source
# MAGIC %md
# MAGIC # Delete invalid DynamoDB entries
# MAGIC
# MAGIC Purges bad-cohort mappings from the taxicab DynamoDB tables in `us-east-1`:
# MAGIC `harvested-pdf` and `grobid-xml`. These tables back the taxicab API
# MAGIC (`/taxicab/doi/<doi>` etc.); without this purge, stale entries for our
# MAGIC bad cohort keep showing up when browsing the API even though the
# MAGIC underlying R2 bytes and D1 rows are already gone. Oxjob #185.
# MAGIC
# MAGIC Pairs with:
# MAGIC   - `notebooks/scraping/delete_invalid_r2_objects.py` (R2 bytes)
# MAGIC   - `notebooks/scraping/strip_invalid_pdfs_from_pdf_works.py` (pdf_works strip)
# MAGIC   - `scripts/run_d1_delete_invalid_pdfs.py` (D1 rows)
# MAGIC
# MAGIC ## Sources (mirrors the R2 delete notebook for symmetry)
# MAGIC
# MAGIC - **PDF entries**: `openalex.pdf.invalid_pdfs.source_pdf_id` → DDB `id`
# MAGIC   in `harvested-pdf` (partition key, bare UUID, no suffix).
# MAGIC - **XML entries**: `pdf_works @ VERSION AS OF <pre-strip>` → for each row
# MAGIC   whose `docs.pdf` id was in cohort, take the `docs.parsed-pdf` id on the
# MAGIC   same row, strip the `.xml.gz` suffix → DDB `id` in `grobid-xml`.
# MAGIC   (DDB stores the bare UUID; pdf_works stores `<uuid>.xml.gz`.)
# MAGIC
# MAGIC ## Approach
# MAGIC
# MAGIC Spark `mapPartitions` over the id list, boto3 `Table.batch_writer()` per
# MAGIC partition. `batch_writer()` auto-batches into BatchWriteItem (25/req max)
# MAGIC and auto-retries `UnprocessedItems`. Tables are `PAY_PER_REQUEST`; DDB
# MAGIC auto-scales.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC DeleteItem on a non-existent key is a no-op (no error, no charge for the
# MAGIC missing item). Safe to re-run.
# MAGIC
# MAGIC ## AWS auth
# MAGIC
# MAGIC `dbutils.secrets.get("webscraper", "aws_access_key_id" / "aws_secret_access_key")`
# MAGIC — same IAM identity as `AWS_ACCESS_KEY_ID_OPENALEX_INGEST` in walden's `.env`.
# MAGIC Verified read+delete against `harvested-pdf` and `grobid-xml` in `us-east-1`.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `target`: `pdfs` / `xmls` / `both` (default `pdfs`)
# MAGIC   - `dry_run`: `true` / `false` (default `true` — counts only, no deletes)
# MAGIC   - `sample_limit`: 0 = full cohort, >0 = pilot mode
# MAGIC   - `pdf_works_version`: Delta version of pdf_works for XML key lookup (pre-strip; typically v4224)
# MAGIC   - `num_partitions`: Spark partitions for the delete (default 256)
# MAGIC   - `threads_per_task`: concurrent batch_writer instances per Spark task (default 8)

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DDB_REGION = "us-east-1"
DDB_PDF_TABLE = "harvested-pdf"
DDB_XML_TABLE = "grobid-xml"

AWS_ACCESS_KEY = dbutils.secrets.get(scope="webscraper", key="aws_access_key_id")
AWS_SECRET_KEY = dbutils.secrets.get(scope="webscraper", key="aws_secret_access_key")

dbutils.widgets.dropdown("target", "pdfs", ["pdfs", "xmls", "both"], "Target (pdfs / xmls / both)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no deletes)")
dbutils.widgets.text("sample_limit", "0", "Sample limit (0 = full)")
dbutils.widgets.text("pdf_works_version", "4224", "pdf_works Delta version for XML key lookup (pre-strip)")
dbutils.widgets.text("num_partitions", "256", "Spark partitions")
dbutils.widgets.text("threads_per_task", "8", "Threads per Spark task")

TARGET = dbutils.widgets.get("target")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
SAMPLE_LIMIT = int(dbutils.widgets.get("sample_limit"))
PDF_WORKS_VERSION = int(dbutils.widgets.get("pdf_works_version"))
NUM_PARTITIONS = int(dbutils.widgets.get("num_partitions"))
THREADS_PER_TASK = int(dbutils.widgets.get("threads_per_task"))

print(f"target={TARGET}  dry_run={DRY_RUN}  sample_limit={SAMPLE_LIMIT:,}  "
      f"pdf_works_version={PDF_WORKS_VERSION}  partitions={NUM_PARTITIONS}  threads={THREADS_PER_TASK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the id list

# COMMAND ----------

def build_pdf_ids():
    """One row per (table, id) where id = source_pdf_id (bare UUID)."""
    limit = f"LIMIT {SAMPLE_LIMIT}" if SAMPLE_LIMIT > 0 else ""
    return spark.sql(f"""
      SELECT
        '{DDB_PDF_TABLE}' AS ddb_table,
        source_pdf_id AS id
      FROM openalex.pdf.invalid_pdfs
      {limit}
    """)


def build_xml_ids():
    """One row per (table, id) where id is the bare UUID of a cascaded GROBID
    XML. Source: pdf_works @ time-travel version (pre-strip). For each
    pdf_works row whose docs.pdf id is in invalid_pdfs, take the docs.parsed-pdf
    id on the same row, strip the '.xml.gz' suffix.
    """
    limit = f"LIMIT {SAMPLE_LIMIT}" if SAMPLE_LIMIT > 0 else ""
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW _pdf_works_pre AS
      SELECT native_id, ids
      FROM openalex.pdf.pdf_works VERSION AS OF {PDF_WORKS_VERSION}
    """)
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW _pre_exploded AS
      SELECT native_id, EXPLODE(ids) AS i
      FROM _pdf_works_pre
    """)
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW _victim_native_ids AS
      SELECT DISTINCT e.native_id
      FROM _pre_exploded e
      JOIN openalex.pdf.invalid_pdfs c
        ON e.i.namespace = 'docs.pdf'
       AND e.i.id        = CONCAT(c.source_pdf_id, '.pdf')
    """)
    # docs.parsed-pdf id is '<uuid>.xml.gz' in pdf_works; DDB stores the bare uuid
    return spark.sql(f"""
      SELECT
        '{DDB_XML_TABLE}' AS ddb_table,
        regexp_replace(e.i.id, '\\\\.xml\\\\.gz$', '') AS id
      FROM _pre_exploded e
      JOIN _victim_native_ids v ON e.native_id = v.native_id
      WHERE e.i.namespace = 'docs.parsed-pdf'
        AND e.i.id IS NOT NULL
      {limit}
    """)


if TARGET == "pdfs":
    df = build_pdf_ids()
elif TARGET == "xmls":
    df = build_xml_ids()
else:  # both
    df = build_pdf_ids().unionByName(build_xml_ids())

df = df.repartition(NUM_PARTITIONS)
n = df.count()
print(f"IDs to delete: {n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-table count preview

# COMMAND ----------

df.groupBy("ddb_table").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE (Spark mapPartitions + boto3 Table.batch_writer)

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — skipping deletes. Set dry_run=false to apply.")
else:
    _AWS_REGION = DDB_REGION
    _AWS_ACCESS_KEY = AWS_ACCESS_KEY
    _AWS_SECRET_KEY = AWS_SECRET_KEY
    _THREADS_PER_TASK = THREADS_PER_TASK

    def delete_partition(rows):
        from collections import defaultdict
        from concurrent.futures import ThreadPoolExecutor

        import boto3
        from botocore.client import Config as BotoConfig
        from botocore.exceptions import ClientError

        ddb = boto3.resource(
            "dynamodb",
            region_name=_AWS_REGION,
            aws_access_key_id=_AWS_ACCESS_KEY,
            aws_secret_access_key=_AWS_SECRET_KEY,
            config=BotoConfig(
                retries={"max_attempts": 10, "mode": "standard"},
                max_pool_connections=_THREADS_PER_TASK * 4,
                connect_timeout=10,
                read_timeout=30,
            ),
        )

        # Group ids by table; one batch_writer per table per worker thread.
        per_table = defaultdict(list)
        for r in rows:
            per_table[r.ddb_table].append(r.id)

        def delete_for_table(table_name, ids):
            # Returns (table_name, deleted, exception, error_sample, sample_id)
            table = ddb.Table(table_name)
            count = 0
            try:
                with table.batch_writer() as writer:
                    for id_val in ids:
                        writer.delete_item(Key={"id": id_val})
                        count += 1
                return table_name, count, 0, None, None
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "?")
                msg = e.response.get("Error", {}).get("Message", "?")[:200]
                return table_name, count, len(ids) - count, f"ClientError {code}: {msg}", ids[count] if count < len(ids) else None
            except Exception as e:
                return table_name, count, len(ids) - count, f"{type(e).__name__}: {str(e)[:200]}", ids[count] if count < len(ids) else None

        # Shard each table's id list across worker threads so a partition with
        # one table still parallelises.
        def shard(lst, n):
            return [lst[i::n] for i in range(min(n, max(1, len(lst))))]

        tasks = []
        for table_name, ids in per_table.items():
            for chunk in shard(ids, _THREADS_PER_TASK):
                if chunk:
                    tasks.append((table_name, chunk))

        if not tasks:
            return iter([])

        with ThreadPoolExecutor(max_workers=_THREADS_PER_TASK) as ex:
            results = list(ex.map(lambda t: delete_for_table(*t), tasks))

        agg: dict = {}
        for table_name, d, x, err_sample, sample_id in results:
            row = agg.setdefault(table_name, {"d": 0, "x": 0, "err": None, "id": None})
            row["d"] += d
            row["x"] += x
            if row["err"] is None and err_sample:
                row["err"] = err_sample
                row["id"] = sample_id
        return iter([
            (table_name, v["d"], v["x"], v["err"], v["id"])
            for table_name, v in agg.items()
        ])

    result_schema = StructType([
        StructField("ddb_table", StringType(), False),
        StructField("deleted", IntegerType(), False),
        StructField("exception", IntegerType(), False),
        StructField("error_sample", StringType(), True),
        StructField("sample_id", StringType(), True),
    ])

    print("Starting mapPartitions delete…")
    t0 = time.time()
    result_rdd = df.rdd.mapPartitions(delete_partition)
    result_df = (
        spark.createDataFrame(result_rdd, schema=result_schema)
        .groupBy("ddb_table")
        .agg(
            F.sum("deleted").alias("deleted"),
            F.sum("exception").alias("exception"),
            F.first("error_sample", ignorenulls=True).alias("error_sample"),
            F.first("sample_id", ignorenulls=True).alias("sample_id"),
        )
    )
    result_df.show(truncate=False)
    elapsed = time.time() - t0
    print(f"DELETE complete in {elapsed:.0f}s")
