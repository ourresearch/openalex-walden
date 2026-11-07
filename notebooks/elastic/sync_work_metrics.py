# Databricks notebook source
# MAGIC %md
# MAGIC # Sync work metrics to Elasticsearch (oxjob #2082)
# MAGIC
# MAGIC `CreateWorksEnriched` recomputes `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` for every
# MAGIC work each night, but they are left out of the content hash (#679), so `updated_date` does not move and
# MAGIC `sync_works` never re-sends them. Without this notebook the works index keeps the values of the night each work was
# MAGIC last re-stamped or the index rebuilt, while the snapshot has today's (7 Nov 2026: 10.8% of works off by > 1% one
# MAGIC night after a rebuild).
# MAGIC
# MAGIC This notebook sends only those three fields, as partial `_update`s, for works whose served value is off by more than:
# MAGIC fwci 1% (relative), `citation_normalized_percentile.value` 0.01, `cited_by_percentile_year` min or max 2 points, or
# MAGIC whose null-ness changed. `updated_date` is never touched: 0 re-stamps.
# MAGIC
# MAGIC **What the index holds** is kept in `openalex.works.es_work_metrics_served` (one row per work: the values and the
# MAGIC `updated_date` the index has). Each run:
# MAGIC 1. Works whose `updated_date` moved (sync_works re-sent them whole), new works, gone works: the state follows Delta.
# MAGIC 2. The rest are compared with Delta; those past the lines above form the push (largest first, non-xpac first, at
# MAGIC    most `max_push`; the rest wait for the next run).
# MAGIC 3. Partial updates to the `works` alias; the state takes the new values for every work the index accepted.
# MAGIC
# MAGIC **After an index rebuild** from `openalex_works` version V, run once with `reseed_version=V` (the state becomes V,
# MAGIC then step 1 catches the works re-stamped since).
# MAGIC
# MAGIC **Revert:** remove the task from the End 2 End (or don't run this notebook). Nothing else depends on it; the next
# MAGIC rebuild serves Delta's values anyway. The pushed values are Delta's own, so there is nothing to roll back.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0
# MAGIC %restart_python

# COMMAND ----------

import time
from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

dbutils.widgets.dropdown("mode", "nightly", ["nightly", "dry_run"])
dbutils.widgets.text("max_push", "40000000")       # cap per run; the rest wait (largest first, non-xpac first)
dbutils.widgets.text("limit", "")                  # trial: push at most this many (overrides max_push when smaller)
dbutils.widgets.text("writers", "32")              # concurrent bulk writers (Spark partitions)
dbutils.widgets.text("chunk_size", "1000")         # update actions per bulk request
dbutils.widgets.text("reseed_version", "")         # after a rebuild: openalex_works version the new index loaded
dbutils.widgets.dropdown("refresh", "false", ["true", "false"])  # refresh the index at the end (refresh_interval is 1h)

MODE = dbutils.widgets.get("mode")
DRY_RUN = MODE == "dry_run"
MAX_PUSH = int(dbutils.widgets.get("max_push") or 0) or None
LIMIT = int(dbutils.widgets.get("limit") or 0) or None
WRITERS = int(dbutils.widgets.get("writers") or 32)
CHUNK = int(dbutils.widgets.get("chunk_size") or 1000)
RESEED_VERSION = dbutils.widgets.get("reseed_version").strip()
REFRESH = dbutils.widgets.get("refresh") == "true"
CAP = min(x for x in (MAX_PUSH, LIMIT) if x) if (MAX_PUSH or LIMIT) else None

WORKS_TABLE = "openalex.works.openalex_works"
SERVED_TABLE = "openalex.works.es_work_metrics_served"
PUSH_TABLE = "openalex.works.es_work_metrics_push"       # this run's push (overwritten each run)
FAILED_TABLE = "openalex.works.es_work_metrics_failed"   # this run's rejected ids (overwritten each run)
RUNS_TABLE = "openalex.works.es_work_metrics_runs"       # one row per run
ELASTIC_INDEX = "works"
ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

FWCI_REL, CNP_ABS = 0.01, 0.01
PCT_POINTS = 1.5  # min or max moved 2+ points (integers: a 1-point move scores 0.67, 2 points 1.33)
started_at = datetime.utcnow()
t_start = time.time()
print(f"mode={MODE} cap={CAP} writers={WRITERS} chunk={CHUNK} reseed_version={RESEED_VERSION or '-'}")

# COMMAND ----------

# Reseed after a rebuild: the new index holds version V's values for every work.
if RESEED_VERSION:
    if not RESEED_VERSION.isdigit():
        raise ValueError(f"reseed_version must be an openalex_works version number, got {RESEED_VERSION!r}")
    if DRY_RUN:
        raise ValueError("reseed_version writes the state table; run it with mode=nightly")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SERVED_TABLE} CLUSTER BY (id) AS
        SELECT id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year,
               current_timestamp() AS served_at, 'rebuild (openalex_works v{RESEED_VERSION})' AS served_by
        FROM {WORKS_TABLE} VERSION AS OF {RESEED_VERSION}
    """)
    print(f"Reseeded {SERVED_TABLE} from {WORKS_TABLE} v{RESEED_VERSION}")

# COMMAND ----------

# Step 1. The index follows Delta for works sync_works re-sent (updated_date moved), new works and gone works.
# Runs after Sync_to_Elasticsearch in the End 2End, so those works already carry tonight's values.
STEP1 = f"""
MERGE INTO {SERVED_TABLE} AS s
USING (SELECT id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year FROM {WORKS_TABLE}) AS c
ON s.id = c.id
WHEN MATCHED AND NOT (s.updated_date <=> c.updated_date) THEN UPDATE SET
  s.updated_date = c.updated_date, s.fwci = c.fwci, s.citation_normalized_percentile = c.citation_normalized_percentile,
  s.cited_by_percentile_year = c.cited_by_percentile_year, s.served_at = current_timestamp(), s.served_by = 'sync_works'
WHEN NOT MATCHED THEN INSERT (id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year, served_at, served_by)
  VALUES (c.id, c.updated_date, c.fwci, c.citation_normalized_percentile, c.cited_by_percentile_year, current_timestamp(), 'sync_works')
WHEN NOT MATCHED BY SOURCE THEN DELETE
"""
if DRY_RUN:
    step1 = spark.sql(f"""
        SELECT count_if(s.id IS NULL) AS new_works, count_if(s.id IS NOT NULL AND NOT (s.updated_date <=> c.updated_date)) AS resent
        FROM {WORKS_TABLE} c LEFT JOIN {SERVED_TABLE} s ON s.id = c.id
    """).collect()[0].asDict()
    print(f"dry_run step 1 (not written): {step1}")
else:
    m = spark.sql(STEP1).collect()[0].asDict()
    step1 = {k: m.get(k) for k in ("num_updated_rows", "num_inserted_rows", "num_deleted_rows")}
    print(f"Step 1: {step1}")

# COMMAND ----------

# Step 2. The push: works past the lines, largest move first, non-xpac first, at most CAP.
# In dry_run, works sync_works would have re-sent are left out the same way (their updated_date differs).
DIFF = f"""
SELECT c.id, coalesce(c.is_xpac, false) AS is_xpac, c.fwci, c.citation_normalized_percentile, c.cited_by_percentile_year,
  greatest(
    coalesce(abs(c.fwci - s.fwci) / greatest(abs(c.fwci), 0.0001) / {FWCI_REL}, 0),
    coalesce(abs(c.citation_normalized_percentile.value - s.citation_normalized_percentile.value) / {CNP_ABS}, 0),
    coalesce(greatest(abs(c.cited_by_percentile_year.min - s.cited_by_percentile_year.min),
                      abs(c.cited_by_percentile_year.max - s.cited_by_percentile_year.max)) / {PCT_POINTS}, 0),
    IF((c.fwci IS NULL) <> (s.fwci IS NULL)
       OR (c.citation_normalized_percentile.value IS NULL) <> (s.citation_normalized_percentile.value IS NULL)
       OR (c.cited_by_percentile_year IS NULL) <> (s.cited_by_percentile_year IS NULL)
       OR NOT (c.citation_normalized_percentile.is_in_top_1_percent <=> s.citation_normalized_percentile.is_in_top_1_percent)
       OR NOT (c.citation_normalized_percentile.is_in_top_10_percent <=> s.citation_normalized_percentile.is_in_top_10_percent),
       1000, 0)
  ) AS score
FROM {WORKS_TABLE} c JOIN {SERVED_TABLE} s ON s.id = c.id AND s.updated_date <=> c.updated_date
"""
spark.sql(f"""
    CREATE OR REPLACE TABLE {PUSH_TABLE} AS
    SELECT * FROM ({DIFF}) WHERE score > 1
""")
over = spark.sql(f"""
    SELECT count(*) AS n, count_if(NOT is_xpac) AS n_default, count_if(score >= 5) AS n_5x
    FROM {PUSH_TABLE}
""").collect()[0]
print(f"Past the lines: {over.n:,} works ({over.n_default:,} non-xpac; {over.n_5x:,} at 5x the line or more)")

if CAP and over.n > CAP:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {PUSH_TABLE} AS
        SELECT * FROM {PUSH_TABLE} ORDER BY is_xpac, score DESC, id LIMIT {CAP}
    """)
    print(f"Capped at {CAP:,}; {over.n - CAP:,} wait for the next run")
n_push = spark.table(PUSH_TABLE).count()
print(f"Push: {n_push:,} works")

# COMMAND ----------

# Step 3. Partial updates. Only these three fields; never an upsert (a missing doc stays missing: sync_works owns docs).
def send(rows):
    client = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True,
                           http_compress=True)
    ok = missing = 0
    failed = []

    def actions():
        for r in rows:
            cnp = r.citation_normalized_percentile
            py = r.cited_by_percentile_year
            yield {
                "_op_type": "update",
                "_index": ELASTIC_INDEX,
                "_id": f"https://openalex.org/W{r.id}",
                "retry_on_conflict": 3,
                "doc": {
                    "fwci": r.fwci,
                    "citation_normalized_percentile": None if cnp is None else cnp.asDict(),
                    "cited_by_percentile_year": None if py is None else py.asDict(),
                },
            }

    try:
        for success, info in helpers.streaming_bulk(client, actions(), chunk_size=CHUNK, raise_on_error=False,
                                                    raise_on_exception=False, max_retries=5, initial_backoff=2):
            if success:
                ok += 1
                continue
            item = info.get("update", {})
            wid = int(str(item.get("_id", "W-1")).rsplit("W", 1)[1])
            if item.get("status") == 404:
                missing += 1          # not in the index (sync_works' to fix); counted, state follows Delta
            else:
                failed.append((wid, str(item.get("error"))[:500]))
    finally:
        client.close()
    yield ("counts", ok, missing, len(failed))
    for wid, err in failed[:100000]:
        yield ("failed", wid, 0, err)


t_send = time.time()
if DRY_RUN or n_push == 0:
    sent = {"ok": 0, "missing": 0, "failed": 0}
    print("dry_run: nothing sent" if DRY_RUN else "Nothing to send")
    spark.createDataFrame([], "id BIGINT, error STRING").write.mode("overwrite").saveAsTable(FAILED_TABLE)
else:
    out = (spark.table(PUSH_TABLE).repartition(WRITERS)
           .rdd.mapPartitions(send)
           .map(lambda t: (t[0], int(t[1]), int(t[2]) if t[0] == "counts" else 0, str(t[3])))
           .toDF(StructType([StructField("kind", StringType()), StructField("a", LongType()),
                             StructField("b", LongType()), StructField("c", StringType())])))
    out.write.mode("overwrite").saveAsTable(FAILED_TABLE + "_raw")   # one action: the RDD is sent once
    raw = spark.table(FAILED_TABLE + "_raw")
    agg = raw.where("kind = 'counts'").agg(F.sum("a").alias("ok"), F.sum("b").alias("missing"),
                                           F.sum(F.col("c").cast("long")).alias("failed")).collect()[0]
    sent = {"ok": int(agg.ok or 0), "missing": int(agg.missing or 0), "failed": int(agg.failed or 0)}
    raw.where("kind = 'failed'").select(F.col("a").alias("id"), F.col("c").alias("error")) \
        .write.mode("overwrite").saveAsTable(FAILED_TABLE)
    spark.sql(f"DROP TABLE IF EXISTS {FAILED_TABLE}_raw")
secs = time.time() - t_send
print(f"Sent in {secs/60:.1f} min: {sent} ({(sent['ok'] + sent['missing']) / max(secs, 1):,.0f} docs/s)")

# COMMAND ----------

# Step 4. The state takes the pushed values for every work the index accepted (or doesn't hold).
if not DRY_RUN and n_push:
    m = spark.sql(f"""
        MERGE INTO {SERVED_TABLE} AS s
        USING (SELECT p.* FROM {PUSH_TABLE} p LEFT ANTI JOIN {FAILED_TABLE} f ON f.id = p.id) AS p
        ON s.id = p.id
        WHEN MATCHED THEN UPDATE SET
          s.fwci = p.fwci, s.citation_normalized_percentile = p.citation_normalized_percentile,
          s.cited_by_percentile_year = p.cited_by_percentile_year, s.served_at = current_timestamp(),
          s.served_by = 'sync_work_metrics'
    """).collect()[0].asDict()
    print(f"Step 4: state updated for {m.get('num_updated_rows'):,} works")

if REFRESH and not DRY_RUN and n_push:
    c = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=600)
    c.indices.refresh(index=ELASTIC_INDEX, request_timeout=600)
    c.close()
    print(f"Refreshed {ELASTIC_INDEX}")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
      started_at TIMESTAMP, mode STRING, reseed_version STRING, resent_or_new BIGINT, gone BIGINT,
      past_lines BIGINT, pushed BIGINT, ok BIGINT, missing BIGINT, failed BIGINT, send_seconds DOUBLE, total_seconds DOUBLE
    )
""")
row = [(started_at, MODE, RESEED_VERSION or None,
        int((step1.get("num_updated_rows") or 0) + (step1.get("num_inserted_rows") or 0)) if not DRY_RUN
        else int(step1["resent"] + step1["new_works"]),
        int(step1.get("num_deleted_rows") or 0) if not DRY_RUN else 0,
        int(over.n), int(n_push), sent["ok"], sent["missing"], sent["failed"], float(secs), float(time.time() - t_start))]
spark.createDataFrame(row, spark.table(RUNS_TABLE).schema).write.mode("append").saveAsTable(RUNS_TABLE)
print(row)

# A run with many rejected updates fails loudly; the state already skips them, so the next run retries them.
if sent["failed"] > max(1000, 0.001 * n_push):
    raise Exception(f"{sent['failed']:,} of {n_push:,} updates rejected; see {FAILED_TABLE}")
