# Databricks notebook source
%pip install elasticsearch==8.19.0

# COMMAND ----------

import uuid
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

# Watermark-incremental (oxjob #2287), as sync_works and sync_locations (#850): send what was stamped since the last
# clean run started, kept in the shared state table under the index name. Replaces a fixed 2-day window that sent
# every build's stamps two mornings running.
SYNC_STATE_TABLE = "openalex.works.elastic_sync_state"
FALLBACK_LOOKBACK_DAYS = 2     # no watermark row yet: midnight UTC two days back, the window before #2287
# Clock skew only: the Authors build stamps minutes before this task, so a longer buffer re-sends the previous build.
WATERMARK_BUFFER_SECONDS = 60
ERROR_TOLERANCE = 500          # a run with fewer doc errors still advances (sync_works 08-18: 1 error froze a 44M re-send)

# Captured before the works-sync wait and any read; becomes the next watermark only if this run finishes clean.
sync_started_at = datetime.utcnow()

# COMMAND ----------

# index_name is a widget so a rebuild can load a fresh index beside the live one (oxjob #1531: authors-v20; #1617: authors-v21; #1565: authors-v22); the job yaml passes it.
dbutils.widgets.text("index_name", "authors-v22")
CONFIG = {
    "table_name": "openalex.authors.openalex_authors",
    "index_name": dbutils.widgets.get("index_name")
}

# oxjob #1617: the keys OQL's `at [X] since Y` and `in [country] since Y` read (one SQL
# expression each, so the same text runs on a SQL warehouse to check it)
INSTITUTION_YEARS_SQL = """
filter(array_distinct(flatten(filter(transform(affiliations, a -> flatten(filter(transform(
    CASE WHEN size(a.institution.lineage) > 0 THEN a.institution.lineage
         ELSE array(a.institution.id) END,
    inst -> transform(a.years, y -> concat(regexp_extract(inst, '([A-Za-z][0-9]+)$', 1), ':',
                                           cast(y AS string)))),
    x -> x IS NOT NULL))), x -> x IS NOT NULL))),
  k -> k IS NOT NULL AND NOT startswith(k, ':'))
"""
COUNTRY_YEARS_SQL = """
filter(array_distinct(flatten(filter(transform(affiliations, a -> transform(a.years,
    y -> concat(upper(a.institution.country_code), ':', cast(y AS string)))),
    x -> x IS NOT NULL))),
  k -> k IS NOT NULL)
"""

dbutils.widgets.text("is_full_sync", "false")
# since_date (oxjob #2287): catch-up from a UTC date or time ("2026-11-14", "2026-11-14 12:00:00"); the watermark is
# untouched, like sync_works' since_ts. dry_run: report what would be sent and stop, before the works-sync wait.
dbutils.widgets.text("since_date", "")
dbutils.widgets.text("dry_run", "false")
IS_FULL_SYNC = dbutils.widgets.get("is_full_sync").lower() == "true"
SINCE_DATE = dbutils.widgets.get("since_date").strip()
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"
print(f"IS_FULL_SYNC: {IS_FULL_SYNC}  SINCE_DATE: {SINCE_DATE or '-'}  DRY_RUN: {DRY_RUN}")
if IS_FULL_SYNC and SINCE_DATE:
    raise ValueError("is_full_sync and since_date can't both be set")

def read_watermark():
    if not spark.catalog.tableExists(SYNC_STATE_TABLE):
        return None
    rows = spark.sql(
        f"SELECT last_sync_started_at FROM {SYNC_STATE_TABLE} WHERE index_name = '{CONFIG['index_name']}'"
    ).collect()
    return rows[0].last_sync_started_at if rows else None

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        retry_on_timeout=True,
        request_timeout=180
    )

    def generate_actions(op_type = "index"):
        for row in partition:
            yield {
                "_op_type": op_type,
                "_index": CONFIG["index_name"],
                "_id": row.id,
                "_source": row._source.asDict(True)
            }

    # oxjob #2287: failed docs are counted and returned, so the watermark advances only after a clean run (the old code
    # dropped the rest of a partition at its first failure, silently). Past 1000 failures ES is in trouble: stop here.
    count, n_errors, reasons = 0, 0, []
    try:
        for success, info in helpers.parallel_bulk(
            client,
            generate_actions(),
            chunk_size=500,
            thread_count=4,
            raise_on_error=False,
            raise_on_exception=False
        ):
            if success:
                count += 1
                continue
            n_errors += 1
            if len(reasons) < 5:
                reasons.append(str(info)[:500])
            if n_errors > 1000:
                break
    finally:
        client.close()

    if n_errors:
        print(f"PARTITION HAD {n_errors} FAILED DOCS (indexed {count}) to {index_name}"
              f"{'; stopped early' if n_errors > 1000 else ''}. First reasons:")
        for r in reasons:
            print(r)
    yield (count, n_errors)

# COMMAND ----------

# Which rows: everything (full sync), a since_date catch-up, or everything stamped since the watermark.
if IS_FULL_SYNC:
    sync_since, since_from = None, "full sync"
elif SINCE_DATE:
    sync_since, since_from = datetime.fromisoformat(SINCE_DATE), "since_date (watermark untouched)"
else:
    watermark = read_watermark()
    if watermark is not None:
        sync_since = watermark - timedelta(seconds=WATERMARK_BUFFER_SECONDS)
        since_from = f"watermark {watermark} minus {WATERMARK_BUFFER_SECONDS}s"
    else:
        sync_since = (sync_started_at - timedelta(days=FALLBACK_LOOKBACK_DAYS)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        since_from = f"no watermark for {CONFIG['index_name']}: {FALLBACK_LOOKBACK_DAYS}-day fallback"
print(f"Selecting updated_date >= {sync_since} UTC ({since_from})")

df = spark.table(CONFIG["table_name"])
if sync_since is not None:
    df = df.filter(F.col("updated_date") >= sync_since.strftime("%Y-%m-%d %H:%M:%S"))

if DRY_RUN:
    # The exit value is the run's output (it replaces this cell's prints), so the count goes in it.
    dbutils.notebook.exit(f"DRY RUN: {df.count():,} authors with updated_date >= {sync_since} UTC ({since_from}) "
                          f"would go to {CONFIG['index_name']}; nothing sent")

# COMMAND ----------

# oxjob #1642 (1 Nov 2026): don't write authors while the End 2 End's works sync is writing. Since #1949 this job runs
# right after each 12:00 UTC Authors build, so on a night that runs late it lands on Sync_to_Elasticsearch: on 1 Nov at
# 06:31 CT (26-node base) the two together made the API log ~340 main-ES ConnectionTimeouts in one minute. Wait (up to
# 3 h) while that task runs. Any error in this check proceeds, so the wait can never stop the sync.
from databricks.sdk import WorkspaceClient

E2E_JOB_ID, E2E_TASK, WAIT_MAX_S = 616701029470182, "Sync_to_Elasticsearch", 3 * 3600


def works_sync_running():
    w = WorkspaceClient()
    for r in w.jobs.list_runs(job_id=E2E_JOB_ID, active_only=True):
        run = w.jobs.get_run(r.run_id)
        if any(t.task_key == E2E_TASK and t.state and str(t.state.life_cycle_state).endswith("RUNNING") for t in run.tasks or []):
            return True
    return False


t0 = time.time()
try:
    while works_sync_running() and time.time() - t0 < WAIT_MAX_S:
        print(f"End 2 End {E2E_TASK} is running: waiting ({(time.time() - t0) / 60:.0f} min so far)", flush=True)
        time.sleep(120)
except Exception as e:
    print(f"works-sync wait skipped: {e}")
print(f"works-sync wait: {(time.time() - t0) / 60:.0f} min")

# COMMAND ----------

# Set replicas to 0 for faster bulk indexing during full sync
if IS_FULL_SYNC:
    try:
        client = Elasticsearch(
            hosts=[ELASTIC_URL],
            request_timeout=180,
            max_retries=5,
            retry_on_timeout=True
        )
        if client.indices.exists(index=CONFIG["index_name"]):
            client.indices.put_settings(index=CONFIG["index_name"], body={
                "index": {"number_of_replicas": 0}
            })
            print(f"Set replicas to 0 on {CONFIG['index_name']} for full sync")
        else:
            print(f"Index {CONFIG['index_name']} does not exist yet - will create with default settings")
    finally:
        client.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Sync

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

totals, total_errors = None, 0
try:
    df = (df
        .withColumn("id", F.concat(F.lit("https://openalex.org/A"), F.col("id").cast("string")))
        .withColumn("topics", F.slice(F.col("topics"), 1, 5))
        .withColumn("topic_share", F.slice(F.col("topic_share"), 1, 5))
        .withColumn("display_name_alternatives", F.col("raw_author_names"))
        # Institution-year and country-year keys (oxjob #1617), so OQL's `at [UBC](I141945490)
        # since 2022` is one terms query: "I141945490:2022" for the institution and every
        # institution in its lineage (a department's years count for its university), and
        # "CA:2022" for the institution's country, in each year the record lists it.
        .withColumn("institution_years", F.expr(INSTITUTION_YEARS_SQL))
        .withColumn("country_years", F.expr(COUNTRY_YEARS_SQL))
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    print(f"Total records to process: {df.count():,}")
    df = df.repartition(1024)

    results_rdd = df.rdd.mapPartitions(lambda p: send_partition_to_elastic(p, CONFIG["index_name"]))
    # ONE action only: a second action would recompute the RDD and re-send every partition.
    totals = spark.createDataFrame(results_rdd, "indexed_count LONG, error_count LONG").agg(
        F.count("*").alias("partitions"),
        F.sum("indexed_count").alias("indexed"),
        F.sum("error_count").alias("errors"),
    ).collect()[0]
    total_errors = int(totals.errors or 0)
    print(f"Processed {totals.partitions} partitions; indexed {totals.indexed or 0:,} docs, {total_errors} doc errors")

    print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")

except Exception as e:
    print(f"Failed to process {CONFIG['table_name']}: {e}")
    log.error(f"Failed to process {CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nIndexing operation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advance Watermark

# COMMAND ----------

# As sync_locations (#850): a failed or partial run leaves the watermark in place and the next run re-covers the gap.
# Under ERROR_TOLERANCE doc errors it still advances; those authors go again only when re-stamped.
if SINCE_DATE:
    print("since_date catch-up: watermark not touched")
elif totals is None:
    print("Watermark NOT advanced: the sync failed (see above). The next run re-covers the gap.")
elif total_errors >= ERROR_TOLERANCE:
    print(f"Watermark NOT advanced: {total_errors} doc errors (tolerance {ERROR_TOLERANCE}). "
          "The incremental window regrows until a clean run.")
else:
    if total_errors:
        print(f"Tolerating {total_errors} doc errors (< {ERROR_TOLERANCE})")
    stamp = sync_started_at.strftime("%Y-%m-%d %H:%M:%S")
    # Other syncs MERGE their own rows of this table the same morning, hence the retry (as BuildLakebaseWorksDocs).
    for attempt in range(3):
        try:
            spark.sql(f"""MERGE INTO {SYNC_STATE_TABLE} AS t
USING (SELECT '{CONFIG['index_name']}' AS index_name,
              TIMESTAMP'{stamp}' AS last_sync_started_at,
              CURRENT_TIMESTAMP() AS updated_at) AS s
ON t.index_name = s.index_name
WHEN MATCHED THEN UPDATE SET t.last_sync_started_at = s.last_sync_started_at,
                             t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (index_name, last_sync_started_at, updated_at)
VALUES (s.index_name, s.last_sync_started_at, s.updated_at)
""")
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f"Watermark MERGE attempt {attempt + 1} failed ({e}); retrying in 30 s")
            time.sleep(30)
    print(f"Watermark for {CONFIG['index_name']} advanced to {stamp} (UTC)")

# COMMAND ----------

# refresh and restore replicas
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    request_timeout=180,
    max_retries=5,
    retry_on_timeout=True
)

client.indices.refresh(index=CONFIG["index_name"])
print(f"Refreshed index {CONFIG['index_name']}")

if IS_FULL_SYNC:
    client.indices.put_settings(index=CONFIG["index_name"], body={
        "index": {"number_of_replicas": 1}
    })
    print(f"Restored replicas to 1 on {CONFIG['index_name']}")

client.close()
