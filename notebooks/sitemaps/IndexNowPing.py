# Databricks notebook source
# MAGIC %md
# MAGIC # IndexNow ping for changed openalex.org record pages (oxjob #1689)
# MAGIC
# MAGIC Runs after `BuildSitemaps` (mode=publish). Reads the URLs that run queued in
# MAGIC `openalex.sitemaps.indexnow_queue` (new records first, then updated, then removed), and POSTs them to
# MAGIC https://api.indexnow.org/indexnow, 10,000 per POST, at most `max_urls` a night, with the key served at
# MAGIC https://openalex.org/<key>.txt (`utils/indexnow.py`). `send=false` (the default) builds and checks every
# MAGIC batch and sends nothing. Each run is logged in `openalex.sitemaps.indexnow_log`.

# COMMAND ----------

import json
import os
import sys
import time
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "indexnow.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "indexnow.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import indexnow as ix  # noqa: E402

from pyspark.sql import Row  # noqa: E402

dbutils.widgets.text("send", "false")
dbutils.widgets.text("max_urls", "1000000")
dbutils.widgets.text("run_id", "")   # "" = the latest run in the queue
SEND = dbutils.widgets.get("send").lower() == "true"
MAX_URLS = int(dbutils.widgets.get("max_urls"))
QUEUE = "openalex.sitemaps.indexnow_queue"
LOG = "openalex.sitemaps.indexnow_log"

run_id = dbutils.widgets.get("run_id") or spark.sql(f"SELECT max(run_id) AS r FROM {QUEUE}").collect()[0].r
if run_id is None:
    dbutils.notebook.exit("queue empty")
if spark.catalog.tableExists(LOG) and SEND and spark.sql(
        f"SELECT count(*) AS n FROM {LOG} WHERE run_id = '{run_id}' AND sent = TRUE").collect()[0].n:
    dbutils.notebook.exit(f"run {run_id} already pinged")

# Priority order, then newest change first. Only up to the cap leaves Spark (<= max_urls short strings).
urls = [r.url for r in spark.sql(f"""
    SELECT url FROM {QUEUE} WHERE run_id = '{run_id}'
    ORDER BY priority, lastmod DESC, url LIMIT {MAX_URLS}""").collect()]
total = spark.sql(f"SELECT count(*) AS n FROM {QUEUE} WHERE run_id = '{run_id}'").collect()[0].n
print(f"run {run_id}: {total:,} URLs queued, {len(urls):,} within the cap of {MAX_URLS:,}; send={SEND}")

t0 = time.time()
res = ix.ping(urls, send=SEND, max_urls=MAX_URLS)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG} (run_id STRING, at TIMESTAMP, sent BOOLEAN, queued BIGINT,
              urls BIGINT, batches INT, statuses STRING, stopped_reason STRING, seconds DOUBLE)""")
spark.createDataFrame([Row(run_id=run_id, at=datetime.now(timezone.utc).replace(tzinfo=None), sent=SEND,
                           queued=int(total), urls=res.sent_urls, batches=res.sent_batches,
                           statuses=json.dumps(res.statuses[-20:]), stopped_reason=res.stopped_reason,
                           seconds=time.time() - t0)]).write.mode("append").saveAsTable(LOG)
if res.stopped_reason:
    raise RuntimeError(f"IndexNow stopped: {res.stopped_reason} after {res.sent_urls:,} URLs")
