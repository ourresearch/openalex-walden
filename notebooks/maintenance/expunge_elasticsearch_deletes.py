# Databricks notebook source
# MAGIC %md
# MAGIC # Expunge Elasticsearch Deletes
# MAGIC
# MAGIC Runs `POST /{index}/_forcemerge?only_expunge_deletes=true` to reclaim deleted-doc
# MAGIC bloat on a single ES index. Intended for scheduled weekly maintenance.
# MAGIC
# MAGIC Safeguards:
# MAGIC 1. Skip if current delete ratio is below `delete_threshold_pct`.
# MAGIC 2. Abort if any data node disk usage exceeds `max_disk_pct`.
# MAGIC 3. Wait up to `bulk_wait_minutes` for active bulk indexing to finish before starting.
# MAGIC 4. Fire async (`wait_for_completion=false`) and poll the tasks API.
# MAGIC 5. Time out after `max_runtime_hours`.

# COMMAND ----------

%pip install elasticsearch==8.19.0

# COMMAND ----------

import time
from datetime import datetime, timezone
from elasticsearch import Elasticsearch

dbutils.widgets.text("index_name", "", "ES index name (e.g. authors-v18)")
dbutils.widgets.text("delete_threshold_pct", "15", "Skip if deleted docs below this percent")
dbutils.widgets.text("max_disk_pct", "80", "Abort if any node disk usage above this percent")
dbutils.widgets.text("bulk_wait_minutes", "60", "Max minutes to wait for active bulk indexing")
dbutils.widgets.text("max_runtime_hours", "12", "Abort the merge if still running after this many hours")
dbutils.widgets.text("poll_interval_seconds", "300", "How often to poll task status during merge")

INDEX_NAME = dbutils.widgets.get("index_name").strip()
DELETE_THRESHOLD_PCT = float(dbutils.widgets.get("delete_threshold_pct"))
MAX_DISK_PCT = float(dbutils.widgets.get("max_disk_pct"))
BULK_WAIT_MINUTES = int(dbutils.widgets.get("bulk_wait_minutes"))
MAX_RUNTIME_HOURS = float(dbutils.widgets.get("max_runtime_hours"))
POLL_INTERVAL_SECONDS = int(dbutils.widgets.get("poll_interval_seconds"))

if not INDEX_NAME:
    raise ValueError("index_name is required")

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
es = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=60, max_retries=3)

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}")

# COMMAND ----------

# MAGIC %md ## 1. Capture baseline stats

# COMMAND ----------

def get_stats(index):
    s = es.indices.stats(index=index, metric=["docs", "store", "segments"])
    total = s["_all"]["total"]
    return {
        "docs": total["docs"]["count"],
        "deleted": total["docs"]["deleted"],
        "store_bytes": total["store"]["size_in_bytes"],
        "segments": total["segments"]["count"],
    }

before = get_stats(INDEX_NAME)
delete_pct = 100.0 * before["deleted"] / (before["docs"] + before["deleted"]) if (before["docs"] + before["deleted"]) else 0.0
log(f"Baseline {INDEX_NAME}: docs={before['docs']:,} deleted={before['deleted']:,} ({delete_pct:.2f}%) segments={before['segments']} size={before['store_bytes']/1024**3:.1f} GB")

if delete_pct < DELETE_THRESHOLD_PCT:
    log(f"Delete ratio {delete_pct:.2f}% below threshold {DELETE_THRESHOLD_PCT}% — skipping forcemerge")
    dbutils.notebook.exit(f"skipped: delete_pct={delete_pct:.2f}% < threshold={DELETE_THRESHOLD_PCT}%")

# COMMAND ----------

# MAGIC %md ## 2. Disk headroom check

# COMMAND ----------

alloc = es.cat.allocation(format="json", bytes="b")
over = [
    (a["node"], float(a["disk.percent"]))
    for a in alloc
    if a.get("disk.percent") and a["disk.percent"] != "null" and float(a["disk.percent"]) > MAX_DISK_PCT
]
if over:
    details = ", ".join(f"{n}={p}%" for n, p in over)
    raise RuntimeError(f"Aborting: node(s) above {MAX_DISK_PCT}% disk: {details}")

top_used = sorted(
    ((a["node"], float(a["disk.percent"])) for a in alloc if a.get("disk.percent") and a["disk.percent"] != "null"),
    key=lambda x: -x[1],
)[:3]
log(f"Disk OK. Top 3 nodes: {top_used}")

# COMMAND ----------

# MAGIC %md ## 3. Wait for active bulk indexing to settle

# COMMAND ----------

def active_bulk_tasks():
    resp = es.tasks.list(actions="*bulk*", detailed=False)
    tasks = []
    for node_id, node in resp.get("nodes", {}).items():
        tasks.extend(node.get("tasks", {}).keys())
    return tasks

wait_deadline = time.time() + BULK_WAIT_MINUTES * 60
while True:
    tasks = active_bulk_tasks()
    if not tasks:
        log("No active bulk indexing — proceeding")
        break
    if time.time() >= wait_deadline:
        raise RuntimeError(f"Aborting: {len(tasks)} bulk task(s) still active after {BULK_WAIT_MINUTES} min wait")
    log(f"{len(tasks)} bulk task(s) active, waiting 60s...")
    time.sleep(60)

# COMMAND ----------

# MAGIC %md ## 4. Kick off forcemerge async

# COMMAND ----------

resp = es.indices.forcemerge(
    index=INDEX_NAME,
    only_expunge_deletes=True,
    wait_for_completion=False,
)
task_id = resp["task"]
log(f"Started forcemerge task {task_id}")

# COMMAND ----------

# MAGIC %md ## 5. Poll until complete

# COMMAND ----------

merge_deadline = time.time() + MAX_RUNTIME_HOURS * 3600
last_log = 0
while True:
    status = es.tasks.get(task_id=task_id)
    if status.get("completed"):
        log(f"Forcemerge task {task_id} completed")
        break
    if time.time() >= merge_deadline:
        log(f"Runtime exceeded {MAX_RUNTIME_HOURS}h — cancelling task {task_id}")
        try:
            es.tasks.cancel(task_id=task_id)
        except Exception as e:
            log(f"Cancel request error (merge may still be in flight): {e}")
        raise RuntimeError(f"Aborting: forcemerge exceeded {MAX_RUNTIME_HOURS}h")

    now = time.time()
    if now - last_log > 600:
        running_nanos = status.get("task", {}).get("running_time_in_nanos", 0)
        running_min = running_nanos / 1e9 / 60
        desc = status.get("task", {}).get("description", "")
        log(f"Still running ({running_min:.1f} min): {desc}")
        last_log = now

    time.sleep(POLL_INTERVAL_SECONDS)

# COMMAND ----------

# MAGIC %md ## 6. Report results

# COMMAND ----------

after = get_stats(INDEX_NAME)
reclaimed_gb = (before["store_bytes"] - after["store_bytes"]) / 1024**3
log(
    "Done. "
    f"deleted: {before['deleted']:,} -> {after['deleted']:,}; "
    f"segments: {before['segments']} -> {after['segments']}; "
    f"store: {before['store_bytes']/1024**3:.1f} -> {after['store_bytes']/1024**3:.1f} GB "
    f"(reclaimed {reclaimed_gb:.1f} GB)"
)

dbutils.notebook.exit(
    f"ok: deleted {before['deleted']:,}->{after['deleted']:,}, segments {before['segments']}->{after['segments']}, reclaimed {reclaimed_gb:.1f}GB"
)
