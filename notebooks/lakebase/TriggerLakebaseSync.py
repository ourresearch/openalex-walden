# Databricks notebook source
# MAGIC %md
# MAGIC # Trigger Lakebase synced-table refreshes (oxjob #2347)
# MAGIC
# MAGIC Starts a refresh of each synced table in `synced_tables` (comma-separated), as `BuildLakebaseWorksDocs` does for the
# MAGIC docs: not awaited. A refresh still applying yesterday's changes is skipped (its CDF cursor catches up next run); a
# MAGIC previous refresh that ended FAILED or CANCELED fails this task, one run late, which is the alert.

# COMMAND ----------

# MAGIC %pip install -U -q databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceConflict

dbutils.widgets.text("synced_tables", "")
SYNCED_TABLES = [t.strip() for t in dbutils.widgets.get("synced_tables").split(",") if t.strip()]
if not SYNCED_TABLES:
    raise ValueError("synced_tables is empty")

w = WorkspaceClient()
failed_prev, still_running = {}, []
for name in SYNCED_TABLES:
    st = w.database.get_synced_database_table(name=name)
    pid = getattr(st.data_synchronization_status, "pipeline_id", None) or getattr(getattr(st, "spec", None), "pipeline_id", None)
    if not pid:
        raise Exception(f"Could not resolve pipeline id for synced table {name}: {st}")
    latest = w.pipelines.list_updates(pipeline_id=pid, max_results=1).updates or []
    prev = latest[0].state.value if latest else None
    if prev in ("FAILED", "CANCELED"):
        failed_prev[name] = prev
    try:
        w.pipelines.start_update(pipeline_id=pid)
        print(f"Triggered refresh: {name} (pipeline {pid})")
    except ResourceConflict as e:
        still_running.append(name)
        print(f"Skipped {name}: previous update still applying ({e})")
if failed_prev:
    raise Exception(f"Previous synced-table refresh(es) ended dirty (found at trigger time, one run late): {failed_prev}")
print(f"{len(SYNCED_TABLES) - len(still_running)}/{len(SYNCED_TABLES)} refreshes triggered (not awaited)")
