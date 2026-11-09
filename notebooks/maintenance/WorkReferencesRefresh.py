# Databricks notebook source
# MAGIC %md
# MAGIC # work_references list table (oxjob #2054, #835 B3 + B4)
# MAGIC
# MAGIC Between End 2 End runs only (why: the header of `work_references_refresh.sql`). mode=start (phase 1), once, after
# MAGIC the per-list parse_work_references is live: records every list with a reference as it stands now as filed, so each
# MAGIC night after files only the lists that appear, change, move or go; a second start is a no-op once the table holds
# MAGIC lists. mode=refresh (phase 2): takes the fingerprints again from the stored references; the job's next task runs
# MAGIC parse_work_references once, which puts every stale, never-filed, moved or gone list right. mode=undo: after a
# MAGIC refresh, restores work_references and the list table to the moment before the latest refresh not yet undone (Delta
# MAGIC time travel: within 7 days), so lists filed since are filed again the next night; run it BEFORE reverting the phase 2
# MAGIC commit (the restore ships in it). With no refresh it drops the list table (then revert phase 1). Every run writes
# MAGIC one row to the runs table.

# COMMAND ----------

import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

dbutils.widgets.dropdown("mode", "start", ["start", "refresh", "undo"], "mode")
MODE = dbutils.widgets.get("mode")
WR, LISTS, LM = "openalex.works.work_references", "openalex.works.work_reference_lists", "openalex.works.locations_mapped"
RUNS = "openalex.works.work_references_refresh_runs"
# parse_referenced_works' warehouse: the list fingerprints recorded here must come from the engine that computes them nightly
WAREHOUSE = "3996dc0a9b183ce3"
w = WorkspaceClient()

SQL_FILE = os.path.join(os.getcwd(), "work_references_refresh.sql")
STEPS = {}
for chunk in open(SQL_FILE, encoding="utf-8").read().split("-- @step ")[1:]:
    name, body = chunk.split("\n", 1)
    STEPS[name.strip()] = body.strip().rstrip(";")


def run(step, **subs):
    sql = STEPS[step].replace("__WR__", WR).replace("__LISTS__", LISTS).replace("__LM__", LM).replace("__RUNS__", RUNS)
    for k, v in subs.items():
        sql = sql.replace(k, v)
    print(f"-- {step}")
    st = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=sql, wait_timeout="50s")
    while st.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(15)
        st = w.statement_execution.get_statement(st.statement_id)
    assert st.status.state == StatementState.SUCCEEDED, f"{step}: {st.status}"
    return (st.result.data_array if st.result else None) or []


# COMMAND ----------

e2e = [j.job_id for j in w.jobs.list(name="Walden End 2 End")]
active = [r.run_id for j in e2e for r in w.jobs.list_runs(job_id=j, active_only=True)]
assert not active, f"Walden End 2 End is running ({active}); run this between nights"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (run_at TIMESTAMP, finished_at TIMESTAMP, mode STRING, status STRING,
  version_before BIGINT, lists BIGINT, lists_with_hash BIGINT, seconds DOUBLE, lists_version_before BIGINT)""")
if "lists_version_before" not in spark.table(RUNS).columns:
    spark.sql(f"ALTER TABLE {RUNS} ADD COLUMNS (lists_version_before BIGINT)")
t0, started = time.time(), datetime.now(timezone.utc)
version = spark.sql(f"DESCRIBE HISTORY {WR} LIMIT 1").collect()[0].version
lists_version = spark.sql(f"DESCRIBE HISTORY {LISTS} LIMIT 1").collect()[0].version if spark.catalog.tableExists(LISTS) else None
lists = hashed = None

if MODE == "start":
    # missing, or created empty by a parse_work_references run that got there first (its tripwire then stopped it)
    if not spark.catalog.tableExists(LISTS) or spark.table(LISTS).isEmpty():
        run("start")
    lists, hashed = (int(v) for v in run("counts")[0])
    print(f"{LISTS}: {lists:,} lists")

elif MODE == "refresh":
    run("refresh")
    lists, hashed = (int(v) for v in run("counts")[0])
    print(f"{LISTS}: {lists:,} lists, {hashed:,} with a fingerprint (the rest are re-filed)")

elif MODE == "undo":
    # the latest refresh not yet undone: both tables go back to that moment, so lists filed since are filed again
    last = spark.sql(f"""SELECT version_before, lists_version_before FROM {RUNS}
      WHERE mode = 'refresh' AND status = 'ok'
        AND run_at > coalesce((SELECT max(run_at) FROM {RUNS} WHERE mode = 'undo' AND status = 'ok'), TIMESTAMP'1970-01-01')
      ORDER BY run_at DESC LIMIT 1""").collect()
    if not last:
        run("undo_drop")
    else:
        run("undo_restore", __VERSION__=str(last[0].version_before))
        run("undo_restore_lists", __LISTS_VERSION__=str(last[0].lists_version_before))

row = (f"TIMESTAMP'{started:%Y-%m-%d %H:%M:%S}', current_timestamp(), '{MODE}', 'ok', {version}, "
       f"{lists if lists is not None else 'NULL'}, {hashed if hashed is not None else 'NULL'}, {time.time() - t0:.0f}, "
       f"{lists_version if lists_version is not None else 'NULL'}")
run("log", __RUN_ROW__=row)
print(f"{MODE} ok")
