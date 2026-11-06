# Databricks notebook source
# MAGIC %md
# MAGIC # work_references list table (oxjob #2054, #835 B3 + B4)
# MAGIC
# MAGIC Between End 2 End runs only. mode=start, once, after the per-list parse_work_references is live: records every
# MAGIC list with a reference as it stands now as filed (why: the header of `work_references_refresh.sql`), so each night
# MAGIC after files only the lists that appear, change, move or go. A second start is a no-op once the table holds lists.
# MAGIC mode=undo drops the list table; revert the parse_work_references commit first, or the next night's tripwire stops
# MAGIC on the missing table. Every run writes one row to the runs table.

# COMMAND ----------

import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

dbutils.widgets.dropdown("mode", "start", ["start", "undo"], "mode")
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
  version_before BIGINT, lists BIGINT, lists_with_hash BIGINT, seconds DOUBLE)""")
t0, started = time.time(), datetime.now(timezone.utc)
version = spark.sql(f"DESCRIBE HISTORY {WR} LIMIT 1").collect()[0].version
lists = hashed = None

if MODE == "start":
    # missing, or created empty by a parse_work_references run that got there first (its tripwire then stopped it)
    if not spark.catalog.tableExists(LISTS) or spark.table(LISTS).isEmpty():
        run("start")
    lists, hashed = (int(v) for v in run("counts")[0])
    print(f"{LISTS}: {lists:,} lists")

elif MODE == "undo":
    run("undo_drop")

row = (f"TIMESTAMP'{started:%Y-%m-%d %H:%M:%S}', current_timestamp(), '{MODE}', 'ok', {version}, "
       f"{lists if lists is not None else 'NULL'}, {hashed if hashed is not None else 'NULL'}, {time.time() - t0:.0f}")
run("log", __RUN_ROW__=row)
print(f"{MODE} ok")
