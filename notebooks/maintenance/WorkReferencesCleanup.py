# Databricks notebook source
# MAGIC %md
# MAGIC # work_references cleanup (oxjob #1700)
# MAGIC
# MAGIC Removes the rows that lock works out of parse_work_references' insert: unmapped rows and repository placeholders
# MAGIC (why, and what is kept: the header of `work_references_cleanup.sql`). mode=dry_run counts only; mode=write backs up
# MAGIC the placeholders, deletes both kinds, checks 0 are left; mode=undo puts the placeholders back and removes what was
# MAGIC read for those works since. Every run writes one row to the runs table. A second write is a no-op.
# MAGIC Never run alongside End 2 End: the run refuses while a "Walden End 2 End" run is active.

# COMMAND ----------

import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "write", "undo"], "mode")
dbutils.widgets.text("wr_table", "openalex.works.work_references", "work_references (a rehearsal points at a copy)")
dbutils.widgets.text("removed_table", "openalex.works.work_references_placeholder_backup", "backup of the removed placeholder rows")
dbutils.widgets.text("runs_table", "openalex.works.work_references_cleanup_runs", "one row per run")
# Tripwires: about 1.5x the 5 Nov 2026 sizes (294,535,260 placeholder rows; 4,381,190,257 unmapped rows).
dbutils.widgets.text("max_placeholder_rows", "430000000", "fail above this many placeholder rows")
dbutils.widgets.text("max_unmapped_rows", "6600000000", "fail above this many unmapped rows")
MODE = dbutils.widgets.get("mode")
WR, REMOVED, RUNS = (dbutils.widgets.get(k) for k in ("wr_table", "removed_table", "runs_table"))
MAX_PH, MAX_UNMAPPED = int(dbutils.widgets.get("max_placeholder_rows")), int(dbutils.widgets.get("max_unmapped_rows"))

# Git-source job: the working directory is this notebook's folder (as RunMatchSql.py reads sql/<file>).
SQL_FILE = os.path.join(os.getcwd(), "work_references_cleanup.sql")
STEPS = {}
for chunk in open(SQL_FILE, encoding="utf-8").read().split("-- @step ")[1:]:
    name, body = chunk.split("\n", 1)
    STEPS[name.strip()] = body.strip().rstrip(";")


def run(step, **subs):
    sql = STEPS[step].replace("__WR__", WR).replace("__REMOVED__", REMOVED).replace("__RUNS__", RUNS)
    for k, v in subs.items():
        sql = sql.replace(k, v)
    print(f"-- {step}"); return spark.sql(sql).collect()


# COMMAND ----------

w = WorkspaceClient()
e2e = [j.job_id for j in w.jobs.list(name="Walden End 2 End")]
active = [r.run_id for j in e2e for r in w.jobs.list_runs(job_id=j, active_only=True)]
assert not active, f"Walden End 2 End is running ({active}); run this between nights"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (run_at TIMESTAMP, finished_at TIMESTAMP, mode STRING, status STRING,
  unmapped_before BIGINT, placeholder_before BIGINT, removed_rows BIGINT, unmapped_after BIGINT, placeholder_after BIGINT,
  restored_rows BIGINT, seconds DOUBLE)""")
t0, started = time.time(), datetime.now(timezone.utc)
before = run("counts")[0]
print(before)
assert before.placeholder_rows <= MAX_PH and before.unmapped_rows <= MAX_UNMAPPED, \
    f"tripwire: {before} is above max_placeholder_rows={MAX_PH} / max_unmapped_rows={MAX_UNMAPPED}; size it before raising"
removed = restored = None
after = before

if MODE == "write":
    if not spark.catalog.tableExists(REMOVED):
        run("backup")
    removed = run("removed_count")[0].removed_rows
    # A second write finds 0 placeholders and deletes nothing. Otherwise the backup must hold exactly what is about to go:
    # anything else means placeholders were written after the backup (the code fix is not live), so stop.
    assert before.placeholder_rows in (0, removed), \
        f"{before.placeholder_rows} placeholder rows in {WR} but {removed} in {REMOVED}: is the parse_work_references fix live?"
    if before.placeholder_rows or before.unmapped_rows:
        run("delete")
    after = run("counts")[0]
    assert after.placeholder_rows == 0 and after.unmapped_rows == 0, after

elif MODE == "undo":
    assert spark.catalog.tableExists(REMOVED), f"no backup table {REMOVED}"
    removed = run("removed_count")[0].removed_rows
    # Rows read since the write go first: a list read for those works can hold an empty entry, shaped like a placeholder.
    run("undo_unread")
    present = run("undo_restored_count")[0].placeholder_rows_present
    assert present in (0, removed), f"{present} of {removed} placeholder rows already back: a partial undo, look first"
    if present == 0:
        run("undo_restore")
    restored = run("undo_restored_count")[0].placeholder_rows_present
    assert restored == removed, (restored, removed)
    after = run("counts")[0]

row = (f"TIMESTAMP'{started:%Y-%m-%d %H:%M:%S}', current_timestamp(), '{MODE}', 'ok', {before.unmapped_rows}, "
       f"{before.placeholder_rows}, {removed if removed is not None else 'NULL'}, {after.unmapped_rows}, "
       f"{after.placeholder_rows}, {restored if restored is not None else 'NULL'}, {time.time() - t0:.0f}")
run("log", __RUN_ROW__=row)
print(f"{MODE} ok: before {before}, after {after}, removed {removed}, restored {restored}")
