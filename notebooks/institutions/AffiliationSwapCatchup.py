# Databricks notebook source
# MAGIC %md
# MAGIC # Affiliation swap catch-up (oxjob #1386), one-off
# MAGIC
# MAGIC The swap night runs End 2 End with `hash_rebaseline` pre-cleared: every work whose content changed keeps its
# MAGIC `updated_date`, so the nightly ES / Lakebase syncs (incremental by `updated_date`) don't carry the change. This
# MAGIC notebook is the control task of the one-off job that does, on the server, with no session awake:
# MAGIC
# MAGIC - `prepare`: wait for the night's Works_Enriched; read the `openalex_works_hash` MERGE's changed-row count and
# MAGIC   **cancel End 2 End above `cancel_above`**; wait for Guardrails to pass (the run's publish gate); build
# MAGIC   `changed_works` (hash changed, date kept: the swap plus the night's ordinary changes) and `changed_rows` (their
# MAGIC   full rows) on a SQL warehouse, which prunes files by id; set the works index's replicas to 0.
# MAGIC - `wait_lakebase`: wait until the run's own Sync_Works_to_Lakebase is done (same docs tables).
# MAGIC - `restore_replicas`: replicas back to 1, refresh.
# MAGIC - `wait_authors`: wait for the day's Authors build to succeed.
# MAGIC
# MAGIC Jason approved the plan 2026-09-27 (charter § Write log).

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import datetime as dt
import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

dbutils.widgets.text("mode", "prepare")
dbutils.widgets.text("run_start_utc", "2026-09-28 05:00:00")
dbutils.widgets.text("cancel_above", "100000000")
dbutils.widgets.text("warehouse_id", "3996dc0a9b183ce3")
dbutils.widgets.text("changed_works", "openalex.institutions.oxjob1386_changed_works")
dbutils.widgets.text("changed_rows", "openalex.institutions.oxjob1386_changed_rows")
dbutils.widgets.text("test_rows_table", "")   # test: skip the build and use this rows table
dbutils.widgets.text("dry_run", "false")      # test: never cancel, never touch replicas
dbutils.widgets.text("authors_after_utc", "2026-09-28 12:00:00")
dbutils.widgets.text("max_wait_hours", "16")

MODE = dbutils.widgets.get("mode").strip()
RUN_START = dt.datetime.strptime(dbutils.widgets.get("run_start_utc"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
CANCEL_ABOVE = int(dbutils.widgets.get("cancel_above"))
WH = dbutils.widgets.get("warehouse_id").strip()
CHANGED_WORKS = dbutils.widgets.get("changed_works").strip()
CHANGED_ROWS = dbutils.widgets.get("changed_rows").strip()
TEST_ROWS = dbutils.widgets.get("test_rows_table").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
AUTHORS_AFTER = dt.datetime.strptime(dbutils.widgets.get("authors_after_utc"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
DEADLINE = time.time() + float(dbutils.widgets.get("max_wait_hours")) * 3600

E2E_JOB = 616701029470182
AUTHORS_JOB = 63282467302934
INDEX = "works-v34"
w = WorkspaceClient()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def sql(statement):
    r = w.statement_execution.execute_statement(warehouse_id=WH, statement=statement, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(10)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL {r.status.state}: {r.status.error.message if r.status.error else ''}")
    return (r.result.data_array or []) if r.result else []


def find_run(job_id, after):
    ms = int(after.timestamp() * 1000) - 5 * 60 * 1000
    runs = [r for r in w.jobs.list_runs(job_id=job_id, expand_tasks=True, limit=5) if (r.start_time or 0) >= ms]
    return sorted(runs, key=lambda r: r.start_time)[0] if runs else None


def wait_for_run(job_id, after, what):
    while time.time() < DEADLINE:
        r = find_run(job_id, after)
        if r is not None:
            log(f"{what} run {r.run_id} found")
            return r.run_id
        time.sleep(120)
    raise TimeoutError(f"no {what} run after {after}")


def wait_task(run_id, key):
    last = None
    while time.time() < DEADLINE:
        run = w.jobs.get_run(run_id)
        t = next((t for t in run.tasks if t.task_key == key), None)
        lc = t.state.life_cycle_state.value if t and t.state and t.state.life_cycle_state else None
        res = t.state.result_state.value if t and t.state and t.state.result_state else None
        if (lc, res) != last:
            log(f"  {key}: {lc} {res or ''}")
            last = (lc, res)
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"):
            return res
        run_lc = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else None
        if run_lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return res
        time.sleep(60)
    raise TimeoutError(f"{key} not done by the deadline")


def es():
    from elasticsearch import Elasticsearch
    return Elasticsearch(hosts=[dbutils.secrets.get(scope="elastic", key="elastic_url")], request_timeout=180,
                         max_retries=5, retry_on_timeout=True)


# COMMAND ----------

if MODE == "prepare":
    rid = wait_for_run(E2E_JOB, RUN_START, "End 2 End")
    res = wait_task(rid, "Works_Enriched")
    if res != "SUCCESS":
        raise RuntimeError(f"Works_Enriched ended {res}: no catch-up")
    rows = sql(f"""SELECT version, operationMetrics['numTargetRowsUpdated'], operationMetrics['numTargetRowsInserted']
                   FROM (DESCRIBE HISTORY openalex.works.openalex_works_hash)
                   WHERE operation = 'MERGE' AND timestamp >= TIMESTAMP'{RUN_START:%Y-%m-%d %H:%M:%S}'
                   ORDER BY version LIMIT 1""")
    if not rows:
        raise RuntimeError("no openalex_works_hash MERGE since the run started")
    version, changed, inserted = int(rows[0][0]), int(rows[0][1] or 0), int(rows[0][2] or 0)
    log(f"openalex_works_hash MERGE v{version}: {changed:,} works changed, {inserted:,} new")
    if changed > CANCEL_ABOVE:
        if DRY:
            log(f"DRY: would cancel End 2 End run {rid}")
        else:
            w.jobs.cancel_run(rid)
            raise RuntimeError(f"CANCELLED End 2 End run {rid}: {changed:,} works changed > {CANCEL_ABOVE:,}")
    res = wait_task(rid, "Guardrails")
    if res != "SUCCESS":
        raise RuntimeError(f"Guardrails ended {res}: publish blocked, so no catch-up")
    if TEST_ROWS:
        rows_table, n = TEST_ROWS, int(sql(f"SELECT COUNT(*) FROM {TEST_ROWS}")[0][0])
        log(f"TEST: using {TEST_ROWS} ({n:,} rows)")
    else:
        sql(f"""CREATE OR REPLACE TABLE {CHANGED_WORKS} CLUSTER BY (id) AS
                SELECT a.id FROM openalex.works.openalex_works_hash VERSION AS OF {version} a
                JOIN openalex.works.openalex_works_hash VERSION AS OF {version - 1} b ON a.id = b.id
                WHERE a.content_hash <> b.content_hash AND a.updated_date <=> b.updated_date""")
        log(f"changed works (hash changed, date kept): {int(sql(f'SELECT COUNT(*) FROM {CHANGED_WORKS}')[0][0]):,}")
        sql(f"""CREATE OR REPLACE TABLE {CHANGED_ROWS} AS
                SELECT w.* FROM openalex.works.openalex_works w LEFT SEMI JOIN {CHANGED_WORKS} c ON c.id = w.id""")
        rows_table, n = CHANGED_ROWS, int(sql(f"SELECT COUNT(*) FROM {CHANGED_ROWS}")[0][0])
        log(f"changed rows materialized: {n:,}")
    if DRY:
        log("DRY: would set works replicas to 0")
    elif n >= 1_000_000:
        c = es()
        c.indices.put_settings(index=INDEX, body={"index": {"number_of_replicas": 0}})
        log(f"{INDEX}: replicas 0 for the catch-up")
    dbutils.jobs.taskValues.set(key="rows", value=n)
    dbutils.notebook.exit(json.dumps({"rows_table": rows_table, "rows": n, "changed": changed}))

elif MODE == "wait_lakebase":
    rid = wait_for_run(E2E_JOB, RUN_START, "End 2 End")
    res = wait_task(rid, "Sync_Works_to_Lakebase")
    log(f"End 2 End Sync_Works_to_Lakebase ended {res}; the Lakebase catch-up may start")

elif MODE == "restore_replicas":
    if DRY:
        log("DRY: would restore replicas to 1")
    else:
        c = es()
        c.indices.put_settings(index=INDEX, body={"index": {"number_of_replicas": 1}})
        c.indices.refresh(index=INDEX)
        log(f"{INDEX}: replicas back to 1, refreshed")

elif MODE == "wait_authors":
    rid = wait_for_run(AUTHORS_JOB, AUTHORS_AFTER, "Authors")
    while time.time() < DEADLINE:
        s = w.jobs.get_run(rid).state
        lc = s.life_cycle_state.value if s.life_cycle_state else None
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            res = s.result_state.value if s.result_state else None
            log(f"Authors run {rid} ended {res}")
            if res != "SUCCESS":
                raise RuntimeError("Authors build did not succeed; authors ES sync left to its schedule")
            break
        time.sleep(300)
    else:
        raise TimeoutError("Authors build not done by the deadline")

else:
    raise ValueError(f"unknown mode {MODE!r}")
