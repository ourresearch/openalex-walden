# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords catch-up (oxjob #1322), one-off
# MAGIC
# MAGIC The keywords ship night rebaselined `openalex_works_hash` (every work's keywords changed, no work re-stamped), so
# MAGIC the nightly ES / Lakebase syncs (incremental by `updated_date`) don't carry the new keywords. This notebook is the
# MAGIC control task of the one-off job that reloads every work, on the server, with no session awake:
# MAGIC
# MAGIC - `prepare`: wait for the night's End 2 End; require Guardrails SUCCESS (else stop: nothing published); wait for
# MAGIC   the run's own Sync_to_Elasticsearch to finish; set the works index's replicas to 0.
# MAGIC - `wait_lakebase`: wait until the run's own Sync_Works_to_Lakebase is done.
# MAGIC - `restore_replicas`: replicas back to 1, refresh.
# MAGIC
# MAGIC Re-run with replicas kept on (oxjob #1443: the replicas-0 run took API search down on 2026-09-30):
# MAGIC - `prepare_live`: replicas 1; wait until the cluster is green (and, with `wait_balanced`, settled with works shards on every
# MAGIC   data node); merge threads 4.
# MAGIC - `watchdog`: alongside the chunks; if any data node's search queue stays above `queue_limit`, merge threads back
# MAGIC   to 8 and cancel the run (repair the run later to resume the unfinished chunks).
# MAGIC - `finish`: merge threads back to 8, refresh.
# MAGIC
# MAGIC Adapted from AffiliationSwapCatchup (oxjob #1386). Jason asked 2026-09-29 18:18 CT to get keywords fully into the API ASAP.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import datetime as dt
import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

dbutils.widgets.text("mode", "prepare")
dbutils.widgets.text("run_start_utc", "2026-09-30 05:00:00")
dbutils.widgets.text("warehouse_id", "3996dc0a9b183ce3")
dbutils.widgets.text("dry_run", "false")      # test: never touch replicas
dbutils.widgets.text("max_wait_hours", "30")
dbutils.widgets.text("job_run_id", "")        # watchdog: {{job.run_id}}
dbutils.widgets.text("queue_limit", "50")
dbutils.widgets.text("strikes", "3")          # consecutive 60 s checks over the limit
dbutils.widgets.text("wait_balanced", "false")  # prepare_live: also wait for relocations to finish and works shards on every data node

MODE = dbutils.widgets.get("mode").strip()
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip()
QUEUE_LIMIT = int(dbutils.widgets.get("queue_limit"))
STRIKES = int(dbutils.widgets.get("strikes"))
WAIT_BALANCED = dbutils.widgets.get("wait_balanced").strip().lower() == "true"
RUN_START = dt.datetime.strptime(dbutils.widgets.get("run_start_utc"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
WH = dbutils.widgets.get("warehouse_id").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
DEADLINE = time.time() + float(dbutils.widgets.get("max_wait_hours")) * 3600

E2E_JOB = 616701029470182
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
        # BLOCKED = waiting on upstream tasks, not done; a retried task has one entry per attempt, the latest counts.
        t = max((t for t in run.tasks if t.task_key == key), key=lambda t: t.attempt_number or 0, default=None)
        lc = t.state.life_cycle_state.value if t and t.state and t.state.life_cycle_state else None
        res = t.state.result_state.value if t and t.state and t.state.result_state else None
        if (lc, res) != last:
            log(f"  {key}: {lc} {res or ''}")
            last = (lc, res)
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
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
    res = wait_task(rid, "Guardrails")
    if res != "SUCCESS":
        raise RuntimeError(f"Guardrails ended {res}: publish blocked, so no keywords catch-up")
    res = wait_task(rid, "Sync_to_Elasticsearch")
    log(f"End 2 End Sync_to_Elasticsearch ended {res}; the ES reload may start")
    if DRY:
        log("DRY: would set replicas to 0")
    else:
        es().indices.put_settings(index=INDEX, body={"index": {"number_of_replicas": 0}})
        log(f"{INDEX}: replicas 0 for the reload")

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

elif MODE == "prepare_live":
    c = es()
    if not DRY:
        c.indices.put_settings(index=INDEX, body={"index": {"number_of_replicas": 1}})
    while time.time() < DEADLINE:
        h = c.cluster.health()
        data_nodes = {n["name"] for n in c.cat.nodes(format="json", h="name,node.role") if "d" in n["node.role"] or "h" in n["node.role"]}
        holding = {s["node"] for s in c.cat.shards(index=INDEX, format="json", h="node,state") if s["state"] == "STARTED"}
        idle = sorted(data_nodes - holding)
        busy = h["relocating_shards"] + h["initializing_shards"] + h["unassigned_shards"]
        log(f"health {h['status']}, relocating/initializing/unassigned {busy}, data nodes without {INDEX}: {len(idle)}")
        if h["status"] == "green" and (not WAIT_BALANCED or (busy == 0 and not idle)):
            break
        time.sleep(300)
    else:
        raise TimeoutError("cluster not balanced by the deadline")
    if DRY:
        log("DRY: would set merge threads 4")
    else:
        c.indices.put_settings(index=INDEX, body={"index": {"merge.scheduler.max_thread_count": 4}})
        log(f"{INDEX}: replicas 1, merge threads 4; chunks may start")

elif MODE == "watchdog":
    c = es()
    strikes = 0
    while True:
        run = w.jobs.get_run(int(JOB_RUN_ID))
        chunks = [t for t in run.tasks if t.task_key.startswith("es_chunk_")]
        if chunks and all(t.state and t.state.life_cycle_state and t.state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR") for t in chunks):
            log("all chunks finished; watchdog done")
            break
        pools = c.nodes.stats(metric="thread_pool")["nodes"].values()
        worst = max(((n["thread_pool"]["search"]["queue"], n["name"]) for n in pools), default=(0, ""))
        strikes = strikes + 1 if worst[0] > QUEUE_LIMIT else 0
        if strikes:
            log(f"search queue {worst[0]} on {worst[1]} (> {QUEUE_LIMIT}), strike {strikes}/{STRIKES}")
        if strikes >= STRIKES:
            c.indices.put_settings(index=INDEX, body={"index": {"merge.scheduler.max_thread_count": 8}})
            log("search saturating: merge threads back to 8, cancelling the run (repair it later to resume)")
            w.jobs.cancel_run(int(JOB_RUN_ID))
            break
        time.sleep(60)

elif MODE == "finish":
    c = es()
    c.indices.put_settings(index=INDEX, body={"index": {"merge.scheduler.max_thread_count": 8}})
    c.indices.refresh(index=INDEX)
    log(f"{INDEX}: merge threads back to 8, refreshed")

else:
    raise ValueError(f"unknown mode {MODE!r}")
