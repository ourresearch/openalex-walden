# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh one type of the edge autocomplete index (oxjob #1529)
# MAGIC
# MAGIC Rebuilds `openalex.autocomplete.nodes_<type>` from its source table (the tree design and SQL are in
# MAGIC `utils/autocomplete_edge.py`), then copies what changed into Cloudflare Workers KV for the Worker
# MAGIC `openalex-autocomplete-edge` (github.com/ourresearch/openalex-autocomplete-edge) and into its Asia-Pacific
# MAGIC Durable Object copies. Writes only `openalex.autocomplete.*` and the edge; nothing here feeds works or ES.
# MAGIC
# MAGIC - **delta** (a typical night, ~36% of keys): changed keys rewritten in place, children before parents, removed
# MAGIC   keys deleted;
# MAGIC - **rebuild** (first load, a new key space, or a delta over half the keys): a new build under its own key
# MAGIC   prefix, a 3-minute propagation wait, a key-count check by listing and 100 random reads, then the version
# MAGIC   pointer `ver:<t>` flips; the old build's keys are deleted on the next run. The Worker never reads a build
# MAGIC   before its flip, because KV caches "absent" for the whole cache TTL (#1504 lost a day of keys that way).
# MAGIC
# MAGIC One row per run in `openalex.autocomplete.refresh_runs` (keys written and deleted, $ estimate, minutes).

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "autocomplete_edge.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "autocomplete_edge.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import autocomplete_edge as ace  # noqa: E402

dbutils.widgets.text("typ", "keywords")
dbutils.widgets.text("steps", "build,load")          # build, load, copy (backfill a newly placed copy region)
dbutils.widgets.text("force_rebuild", "false")
dbutils.widgets.text("copies", "")                   # regions whose copy gets every write (e.g. "sin,hkg,nrt,mel")
TYP = dbutils.widgets.get("typ").strip()
STEPS = {s.strip() for s in dbutils.widgets.get("steps").split(",") if s.strip()}
FORCE = dbutils.widgets.get("force_rebuild").strip().lower() == "true"
REGIONS = [r.strip() for r in dbutils.widgets.get("copies").split(",") if r.strip()]
assert TYP in ace.TYPES, f"unknown type {TYP}"
try:
    RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().get()
except Exception:
    RUN_ID = ""


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def sql(q):
    return [tuple(r) for r in spark.sql(q).collect()]


def rows(q):
    for r in spark.sql(q).toLocalIterator():
        yield tuple(r)

# COMMAND ----------

for s in ace.setup_sql():
    spark.sql(s)

if "build" in STEPS:
    t00 = time.time()
    for name, q in ace.steps(TYP):
        t0 = time.time()
        spark.sql(q)
        log(f"{TYP}.{name}: {time.time() - t0:.0f}s")
    n, b = sql(f"SELECT count(*), sum(bytes) FROM {ace.nodes_table(TYP)}")[0]
    log(f"{TYP}: built {n:,} keys, {int(b or 0):,} bytes in {time.time() - t00:.0f}s")
    assert n > 0, f"{TYP}: empty build, not loading"

# COMMAND ----------

if "load" in STEPS:
    # scope autocomplete-edge: a Cloudflare token that can only write Workers KV, and the Worker's admin key
    kv = ace.KV(dbutils.secrets.get(scope="autocomplete-edge", key="kv_api_token"))
    admin = dbutils.secrets.get(scope="autocomplete-edge", key="admin_key") if REGIONS else ""
    copies = ace.Copies(admin, REGIONS)
    try:
        run = ace.refresh(TYP, sql, rows, kv, copies, log=log, force_rebuild=FORCE, job_run_id=str(RUN_ID))
    except Exception as e:
        ace.record_run(sql, dict(typ=TYP, src_table=ace.TYPES[TYP][1], status="failed", message=repr(e), job_run_id=str(RUN_ID)))
        raise
    ace.record_run(sql, run)
    log(f"{TYP}: {run['mode']} build {run['build']}: wrote {run['keys_written']:,}, deleted {run['keys_deleted']:,}, "
        f"~${run['est_usd']}, {run['minutes']} min")
    if copies.regions:
        ace.verify_copies(sql, kv, copies, log=log)   # sets copyok:<region>; an incomplete copy is not raced

# COMMAND ----------

if "copy" in STEPS:
    assert REGIONS, "copy needs copies=<regions>"
    copies = ace.Copies(dbutils.secrets.get(scope="autocomplete-edge", key="admin_key"), REGIONS)
    ace.backfill_copies(TYP, sql, copies, log=log)
    ace.verify_copies(sql, ace.KV(dbutils.secrets.get(scope="autocomplete-edge", key="kv_api_token")), copies, log=log)
