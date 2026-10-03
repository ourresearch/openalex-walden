# Databricks notebook source
# MAGIC %md
# MAGIC # Edge autocomplete freshness check (oxjob #1529)
# MAGIC
# MAGIC Fails (and so emails) when any entity type is stale at the edge: its source table's latest version was committed
# MAGIC more than 60 minutes ago and no successful refresh in `openalex.autocomplete.refresh_runs` has loaded that
# MAGIC version or a later one. Also prints, per type, how long the last refresh took from the table's commit to live.

# COMMAND ----------

import os
import sys
import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "autocomplete_edge.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "autocomplete_edge.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import autocomplete_edge as ace  # noqa: E402

now = spark.sql("SELECT current_timestamp()").collect()[0][0]
stale, lines = [], []
for typ, (t, src) in ace.TYPES.items():
    h = spark.sql(f"DESCRIBE HISTORY {src} LIMIT 1").collect()[0]
    version, committed = h["version"], h["timestamp"]
    last = spark.sql(f"""SELECT max(src_version) AS v, max(finished_at) AS f FROM {ace.SCHEMA}.refresh_runs
                         WHERE typ = '{typ}' AND status = 'ok'""").collect()[0]
    age_min = (now - committed).total_seconds() / 60
    ok = last["v"] is not None and last["v"] >= version
    lines.append(f"{typ:13s} source v{version} committed {committed} ({age_min:.0f} min ago); loaded v{last['v']} at {last['f']}")
    if not ok and age_min > 60:
        stale.append(f"{typ} (source v{version}, {age_min:.0f} min old; edge has v{last['v']})")
print("\n".join(lines))
if stale:
    raise RuntimeError("edge autocomplete is stale for: " + "; ".join(stale))
