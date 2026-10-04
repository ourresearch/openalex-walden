# Databricks notebook source
# MAGIC %md
# MAGIC # Edge autocomplete freshness check (oxjob #1529)
# MAGIC
# MAGIC Fails (and so emails) when any entity type is stale at the edge: its source table's latest data version (VACUUM,
# MAGIC OPTIMIZE and other maintenance commits skipped) was committed
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

# table maintenance adds commits but no data (10-04: a VACUUM moved institutions_api v1816 -> v1818 and tripped this
# check while the edge was current), so staleness is measured from the newest commit that can change rows
MAINTENANCE = ("VACUUM START", "VACUUM END", "OPTIMIZE", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ANALYZE",
               "SET COLUMN COMMENT", "CHANGE COLUMN", "ADD CONSTRAINT", "DROP CONSTRAINT", "REORG")


def last_data_commit(src):
    ops = ", ".join(f"'{o}'" for o in MAINTENANCE)
    return spark.sql(f"""SELECT version, timestamp FROM (DESCRIBE HISTORY {src} LIMIT 50)
                          WHERE operation NOT IN ({ops}) ORDER BY version DESC LIMIT 1""").collect()[0]

stale, lines = [], []
for typ, (t, src) in ace.TYPES.items():
    if typ == "front":
        # the front tree reads six tables: stale when any of them committed after its last good run, over an hour ago
        newest = max(last_data_commit(s)["timestamp"] for s in ace.FRONT_SOURCES)
        f = spark.sql(f"SELECT max(started_at) FROM {ace.SCHEMA}.refresh_runs WHERE typ = 'front' AND status = 'ok'").collect()[0][0]
        age_min = (now - newest).total_seconds() / 60
        lines.append(f"front         newest source commit {newest} ({age_min:.0f} min ago); last good run started {f}")
        if (f is None or f < newest) and age_min > 60:
            stale.append(f"front (a source committed {newest}, {age_min:.0f} min ago; last good run started {f})")
        continue
    h = last_data_commit(src)
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
