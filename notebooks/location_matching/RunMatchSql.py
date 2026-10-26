# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works: run one SQL file with the night's pins (oxjob #1668)
# MAGIC
# MAGIC Fills `__NEW__` / `__OLD__` / `__V__` from the `prepare` task's values and runs the statements of `sql/<file>` in order
# MAGIC (split on a line holding only `;`). Used for `features.sql` (3/6).

# COMMAND ----------

import datetime as dt
import re
import time

dbutils.widgets.text("sql", "features.sql")
for k in ("NEW", "OLD", "V"):
    dbutils.widgets.text(k, "")
PINS = {k: dbutils.jobs.taskValues.get(taskKey="prepare", key=k, debugValue=dbutils.widgets.get(k)) for k in ("NEW", "OLD", "V")}
PATH = f"sql/{dbutils.widgets.get('sql').strip()}"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


text = open(PATH).read()
for k, v in PINS.items():
    text = text.replace(f"__{k}__", str(v))
assert not re.search(r"__(NEW|OLD|V)__", text), f"{PATH}: a pin is missing"
for stmt in [s.strip() for s in text.split("\n;\n") if s.strip()]:
    t = time.time()
    df = spark.sql(stmt)
    body = "\n".join(l for l in stmt.splitlines() if not l.lstrip().startswith("--")).lstrip().upper()
    log(f"{PATH}: {time.time() - t:.0f}s  {body[:80]!r}")
    if body.startswith(("SELECT", "WITH")):
        for r in df.limit(60).collect():
            print("   ", r.asDict())
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
