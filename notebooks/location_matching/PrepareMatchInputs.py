# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 1/6: pins and inputs (oxjob #1668)
# MAGIC
# MAGIC Shadow mode: runs after the nightly's MapWorkIds and never writes `location_work_ids` or `work_id_map`.
# MAGIC
# MAGIC Pins the night from Delta history: NEW = the night's first `location_work_ids` MERGE (MapWorkIds), OLD = the MERGE before
# MAGIC it, V = the last `openalex_works` version before NEW (the works as the records found them). Exits with `go = false` when the
# MAGIC night has no MERGE yet or was already decided (unless `force`). Then builds the inputs: `sql/keys.sql` (blocking keys of
# MAGIC every work at V), `sql/frame.sql` (the anchors that reached the title tier or minted / parked) and `sql/build.sql` (seeds,
# MAGIC SQL-channel candidates, qwen3 seed vectors).

# COMMAND ----------

import datetime as dt
import time

for name, default in [("night", ""), ("mode", "shadow"), ("force", "false")]:
    dbutils.widgets.text(name, default)
NIGHT = dbutils.widgets.get("night").strip() or dt.datetime.utcnow().strftime("%Y-%m-%d")
MODE = dbutils.widgets.get("mode").strip()
FORCE = dbutils.widgets.get("force").strip().lower() == "true"
T = "openalex.works.location_work_match_"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def run_sql(path, pins):
    """Run a `;`-separated SQL file (statements split on a line holding only `;`) with __KEY__ placeholders filled from pins."""
    text = open(path).read()
    for k, v in pins.items():
        text = text.replace(f"__{k}__", str(v))
    for stmt in [s.strip() for s in text.split("\n;\n") if s.strip()]:
        t = time.time()
        df = spark.sql(stmt)
        body = "\n".join(l for l in stmt.splitlines() if not l.lstrip().startswith("--")).lstrip().upper()
        log(f"{path}: {time.time() - t:.0f}s  {body[:80]!r}")
        if body.startswith(("SELECT", "WITH")):
            for r in df.limit(60).collect():
                print("   ", r.asDict())

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T}runs (night DATE, mode STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
  lwi_new BIGINT, lwi_old BIGINT, works_version BIGINT, records BIGINT, with_candidates BIGINT, to_qwen BIGINT, qwen_ok BOOLEAN,
  qwen_usd_est DOUBLE, model_binds BIGINT, nightly_binds BIGINT, same_bind BIGINT, both_mint BIGINT, model_binds_nightly_mints BIGINT,
  model_mints_nightly_binds BIGINT, other_work BIGINT, seconds MAP<STRING, DOUBLE>, notes STRING) CLUSTER BY (night)
  COMMENT 'Match Locations To Works (oxjob #1668): one row per night and mode, written by the job (status running -> done)'""")

merges = spark.sql("""SELECT version, timestamp FROM (DESCRIBE HISTORY openalex.works.location_work_ids)
  WHERE operation = 'MERGE' ORDER BY version""").collect()
night_merges = [m for m in merges if m.timestamp.strftime("%Y-%m-%d") == NIGHT]
go = bool(night_merges)
if not go:
    log(f"no location_work_ids MERGE on {NIGHT} yet: the nightly has not reached MapWorkIds")
else:
    new = night_merges[0]
    old = max((m for m in merges if m.version < new.version), key=lambda m: m.version)
    v = spark.sql(f"""SELECT max(version) AS v FROM (DESCRIBE HISTORY openalex.works.openalex_works)
      WHERE timestamp < '{new.timestamp.strftime('%Y-%m-%d %H:%M:%S')}'""").collect()[0].v
    done = spark.sql(f"SELECT count(*) AS n FROM {T}runs WHERE night = DATE'{NIGHT}' AND mode = '{MODE}' AND status = 'done'").collect()[0].n
    if done and not FORCE:
        go = False
        log(f"{NIGHT} ({MODE}) already decided; force = true to redo it")
    else:
        pins = {"NEW": new.version, "OLD": old.version, "V": v}
        log(f"night {NIGHT}: location_work_ids {old.version} -> {new.version} ({new.timestamp} UTC), openalex_works {v}")
        spark.sql(f"DELETE FROM {T}runs WHERE night = DATE'{NIGHT}' AND mode = '{MODE}'")
        spark.sql(f"""INSERT INTO {T}runs (night, mode, status, started_at, lwi_new, lwi_old, works_version)
          VALUES (DATE'{NIGHT}', '{MODE}', 'running', current_timestamp(), {new.version}, {old.version}, {v})""")
        for f in ("sql/keys.sql", "sql/frame.sql", "sql/build.sql"):
            run_sql(f, pins)
        for k, val in pins.items():
            dbutils.jobs.taskValues.set(key=k, value=int(val))

dbutils.jobs.taskValues.set(key="go", value="true" if go else "false")
dbutils.jobs.taskValues.set(key="night", value=NIGHT)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit("go" if go else "nothing to do")
