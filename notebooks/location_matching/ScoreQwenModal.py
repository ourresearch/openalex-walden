# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 5/6: Qwen on Modal (oxjob #1668)
# MAGIC
# MAGIC Records whose top XGBoost score is >= 0.01 go to the fine-tuned Qwen (app `location-work-matcher`, `modal_location_matcher.py`
# MAGIC in this folder; Qwen/Qwen3.5-9B + adapter ep2_9b; vLLM does not run on Databricks). `sql/qwen_pairs.sql` builds each record's
# MAGIC top 10 candidates in the judge format; this notebook turns them into the training prompt (record + shortlist, one slot per
# MAGIC candidate, `qwen/rows.py row()` of oxjob #1668), POSTs the prompt table to the app's `/start` (bearer token from secret scope
# MAGIC `modal`), polls `/status`, checks the shard files and loads `location_work_match_qwen` (seed_id, cid, p).
# MAGIC
# MAGIC No fallback (Casey 2026-10-26): when the estimate is over `max_usd`, the call fails, runs past `max_minutes` or leaves any
# MAGIC prompt unscored, the task fails and so does the run. The matcher never decides with less than the full stack.

# COMMAND ----------

import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.getcwd())
import lwm  # noqa: E402

from pyspark.sql import Window, functions as F

for name, default in [("volume_dir", "/Volumes/openalex/works/data/location_work_match"),
                      ("endpoint", "https://wordslikethis--location-work-matcher-api.modal.run"), ("secret_scope", "modal"),
                      ("secret_key", "location_matcher_token"), ("warehouse_id", "3996dc0a9b183ce3"), ("max_usd", "60"),
                      ("max_minutes", "240"), ("shard_records", "4000"), ("run_id", "manual"), ("V", "")]:
    dbutils.widgets.text(name, default)
VOL = dbutils.widgets.get("volume_dir").strip().rstrip("/")
ENDPOINT = dbutils.widgets.get("endpoint").strip().rstrip("/")
WH = dbutils.widgets.get("warehouse_id").strip()
MAX_USD, MAX_MIN = float(dbutils.widgets.get("max_usd")), float(dbutils.widgets.get("max_minutes"))
SHARD = int(dbutils.widgets.get("shard_records"))
RUN = dbutils.widgets.get("run_id").strip()
NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue="")
V = dbutils.jobs.taskValues.get(taskKey="prepare", key="V", debugValue=dbutils.widgets.get("V"))
T = "openalex.works.location_work_match_"
ADAPTER = "ep2_9b"
HEAD = ("Incoming bibliographic record and a shortlist of existing works. For each candidate answer yes only if the record "
        "is the same work (a copy, or the preprint / published version of it); otherwise no. At most one yes.\n")
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def finish(note, usd=0.0):
    dbutils.jobs.taskValues.set(key="qwen_usd_est", value=float(usd))
    dbutils.jobs.taskValues.set(key="qwen_note", value=note[:500])
    dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
    log(note)
    dbutils.notebook.exit(note[:1000])

# COMMAND ----------

lwm.run_sql_file("sql/qwen_pairs.sql", {"V": V})


@F.udf("struct<prefix:string, seats:string>")
def prompt(rec_a, cands):
    """= oxjob #1668 qwen/rows.py row() on run_jev.rec() records: candidates in XGBoost rank order, at most 10."""
    rec = lambda s: {k: v for k, v in json.loads(s).items() if k not in ("prov", "cites") and v not in (None, "", [])}
    cs = sorted(cands, key=lambda c: c["rank"])[:10]
    lines = [f"RECORD: {json.dumps(rec(rec_a), ensure_ascii=False)}", "CANDIDATES:"]
    lines += [f"c{i + 1}: {json.dumps(rec(c['rec_b']), ensure_ascii=False)}" for i, c in enumerate(cs)]
    return (HEAD + "\n".join(lines) + "\nANSWERS:\n", json.dumps([{"line": f"c{i + 1}", "key": str(c["cid"])} for i, c in enumerate(cs)]))


rows = (spark.table(f"{T}qwen_pairs").groupBy("seed_id")
        .agg(F.first("rec_a").alias("rec_a"), F.collect_list(F.struct("rank", "cid", "rec_b")).alias("cands"))
        .withColumn("x", prompt("rec_a", "cands"))
        .withColumn("shard", F.floor((F.row_number().over(Window.orderBy("seed_id")) - 1) / SHARD).cast("int"))
        .select("shard", "seed_id", F.col("x.prefix").alias("prefix"), F.col("x.seats").alias("seats")))
rows.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{T}qwen_rows")
n = spark.table(f"{T}qwen_rows").count()
n_pairs = spark.table(f"{T}qwen_pairs").count()
est = n * 0.23 * (4.54 + 8 * 0.0473 + 96 * 0.008) / 3600 + 1.5   # 0.23 GPU-s / record on HF (10-15 / 10-24 shadow), plus model loads
log(f"{n:,} records ({n_pairs:,} pairs) for Qwen; estimate ${est:.2f} (max_usd {MAX_USD})")
if n == 0:
    spark.sql(f"CREATE OR REPLACE TABLE {T}qwen (seed_id BIGINT, cid BIGINT, p DOUBLE, adapter STRING)")
    finish("no records above 0.01: nothing for Qwen")
if est > MAX_USD:
    raise RuntimeError(f"Qwen estimate ${est:.2f} over max_usd {MAX_USD}: raise max_usd on purpose to score this night")

# COMMAND ----------

TOKEN = None


def post(path, body, tries=5):
    for t in range(tries):
        try:
            req = urllib.request.Request(f"{ENDPOINT}{path}", data=json.dumps(body).encode(), method="POST",
                                         headers={"authorization": f"Bearer {TOKEN}", "content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code < 500 or t == tries - 1:
                raise RuntimeError(f"{path}: HTTP {e.code} {e.read()[:500]!r}")
        except (urllib.error.URLError, TimeoutError) as e:
            if t == tries - 1:
                raise
            log(f"{path}: {e!r}; retrying")
        time.sleep(15 * (t + 1))


build_id = f"lwm{NIGHT.replace('-', '')}r{RUN}"[:40]
out_dir = f"{VOL}/{build_id}"
try:
    TOKEN = dbutils.secrets.get(scope=dbutils.widgets.get("secret_scope").strip(), key=dbutils.widgets.get("secret_key").strip())
    dbutils.fs.mkdirs(out_dir)
    r = post("/start", {"table_in": f"{T}qwen_rows", "out_dir": out_dir, "build_id": build_id, "warehouse": WH})
    call_id = r["call_id"]
    log(f"started Modal call {call_id} ({r.get('version')})")
    t0, last = time.time(), 0
    while True:
        s = post("/status", {"call_id": call_id})
        if s["state"] == "done":
            res = s["result"]; break
        if s["state"] == "failed":
            raise RuntimeError(f"Modal call {call_id} failed: {s.get('error')}")
        if time.time() - t0 > MAX_MIN * 60:
            raise TimeoutError(f"Modal call {call_id} not done after {MAX_MIN} min")
        if time.time() - last > 300:
            log(f"running ({(time.time() - t0) / 60:.0f} min)"); last = time.time()
        time.sleep(30)
    log(f"Modal done: {res['shards_done']}/{res['shards']} shards, {res['records']:,} records, {res['pairs']:,} pairs, "
        f"{res['shard_seconds']:.0f} shard-s, ~${res['usd_est']:.2f}, wall {res['wall_s']:.0f} s, errors {len(res['errors'])}")
    if res["errors"] or res["shards_done"] != res["shards"] or res["records"] != n or res["exported"] != n:
        raise RuntimeError(f"incomplete scoring: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:1500]}")
except Exception as e:
    raise RuntimeError(f"Qwen did not run in full: {e!r}"[:1500])

# COMMAND ----------

q = (spark.read.schema("seed_id BIGINT, cid BIGINT, p DOUBLE, shard STRING").parquet(f"{out_dir}/*.parquet")
     .select("seed_id", "cid", "p", F.lit(ADAPTER).alias("adapter")))
q.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{T}qwen")
c = spark.sql(f"""SELECT count(*) AS pairs, count(DISTINCT seed_id) AS seeds,
    (SELECT count(*) FROM {T}qwen_rows r LEFT ANTI JOIN {T}qwen q ON q.seed_id = r.seed_id) AS missing FROM {T}qwen""").collect()[0]
if c.missing or c.seeds != n:
    raise RuntimeError(f"Qwen scores do not cover the prompts: {c.asDict()}")
finish(f"Qwen scored {c.seeds:,} records / {c.pairs:,} pairs", res["usd_est"])
