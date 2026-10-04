# Databricks notebook source
# MAGIC %md
# MAGIC # Topics nightly 2/3: score the queue on Modal (oxjob #1531)
# MAGIC
# MAGIC vLLM does not run on Databricks (FIPS), so the q8b_2m student runs on Modal (`modal_topics_nightly.py` in this folder, app
# MAGIC `openalex-topics-nightly`, at most 8 H100s). This task POSTs the queue table to the app's `/start` (bearer token from secret scope
# MAGIC `modal`), polls `/status`, then checks that the shard parquet files in `<volume_dir>/<build_id>/` hold exactly the queued works and
# MAGIC loads them into the append-only ledger `<state_prefix>raw` (raw top-10 classes per work, the `topics_v2_scores` schema plus
# MAGIC source / build_id / scored_at).
# MAGIC
# MAGIC Spend guard: refuses to start if the estimate for the queue is over `max_usd`. A retry never starts a second GPU run: the call id
# MAGIC is kept in `<volume_dir>/<build_id>/_call_id.txt` and polled again (dropped when the call failed, so the retry starts a fresh
# MAGIC one); a build already loaded is skipped.

# COMMAND ----------

import datetime as dt
import json
import time
import urllib.error
import urllib.request

for name, default in [("state_prefix", "openalex.works.work_topics_v2_"), ("volume_dir", "/Volumes/openalex/works/data/topics_nightly"),
                      ("endpoint", "https://wordslikethis--openalex-topics-nightly-api.modal.run"), ("secret_scope", "modal"),
                      ("secret_key", "topics1531_nightly_token"), ("warehouse_id", "3996dc0a9b183ce3"), ("max_usd", "30"), ("max_minutes", "150"),
                      ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
VOL = dbutils.widgets.get("volume_dir").strip().rstrip("/")
ENDPOINT = dbutils.widgets.get("endpoint").strip().rstrip("/")
WH = dbutils.widgets.get("warehouse_id").strip()
MAX_USD = float(dbutils.widgets.get("max_usd"))
MAX_MIN = float(dbutils.widgets.get("max_minutes"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW = f"{P}queue", f"{P}raw"
SOURCE = "q8b_2m"   # = work_topics_v2.source
TOKEN = dbutils.secrets.get(scope=dbutils.widgets.get("secret_scope").strip(), key=dbutils.widgets.get("secret_key").strip())


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


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

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RAW} (work_id BIGINT, topic_ids ARRAY<INT>, probs ARRAY<FLOAT>, not_classifiable BOOLEAN, shard STRING,
  source STRING, build_id STRING, scored_at TIMESTAMP) CLUSTER BY (build_id)
  COMMENT 'Topics nightly (oxjob #1531): raw q8b_2m scores per queued work (top-10 topic ids, -1 = not classifiable, probs aligned), append-only, one build per run'""")
q = spark.sql(f"SELECT count(*) AS n, count(DISTINCT build_id) AS builds, max(build_id) AS b FROM {QUEUE}").collect()[0]
n, build_id = int(q.n), q.b
if n == 0:
    dbutils.notebook.exit("empty queue: nothing to score")
assert q.builds == 1, f"queue mixes {q.builds} builds"
est = n / 1e6 * 5.5 + 0.45 * min(8, max(1, n // 25000))   # ~300 works/s per H100 at ~$5.2/h, plus ~5 min load per container (estimate)
log(f"build {build_id}: {n:,} works; estimate ${est:.2f} (max_usd {MAX_USD})")
if spark.sql(f"SELECT count(*) AS n FROM {RAW} WHERE build_id = '{build_id}'").collect()[0].n == n:
    dbutils.notebook.exit(f"build {build_id} already scored and loaded")
if DRY:
    dbutils.notebook.exit("dry run")
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose to score this queue")

# COMMAND ----------

out_dir = f"{VOL}/{build_id}"
call_file = f"{out_dir}/_call_id.txt"
dbutils.fs.mkdirs(out_dir)
if "_call_id.txt" in {f.name for f in dbutils.fs.ls(out_dir)}:
    call_id = dbutils.fs.head(call_file).strip()
    log(f"resuming Modal call {call_id}")
else:
    r = post("/start", {"table_in": QUEUE, "out_dir": out_dir, "build_id": build_id, "warehouse": WH})
    call_id = r["call_id"]
    dbutils.fs.put(call_file, call_id, overwrite=True)
    log(f"started Modal call {call_id} ({r.get('version')})")

t0 = time.time(); last = 0
while True:
    s = post("/status", {"call_id": call_id})
    if s["state"] == "done":
        res = s["result"]; break
    if s["state"] == "failed":
        dbutils.fs.rm(call_file)   # so a retry starts a fresh call instead of re-reading this failure
        raise RuntimeError(f"Modal call {call_id} failed: {s.get('error')}")
    if time.time() - t0 > MAX_MIN * 60:
        raise TimeoutError(f"Modal call {call_id} not done after {MAX_MIN} min (a retry resumes polling it)")
    if time.time() - last > 300:
        log(f"running ({(time.time() - t0) / 60:.0f} min)"); last = time.time()
    time.sleep(30)
log(f"Modal done: {res['shards_done']}/{res['shards']} shards, {res['works']:,} works ({res['nc']:,} not classifiable), export {res['export_s']:.0f} s, "
    f"{res['shard_seconds']:.0f} shard-s, ~${res['usd_est']:.2f} (excl. model load), wall {res['wall_s']:.0f} s, errors {len(res['errors'])}")
if res["errors"] or res["shards_done"] != res["shards"] or res["works"] != n or res["exported"] != n:
    dbutils.fs.rm(call_file)   # a retry re-runs every shard (files are overwritten)
    raise RuntimeError(f"incomplete scoring: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:2000]}")

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

raw = (spark.read.schema("work_id BIGINT, topic_ids ARRAY<INT>, probs ARRAY<FLOAT>, not_classifiable BOOLEAN, shard STRING").parquet(f"{out_dir}/*.parquet")
       .withColumn("source", F.lit(SOURCE)).withColumn("build_id", F.lit(build_id)).withColumn("scored_at", F.current_timestamp()))
raw.createOrReplaceTempView("tp_nightly_raw_files")
c = spark.sql(f"""SELECT count(*) AS rows, count(DISTINCT work_id) AS ids,
    (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN tp_nightly_raw_files r ON r.work_id = q.work_id) AS missing,
    (SELECT count(*) FROM tp_nightly_raw_files r LEFT ANTI JOIN {QUEUE} q ON r.work_id = q.work_id) AS extra,
    count_if(topic_ids IS NULL OR probs IS NULL OR size(topic_ids) <> 10 OR size(probs) <> 10 OR not_classifiable IS NULL
             OR not_classifiable <> (topic_ids[0] = -1)) AS bad FROM tp_nightly_raw_files""").collect()[0]
if not (c.rows == c.ids == n and c.missing == 0 and c.extra == 0 and c.bad == 0):
    raise RuntimeError(f"shard files do not match the queue: {c.asDict()}")
spark.sql(f"DELETE FROM {RAW} WHERE build_id = '{build_id}'")   # a half-loaded earlier attempt
spark.sql(f"INSERT INTO {RAW} SELECT work_id, topic_ids, probs, not_classifiable, shard, source, build_id, scored_at FROM tp_nightly_raw_files")
e = spark.sql(f"SELECT count(*) AS n, count_if(not_classifiable) AS nc FROM {RAW} WHERE build_id = '{build_id}'").collect()[0]
if e.n != n:
    raise RuntimeError(f"{RAW} holds {e.n:,} rows for build {build_id}, queue {n:,}")
log(f"loaded {e.n:,} rows into {RAW} ({e.nc:,} not classifiable, {e.nc / e.n:.2%})")
dbutils.jobs.taskValues.set(key="usd_est", value=float(res["usd_est"]))
dbutils.jobs.taskValues.set(key="modal_wall_s", value=float(res["wall_s"]))
