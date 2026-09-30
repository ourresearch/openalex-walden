# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords nightly 2/4: tag the queue on Modal (oxjob #1322)
# MAGIC
# MAGIC vLLM does not run on Databricks (FIPS), so the student runs on Modal (`modal_keywords_nightly.py` in this folder, app
# MAGIC `oxjob1322-keywords-nightly`, at most 10 B200s). This task POSTs the queue table + shard list to the app's `/start`
# MAGIC (bearer token from secret scope `modal`), polls `/status`, then checks that every shard file landed in
# MAGIC `<volume_dir>/<build_id>/` and loads them into the append-only raw table `<state_prefix>raw` (one row per queued work,
# MAGIC `keywords` + per-keyword mean token log-prob `lp`, exactly as the corpus retag wrote them).
# MAGIC
# MAGIC Spend guard: refuses to start if the estimate for the queue is over `max_usd`. A retry never starts a second GPU run:
# MAGIC the call id is kept in `<volume_dir>/<build_id>/_call_id.txt` and polled again; a build already loaded is skipped.

# COMMAND ----------

import datetime as dt
import json
import time
import urllib.error
import urllib.request

for name, default in [("state_prefix", "openalex.works.work_keywords_v2_"), ("volume_dir", "/Volumes/openalex/authors/oxjob_tmp/oxjob1322_god/nightly"),
                      ("endpoint", "https://wordslikethis--oxjob1322-keywords-nightly-api.modal.run"), ("secret_scope", "modal"),
                      ("secret_key", "kw1322_nightly_token"), ("warehouse_id", "3996dc0a9b183ce3"), ("max_usd", "60"), ("max_minutes", "120"),
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
TAGGER_VERSION, MODEL, QUANT = "kw1322-student-v3fix", "/vol/models/student_qwen4b_v3fix", "fp8"
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

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RAW} (id STRING, keywords ARRAY<STRING>, lp ARRAY<DOUBLE>, model STRING, shard BIGINT,
  build_id STRING, tagger_version STRING, tagged_at TIMESTAMP) CLUSTER BY (build_id)
  COMMENT 'Keywords nightly (oxjob #1322): raw student output per tagged work (id W<n>), append-only, one build per run'""")
q = spark.sql(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}").collect()[0]
n, build_id = int(q.n), q.b
if n == 0:
    dbutils.notebook.exit("empty queue: nothing to tag")
shards = [int(r.shard) for r in spark.sql(f"SELECT DISTINCT shard FROM {QUEUE} ORDER BY shard").collect()]
est = n / 1e6 * 15 + 0.5 * min(len(shards), 10)   # measured 2026-09-30: $7.49 for 613,905 works on 10 B200s incl. cold starts
log(f"build {build_id}: {n:,} works in {len(shards)} shards; estimate ${est:.2f} (max_usd {MAX_USD})")
if spark.sql(f"SELECT count(*) AS n FROM {RAW} WHERE build_id = '{build_id}'").collect()[0].n == n:
    dbutils.notebook.exit(f"build {build_id} already tagged and loaded")
if DRY:
    dbutils.notebook.exit("dry run")
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose to tag this queue")

# COMMAND ----------

out_dir = f"{VOL}/{build_id}"
call_file = f"{out_dir}/_call_id.txt"
dbutils.fs.mkdirs(out_dir)
if "_call_id.txt" in {f.name for f in dbutils.fs.ls(out_dir)}:
    call_id = dbutils.fs.head(call_file).strip()
    log(f"resuming Modal call {call_id}")
else:
    r = post("/start", {"table_in": QUEUE, "out_dir": out_dir, "shards": shards, "model": MODEL, "quant": QUANT, "warehouse": WH})
    call_id = r["call_id"]
    dbutils.fs.put(call_file, call_id, overwrite=True)
    log(f"started Modal call {call_id} ({r['shards']} shards)")

t0 = time.time(); last = 0
while True:
    s = post("/status", {"call_id": call_id})
    if s["state"] == "done":
        res = s["result"]; break
    if s["state"] == "failed":
        raise RuntimeError(f"Modal call {call_id} failed: {s.get('error')}")
    if time.time() - t0 > MAX_MIN * 60:
        raise TimeoutError(f"Modal call {call_id} not done after {MAX_MIN} min (a retry resumes polling it)")
    if time.time() - last > 300:
        log(f"running ({(time.time() - t0) / 60:.0f} min)"); last = time.time()
    time.sleep(30)
log(f"Modal done: {res['shards_done']}/{res['shards']} shards, {res['works']:,} works, {res['shard_seconds']:.0f} shard-s, "
    f"~${res['usd_est']:.2f} (excl. cold starts), wall {res['wall_s']:.0f} s, errors {len(res['errors'])}")
if res["errors"] or res["shards_done"] != len(shards) or res["works"] != n:
    raise RuntimeError(f"incomplete tagging: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:2000]}")

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

raw = (spark.read.schema("id STRING, keywords ARRAY<STRING>, lp ARRAY<DOUBLE>, model STRING, shard BIGINT").json(f"{out_dir}/*.jsonl.gz")
       .withColumn("build_id", F.lit(build_id)).withColumn("tagger_version", F.lit(TAGGER_VERSION)).withColumn("tagged_at", F.current_timestamp()))
raw.createOrReplaceTempView("kw_nightly_raw_files")
c = spark.sql(f"""SELECT count(*) AS rows, count(DISTINCT id) AS ids,
    (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN kw_nightly_raw_files r ON r.id = concat('W', CAST(q.id AS STRING))) AS missing,
    (SELECT count(*) FROM kw_nightly_raw_files r LEFT ANTI JOIN {QUEUE} q ON r.id = concat('W', CAST(q.id AS STRING))) AS extra,
    count_if(keywords IS NULL OR lp IS NULL OR size(keywords) <> size(lp)) AS bad FROM kw_nightly_raw_files""").collect()[0]
if not (c.rows == c.ids == n and c.missing == 0 and c.extra == 0 and c.bad == 0):
    raise RuntimeError(f"shard files do not match the queue: {c.asDict()}")
spark.sql(f"DELETE FROM {RAW} WHERE build_id = '{build_id}'")   # a half-loaded earlier attempt
spark.sql(f"INSERT INTO {RAW} SELECT id, keywords, lp, model, shard, build_id, tagger_version, tagged_at FROM kw_nightly_raw_files")
e = spark.sql(f"SELECT count(*) AS n, count_if(size(keywords) = 0) AS empty, round(avg(size(keywords)), 2) AS kw FROM {RAW} WHERE build_id = '{build_id}'").collect()[0]
log(f"loaded {e.n:,} rows into {RAW} (model returned no keyword for {e.empty:,}; {e.kw} raw keywords per work)")
dbutils.jobs.taskValues.set(key="usd_est", value=float(res["usd_est"]))
dbutils.jobs.taskValues.set(key="modal_wall_s", value=float(res["wall_s"]))
