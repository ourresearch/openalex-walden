# Databricks notebook source
# MAGIC %md
# MAGIC # Work types 2/2: score the queue on Modal, load the scores (oxjob #1976; TagTopicsModal's shape)
# MAGIC
# MAGIC vLLM does not run on Databricks (FIPS), so stage 2 runs on Modal (`modal_worktypes_nightly.py` in this folder, app
# MAGIC `openalex-worktypes-nightly`, worktype-stage2's: the v6c scorer, at most 8 H100s). This task POSTs the queue table to
# MAGIC the app's `/start` (bearer token from secret scope `modal`), polls `/status`, then checks that the shard parquet files in
# MAGIC `<volume_dir>/<build_id>/` hold exactly the queued texts (`sql/03_scores_load.sql`) and inserts each text's scores once
# MAGIC into `openalex.works.work_type_scores`. The End 2 End cell reads them the next night.
# MAGIC
# MAGIC Spend guard: refuses to start if the estimate for the queue is over `max_usd`. A retry never starts a second GPU run: the
# MAGIC call id is kept in `<volume_dir>/<build_id>/_call_id.txt` and polled again (dropped when the call failed, so the retry
# MAGIC starts a fresh one); a build already loaded is skipped. After a good load the build's shard and scratch files are removed.

# COMMAND ----------

import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import sql_templates as T  # noqa: E402
import work_type_cell as C  # noqa: E402

for name, default in [("schema", "openalex.works"), ("queue", "openalex.transient.work_type_queue"),
                      ("policy_id", "pol5-4ed2aed64c8e"), ("volume_dir", "/Volumes/openalex/works/work_types"),
                      ("endpoint", "https://wordslikethis--openalex-worktypes-nightly-api.modal.run"), ("secret_scope", "modal"),
                      ("secret_key", "worktypes_nightly_token"), ("warehouse_id", "3996dc0a9b183ce3"), ("max_usd", "10"),
                      ("max_minutes", "150"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)  # noqa: F821
G = {k: dbutils.widgets.get(k).strip() for k in ("schema", "queue", "policy_id", "volume_dir", "endpoint", "secret_scope",  # noqa: F821
                                                  "secret_key", "warehouse_id", "max_usd", "max_minutes", "dry_run")}
S, QUEUE, VOL, ENDPOINT, WH = G["schema"], G["queue"], G["volume_dir"].rstrip("/"), G["endpoint"].rstrip("/"), G["warehouse_id"]
MAX_USD, MAX_MIN, DRY = float(G["max_usd"]), float(G["max_minutes"]), G["dry_run"].lower() == "true"
POL = T.load_policy(G["policy_id"])
if POL["model_id"] not in C.SCORED_MODELS:
    raise RuntimeError(f"{POL['policy_id']}: its model {POL['model_id']} isn't scored by the Modal app ({C.SCORED_MODELS})")
for t in (S + ".x", QUEUE):
    assert t.startswith("openalex.") and t.replace(".", "").replace("_", "").isalnum(), f"bad table name {t!r}"
assert VOL.startswith("/Volumes/openalex/") and ".." not in VOL and len(WH) == 16, (VOL, WH)
TOKEN = dbutils.secrets.get(scope=G["secret_scope"], key=G["secret_key"])  # noqa: F821
P = {"S": S, "queue": QUEUE, "model_id": POL["model_id"], "render_version": POL["render_version"], "n_classes": str(len(POL["classes"]))}


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def one(sql):
    return spark.sql(sql).collect()[0]  # noqa: F821


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

q = one(f"SELECT count(*) AS n, count(DISTINCT build_id) AS builds, max(build_id) AS b, count(DISTINCT model_version) AS models, "
        f"max(model_version) AS m, count(DISTINCT shard) AS shards FROM {QUEUE}")
n, build_id = int(q.n), q.b
if n == 0:
    dbutils.notebook.exit("empty queue: nothing to score")  # noqa: F821
assert q.builds == 1 and q.models == 1 and q.m == POL["model_id"], f"queue: {q.asDict()}, policy model {POL['model_id']}"
est = n / 1e6 * 2.8 + 0.18 * min(8, int(q.shards))   # ~520 texts/s per H100 at ~$5.2/h, plus ~2 min load per container
log(f"build {build_id}: {n:,} texts in {q.shards} shards; estimate ${est:.2f} (max_usd {MAX_USD})")
loaded = one(f"""SELECT count(*) AS n FROM {QUEUE} q JOIN {S}.work_type_scores s
                 ON s.work_id = q.work_id AND s.input_hash = q.input_hash AND s.model_version = q.model_version""").n
if loaded == n:
    dbutils.notebook.exit(f"build {build_id} already scored and loaded")  # noqa: F821
if DRY:
    dbutils.notebook.exit("dry run")  # noqa: F821
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose to score this queue")

# COMMAND ----------

out_dir = f"{VOL}/{build_id}"
call_file = f"{out_dir}/_call_id.txt"
dbutils.fs.mkdirs(out_dir)  # noqa: F821
if "_call_id.txt" in {f.name for f in dbutils.fs.ls(out_dir)}:  # noqa: F821
    call_id = dbutils.fs.head(call_file).strip()  # noqa: F821
    log(f"resuming Modal call {call_id}")
else:
    r = post("/start", {"table_in": QUEUE, "out_dir": out_dir, "build_id": build_id, "warehouse": WH})
    call_id = r["call_id"]
    dbutils.fs.put(call_file, call_id, overwrite=True)  # noqa: F821
    log(f"started Modal call {call_id} ({r.get('version')})")

t0 = time.time()
last = 0
while True:
    s = post("/status", {"call_id": call_id})
    if s["state"] == "done":
        res = s["result"]
        break
    if s["state"] == "failed":
        dbutils.fs.rm(call_file)  # noqa: F821   (so a retry starts a fresh call instead of re-reading this failure)
        raise RuntimeError(f"Modal call {call_id} failed: {s.get('error')}")
    if time.time() - t0 > MAX_MIN * 60:
        raise TimeoutError(f"Modal call {call_id} not done after {MAX_MIN} min (a retry resumes polling it)")
    if time.time() - last > 300:
        log(f"running ({(time.time() - t0) / 60:.0f} min)")
        last = time.time()
    time.sleep(30)
log(f"Modal done: {res.get('shards_done')}/{res.get('shards')} shards, {res.get('works'):,} texts, ~${res.get('usd_est', 0):.2f}, "
    f"wall {res.get('wall_s', 0):.0f} s, errors {len(res.get('errors') or [])}")
if res.get("errors") or res.get("shards_done") != res.get("shards") or res.get("works") != n or res.get("exported") != n:
    dbutils.fs.rm(call_file)  # noqa: F821   (a retry re-runs every shard; files are overwritten)
    raise RuntimeError(f"incomplete scoring: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:2000]}")

# COMMAND ----------

(spark.read.schema("work_id BIGINT, input_hash STRING, model_version STRING, render_version STRING, class_idx INT, "  # noqa: F821
                   "confidence DOUBLE, logits ARRAY<FLOAT>, shard STRING")
 .parquet(f"{out_dir}/*.parquet").createOrReplaceTempView("wt_shards"))
check, load = T.statements(T.render("03_scores_load.sql", **P))
c = spark.sql(check).collect()[0]  # noqa: F821
bad = {k: v for k, v in c.asDict().items() if k not in ("rows_", "ids") and v}
if bad or not (c.rows_ == c.ids == n):
    raise RuntimeError(f"shard files do not match the queue, NOTHING loaded: {c.asDict()} (queue {n:,})")
m = spark.sql(load).collect()[0]  # noqa: F821
loaded = one(f"""SELECT count(*) AS n FROM {QUEUE} q JOIN {S}.work_type_scores s
                 ON s.work_id = q.work_id AND s.input_hash = q.input_hash AND s.model_version = q.model_version""").n
if loaded != n:
    raise RuntimeError(f"{loaded:,} of {n:,} queued texts are in {S}.work_type_scores after the load")
log(f"loaded build {build_id}: {int(m.num_inserted_rows):,} new score rows, {int(m.num_updated_rows):,} already there (touched)")
for d in (out_dir, f"{VOL}/_build/{build_id}"):      # the build is loaded: its shard and scratch files go
    dbutils.fs.rm(d, True)  # noqa: F821
dbutils.jobs.taskValues.set(key="usd_est", value=float(res.get("usd_est", 0.0)))  # noqa: F821
dbutils.jobs.taskValues.set(key="modal_wall_s", value=float(res.get("wall_s", 0.0)))  # noqa: F821
