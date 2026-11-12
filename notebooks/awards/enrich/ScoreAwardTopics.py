# Databricks notebook source
# MAGIC %md
# MAGIC # Awards enrich 2/4: topics on Modal (Jason's app `openalex-topics-nightly`, q8b_2m), into `award_topics_v2`
# MAGIC
# MAGIC POSTs `modal_queue_table` to the app's `/start` exactly as `notebooks/topics/nightly/TagTopicsModal.py` does (same body,
# MAGIC bearer token, call-id file, poll), checks the shard parquet files hold exactly the queued awards, appends them to the ledger
# MAGIC `<state_prefix>topics_raw`, then builds the served rows with MergeTopicsV2's builder (top 3 after dropping the
# MAGIC not-classifiable class, score = ROUND(prob, 4), names from openalex.common.topics / subfields / fields / domains, `[]` when
# MAGIC not classifiable) and MERGEs them into `target` by award_id (shape of openalex.awards.award_topics + not_classifiable,
# MAGIC text_key). Nightly Grants is not changed here: it keeps reading `inputs.topics` (openalex.awards.award_topics).
# MAGIC
# MAGIC Guards: `max_usd` (Jason's estimate), the app's own request rules (checked before the token is read), the
# MAGIC not-classifiable share among queued awards with a description (`max_nc_share`) and over all of them (`max_nc_share_all`).
# MAGIC `dry_run = true` (the default): logs the request and the estimate, reads no secret, calls nothing, writes nothing.

# COMMAND ----------

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import modal_call as M  # noqa: E402
import papers_topics as PT  # noqa: E402

for name, default in [("state_prefix", "openalex_dev.rohan_lab.awt_"), ("modal_queue_table", "openalex.transient.awt_queue"),
                      ("target", "openalex_dev.rohan_lab.awt_award_topics_v2"),
                      ("volume_dir", "/Volumes/openalex/works/data/topics_nightly/awards"), ("endpoint", M.APPS["topics"]["endpoint"]),
                      ("secret_scope", "modal"), ("secret_key", M.APPS["topics"]["secret_key"]), ("warehouse_id", ""), ("approved_warehouse", ""),
                      ("max_usd", "2"), ("max_minutes", "60"), ("max_nc_share", "0.30"), ("max_nc_share_all", "0.70"), ("dry_run", "true")]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ("state_prefix", "modal_queue_table", "target", "volume_dir", "endpoint", "secret_scope",
                                                  "secret_key", "warehouse_id", "approved_warehouse")}
P, MQ, TARGET, VOL = W["state_prefix"], W["modal_queue_table"], W["target"], W["volume_dir"].rstrip("/")
MAX_USD, MAX_MIN = float(dbutils.widgets.get("max_usd")), float(dbutils.widgets.get("max_minutes"))
MAX_NC, MAX_NC_ALL = float(dbutils.widgets.get("max_nc_share")), float(dbutils.widgets.get("max_nc_share_all"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW = f"{P}queue", f"{P}topics_raw"
SOURCE = M.APPS["topics"]["model_tag"]
TOPICS_TYPE, VOCAB = PT.TOPICS_TYPE, PT.VOCAB   # one copy of the served shape and the vocabulary, shared with the papers task


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def one(sql):
    return spark.sql(sql).collect()[0]

def planned():
    """(awards, shards, build_id) for a dry run, from build_queue's task values (its dry run writes no queue). Run by hand
    (no job), they fall back to the queue table if one exists, else 0."""
    tv = lambda k, d: dbutils.jobs.taskValues.get(taskKey="build_queue", key=k, default=d, debugValue=d)
    n = int(tv("queued", -1))
    if n < 0 and spark.catalog.tableExists(QUEUE):
        r = spark.sql(f"SELECT count(*) AS n, count(DISTINCT shard) AS s, max(build_id) AS b FROM {QUEUE}").collect()[0]
        return int(r.n), int(r.s), r.b or "awt-dry"
    return max(n, 0), int(tv("shards", 1)), tv("build_id", "awt-dry")

# COMMAND ----------

if DRY:
    n, n_shards, build_id = planned()
else:
    q = one(f"SELECT count(*) AS n, count(DISTINCT build_id) AS builds, max(build_id) AS b, count(DISTINCT shard) AS shards FROM {QUEUE}")
    n, n_shards, build_id = int(q.n), int(q.shards), q.b
    assert n == 0 or q.builds == 1, f"queue mixes {q.builds} builds"
if n == 0:
    dbutils.notebook.exit("empty queue: nothing to score")
est = M.estimate_usd("topics", n, n_shards)
body = M.start_body("topics", MQ, f"{VOL}/{build_id}", build_id, W["warehouse_id"], [])
bad = M.check_request("topics", body, W["approved_warehouse"])
log(f"build {build_id}: {n:,} awards; estimate ${est:.2f} (max_usd {MAX_USD}); request {json.dumps(body)}")
if bad:
    log("the app / our rules would refuse this request: " + "; ".join(bad))
if DRY:
    dbutils.notebook.exit(json.dumps({"dry_run": True, "awards": n, "usd_est": round(est, 2), "request": body, "refusals": bad}))
if bad:
    raise RuntimeError("refusing to call the topics app: " + "; ".join(bad))
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose to score this queue")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RAW} (award_id BIGINT, topic_ids ARRAY<INT>, probs ARRAY<FLOAT>, not_classifiable BOOLEAN,
  shard STRING, source STRING, build_id STRING, scored_at TIMESTAMP) CLUSTER BY (build_id)
  COMMENT 'Awards enrich: raw q8b_2m scores per queued award (top-10 topic ids, -1 = not classifiable), append-only, one build per run'""")
L = one(f"""SELECT count(*) AS rows, count(DISTINCT award_id) AS ids,
  (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN (SELECT award_id FROM {RAW} WHERE build_id = '{build_id}') r ON r.award_id = q.award_id) AS missing
  FROM {RAW} WHERE build_id = '{build_id}'""")
loaded = L.rows == L.ids == n and L.missing == 0
if loaded:   # a repair or retry: no second Modal call, no second INSERT of the same build
    log(f"build {build_id} already loaded in {RAW} ({L.rows:,} rows): skipping Modal and the raw INSERT")
else:
    token = dbutils.secrets.get(scope=W["secret_scope"], key=W["secret_key"])
    res = M.ModalCall("topics", W["endpoint"], token, dbutils.fs, log).run(body, MAX_MIN)
    log(f"Modal done: {res['shards_done']}/{res['shards']} shards, {res['works']:,} awards ({res['nc']:,} not classifiable), "
        f"{res['shard_seconds']:.0f} shard-s, ~${res['usd_est']:.2f} (excl. model load), wall {res['wall_s']:.0f} s, errors {len(res['errors'])}")
    if res["errors"] or res["shards_done"] != res["shards"] or res["works"] != n or res["exported"] != n:
        dbutils.fs.rm(f"{VOL}/{build_id}/_call_id.txt")
        raise RuntimeError(f"incomplete scoring: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:2000]}")
    dbutils.jobs.taskValues.set(key="usd_est", value=float(res["usd_est"]))
    from pyspark.sql import functions as F  # noqa: E402
    raw = (spark.read.schema("work_id BIGINT, topic_ids ARRAY<INT>, probs ARRAY<FLOAT>, not_classifiable BOOLEAN, shard STRING")
           .parquet(f"{VOL}/{build_id}/*.parquet").withColumnRenamed("work_id", "award_id")
           .withColumn("source", F.lit(SOURCE)).withColumn("build_id", F.lit(build_id)).withColumn("scored_at", F.current_timestamp()))
    raw.createOrReplaceTempView("awt_topics_files")
    c = one(f"""SELECT count(*) AS rows, count(DISTINCT award_id) AS ids,
        (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN awt_topics_files r ON r.award_id = q.award_id) AS missing,
        count_if(NOT (size(topic_ids) <=> 10 AND size(probs) <=> 10 AND not_classifiable <=> (topic_ids[0] = -1))) AS bad
        FROM awt_topics_files""")
    if not (c.rows == c.ids == n and c.missing == 0 and c.bad == 0):
        raise RuntimeError(f"shard files do not match the queue: {c.asDict()}")
    spark.sql(f"DELETE FROM {RAW} WHERE build_id = '{build_id}'")
    spark.sql(f"INSERT INTO {RAW} SELECT award_id, topic_ids, probs, not_classifiable, shard, source, build_id, scored_at FROM awt_topics_files")

# COMMAND ----------

# Gates: NC shares (with a description = the model-health signal; all = catches a model failing on everything), ids resolve.
g = one(f"""SELECT count(*) AS n, count_if(r.not_classifiable) AS nc,
    count_if(length(q.abstract) > 0) AS n_desc, count_if(length(q.abstract) > 0 AND r.not_classifiable) AS nc_desc,
    count_if(exists(r.topic_ids, x -> x <> -1 AND NOT array_contains(t.ids, x))) AS unresolved,
    (SELECT count(*) - count(DISTINCT tid) FROM ({VOCAB})) AS vocab_dupes
  FROM {RAW} r JOIN {QUEUE} q ON q.award_id = r.award_id
  CROSS JOIN (SELECT collect_set(CAST(topic_id AS INT)) AS ids FROM openalex.common.topics) t WHERE r.build_id = '{build_id}'""")
log(f"gate: {g.asDict()}")
bad = []
if g.n != n: bad.append(f"ledger {g.n:,} != queue {n:,}")
if g.n_desc and g.nc_desc / g.n_desc > MAX_NC: bad.append(f"NC share with a description {g.nc_desc / g.n_desc:.3f} > {MAX_NC}")
if g.n and g.nc / g.n > MAX_NC_ALL: bad.append(f"NC share over all {g.nc / g.n:.3f} > {MAX_NC_ALL}")
if g.unresolved: bad.append(f"{g.unresolved:,} awards carry a topic id not in openalex.common.topics")
if g.vocab_dupes: bad.append(f"the topic vocabulary repeats {g.vocab_dupes:,} topic ids (an award would get more than 3 topics)")
if bad:
    raise RuntimeError("refusing to merge, NOTHING written to the target: " + "; ".join(bad))

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (award_id BIGINT, topics {TOPICS_TYPE}, not_classifiable BOOLEAN, source STRING,
  created_datetime TIMESTAMP, updated_datetime TIMESTAMP, text_key BIGINT)
  COMMENT 'Awards enrich: q8b_2m topics per award (shape of openalex.awards.award_topics + not_classifiable, text_key)'""")
# MergeTopicsV2's builder: the first 3 topic ids that are not -1, in array order, broadcast-joined to the vocabulary and gathered
# back in position order. Never a CROSS JOIN of the vocabulary onto the awards: it stalled the 6.68M backfill (oxjob #2125).
spark.sql(f"""MERGE INTO {TARGET} v USING (
WITH meta AS ({VOCAB}),
sc AS (SELECT * FROM {RAW} WHERE build_id = '{build_id}'),
top3 AS (
  SELECT sc.award_id, e.rn, e.x.topic_ids AS tid, e.x.probs AS p
  FROM sc LATERAL VIEW posexplode(slice(filter(arrays_zip(sc.topic_ids, sc.probs), x -> x.topic_ids <> -1), 1, 3)) e AS rn, x
  WHERE NOT sc.not_classifiable),
agg AS (
  SELECT /*+ BROADCAST(m) */ k.award_id, transform(array_sort(collect_list(named_struct('rn', k.rn,
      'id', concat('https://openalex.org/T', k.tid), 'display_name', m.display_name, 'score', CAST(ROUND(k.p, 4) AS FLOAT),
      'subfield', m.subfield, 'field', m.field, 'domain', m.domain))),
    x -> named_struct('id', x.id, 'display_name', x.display_name, 'score', x.score, 'subfield', x.subfield, 'field', x.field,
                      'domain', x.domain)) AS topics
  FROM top3 k LEFT JOIN meta m ON m.tid = k.tid GROUP BY k.award_id)
SELECT sc.award_id,
  CASE WHEN sc.not_classifiable THEN CAST(ARRAY() AS {TOPICS_TYPE})
       ELSE coalesce(CAST(a.topics AS {TOPICS_TYPE}), CAST(ARRAY() AS {TOPICS_TYPE})) END AS topics,
  sc.not_classifiable, '{SOURCE}' AS source, current_timestamp() AS ts, q.text_key
FROM sc LEFT JOIN agg a ON a.award_id = sc.award_id JOIN {QUEUE} q ON q.award_id = sc.award_id) r
ON v.award_id = r.award_id
WHEN MATCHED THEN UPDATE SET v.topics = r.topics, v.not_classifiable = r.not_classifiable, v.source = r.source,
  v.updated_datetime = r.ts, v.text_key = r.text_key
WHEN NOT MATCHED THEN INSERT (award_id, topics, not_classifiable, source, created_datetime, updated_datetime, text_key)
  VALUES (r.award_id, r.topics, r.not_classifiable, r.source, r.ts, r.ts, r.text_key)""")
c = one(f"""SELECT count(*) AS n, count_if(NOT not_classifiable AND size(topics) <> 3) AS bad_size,
  count_if(exists(topics, t -> t.display_name IS NULL OR t.field.id IS NULL)) AS unresolved
  FROM {TARGET} WHERE award_id IN (SELECT award_id FROM {QUEUE})""")
if c.n != n or c.bad_size or c.unresolved:
    raise RuntimeError(f"target check failed after merge: {c.asDict()} (queue {n:,})")
log(f"build {build_id}: {n:,} awards served in {TARGET} ({g.nc:,} not classifiable, {g.nc / n:.2%})")
