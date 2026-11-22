# Databricks notebook source
# MAGIC %md
# MAGIC # Sync work links to Elasticsearch (oxjob #1490)
# MAGIC
# MAGIC `openalex.works.work_links` (built by `notebooks/end2end/CreateWorkLinks`) -> the `links` index: one doc per link,
# MAGIC three keyword fields, `origin`, `target` and `relationship`, as full URLs. `_id` = `origin|target|relationship`
# MAGIC (the W digits and the relationship value), so a link always lands on the same doc.
# MAGIC
# MAGIC `openalex.works.work_links_indexed` holds what the index has. Each run: links in `work_links` and not in it are
# MAGIC indexed, links in it and not in `work_links` are deleted, and after each chunk it takes the links the index
# MAGIC accepted. A failed or stopped run is re-covered by the next. The first run (an empty state) is the full build.
# MAGIC
# MAGIC `index` names the index this run writes. Each index keeps its own state table (`links-v2` ->
# MAGIC `work_links_indexed_v2`), so a new version is built beside the live one and only the `links` alias moves.
# MAGIC
# MAGIC `slice_ppm` limits a run to a hash slice of the links (10000 = 1%), to measure size and speed before a full build.
# MAGIC Load on search (#1642): `writers` bulk streams at night, `day_writers` 07:00-19:00 CT; every stream but the first
# MAGIC pauses while data-node p95 CPU is over 85% in 3 of the last 4 minutes (the `sync_work_metrics` rule).
# MAGIC Plan: oxjobs `working/references-entity/LINKS_TABLE_PLAN.md`.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0
# MAGIC %restart_python

# COMMAND ----------

import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from elasticsearch import Elasticsearch, helpers

dbutils.widgets.dropdown("mode", "nightly", ["nightly", "dry_run"])
dbutils.widgets.text("max_push", "200000000")     # changes per run at most; the rest wait for the next run
dbutils.widgets.text("writers", "24")              # concurrent bulk streams (Spark partitions) at night
dbutils.widgets.text("day_writers", "6")           # cap 07:00-19:00 CT
dbutils.widgets.text("chunk_rows", "50000000")     # changes per checkpointed chunk
dbutils.widgets.text("bulk_size", "5000")          # actions per bulk request
dbutils.widgets.text("slice_ppm", "1000000")       # parts per million of links this run covers (10000 = 1%)
dbutils.widgets.text("stop_at", "")                # CT HH:MM: start no chunk after the first such time from the start
dbutils.widgets.text("index", "links-v1")          # the index this run writes; its state table follows its version
dbutils.widgets.text("index_sort", "target,origin")  # sort of a NEW index (empty = none); an existing index keeps its own

MODE = dbutils.widgets.get("mode")
DRY_RUN = MODE == "dry_run"
CAP = int(dbutils.widgets.get("max_push") or 0) or None
WRITERS = max(1, int(dbutils.widgets.get("writers") or 24))
DAY_WRITERS = max(1, int(dbutils.widgets.get("day_writers") or 6))
CHUNK_ROWS = int(dbutils.widgets.get("chunk_rows") or 50_000_000)
BULK = int(dbutils.widgets.get("bulk_size") or 5000)
SLICE_PPM = int(dbutils.widgets.get("slice_ppm") or 1_000_000)
STOP_AT = dbutils.widgets.get("stop_at").strip()
INDEX = dbutils.widgets.get("index").strip() or "links-v1"
SORT = [f.strip() for f in dbutils.widgets.get("index_sort").split(",") if f.strip()]

LINKS_TABLE = "openalex.works.work_links"
_v = "" if INDEX == "links-v1" else "_" + INDEX.rsplit("-", 1)[-1]   # links-v2 -> _v2
STATE_TABLE = f"openalex.works.work_links_indexed{_v}"
_scratch = _v + ("_dry" if DRY_RUN else "")
DIFF_TABLE = f"openalex.works.work_links_sync_diff{_scratch}"
FAILED_TABLE = f"openalex.works.work_links_sync_failed{_scratch}"
ALIAS = "links"
ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
HOT_P95 = 85
CT = ZoneInfo("America/Chicago")
KEY = "origin_work_id, target_work_id, relationship"
IN_SLICE = f"pmod(xxhash64({KEY}), 1000000) < {SLICE_PPM}"
t_start = time.time()
STOP_DT = None
if STOP_AT:                                        # 04:30 from a 13:00 start means tomorrow 04:30
    _now = datetime.now(CT)
    STOP_DT = _now.replace(hour=int(STOP_AT[:2]), minute=int(STOP_AT[3:5]), second=0, microsecond=0)
    if STOP_DT <= _now:
        STOP_DT += timedelta(days=1)
print(f"mode={MODE} index={INDEX} state={STATE_TABLE} cap={CAP} writers={WRITERS} day_writers={DAY_WRITERS} "
      f"chunk_rows={CHUNK_ROWS:,} slice_ppm={SLICE_PPM} stop_at={f'{STOP_DT:%a %H:%M} CT' if STOP_DT else '-'}")

# COMMAND ----------

# The index, created on its first run, kept slim: three keyword fields, no text, no analyzers. (No synthetic _source:
# this cluster turns it into stored _source; links-v1 asked for it and got stored, checked 22 Nov 2026.)
# 20 primary shards, ~190M links each (links-v1 had 10 at ~374M, over Elastic's ~200M-docs-per-shard guidance).
# Stored in order by target, then origin, so a work's incoming links sit together and sorted lookups stay fast.
# Built with no replica and no refresh; once full, set number_of_replicas 1 and refresh_interval 30s by hand.
INDEX_BODY = {
    "settings": {"index": {"number_of_shards": 20, "number_of_replicas": 0, "refresh_interval": "-1",
                           **({"sort": {"field": SORT, "order": ["asc"] * len(SORT)}} if SORT else {})}},
    "mappings": {"dynamic": "strict", "properties": {
        "origin": {"type": "keyword"}, "target": {"type": "keyword"}, "relationship": {"type": "keyword"}}},
}


def es_client():
    return Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True,
                         http_compress=True)


if not DRY_RUN:
    client = es_client()
    try:
        if not client.indices.exists(index=INDEX):
            client.indices.create(index=INDEX, body=INDEX_BODY)
            print(f"Created {INDEX}: {INDEX_BODY['settings']}")
        if not client.indices.exists_alias(name=ALIAS):
            client.indices.put_alias(index=INDEX, name=ALIAS)
            print(f"Alias {ALIAS} -> {INDEX}")
    finally:
        client.close()
spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} (origin_work_id BIGINT, target_work_id BIGINT, relationship STRING)")

# COMMAND ----------

# This run's changes: links to index, links to delete.
spark.sql(f"""
CREATE OR REPLACE TABLE {DIFF_TABLE} AS
SELECT {KEY}, 'index' AS action FROM (
  SELECT {KEY} FROM {LINKS_TABLE} WHERE {IN_SLICE}
  EXCEPT SELECT {KEY} FROM {STATE_TABLE} WHERE {IN_SLICE})
UNION ALL
SELECT {KEY}, 'delete' AS action FROM (
  SELECT {KEY} FROM {STATE_TABLE} WHERE {IN_SLICE}
  EXCEPT SELECT {KEY} FROM {LINKS_TABLE} WHERE {IN_SLICE})
""")
counts = {r.action: r.n for r in spark.sql(f"SELECT action, count(*) AS n FROM {DIFF_TABLE} GROUP BY action").collect()}
n_diff = sum(counts.values())
n_push = min(n_diff, CAP) if CAP else n_diff
k = max(1, -(-n_diff // CHUNK_ROWS))
n_chunks = max(1, -(-n_push // CHUNK_ROWS)) if n_push else 0
print(f"Changes: {counts}; this run sends {n_push:,} in {n_chunks} of {k} chunks")

# COMMAND ----------

def data_p95(client):
    n = client.nodes.stats(metric="os", filter_path="nodes.*.os.cpu.percent,nodes.*.roles")["nodes"].values()
    cpu = sorted(x["os"]["cpu"]["percent"] for x in n if any(r.startswith("data") for r in x.get("roles", [])))
    return cpu[max(0, int(len(cpu) * .95) - 1)] if cpu else 0


MAX_FAILED_ROWS = 100_000


def send(idx, rows):
    client = es_client()
    ok = missing = paused_s = 0
    failed, recent, last_check = [], [], 0.0

    def actions():
        nonlocal paused_s, last_check, recent
        for r in rows:
            if idx > 0 and time.time() - last_check >= 60:
                try:
                    recent = (recent + [data_p95(client)])[-4:]
                except Exception:
                    pass
                while sum(c > HOT_P95 for c in recent) >= 3:   # hot in 3 of the last 4 minutes: wait
                    time.sleep(60); paused_s += 60
                    try:
                        recent = (recent + [data_p95(client)])[-4:]
                    except Exception:
                        recent = recent[-3:]
                last_check = time.time()
            doc_id = f"{r.origin_work_id}|{r.target_work_id}|{r.relationship}"
            if r.action == "delete":
                yield {"_op_type": "delete", "_index": INDEX, "_id": doc_id}
            else:
                yield {"_op_type": "index", "_index": INDEX, "_id": doc_id, "_source": {
                    "origin": f"https://openalex.org/W{r.origin_work_id}",
                    "target": f"https://openalex.org/W{r.target_work_id}",
                    "relationship": f"https://openalex.org/relationships/{r.relationship}"}}

    try:
        for success, info in helpers.streaming_bulk(client, actions(), chunk_size=BULK, raise_on_error=False,
                                                    raise_on_exception=False, max_retries=5, initial_backoff=2):
            if success:
                ok += 1
                continue
            op, item = next(iter(info.items()))
            if op == "delete" and item.get("status") == 404:
                missing += 1          # already gone: the state drops it as well
            else:
                o, t, rel = str(item.get("_id", "0|0|")).split("|", 2)
                failed.append((int(o), int(t), rel, str(item.get("error"))[:500]))
    finally:
        client.close()
    yield ("counts", ok, missing, len(failed), paused_s)
    for f in failed[:MAX_FAILED_ROWS]:
        yield ("failed",) + f


FAILED_SCHEMA = "origin_work_id BIGINT, target_work_id BIGINT, relationship STRING, error STRING"
spark.sql(f"CREATE OR REPLACE TABLE {FAILED_TABLE} ({FAILED_SCHEMA})")
tot = {"ok": 0, "missing": 0, "failed": 0}
stopped, too_many_rejected, t_send = None, False, time.time()
if DRY_RUN:
    stopped, n_chunks = "dry_run: nothing sent", 0
for h in range(n_chunks):
    now = datetime.now(CT)
    if STOP_DT and now >= STOP_DT:
        stopped = f"stop_at {STOP_DT:%a %H:%M} CT reached before chunk {h + 1}/{n_chunks}"
        break
    writers = min(WRITERS, DAY_WRITERS) if 7 <= now.hour < 19 else WRITERS
    t0 = time.time()
    chunk_sql = f"SELECT * FROM {DIFF_TABLE} WHERE pmod(xxhash64({KEY}), {k}) = {h}"
    out = spark.sql(chunk_sql).repartition(writers).rdd.mapPartitionsWithIndex(send).collect()
    c = [o for o in out if o[0] == "counts"]
    ok, missing, nfail, paused = (sum(o[j] for o in c) for j in (1, 2, 3, 4))
    sent = ok + missing + nfail
    fails = [o[1:] for o in out if o[0] == "failed"]
    if fails:
        spark.createDataFrame(fails, FAILED_SCHEMA).write.mode("append").saveAsTable(FAILED_TABLE)
    if nfail > max(1000, 0.001 * sent):
        too_many_rejected = True
        stopped = f"chunk {h + 1}: {nfail:,} of {sent:,} rejected (not checkpointed); see {FAILED_TABLE}"
        break
    # The state takes every link of this chunk the index accepted.
    accepted = f"SELECT p.* FROM ({chunk_sql}) p LEFT ANTI JOIN {FAILED_TABLE} f USING ({KEY})"
    spark.sql(f"""
        MERGE INTO {STATE_TABLE} AS s
        USING ({accepted}) AS p
        ON s.origin_work_id = p.origin_work_id AND s.target_work_id = p.target_work_id
           AND s.relationship = p.relationship
        WHEN MATCHED AND p.action = 'delete' THEN DELETE
        WHEN NOT MATCHED AND p.action = 'index' THEN INSERT ({KEY}) VALUES (p.origin_work_id, p.target_work_id, p.relationship)
    """)
    tot["ok"] += ok; tot["missing"] += missing; tot["failed"] += nfail
    done = sum(tot.values())
    rate = done / max(time.time() - t_send, 1)
    print(f"{datetime.now(CT):%H:%M} CT chunk {h + 1}/{n_chunks}: {sent:,} in {(time.time() - t0) / 60:.1f} min on "
          f"{writers} writers (ok {ok:,}, missing {missing:,}, rejected {nfail:,}, paused {paused}s); total {done:,}/"
          f"{n_push:,}, {rate:,.0f} docs/s, ETA {(n_push - done) / max(rate, 1) / 60:.0f} min", flush=True)
if stopped:
    print(stopped)

# COMMAND ----------

print(f"Run: changes {counts}, sent {tot}, {(time.time() - t_start) / 60:.1f} min")
if too_many_rejected:
    raise Exception(stopped)
dbutils.notebook.exit(f"changes {counts}, sent {tot}, stopped {stopped}")
