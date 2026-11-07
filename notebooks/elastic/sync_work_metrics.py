# Databricks notebook source
# MAGIC %md
# MAGIC # Sync work metrics to Elasticsearch (oxjob #2082)
# MAGIC
# MAGIC `CreateWorksEnriched` recomputes `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` for every
# MAGIC work each night, but they are left out of the content hash (#679), so `updated_date` does not move and
# MAGIC `sync_works` never re-sends them. Without this notebook the works index keeps the values of the night each work was
# MAGIC last re-stamped or the index rebuilt, while the snapshot has today's (7 Nov 2026: 10.8% of works off by > 1% one
# MAGIC night after a rebuild).
# MAGIC
# MAGIC This notebook sends only those three fields, as partial `_update`s, for works whose served value is off by more than:
# MAGIC fwci 1% (relative), `citation_normalized_percentile.value` 0.01, `cited_by_percentile_year` min or max 2 points, or
# MAGIC whose null-ness or top-1/10% flag differs. `updated_date` is never touched: 0 re-stamps.
# MAGIC
# MAGIC **What the index holds** is kept in `openalex.works.es_work_metrics_served` (one row per work: the values and the
# MAGIC `updated_date` the index has). Each run:
# MAGIC 1. Works whose `updated_date` moved (sync_works re-sent them whole), new works, gone works: the state follows Delta.
# MAGIC 2. The rest are compared with Delta; those past the lines form the push (non-xpac first, largest move first, at
# MAGIC    most `max_push`; the rest wait for the next run).
# MAGIC 3. Partial updates to the `works` alias in chunks of `chunk_rows`; after each chunk the state takes the new values
# MAGIC    for every work the index accepted, so a stop or cancel loses at most one chunk (re-sent next run).
# MAGIC
# MAGIC **Load on search (the E2E shepherd's rules, #1642):** US daytime (07:00-19:00 CT) at most 2 writers; every writer but
# MAGIC the first pauses while data-node p95 CPU is over 85% in 3 of the last 4 minutes; `stop_at` (CT, HH:MM) ends the run
# MAGIC cleanly at the next chunk boundary.
# MAGIC
# MAGIC **After an index rebuild** from `openalex_works` version V, run once with `reseed_version=V` (the state becomes V,
# MAGIC then step 1 catches the works re-stamped since).
# MAGIC
# MAGIC **Revert:** remove the task from the End 2 End (or don't run this notebook). Nothing else depends on it; the next
# MAGIC rebuild serves Delta's values anyway. The pushed values are Delta's own, so there is nothing to roll back.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0
# MAGIC %restart_python

# COMMAND ----------

import math
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from elasticsearch import Elasticsearch, helpers

dbutils.widgets.dropdown("mode", "nightly", ["nightly", "dry_run"])
dbutils.widgets.text("max_push", "10000000")       # cap per run; the rest wait (non-xpac first, largest first)
dbutils.widgets.text("limit", "")                  # trial: push at most this many (overrides max_push when smaller)
dbutils.widgets.text("writers", "3")               # concurrent bulk writers at night; daytime is capped at 2
dbutils.widgets.text("chunk_rows", "5000000")      # works per checkpointed chunk
dbutils.widgets.text("bulk_size", "1000")          # update actions per bulk request
dbutils.widgets.text("stop_at", "")                # CT HH:MM: start no chunk at or after this time
dbutils.widgets.text("reseed_version", "")         # after a rebuild: openalex_works version the new index loaded

MODE = dbutils.widgets.get("mode")
DRY_RUN = MODE == "dry_run"
MAX_PUSH = int(dbutils.widgets.get("max_push") or 0) or None
LIMIT = int(dbutils.widgets.get("limit") or 0) or None
CAP = min(x for x in (MAX_PUSH, LIMIT) if x) if (MAX_PUSH or LIMIT) else None
WRITERS = max(1, int(dbutils.widgets.get("writers") or 3))
CHUNK_ROWS = int(dbutils.widgets.get("chunk_rows") or 5_000_000)
BULK = int(dbutils.widgets.get("bulk_size") or 1000)
STOP_AT = dbutils.widgets.get("stop_at").strip()
RESEED_VERSION = dbutils.widgets.get("reseed_version").strip()

WORKS_TABLE = "openalex.works.openalex_works"
SERVED_TABLE = "openalex.works.es_work_metrics_served"
PUSH_TABLE = "openalex.works.es_work_metrics_push"       # this run's push (overwritten each run)
FAILED_TABLE = "openalex.works.es_work_metrics_failed"   # rejected ids, appended per chunk, cleared each run
RUNS_TABLE = "openalex.works.es_work_metrics_runs"       # one row per chunk and one per run
ELASTIC_INDEX = "works"
ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

FWCI_REL, CNP_ABS = 0.01, 0.01
PCT_POINTS = 1.5  # min or max moved 2+ points (integers: a 1-point move scores 0.67, 2 points 1.33)
HOT_P95, DAY_WRITERS = 85, 2
CT = ZoneInfo("America/Chicago")
started_at = datetime.utcnow()
t_start = time.time()
print(f"mode={MODE} cap={CAP} writers={WRITERS} chunk_rows={CHUNK_ROWS:,} stop_at={STOP_AT or '-'} "
      f"reseed_version={RESEED_VERSION or '-'}")

# COMMAND ----------

# Reseed after a rebuild: the new index holds version V's values for every work.
if RESEED_VERSION:
    if not RESEED_VERSION.isdigit():
        raise ValueError(f"reseed_version must be an openalex_works version number, got {RESEED_VERSION!r}")
    if DRY_RUN:
        raise ValueError("reseed_version writes the state table; run it with mode=nightly")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SERVED_TABLE} CLUSTER BY (id) AS
        SELECT id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year,
               current_timestamp() AS served_at, 'rebuild (openalex_works v{RESEED_VERSION})' AS served_by
        FROM {WORKS_TABLE} VERSION AS OF {RESEED_VERSION}
    """)
    print(f"Reseeded {SERVED_TABLE} from {WORKS_TABLE} v{RESEED_VERSION}")

# COMMAND ----------

# Step 1. The index follows Delta for works sync_works re-sent (updated_date moved), new works and gone works.
# In the End 2 End this runs after Sync_to_Elasticsearch, so those works already carry tonight's values. Run before
# the sync, it only marks them early; the sync sends the same values minutes later.
if DRY_RUN:
    step1 = spark.sql(f"""
        SELECT count_if(s.id IS NULL) AS num_inserted_rows,
               count_if(s.id IS NOT NULL AND NOT (s.updated_date <=> c.updated_date)) AS num_updated_rows,
               CAST(NULL AS BIGINT) AS num_deleted_rows
        FROM {WORKS_TABLE} c LEFT JOIN {SERVED_TABLE} s ON s.id = c.id
    """).collect()[0].asDict()
    print(f"dry_run step 1 (not written): {step1}")
else:
    step1 = spark.sql(f"""
        MERGE INTO {SERVED_TABLE} AS s
        USING (SELECT id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year
               FROM {WORKS_TABLE}) AS c
        ON s.id = c.id
        WHEN MATCHED AND NOT (s.updated_date <=> c.updated_date) THEN UPDATE SET
          s.updated_date = c.updated_date, s.fwci = c.fwci,
          s.citation_normalized_percentile = c.citation_normalized_percentile,
          s.cited_by_percentile_year = c.cited_by_percentile_year, s.served_at = current_timestamp(),
          s.served_by = 'sync_works'
        WHEN NOT MATCHED THEN INSERT
          (id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year, served_at, served_by)
          VALUES (c.id, c.updated_date, c.fwci, c.citation_normalized_percentile, c.cited_by_percentile_year,
                  current_timestamp(), 'sync_works')
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """).collect()[0].asDict()
    print(f"Step 1: {step1}")

# COMMAND ----------

# Step 2. The push: works past the lines (score > 1 = past at least one line), banded by priority:
# band 0 null-ness / flag differences, 1 at 10x a line or more, 2 at 3x, 3 the rest; xpac works after all of those.
spark.sql(f"""
CREATE OR REPLACE TABLE {PUSH_TABLE} AS
WITH d AS (
  SELECT c.id, coalesce(c.is_xpac, false) AS is_xpac, c.fwci, c.citation_normalized_percentile,
    c.cited_by_percentile_year,
    IF((c.fwci IS NULL) <> (s.fwci IS NULL)
       OR (c.citation_normalized_percentile.value IS NULL) <> (s.citation_normalized_percentile.value IS NULL)
       OR (c.cited_by_percentile_year IS NULL) <> (s.cited_by_percentile_year IS NULL)
       OR NOT (c.citation_normalized_percentile.is_in_top_1_percent <=> s.citation_normalized_percentile.is_in_top_1_percent)
       OR NOT (c.citation_normalized_percentile.is_in_top_10_percent <=> s.citation_normalized_percentile.is_in_top_10_percent),
       1000.0,
       greatest(
         coalesce(abs(c.fwci - s.fwci) / greatest(abs(c.fwci), 0.0001) / {FWCI_REL}, 0),
         coalesce(abs(c.citation_normalized_percentile.value - s.citation_normalized_percentile.value) / {CNP_ABS}, 0),
         coalesce(greatest(abs(c.cited_by_percentile_year.min - s.cited_by_percentile_year.min),
                           abs(c.cited_by_percentile_year.max - s.cited_by_percentile_year.max)) / {PCT_POINTS}, 0))
    ) AS score
  FROM {WORKS_TABLE} c JOIN {SERVED_TABLE} s ON s.id = c.id AND s.updated_date <=> c.updated_date
)
SELECT *, IF(is_xpac, 10, 0) + CASE WHEN score >= 1000 THEN 0 WHEN score >= 10 THEN 1 WHEN score >= 3 THEN 2 ELSE 3 END
          AS band
FROM d WHERE score > 1
""")
bands = spark.sql(f"SELECT band, count(*) AS n FROM {PUSH_TABLE} GROUP BY band ORDER BY band").collect()
past = sum(b.n for b in bands)
print(f"Past the lines: {past:,} works; by band: {[(b.band, b.n) for b in bands]}")

# Cap: whole bands first, then the largest moves of the band that crosses the cap.
if CAP and past > CAP:
    left, keep = CAP, []
    for b in bands:
        if left <= 0:
            break
        keep.append((b.band, min(b.n, left)))
        left -= b.n
    last_band, last_n = keep[-1]
    spark.sql(f"""
        CREATE OR REPLACE TABLE {PUSH_TABLE} AS
        SELECT * FROM {PUSH_TABLE} WHERE band < {last_band}
        UNION ALL
        SELECT * FROM (SELECT * FROM {PUSH_TABLE} WHERE band = {last_band} ORDER BY score DESC, id LIMIT {last_n})
    """)
    bands = spark.sql(f"SELECT band, count(*) AS n FROM {PUSH_TABLE} GROUP BY band ORDER BY band").collect()
    print(f"Capped at {CAP:,}; {past - CAP:,} wait for the next run")
n_push = sum(b.n for b in bands)
chunks = [(b.band, h, math.ceil(b.n / CHUNK_ROWS)) for b in bands for h in range(math.ceil(b.n / CHUNK_ROWS))]
print(f"Push: {n_push:,} works in {len(chunks)} chunks")

# COMMAND ----------

# Step 3. Partial updates, chunk by chunk. Only these three fields; never an upsert (a missing doc stays missing:
# sync_works owns docs). Writer 0 always runs; the others pause while ES data nodes are hot.
def es_client():
    return Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True,
                         http_compress=True)


def data_p95(client):
    n = client.nodes.stats(metric="os", filter_path="nodes.*.os.cpu.percent,nodes.*.roles")["nodes"].values()
    cpu = sorted(x["os"]["cpu"]["percent"] for x in n if any(r.startswith("data") for r in x.get("roles", [])))
    return cpu[max(0, int(len(cpu) * .95) - 1)] if cpu else 0


def send(idx, rows):
    client = es_client()
    ok = missing = paused_s = 0
    failed, recent, last_check = [], [], 0.0

    def actions():
        nonlocal paused_s, last_check, recent
        for r in rows:
            if idx > 0 and time.time() - last_check >= 60:
                last_check = time.time()
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
            cnp, py = r.citation_normalized_percentile, r.cited_by_percentile_year
            yield {
                "_op_type": "update",
                "_index": ELASTIC_INDEX,
                "_id": f"https://openalex.org/W{r.id}",
                "retry_on_conflict": 3,
                "doc": {
                    "fwci": r.fwci,
                    "citation_normalized_percentile": None if cnp is None else cnp.asDict(),
                    "cited_by_percentile_year": None if py is None else py.asDict(),
                },
            }

    try:
        for success, info in helpers.streaming_bulk(client, actions(), chunk_size=BULK, raise_on_error=False,
                                                    raise_on_exception=False, max_retries=5, initial_backoff=2):
            if success:
                ok += 1
                continue
            item = info.get("update", {})
            wid = int(str(item.get("_id", "W-1")).rsplit("W", 1)[1])
            if item.get("status") == 404:
                missing += 1          # not in the index (sync_works' to fix); counted, state follows Delta
            else:
                failed.append((wid, str(item.get("error"))[:500]))
    finally:
        client.close()
    yield ("counts", ok, missing, f"{len(failed)}|{paused_s}")
    for wid, err in failed[:100000]:
        yield ("failed", wid, 0, err)


def now_ct():
    return datetime.now(CT)


spark.sql(f"CREATE OR REPLACE TABLE {FAILED_TABLE} (id BIGINT, error STRING, run_started_at TIMESTAMP)")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
  run_started_at TIMESTAMP, row_kind STRING, mode STRING, chunk STRING, writers INT, resent_or_new BIGINT, gone BIGINT,
  past_lines BIGINT, pushed BIGINT, ok BIGINT, missing BIGINT, failed BIGINT, paused_seconds BIGINT, seconds DOUBLE,
  at TIMESTAMP)""")
runs_schema = spark.table(RUNS_TABLE).schema
tot = {"ok": 0, "missing": 0, "failed": 0}
stopped, t_send = None, time.time()
for i, (band, h, k) in enumerate(chunks):
    if DRY_RUN:
        stopped = "dry_run: nothing sent"
        break
    if STOP_AT and now_ct().strftime("%H:%M") >= STOP_AT:
        stopped = f"stop_at {STOP_AT} CT reached before chunk {i + 1}/{len(chunks)}"
        break
    writers = min(WRITERS, DAY_WRITERS) if 7 <= now_ct().hour < 19 else WRITERS
    t0 = time.time()
    part = spark.sql(f"SELECT * FROM {PUSH_TABLE} WHERE band = {band} AND pmod(xxhash64(id), {k}) = {h}")
    out = part.repartition(writers).rdd.mapPartitionsWithIndex(send).collect()  # small: counts + rejected ids only
    c = [o for o in out if o[0] == "counts"]
    ok, missing = sum(o[1] for o in c), sum(o[2] for o in c)
    nfail = sum(int(o[3].split("|")[0]) for o in c)
    paused = sum(int(o[3].split("|")[1]) for o in c)
    fails = [(o[1], o[3], started_at) for o in out if o[0] == "failed"]
    if fails:
        spark.createDataFrame(fails, "id BIGINT, error STRING, run_started_at TIMESTAMP") \
            .write.mode("append").saveAsTable(FAILED_TABLE)
    # The state takes the pushed values for every work of this chunk the index accepted (or doesn't hold).
    spark.sql(f"""
        MERGE INTO {SERVED_TABLE} AS s
        USING (SELECT p.* FROM {PUSH_TABLE} p LEFT ANTI JOIN {FAILED_TABLE} f ON f.id = p.id
               WHERE p.band = {band} AND pmod(xxhash64(p.id), {k}) = {h}) AS p
        ON s.id = p.id
        WHEN MATCHED THEN UPDATE SET
          s.fwci = p.fwci, s.citation_normalized_percentile = p.citation_normalized_percentile,
          s.cited_by_percentile_year = p.cited_by_percentile_year, s.served_at = current_timestamp(),
          s.served_by = 'sync_work_metrics'
    """)
    secs = time.time() - t0
    for key, v in (("ok", ok), ("missing", missing), ("failed", nfail)):
        tot[key] += v
    done = tot["ok"] + tot["missing"] + tot["failed"]
    rate = done / max(time.time() - t_send, 1)
    print(f"{now_ct():%H:%M} CT chunk {i + 1}/{len(chunks)} band {band}: {ok + missing + nfail:,} docs in {secs / 60:.1f} min "
          f"on {writers} writers (ok {ok:,}, missing {missing:,}, rejected {nfail:,}, paused {paused}s); "
          f"total {done:,}/{n_push:,}, {rate:,.0f} docs/s, ETA {(n_push - done) / max(rate, 1) / 60:.0f} min", flush=True)
    spark.createDataFrame([(started_at, "chunk", MODE, f"{band}/{h}/{k}", writers, None, None, None,
                            ok + missing + nfail, ok, missing, nfail, paused, float(secs), datetime.utcnow())],
                          runs_schema).write.mode("append").saveAsTable(RUNS_TABLE)
    if nfail > max(1000, 0.001 * (ok + missing + nfail)):
        stopped = f"chunk {i + 1}: {nfail:,} updates rejected; see {FAILED_TABLE}"
        break
if stopped:
    print(stopped)

# COMMAND ----------

spark.createDataFrame([(started_at, "run", MODE, stopped, WRITERS,
                        int((step1.get("num_updated_rows") or 0) + (step1.get("num_inserted_rows") or 0)),
                        int(step1.get("num_deleted_rows") or 0), int(past), int(n_push),
                        tot["ok"], tot["missing"], tot["failed"], None, float(time.time() - t_start), datetime.utcnow())],
                      runs_schema).write.mode("append").saveAsTable(RUNS_TABLE)
print(f"Run: past the lines {past:,}, pushed {n_push:,}, sent {tot}, {(time.time() - t_start) / 60:.1f} min")
if stopped and "rejected" in stopped:
    raise Exception(stopped)   # the state skipped them, so the next run retries them
dbutils.notebook.exit(f"past {past}, pushed {n_push}, sent {tot}, stopped {stopped}")
