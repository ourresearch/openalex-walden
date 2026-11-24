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
# MAGIC whose null-ness or top-1/10% flag differs. `updated_date` is never touched: 0 re-stamps. The same lines are in
# MAGIC `notebooks/lakebase/BuildLakebaseWorkMetrics` (single-work lookups); change both together.
# MAGIC
# MAGIC **What the index holds** is kept in `openalex.works.es_work_metrics_served` (one row per work: the values and the
# MAGIC `updated_date` the index has). Each run compares it with Delta in one pass:
# MAGIC 1. Works whose `updated_date` moved (sync_works re-sent them whole), new works, gone works: the state follows Delta.
# MAGIC 2. The rest past the lines form the push (non-xpac first; works past the fwci or percentile-year line before works
# MAGIC    past only the CNP line; largest move first; at most about `max_push`; the rest wait for the next run). FWCI goes
# MAGIC    first so a bulk CNP correction (#1271, 10 Nov 2026: ~216M values, up to 0.95 each) drains behind the daily
# MAGIC    fwci drift instead of crowding it out of the cap for weeks. The cap never sorts the rows (#1271: a
# MAGIC    sort-and-limit over those values lost the Lakebase twin's executor): it counts rows per band and score bucket
# MAGIC    and keeps whole buckets in order, plus a hash slice of the bucket that crosses the cap.
# MAGIC 3. Partial updates to the `works` alias in chunks of `chunk_rows`; after each chunk the state takes the new values
# MAGIC    for every work the index accepted, so a stop or cancel loses at most one chunk (re-sent next run).
# MAGIC
# MAGIC **Load on search (the E2E shepherd's rules, #1642):** a "writer" here is one bulk stream (one Spark partition).
# MAGIC 07:00-19:00 CT the run uses at most `day_writers` streams (7 Nov at 26 nodes: 8 streams ~2-4K docs/s; 12 took the
# MAGIC API probe p50 to 0.9 s). Every stream but the first pauses while data-node p95 CPU is over 85% in 3 of the last 4
# MAGIC minutes; `stop_at` (CT, HH:MM) ends the run cleanly at the next chunk boundary. A partial update costs a get plus a
# MAGIC full re-index of the ~16 KB doc.
# MAGIC
# MAGIC **After an index rebuild** from `openalex_works` version V, run once with `reseed_version=V` (the state becomes V,
# MAGIC then step 1 catches the works re-stamped since). `docs/ES_INDEX_REBUILD.md` step 10 says so too.
# MAGIC
# MAGIC `mode=dry_run` counts what a run would do, writing only `*_dry` scratch tables (safe beside a live run).
# MAGIC
# MAGIC **Revert:** remove the task from the End 2 End (or don't run this notebook). Nothing else depends on it; the next
# MAGIC rebuild serves Delta's values anyway. The pushed values are Delta's own, so there is nothing to roll back.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0
# MAGIC %restart_python

# COMMAND ----------

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from elasticsearch import Elasticsearch, helpers

dbutils.widgets.dropdown("mode", "nightly", ["nightly", "dry_run"])
dbutils.widgets.text("max_push", "10000000")       # cap per run; the rest wait (non-xpac first, largest first)
dbutils.widgets.text("writers", "24")              # concurrent bulk streams (Spark partitions) at night
dbutils.widgets.text("day_writers", "6")           # cap 07:00-19:00 CT
dbutils.widgets.text("chunk_rows", "5000000")      # works per checkpointed chunk
dbutils.widgets.text("bulk_size", "1000")          # update actions per bulk request
dbutils.widgets.text("stop_at", "")                # CT HH:MM: start no chunk at or after this time
dbutils.widgets.text("reseed_version", "")         # after a rebuild: openalex_works version the new index loaded

MODE = dbutils.widgets.get("mode")
DRY_RUN = MODE == "dry_run"
CAP = int(dbutils.widgets.get("max_push") or 0) or None
WRITERS = max(1, int(dbutils.widgets.get("writers") or 24))
DAY_WRITERS = max(1, int(dbutils.widgets.get("day_writers") or 6))
CHUNK_ROWS = int(dbutils.widgets.get("chunk_rows") or 5_000_000)
BULK = int(dbutils.widgets.get("bulk_size") or 1000)
STOP_AT = dbutils.widgets.get("stop_at").strip()
RESEED_VERSION = dbutils.widgets.get("reseed_version").strip()

WORKS_TABLE = "openalex.works.openalex_works"
SERVED_TABLE = "openalex.works.es_work_metrics_served"
_scratch = "_dry" if DRY_RUN else ""
DIFF_TABLE = f"openalex.works.es_work_metrics_diff{_scratch}"      # this run's comparison (overwritten each run)
PUSH_TABLE = f"openalex.works.es_work_metrics_push{_scratch}"      # this run's push (overwritten each run)
FAILED_TABLE = f"openalex.works.es_work_metrics_failed{_scratch}"  # rejected ids, appended per chunk, cleared each run
FAILED_SCHEMA = "id BIGINT, error STRING, run_started_at TIMESTAMP"
RUNS_TABLE = "openalex.works.es_work_metrics_runs"                # one row per chunk and one per run
ELASTIC_INDEX = "works"
ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

FWCI_REL, CNP_ABS = 0.01, 0.01
PCT_POINTS = 1.5  # min or max moved 2+ points (integers: a 1-point move scores 0.67, 2 points 1.33)
HOT_P95 = 85
CT = ZoneInfo("America/Chicago")
started_at = datetime.utcnow()
t_start = time.time()
print(f"mode={MODE} cap={CAP} writers={WRITERS} day_writers={DAY_WRITERS} chunk_rows={CHUNK_ROWS:,} "
      f"stop_at={STOP_AT or '-'} reseed_version={RESEED_VERSION or '-'}")

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

# One comparison of Delta with the state (one snapshot of each, one full outer join):
#   gone / new / resent: the state follows Delta (sync_works re-sent resent works whole; in the End 2 End this runs after
#     Sync_to_Elasticsearch, and run before it only marks them early: the sync sends the same values minutes later);
#   push: same updated_date, past a line (score > 1 = past at least one line); band 0 null-ness / flag differences,
#     then by the fwci / percentile-year move (fp): 1 at 10x a line or more, 2 at 3x, 3 past; 4 past only the CNP
#     line (#2082, 10 Nov 2026: keeps a bulk CNP drain behind the daily fwci drift); xpac works after all of those.
spark.sql(f"""
CREATE OR REPLACE TABLE {DIFF_TABLE} AS
WITH j AS (
  SELECT coalesce(c.id, s.id) AS id, c.id IS NOT NULL AS in_works, s.id IS NOT NULL AS in_state,
    c.updated_date, coalesce(c.is_xpac, false) AS is_xpac, c.fwci, c.citation_normalized_percentile,
    c.cited_by_percentile_year,
    NOT (s.updated_date <=> c.updated_date) AS ud_moved,
    (c.fwci IS NULL) <> (s.fwci IS NULL)
       OR (c.citation_normalized_percentile.value IS NULL) <> (s.citation_normalized_percentile.value IS NULL)
       OR (c.cited_by_percentile_year IS NULL) <> (s.cited_by_percentile_year IS NULL)
       OR NOT (c.citation_normalized_percentile.is_in_top_1_percent <=> s.citation_normalized_percentile.is_in_top_1_percent)
       OR NOT (c.citation_normalized_percentile.is_in_top_10_percent <=> s.citation_normalized_percentile.is_in_top_10_percent)
       AS flagged,
    greatest(
      coalesce(abs(c.fwci - s.fwci) / greatest(abs(c.fwci), 0.0001) / {FWCI_REL}, 0),
      coalesce(greatest(abs(c.cited_by_percentile_year.min - s.cited_by_percentile_year.min),
                        abs(c.cited_by_percentile_year.max - s.cited_by_percentile_year.max)) / {PCT_POINTS}, 0)
    ) AS fp,
    coalesce(abs(c.citation_normalized_percentile.value - s.citation_normalized_percentile.value) / {CNP_ABS}, 0) AS cnp
  FROM (SELECT id, updated_date, is_xpac, fwci, citation_normalized_percentile, cited_by_percentile_year
        FROM {WORKS_TABLE}) c
  FULL OUTER JOIN {SERVED_TABLE} s ON s.id = c.id
), k AS (SELECT *, IF(flagged, 1000.0, greatest(fp, cnp)) AS score FROM j)
SELECT *, CASE WHEN NOT in_works THEN 'gone' WHEN NOT in_state THEN 'new' WHEN ud_moved THEN 'resent' ELSE 'push' END
          AS action,
       IF(is_xpac, 10, 0) + CASE WHEN score >= 1000 THEN 0 WHEN fp >= 10 THEN 1 WHEN fp >= 3 THEN 2 WHEN fp > 1 THEN 3
                                 ELSE 4 END
          AS band
FROM k
WHERE NOT in_works OR NOT in_state OR ud_moved OR score > 1
""")
counts = {r.action: r.n for r in spark.sql(f"SELECT action, count(*) AS n FROM {DIFF_TABLE} GROUP BY action").collect()}
print(f"Compared: {counts}")

if not DRY_RUN:
    spark.sql(f"""
        MERGE INTO {SERVED_TABLE} AS s
        USING (SELECT * FROM {DIFF_TABLE} WHERE action IN ('gone', 'new', 'resent')) AS d
        ON s.id = d.id
        WHEN MATCHED AND d.action = 'gone' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          s.updated_date = d.updated_date, s.fwci = d.fwci,
          s.citation_normalized_percentile = d.citation_normalized_percentile,
          s.cited_by_percentile_year = d.cited_by_percentile_year, s.served_at = current_timestamp(),
          s.served_by = 'sync_works'
        WHEN NOT MATCHED AND d.action = 'new' THEN INSERT
          (id, updated_date, fwci, citation_normalized_percentile, cited_by_percentile_year, served_at, served_by)
          VALUES (d.id, d.updated_date, d.fwci, d.citation_normalized_percentile, d.cited_by_percentile_year,
                  current_timestamp(), 'sync_works')
    """)

# COMMAND ----------

# The push, capped: lower bands whole (fwci / percentile-year moves before CNP-only ones), then the largest moves of the
# band that crosses the cap. No sort of the rows
# (#1271): keys order band, then score in 1/32-doubling buckets, largest first (same buckets as
# notebooks/lakebase/BuildLakebaseWorkMetrics).
PUSH_KEY = "band * 10000 + 5000 - CAST(floor(log2(greatest(score, 1)) * 32) AS INT)"


def cap_filter(rows_sql, key_sql, cap):
    """A predicate keeping about `cap` rows of `rows_sql`, lowest key first: one GROUP BY on the driver, no sort of the
    rows. Whole keys below the one that crosses the cap, then a hash slice of that key (+- a few thousand rows)."""
    taken = 0
    for r in spark.sql(f"SELECT {key_sql} AS k, count(*) AS n FROM ({rows_sql}) GROUP BY 1 ORDER BY 1").collect():
        if taken + r.n > cap:
            ppm = (cap - taken) * 1_000_000 // r.n
            return f"({key_sql} < {r.k} OR ({key_sql} = {r.k} AND pmod(xxhash64(id), 1000000) < {ppm}))"
        taken += r.n
    return "true"


past = counts.get("push", 0)
push_rows = f"SELECT * FROM {DIFF_TABLE} WHERE action = 'push'"
keep = cap_filter(push_rows, PUSH_KEY, CAP) if CAP and past > CAP else "true"
spark.sql(f"""
    CREATE OR REPLACE TABLE {PUSH_TABLE} AS
    SELECT id, is_xpac, fwci, citation_normalized_percentile, cited_by_percentile_year, score, band
    FROM ({push_rows}) WHERE {keep}
""")
if keep != "true":
    print(f"Capped at about {CAP:,} ({keep}); the rest wait for the next run")
bands = spark.sql(f"SELECT band, count(*) AS n FROM {PUSH_TABLE} GROUP BY band ORDER BY band").collect()
n_push = sum(b.n for b in bands)
chunks = []
for b in bands:
    k = -(-b.n // CHUNK_ROWS)
    chunks += [(b.band, h, k) for h in range(k)]
print(f"Past the lines: {past:,}; push {n_push:,} in {len(chunks)} chunks; by band {[(b.band, b.n) for b in bands]}")

# COMMAND ----------

# Partial updates, chunk by chunk. Only these three fields; never an upsert (a missing doc stays missing:
# sync_works owns docs). Stream 0 always runs; the others pause while ES data nodes are hot.
def es_client():
    return Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True,
                         http_compress=True)


def data_p95(client):
    n = client.nodes.stats(metric="os", filter_path="nodes.*.os.cpu.percent,nodes.*.roles")["nodes"].values()
    cpu = sorted(x["os"]["cpu"]["percent"] for x in n if any(r.startswith("data") for r in x.get("roles", [])))
    return cpu[max(0, int(len(cpu) * .95) - 1)] if cpu else 0


MAX_FAILED_ROWS = 100_000  # rejected ids returned per stream; a chunk past the rejection line is never checkpointed


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
    yield ("counts", ok, missing, len(failed), paused_s)
    for wid, err in failed[:MAX_FAILED_ROWS]:
        yield ("failed", wid, err)


def now_ct():
    return datetime.now(CT)


spark.sql(f"CREATE OR REPLACE TABLE {FAILED_TABLE} ({FAILED_SCHEMA})")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
  run_started_at TIMESTAMP, row_kind STRING, mode STRING, chunk STRING, writers INT, resent_or_new BIGINT, gone BIGINT,
  past_lines BIGINT, pushed BIGINT, ok BIGINT, missing BIGINT, failed BIGINT, paused_seconds BIGINT, seconds DOUBLE,
  at TIMESTAMP)""")
runs_schema = spark.table(RUNS_TABLE).schema


def log_row(**cols):
    cols = {"run_started_at": started_at, "mode": MODE, "at": datetime.utcnow(), **cols}
    spark.createDataFrame([tuple(cols.get(f.name) for f in runs_schema)], runs_schema) \
        .write.mode("append").saveAsTable(RUNS_TABLE)


tot = {"ok": 0, "missing": 0, "failed": 0}
stopped, too_many_rejected, t_send = None, False, time.time()
if DRY_RUN and chunks:
    stopped = "dry_run: nothing sent"
    chunks = []
for i, (band, h, k) in enumerate(chunks):
    if STOP_AT and now_ct().strftime("%H:%M") >= STOP_AT:
        stopped = f"stop_at {STOP_AT} CT reached before chunk {i + 1}/{len(chunks)}"
        break
    writers = min(WRITERS, DAY_WRITERS) if 7 <= now_ct().hour < 19 else WRITERS
    t0 = time.time()
    chunk_sql = f"SELECT * FROM {PUSH_TABLE} WHERE band = {band} AND pmod(xxhash64(id), {k}) = {h}"
    out = spark.sql(chunk_sql).repartition(writers).rdd.mapPartitionsWithIndex(send).collect()  # counts + rejected ids
    c = [o for o in out if o[0] == "counts"]
    ok, missing, nfail, paused = (sum(o[j] for o in c) for j in (1, 2, 3, 4))
    sent = ok + missing + nfail
    if nfail > max(1000, 0.001 * sent):
        # Not checkpointed: the whole chunk stays unsent in the state and is re-sent by the next run.
        too_many_rejected = True
        stopped = f"chunk {i + 1}: {nfail:,} of {sent:,} updates rejected (chunk not checkpointed); see {FAILED_TABLE}"
    fails = [(o[1], o[2], started_at) for o in out if o[0] == "failed"]
    if fails:
        spark.createDataFrame(fails, FAILED_SCHEMA).write.mode("append").saveAsTable(FAILED_TABLE)
    if too_many_rejected:
        break
    # The state takes the pushed values for every work of this chunk the index accepted (or doesn't hold).
    spark.sql(f"""
        MERGE INTO {SERVED_TABLE} AS s
        USING (SELECT p.* FROM ({chunk_sql}) p LEFT ANTI JOIN {FAILED_TABLE} f ON f.id = p.id) AS p
        ON s.id = p.id
        WHEN MATCHED THEN UPDATE SET
          s.fwci = p.fwci, s.citation_normalized_percentile = p.citation_normalized_percentile,
          s.cited_by_percentile_year = p.cited_by_percentile_year, s.served_at = current_timestamp(),
          s.served_by = 'sync_work_metrics'
    """)
    secs = time.time() - t0
    tot["ok"] += ok; tot["missing"] += missing; tot["failed"] += nfail
    done = sum(tot.values())
    rate = done / max(time.time() - t_send, 1)
    print(f"{now_ct():%H:%M} CT chunk {i + 1}/{len(chunks)} band {band}: {sent:,} docs in {secs / 60:.1f} min on {writers} "
          f"writers (ok {ok:,}, missing {missing:,}, rejected {nfail:,}, paused {paused}s); total {done:,}/{n_push:,}, "
          f"{rate:,.0f} docs/s, ETA {(n_push - done) / max(rate, 1) / 60:.0f} min", flush=True)
    log_row(row_kind="chunk", chunk=f"{band}/{h}/{k}", writers=writers, pushed=sent, ok=ok, missing=missing,
            failed=nfail, paused_seconds=paused, seconds=float(secs))
if stopped:
    print(stopped)

# COMMAND ----------

log_row(row_kind="run", chunk=stopped, writers=WRITERS,
        resent_or_new=int(counts.get("resent", 0) + counts.get("new", 0)), gone=int(counts.get("gone", 0)),
        past_lines=int(past), pushed=int(n_push), ok=tot["ok"], missing=tot["missing"], failed=tot["failed"],
        seconds=float(time.time() - t_start))
print(f"Run: past the lines {past:,}, pushed {n_push:,}, sent {tot}, {(time.time() - t_start) / 60:.1f} min")
if too_many_rejected:
    raise Exception(stopped)   # the chunk was not checkpointed, so the next run re-sends it
dbutils.notebook.exit(f"past {past}, pushed {n_push}, sent {tot}, stopped {stopped}")
