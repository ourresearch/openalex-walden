# Databricks notebook source
# MAGIC %md
# MAGIC # Authorship Daily Metrics (oxjob #640)
# MAGIC
# MAGIC Daily observation-only monitor for the author-matching pipeline. Reads
# MAGIC pipeline tables (never writes them) and persists three things:
# MAGIC
# MAGIC 1. **`openalex.monitoring.metrics`** — the shared monitoring sink (oxjob #1116):
# MAGIC    one row per `snapshot_date` x `metric` x `dimension`, under
# MAGIC    `component = 'author_matching'`, `source = 'AuthorshipDailyMetrics'`,
# MAGIC    delete-then-append per (date, component, source). History before
# MAGIC    2026-09-15 was copied in from the retired `authorship_daily_metrics`.
# MAGIC 2. **`openalex.authors.work_author_list_fingerprint`** — one compact row per
# MAGIC    work: author-list size, name-list hash, content hash (mirrors the
# MAGIC    oxjob 401-WB diff struct), seat counts from `work_authors`. Diffing
# MAGIC    live state against yesterday's fingerprint detects list growth,
# MAGIC    shrinkage, and name changes on ALL seats — including unbound seats the
# MAGIC    #608 guard cannot see.
# MAGIC 3. **`openalex.authors.work_author_change_events`** — append-only, one row
# MAGIC    per work whose author-list INPUT changed (GREW / SHRANK /
# MAGIC    NAMES_CHANGED / ORCIDS_CHANGED / METADATA_CHANGED), with
# MAGIC    `incompatible_positions` judging name changes via the #608
# MAGIC    `names_compatible` comparator (cosmetic drift vs different person).
# MAGIC    This is the observed workload for a future rematch-on-change trigger.
# MAGIC
# MAGIC **Since oxjob #1116 the run-outcome metrics (match outcomes, tiers, mints,
# MAGIC ORCID sanity, block skew, name concentration, the impossible-name check, the
# MAGIC assignment log, author counts) are written by the notebook that does the job:**
# MAGIC the last cells of `notebooks/end2end/MatchAuthors` into `openalex.monitoring.metrics`.
# MAGIC This observer keeps only what an observer can see: the fingerprint diff and its
# MAGIC change events, the reservoir/stale-seat state, name-change quality, the bound share
# MAGIC of seats on recently created works, and the end2end stage runtimes from the Jobs
# MAGIC API. Only metrics a check reads are emitted (keep it small).
# MAGIC
# MAGIC Stage wall time and SQL cost for the authorship tasks of the day's end2end
# MAGIC run come from the Jobs API and the warehouse query history (the job's
# MAGIC service principal owns both), so runtime regressions land in the same
# MAGIC tall table as the quality metrics.
# MAGIC
# MAGIC **Timing**: runs standalone at 22:30 UTC, after end2end (05:00 start) has
# MAGIC finished, so the run-state tables MatchAuthors/UpdateWorkAuthors
# MAGIC `CREATE OR REPLACE` each run (`pending_author_assignments`,
# MAGIC `author_matching_new_author_queue`, `work_authors_string_drift`) still hold
# MAGIC today's run and can be snapshotted before tomorrow overwrites them.
# MAGIC
# MAGIC **First run** bootstraps the fingerprint for every work (heavy, one-time:
# MAGIC full `work_authors` aggregate + hash of every base author list) and skips
# MAGIC change-event detection — there is no previous state to diff against.
# MAGIC
# MAGIC Metrics re-runs for a date are idempotent (delete-then-append). Change
# MAGIC events are append-only and never deleted by this notebook: a same-day
# MAGIC re-run only appends changes that happened after the first run (the
# MAGIC fingerprint has already advanced), so no duplicates arise.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.monitoring_metrics import METRICS_TABLE, emit  # noqa: E402

COMPONENT = "author_matching"
SOURCE = "AuthorshipDailyMetrics"

BASE_TABLE = "openalex.works.openalex_works_base"
SEATS_TABLE = "openalex.works.work_authors"

# Ephemeral run-state tables (CREATE OR REPLACE'd by each end2end run) — read-only here,
# used only to bound the fingerprint candidate set.
MATCH_BATCH_TABLE = "openalex.authors.author_matching_batch"
AFFIL_BATCH_TABLE = "openalex.authors.affiliation_update_batch"
DRIFT_TABLE = "openalex.authors.work_authors_string_drift"

E2E_JOB_NAME = "Walden End 2 End"
AUTHORSHIP_STAGES = ["Parsed_Author_Names", "Sync_Work_Author_Curations", "Author_Affiliations",
                     "Author_Matching", "Apply_Work_Author_Curations", "Authorships"]

# Monitor-owned tables (the only tables this notebook writes, plus the shared sink).
FINGERPRINT_TABLE = "openalex.authors.work_author_list_fingerprint"
EVENTS_TABLE = "openalex.authors.work_author_change_events"
CAND_TABLE = "openalex.authors.authorship_monitor_candidates"  # per-run scratch, kept for forensics

# COMMAND ----------

# Job parameters
dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = (
    datetime.strptime(_sd, "%Y-%m-%d").date()
    if _sd else datetime.now(timezone.utc).date()
)
print(f"RUN_DATE={RUN_DATE}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FINGERPRINT_TABLE} (
  work_id      BIGINT,
  base_n       INT,       -- SIZE(openalex_works_base.authorships), 0 if NULL/empty
  names_hash   BIGINT,    -- hash of ordered raw_author_name list
  content_hash BIGINT,    -- hash of ordered (name, orcid, is_corresponding, affils) — the 401-WB diff struct
  seat_n       INT,       -- rows in work_authors for this work
  null_seat_n  INT,       -- work_authors rows with author_id IS NULL
  eligible     BOOLEAN,   -- passes MatchAuthors gates (work_id > 7e9, created >= 2025-12-20)
  as_of        TIMESTAMP, -- when this row was last refreshed; MAX(as_of) is the run watermark
  orcids_hash  BIGINT     -- hash of ordered raw_orcid list; NULL = not yet computed for this row
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
  event_date      DATE,
  work_id         BIGINT,
  event_type      STRING,   -- GREW | SHRANK | NAMES_CHANGED | METADATA_CHANGED
  prev_base_n     INT,
  new_base_n      INT,
  names_changed   BOOLEAN,
  content_changed BOOLEAN,
  prev_seat_n     INT,
  new_seat_n      INT,
  prev_null_seat_n INT,
  new_null_seat_n  INT,
  detected_at     TIMESTAMP,
  orcids_changed  BOOLEAN,
  incompatible_positions INT  -- seats whose occupant name change fails names_compatible; NULL = not judged
)
""")

# Schema migration for tables created before these columns existed (idempotent).
for _tbl, _cols in [(FINGERPRINT_TABLE, {"orcids_hash": "BIGINT"}),
                    (EVENTS_TABLE, {"orcids_changed": "BOOLEAN",
                                    "incompatible_positions": "INT"})]:
    _have = {f.name for f in spark.table(_tbl).schema}
    _missing = [f"{c} {t}" for c, t in _cols.items() if c not in _have]
    if _missing:
        spark.sql(f"ALTER TABLE {_tbl} ADD COLUMNS ({', '.join(_missing)})")
        print(f"migrated {_tbl}: added {_missing}")

# COMMAND ----------

BOOTSTRAP = spark.table(FINGERPRINT_TABLE).isEmpty()

WATERMARK = "1900-01-01 00:00:00" if BOOTSTRAP else (
    spark.sql(f"SELECT MAX(as_of) AS wm FROM {FINGERPRINT_TABLE}").collect()[0]["wm"]
    .strftime("%Y-%m-%d %H:%M:%S")
)

ephemeral_present = {
    t: spark.catalog.tableExists(t)
    for t in [MATCH_BATCH_TABLE, AFFIL_BATCH_TABLE, DRIFT_TABLE]
}
print(f"BOOTSTRAP={BOOTSTRAP}  WATERMARK={WATERMARK}")
print(f"ephemeral tables present: {ephemeral_present}")

# COMMAND ----------

# One-time backfill: rows fingerprinted before orcids_hash existed. Hashes come
# from CURRENT base, so ORCID changes on backfill day itself go undetected —
# detection starts the following run. as_of untouched (watermark semantics).
if not BOOTSTRAP:
    _null_hashes = spark.sql(
        f"SELECT COUNT(*) AS c FROM {FINGERPRINT_TABLE} WHERE orcids_hash IS NULL"
    ).collect()[0]["c"]
    if _null_hashes > 100_000_000:
        print(f"backfilling orcids_hash for {_null_hashes:,} rows (one-time)")
        spark.sql(f"""
        MERGE INTO {FINGERPRINT_TABLE} t
        USING (
          SELECT id AS work_id,
                 xxhash64(to_json(TRANSFORM(
                   ARRAY_SORT(authorships, (l, r) ->
                     CASE WHEN l.author_order_number < r.author_order_number THEN -1
                          WHEN l.author_order_number > r.author_order_number THEN 1 ELSE 0 END),
                   a -> COALESCE(a.raw_orcid, '')))) AS orcids_hash
          FROM {BASE_TABLE}
        ) s
        ON t.work_id = s.work_id AND t.orcids_hash IS NULL
        WHEN MATCHED THEN UPDATE SET t.orcids_hash = s.orcids_hash
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Candidate set — works whose authorship state may have changed since the watermark
# MAGIC
# MAGIC Union of: base rows updated past the watermark, everything today's
# MAGIC UpdateWorkAuthors / MatchAuthors batches touched (covers oxjob 592 drift
# MAGIC works whose base `updated_date` never moves), and seats whose `updated_at`
# MAGIC moved (covers curation applies). On bootstrap: every work in base.

# COMMAND ----------

if BOOTSTRAP:
    cand_ids_sql = f"SELECT id AS work_id FROM {BASE_TABLE}"
else:
    parts = [
        f"SELECT id AS work_id FROM {BASE_TABLE} WHERE updated_date > TIMESTAMP'{WATERMARK}'",
        f"SELECT work_id FROM {SEATS_TABLE} WHERE updated_at > TIMESTAMP'{WATERMARK}'",
    ]
    for t in [AFFIL_BATCH_TABLE, MATCH_BATCH_TABLE, DRIFT_TABLE]:
        if ephemeral_present[t]:
            parts.append(f"SELECT work_id FROM {t}")
    cand_ids_sql = " UNION ALL ".join(f"({p})" for p in parts)

spark.sql(f"CREATE OR REPLACE TEMP VIEW _cand_ids AS SELECT DISTINCT work_id FROM ({cand_ids_sql})")

# Current authorship state for every candidate. The sort + struct mirror the
# former 401-WB change-detection compare (retired by the CreateWorkAuthorships
# full rebuild, oxjob #660); the fingerprint definition itself is unchanged.
spark.sql(f"""
CREATE OR REPLACE TABLE {CAND_TABLE} AS
WITH base_state AS (
  SELECT
    b.id AS work_id,
    COALESCE(SIZE(b.authorships), 0) AS base_n,
    ARRAY_SORT(b.authorships, (l, r) ->
      CASE WHEN l.author_order_number < r.author_order_number THEN -1
           WHEN l.author_order_number > r.author_order_number THEN 1 ELSE 0 END) AS sorted_auths,
    (b.id > 7000000000 AND b.created_date >= TIMESTAMP'2025-12-20') AS eligible
  FROM {BASE_TABLE} b
  WHERE b.id IN (SELECT work_id FROM _cand_ids)
),
seat_state AS (
  SELECT work_id,
         COUNT(*) AS seat_n,
         SUM(CASE WHEN author_id IS NULL THEN 1 ELSE 0 END) AS null_seat_n
  FROM {SEATS_TABLE}
  WHERE work_id IN (SELECT work_id FROM _cand_ids)
  GROUP BY work_id
)
SELECT
  bs.work_id,
  bs.base_n,
  xxhash64(to_json(TRANSFORM(bs.sorted_auths, a -> COALESCE(a.raw_author_name, '')))) AS names_hash,
  xxhash64(to_json(TRANSFORM(bs.sorted_auths, a -> STRUCT(
      a.raw_author_name AS n, a.raw_orcid AS o, a.is_corresponding AS c,
      ARRAY_SORT(a.raw_affiliation_strings) AS af)))) AS content_hash,
  xxhash64(to_json(TRANSFORM(bs.sorted_auths, a -> COALESCE(a.raw_orcid, '')))) AS orcids_hash,
  CAST(COALESCE(ss.seat_n, 0) AS INT) AS seat_n,
  CAST(COALESCE(ss.null_seat_n, 0) AS INT) AS null_seat_n,
  bs.eligible
FROM base_state bs
LEFT JOIN seat_state ss ON bs.work_id = ss.work_id
""")

n_candidates = spark.table(CAND_TABLE).count()
print(f"candidates: {n_candidates:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change events — diff candidates against the previous fingerprint
# MAGIC
# MAGIC Emitted BEFORE the fingerprint refresh. Only real input changes to works
# MAGIC we have previous state for; brand-new works are counted as a metric, not
# MAGIC logged as events. Seat-only changes (author_id fills from matching) are
# MAGIC routine and tracked in metrics, not events.

# COMMAND ----------

NEW_WORKS = 0
if not BOOTSTRAP:
    NEW_WORKS = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {CAND_TABLE} c
        LEFT ANTI JOIN {FINGERPRINT_TABLE} f ON c.work_id = f.work_id
    """).collect()[0]["c"]

    spark.sql(f"""
    INSERT INTO {EVENTS_TABLE}
      (event_date, work_id, event_type, prev_base_n, new_base_n,
       names_changed, content_changed, prev_seat_n, new_seat_n,
       prev_null_seat_n, new_null_seat_n, detected_at,
       orcids_changed, incompatible_positions)
    SELECT
      DATE'{RUN_DATE}' AS event_date,
      c.work_id,
      -- ORCIDS_CHANGED requires a non-NULL previous hash: rows fingerprinted
      -- before the orcids_hash column existed must not read as changes.
      CASE WHEN c.base_n > f.base_n THEN 'GREW'
           WHEN c.base_n < f.base_n THEN 'SHRANK'
           WHEN NOT (c.names_hash <=> f.names_hash) THEN 'NAMES_CHANGED'
           WHEN f.orcids_hash IS NOT NULL
                AND NOT (c.orcids_hash <=> f.orcids_hash) THEN 'ORCIDS_CHANGED'
           ELSE 'METADATA_CHANGED' END AS event_type,
      f.base_n AS prev_base_n,
      c.base_n AS new_base_n,
      NOT (c.names_hash <=> f.names_hash) AS names_changed,
      NOT (c.content_hash <=> f.content_hash) AS content_changed,
      f.seat_n AS prev_seat_n,
      c.seat_n AS new_seat_n,
      f.null_seat_n AS prev_null_seat_n,
      c.null_seat_n AS new_null_seat_n,
      current_timestamp() AS detected_at,
      (f.orcids_hash IS NOT NULL AND NOT (c.orcids_hash <=> f.orcids_hash)) AS orcids_changed,
      CAST(NULL AS INT) AS incompatible_positions
    FROM {CAND_TABLE} c
    JOIN {FINGERPRINT_TABLE} f ON c.work_id = f.work_id
    WHERE c.base_n <> f.base_n
       OR NOT (c.names_hash <=> f.names_hash)
       OR NOT (c.content_hash <=> f.content_hash)
    """)
    print(f"new works: {NEW_WORKS:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reservoir flow (pre-refresh) + name-change compatibility judging
# MAGIC
# MAGIC Flow must be read against the OLD fingerprint, so it runs before the MERGE.
# MAGIC The judge pass recovers yesterday's names via Delta time travel at the
# MAGIC watermark and scores each changed occupant with the #608 `names_compatible`
# MAGIC comparator — the split between cosmetic drift and wrong-person replacement.

# COMMAND ----------

FLOW = None
if not BOOTSTRAP:
    FLOW = spark.sql(f"""
        SELECT
          SUM(CASE WHEN f.work_id IS NOT NULL THEN GREATEST(f.null_seat_n - c.null_seat_n, 0) ELSE 0 END) AS filled,
          SUM(CASE WHEN f.work_id IS NOT NULL THEN GREATEST(c.null_seat_n - f.null_seat_n, 0) ELSE 0 END) AS added_existing,
          SUM(CASE WHEN f.work_id IS NULL THEN c.null_seat_n ELSE 0 END) AS added_new_works,
          SUM(CASE WHEN f.work_id IS NOT NULL AND c.eligible THEN GREATEST(f.null_seat_n - c.null_seat_n, 0) ELSE 0 END) AS eligible_filled,
          SUM(CASE WHEN f.work_id IS NOT NULL AND c.eligible THEN GREATEST(c.null_seat_n - f.null_seat_n, 0) ELSE 0 END) AS eligible_added_existing,
          SUM(CASE WHEN f.work_id IS NULL AND c.eligible THEN c.null_seat_n ELSE 0 END) AS eligible_added_new_works
        FROM {CAND_TABLE} c
        LEFT JOIN {FINGERPRINT_TABLE} f ON c.work_id = f.work_id
    """).collect()[0].asDict()

# COMMAND ----------

# Same ranges as the #608 guard batch (UpdateWorkAuthors); Java regex \uXXXX form.
CJK_RE = r'[\u1100-\u11FF\u3040-\u30FF\u3130-\u318F\u3400-\u4DBF\u4E00-\u9FFF\uAC00-\uD7AF\uF900-\uFAFF]'

if not BOOTSTRAP:
    try:
        spark.sql(f"""
        MERGE INTO {EVENTS_TABLE} t
        USING (
          WITH ev AS (
            SELECT work_id FROM {EVENTS_TABLE}
            WHERE event_date = DATE'{RUN_DATE}' AND names_changed
          ),
          before AS (
            SELECT work_id, author_sequence, raw_author_name
            FROM {SEATS_TABLE} TIMESTAMP AS OF '{WATERMARK}'
            WHERE work_id IN (SELECT work_id FROM ev)
          ),
          after AS (
            SELECT work_id, author_sequence, raw_author_name
            FROM {SEATS_TABLE}
            WHERE work_id IN (SELECT work_id FROM ev)
          ),
          changed AS (
            SELECT b.work_id, b.raw_author_name AS before_name, a.raw_author_name AS after_name
            FROM before b
            JOIN after a ON b.work_id = a.work_id AND b.author_sequence = a.author_sequence
            WHERE NOT (LOWER(TRIM(b.raw_author_name)) <=> LOWER(TRIM(a.raw_author_name)))
          ),
          judged AS (
            SELECT c.work_id,
              CASE
                WHEN c.before_name RLIKE '{CJK_RE}' OR c.after_name RLIKE '{CJK_RE}' THEN 'ABSTAIN_CJK'
                WHEN an_b.match_last IS NULL OR an_a.match_last IS NULL THEN 'ABSTAIN_UNPARSED'
                WHEN openalex.authors.names_compatible(
                       an_b.match_last, an_b.match_first,
                       an_a.match_last, an_a.match_first,
                       c.before_name, c.after_name) THEN 'COMPATIBLE'
                ELSE 'INCOMPATIBLE'
              END AS verdict
            FROM changed c
            LEFT JOIN openalex.authors.author_names an_b ON TRIM(c.before_name) = an_b.raw_author_name
            LEFT JOIN openalex.authors.author_names an_a ON TRIM(c.after_name) = an_a.raw_author_name
          )
          SELECT e.work_id,
                 CAST(COALESCE(j.incompat, 0) AS INT) AS incompat
          FROM ev e
          LEFT JOIN (
            SELECT work_id, COUNT(CASE WHEN verdict = 'INCOMPATIBLE' THEN 1 END) AS incompat
            FROM judged GROUP BY work_id
          ) j ON e.work_id = j.work_id
        ) s
        ON t.work_id = s.work_id AND t.event_date = DATE'{RUN_DATE}'
        WHEN MATCHED THEN UPDATE SET t.incompatible_positions = s.incompat
        """)
    except Exception as e:
        # Time travel can fail if work_authors history was vacuumed past the
        # watermark; events keep incompatible_positions = NULL for the day.
        print(f"name-compatibility judge pass skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh the fingerprint (upsert candidates)

# COMMAND ----------

spark.sql(f"""
MERGE INTO {FINGERPRINT_TABLE} t
USING {CAND_TABLE} s
ON t.work_id = s.work_id
WHEN MATCHED THEN UPDATE SET
  t.base_n = s.base_n, t.names_hash = s.names_hash, t.content_hash = s.content_hash,
  t.orcids_hash = s.orcids_hash,
  t.seat_n = s.seat_n, t.null_seat_n = s.null_seat_n, t.eligible = s.eligible,
  t.as_of = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (work_id, base_n, names_hash, content_hash, orcids_hash, seat_n, null_seat_n, eligible, as_of)
VALUES
  (s.work_id, s.base_n, s.names_hash, s.content_hash, s.orcids_hash, s.seat_n, s.null_seat_n,
   s.eligible, current_timestamp())
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics
# MAGIC
# MAGIC Collected as (metric, dimension, value) triples, then written
# MAGIC delete-then-append for `snapshot_date`.

# COMMAND ----------

rows = []  # (metric, dimension, value)

def add(metric, dimension, value):
    rows.append((metric, dimension, int(value or 0)))

def add_query(sql, metric, dim_col, val_col):
    for r in spark.sql(sql).collect():
        add(metric, str(r[dim_col]) if r[dim_col] is not None else "(null)", r[val_col])

# COMMAND ----------

# --- Author-list change events (from the events table, so same-day re-runs
# --- aggregate the full day) ------------------------------------------------
if not BOOTSTRAP:
    add("author_list_changes", "NEW_WORK", NEW_WORKS)
    add_query(f"""
        SELECT event_type AS d, COUNT(*) AS c FROM {EVENTS_TABLE}
        WHERE event_date = DATE'{RUN_DATE}' GROUP BY 1
    """, "author_list_changes", "d", "c")

    if FLOW is not None:
        for k in ["filled", "added_existing", "added_new_works",
                  "eligible_filled", "eligible_added_existing", "eligible_added_new_works"]:
            add("null_reservoir_flow", k, FLOW[k])

    # Name-change compatibility split (NULL incompatible_positions = judge pass
    # skipped, e.g. time travel unavailable — those days emit works_judged only).
    nq = spark.sql(f"""
        SELECT COUNT(CASE WHEN incompatible_positions IS NOT NULL THEN 1 END) AS works_judged,
               COUNT(CASE WHEN incompatible_positions > 0 THEN 1 END) AS works_incompatible,
               SUM(COALESCE(incompatible_positions, 0)) AS incompatible_positions
        FROM {EVENTS_TABLE}
        WHERE event_date = DATE'{RUN_DATE}' AND names_changed
    """).collect()[0]
    add("name_change_quality", "works_judged", nq["works_judged"])
    add("name_change_quality", "works_incompatible", nq["works_incompatible"])
    add("name_change_quality", "incompatible_positions", nq["incompatible_positions"])

# COMMAND ----------

# --- Fingerprint state (post-refresh): stale-seat debt + null reservoir ----
fp = spark.sql(f"""
    SELECT COUNT(*) AS works_tracked,
           SUM(CASE WHEN seat_n > base_n THEN seat_n - base_n ELSE 0 END) AS stale_seat_rows,
           COUNT(CASE WHEN seat_n > base_n THEN 1 END) AS works_with_stale_seats,
           COUNT(CASE WHEN seat_n < base_n THEN 1 END) AS works_missing_seats,
           SUM(null_seat_n) AS null_seats,
           SUM(CASE WHEN eligible THEN null_seat_n ELSE 0 END) AS null_seats_eligible,
           COUNT(CASE WHEN null_seat_n > 0 THEN 1 END) AS works_with_null_seats
    FROM {FINGERPRINT_TABLE}
""").collect()[0]
add("fingerprint_works_tracked", None, fp["works_tracked"])
add("stale_seats", "rows", fp["stale_seat_rows"])
add("stale_seats", "works", fp["works_with_stale_seats"])
add("stale_seats", "works_missing_seats", fp["works_missing_seats"])
add("null_reservoir", "seats", fp["null_seats"])
add("null_reservoir", "seats_match_eligible", fp["null_seats_eligible"])
add("null_reservoir", "works", fp["works_with_null_seats"])

# COMMAND ----------

# --- Stage runtime + SQL cost of the day's end2end run ----------------------
# Jobs API + warehouse query history, both owned by this job's service principal (the
# system.query / system.lakeflow tables are admin-only). Observation-only: never fails the run.
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import QueryFilter, TimeRange

    _w = WorkspaceClient()
    _day0 = int(datetime(RUN_DATE.year, RUN_DATE.month, RUN_DATE.day, tzinfo=timezone.utc).timestamp() * 1000)
    _jid = next(j.job_id for j in _w.jobs.list(name=E2E_JOB_NAME))
    _runs = list(_w.jobs.list_runs(job_id=_jid, start_time_from=_day0,
                                   start_time_to=_day0 + 86_400_000, expand_tasks=True))
    add("e2e_runs_on_date", None, len(_runs))

    def _ok(t):
        return bool(t and t.state and t.state.result_state and t.state.result_state.value == "SUCCESS")

    # The latest run of the day whose Author_Matching succeeded (repairs re-run subsets).
    _run = max((r for r in _runs
                if _ok({t.task_key: t for t in (r.tasks or [])}.get("Author_Matching"))),
               key=lambda r: r.start_time, default=None)
    if _run is not None:
        _tasks = {t.task_key: t for t in _run.tasks}
        for s in AUTHORSHIP_STAGES:
            t = _tasks.get(s)
            if t and t.start_time and t.end_time:
                add("stage_wall_sec", s, (t.end_time - t.start_time) // 1000)
                add("stage_success", s, 1 if _ok(t) else 0)

        am = _tasks["Author_Matching"]
        _f = QueryFilter(warehouse_ids=[am.notebook_task.warehouse_id],
                         query_start_time_range=TimeRange(start_time_ms=am.start_time, end_time_ms=am.end_time))
        # SDK >= ~0.30 returns a ListQueriesResponse page; older cluster-bundled SDKs return
        # an auto-paginating iterator of QueryInfo.
        _resp = _w.query_history.list(filter_by=_f, include_metrics=True, max_results=100)
        if hasattr(_resp, "res"):
            _qs = list(_resp.res or [])
            while _resp.has_next_page:
                _resp = _w.query_history.list(filter_by=_f, include_metrics=True, max_results=100,
                                              page_token=_resp.next_page_token)
                _qs += list(_resp.res or [])
        else:
            _qs = list(_resp)

        def _m(q, k):
            return int(getattr(q.metrics, k, 0) or 0) if q.metrics else 0

        add("author_matching_sql", "statements", len(_qs))
        add("author_matching_sql", "wall_sec", sum((q.duration or 0) for q in _qs) // 1000)
        add("author_matching_sql", "task_sec", sum(_m(q, "task_total_time_ms") for q in _qs) // 1000)
        add("author_matching_sql", "read_gb", sum(_m(q, "read_bytes") for q in _qs) // 10**9)
        add("author_matching_sql", "spill_gb", sum(_m(q, "spill_to_disk_bytes") for q in _qs) // 10**9)
        if _qs:
            _top = max(_qs, key=lambda q: q.duration or 0)
            add("author_matching_sql", "longest_statement_wall_sec", (_top.duration or 0) // 1000)
            add("author_matching_sql", "longest_statement_task_sec", _m(_top, "task_total_time_ms") // 1000)
            add("author_matching_sql", "longest_statement_spill_gb", _m(_top, "spill_to_disk_bytes") // 10**9)
except Exception as e:
    # surfaced in the tall table: notebook stdout is not reachable from the Jobs API
    add("collector_error", f"stage_runtime: {type(e).__name__}: {str(e)[:100]}", 1)
    print(f"stage runtime collection skipped: {e!r}")

# COMMAND ----------

# --- Bound share of seats on recently created works: the user-visible outcome of
# --- matching. Works created 8..1 days ago — today's creations may not have been
# --- through MatchAuthors yet. Seat counts come from the fingerprint (already refreshed).
nw = spark.sql(f"""
    SELECT COUNT(*) AS works,
           SUM(f.seat_n) AS total,
           SUM(f.seat_n - f.null_seat_n) AS bound
    FROM {FINGERPRINT_TABLE} f
    JOIN (SELECT id FROM {BASE_TABLE}
          WHERE created_date >= DATE'{RUN_DATE}' - INTERVAL 8 DAYS
            AND created_date <  DATE'{RUN_DATE}' - INTERVAL 1 DAY) b ON b.id = f.work_id
""").collect()[0]
add("new_works_seats", "works", nw["works"] or 0)
add("new_works_seats", "total", nw["total"] or 0)
add("new_works_seats", "bound", nw["bound"] or 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (idempotent per snapshot_date, component, source)

# COMMAND ----------

n = emit(spark, COMPONENT, rows, source=SOURCE, snapshot_date=RUN_DATE)
print(f"done: wrote {n} metric rows for {RUN_DATE} to {METRICS_TABLE} (bootstrap={BOOTSTRAP})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check — today's snapshot

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric, dimension, value
    FROM {METRICS_TABLE}
    WHERE snapshot_date = DATE'{RUN_DATE}' AND component = '{COMPONENT}' AND source = '{SOURCE}'
    ORDER BY metric, (dimension IS NOT NULL), value DESC
"""))
