# Databricks notebook source
# MAGIC %md
# MAGIC # Authorship Daily Metrics (oxjob #640)
# MAGIC
# MAGIC Daily observation-only monitor for the author-matching pipeline. Reads
# MAGIC pipeline tables (never writes them) and persists three things:
# MAGIC
# MAGIC 1. **`openalex.authors.authorship_daily_metrics`** — tall metrics table,
# MAGIC    same shape as `openalex.works.works_daily_metrics`: one row per
# MAGIC    `snapshot_date` x `metric` x `dimension`, delete-then-append per date.
# MAGIC 2. **`openalex.authors.work_author_list_fingerprint`** — one compact row per
# MAGIC    work: author-list size, name-list hash, content hash (mirrors the
# MAGIC    oxjob 401-WB diff struct), seat counts from `work_authors`. Diffing
# MAGIC    live state against yesterday's fingerprint detects list growth,
# MAGIC    shrinkage, and name changes on ALL seats — including unbound seats the
# MAGIC    #608 guard cannot see.
# MAGIC 3. **`openalex.authors.work_author_change_events`** — append-only, one row
# MAGIC    per work whose author-list INPUT changed (GREW / SHRANK /
# MAGIC    NAMES_CHANGED / METADATA_CHANGED). This is the observed workload for a
# MAGIC    future rematch-on-change trigger.
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

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType,
)

BASE_TABLE = "openalex.works.openalex_works_base"
SEATS_TABLE = "openalex.works.work_authors"
AUTHORSHIPS_TABLE = "openalex.works.work_authorships"
AUTHORS_TABLE = "openalex.authors.authors"
AFM_TABLE = "openalex.authors.authors_for_matching"

# Ephemeral run-state tables (CREATE OR REPLACE'd by each end2end run) — read-only here.
PENDING_TABLE = "openalex.authors.pending_author_assignments"
MATCH_BATCH_TABLE = "openalex.authors.author_matching_batch"
MINT_QUEUE_TABLE = "openalex.authors.author_matching_new_author_queue"
AFFIL_BATCH_TABLE = "openalex.authors.affiliation_update_batch"
DRIFT_TABLE = "openalex.authors.work_authors_string_drift"
GUARD_TELEMETRY_TABLE = "openalex.authors.author_guard_telemetry"

# Monitor-owned tables (the only tables this notebook writes).
DST_TABLE = "openalex.authors.authorship_daily_metrics"
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
CREATE TABLE IF NOT EXISTS {DST_TABLE} (
  snapshot_date    DATE,
  snapshot_version BIGINT,   -- Delta version of work_authors at compute time
  metric           STRING,
  dimension        STRING,
  value            BIGINT,
  computed_at      TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FINGERPRINT_TABLE} (
  work_id      BIGINT,
  base_n       INT,       -- SIZE(openalex_works_base.authorships), 0 if NULL/empty
  names_hash   BIGINT,    -- hash of ordered raw_author_name list
  content_hash BIGINT,    -- hash of ordered (name, orcid, is_corresponding, affils) — the 401-WB diff struct
  seat_n       INT,       -- rows in work_authors for this work
  null_seat_n  INT,       -- work_authors rows with author_id IS NULL
  eligible     BOOLEAN,   -- passes MatchAuthors gates (work_id > 7e9, created >= 2025-12-20)
  as_of        TIMESTAMP  -- when this row was last refreshed; MAX(as_of) is the run watermark
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
  detected_at     TIMESTAMP
)
""")

# COMMAND ----------

BOOTSTRAP = spark.table(FINGERPRINT_TABLE).isEmpty()

WATERMARK = "1900-01-01 00:00:00" if BOOTSTRAP else (
    spark.sql(f"SELECT MAX(as_of) AS wm FROM {FINGERPRINT_TABLE}").collect()[0]["wm"]
    .strftime("%Y-%m-%d %H:%M:%S")
)

ephemeral_present = {
    t: spark.catalog.tableExists(t)
    for t in [PENDING_TABLE, MATCH_BATCH_TABLE, MINT_QUEUE_TABLE,
              AFFIL_BATCH_TABLE, DRIFT_TABLE]
}
print(f"BOOTSTRAP={BOOTSTRAP}  WATERMARK={WATERMARK}")
print(f"ephemeral tables present: {ephemeral_present}")

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
# oxjob 401-WB change-detection compare in UpdateWorkAuthorships exactly, so
# content_hash changes if and only if that intake would consider the list changed.
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
    SELECT
      DATE'{RUN_DATE}' AS event_date,
      c.work_id,
      CASE WHEN c.base_n > f.base_n THEN 'GREW'
           WHEN c.base_n < f.base_n THEN 'SHRANK'
           WHEN NOT (c.names_hash <=> f.names_hash) THEN 'NAMES_CHANGED'
           ELSE 'METADATA_CHANGED' END AS event_type,
      f.base_n AS prev_base_n,
      c.base_n AS new_base_n,
      NOT (c.names_hash <=> f.names_hash) AS names_changed,
      NOT (c.content_hash <=> f.content_hash) AS content_changed,
      f.seat_n AS prev_seat_n,
      c.seat_n AS new_seat_n,
      f.null_seat_n AS prev_null_seat_n,
      c.null_seat_n AS new_null_seat_n,
      current_timestamp() AS detected_at
    FROM {CAND_TABLE} c
    JOIN {FINGERPRINT_TABLE} f ON c.work_id = f.work_id
    WHERE c.base_n <> f.base_n
       OR NOT (c.names_hash <=> f.names_hash)
       OR NOT (c.content_hash <=> f.content_hash)
    """)
    print(f"new works: {NEW_WORKS:,}")

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
  t.seat_n = s.seat_n, t.null_seat_n = s.null_seat_n, t.eligible = s.eligible,
  t.as_of = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (work_id, base_n, names_hash, content_hash, seat_n, null_seat_n, eligible, as_of)
VALUES
  (s.work_id, s.base_n, s.names_hash, s.content_hash, s.seat_n, s.null_seat_n, s.eligible,
   current_timestamp())
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

# --- MatchAuthors run-state (today's ephemeral tables) --------------------
for t, name in [(PENDING_TABLE, "pending_author_assignments"),
                (MATCH_BATCH_TABLE, "author_matching_batch"),
                (MINT_QUEUE_TABLE, "author_matching_new_author_queue"),
                (DRIFT_TABLE, "work_authors_string_drift")]:
    add("ephemeral_table_rows", name,
        spark.table(t).count() if ephemeral_present.get(t, spark.catalog.tableExists(t)) else -1)

if ephemeral_present[PENDING_TABLE]:
    add_query(f"SELECT match_outcome AS d, COUNT(*) AS c FROM {PENDING_TABLE} GROUP BY 1",
              "match_outcome", "d", "c")
    add_query(f"SELECT COALESCE(match_method, '(none)') AS d, COUNT(*) AS c FROM {PENDING_TABLE} GROUP BY 1",
              "match_method", "d", "c")

    # name_match_tier: which cascade tier fired (added to MatchAuthors 2026-07-20,
    # oxjob #640). Guarded so the monitor still runs against a pre-change batch.
    if "name_match_tier" in [f.name for f in spark.table(PENDING_TABLE).schema]:
        add_query(f"""
            SELECT CASE WHEN match_method = 'orcid' THEN 'orcid'
                        ELSE COALESCE(name_match_tier, '(none)') END AS d,
                   COUNT(*) AS c
            FROM {PENDING_TABLE} GROUP BY 1
        """, "match_tier", "d", "c")

    qa = spark.sql(f"""
        SELECT SUM(CASE WHEN orcid_name_conflict THEN 1 ELSE 0 END) AS name_conflict,
               SUM(CASE WHEN orcid_blind_match THEN 1 ELSE 0 END) AS blind_match,
               SUM(CASE WHEN orcid_match_count > 1 THEN 1 ELSE 0 END) AS splinter_orcid
        FROM {PENDING_TABLE}
    """).collect()[0]
    add("orcid_qa", "name_conflict", qa["name_conflict"])
    add("orcid_qa", "blind_match", qa["blind_match"])
    add("orcid_qa", "splinter_orcid", qa["splinter_orcid"])

    # Blocking health: block sizes are not persisted by the cascade, so
    # recompute them for today's block keys only.
    bs = spark.sql(f"""
        WITH block_sizes AS (
            SELECT block_key, COUNT(*) AS n
            FROM {AFM_TABLE}
            WHERE block_key IN (SELECT DISTINCT block_key FROM {PENDING_TABLE} WHERE block_key IS NOT NULL)
            GROUP BY block_key
        ),
        joined AS (
            SELECT p.match_outcome, COALESCE(b.n, 0) AS n
            FROM {PENDING_TABLE} p
            LEFT JOIN block_sizes b ON p.block_key = b.block_key
        )
        SELECT
          CAST(percentile_approx(n, 0.5) AS BIGINT) AS p50,
          CAST(percentile_approx(n, 0.95) AS BIGINT) AS p95,
          MAX(n) AS max_n
        FROM joined
    """).collect()[0]
    add("batch_block_size", "p50", bs["p50"])
    add("batch_block_size", "p95", bs["p95"])
    add("batch_block_size", "max", bs["max_n"])

    add_query(f"""
        WITH block_sizes AS (
            SELECT block_key, COUNT(*) AS n
            FROM {AFM_TABLE}
            WHERE block_key IN (SELECT DISTINCT block_key FROM {PENDING_TABLE} WHERE block_key IS NOT NULL)
            GROUP BY block_key
        )
        SELECT CASE WHEN COALESCE(b.n, 0) = 0 THEN '0'
                    WHEN b.n <= 10 THEN '1-10'
                    WHEN b.n <= 100 THEN '11-100'
                    WHEN b.n <= 1000 THEN '101-1000'
                    ELSE '1000+' END AS d,
               COUNT(*) AS c
        FROM {PENDING_TABLE} p
        LEFT JOIN block_sizes b ON p.block_key = b.block_key
        WHERE p.match_outcome = 'AMBIGUOUS'
        GROUP BY 1
    """, "ambiguous_by_block_size", "d", "c")

if ephemeral_present[MINT_QUEUE_TABLE]:
    mq = spark.sql(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN orcid IS NOT NULL THEN 1 ELSE 0 END) AS with_orcid
        FROM {MINT_QUEUE_TABLE}
    """).collect()[0]
    add("new_authors_minted", None, mq["total"])
    add("new_authors_minted", "with_orcid", mq["with_orcid"])

if ephemeral_present[DRIFT_TABLE]:
    add("string_drift_works", None,
        spark.sql(f"SELECT COUNT(DISTINCT work_id) AS c FROM {DRIFT_TABLE}").collect()[0]["c"])

# COMMAND ----------

# --- #608 guard telemetry (already persisted; roll up today's runs) --------
gt = spark.sql(f"""
    SELECT COUNT(*) AS runs,
           SUM(changed_name_positions) AS changed_name_positions,
           SUM(incompatible) AS incompatible,
           SUM(abstain_unparsed) AS abstain_unparsed,
           SUM(abstain_cjk) AS abstain_cjk,
           SUM(curated_holds) AS curated_holds,
           SUM(would_invalidate) AS would_invalidate,
           SUM(rebindable) AS rebindable,
           SUM(realign_tier) AS realign_tier,
           SUM(legacy_tier) AS legacy_tier,
           SUM(isolated_holds) AS isolated_holds
    FROM {GUARD_TELEMETRY_TABLE}
    WHERE DATE(run_at) = DATE'{RUN_DATE}'
""").collect()[0]
add("guard_runs_on_date", None, gt["runs"])
for k in ["changed_name_positions", "incompatible", "abstain_unparsed",
          "abstain_cjk", "curated_holds", "would_invalidate", "rebindable",
          "realign_tier", "legacy_tier", "isolated_holds"]:
    add("guard_telemetry", k, gt[k])

# COMMAND ----------

# --- Author-list change events (from the events table, so same-day re-runs
# --- aggregate the full day) ------------------------------------------------
if not BOOTSTRAP:
    add("author_list_changes", "NEW_WORK", NEW_WORKS)
    add_query(f"""
        SELECT event_type AS d, COUNT(*) AS c FROM {EVENTS_TABLE}
        WHERE event_date = DATE'{RUN_DATE}' GROUP BY 1
    """, "author_list_changes", "d", "c")

    ev = spark.sql(f"""
        SELECT SUM(GREATEST(new_base_n - prev_base_n, 0)) AS seats_added,
               SUM(GREATEST(prev_base_n - new_base_n, 0)) AS seats_removed
        FROM {EVENTS_TABLE} WHERE event_date = DATE'{RUN_DATE}'
    """).collect()[0]
    add("author_list_seats_added", None, ev["seats_added"])
    add("author_list_seats_removed", None, ev["seats_removed"])

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

# --- Durable-table daily activity ------------------------------------------
au = spark.sql(f"""
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN orcid IS NOT NULL THEN 1 ELSE 0 END) AS with_orcid,
           SUM(CASE WHEN DATE(created_date) = DATE'{RUN_DATE}' THEN 1 ELSE 0 END) AS created_on_date
    FROM {AUTHORS_TABLE}
""").collect()[0]
add("authors_total", None, au["total"])
add("authors_with_orcid", None, au["with_orcid"])
add("authors_created_on_date", None, au["created_on_date"])

wa = spark.sql(f"""
    SELECT SUM(CASE WHEN DATE(created_at) = DATE'{RUN_DATE}' THEN 1 ELSE 0 END) AS seats_created,
           SUM(CASE WHEN DATE(updated_at) = DATE'{RUN_DATE}' THEN 1 ELSE 0 END) AS seats_updated
    FROM {SEATS_TABLE}
""").collect()[0]
add("seats_created_on_date", None, wa["seats_created"])
add("seats_updated_on_date", None, wa["seats_updated"])

add("works_authorships_updated_on_date", None, spark.sql(f"""
    SELECT COUNT(*) AS c FROM {AUTHORSHIPS_TABLE}
    WHERE DATE(updated_datetime) = DATE'{RUN_DATE}'
""").collect()[0]["c"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (idempotent per snapshot_date)

# COMMAND ----------

seats_version = int(
    spark.sql(f"DESCRIBE HISTORY {SEATS_TABLE} LIMIT 1").collect()[0]["version"]
)

schema = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("snapshot_version", LongType(), True),
    StructField("metric", StringType(), False),
    StructField("dimension", StringType(), True),
    StructField("value", LongType(), False),
])

df = (spark.createDataFrame(
        [(RUN_DATE, seats_version, m, d, v) for m, d, v in rows], schema)
      .withColumn("computed_at", F.current_timestamp()))

spark.sql(f"DELETE FROM {DST_TABLE} WHERE snapshot_date = DATE'{RUN_DATE}'")
df.write.format("delta").mode("append").saveAsTable(DST_TABLE)

print(f"done: wrote {len(rows)} metric rows for {RUN_DATE} (bootstrap={BOOTSTRAP})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check — today's snapshot

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric, dimension, value
    FROM {DST_TABLE}
    WHERE snapshot_date = DATE'{RUN_DATE}'
    ORDER BY metric, (dimension IS NOT NULL), value DESC
"""))
