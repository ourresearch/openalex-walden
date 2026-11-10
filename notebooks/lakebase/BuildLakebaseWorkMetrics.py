# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase work metrics (oxjob #2082)
# MAGIC
# MAGIC Single-work lookups (`GET /works/W…`) read the stored doc from Lakebase (`lakebase_works_docs_*`), which is
# MAGIC rewritten only when a work is re-stamped. `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year`
# MAGIC move every night without a re-stamp (#679), so the doc keeps old values. Rewriting docs for them would bring back the
# MAGIC multi-hour refreshes #867 removed, so they live in one narrow row per work instead:
# MAGIC `openalex.works.lakebase_work_metrics` -> synced table `openalex.lakebase.lakebase_work_metrics`, which the API
# MAGIC joins onto the doc (openalex-elastic-api `works/lakebase.py`, `METRICS_OVERLAY`).
# MAGIC
# MAGIC Lines (same as `notebooks/elastic/sync_work_metrics`; change both together): a row is rewritten when fwci moves by
# MAGIC > 1%, CNP value by > 0.01, a percentile-year bound by 2+ points, or a null-ness / top-1/10% flag changes. Works new
# MAGIC to the table are inserted; gone works deleted. At most about `max_rows` rewrites a night (new, gone and flagged
# MAGIC works first, then the largest moves); the rest wait a night. The cap never sorts the rows (#1271, 10 Nov 2026: an
# MAGIC `ORDER BY score LIMIT 20M` over 225M rows put them on one executor and lost it): it counts rows per score bucket
# MAGIC and keeps whole buckets, largest moves first, plus a hash slice of the bucket that crosses the cap.
# MAGIC
# MAGIC - `mode=full_build`: (re)create the table from `openalex_works` (one-time; then create the synced table, TRIGGERED,
# MAGIC   primary key `work_id`, and `GRANT SELECT ON lakebase.lakebase_work_metrics TO openalex_api_ro`).
# MAGIC - `mode=nightly`: one MERGE of the rows to change, then trigger the synced table's refresh (`trigger_sync=true`).
# MAGIC - `mode=dry_run`: what nightly would change, capped, in `lakebase_work_metrics_changes_dry`; no MERGE, no refresh.
# MAGIC
# MAGIC **Revert:** set `METRICS_OVERLAY = False` in the API (or config var `LAKEBASE_METRICS_OVERLAY=0`, no deploy);
# MAGIC docs answer alone, as before. Then drop the synced table and this task.

# COMMAND ----------

# MAGIC %pip install -U -q databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "nightly", "full_build"])
dbutils.widgets.dropdown("trigger_sync", "false", ["true", "false"])
dbutils.widgets.text("max_rows", "20000000")   # rows rewritten a night at most (the synced-table refresh time)
MODE = dbutils.widgets.get("mode")
TRIGGER = dbutils.widgets.get("trigger_sync") == "true"
MAX_ROWS = int(dbutils.widgets.get("max_rows") or 0) or None

WORKS_TABLE = "openalex.works.openalex_works"
METRICS_TABLE = "openalex.works.lakebase_work_metrics"
SYNCED_TABLE = "openalex.lakebase.lakebase_work_metrics"
CHANGES_TABLE = "openalex.works.lakebase_work_metrics_changes"  # tonight's rewrites (overwritten each run)
FWCI_REL, CNP_ABS, PCT_POINTS = 0.01, 0.01, 2

CURRENT = f"""
SELECT id AS work_id, fwci,
  citation_normalized_percentile IS NOT NULL AS has_cnp,
  citation_normalized_percentile.value AS cnp_value,
  citation_normalized_percentile.is_in_top_1_percent AS cnp_top_1,
  citation_normalized_percentile.is_in_top_10_percent AS cnp_top_10,
  cited_by_percentile_year IS NOT NULL AS has_pct,
  cited_by_percentile_year.min AS pct_min,
  cited_by_percentile_year.max AS pct_max
FROM {WORKS_TABLE} WHERE id IS NOT NULL
"""

# One full outer join of Delta with the table: each row's kind (new / gone / moved) and, for moved rows, how far past
# the line it is (for the cap).
COMPARED = f"""
WITH j AS (
  SELECT c.*, coalesce(c.work_id, t.work_id) AS id, c.work_id IS NULL AS gone, t.work_id IS NULL AS is_new,
    NOT (t.has_cnp <=> c.has_cnp) OR NOT (t.has_pct <=> c.has_pct)
      OR (t.fwci IS NULL) <> (c.fwci IS NULL) OR (t.cnp_value IS NULL) <> (c.cnp_value IS NULL)
      OR (t.pct_min IS NULL) <> (c.pct_min IS NULL) OR (t.pct_max IS NULL) <> (c.pct_max IS NULL)
      OR NOT (t.cnp_top_1 <=> c.cnp_top_1) OR NOT (t.cnp_top_10 <=> c.cnp_top_10) AS flagged,
    coalesce(abs(t.fwci - c.fwci) / greatest(abs(c.fwci), 0.0001) / {FWCI_REL}, 0) AS fwci_x,
    coalesce(abs(t.cnp_value - c.cnp_value) / {CNP_ABS}, 0) AS cnp_x,
    coalesce(greatest(abs(t.pct_min - c.pct_min), abs(t.pct_max - c.pct_max)), 0) AS pct_d
  FROM ({CURRENT}) c FULL OUTER JOIN {METRICS_TABLE} t ON t.work_id = c.work_id
)
SELECT *, CASE WHEN gone OR is_new OR flagged THEN 1e9 ELSE greatest(fwci_x, cnp_x, pct_d / {PCT_POINTS}) END AS score
FROM j
WHERE gone OR is_new OR flagged OR fwci_x > 1 OR cnp_x > 1 OR pct_d >= {PCT_POINTS}
"""

# The cap's order without sorting the rows: key 0 = gone / new / flagged; then rows past the fwci or percentile-year
# line (keys 1..2000), then rows past only the CNP line (2001..4000), each by score in 1/32-doubling buckets, largest
# first. FWCI goes first so a bulk CNP correction (#1271, 10 Nov 2026: ~216M values) drains behind the daily fwci drift
# instead of crowding it out of the cap (#2082). Same order in notebooks/elastic/sync_work_metrics.
CAP_KEY = ("CASE WHEN gone OR is_new OR flagged THEN 0 "
           "ELSE greatest(1, 2000 - CAST(floor(log2(greatest(score, 1)) * 32) AS INT)) "
           f"+ IF(fwci_x > 1 OR pct_d >= {PCT_POINTS}, 0, 2000) END")


def cap_filter(table, key_sql, cap):
    """A predicate keeping about `cap` rows of `table`, lowest key first: one GROUP BY on the driver, no sort of the
    rows. Whole keys below the one that crosses the cap, then a hash slice of that key (+- a few thousand rows)."""
    taken = 0
    for r in spark.sql(f"SELECT {key_sql} AS k, count(*) AS n FROM {table} GROUP BY 1 ORDER BY 1").collect():
        if taken + r.n > cap:
            ppm = (cap - taken) * 1_000_000 // r.n
            return f"({key_sql} < {r.k} OR ({key_sql} = {r.k} AND pmod(xxhash64(id), 1000000) < {ppm}))"
        taken += r.n
    return "true"

# COMMAND ----------

if MODE == "full_build":
    spark.sql(f"""
        CREATE OR REPLACE TABLE {METRICS_TABLE} (
          work_id BIGINT NOT NULL, fwci DOUBLE, has_cnp BOOLEAN, cnp_value DOUBLE, cnp_top_1 BOOLEAN,
          cnp_top_10 BOOLEAN, has_pct BOOLEAN, pct_min INT, pct_max INT, updated_at TIMESTAMP,
          CONSTRAINT lakebase_work_metrics_pk PRIMARY KEY (work_id)
        ) CLUSTER BY (work_id)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        COMMENT 'oxjob #2082: fwci and percentiles per work for the Lakebase single-work overlay'
    """)
    spark.sql(f"INSERT INTO {METRICS_TABLE} SELECT *, current_timestamp() FROM ({CURRENT})")
    print(f"{METRICS_TABLE}: {spark.table(METRICS_TABLE).count():,} rows")
else:
    # Gone, new and flagged works first; then fwci / percentile-year moves, then CNP-only moves, each largest first; at
    # most about MAX_ROWS in all.
    changes = CHANGES_TABLE + ("_dry" if MODE == "dry_run" else "")
    spark.sql(f"CREATE OR REPLACE TABLE {changes} AS {COMPARED}")
    FP_MOVED = f"NOT (gone OR is_new OR flagged) AND (fwci_x > 1 OR pct_d >= {PCT_POINTS})"
    n, n_fp = spark.sql(f"SELECT count(*), count_if({FP_MOVED}) FROM {changes}").collect()[0]
    if MAX_ROWS and n > MAX_ROWS:
        keep = cap_filter(changes, CAP_KEY, MAX_ROWS)
        spark.sql(f"DELETE FROM {changes} WHERE NOT {keep}")
        print(f"{n:,} rows past the lines; cap {MAX_ROWS:,} keeps {keep}")
    r = spark.sql(f"SELECT count(*) AS kept, count_if(is_new) AS new_works, count_if(gone) AS gone, "
                  f"count_if(flagged AND NOT is_new AND NOT gone) AS flagged, count_if({FP_MOVED}) AS fp_moved, "
                  f"min(CASE WHEN NOT (gone OR is_new OR flagged) THEN score END) AS min_moved_score "
                  f"FROM {changes}").collect()[0]
    print(f"{MODE}: {n:,} past the lines; this run {r.kept:,} (new {r.new_works:,}, gone {r.gone:,}, "
          f"flagged {r.flagged:,}, fwci/pct-year moves {r.fp_moved:,} of {n_fp:,}, moves down to score {r.min_moved_score}); "
          f"{n - r.kept:,} wait a night")
if MODE == "nightly":
    m = spark.sql(f"""
        MERGE INTO {METRICS_TABLE} AS t
        USING {CHANGES_TABLE} AS c
        ON t.work_id = c.id
        WHEN MATCHED AND c.gone THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          t.fwci = c.fwci, t.has_cnp = c.has_cnp, t.cnp_value = c.cnp_value, t.cnp_top_1 = c.cnp_top_1,
          t.cnp_top_10 = c.cnp_top_10, t.has_pct = c.has_pct, t.pct_min = c.pct_min, t.pct_max = c.pct_max,
          t.updated_at = current_timestamp()
        WHEN NOT MATCHED AND NOT c.gone THEN INSERT
          (work_id, fwci, has_cnp, cnp_value, cnp_top_1, cnp_top_10, has_pct, pct_min, pct_max, updated_at)
          VALUES (c.work_id, c.fwci, c.has_cnp, c.cnp_value, c.cnp_top_1, c.cnp_top_10, c.has_pct, c.pct_min,
                  c.pct_max, current_timestamp())
    """).collect()[0].asDict()
    print(f"nightly: {n:,} past the lines; MERGE {m}")

# COMMAND ----------

if TRIGGER and MODE != "dry_run":
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import ResourceConflict

    w = WorkspaceClient()
    st = w.database.get_synced_database_table(name=SYNCED_TABLE)
    pid = getattr(st.data_synchronization_status, "pipeline_id", None) or getattr(getattr(st, "spec", None), "pipeline_id", None)
    if not pid:
        raise Exception(f"Could not resolve pipeline id for synced table {SYNCED_TABLE}: {st}")
    latest = w.pipelines.list_updates(pipeline_id=pid, max_results=1).updates or []
    prev = latest[0].state.value if latest else None
    try:
        w.pipelines.start_update(pipeline_id=pid)
        print(f"Triggered refresh: {SYNCED_TABLE} (pipeline {pid})")
    except ResourceConflict as e:
        print(f"Skipped {SYNCED_TABLE}: previous update still applying ({e})")
    if prev in ("FAILED", "CANCELED"):
        raise Exception(f"Previous refresh of {SYNCED_TABLE} ended {prev} (found at trigger time, one run late)")
