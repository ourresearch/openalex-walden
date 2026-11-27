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
# MAGIC **Compared with what single GETs serve (oxjob #2659).** The API applies a metrics row only when it was written at
# MAGIC or after the work's doc (both tables' `updated_at`); otherwise the doc's own values answer, since a re-stamp rewrote
# MAGIC the doc with that night's values. So the lines are measured against the served value: the row's, or the doc's when
# MAGIC the doc is newer. The doc's values live in `openalex.works.lakebase_work_doc_metrics` (not synced), seeded once from
# MAGIC the docs tables (`mode=seed_docs`) and kept current from their change feed at the start of every run. Works re-stamped
# MAGIC tonight then cost no row (their new doc already serves tonight's values), and a doc that drifted past the lines gets
# MAGIC a row. Before, the cap went to rows the doc had already replaced, while the drifted docs were never looked at.
# MAGIC `compare_to=row` restores the old comparison (with the row alone).
# MAGIC
# MAGIC - `mode=full_build`: (re)create the table from `openalex_works` (one-time; then create the synced table, TRIGGERED,
# MAGIC   primary key `work_id`, and `GRANT SELECT ON lakebase.lakebase_work_metrics TO openalex_api_ro`).
# MAGIC - `mode=seed_docs`: (re)build `lakebase_work_doc_metrics` from the docs tables (one-time, or to recover).
# MAGIC - `mode=nightly`: one MERGE of the rows to change, then trigger the synced table's refresh (`trigger_sync=true`).
# MAGIC - `mode=dry_run`: what nightly would change, capped, in `lakebase_work_metrics_changes_dry`; no MERGE, no refresh.
# MAGIC   It does bring `lakebase_work_doc_metrics` up to date (a mirror of the docs, read by nothing else).
# MAGIC
# MAGIC **Revert:** set `METRICS_OVERLAY = False` in the API (or config var `LAKEBASE_METRICS_OVERLAY=0`, no deploy);
# MAGIC docs answer alone, as before. Then drop the synced table and this task.

# COMMAND ----------

# MAGIC %pip install -U -q databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "nightly", "full_build", "seed_docs"])
dbutils.widgets.dropdown("trigger_sync", "false", ["true", "false"])
dbutils.widgets.text("max_rows", "20000000")   # rows rewritten a night at most (the synced-table refresh time)
dbutils.widgets.dropdown("compare_to", "served", ["served", "row"])  # served = row or doc, as the API picks (#2659)
MODE = dbutils.widgets.get("mode")
TRIGGER = dbutils.widgets.get("trigger_sync") == "true"
MAX_ROWS = int(dbutils.widgets.get("max_rows") or 0) or None
COMPARE_TO = dbutils.widgets.get("compare_to")

WORKS_TABLE = "openalex.works.openalex_works"
METRICS_TABLE = "openalex.works.lakebase_work_metrics"
SYNCED_TABLE = "openalex.lakebase.lakebase_work_metrics"
CHANGES_TABLE = "openalex.works.lakebase_work_metrics_changes"  # tonight's rewrites (overwritten each run)
FWCI_REL, CNP_ABS, PCT_POINTS = 0.01, 0.01, 2

# oxjob #2659: the metric values each stored doc carries, and the docs tables' Delta version last read into it.
N_DOC_SHARDS = 8
DOCS_TABLE = lambda s: f"openalex.works.lakebase_works_docs_{s}"  # notebooks/lakebase/BuildLakebaseWorksDocs
DOC_METRICS_TABLE = "openalex.works.lakebase_work_doc_metrics"
DOC_STATE_TABLE = "openalex.works.lakebase_work_doc_metrics_state"
VALUE_COLS = ["fwci", "has_cnp", "cnp_value", "cnp_top_1", "cnp_top_10", "has_pct", "pct_min", "pct_max"]
DOC_JSON = ("fwci DOUBLE, citation_normalized_percentile STRUCT<value: DOUBLE, is_in_top_1_percent: BOOLEAN, "
            "is_in_top_10_percent: BOOLEAN>, cited_by_percentile_year STRUCT<min: INT, max: INT>")


def doc_values(src, extra=""):
    """The doc's values in CURRENT's shape (below), plus doc_updated_at, from `src` rows (work_id, doc, updated_at)."""
    return f"""
SELECT work_id, j.fwci AS fwci,
  j.citation_normalized_percentile IS NOT NULL AS has_cnp,
  j.citation_normalized_percentile.value AS cnp_value,
  j.citation_normalized_percentile.is_in_top_1_percent AS cnp_top_1,
  j.citation_normalized_percentile.is_in_top_10_percent AS cnp_top_10,
  j.cited_by_percentile_year IS NOT NULL AS has_pct,
  j.cited_by_percentile_year.min AS pct_min,
  j.cited_by_percentile_year.max AS pct_max,
  updated_at AS doc_updated_at{extra}
FROM (SELECT *, from_json(doc, '{DOC_JSON}') AS j FROM {src})
"""

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

# What a single GET serves for each work with a row: the row, unless the doc was written after it (openalex-elastic-api
# works/lakebase.py joins the row only when m.updated_at >= d.updated_at). Works with no row are `is_new` below.
SERVED = f"""
SELECT m.work_id, {', '.join(f'IF(d.work_id IS NULL OR m.updated_at >= d.doc_updated_at, m.{c}, d.{c}) AS {c}' for c in VALUE_COLS)}
FROM {METRICS_TABLE} m LEFT JOIN {DOC_METRICS_TABLE} d ON d.work_id = m.work_id
"""
TARGET = f"({SERVED})" if COMPARE_TO == "served" else METRICS_TABLE

# One full outer join of Delta with what is served: each row's kind (new / gone / moved) and, for moved rows, how far
# past the line it is (for the cap).
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
  FROM ({CURRENT}) c FULL OUTER JOIN {TARGET} t ON t.work_id = c.work_id
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


def _latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0].version


def _save_versions(versions):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DOC_STATE_TABLE} (shard INT, version BIGINT, read_at TIMESTAMP)")
    rows = ", ".join(f"({s}, {v})" for s, v in versions.items())
    spark.sql(f"""
        MERGE INTO {DOC_STATE_TABLE} AS t
        USING (SELECT * FROM VALUES {rows} AS v(shard, version)) AS s ON t.shard = s.shard
        WHEN MATCHED THEN UPDATE SET t.version = s.version, t.read_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (shard, version, read_at) VALUES (s.shard, s.version, current_timestamp())
    """)


def _shard_at(s, v):
    return f"(SELECT work_id, doc, updated_at FROM {DOCS_TABLE(s)} VERSION AS OF {v})"


def seed_doc_metrics():
    """(Re)build DOC_METRICS_TABLE from every docs shard as of its current version, and record those versions."""
    versions = {s: _latest_version(DOCS_TABLE(s)) for s in range(N_DOC_SHARDS)}
    src = "(" + " UNION ALL ".join(f"SELECT * FROM {_shard_at(s, v)}" for s, v in versions.items()) + ")"
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DOC_METRICS_TABLE} CLUSTER BY (work_id)
        COMMENT 'oxjob #2659: fwci and percentiles each Lakebase works doc carries (notebooks/lakebase/BuildLakebaseWorkMetrics)'
        AS {doc_values(src)}
    """)
    _save_versions(versions)
    print(f"seed_docs: {spark.table(DOC_METRICS_TABLE).count():,} doc rows at versions {versions}")


def refresh_doc_metrics():
    """Bring DOC_METRICS_TABLE up to the docs tables' current versions from their change feed: the latest insert or
    update per work since the version last read, and deletes. A shard whose feed can't be read from there (vacuumed
    after a long gap) is re-read whole. Seeds the table when it doesn't exist yet."""
    if not spark.catalog.tableExists(DOC_STATE_TABLE) or not spark.catalog.tableExists(DOC_METRICS_TABLE):
        print(f"{DOC_METRICS_TABLE} not seeded yet: seeding")
        return seed_doc_metrics()
    done = {r.shard: r.version for r in spark.table(DOC_STATE_TABLE).collect()}
    staging = "openalex.works._lakebase_doc_metrics_incr"
    versions, report = {}, {}
    for s in range(N_DOC_SHARDS):
        last, v = done.get(s), _latest_version(DOCS_TABLE(s))
        versions[s] = v
        if last is not None and v <= last:
            report[s] = "no new versions"
            continue
        try:
            if last is None:
                raise ValueError("no version recorded")
            latest = f"""(SELECT work_id, doc, updated_at, _change_type = 'delete' AS is_delete FROM (
                SELECT *, row_number() OVER (PARTITION BY work_id ORDER BY _commit_version DESC) AS rn
                FROM table_changes('{DOCS_TABLE(s)}', {last + 1}, {v}) WHERE _change_type <> 'update_preimage') WHERE rn = 1)"""
            spark.sql(f"CREATE OR REPLACE TABLE {staging} AS {doc_values(latest, ', is_delete')}")
        except Exception as e:  # the feed from `last` is gone (or none recorded): read the shard whole
            print(f"shard {s}: change feed {last}..{v} unreadable ({str(e)[:300]}); re-reading the shard at {v}")
            spark.sql(f"DELETE FROM {DOC_METRICS_TABLE} WHERE pmod(work_id, {N_DOC_SHARDS}) = {s}")
            spark.sql(f"INSERT INTO {DOC_METRICS_TABLE} {doc_values(_shard_at(s, v))}")
            report[s] = f"re-read whole at {v}"
            continue
        up = spark.sql(f"""
            MERGE INTO {DOC_METRICS_TABLE} AS t
            USING (SELECT * EXCEPT (is_delete) FROM {staging} WHERE NOT is_delete) AS c ON t.work_id = c.work_id
            WHEN MATCHED AND c.doc_updated_at >= t.doc_updated_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """).collect()[0].asDict()
        dl = spark.sql(f"""
            MERGE INTO {DOC_METRICS_TABLE} AS t USING (SELECT work_id FROM {staging} WHERE is_delete) AS c
            ON t.work_id = c.work_id WHEN MATCHED THEN DELETE
        """).collect()[0].asDict()
        report[s] = (f"versions {last + 1}..{v}: {up.get('num_updated_rows', 0):,} updated, "
                     f"{up.get('num_inserted_rows', 0):,} new, {dl.get('num_deleted_rows', 0):,} deleted")
    _save_versions(versions)
    for s, msg in report.items():
        print(f"doc metrics shard {s}: {msg}")

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
elif MODE == "seed_docs":
    seed_doc_metrics()
else:
    if COMPARE_TO == "served":
        refresh_doc_metrics()
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
    print(f"{MODE} (compared with the {COMPARE_TO} values): {n:,} past the lines; this run {r.kept:,} (new {r.new_works:,}, gone {r.gone:,}, "
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

if TRIGGER and MODE in ("nightly", "full_build"):
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
