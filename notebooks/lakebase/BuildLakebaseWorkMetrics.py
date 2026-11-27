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
# MAGIC A work with a doc and no row is compared on its doc too. `compare_to=row` restores the old comparison (row alone).
# MAGIC The mirror follows the docs' Delta tables; the API reads their synced copies, so for the hour or two a docs refresh
# MAGIC takes after the run, a work this run left to its new doc still serves its previous doc (accepted lag).
# MAGIC
# MAGIC - `mode=full_build`: (re)create the table from `openalex_works` (one-time; then create the synced table, TRIGGERED,
# MAGIC   primary key `work_id`, and `GRANT SELECT ON lakebase.lakebase_work_metrics TO openalex_api_ro`).
# MAGIC - `mode=seed_docs`: (re)build `lakebase_work_doc_metrics` from the docs tables (one-time, or to recover).
# MAGIC - `mode=nightly`: one MERGE of the rows to change, then trigger the synced table's refresh (`trigger_sync=true`).
# MAGIC - `mode=dry_run`: what nightly would change, capped, in `lakebase_work_metrics_changes_dry`; no MERGE, no refresh,
# MAGIC   and `lakebase_work_doc_metrics` is read, never written (tonight's doc changes join it as a view).
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



def served_sql(doc_src):
    """What a single GET serves per work: its row, unless its doc was written after the row (openalex-elastic-api
    works/lakebase.py joins the row only when m.updated_at >= d.updated_at), or the doc when there is no row. A work
    with a doc and no row is compared on its doc and gets a row only when that drifted past the lines."""
    use_row = "m.work_id IS NOT NULL AND (d.work_id IS NULL OR m.updated_at >= d.doc_updated_at)"
    return f"""
SELECT coalesce(m.work_id, d.work_id) AS work_id, {', '.join(f'IF({use_row}, m.{c}, d.{c}) AS {c}' for c in VALUE_COLS)}
FROM {METRICS_TABLE} m FULL OUTER JOIN {doc_src} d ON d.work_id = m.work_id
"""


# One full outer join of Delta with what is served (`target`: a table or a subquery in parentheses): each row's kind
# (new = neither row nor doc / gone / moved) and, for moved rows, how far past the line it is (for the cap).
def compared_sql(target):
    return f"""
WITH j AS (
  SELECT c.*, coalesce(c.work_id, t.work_id) AS id, c.work_id IS NULL AS gone, t.work_id IS NULL AS is_new,
    NOT (t.has_cnp <=> c.has_cnp) OR NOT (t.has_pct <=> c.has_pct)
      OR (t.fwci IS NULL) <> (c.fwci IS NULL) OR (t.cnp_value IS NULL) <> (c.cnp_value IS NULL)
      OR (t.pct_min IS NULL) <> (c.pct_min IS NULL) OR (t.pct_max IS NULL) <> (c.pct_max IS NULL)
      OR NOT (t.cnp_top_1 <=> c.cnp_top_1) OR NOT (t.cnp_top_10 <=> c.cnp_top_10) AS flagged,
    coalesce(abs(t.fwci - c.fwci) / greatest(abs(c.fwci), 0.0001) / {FWCI_REL}, 0) AS fwci_x,
    coalesce(abs(t.cnp_value - c.cnp_value) / {CNP_ABS}, 0) AS cnp_x,
    coalesce(greatest(abs(t.pct_min - c.pct_min), abs(t.pct_max - c.pct_max)), 0) AS pct_d
  FROM ({CURRENT}) c FULL OUTER JOIN {target} t ON t.work_id = c.work_id
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


def _docs_heads():
    """{shard: (Delta table id, latest version)} of the docs tables. The id tells a recreated table from the one read."""
    return {s: (spark.sql(f"DESCRIBE DETAIL {DOCS_TABLE(s)}").collect()[0].id, _latest_version(DOCS_TABLE(s)))
            for s in range(N_DOC_SHARDS)}


def _save_heads(heads):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DOC_STATE_TABLE} (shard INT, table_id STRING, version BIGINT, read_at TIMESTAMP)")
    rows = ", ".join(f"({s}, '{tid}', {v})" for s, (tid, v) in heads.items())
    spark.sql(f"""
        MERGE INTO {DOC_STATE_TABLE} AS t
        USING (SELECT * FROM VALUES {rows} AS v(shard, table_id, version)) AS s ON t.shard = s.shard
        WHEN MATCHED THEN UPDATE SET t.table_id = s.table_id, t.version = s.version, t.read_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (shard, table_id, version, read_at)
          VALUES (s.shard, s.table_id, s.version, current_timestamp())
    """)


def seed_doc_metrics():
    """(Re)build DOC_METRICS_TABLE from every docs shard as of its current version (one atomic CREATE OR REPLACE), and
    record those versions. ~720 GB of docs: minutes on 8 workers; avoid on the nightly 2-worker cluster."""
    heads = _docs_heads()
    src = "(" + " UNION ALL ".join(f"SELECT work_id, doc, updated_at FROM {DOCS_TABLE(s)} VERSION AS OF {v}"
                                   for s, (_, v) in heads.items()) + ")"
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DOC_METRICS_TABLE} CLUSTER BY (work_id)
        COMMENT 'oxjob #2659: fwci and percentiles each Lakebase works doc carries (notebooks/lakebase/BuildLakebaseWorkMetrics)'
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        AS {doc_values(src)}
    """)
    _save_heads(heads)
    print(f"seed_docs: {spark.table(DOC_METRICS_TABLE).count():,} doc rows at versions "
          f"{ {s: v for s, (_, v) in heads.items()} }")


def refresh_doc_metrics(write):
    """The docs' values as of the docs tables' current versions; returns the table (or view) holding them.

    Reads only what changed since the versions last read: the docs tables' change feed, parsed to the narrow values
    first, then the latest change per work (a delete loses to an insert or update in the same commit). write=True
    MERGEs it into DOC_METRICS_TABLE and records the versions (one writer at a time: the nightly task, or a hand run
    while the End 2 End is not in this task). write=False (dry runs) leaves both tables alone and returns a view.
    A shard that can't be followed by its feed (never read, recreated, version gone backwards, or last read 6+ days ago,
    past the 7-day VACUUM that drops old change files) makes write=True re-seed in full and write=False refuse."""
    state = ({r.shard: r for r in spark.table(DOC_STATE_TABLE).collect()}
             if spark.catalog.tableExists(DOC_STATE_TABLE) and spark.catalog.tableExists(DOC_METRICS_TABLE) else {})
    heads = _docs_heads()
    now = spark.sql("SELECT current_timestamp() AS t").collect()[0].t
    lost = [s for s, (tid, v) in heads.items()
            if s not in state or state[s].table_id != tid or v < state[s].version
            or (now - state[s].read_at).total_seconds() > 6 * 86400]
    if lost:
        if not write:
            raise Exception(f"{DOC_METRICS_TABLE} can't follow docs shards {lost} by their change feed: run mode=seed_docs")
        print(f"docs shards {lost} can't be followed by their change feed: re-seeding {DOC_METRICS_TABLE} in full")
        seed_doc_metrics()
        return DOC_METRICS_TABLE
    ranges = [(s, state[s].version + 1, v) for s, (_, v) in heads.items() if v > state[s].version]
    if not ranges:
        if write:
            _save_heads(heads)
        print("doc metrics: no new docs versions")
        return DOC_METRICS_TABLE
    feed = " UNION ALL ".join(
        f"SELECT work_id, doc, updated_at, _commit_version, _change_type FROM table_changes('{DOCS_TABLE(s)}', {a}, {b}) "
        f"WHERE _change_type <> 'update_preimage'" for s, a, b in ranges)
    parsed = doc_values(f"({feed})", ", _commit_version, _change_type = 'delete' AS is_delete, "
                                     "(j IS NULL AND doc IS NOT NULL) AS unparsed")
    staging = "openalex.works._lakebase_doc_metrics_incr" + ("" if write else "_dry")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {staging} AS
        SELECT * EXCEPT (rn, _commit_version) FROM (
          SELECT *, row_number() OVER (PARTITION BY work_id ORDER BY _commit_version DESC, is_delete, doc_updated_at DESC) AS rn
          FROM ({parsed})) WHERE rn = 1
    """)
    st = spark.sql(f"SELECT count(*) AS n, count_if(is_delete) AS dels, count_if(unparsed) AS unparsed FROM {staging}").collect()[0]
    print(f"doc metrics: versions {[(s, a, b) for s, a, b in ranges]}: {st.n:,} works changed ({st.dels:,} deleted, "
          f"{st.unparsed:,} docs unparsed: their nulls compare as flagged, so they get a row)")
    cols = ["work_id"] + VALUE_COLS + ["doc_updated_at"]
    if not write:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW doc_metrics_now AS
            SELECT d.* FROM {DOC_METRICS_TABLE} d LEFT ANTI JOIN {staging} c ON c.work_id = d.work_id
            UNION ALL SELECT {', '.join(cols)} FROM {staging} WHERE NOT is_delete
        """)
        return "doc_metrics_now"
    m = spark.sql(f"""
        MERGE INTO {DOC_METRICS_TABLE} AS t USING {staging} AS c ON t.work_id = c.work_id
        WHEN MATCHED AND c.is_delete THEN DELETE
        WHEN MATCHED THEN UPDATE SET {', '.join(f't.{c} = c.{c}' for c in cols[1:])}
        WHEN NOT MATCHED AND NOT c.is_delete THEN INSERT ({', '.join(cols)}) VALUES ({', '.join(f'c.{c}' for c in cols)})
    """).collect()[0].asDict()
    _save_heads(heads)
    print(f"doc metrics MERGE {m}")
    return DOC_METRICS_TABLE

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
    target = (f"({served_sql(refresh_doc_metrics(write=MODE == 'nightly'))})" if COMPARE_TO == "served"
              else METRICS_TABLE)
    # Gone, new and flagged works first; then fwci / percentile-year moves, then CNP-only moves, each largest first; at
    # most about MAX_ROWS in all.
    changes = CHANGES_TABLE + ("_dry" if MODE == "dry_run" else "")
    spark.sql(f"CREATE OR REPLACE TABLE {changes} AS {compared_sql(target)}")
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
