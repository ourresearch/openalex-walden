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
# MAGIC Lines (same as `notebooks/elastic/sync_work_metrics`): a row is rewritten when fwci moves by > 1%, CNP value by
# MAGIC > 0.01, a percentile-year bound by 2+ points, or a null-ness / top-1/10% flag changes. Works new to the table are
# MAGIC inserted; gone works deleted.
# MAGIC
# MAGIC - `mode=full_build`: (re)create the table from `openalex_works` (one-time; then create the synced table, TRIGGERED,
# MAGIC   primary key `work_id`, and `GRANT SELECT ON lakebase.lakebase_work_metrics TO openalex_api_ro`).
# MAGIC - `mode=nightly`: MERGE the moved rows, then trigger the synced table's refresh (`trigger_sync=true`).
# MAGIC - `mode=dry_run`: count what nightly would change.
# MAGIC
# MAGIC **Revert:** set `METRICS_OVERLAY = False` in the API (or config var `LAKEBASE_METRICS_OVERLAY=0`, no deploy);
# MAGIC docs answer alone, as before. Then drop the synced table and this task.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "nightly", "full_build"])
dbutils.widgets.dropdown("trigger_sync", "false", ["true", "false"])
MODE = dbutils.widgets.get("mode")
TRIGGER = dbutils.widgets.get("trigger_sync") == "true"

WORKS_TABLE = "openalex.works.openalex_works"
METRICS_TABLE = "openalex.works.lakebase_work_metrics"
SYNCED_TABLE = "openalex.lakebase.lakebase_work_metrics"
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

MOVED = f"""
    NOT (t.has_cnp <=> c.has_cnp) OR NOT (t.has_pct <=> c.has_pct)
    OR (t.fwci IS NULL) <> (c.fwci IS NULL) OR (t.cnp_value IS NULL) <> (c.cnp_value IS NULL)
    OR NOT (t.cnp_top_1 <=> c.cnp_top_1) OR NOT (t.cnp_top_10 <=> c.cnp_top_10)
    OR abs(t.fwci - c.fwci) > {FWCI_REL} * greatest(abs(c.fwci), 0.0001)
    OR abs(t.cnp_value - c.cnp_value) > {CNP_ABS}
    OR abs(t.pct_min - c.pct_min) >= {PCT_POINTS} OR abs(t.pct_max - c.pct_max) >= {PCT_POINTS}
    OR (t.pct_min IS NULL) <> (c.pct_min IS NULL) OR (t.pct_max IS NULL) <> (c.pct_max IS NULL)
"""

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
elif MODE == "dry_run":
    r = spark.sql(f"""
        SELECT count_if(t.work_id IS NULL) AS new_works, count_if(t.work_id IS NOT NULL AND ({MOVED})) AS moved
        FROM ({CURRENT}) c LEFT JOIN {METRICS_TABLE} t ON t.work_id = c.work_id
    """).collect()[0]
    gone = spark.sql(f"SELECT count(*) AS n FROM {METRICS_TABLE} t LEFT ANTI JOIN {WORKS_TABLE} w ON w.id = t.work_id").collect()[0].n
    print(f"dry_run: new {r.new_works:,}, moved past the lines {r.moved:,}, gone {gone:,}")
else:
    m = spark.sql(f"""
        MERGE INTO {METRICS_TABLE} AS t
        USING ({CURRENT}) AS c
        ON t.work_id = c.work_id
        WHEN MATCHED AND ({MOVED}) THEN UPDATE SET
          t.fwci = c.fwci, t.has_cnp = c.has_cnp, t.cnp_value = c.cnp_value, t.cnp_top_1 = c.cnp_top_1,
          t.cnp_top_10 = c.cnp_top_10, t.has_pct = c.has_pct, t.pct_min = c.pct_min, t.pct_max = c.pct_max,
          t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
          (work_id, fwci, has_cnp, cnp_value, cnp_top_1, cnp_top_10, has_pct, pct_min, pct_max, updated_at)
          VALUES (c.work_id, c.fwci, c.has_cnp, c.cnp_value, c.cnp_top_1, c.cnp_top_10, c.has_pct, c.pct_min,
                  c.pct_max, current_timestamp())
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """).collect()[0].asDict()
    print(f"nightly MERGE: {m}")

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
