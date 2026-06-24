# Databricks notebook source
# MAGIC %md
# MAGIC # Works Daily Metrics
# MAGIC
# MAGIC Computes a daily set of coverage/health metrics over
# MAGIC `openalex.works.openalex_works` and appends them (tall format) to
# MAGIC `openalex.works.works_daily_metrics`.
# MAGIC
# MAGIC **Tall schema** — one row per `snapshot_date` x `metric` x `dimension`:
# MAGIC
# MAGIC | column           | type      | notes                                                        |
# MAGIC |------------------|-----------|--------------------------------------------------------------|
# MAGIC | snapshot_date    | date      | calendar day the metric describes                            |
# MAGIC | snapshot_version | bigint    | Delta version of openalex_works the metric was computed from |
# MAGIC | metric           | string    | e.g. `works_total`, `works_by_oa_status`                     |
# MAGIC | dimension        | string    | NULL for scalar metrics; the group value for `*_by_*`        |
# MAGIC | value            | bigint    | the count                                                    |
# MAGIC | computed_at      | timestamp | when this run computed the row                               |
# MAGIC
# MAGIC **Daily run**: computes metrics on the current (live) table, tagged with
# MAGIC `snapshot_date`. Re-running a date is idempotent (deletes that date's rows
# MAGIC then re-inserts).
# MAGIC
# MAGIC **First run / backfill**: when the table doesn't exist yet (or `backfill_days`
# MAGIC is set), it walks Delta time travel for the prior N days (default 30), picking
# MAGIC the table version current at end-of-day for each date. openalex_works retains
# MAGIC 30 days of history (`delta.deletedFileRetentionDuration` /
# MAGIC `logRetentionDuration` = 30 days), so a 30-day backfill is the practical max.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType,
)

SRC_TABLE = "openalex.works.openalex_works"
DST_TABLE = "openalex.works.works_daily_metrics"

# COMMAND ----------

# Job parameters
dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")
dbutils.widgets.text("backfill_days", "", "Backfill N prior days via time travel (blank = auto: 30 on first run, else 0)")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = (
    datetime.strptime(_sd, "%Y-%m-%d").date()
    if _sd else datetime.now(timezone.utc).date()
)

table_exists = spark.catalog.tableExists(DST_TABLE)

_bf = dbutils.widgets.get("backfill_days").strip()
if _bf:
    BACKFILL_DAYS = int(_bf)
elif not table_exists:
    BACKFILL_DAYS = 30  # auto-backfill the retention window on the very first run
else:
    BACKFILL_DAYS = 0

print(f"RUN_DATE={RUN_DATE}  BACKFILL_DAYS={BACKFILL_DAYS}  dst_exists={table_exists}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric definitions
# MAGIC
# MAGIC `_thin` is a one-pass projection of every work down to small integer flags +
# MAGIC the three grouping dimensions. We `CACHE` it so the expensive read of the
# MAGIC nested `authorships` column happens **once** per snapshot, and the scalar
# MAGIC aggregate + the three GROUP BYs all run against the cached thin table.

# COMMAND ----------

# Per-work flag projection. {asof} is either "" (live) or "VERSION AS OF <n>".
# {d} is the snapshot date used for the created/updated-on-date velocity flags.
THIN_SQL = """
CREATE OR REPLACE TEMP VIEW _thin AS
SELECT
  CASE WHEN is_xpac THEN 1 ELSE 0 END                                                AS f_xpac,
  CASE WHEN abstract IS NOT NULL AND length(trim(abstract)) > 0 THEN 1 ELSE 0 END    AS f_abstract,
  CASE WHEN abstract_inverted_index IS NOT NULL
            AND length(trim(abstract_inverted_index)) > 0 THEN 1 ELSE 0 END          AS f_aii,
  CASE WHEN EXISTS(authorships, a ->
            EXISTS(a.raw_affiliation_strings, s -> s IS NOT NULL AND length(trim(s)) > 0))
       THEN 1 ELSE 0 END                                                             AS f_affil,
  CASE WHEN EXISTS(authorships, a -> a.is_corresponding) THEN 1 ELSE 0 END           AS f_corr,
  CASE WHEN fulltext IS NOT NULL AND length(trim(fulltext)) > 0 THEN 1 ELSE 0 END    AS f_fulltext,
  CASE WHEN has_content.pdf THEN 1 ELSE 0 END                                        AS f_pdf,
  CASE WHEN has_content.grobid_xml THEN 1 ELSE 0 END                                 AS f_grobid,
  CASE WHEN referenced_works_count > 0 THEN 1 ELSE 0 END                             AS f_refs,
  CASE WHEN doi IS NOT NULL THEN 1 ELSE 0 END                                        AS f_doi,
  CASE WHEN institutions_distinct_count > 0 THEN 1 ELSE 0 END                        AS f_inst,
  CASE WHEN EXISTS(authorships, a -> a.author.orcid IS NOT NULL) THEN 1 ELSE 0 END   AS f_orcid,
  CASE WHEN open_access.is_oa THEN 1 ELSE 0 END                                      AS f_is_oa,
  CASE WHEN is_retracted THEN 1 ELSE 0 END                                           AS f_retracted,
  CASE WHEN is_paratext THEN 1 ELSE 0 END                                            AS f_paratext,
  CASE WHEN language IS NOT NULL THEN 1 ELSE 0 END                                   AS f_language,
  CASE WHEN primary_topic.id IS NOT NULL THEN 1 ELSE 0 END                           AS f_topic,
  CASE WHEN size(awards) > 0 THEN 1 ELSE 0 END                                       AS f_awards,
  CASE WHEN size(funders) > 0 THEN 1 ELSE 0 END                                      AS f_funders,
  CASE WHEN created_date = DATE('{d}') THEN 1 ELSE 0 END                             AS f_created_on_date,
  CASE WHEN to_date(updated_date) = DATE('{d}') THEN 1 ELSE 0 END                    AS f_updated_on_date,
  open_access.oa_status            AS oa_status,
  type                             AS work_type,
  primary_location.provenance      AS provenance,
  indexed_in                       AS indexed_in
FROM {src} {asof}
"""

# scalar metric column -> output metric name (NULL dimension)
SCALAR_METRICS = [
    ("count_total",        "works_total"),
    ("f_xpac",             "works_xpac"),
    ("f_abstract",         "works_with_abstract"),
    ("f_aii",              "works_with_abstract_inverted_index"),
    ("f_affil",            "works_with_affiliation_strings"),
    ("f_corr",             "works_with_corresponding_author"),
    ("f_fulltext",         "works_with_fulltext"),
    ("f_pdf",              "works_with_pdf"),
    ("f_grobid",           "works_with_grobid_xml"),
    ("f_refs",             "works_with_references"),
    ("f_doi",              "works_with_doi"),
    ("f_inst",             "works_with_matched_institution"),
    ("f_orcid",            "works_with_author_orcid"),
    ("f_is_oa",            "works_is_oa"),
    ("f_retracted",        "works_retracted"),
    ("f_paratext",         "works_paratext"),
    ("f_language",         "works_with_language"),
    ("f_topic",            "works_with_primary_topic"),
    ("f_awards",           "works_with_awards"),
    ("f_funders",          "works_with_funders"),
    ("f_created_on_date",  "works_created_on_date"),
    ("f_updated_on_date",  "works_updated_on_date"),
]

SCALAR_SQL = "SELECT COUNT(*) AS count_total, " + ", ".join(
    f"SUM({col}) AS {col}" for col, _ in SCALAR_METRICS if col != "count_total"
) + " FROM _thin"

# grouped metrics: metric name -> grouping column (dimension)
GROUPED_METRICS = {
    "works_by_oa_status":  "oa_status",
    "works_by_type":       "work_type",
    "works_by_provenance": "provenance",
}

# COMMAND ----------

def compute_for(snapshot_date, asof_clause, version):
    """Return list of (snapshot_date, version, metric, dimension, value) rows."""
    spark.sql(THIN_SQL.format(src=SRC_TABLE, asof=asof_clause, d=snapshot_date))
    spark.sql("CACHE TABLE _thin")
    try:
        rows = []

        # scalar metrics -> one row, unpivoted
        srow = spark.sql(SCALAR_SQL).collect()[0].asDict()
        for col, metric in SCALAR_METRICS:
            rows.append((snapshot_date, version, metric, None, int(srow[col] or 0)))

        # grouped metrics
        for metric, dim_col in GROUPED_METRICS.items():
            for r in spark.sql(
                f"SELECT {dim_col} AS dim, COUNT(*) AS c FROM _thin GROUP BY {dim_col}"
            ).collect():
                rows.append((snapshot_date, version, metric,
                             r["dim"] if r["dim"] is not None else "(null)", int(r["c"])))

        # works_by_indexed_in (explode the array<string>; works in 0 indexes drop out)
        for r in spark.sql(
            "SELECT x AS dim, COUNT(*) AS c FROM _thin "
            "LATERAL VIEW explode(indexed_in) t AS x GROUP BY x"
        ).collect():
            rows.append((snapshot_date, version, "works_by_indexed_in",
                         r["dim"] if r["dim"] is not None else "(null)", int(r["c"])))

        return rows
    finally:
        spark.sql("UNCACHE TABLE _thin")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve which (date, version) snapshots to compute

# COMMAND ----------

# Map each backfill date -> the Delta version current at end-of-day (latest commit
# whose timestamp <= 23:59:59 of that date).
hist = (
    spark.sql(f"DESCRIBE HISTORY {SRC_TABLE}")
    .select("version", "timestamp")
    .collect()
)
hist = sorted(((int(h["version"]), h["timestamp"]) for h in hist), key=lambda x: x[1])
latest_version = hist[-1][0]


def _retention_days():
    """deletedFileRetentionDuration in days (default 30 if unreadable)."""
    import re
    try:
        val = spark.sql(
            f"SHOW TBLPROPERTIES {SRC_TABLE} ('delta.deletedFileRetentionDuration')"
        ).collect()[0]["value"].lower()  # e.g. "30 days" / "720 hours" / "interval ..."
        m = re.search(r"(\d+)\s*day", val)
        if m:
            return int(m.group(1))
        m = re.search(r"(\d+)\s*hour", val)
        if m:
            return int(m.group(1)) // 24
    except Exception as e:
        print(f"  could not read retention ({e}); defaulting to 30 days")
    return 30


# Delta enforces deletedFileRetentionDuration as a ROLLING window against the
# current wall-clock time, so any version older than (now - retention) is
# unreadable — and the boundary advances during the (multi-hour) backfill. Keep a
# 1-day safety margin below the horizon so a chosen version can't slip past it
# mid-run. Net effect: a 30-day retention yields ~29 reachable daily snapshots.
RETENTION_DAYS = _retention_days()
SAFETY_MARGIN_DAYS = 1
safe_lower_bound = datetime.now(timezone.utc) - timedelta(
    days=RETENTION_DAYS - SAFETY_MARGIN_DAYS
)
print(f"retention={RETENTION_DAYS}d -> only time-travel to versions newer than "
      f"{safe_lower_bound:%Y-%m-%d %H:%M} UTC")


def version_asof_end_of(d):
    cutoff = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
    cands = [
        (v, ts) for v, ts in hist
        if safe_lower_bound <= ts.replace(tzinfo=timezone.utc) <= cutoff
    ]
    return max(cands, key=lambda x: x[0])[0] if cands else None


# Live "today" snapshot (current table) + any backfill days.
targets = [(RUN_DATE, "", latest_version)]  # (date, asof_clause, version)
for n in range(1, BACKFILL_DAYS + 1):
    d = RUN_DATE - timedelta(days=n)
    v = version_asof_end_of(d)
    if v is None:
        print(f"  skip {d}: no reachable version (outside retention window)")
        continue
    targets.append((d, f"VERSION AS OF {v}", v))

targets.sort(key=lambda t: t[0])
print(f"computing {len(targets)} snapshot(s): "
      f"{targets[0][0]} .. {targets[-1][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute all snapshots

# COMMAND ----------

all_rows = []
for d, asof, v in targets:
    print(f"-> {d}  (version {v}{' live' if asof == '' else ''}) ...")
    all_rows.extend(compute_for(d, asof, v))
print(f"total rows computed: {len(all_rows)}")

# COMMAND ----------

schema = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("snapshot_version", LongType(), True),
    StructField("metric", StringType(), False),
    StructField("dimension", StringType(), True),
    StructField("value", LongType(), False),
])
out = (
    spark.createDataFrame(all_rows, schema)
    .withColumn("computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (idempotent per snapshot_date)

# COMMAND ----------

if not table_exists:
    (out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DST_TABLE))
    print(f"created {DST_TABLE} with {out.count()} rows")
else:
    dates = [r["snapshot_date"] for r in out.select("snapshot_date").distinct().collect()]
    in_list = ", ".join(f"DATE'{d}'" for d in dates)
    spark.sql(f"DELETE FROM {DST_TABLE} WHERE snapshot_date IN ({in_list})")
    out.write.format("delta").mode("append").saveAsTable(DST_TABLE)
    print(f"replaced {len(dates)} date(s) in {DST_TABLE}; appended {out.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check — most recent snapshot

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric, dimension, value
    FROM {DST_TABLE}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {DST_TABLE})
    ORDER BY (dimension IS NOT NULL), metric, value DESC
"""))
