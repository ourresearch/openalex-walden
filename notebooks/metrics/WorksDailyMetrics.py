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
# MAGIC Computes metrics on the current (live) `openalex_works` and writes them
# MAGIC tagged with `snapshot_date` (default today UTC). Re-running a date is
# MAGIC idempotent — it deletes that date's rows then re-inserts — so a same-day
# MAGIC re-run refreshes it and any day can be corrected by re-running with
# MAGIC `snapshot_date` set to that date.
# MAGIC
# MAGIC The initial 30-day history was seeded once via Delta time travel (see git
# MAGIC history, commit removing the backfill path); this notebook is now daily-only.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType,
)

SRC_TABLE = "openalex.works.openalex_works"
DST_TABLE = "openalex.works.works_daily_metrics"

# COMMAND ----------

# Job parameters
dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = (
    datetime.strptime(_sd, "%Y-%m-%d").date()
    if _sd else datetime.now(timezone.utc).date()
)

table_exists = spark.catalog.tableExists(DST_TABLE)
print(f"RUN_DATE={RUN_DATE}  dst_exists={table_exists}")

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
# MAGIC ## Record the live table version

# COMMAND ----------

# The current Delta version of openalex_works, stored on each row for provenance.
latest_version = int(
    spark.sql(f"DESCRIBE HISTORY {SRC_TABLE} LIMIT 1").collect()[0]["version"]
)
print(f"openalex_works current version = {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute + write today's snapshot (idempotent per snapshot_date)

# COMMAND ----------

schema = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("snapshot_version", LongType(), True),
    StructField("metric", StringType(), False),
    StructField("dimension", StringType(), True),
    StructField("value", LongType(), False),
])

print(f"-> {RUN_DATE}  (version {latest_version}) computing ...")
rows = compute_for(RUN_DATE, "", latest_version)  # "" = read the live table

df = (spark.createDataFrame(rows, schema)
      .withColumn("computed_at", F.current_timestamp()))

if not table_exists:
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(DST_TABLE))
else:
    # Replace just RUN_DATE so same-day re-runs / corrections are idempotent.
    spark.sql(f"DELETE FROM {DST_TABLE} WHERE snapshot_date = DATE'{RUN_DATE}'")
    df.write.format("delta").mode("append").saveAsTable(DST_TABLE)

print(f"done: wrote {len(rows)} rows for {RUN_DATE}")

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
