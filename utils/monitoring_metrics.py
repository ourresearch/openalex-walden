"""Monitoring shared metrics sink (oxjob #1116).

Any job can add its metrics to the shared area at the end of a run:

    from utils.monitoring_metrics import emit
    emit(spark, "ingest_repo", [("files_loaded", None, 1234), ("rows_added", "repo_items", 98765)])

Rows land in one tall table, `openalex.monitoring.metrics`, keyed by component, and are
delete-then-append per (snapshot_date, component, source) so a rerun replaces its own rows
and nothing else. Two ways to feed it, both write the same shape:

  self-report  — the job's last task calls emit() with the numbers only the job knows
                 (rows processed, records dropped by filter reason, wall time).
  observe      — a separate notebook reads pipeline tables after the run and emits state
                 metrics (backlog, coverage, precision samples). This is what
                 AuthorshipDailyMetrics does; its table stays where it is and is registered
                 in the checks YAML via `metrics_table`.

The engine reads whichever table the component's YAML names; a component may use both
(`metrics_table` for its legacy table, plus rows in monitoring.metrics under `source`).
"""

from __future__ import annotations

from datetime import date, datetime, timezone

METRICS_TABLE = "openalex.monitoring.metrics"

SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS openalex.monitoring"
DDL = f"""
CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
  snapshot_date DATE NOT NULL,
  component     STRING NOT NULL,
  metric        STRING NOT NULL,
  dimension     STRING,
  value         DOUBLE NOT NULL,
  source        STRING NOT NULL,   -- job/task/notebook that emitted the row
  computed_at   TIMESTAMP NOT NULL
) USING DELTA
CLUSTER BY (component, snapshot_date)
"""


def emit(spark, component: str, rows, source: str, snapshot_date: date | None = None) -> int:
    """rows: iterable of (metric, dimension|None, value). Returns rows written."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

    snapshot_date = snapshot_date or datetime.now(timezone.utc).date()
    data = [(snapshot_date, component, m, d, float(v or 0)) for m, d, v in rows]
    if not data:
        return 0
    spark.sql(SCHEMA_DDL)
    spark.sql(DDL)
    schema = StructType([
        StructField("snapshot_date", DateType(), False),
        StructField("component", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("dimension", StringType(), True),
        StructField("value", DoubleType(), False),
    ])
    df = (spark.createDataFrame(data, schema)
          .withColumn("source", F.lit(source))
          .withColumn("computed_at", F.current_timestamp()))
    spark.sql(
        f"DELETE FROM {METRICS_TABLE} WHERE snapshot_date = DATE'{snapshot_date}' "
        f"AND component = '{component}' AND source = '{source}'"
    )
    df.write.format("delta").mode("append").saveAsTable(METRICS_TABLE)
    return len(data)
