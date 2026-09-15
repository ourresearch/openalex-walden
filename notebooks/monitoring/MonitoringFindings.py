# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring — findings (oxjob #1116)
# MAGIC
# MAGIC The shared engine. For every component with a checks file in `monitoring/checks/`
# MAGIC (or the one named by the `component` parameter), read its metrics table, evaluate
# MAGIC every check for `snapshot_date`, and write one row per check (× captured dimension)
# MAGIC to `openalex.monitoring.findings`. Status is deterministic (`utils/monitoring_engine.py`);
# MAGIC the LLM step (`MonitoringReport`) only ever adds narrative on top.
# MAGIC
# MAGIC No component-specific code lives here. Adding a component = a metrics writer + a
# MAGIC YAML file.
# MAGIC
# MAGIC `backfill_days` > 0 re-evaluates that many trailing days in order (consecutive-day
# MAGIC confirmation and baseline exclusion need order), delete-then-append per
# MAGIC (snapshot_date, component).

# COMMAND ----------

import glob
import os
import sys
from datetime import datetime, timedelta, timezone

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, DoubleType, StringType,
                               StructField, StructType)

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, REPO_ROOT)
from utils.monitoring_engine import MetricStore, evaluate_day, summarize  # noqa: E402

MONITORING_SCHEMA = "openalex.monitoring"
FINDINGS_TABLE = f"{MONITORING_SCHEMA}.findings"
OVERRIDES_TABLE = f"{MONITORING_SCHEMA}.baseline_overrides"
CHECKS_TABLE = f"{MONITORING_SCHEMA}.checks"
CHECKS_DIR = os.path.join(REPO_ROOT, "monitoring", "checks")

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "YYYY-MM-DD (blank = today UTC)")
dbutils.widgets.text("component", "", "one component (blank = every checks file)")
dbutils.widgets.text("backfill_days", "0", "re-evaluate this many trailing days, in order")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = datetime.strptime(_sd, "%Y-%m-%d").date() if _sd else datetime.now(timezone.utc).date()
ONLY = dbutils.widgets.get("component").strip() or None
BACKFILL = int(dbutils.widgets.get("backfill_days") or 0)
print(f"RUN_DATE={RUN_DATE} component={ONLY or '*'} backfill_days={BACKFILL}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MONITORING_SCHEMA}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FINDINGS_TABLE} (
  snapshot_date DATE NOT NULL,
  component     STRING NOT NULL,
  section_id    STRING NOT NULL,
  section       STRING NOT NULL,
  check_id      STRING NOT NULL,
  title         STRING NOT NULL,
  dimension     STRING,
  rule          STRING NOT NULL,
  page          BOOLEAN NOT NULL,
  value         DOUBLE,
  baseline      DOUBLE,
  mad           DOUBLE,
  deviation     DOUBLE,
  status        STRING NOT NULL,   -- ok | watch | critical | known | insufficient_history | insufficient_n | no_data
  detail        STRING,
  verdict       STRING,            -- filled by MonitoringReport (LLM), nullable
  suggestion    STRING,            -- filled by MonitoringReport (LLM), nullable
  computed_at   TIMESTAMP NOT NULL
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OVERRIDES_TABLE} (
  component     STRING NOT NULL,
  check_id      STRING NOT NULL,
  dimension     STRING,
  accepted_from DATE NOT NULL,     -- baseline window starts here (Guardrails-style "accept as new normal")
  who           STRING,
  why           STRING,
  created_at    TIMESTAMP NOT NULL
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHECKS_TABLE} (
  component   STRING NOT NULL,
  section_id  STRING NOT NULL,
  section     STRING NOT NULL,
  check_id    STRING NOT NULL,
  title       STRING NOT NULL,
  expr        STRING NOT NULL,
  rule        STRING NOT NULL,
  page        BOOLEAN NOT NULL,
  enabled     BOOLEAN NOT NULL,
  failure_modes STRING,
  spec_json   STRING NOT NULL,     -- the full check dict, for the report prompt and the dashboard
  loaded_at   TIMESTAMP NOT NULL
) USING DELTA
""")

# COMMAND ----------

def load_specs():
    specs = []
    for path in sorted(glob.glob(os.path.join(CHECKS_DIR, "*.yaml"))):
        with open(path) as fh:
            spec = yaml.safe_load(fh)
        if ONLY and spec["component"] != ONLY:
            continue
        specs.append(spec)
    if not specs:
        raise RuntimeError(f"no checks files matched in {CHECKS_DIR} (component={ONLY})")
    return specs


def sync_checks_table(spec):
    """Mirror the YAML into monitoring.checks so the report prompt and dashboard read data, not files."""
    import json
    rows = []
    for s in spec.get("sections", []):
        for c in s.get("checks", []):
            rows.append((spec["component"], s["id"], s["question"], c["id"], c["title"], c["expr"],
                         c.get("rule", "relative"), bool(c.get("page", False)), bool(c.get("enabled", True)),
                         c.get("failure_modes"), json.dumps(c, default=str)))
    schema = StructType([
        StructField("component", StringType(), False), StructField("section_id", StringType(), False),
        StructField("section", StringType(), False), StructField("check_id", StringType(), False),
        StructField("title", StringType(), False), StructField("expr", StringType(), False),
        StructField("rule", StringType(), False), StructField("page", BooleanType(), False),
        StructField("enabled", BooleanType(), False), StructField("failure_modes", StringType(), True),
        StructField("spec_json", StringType(), False),
    ])
    spark.sql(f"DELETE FROM {CHECKS_TABLE} WHERE component = '{spec['component']}'")
    (spark.createDataFrame(rows, schema).withColumn("loaded_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(CHECKS_TABLE))


def load_metrics(spec, first_day, last_day):
    """Rows from the component's registered table and/or the shared monitoring.metrics table."""
    lookback = first_day - timedelta(days=45)  # window + rolling headroom
    frames = []
    table = spec.get("metrics_table")
    if table:
        frames.append(spark.sql(f"""
            SELECT snapshot_date, metric, dimension, CAST(value AS DOUBLE) AS value
            FROM {table} WHERE snapshot_date BETWEEN DATE'{lookback}' AND DATE'{last_day}'"""))
    if spark.catalog.tableExists(f"{MONITORING_SCHEMA}.metrics"):
        frames.append(spark.sql(f"""
            SELECT snapshot_date, metric, dimension, value
            FROM {MONITORING_SCHEMA}.metrics
            WHERE component = '{spec['component']}'
              AND snapshot_date BETWEEN DATE'{lookback}' AND DATE'{last_day}'"""))
    if not frames:
        raise RuntimeError(f"{spec['component']}: no metrics source (set metrics_table or emit to monitoring.metrics)")
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    return [r.asDict() for r in df.collect()]


def load_prior(component, first_day):
    return [r.asDict() for r in spark.sql(f"""
        SELECT snapshot_date, check_id, dimension, status, rule
        FROM {FINDINGS_TABLE}
        WHERE component = '{component}' AND snapshot_date < DATE'{first_day}'
          AND snapshot_date >= DATE'{first_day - timedelta(days=45)}'""").collect()]


def load_overrides(component):
    return {(r["check_id"], r["dimension"]): r["accepted_from"] for r in spark.sql(f"""
        SELECT check_id, dimension, MAX(accepted_from) AS accepted_from
        FROM {OVERRIDES_TABLE} WHERE component = '{component}' GROUP BY 1, 2""").collect()}

# COMMAND ----------

FINDINGS_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False), StructField("component", StringType(), False),
    StructField("section_id", StringType(), False), StructField("section", StringType(), False),
    StructField("check_id", StringType(), False), StructField("title", StringType(), False),
    StructField("dimension", StringType(), True), StructField("rule", StringType(), False),
    StructField("page", BooleanType(), False), StructField("value", DoubleType(), True),
    StructField("baseline", DoubleType(), True), StructField("mad", DoubleType(), True),
    StructField("deviation", DoubleType(), True), StructField("status", StringType(), False),
    StructField("detail", StringType(), True),
])
COLS = [f.name for f in FINDINGS_SCHEMA.fields]

days = [RUN_DATE - timedelta(days=i) for i in range(BACKFILL, -1, -1)]

for spec in load_specs():
    comp = spec["component"]
    sync_checks_table(spec)
    store = MetricStore(load_metrics(spec, days[0], days[-1]))
    prior = load_prior(comp, days[0])
    overrides = load_overrides(comp)
    new_rows = []
    for d in days:
        todays = evaluate_day(spec, store, d, prior + new_rows, overrides)
        new_rows.extend(todays)
        print(f"{comp} {d}: {summarize(todays)}")
    if not new_rows:
        continue
    df = (spark.createDataFrame([tuple(r.get(c) for c in COLS) for r in new_rows], FINDINGS_SCHEMA)
          .withColumn("verdict", F.lit(None).cast(StringType()))
          .withColumn("suggestion", F.lit(None).cast(StringType()))
          .withColumn("computed_at", F.current_timestamp()))
    spark.sql(f"DELETE FROM {FINDINGS_TABLE} WHERE component = '{comp}' "
              f"AND snapshot_date BETWEEN DATE'{days[0]}' AND DATE'{days[-1]}'")
    df.write.format("delta").mode("append").saveAsTable(FINDINGS_TABLE)
    print(f"{comp}: wrote {len(new_rows)} findings for {days[0]}..{days[-1]}")

# COMMAND ----------

display(spark.sql(f"""
SELECT component, status, COUNT(*) n FROM {FINDINGS_TABLE}
WHERE snapshot_date = DATE'{RUN_DATE}' GROUP BY 1, 2 ORDER BY 1, 2"""))
