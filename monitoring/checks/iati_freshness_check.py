"""IATI awards freshness check (oxjob #1570). Read-only.

Fails (non-zero exit, so the job's on_failure email fires) when:
  1. openalex.awards.iati_raw has had no data commit for MAX_AGE_DAYS (the weekly refresh stopped), or
  2. an enabled IATI provenance has no rows in openalex_awards_raw at the IATI priority, or
  3. an IATI provenance in openalex_awards_raw is short in the served awards table (awards_api) by more than
     MAX_SERVED_GAP (the nightly dropped it), ignoring rows the nightly folded into another award.
Runs as the last task of the job "IATI awards weekly refresh" (jobs/iati_awards_refresh.yaml) or locally:
  DATABRICKS_CONFIG_PROFILE=dbc-ce570f73-0362 python monitoring/checks/iati_freshness_check.py
"""
import os, sys, json

MAX_AGE_DAYS = float(os.getenv("MAX_AGE_DAYS", "8"))
MAX_SERVED_GAP = float(os.getenv("MAX_SERVED_GAP", "0.02"))
PRIORITY = int(os.getenv("IATI_PRIORITY", "600"))
PROVENANCES = [p.strip() for p in os.getenv("IATI_PROVENANCES", "iati_fcdo,iati_sida,iati_norad").split(",") if p.strip()]
MAINTENANCE = {"OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ANALYZE",
               "COMPUTE STATS", "COMPUTE STATISTICS", "REORG", "FSCK"}

if os.getenv("DATABRICKS_RUNTIME_VERSION"):            # job task on Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:                                                   # laptop, Databricks Connect
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile(os.getenv("DATABRICKS_CONFIG_PROFILE", "dbc-ce570f73-0362")).serverless(True).getOrCreate()

problems, report = [], {}
hist = spark.sql("SELECT version, operation, (unix_timestamp(current_timestamp()) - unix_timestamp(timestamp)) / 86400.0 AS age_d "
                 "FROM (DESCRIBE HISTORY openalex.awards.iati_raw) ORDER BY version DESC LIMIT 100").collect()
data = [h for h in hist if h["operation"] not in MAINTENANCE]
age = float(data[0]["age_d"]) if data else None
report["iati_raw_newest_data_version"] = data[0]["version"] if data else None
report["iati_raw_age_days"] = round(age, 2) if age is not None else None
if age is None or age > MAX_AGE_DAYS:
    problems.append(f"openalex.awards.iati_raw: no data commit for {age if age is None else round(age, 1)} days (> {MAX_AGE_DAYS})")

raw = {r["provenance"]: r["n"] for r in spark.sql(
    f"SELECT provenance, count(*) n FROM openalex.awards.openalex_awards_raw "
    f"WHERE provenance RLIKE '^iati_[a-z0-9_]+$' AND priority = {PRIORITY} GROUP BY 1").collect()}
served = {r["provenance"]: r["n"] for r in spark.sql(
    "SELECT provenance, count(*) n FROM openalex.awards.awards_api WHERE provenance RLIKE '^iati_[a-z0-9_]+$' GROUP BY 1").collect()}
report["raw"], report["served"] = raw, served
for p in PROVENANCES:
    if not raw.get(p):
        problems.append(f"{p}: no rows in openalex_awards_raw at priority {PRIORITY}")
    elif served.get(p, 0) < (1 - MAX_SERVED_GAP) * raw[p]:
        problems.append(f"{p}: {served.get(p, 0)} served vs {raw[p]} in openalex_awards_raw (gap > {MAX_SERVED_GAP:.0%})")

print(json.dumps(report, indent=1, default=str))
if problems:
    msg = "IATI AWARDS STALE OR SHORT:\n- " + "\n- ".join(problems)
    print(msg)
    sys.exit(msg)
print("OK: iati_raw refreshed within", MAX_AGE_DAYS, "days; every IATI provenance published and served")
