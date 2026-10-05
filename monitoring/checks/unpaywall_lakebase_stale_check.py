"""Lakebase staleness alarm for the Unpaywall API. STAGED 2026-10-05, NOT DEPLOYED. Read-only.

Fails (non-zero exit / raised error, so the Databricks job's on_failure email fires) when:
  1. a synced table is not ONLINE, or
  2. the source Delta table has a DATA commit (not OPTIMIZE/VACUUM/property changes) newer than the version Lakebase serves,
     and that commit is more than MAX_LAG_HOURS old  -> Lakebase is serving data more than a day behind, or
  3. the DOI source table itself has had no data commit for MAX_SOURCE_AGE_HOURS (the nightly write stopped upstream).
Runs as the job "Unpaywall Lakebase stale-data check" (jobs/unpaywall_lakebase_stale_check.yaml, serverless) or locally with
Databricks Connect:  DATABRICKS_CONFIG_PROFILE=dbc-ce570f73-0362 python monitoring/checks/unpaywall_lakebase_stale_check.py
"""
import os, sys, json, datetime as dt

MAX_LAG_HOURS = float(os.getenv("MAX_LAG_HOURS", "24"))
MAX_SOURCE_AGE_HOURS = float(os.getenv("MAX_SOURCE_AGE_HOURS", "48"))
PAIRS = [  # (synced table, source Delta table, check source age?)
    ("openalex.unpaywall.unpaywall_from_walden", "openalex.unpaywall.unpaywall", True),
    ("openalex.unpaywall.export_metadata_serving", "openalex.unpaywall.export_metadata", False),
]
MAINTENANCE = {"OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ANALYZE",
               "COMPUTE STATISTICS", "ADD CONSTRAINT", "DROP CONSTRAINT", "CHANGE COLUMN", "ADD COLUMNS",
               "CLUSTER BY", "REORG", "UPGRADE PROTOCOL", "SET TABLE PROPERTIES", "FSCK"}

from databricks.sdk import WorkspaceClient
profile = os.getenv("DATABRICKS_CONFIG_PROFILE")
w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
if os.getenv("DATABRICKS_RUNTIME_VERSION"):            # job task on Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:                                                   # laptop, Databricks Connect
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile(profile or "dbc-ce570f73-0362").serverless(True).getOrCreate()

now = dt.datetime.now(dt.timezone.utc)
problems, report = [], []
for synced, source, check_age in PAIRS:
    st = w.api_client.do("GET", f"/api/2.0/postgres/synced_tables/{synced}")["status"]
    state = st.get("detailed_state", "?")
    info = (st.get("last_sync") or {}).get("delta_table_sync_info") or {}
    served = int(info.get("delta_commit_version", -1))
    if "ONLINE" not in state:
        problems.append(f"{synced}: state {state}")
    hist = spark.sql(f"SELECT version, operation, (unix_timestamp(current_timestamp()) - unix_timestamp(timestamp)) / 3600.0 AS age_h "
                     f"FROM (DESCRIBE HISTORY {source}) ORDER BY version DESC LIMIT 200").collect()  # ages computed in SQL: no time-zone mix-ups
    data = [h for h in hist if h["operation"] not in MAINTENANCE]
    newest = data[0] if data else None
    unserved = [h for h in data if h["version"] > served]
    oldest_unserved_h = max((float(h["age_h"]) for h in unserved), default=0.0)
    line = {"synced": synced, "state": state, "served_version": served, "served_commit_time": info.get("delta_commit_time"),
            "newest_data_version": newest["version"] if newest else None, "unserved_data_commits": len(unserved),
            "oldest_unserved_hours": round(oldest_unserved_h, 1)}
    if unserved and oldest_unserved_h > MAX_LAG_HOURS:
        problems.append(f"{synced}: serves v{served}; source has {len(unserved)} newer data commit(s), oldest {oldest_unserved_h:.1f} h old (> {MAX_LAG_HOURS} h)")
    if check_age and newest:
        age = float(newest["age_h"]); line["source_newest_data_age_hours"] = round(age, 1)
        if age > MAX_SOURCE_AGE_HOURS:
            problems.append(f"{source}: no data commit for {age:.1f} h (> {MAX_SOURCE_AGE_HOURS} h): nightly write stopped upstream")
    report.append(line)

print(json.dumps({"when_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "tables": report}, indent=1, default=str))
if problems:
    msg = "LAKEBASE STALE (Unpaywall API may be serving old data):\n- " + "\n- ".join(problems)
    print(msg)
    sys.exit(msg)  # non-zero exit fails the task, which sends the on_failure email
print("OK: every Lakebase table is ONLINE and within", MAX_LAG_HOURS, "h of its source")
