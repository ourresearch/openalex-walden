# Databricks notebook source
# MAGIC %md
# MAGIC # Landing-page scoreboard (oxjob #1789)
# MAGIC
# MAGIC Nightly. Rewrites the last `days_back` fetch days (UTC) of `openalex.monitoring.landing_page_funnel`
# MAGIC (fetch -> stored -> parsed -> fields, per host) and `openalex.monitoring.landing_page_alerts`, from the SQL in
# MAGIC `monitoring/landing_pages/`. Re-scoring 3 days lets late parses land (Parseland lags fetches by ~5 h, p99 12 h);
# MAGIC the newest day scored is yesterday, so today's half-parsed day is never judged.
# MAGIC
# MAGIC Reads `taxicab.taxicab_results`, `parseland.parsed_pages`, `landing_page.meta_tags`; writes only those two
# MAGIC monitoring tables. Fails (so Sentry hears through the desk failure bridge) only when the scoreboard itself is
# MAGIC broken: no fetch rows for yesterday. Alarms are rows, not failures; a desk task turns new paging rows into a
# MAGIC wake-up for oxjob #1789. Tables, functions and the host key: `monitoring/landing_pages/README.md`.

# COMMAND ----------

import os
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
SQL_DIR = os.path.join(REPO_ROOT, "monitoring", "landing_pages")

dbutils.widgets.text("end_date", "", "exclusive end day YYYY-MM-DD (blank = today UTC)")
dbutils.widgets.text("days_back", "3", "fetch days to rewrite, ending the day before end_date")

_e = dbutils.widgets.get("end_date").strip()
END = datetime.strptime(_e, "%Y-%m-%d").date() if _e else datetime.now(timezone.utc).date()
START = END - timedelta(days=int(dbutils.widgets.get("days_back") or 3))
print(f"window [{START}, {END})")

# COMMAND ----------

# Each file is one statement (the backfill runner, scripts/landing_page_scoreboard.py, runs the same files).
for name in ("funnel.sql", "alerts.sql"):
    t0 = datetime.now()
    spark.sql(open(os.path.join(SQL_DIR, name)).read().format(start=START.isoformat(), end=END.isoformat()))
    print(f"{name}: {(datetime.now() - t0).seconds}s")

# COMMAND ----------

yesterday = (END - timedelta(days=1)).isoformat()
rows = spark.sql(f"""
  SELECT count(*) n, coalesce(sum(fetches), 0) fetches, count(DISTINCT host) hosts
  FROM openalex.monitoring.landing_page_funnel WHERE fetch_date = DATE'{yesterday}'
""").first()
print(f"{yesterday}: {rows.n} rows, {rows.fetches} fetches, {rows.hosts} hosts")
if rows.fetches == 0:
    raise Exception(f"Landing-page scoreboard: no taxicab_results fetches on {yesterday}. Either Taxicab stopped "
                    "writing or the scoreboard query broke; check openalex.taxicab.taxicab_results first.")

pages = spark.sql(f"""
  SELECT host, id_namespace, rule, round(100 * useful_rate) u, round(100 * baseline_rate) b, top_failure
  FROM openalex.monitoring.landing_page_alerts WHERE alert_date = DATE'{yesterday}' AND page
  ORDER BY host
""").collect()
for p in pages:
    print(f"PAGE {yesterday} {p.host} ({p.id_namespace}) {p.rule}: useful {p.u}% vs {p.b}%; top failure {p.top_failure}")
print(f"{len(pages)} paging alarm rows for {yesterday}")
