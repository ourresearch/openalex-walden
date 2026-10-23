# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze re-check: apply closures to `pdf_works` (oxjob #1290)
# MAGIC
# MAGIC `bronze_recheck_score` writes each closed PDF URL to `openalex.pdf.pdf_removed` (cleared groups only).
# MAGIC `PDF.py` honours that table on every rebuild (walden `2fa7fbc1`), but rows already in `pdf_works` keep
# MAGIC `is_oa = true` until something rewrites them. This notebook is that write, the same MERGE #1105 used
# MAGIC on 2026-09-17: `is_oa = false` and `updated_date` bumped (the union sequences `locations_parsed` on
# MAGIC it), then `applied_at` stamped. Downstream is the normal pipeline: Union (every 8 h) carries the flip
# MAGIC into `locations_parsed`; the nightly End 2 End turns the work's `oa_status` from bronze to closed (or
# MAGIC green, when a repository copy remains).
# MAGIC
# MAGIC **Guard.** The nightly fails Guardrails when more than 7.5M works re-stamp. One pass closes ~70-185K
# MAGIC works (oxjob #1290 EXPLORE § 3), so `max_rows` (default 1M) stops anything far off that estimate
# MAGIC before it writes. Over it, find out why; a real change that big needs Jason's pre-clear in the same ask.

# COMMAND ----------

dbutils.widgets.text("pass_id", "2026-10", "Pass id")
dbutils.widgets.text("max_rows", "1000000", "Refuse to flip more pdf_works rows than this in one run")
dbutils.widgets.text("removed_table", "openalex.pdf.pdf_removed", "Closures table (PDF.py reads it); tests point elsewhere")
dbutils.widgets.text("dry_run", "true", "true = count only, write nothing")

pass_id = dbutils.widgets.get("pass_id").strip()
max_rows = int(dbutils.widgets.get("max_rows"))
removed_table = dbutils.widgets.get("removed_table").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
SOURCE = f"oxjob #1290 bronze re-check {pass_id}"  # same string as bronze_recheck_score

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW to_apply AS
SELECT DISTINCT pdf_url FROM {removed_table} WHERE source = '{SOURCE}' AND applied_at IS NULL
""")
pending = spark.sql("SELECT count(*) FROM to_apply").collect()[0][0]
flips = spark.sql("""
SELECT count(*) FROM openalex.pdf.pdf_works p JOIN to_apply r ON p.native_id = r.pdf_url WHERE p.is_oa
""").collect()[0][0]
print(f"{SOURCE}: {pending:,} pdf_removed rows waiting; {flips:,} pdf_works rows would flip is_oa -> false")

if flips > max_rows:
    raise Exception(f"{flips:,} rows to flip > max_rows {max_rows:,}: far over the pass estimate; check before applying")
if dry_run or pending == 0:
    dbutils.notebook.exit(f"{'dry run' if dry_run else 'nothing to apply'}: {flips:,} rows would flip")

# COMMAND ----------

# Freeze the set first (a table, not a lazy view) so rows the scorer adds mid-run wait for the next run.
STAGE = "openalex.taxicab.bronze_recheck_apply_stage"
spark.sql(f"CREATE OR REPLACE TABLE {STAGE} AS SELECT * FROM to_apply")
spark.sql(f"CREATE OR REPLACE TEMP VIEW to_apply_now AS SELECT * FROM {STAGE}")
spark.sql("""
MERGE INTO openalex.pdf.pdf_works t
USING to_apply_now r ON t.native_id = r.pdf_url
WHEN MATCHED AND t.is_oa THEN UPDATE SET t.is_oa = false, t.updated_date = current_date()
""")
spark.sql(f"""
MERGE INTO {removed_table} t
USING to_apply_now r ON t.pdf_url = r.pdf_url AND t.source = '{SOURCE}' AND t.applied_at IS NULL
WHEN MATCHED THEN UPDATE SET t.applied_at = current_timestamp()
""")
left = spark.sql("""SELECT count(*) FROM openalex.pdf.pdf_works p JOIN to_apply_now r ON p.native_id = r.pdf_url
                    WHERE p.is_oa""").collect()[0][0]
print(f"applied: {flips:,} pdf_works rows flipped; {left:,} still is_oa (expect 0)")
