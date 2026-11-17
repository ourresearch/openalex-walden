# Databricks notebook source
# MAGIC %md
# MAGIC # Oxjob #780: unlink damaged PDFs from `openalex.pdf.pdf_works` (one-off, branch only)
# MAGIC
# MAGIC #185's recipe (`strip_invalid_pdfs_from_pdf_works.py`, walden before `7be9fb73`) for a short list: on every
# MAGIC `pdf_works` row whose `ids` holds `docs.pdf` = one of the listed files, drop that `docs.pdf` entry and the row's
# MAGIC `docs.parsed-pdf` entry. The change feed carries it to `locations_parsed`, and the next End 2 End sets the
# MAGIC location's `pdf_s3_id` NULL, so a work with no other file shows `has_content.pdf` false. `invalid_pdfs` (written
# MAGIC 17 Nov) keeps a re-parse from linking the file again; a later good fetch of the same URL replaces the row.
# MAGIC
# MAGIC **Never run while the End 2 End is running** (it refreshes `pdf_works`): only after the night publishes and before
# MAGIC 22:30 CT. Before rows are saved to `openalex.transient.oxjob780_pdf_works_before`.
# MAGIC
# MAGIC Revert: `MERGE INTO openalex.pdf.pdf_works p USING openalex.transient.oxjob780_pdf_works_before b
# MAGIC ON p.native_id = b.native_id WHEN MATCHED THEN UPDATE SET p.ids = b.ids`.

# COMMAND ----------

import json

dbutils.widgets.text("source_pdf_ids", "", "comma-separated bare UUIDs")
dbutils.widgets.text("dry_run", "true", "true = plan only")
IDS = [x for x in dbutils.widgets.get("source_pdf_ids").replace(" ", "").split(",") if x]
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
assert IDS and len(IDS) <= 100
bad = ",".join(f"'{x}.pdf'" for x in IDS)
victims = f"exists(ids, x -> x.namespace = 'docs.pdf' AND x.id IN ({bad}))"
NEW_IDS = (f"filter(ids, x -> NOT ((x.namespace = 'docs.pdf' AND x.id IN ({bad})) OR x.namespace = 'docs.parsed-pdf'))")

rows = spark.sql(f"SELECT native_id, ids, {NEW_IDS} AS new_ids FROM openalex.pdf.pdf_works WHERE {victims}").collect()
plan = [{"native_id": r.native_id, "drop": [f"{i.namespace}={i.id}" for i in r.ids if i not in r.new_ids]} for r in rows]
for p in plan:
    print(json.dumps(p))
print(f"{len(plan)} pdf_works rows hold one of {len(IDS)} files")
assert len(plan) <= len(IDS) * 2, "more victim rows than expected: stop"

# COMMAND ----------

if plan and not DRY_RUN:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS openalex.transient.oxjob780_pdf_works_before AS
                  SELECT native_id, ids, current_timestamp() AS saved_at FROM openalex.pdf.pdf_works WHERE false""")
    spark.sql(f"""INSERT INTO openalex.transient.oxjob780_pdf_works_before
                  SELECT native_id, ids, current_timestamp() FROM openalex.pdf.pdf_works WHERE {victims}""")
    saved = spark.sql("SELECT count(DISTINCT native_id) n FROM openalex.transient.oxjob780_pdf_works_before").first().n
    assert saved >= len(plan), f"saved {saved} < {len(plan)}: not stripping"
    spark.sql(f"""MERGE INTO openalex.pdf.pdf_works p
                  USING (SELECT native_id FROM openalex.pdf.pdf_works WHERE {victims}) s ON p.native_id = s.native_id
                  WHEN MATCHED THEN UPDATE SET p.ids = {NEW_IDS.replace('filter(ids,', 'filter(p.ids,')}""")

left = spark.sql(f"SELECT count(*) n FROM openalex.pdf.pdf_works WHERE {victims}").first().n
result = {"dry_run": DRY_RUN, "plan": plan, "victim_rows_left": left}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
