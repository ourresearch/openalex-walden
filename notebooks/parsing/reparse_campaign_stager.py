# Databricks notebook source
# MAGIC %md
# MAGIC ### Reparse campaign stager (oxjob #984)
# MAGIC
# MAGIC Stateless tick, scheduled every 20 min. Replaces the laptop-hosted campaign
# MAGIC runner so batch chaining does not depend on a workstation staying awake.
# MAGIC Each tick does exactly one of:
# MAGIC 1. a reparse-mode **Parse PDFs** run is active → nothing;
# MAGIC 2. the queue still holds rows (a run died mid-batch) → trim the ids that
# MAGIC    already have an outcome, relaunch the drain;
# MAGIC 3. queue empty → stage the next batch of the population onto the ledger
# MAGIC    (`reparse_campaign_789`, batches continue from the last one) and the
# MAGIC    queue, launch the drain. When the population is exhausted it exits —
# MAGIC    pause the job at that point.
# MAGIC
# MAGIC Guard: ≥3 failed reparse runs in the last 6h → this tick fails (email) and
# MAGIC launches nothing, so a broken driver does not burn ECS time all night.

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from databricks.sdk import WorkspaceClient

PARSE_PDFS_JOB_ID = 99046008643006
LEDGER = "openalex.pdf.reparse_campaign_789"
QUEUE = "openalex.pdf.grobid_reparse_queue"
BATCH_SIZE = 700_000            # single-node driver dies at ~3-3.5h sustained load (oxjob #789)
FIRST_BATCH = 52                # 51 = the #984 1K smoke

w = WorkspaceClient()

def scalar(sql):
    return spark.sql(sql).collect()[0][0]

# COMMAND ----------

# The legacy-backfill population: pdf_works rows whose parse id never appeared in
# grobid_processing_results (the Feb-2025 pdf_works_grobid_backfill snapshot).
# Excludes ids already on the ledger, any post-0.9.1 success under the same pdf
# uuid, junk files, and rows with no pdf uuid.
REMAINDER_SELECT = """
WITH pw AS (
    SELECT regexp_replace(get(filter(ids, x -> x.namespace = 'docs.pdf'), 0).id, '\\\\.pdf$', '') AS source_pdf_id,
           get(filter(ids, x -> x.namespace = 'docs.parsed-pdf'), 0).id AS parse_key
    FROM openalex.pdf.pdf_works
    WHERE exists(ids, x -> x.namespace = 'docs.parsed-pdf')
),
legacy AS (
    SELECT DISTINCT pw.source_pdf_id
    FROM pw
    LEFT ANTI JOIN (SELECT s3_key FROM openalex.pdf.grobid_processing_results) g ON pw.parse_key = g.s3_key
    WHERE pw.source_pdf_id IS NOT NULL
),
post AS (
    SELECT DISTINCT source_pdf_id
    FROM openalex.pdf.grobid_processing_results
    WHERE created_date >= '2026-08-04' AND status LIKE 'success%'
),
remainder AS (
    SELECT l.source_pdf_id
    FROM legacy l
    LEFT ANTI JOIN openalex.pdf.reparse_campaign_789 c USING (source_pdf_id)
    LEFT ANTI JOIN post USING (source_pdf_id)
    LEFT ANTI JOIN openalex.pdf.invalid_pdfs i USING (source_pdf_id)
)
"""

# COMMAND ----------

def reparse_runs(active_only=False, since=None):
    out = []
    for run in w.jobs.list_runs(job_id=PARSE_PDFS_JOB_ID, active_only=active_only,
                                start_time_from=int(since.timestamp() * 1000) if since else None):
        full = w.jobs.get_run(run.run_id)
        op = full.overriding_parameters
        params = (op.notebook_params or {}) if op else {}
        if params.get("reparse_queue_only", "").lower() == "true":
            out.append(full)
    return out

def launch_drain(reason):
    run = w.jobs.run_now(job_id=PARSE_PDFS_JOB_ID, notebook_params={
        "reparse_queue_only": "true", "batch_size": str(BATCH_SIZE + 100_000)})
    print(f"launched Parse PDFs run {run.run_id} ({reason})")

# COMMAND ----------

active = reparse_runs(active_only=True)
if active:
    print(f"reparse run {active[0].run_id} active — nothing to do")
    dbutils.notebook.exit("active")

recent_failed = [r for r in reparse_runs(since=datetime.now(timezone.utc) - timedelta(hours=6))
                 if r.state.result_state and r.state.result_state.value not in ("SUCCESS",)]
if len(recent_failed) >= 3:
    raise RuntimeError(f"{len(recent_failed)} failed reparse runs in the last 6h — not launching; inspect "
                       + ", ".join(str(r.run_id) for r in recent_failed))

# COMMAND ----------

queue_n = scalar(f"SELECT COUNT(*) FROM {QUEUE}")
if queue_n > 0:
    # A run died mid-batch: ids that already got an outcome row after they were
    # queued are done — drop them so the relaunch only redoes the remainder.
    spark.sql(f"""
    DELETE FROM {QUEUE} q
    WHERE EXISTS (
        SELECT 1 FROM {LEDGER} c
        JOIN openalex.pdf.grobid_processing_results g ON g.source_pdf_id = c.source_pdf_id
        WHERE c.source_pdf_id = q.source_pdf_id AND g.created_date >= c.queued_at
    )
    """)
    left = scalar(f"SELECT COUNT(*) FROM {QUEUE}")
    print(f"queue had {queue_n:,} undrained rows, {left:,} after trimming finished ids")
    if left > 0:
        launch_drain("resume undrained queue")
        dbutils.notebook.exit("resumed")
    # everything in the queue was already done — fall through and stage the next batch

# COMMAND ----------

last = scalar(f"SELECT COALESCE(MAX(batch), {FIRST_BATCH - 1}) FROM {LEDGER}")
batch = max(int(last) + 1, FIRST_BATCH)
spark.sql(f"""
INSERT INTO {LEDGER} (source_pdf_id, batch, queued_at)
{REMAINDER_SELECT}
SELECT source_pdf_id, {batch} AS batch, current_timestamp() AS queued_at
FROM remainder LIMIT {BATCH_SIZE}
""")
staged = scalar(f"SELECT COUNT(*) FROM {LEDGER} WHERE batch = {batch}")
if staged == 0:
    print("remainder exhausted — campaign complete; pause this job")
    dbutils.notebook.exit("complete")

spark.sql(f"""
INSERT INTO {QUEUE} (source_pdf_id)
SELECT c.source_pdf_id FROM {LEDGER} c
LEFT ANTI JOIN {QUEUE} q USING (source_pdf_id)
WHERE c.batch = {batch}
""")
print(f"staged batch {batch}: {staged:,} ids")
launch_drain(f"batch {batch}")
