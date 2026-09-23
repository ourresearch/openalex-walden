# Databricks notebook source
# MAGIC %md
# MAGIC # RepointDeadIdReferences — move citations off dead work ids onto the work that owns the DOI (oxjob #1256)
# MAGIC
# MAGIC `parse_work_references` fills `work_references.cited_work_id` once, from `work_id_map` by DOI, and
# MAGIC never revisits it. For years the map bound ~743K DOIs to ids that had already died (never ledgered,
# MAGIC so `TrackDeletedWorks` never purged them), so every reference to those DOIs resolved to a 404 id:
# MAGIC 11.5M references across 7.2M citing works counted for nobody while the live Crossref work sat at a
# MAGIC lower `cited_by_count` (`10.1016/0025-5416(87)90399-5`: 14 citing works on dead W2051967083, 0 on
# MAGIC W160712443). `SeedWorkIdMapDoiRows` repair (2026-09-23) deleted those bindings — the audit table
# MAGIC holds every dead id with its DOI, the repair target holds each DOI's anchor — so from now on new
# MAGIC references resolve right. This notebook is the one-time catch-up for the references already there.
# MAGIC
# MAGIC A reference moves only when it carries the DOI the anchor owns: `cited_work_id = dead id` AND
# MAGIC `clean(lower(doi)) = the dead binding's DOI` → `anchor_work_id`. References on a dead id with another
# MAGIC DOI or no DOI stay put. Every citing work whose list changes re-hashes (`updated_date` stamp) and so
# MAGIC does every anchor whose count changes, so the update runs in **waves by citing work** under the 7.5M
# MAGIC Guardrails limit: one wave per night, hand-run with `guardrails_override=true` on the next End 2 End.
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per reference edge to move):
# MAGIC - `stage`    build the edges from `audit_table` (dead ids) × `repair_target` (anchors) × `work_references`;
# MAGIC              assign waves of `wave_size` citing works (a citing work never straddles waves).
# MAGIC - `dry_run`  per-wave edges / citing works / anchors; top anchors by citations gained; samples.
# MAGIC - `execute`  `confirm = yes`, `wave = N`, outside 04:00-08:00 UTC and never while End 2 End is running:
# MAGIC              freeze `<target>_wave<N>_audit` (before-image), one MERGE, stamp `executed_at`. Once per wave.
# MAGIC              Undo = MERGE the audit's `old_cited_work_id` back.
# MAGIC - `verify`   executed edges still on a dead id (expect 0); anchors whose count rose; the example DOI.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify"])
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_dead_id_refs_target")
dbutils.widgets.text("audit_table", "openalex.works.oxjob1256_map_doi_seed_target_repair2_audit")
dbutils.widgets.text("repair_target", "openalex.works.oxjob1256_map_doi_seed_target_repair2")
dbutils.widgets.text("wave_size", "4000000")
dbutils.widgets.text("wave", "1")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table")
DEAD_AUDIT = dbutils.widgets.get("audit_table")
REPAIR = dbutils.widgets.get("repair_target")
WAVE_SIZE = int(dbutils.widgets.get("wave_size"))
WAVE = int(dbutils.widgets.get("wave"))
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_wave{WAVE}_audit"

REFS = "openalex.works.work_references"
REGISTRY = "openalex.works.location_work_ids"
WORKS = "openalex.works.openalex_works"
END2END_JOB_ID = 616701029470182
CLEAN = r"regexp_replace({col}, '[^a-zA-Z0-9\./-]', '')"   # byte-identical to MapWorkIds / the seeds

import datetime, json, time

SUMMARY = {"mode": MODE, "wave": WAVE}


def note(**kw):
    """Serverless notebook tasks return no stdout through the API; everything printed is also returned via notebook.exit."""
    SUMMARY.update(kw)
    print(kw)

print(dict(mode=MODE, target=TARGET, dead_audit=DEAD_AUDIT, repair=REPAIR, wave_size=WAVE_SIZE, wave=WAVE, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def end2end_active():
    from databricks.sdk import WorkspaceClient
    return [r.run_id for r in WorkspaceClient().jobs.list_runs(job_id=END2END_JOB_ID, active_only=True)]


def edges_sql():
    """One row per reference that cites a dead id under the DOI that dead id was bound to."""
    return f"""
    WITH dead AS (
      SELECT DISTINCT a.id AS dead_id, lower({CLEAN.format(col='a.doi')}) AS doi_clean
      FROM {DEAD_AUDIT} a WHERE NOT a.id_is_live AND a.doi IS NOT NULL
    ),
    bind AS (
      SELECT d.dead_id, d.doi_clean, t.anchor_work_id
      FROM dead d JOIN {REPAIR} t ON lower(t.doi_clean) = d.doi_clean AND t.action = 'repair'
      WHERE t.anchor_work_id <> d.dead_id
    )
    SELECT r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind,
           r.cited_work_id AS dead_id, b.doi_clean, b.anchor_work_id
    FROM {REFS} r
    JOIN bind b ON r.cited_work_id = b.dead_id AND lower({CLEAN.format(col='r.doi')}) = b.doi_clean
    WHERE r.doi IS NOT NULL
    """


def wave_pred():
    return f"t.wave = {WAVE} AND t.executed_at IS NULL"


def print_waves(table):
    for r in rows(f"""SELECT wave, COUNT(*) AS edges, COUNT(DISTINCT citing_work_id) AS citing_works,
                             COUNT(DISTINCT anchor_work_id) AS anchors, COUNT(DISTINCT dead_id) AS dead_ids,
                             SUM(CASE WHEN executed_at IS NOT NULL THEN 1 ELSE 0 END) AS executed
                      FROM {table} GROUP BY wave ORDER BY wave"""):
        print(r)

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  WITH e AS ({edges_sql()}),
                  cw AS (SELECT citing_work_id, CEIL(DENSE_RANK() OVER (ORDER BY citing_work_id) / {WAVE_SIZE}) AS wave
                         FROM (SELECT DISTINCT citing_work_id FROM e))
                  SELECT e.*, CAST(cw.wave AS INT) AS wave, current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM e JOIN cw ON cw.citing_work_id = e.citing_work_id""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (wave, citing_work_id)")
    note(staged_seconds=int(time.time() - t0), target=TARGET,
         totals=one(f"""SELECT COUNT(*) AS edges, COUNT(DISTINCT citing_work_id) AS citing_works, COUNT(DISTINCT anchor_work_id) AS anchors,
                               COUNT(DISTINCT dead_id) AS dead_ids, MAX(wave) AS waves FROM {TARGET}"""),
         waves=rows(f"SELECT wave, COUNT(*) AS edges, COUNT(DISTINCT citing_work_id) AS citing_works FROM {TARGET} GROUP BY 1 ORDER BY 1"))
    print_waves(TARGET)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    plan = one(f"""SELECT COUNT(*) AS edges, COUNT(DISTINCT t.citing_work_id) AS citing_works,
                          COUNT(DISTINCT t.anchor_work_id) AS anchors, COUNT(DISTINCT t.dead_id) AS dead_ids
                   FROM {TARGET} t WHERE {wave_pred()}""")
    note(**plan)
    print_waves(TARGET)
    if plan["edges"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": f"nothing to do for wave {WAVE}"}, default=str))
    print("anchors gaining the most citations in this wave:")
    top = rows(f"""SELECT t.anchor_work_id, COUNT(*) AS citations_gained, MAX(w.cited_by_count) AS cited_by_count_today, LEFT(MAX(w.title), 70) AS title
                   FROM {TARGET} t LEFT JOIN {WORKS} w ON w.id = t.anchor_work_id WHERE {wave_pred()}
                   GROUP BY t.anchor_work_id ORDER BY citations_gained DESC LIMIT 10""")
    for r in top: print("  ", r)
    note(top_anchors=top[:5])
    print("sample edges:")
    for r in rows(f"""SELECT t.citing_work_id, LEFT(wc.title, 50) AS citing_title, t.doi_clean, t.dead_id, t.anchor_work_id, LEFT(wa.title, 50) AS anchor_title
                      FROM {TARGET} t LEFT JOIN {WORKS} wc ON wc.id = t.citing_work_id LEFT JOIN {WORKS} wa ON wa.id = t.anchor_work_id
                      WHERE {wave_pred()} ORDER BY RAND(1) LIMIT 8"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("End 2 End rebuilds works 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed")

    t0 = time.time()
    # before-image of exactly the rows the MERGE will touch (undo = MERGE old_cited_work_id back on the same key)
    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind, r.doi,
                         r.cited_work_id AS old_cited_work_id, t.anchor_work_id AS new_cited_work_id, current_timestamp() AS audited_at
                  FROM {REFS} r
                  JOIN (SELECT DISTINCT citing_work_id, dead_id, doi_clean, anchor_work_id FROM {TARGET} t WHERE {wave_pred()}) t
                    ON r.citing_work_id = t.citing_work_id AND r.cited_work_id = t.dead_id
                   AND lower({CLEAN.format(col='r.doi')}) = t.doi_clean""")
    n_audit = one(f"SELECT COUNT(*) AS n FROM {AUDIT}")["n"]
    moved = spark.sql(f"""MERGE INTO {REFS} r
                          USING (SELECT DISTINCT citing_work_id, dead_id, doi_clean, anchor_work_id FROM {TARGET} t WHERE {wave_pred()}) s
                            ON r.citing_work_id = s.citing_work_id AND r.cited_work_id = s.dead_id
                           AND lower({CLEAN.format(col='r.doi')}) = s.doi_clean
                          WHEN MATCHED THEN UPDATE SET r.cited_work_id = s.anchor_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE {wave_pred()}")
    note(executed_seconds=int(time.time() - t0), audit=AUDIT, audited=n_audit, edges_repointed=moved)
    print_waves(TARGET)

# COMMAND ----------

if MODE == "verify":
    live = f"(SELECT DISTINCT work_id FROM {REGISTRY} WHERE work_id IS NOT NULL)"
    note(executed_edges_still_on_dead_id=one(f"""
        SELECT COUNT(*) AS n FROM {REFS} r JOIN {TARGET} t
          ON r.citing_work_id = t.citing_work_id AND r.native_id = t.native_id AND r.native_id_namespace = t.native_id_namespace AND r.ref_ind = t.ref_ind
        WHERE t.executed_at IS NOT NULL AND r.cited_work_id = t.dead_id""")["n"])
    note(references_on_any_dead_id_now=one(f"""
        SELECT COUNT(*) AS n FROM {REFS} r
        LEFT ANTI JOIN {live} p ON p.work_id = r.cited_work_id WHERE r.cited_work_id IS NOT NULL""")["n"])
    print_waves(TARGET)
    print("example: 10.1016/0025-5416(87)90399-5 — dead W2051967083 vs anchor W160712443")
    for r in rows(f"SELECT cited_work_id, COUNT(*) AS refs FROM {REFS} WHERE cited_work_id IN (2051967083, 160712443) GROUP BY 1"):
        print("  ", r)
    for r in rows(f"SELECT id, cited_by_count, updated_date FROM {WORKS} WHERE id = 160712443"):
        print("  ", r)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
