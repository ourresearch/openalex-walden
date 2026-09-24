# Databricks notebook source
# MAGIC %md
# MAGIC # MergeIdenticalKeyWorks — collapse works that share a byte-identical `title_author` key (oxjob #1256 step four)
# MAGIC
# MAGIC The identical-key defect class: a `title_author` key (> 20 chars) carried by the pinned records of 2–3
# MAGIC works where at most ONE of them carries a DOI. The keyless side(s) minted as fresh works because the
# MAGIC map did not hold the key for the DOI-bearing work (leak closed 2026-09-22, walden `3631b877`; stock
# MAGIC 3,985,433 keys / 4,084,352 excess works on 2026-09-24, 86 % uncited, 77 % repo-only). The two-DOI
# MAGIC groups are #880's policy class (versioned DOIs, same-titled items) and are NOT in scope.
# MAGIC
# MAGIC **Winner** = the DOI-bearing work, else the lowest id. **Losers** = the rest. The merge is the
# MAGIC registries' sanctioned correction path (`RepointWorkIds` header): DELETE the loser's registry pins and
# MAGIC its `work_id_map` rows, then let the nightly `MapWorkIds` re-resolve the loser's records — keyless, so
# MAGIC the title tier binds them to the winner (after the delete the key's only id is the winner). The loser
# MAGIC ends the night with no pins → `TrackDeletedWorks` ledgers it (404, deleted_ids.csv, ES delete). The
# MAGIC morning after, `repoint_citations` moves `work_references.cited_work_id` loser → winner by id pair.
# MAGIC
# MAGIC **Holds** (never executed; `hold_reason` in the target): `year_gap` (|loser year − winner year| >
# MAGIC `max_year_gap`, both known — reprint / edition / thesis-vs-article = version-of, not this job),
# MAGIC `short_key` (key ≤ `short_key_len` chars and NOT a known same year), `too_many_locations` (loser has
# MAGIC > `max_locations` locations), `cited_over` (loser cited ≥ `hold_cited_over`: individual sign-off),
# MAGIC `chained` (the loser is itself a winner of another key — merge chains are resolved after the first pass).
# MAGIC Every cited loser (≥ 1) is written to `<target>_cited_review` for the CSV.
# MAGIC
# MAGIC **Waves** are assigned per KEY (all losers of a key in one wave, so MIN(id) over the key is the winner
# MAGIC the next morning), uncited keys first, `wave_size` losers per wave. Nightly budget: TrackDeletedWorks
# MAGIC refuses > 0.5 % of live works deleted (~2.38M on 2026-09-24) and Guardrails > 7.5M stamps (losers +
# MAGIC winners + normal churn) → default 1,500,000.
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per loser):
# MAGIC - `stage`     build the class from `locations_mapped` + `openalex_works`, apply holds, assign waves.
# MAGIC - `dry_run`   `wave = N`: losers, keys, winners, pins + map rows to delete, cited losers; samples.
# MAGIC - `execute`   `confirm = yes`, `wave = N`, outside 04:00-08:00 UTC, never while End 2 End runs: freeze
# MAGIC               `<target>_wave<N>_audit` (the pins and map rows deleted, in full), the two DELETEs, stamp.
# MAGIC               Undo: re-INSERT the audit rows (pins + map rows) — the nightly has not run yet.
# MAGIC - `verify`    the morning after: every audited pin re-pinned; landed on the winner / elsewhere / NULL;
# MAGIC               losers with 0 pins; losers ledgered.
# MAGIC - `repoint_citations`  `wave = N`, `confirm = yes`, after `verify` is clean: `<target>_wave<N>_refs_audit`
# MAGIC               (before-image) then UPDATE `work_references.cited_work_id` loser → winner.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify", "repoint_citations"])
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_identical_key_merge_target")
dbutils.widgets.text("wave_size", "1500000")
dbutils.widgets.text("wave", "1")
dbutils.widgets.text("max_year_gap", "1")
dbutils.widgets.text("short_key_len", "30")
dbutils.widgets.text("max_locations", "50")
dbutils.widgets.text("hold_cited_over", "100")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table")
WAVE_SIZE = int(dbutils.widgets.get("wave_size"))
WAVE = int(dbutils.widgets.get("wave"))
MAX_YEAR_GAP = int(dbutils.widgets.get("max_year_gap"))
SHORT_KEY_LEN = int(dbutils.widgets.get("short_key_len"))
MAX_LOCATIONS = int(dbutils.widgets.get("max_locations"))
HOLD_CITED_OVER = int(dbutils.widgets.get("hold_cited_over"))
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_wave{WAVE}_audit"
REFS_AUDIT = f"{TARGET}_wave{WAVE}_refs_audit"
REVIEW = f"{TARGET}_cited_review"

LM = "openalex.works.locations_mapped"
REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
WORKS = "openalex.works.openalex_works"
REFS = "openalex.works.work_references"
LEDGER = "openalex.works.deleted_works"
END2END_JOB_ID = 616701029470182

import datetime, json, time

SUMMARY = {"mode": MODE, "wave": WAVE}


def note(**kw):
    """Serverless notebook tasks return no stdout through the API; everything printed is also returned via notebook.exit."""
    SUMMARY.update(kw)
    print(kw)

print(dict(mode=MODE, target=TARGET, wave_size=WAVE_SIZE, wave=WAVE, max_year_gap=MAX_YEAR_GAP,
           short_key_len=SHORT_KEY_LEN, max_locations=MAX_LOCATIONS, hold_cited_over=HOLD_CITED_OVER, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def end2end_active():
    from databricks.sdk import WorkspaceClient
    return [r.run_id for r in WorkspaceClient().jobs.list_runs(job_id=END2END_JOB_ID, active_only=True)]


def class_sql():
    """One row per loser with its winner and every field the holds need."""
    return f"""
    WITH k AS (
      SELECT merge_key.title_author AS ta, work_id,
             MAX(NULLIF(merge_key.doi, '') IS NOT NULL) AS has_doi,
             MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only,
             COUNT(*) AS n_locations
      FROM {LM}
      WHERE work_id IS NOT NULL AND merge_key.title_author IS NOT NULL AND LENGTH(merge_key.title_author) > 20
      GROUP BY 1, 2
    ),
    g AS (
      SELECT ta, COUNT(*) AS n_ids, SUM(CASE WHEN has_doi THEN 1 ELSE 0 END) AS n_doi
      FROM k GROUP BY ta HAVING COUNT(*) BETWEEN 2 AND 3
    ),
    sides AS (
      SELECT k.ta, g.n_ids, g.n_doi, k.work_id, k.has_doi, k.repo_only, k.n_locations,
             w.publication_year AS yr, COALESCE(w.cited_by_count, 0) AS cites,
             ROW_NUMBER() OVER (PARTITION BY k.ta ORDER BY k.has_doi DESC, k.work_id) AS rn
      FROM g JOIN k ON k.ta = g.ta
      LEFT JOIN {WORKS} w ON w.id = k.work_id
      WHERE g.n_doi <= 1
    ),
    winners AS (SELECT ta, work_id AS winner_work_id, yr AS winner_yr, has_doi AS winner_has_doi FROM sides WHERE rn = 1),
    losers AS (
      SELECT s.ta, s.n_ids, s.n_doi, w.winner_work_id, w.winner_yr, w.winner_has_doi,
             s.work_id AS loser_work_id, s.yr AS loser_yr, s.cites AS loser_cites, s.repo_only AS loser_repo_only,
             s.n_locations AS loser_locations, LENGTH(s.ta) AS key_len
      FROM sides s JOIN winners w ON w.ta = s.ta WHERE s.rn > 1
    )
    SELECT l.*,
           CASE WHEN l.loser_cites >= {HOLD_CITED_OVER} THEN 'cited_over'
                WHEN l.loser_yr IS NOT NULL AND l.winner_yr IS NOT NULL AND ABS(l.loser_yr - l.winner_yr) > {MAX_YEAR_GAP} THEN 'year_gap'
                WHEN l.key_len <= {SHORT_KEY_LEN} AND NOT (l.loser_yr IS NOT NULL AND l.loser_yr = l.winner_yr) THEN 'short_key'
                WHEN l.loser_locations > {MAX_LOCATIONS} THEN 'too_many_locations'
                WHEN EXISTS (SELECT 1 FROM winners x WHERE x.winner_work_id = l.loser_work_id) THEN 'chained'
                END AS hold_reason
    FROM losers l
    """


def wave_pred():
    return f"t.wave = {WAVE} AND t.hold_reason IS NULL AND t.executed_at IS NULL"


def print_waves(table):
    for r in rows(f"""SELECT wave, hold_reason, COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, COUNT(DISTINCT winner_work_id) AS winners,
                             SUM(loser_locations) AS locations, SUM(CASE WHEN loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                             SUM(loser_cites) AS cites, SUM(CASE WHEN executed_at IS NOT NULL THEN 1 ELSE 0 END) AS executed
                      FROM {table} GROUP BY 1, 2 ORDER BY 1 NULLS LAST, 2 NULLS FIRST"""):
        print(r)

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  WITH c AS ({class_sql()}),
                  keys AS (
                    SELECT ta, MAX(CASE WHEN hold_reason IS NOT NULL THEN 1 ELSE 0 END) AS held,
                           MAX(loser_cites) AS max_cites, COUNT(*) AS n_losers
                    FROM c GROUP BY ta),
                  ordered AS (
                    -- uncited keys first, then by citations; all losers of a key share a wave; held keys get no wave
                    SELECT ta, held, SUM(n_losers) OVER (ORDER BY max_cites, ta ROWS UNBOUNDED PRECEDING) AS cum_losers
                    FROM keys WHERE held = 0)
                  SELECT c.*, CASE WHEN o.ta IS NOT NULL THEN CAST(CEIL(o.cum_losers / {WAVE_SIZE}) AS INT) END AS wave,
                         current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM c LEFT JOIN ordered o ON o.ta = c.ta""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (wave, ta)")
    spark.sql(f"""CREATE OR REPLACE TABLE {REVIEW} AS
                  SELECT t.wave, t.hold_reason, t.loser_work_id, t.loser_cites, t.winner_work_id, t.ta,
                         LEFT(wl.title, 120) AS loser_title, LEFT(ww.title, 120) AS winner_title, t.loser_yr, t.winner_yr
                  FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                  WHERE t.loser_cites > 0 ORDER BY t.loser_cites DESC""")
    note(staged_seconds=int(time.time() - t0), target=TARGET, cited_review=REVIEW,
         totals=one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, SUM(CASE WHEN hold_reason IS NULL THEN 1 ELSE 0 END) AS executable,
                               MAX(wave) AS waves FROM {TARGET}"""),
         holds=rows(f"SELECT hold_reason, COUNT(*) AS losers FROM {TARGET} WHERE hold_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"),
         waves=rows(f"SELECT wave, COUNT(*) AS losers, SUM(loser_cites) AS cites FROM {TARGET} WHERE wave IS NOT NULL GROUP BY 1 ORDER BY 1"))
    print_waves(TARGET)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    plan = one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT t.ta) AS keys, COUNT(DISTINCT t.winner_work_id) AS winners,
                          SUM(t.loser_locations) AS locations, SUM(CASE WHEN t.loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                          SUM(t.loser_cites) AS cites_to_move, MAX(t.loser_cites) AS max_cites
                   FROM {TARGET} t WHERE {wave_pred()}""")
    dels = one(f"""SELECT (SELECT COUNT(*) FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = r.work_id)) AS pins_to_delete,
                          (SELECT COUNT(*) FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = m.id)) AS map_rows_to_delete""")
    note(**plan, **dels)
    print_waves(TARGET)
    if plan["losers"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": f"nothing to do for wave {WAVE}"}, default=str))
    print("sample merges:")
    for r in rows(f"""SELECT t.loser_work_id, LEFT(wl.title, 50) AS loser_title, t.loser_yr, t.loser_cites, t.loser_repo_only,
                             t.winner_work_id, LEFT(ww.title, 50) AS winner_title, t.winner_yr, t.winner_has_doi
                      FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                      WHERE {wave_pred()} ORDER BY RAND(1) LIMIT 10"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed")

    t0 = time.time()
    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT 'pin' AS kind, t.loser_work_id, t.winner_work_id, t.ta,
                         r.provenance, r.native_id_namespace, r.native_id, r.work_id_source, r.openalex_created_dt, r.openalex_updated_dt, r.seeded_dt, r.seeded_from,
                         CAST(NULL AS STRING) AS doi, CAST(NULL AS STRING) AS pmid, CAST(NULL AS STRING) AS arxiv, CAST(NULL AS STRING) AS title_author,
                         CAST(NULL AS DATE) AS created_date, CAST(NULL AS TIMESTAMP) AS updated_date, current_timestamp() AS audited_at
                  FROM {TARGET} t JOIN {REGISTRY} r ON r.work_id = t.loser_work_id WHERE {wave_pred()}
                  UNION ALL
                  SELECT 'map', t.loser_work_id, t.winner_work_id, t.ta,
                         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         m.doi, m.pmid, m.arxiv, m.title_author, m.created_date, m.updated_date, current_timestamp()
                  FROM {TARGET} t JOIN {MAP} m ON m.id = t.loser_work_id WHERE {wave_pred()}""")
    n = one(f"SELECT SUM(CASE WHEN kind = 'pin' THEN 1 ELSE 0 END) AS pins, SUM(CASE WHEN kind = 'map' THEN 1 ELSE 0 END) AS map_rows FROM {AUDIT}")
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id)""").collect()[0].num_affected_rows
    maprows = spark.sql(f"""DELETE FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'map' AND a.loser_work_id = m.id)""").collect()[0].num_affected_rows
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE {wave_pred()}")
    note(executed_seconds=int(time.time() - t0), audit=AUDIT, audited_pins=n["pins"], audited_map_rows=n["map_rows"],
         pins_deleted=pins, map_rows_deleted=maprows)
    assert pins == n["pins"], f"pins deleted {pins} != audited {n['pins']}"
    print_waves(TARGET)

# COMMAND ----------

if MODE == "verify":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    note(landed=rows(f"""
        SELECT CASE WHEN r.work_id IS NULL THEN 'not re-pinned yet / NULL'
                    WHEN r.work_id = a.winner_work_id THEN 'on the winner'
                    WHEN r.work_id = a.loser_work_id THEN 'BACK ON THE LOSER'
                    ELSE 'elsewhere' END AS landed, COUNT(*) AS pins
        FROM {AUDIT} a LEFT JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
        WHERE a.kind = 'pin' GROUP BY 1 ORDER BY 2 DESC"""))
    note(losers=one(f"""
        SELECT COUNT(DISTINCT t.loser_work_id) AS executed_losers,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {REGISTRY} p WHERE p.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_still_pinned,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {LEDGER} d WHERE d.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_ledgered
        FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL"""))
    print("elsewhere samples (a loser record that re-resolved to a third work):")
    for r in rows(f"""SELECT a.provenance, a.native_id, a.loser_work_id, a.winner_work_id, r.work_id AS landed_on, r.work_id_source
                      FROM {AUDIT} a JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
                      WHERE a.kind = 'pin' AND r.work_id IS NOT NULL AND r.work_id NOT IN (a.winner_work_id, a.loser_work_id) LIMIT 8"""):
        print("  ", r)

# COMMAND ----------

if MODE == "repoint_citations":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    if spark.catalog.tableExists(REFS_AUDIT):
        raise Exception(f"{REFS_AUDIT} exists: citations for wave {WAVE} were already repointed")
    pairs = f"(SELECT DISTINCT loser_work_id, winner_work_id FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL)"
    still = one(f"SELECT COUNT(*) AS n FROM {pairs} p WHERE EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = p.loser_work_id)")["n"]
    plan = one(f"""SELECT COUNT(*) AS edges, COUNT(DISTINCT r.citing_work_id) AS citing_works, COUNT(DISTINCT r.cited_work_id) AS losers_cited
                   FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id""")
    note(losers_still_pinned=still, **plan)
    if still > 0:
        raise Exception(f"{still} losers still hold registry pins: run verify and wait for the nightly before repointing citations")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to repoint citations")
    spark.sql(f"""CREATE TABLE {REFS_AUDIT} AS
                  SELECT r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind, r.cited_work_id AS old_cited_work_id,
                         p.winner_work_id AS new_cited_work_id, current_timestamp() AS audited_at
                  FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id""")
    moved = spark.sql(f"""MERGE INTO {REFS} r USING {pairs} p ON r.cited_work_id = p.loser_work_id
                          WHEN MATCHED THEN UPDATE SET r.cited_work_id = p.winner_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    note(refs_audit=REFS_AUDIT, edges_repointed=moved)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
