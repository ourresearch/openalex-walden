# Databricks notebook source
# MAGIC %md
# MAGIC # SplitNotices — move notices off the work they are about (oxjob #2169)
# MAGIC
# MAGIC A notice (retraction, correction, erratum, expression of concern, comment: the help page's "Separate works") pinned to the
# MAGIC work of the article it is about: legacy pins frozen into `location_work_ids` at its 2026-08-12 seed, and #1902's merges of
# MAGIC 2026-11-03. Pins never re-resolve, so this is the registries' correction path (as RepointWorkIds): delete the notice records'
# MAGIC pins and the article work's map entries for the notice's DOI / PMID, and the nightly MapWorkIds re-files them: on the
# MAGIC notice's own work where the map has one, else one new work per notice (DOI first, then PMID). MapWorkIds' notice veto keeps
# MAGIC the legacy PMH crosswalk from pulling a notice's PMC copy back onto the article.
# MAGIC
# MAGIC Pairs: `openalex.works.notice_link_pairs` (MapWorkIds rebuilds it nightly), plus `extra_pairs_sql`: a SELECT of
# MAGIC `notice_ns, notice_id, target_ns, target_id` (ns `doi` = cleaned lowercase DOI, `pmid` = digits) for hand-checked pairs
# MAGIC no record declares. A record moves when it carries a notice's DOI or PMID and its work also holds a record carrying the
# MAGIC target's. Held: a record that itself carries a target id (`carries_target_id`: mislabelled, not a notice), and a record
# MAGIC sharing a DOI / PMID with a held one on the same work (`shares_key_with_held`: moving it would split the notice). Also held
# MAGIC without blocking anything: the article's own MAG record (`articles_own_mag_record`, its MAG id is the work's id), which
# MAGIC carries a notice's PMID because MAG linked the wrong one (1,031 on the 2026-11-11 stage). And every record of a work the
# MAGIC notice owns (`notice_owns_the_work`, ~950 works): its notice records were pinned there before any other record, or its
# MAGIC primary location is a notice and nothing else was pinned there earlier. There the article was merged into the notice's
# MAGIC work, so the article's records are the ones to move (W3026924908: an erratum's work since 2020 that #1902 merged its
# MAGIC 2018 article into); moving the notice off would hand the notice's id to the article.
# MAGIC
# MAGIC - `stage`    build `target_table` (one row per record) and print the plan; refuses a target with executed rows.
# MAGIC - `execute`  dry run; `confirm = yes`, outside 04:00-08:00 UTC: freeze `<target>_audit` and `<target>_map_audit`, then
# MAGIC              delete the pins, delete the article work's map rows keyed by a moving DOI (or by a moving PMID on a row with
# MAGIC              no DOI), and blank a moving PMID on its other rows.
# MAGIC - `verify`   the morning after: every record re-pinned, none back on the article's work.
# MAGIC - `repoint_citations`  the morning after: `work_references` edges citing a moved DOI / PMID on the article's work follow it.
# MAGIC
# MAGIC Undo: Delta history on `location_work_ids` and `work_id_map`, plus the two audit tables.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "execute", "verify", "repoint_citations"])
dbutils.widgets.text("target_table", "openalex.works.oxjob2169_notice_split_target")
dbutils.widgets.text("extra_pairs_sql", "")
dbutils.widgets.text("confirm", "no")
# a rehearsal points these at clones (MapWorkIds' own widgets take the same names)
dbutils.widgets.text("reg", "openalex.works.location_work_ids")
dbutils.widgets.text("map", "openalex.works.work_id_map")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table").strip()
EXTRA = dbutils.widgets.get("extra_pairs_sql").strip()
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_audit"
MAP_AUDIT = f"{TARGET}_map_audit"

REGISTRY = dbutils.widgets.get("reg").strip()
MAP = dbutils.widgets.get("map").strip()
LWT = "openalex.works.locations_w_types"
PAIRS = "openalex.works.notice_link_pairs"
REFS = "openalex.works.work_references"
WORKS = "openalex.works.openalex_works"
CLEAN = "NULLIF(regexp_replace(lower({}), '[^a-zA-Z0-9\\\\./-]', ''), '')"   # MapWorkIds' doi_clean, lowercased like the pairs

import datetime
import json

print({"mode": MODE, "target": TARGET, "extra_pairs_sql": bool(EXTRA), "confirm": CONFIRM, "reg": REGISTRY, "map": MAP})


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def outside_nightly_window():
    if 4 <= datetime.datetime.utcnow().hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run outside 04:00-08:00 UTC")

# COMMAND ----------

if MODE == "stage":
    if spark.catalog.tableExists(TARGET) and one(f"SELECT COUNT(*) AS n FROM {TARGET} WHERE executed_at IS NOT NULL")["n"]:
        raise Exception(f"{TARGET} has executed rows; stage into a new target_table")
    extra = (f"UNION SELECT notice_ns, notice_id, target_ns, target_id, 'extra' FROM ({EXTRA})" if EXTRA else "")
    spark.sql(f"""
      CREATE OR REPLACE TABLE {TARGET} AS
      WITH pairs AS (
        SELECT notice_ns, notice_id, target_ns, target_id, notice_type FROM {PAIRS}
        {extra}
      ),
      live AS (
        SELECT provenance, native_id_namespace, native_id, {CLEAN.format('merge_key.doi')} AS doi_clean,
               NULLIF(trim(merge_key.pmid), '') AS pmid, substr(title, 1, 120) AS title
        FROM {LWT}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id ORDER BY updated_date DESC) = 1
      ),
      ak AS (
        SELECT l.provenance, l.native_id_namespace, l.native_id, r.work_id, k.ns, k.id
        FROM live l
        JOIN {REGISTRY} r USING (provenance, native_id_namespace, native_id)
        LATERAL VIEW explode(array(named_struct('ns', 'doi', 'id', l.doi_clean), named_struct('ns', 'pmid', 'id', l.pmid))) e AS k
        WHERE r.work_id IS NOT NULL AND k.id IS NOT NULL
          AND (k.id IN (SELECT notice_id FROM pairs) OR k.id IN (SELECT target_id FROM pairs))
      ),
      tgt AS (
        SELECT DISTINCT ak.provenance, ak.native_id_namespace, ak.native_id, ak.work_id, p.notice_ns, p.notice_id
        FROM ak JOIN pairs p ON p.target_ns = ak.ns AND p.target_id = ak.id
      ),
      mv AS (
        SELECT n.provenance, n.native_id_namespace, n.native_id, n.work_id AS old_work_id, p.notice_type,
               concat(p.target_ns, ':', p.target_id) AS target
        FROM ak n
        JOIN pairs p ON p.notice_ns = n.ns AND p.notice_id = n.id
        LEFT SEMI JOIN tgt t ON t.work_id = n.work_id AND t.notice_ns = p.notice_ns AND t.notice_id = p.notice_id
      ),
      rec AS (
        SELECT provenance, native_id_namespace, native_id, old_work_id,
               array_sort(collect_set(notice_type)) AS notice_types, array_sort(collect_set(target)) AS targets
        FROM mv GROUP BY ALL
      ),
      held1 AS (
        SELECT DISTINCT r.provenance, r.native_id_namespace, r.native_id
        FROM rec r
        JOIN tgt t ON t.provenance = r.provenance AND t.native_id_namespace = r.native_id_namespace
                  AND t.native_id = r.native_id AND t.work_id = r.old_work_id
      ),
      held_keys AS (
        SELECT DISTINCT ak.work_id, ak.ns, ak.id
        FROM ak LEFT SEMI JOIN held1 h
          ON h.provenance = ak.provenance AND h.native_id_namespace = ak.native_id_namespace AND h.native_id = ak.native_id
      ),
      held2 AS (
        SELECT DISTINCT r.provenance, r.native_id_namespace, r.native_id
        FROM rec r
        JOIN ak ON ak.provenance = r.provenance AND ak.native_id_namespace = r.native_id_namespace AND ak.native_id = r.native_id
        JOIN held_keys hk ON hk.work_id = r.old_work_id AND hk.ns = ak.ns AND hk.id = ak.id
      ),
      notice_primary AS (
        SELECT DISTINCT w.id
        FROM {WORKS} w
        JOIN rec r ON r.old_work_id = w.id AND r.provenance = w.primary_location.provenance
                  AND lower(r.native_id) = lower(w.primary_location.native_id)
      ),
      ages AS (
        SELECT p.work_id,
               MIN(CASE WHEN r.native_id IS NOT NULL THEN p.openalex_created_dt END) AS notice_first,
               MIN(CASE WHEN r.native_id IS NULL THEN p.openalex_created_dt END) AS other_first
        FROM {REGISTRY} p
        LEFT JOIN rec r ON r.provenance = p.provenance AND r.native_id_namespace = p.native_id_namespace
                       AND r.native_id = p.native_id AND r.old_work_id = p.work_id
        WHERE p.work_id IN (SELECT old_work_id FROM rec)
        GROUP BY p.work_id
      ),
      notice_owns AS (
        SELECT a.work_id FROM ages a
        WHERE a.other_first IS NULL OR a.notice_first < a.other_first
           OR (a.work_id IN (SELECT id FROM notice_primary) AND NOT a.other_first < a.notice_first)
      ),
      t AS (
        SELECT r.provenance, r.native_id_namespace, r.native_id, l.doi_clean, l.pmid, l.title, r.old_work_id,
               r.notice_types, r.targets,
               CASE WHEN r.old_work_id IN (SELECT work_id FROM notice_owns) THEN 'notice_owns_the_work'
                    WHEN r.native_id_namespace = 'mag' AND TRY_CAST(r.native_id AS BIGINT) = r.old_work_id
                    THEN 'articles_own_mag_record'
                    WHEN EXISTS (SELECT 1 FROM held1 h WHERE h.provenance = r.provenance
                                   AND h.native_id_namespace = r.native_id_namespace AND h.native_id = r.native_id)
                    THEN 'carries_target_id'
                    WHEN EXISTS (SELECT 1 FROM held2 h WHERE h.provenance = r.provenance
                                   AND h.native_id_namespace = r.native_id_namespace AND h.native_id = r.native_id)
                    THEN 'shares_key_with_held' END AS hold_reason
        FROM rec r
        JOIN live l ON l.provenance = r.provenance AND l.native_id_namespace = r.native_id_namespace
                   AND l.native_id = r.native_id
      ),
      -- where the nightly will file each record: the map's work for its DOI, else its PMID, once the article's rows are gone
      d AS (
        SELECT t.provenance, t.native_id_namespace, t.native_id, MIN(m.id) AS v
        FROM t JOIN {MAP} m ON {CLEAN.format('m.doi')} = t.doi_clean AND m.id <> t.old_work_id
        GROUP BY ALL),
      p AS (
        SELECT t.provenance, t.native_id_namespace, t.native_id, MIN(m.id) AS v
        FROM t JOIN {MAP} m ON m.pmid = t.pmid AND m.id <> t.old_work_id
        GROUP BY ALL)
      SELECT t.*, COALESCE(d.v, p.v) AS planned_home, current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
      FROM t
      LEFT JOIN d ON d.provenance = t.provenance AND d.native_id_namespace = t.native_id_namespace AND d.native_id = t.native_id
      LEFT JOIN p ON p.provenance = t.provenance AND p.native_id_namespace = t.native_id_namespace AND p.native_id = t.native_id
    """)
    out = {
        "plan": one(f"""SELECT COUNT(*) AS records, COUNT_IF(hold_reason IS NULL) AS to_move,
                               COUNT(DISTINCT CASE WHEN hold_reason IS NULL THEN old_work_id END) AS article_works,
                               COUNT_IF(hold_reason IS NULL AND planned_home IS NOT NULL) AS to_notice_own_work,
                               COUNT_IF(hold_reason IS NULL AND planned_home IS NULL) AS to_new_work
                        FROM {TARGET}"""),
        "holds": rows(f"SELECT hold_reason, COUNT(*) AS records FROM {TARGET} WHERE hold_reason IS NOT NULL GROUP BY 1"),
        "by_provenance": rows(f"""SELECT provenance, COUNT(*) AS records FROM {TARGET} WHERE hold_reason IS NULL
                                  GROUP BY 1 ORDER BY 2 DESC"""),
        "by_type": rows(f"""SELECT notice_types[0] AS notice_type, COUNT(*) AS records FROM {TARGET} WHERE hold_reason IS NULL
                            GROUP BY 1 ORDER BY 2 DESC"""),
    }
    print(json.dumps(out, default=str, indent=1))
    dbutils.notebook.exit(json.dumps(out, default=str))

# COMMAND ----------

if MODE == "execute":
    scope = f"SELECT * FROM {TARGET} WHERE executed_at IS NULL AND hold_reason IS NULL"
    map_hits = f"""
      SELECT m.*, x.old_work_id AS audited_work_id,
             CASE WHEN {CLEAN.format('m.doi')} = x.doi_clean OR (NULLIF(m.doi, '') IS NULL AND m.pmid = x.pmid) THEN 'delete'
                  ELSE 'blank_pmid' END AS action
      FROM {MAP} m
      JOIN (SELECT DISTINCT old_work_id, doi_clean, pmid FROM ({scope})) x
        ON m.id = x.old_work_id AND ({CLEAN.format('m.doi')} = x.doi_clean OR m.pmid = x.pmid)"""
    plan = one(f"""SELECT COUNT(*) AS pins_to_delete, COUNT(DISTINCT old_work_id) AS article_works FROM ({scope})""")
    maps = rows(f"SELECT action, COUNT(*) AS map_rows FROM ({map_hits}) GROUP BY 1")
    print({**plan, "map_rows": maps})
    if plan["pins_to_delete"] == 0:
        dbutils.notebook.exit("nothing to split")
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({"dry_run": True, **plan, "map_rows": maps}, default=str))
    outside_nightly_window()
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: this split already ran (or clean it up first)")

    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT *, current_timestamp() AS audited_at, (SELECT MAX(id) FROM {MAP}) AS max_map_id_at_execute
                  FROM ({scope})""")
    spark.sql(f"CREATE OR REPLACE TABLE {MAP_AUDIT} AS {map_hits.replace(f'({scope})', AUDIT)}")
    n = one(f"SELECT COUNT(*) AS n FROM {AUDIT}")["n"]
    assert n == plan["pins_to_delete"], f"audit {n} != planned {plan['pins_to_delete']}"
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r
                         WHERE EXISTS (SELECT 1 FROM {AUDIT} x
                                       WHERE x.provenance = r.provenance AND x.native_id_namespace = r.native_id_namespace
                                         AND x.native_id = r.native_id AND x.old_work_id = r.work_id)""").collect()[0].num_affected_rows
    deleted = spark.sql(f"""DELETE FROM {MAP} m
                            WHERE EXISTS (SELECT 1 FROM {AUDIT} x WHERE x.old_work_id = m.id
                                          AND ({CLEAN.format('m.doi')} = x.doi_clean
                                               OR (NULLIF(m.doi, '') IS NULL AND m.pmid = x.pmid)))""").collect()[0].num_affected_rows
    blanked = spark.sql(f"""MERGE INTO {MAP} m
                            USING (SELECT DISTINCT old_work_id, pmid FROM {AUDIT} WHERE pmid IS NOT NULL) x
                              ON m.id = x.old_work_id AND m.pmid = x.pmid
                            WHEN MATCHED THEN UPDATE SET m.pmid = NULL, m.updated_date = current_timestamp()""").collect()[0].num_affected_rows
    spark.sql(f"""MERGE INTO {TARGET} t
                  USING (SELECT provenance, native_id_namespace, native_id FROM {AUDIT}) a
                    ON t.provenance = a.provenance AND t.native_id_namespace = a.native_id_namespace AND t.native_id = a.native_id
                  WHEN MATCHED THEN UPDATE SET t.executed_at = current_timestamp()""")
    out = {"audit_table": AUDIT, "map_audit_table": MAP_AUDIT, "audited": n, "pins_deleted": pins,
           "map_rows_deleted": deleted, "map_pmids_blanked": blanked}
    assert pins == n, f"pins {pins} != audited {n}"
    print(out)
    dbutils.notebook.exit(json.dumps(out))

# COMMAND ----------

if MODE == "verify":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: run execute first")
    res = one(f"""
        WITH j AS (
          SELECT x.*, r.work_id, r.work_id_source
          FROM {AUDIT} x
          LEFT JOIN {REGISTRY} r ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace
                                AND r.native_id = x.native_id)
        SELECT COUNT(*) AS audited,
               COUNT_IF(work_id IS NULL) AS still_pending,
               COUNT_IF(work_id = old_work_id) AS back_on_article_BAD,
               COUNT_IF(work_id = planned_home) AS on_notice_own_work_as_planned,
               COUNT_IF(work_id > max_map_id_at_execute) AS on_a_new_work,
               COUNT_IF(work_id IS NOT NULL AND work_id <> old_work_id AND work_id <> COALESCE(planned_home, -1)
                        AND work_id <= max_map_id_at_execute) AS on_another_existing_work,
               COUNT(DISTINCT old_work_id) AS article_works,
               COUNT(DISTINCT work_id) AS works_now_holding_them
        FROM j""")
    sources = rows(f"""SELECT r.work_id_source, COUNT(*) AS records FROM {AUDIT} x
                       JOIN {REGISTRY} r ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace
                                        AND r.native_id = x.native_id GROUP BY 1 ORDER BY 2 DESC""")
    out = {**res, "work_id_source": sources}
    print(json.dumps(out, default=str, indent=1))
    dbutils.notebook.exit(json.dumps(out, default=str))

# COMMAND ----------

if MODE == "repoint_citations":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: run execute first")
    moved = f"""
      SELECT x.old_work_id, x.doi_clean, x.pmid, MIN(r.work_id) AS new_work_id
      FROM {AUDIT} x
      JOIN {REGISTRY} r ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace
                       AND r.native_id = x.native_id
      WHERE r.work_id IS NOT NULL AND r.work_id <> x.old_work_id
      GROUP BY ALL"""
    by_doi = f"SELECT old_work_id, doi_clean, MIN(new_work_id) AS new_work_id FROM ({moved}) WHERE doi_clean IS NOT NULL GROUP BY ALL"
    by_pmid = f"SELECT old_work_id, pmid, MIN(new_work_id) AS new_work_id FROM ({moved}) WHERE pmid IS NOT NULL GROUP BY ALL"
    edges = one(f"""
        SELECT (SELECT COUNT(*) FROM {REFS} r JOIN ({by_doi}) v
                  ON {CLEAN.format('r.doi')} = v.doi_clean AND r.cited_work_id = v.old_work_id) AS by_doi,
               (SELECT COUNT(*) FROM {REFS} r JOIN ({by_pmid}) v
                  ON NULLIF(r.doi, '') IS NULL AND r.pmid = v.pmid AND r.cited_work_id = v.old_work_id) AS by_pmid""")
    print(edges)
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({"dry_run": True, **edges}))
    n_doi = spark.sql(f"""
      MERGE INTO {REFS} r USING ({by_doi}) v
        ON {CLEAN.format('r.doi')} = v.doi_clean AND r.cited_work_id = v.old_work_id
      WHEN MATCHED THEN UPDATE SET r.cited_work_id = v.new_work_id, r.updated_timestamp = current_timestamp()
    """).collect()[0].num_affected_rows
    n_pmid = spark.sql(f"""
      MERGE INTO {REFS} r USING ({by_pmid}) v
        ON NULLIF(r.doi, '') IS NULL AND r.pmid = v.pmid AND r.cited_work_id = v.old_work_id
      WHEN MATCHED THEN UPDATE SET r.cited_work_id = v.new_work_id, r.updated_timestamp = current_timestamp()
    """).collect()[0].num_affected_rows
    out = {"edges_repointed_by_doi": n_doi, "edges_repointed_by_pmid": n_pmid}
    print(out)
    dbutils.notebook.exit(json.dumps(out))
