# Databricks notebook source
# MAGIC %md
# MAGIC # SeedWorkIdMapDoiRows — give every DOI-anchored work its DOI row in `work_id_map` (oxjob #1256)
# MAGIC
# MAGIC `MapWorkIds` resolves the DOI tier against `work_id_map` (cleaned doi → MIN(id)), and
# MAGIC `parse_work_references` resolves `cited_work_id` the same way. Both are only as good as the
# MAGIC map's DOI coverage, and the map was seeded with titles for a large slice of works: on
# MAGIC 2026-09-21, 24.35M crossref DOI anchors had no DOI row at all and 827K had a row that points
# MAGIC at another work (usually a repo ghost that minted later carrying the same DOI). Every record
# MAGIC that then arrives with one of those DOIs is `routing_key_unseen` and mints a duplicate, and
# MAGIC every reference to one of them stays unresolved or lands on the ghost (#1118 Cairn: 20K
# MAGIC ghosts in two nights; corpus: 460K DOIs on two works and growing 1-3K/day).
# MAGIC
# MAGIC This notebook makes the map agree with the registry: for each cleaned DOI carried by a
# MAGIC pinned anchor of the chosen provenances, the map must have a row and MIN(id) over that DOI
# MAGIC must be the anchor's work. `insert` adds the missing row; `repair` deletes the rows whose
# MAGIC MIN(id) is another work and re-adds them without their DOI (their pmid / arxiv /
# MAGIC title_author verdicts are untouched), then adds the anchor's row. Registry discipline:
# MAGIC insert-only, plus this audited delete + reinsert sweep (`MapWorkIds` header). Nothing here
# MAGIC moves a pinned anchor or a citation: existing ghosts keep their locations and their
# MAGIC references until the repoint step (RepointWorkIds wave_e + repoint_citations). What changes
# MAGIC is every resolution from tomorrow on, and the NULL `cited_work_id` references the nightly
# MAGIC DOI MERGE has been unable to fill.
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per cleaned DOI of the chosen provenances):
# MAGIC - `stage`    census of live anchors vs the map: `ok` / `insert` / `repair` / `skip_multi_anchor`
# MAGIC              (a DOI pinned to 2+ works within the chosen provenances is never touched).
# MAGIC - `dry_run`  what `execute` would do: rows to insert, map rows to delete and what they carry.
# MAGIC - `execute`  `confirm = yes` only, outside 04:00-08:00 UTC: freeze `<target>_audit` (the map rows
# MAGIC              deleted), then the three statements. Refuses to run twice. Undo: the audit table +
# MAGIC              60-day Delta retention on the map.
# MAGIC - `verify`   the morning after: re-census live; expect `insert` and `repair` at ~0 (anchors that
# MAGIC              arrived after the stage are the residue) and the example DOIs resolving right.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify"])
dbutils.widgets.text("provenances", "crossref")
dbutils.widgets.text("apply", "insert,repair")
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_map_doi_seed_target")
dbutils.widgets.text("hold_cited_over", "1")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
PROVENANCES = [p.strip() for p in dbutils.widgets.get("provenances").split(",") if p.strip()]
APPLY = [a.strip() for a in dbutils.widgets.get("apply").split(",") if a.strip()]
TARGET = dbutils.widgets.get("target_table")
# A repair whose displaced id is cited >= this many times is HELD: citations followed the map row to the
# ghost (stage 2026-09-21: 24,712 cites on repo-only W4213439731 vs 1,390 on the crossref anchor), so
# re-pointing the DOI alone would send new citations to the anchor while the old ones stay on the ghost.
# Those pairs need the merge + work_references UPDATE (RepointWorkIds wave_e / repoint_citations), not a seed.
HOLD_CITED_OVER = int(dbutils.widgets.get("hold_cited_over"))
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_audit"

REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
LWT = "openalex.works.locations_w_types"
WORKS = "openalex.works.openalex_works"

# byte-identical to MapWorkIds' doi_clean / map_doi_lookup key
CLEAN = r"regexp_replace({col}, '[^a-zA-Z0-9\./-]', '')"
PROV_LIST = ", ".join(f"'{p}'" for p in PROVENANCES)
assert set(APPLY) <= {"insert", "repair"}, APPLY

import datetime, time

print(dict(mode=MODE, provenances=PROVENANCES, apply=APPLY, target=TARGET, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def census_sql():
    """One row per cleaned DOI carried by a pinned anchor of the chosen provenances, with the map's verdict."""
    return f"""
    WITH lwt AS (
      SELECT provenance, native_id_namespace, native_id, merge_key.doi AS doi,
             NULLIF({CLEAN.format(col='merge_key.doi')}, '') AS doi_clean
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id
                                         ORDER BY updated_date DESC) AS rn
            FROM {LWT} WHERE provenance IN ({PROV_LIST}))
      WHERE rn = 1 AND NULLIF(merge_key.doi, '') IS NOT NULL
    ),
    anchors AS (
      SELECT l.doi_clean, MIN(l.doi) AS doi, r.work_id, l.provenance
      FROM lwt l
      JOIN {REGISTRY} r
        ON r.provenance = l.provenance AND r.native_id_namespace = l.native_id_namespace AND r.native_id = l.native_id
      WHERE r.work_id IS NOT NULL AND l.doi_clean IS NOT NULL
      GROUP BY l.doi_clean, r.work_id, l.provenance
    ),
    per_doi AS (
      SELECT doi_clean, MIN(doi) AS doi, COUNT(DISTINCT work_id) AS n_anchor_works, MIN(work_id) AS anchor_work_id,
             array_join(array_sort(collect_set(provenance)), '+') AS provenances
      FROM anchors GROUP BY doi_clean
    ),
    map AS (
      SELECT {CLEAN.format(col='doi')} AS doi_clean, MIN(id) AS map_min_id, collect_set(id) AS map_ids
      FROM {MAP} WHERE doi IS NOT NULL AND doi <> '' GROUP BY 1
    )
    SELECT p.doi_clean, p.doi, p.anchor_work_id, p.provenances, p.n_anchor_works, m.map_ids, m.map_min_id,
           CASE WHEN p.n_anchor_works > 1 THEN 'skip_multi_anchor'
                WHEN m.doi_clean IS NULL THEN 'insert'
                WHEN m.map_min_id <> p.anchor_work_id THEN 'repair'
                ELSE 'ok' END AS action,
           w.cited_by_count AS displaced_cited_by_count
    FROM per_doi p
    LEFT JOIN map m ON m.doi_clean = p.doi_clean
    LEFT JOIN {WORKS} w ON w.id = m.map_min_id AND m.map_min_id <> p.anchor_work_id
    """


def print_census(table):
    for r in rows(f"""
        SELECT action, provenances, COUNT(*) AS dois,
               SUM(CASE WHEN displaced_cited_by_count > 0 THEN 1 ELSE 0 END) AS displaced_ids_cited,
               MAX(displaced_cited_by_count) AS max_displaced_cited
        FROM {table} GROUP BY 1, 2 ORDER BY 1, 3 DESC"""):
        print(r)


# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  SELECT *, current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM ({census_sql()})""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (action, doi_clean)")
    print(f"staged in {int(time.time() - t0)}s -> {TARGET}")
    print_census(TARGET)
    print("most-cited displaced ids (the map currently sends this DOI to them, not to the anchor's work):")
    for r in rows(f"""SELECT doi_clean, anchor_work_id, map_min_id, displaced_cited_by_count
                      FROM {TARGET} WHERE action = 'repair' ORDER BY displaced_cited_by_count DESC NULLS LAST LIMIT 20"""):
        print("  ", r)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    apply_list = ", ".join(f"'{a}'" for a in APPLY)
    scope = (f"{TARGET} t WHERE t.action IN ({apply_list}) AND t.executed_at IS NULL "
             f"AND COALESCE(t.displaced_cited_by_count, 0) < {HOLD_CITED_OVER}")
    held = one(f"""SELECT COUNT(*) AS repairs_held_displaced_id_cited, SUM(displaced_cited_by_count) AS cites_on_held_ghosts
                   FROM {TARGET} t WHERE t.action = 'repair' AND t.executed_at IS NULL
                   AND COALESCE(t.displaced_cited_by_count, 0) >= {HOLD_CITED_OVER}""")
    print({**held, "hold_cited_over": HOLD_CITED_OVER})
    plan = one(f"""
        SELECT SUM(CASE WHEN t.action = 'insert' THEN 1 ELSE 0 END) AS rows_to_insert_new,
               SUM(CASE WHEN t.action = 'repair' THEN 1 ELSE 0 END) AS dois_to_repair,
               COUNT(DISTINCT t.anchor_work_id) AS anchor_works,
               SUM(CASE WHEN t.action = 'repair' AND t.displaced_cited_by_count > 0 THEN 1 ELSE 0 END) AS repairs_where_displaced_id_is_cited
        FROM {scope}""")
    # A displaced id with no pin in the registry is a dead work (merged away or deleted; 743K of the 866K
    # repairs on 2026-09-21): its row is dropped outright, other keys included -- a dead id can never be a
    # correct verdict (TrackDeletedWorks policy: a dead id never resolves again). Live ids keep their
    # pmid / arxiv / title_author verdicts and lose only the DOI.
    to_delete = one(f"""
        SELECT COUNT(*) AS map_rows_to_delete,
               SUM(CASE WHEN p.work_id IS NULL THEN 1 ELSE 0 END) AS rows_dropped_dead_id,
               SUM(CASE WHEN p.work_id IS NOT NULL
                         AND (m.pmid IS NOT NULL OR m.arxiv IS NOT NULL OR m.title_author IS NOT NULL) THEN 1 ELSE 0 END) AS rows_reinserted_without_doi,
               COUNT(DISTINCT m.id) AS ids_losing_this_doi
        FROM {MAP} m
        LEFT JOIN (SELECT DISTINCT work_id FROM {REGISTRY} WHERE work_id IS NOT NULL) p ON p.work_id = m.id
        WHERE EXISTS (SELECT 1 FROM {scope} AND t.action = 'repair'
                      AND t.doi_clean = {CLEAN.format(col='m.doi')})""")
    print({**plan, **to_delete})
    if (plan["rows_to_insert_new"] or 0) + (plan["dois_to_repair"] or 0) == 0:
        dbutils.notebook.exit("nothing to do")
    print("sample repairs:")
    for r in rows(f"""SELECT doi_clean, anchor_work_id, provenances, map_ids, displaced_cited_by_count
                      FROM {scope} AND t.action = 'repair' ORDER BY displaced_cited_by_count DESC NULLS LAST LIMIT 10"""):
        print("  ", r)
    print("sample inserts:")
    for r in rows(f"SELECT doi_clean, anchor_work_id, provenances FROM {scope} AND t.action = 'insert' LIMIT 10"):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: already executed against this target (stage again into a new target_table)")

    t0 = time.time()
    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT m.*, (p.work_id IS NOT NULL) AS id_is_live, current_timestamp() AS deleted_at
                  FROM {MAP} m
                  LEFT JOIN (SELECT DISTINCT work_id FROM {REGISTRY} WHERE work_id IS NOT NULL) p ON p.work_id = m.id
                  WHERE EXISTS (SELECT 1 FROM {scope} AND t.action = 'repair'
                                AND t.doi_clean = {CLEAN.format(col='m.doi')})""")
    audited = one(f"SELECT COUNT(*) AS n, SUM(CASE WHEN id_is_live THEN 0 ELSE 1 END) AS dead FROM {AUDIT}")
    print(f"audit frozen: {audited['n']:,} map rows ({audited['dead']:,} on dead ids) -> {AUDIT}")

    if "repair" in APPLY:
        spark.sql(f"""DELETE FROM {MAP} m
                      WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.id = m.id AND a.doi = m.doi
                                    AND a.pmid <=> m.pmid AND a.arxiv <=> m.arxiv AND a.title_author <=> m.title_author)""")
        spark.sql(f"""INSERT INTO {MAP} (id, doi, pmid, arxiv, title_author, created_date, updated_date)
                      SELECT id, NULL, pmid, arxiv, title_author, created_date, current_timestamp()
                      FROM {AUDIT}
                      WHERE id_is_live AND (pmid IS NOT NULL OR arxiv IS NOT NULL OR title_author IS NOT NULL)""")
        print(f"repair: deleted {audited['n']:,} rows, re-inserted the non-DOI verdicts of the live ids")

    spark.sql(f"""INSERT INTO {MAP} (id, doi, pmid, arxiv, title_author, created_date, updated_date)
                  SELECT t.anchor_work_id, t.doi, NULL, NULL, NULL, current_date(), current_timestamp()
                  FROM {scope}""")
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE t.action IN ({apply_list}) AND t.executed_at IS NULL")
    print(f"executed in {int(time.time() - t0)}s:",
          rows(f"SELECT action, COUNT(*) AS n FROM {TARGET} WHERE executed_at IS NOT NULL GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

if MODE == "verify":
    t0 = time.time()
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW census AS {census_sql()}")
    print(f"live census ({int(time.time() - t0)}s):")
    print_census("census")
    if spark.catalog.tableExists(TARGET):
        print("executed rows whose DOI still does not resolve to the anchor (expect 0):")
        for r in rows(f"""SELECT c.action, COUNT(*) AS n FROM {TARGET} t JOIN census c ON c.doi_clean = t.doi_clean
                          WHERE t.executed_at IS NOT NULL AND c.action <> 'ok' GROUP BY 1"""):
            print("  ", r)
    examples = ["10.1103/physrevb.65.045102", "10.5802/alco.124", "10.1890/08-0418.1",
                "10.1145/3770855.3818994", "10.1063/5.0347370"]
    ex = ", ".join(f"'{d}'" for d in examples)
    print("example DOIs (oxjob #1256 EXPLORE):")
    for r in rows(f"""SELECT {CLEAN.format(col='doi')} AS doi_clean, collect_set(id) AS map_ids, MIN(id) AS resolves_to
                      FROM {MAP} WHERE lower({CLEAN.format(col='doi')}) IN ({ex}) GROUP BY 1 ORDER BY 1"""):
        print("  ", r)
