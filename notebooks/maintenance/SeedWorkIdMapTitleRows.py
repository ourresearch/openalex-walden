# Databricks notebook source
# MAGIC %md
# MAGIC # SeedWorkIdMapTitleRows — register every `title_author` a bound anchor carries (oxjob #1256)
# MAGIC
# MAGIC `work_id_map` is a registry of identifier→work bindings (many rows per work; `MapWorkIds`
# MAGIC header). Until 2026-09-22 the mint cells registered only an anchor's routing key and the
# MAGIC enrichment cells filled empty slots on the resolved row, so an anchor that resolved on its
# MAGIC doi / pmid / arxiv never registered its `title_author`. A work whose records carry several
# MAGIC title-key variants (Crossref consortium vs repo first person, translations, `defalcom;` vs
# MAGIC `defalco;m`, normalizer drift) was findable under one of them; the next keyless record carrying
# MAGIC another variant minted a duplicate (~450/day; 4.08M excess works in the identical-key class).
# MAGIC `MapWorkIds` now registers the night's bindings (walden `3631b877`); this notebook is the
# MAGIC backfill of the stock: 2026-09-22 sizing found 80,727,831 pinned (work, title_author) pairs
# MAGIC the map lacks, on 78.3M works; 68.2M of the keys were unseen anywhere; 1.23M pairs push a key
# MAGIC past the title tier's `n_ids <= 3` guard (the key becomes blocked for keyless records — the
# MAGIC guard's purpose — `include_blocked_keys = no` leaves those pairs out).
# MAGIC
# MAGIC Insert-only: one title-only row `(work_id, NULL, NULL, NULL, title_author)` per missing binding,
# MAGIC gated exactly like the title tier (`LENGTH > 20`, not a curated bad title). Nothing moves a
# MAGIC pinned anchor, a citation or a work. Where a key was already bound to another work the pair
# MAGIC becomes `n_ids = 2` and MIN(id) routes new arrivals until the identical-key merge
# MAGIC (RepointWorkIds) collapses the pair. Undo: delete the rows this run inserted —
# MAGIC `DELETE FROM work_id_map m WHERE m.doi IS NULL AND m.pmid IS NULL AND m.arxiv IS NULL AND
# MAGIC EXISTS (SELECT 1 FROM <target> t WHERE t.executed_at IS NOT NULL AND t.work_id = m.id AND
# MAGIC t.title_author = m.title_author AND m.updated_date >= t.executed_at)`.
# MAGIC
# MAGIC Modes (all keyed on `target_table`):
# MAGIC - `stage`    the missing bindings (registry × latest `locations_w_types` row, anti-joined to the map)
# MAGIC              with the key's current distinct-id count.
# MAGIC - `dry_run`  what `execute` would insert, how many keys cross the guard, samples with titles.
# MAGIC - `execute`  `confirm = yes` only, outside 04:00-08:00 UTC; refuses to run twice on a target.
# MAGIC - `verify`   the morning after: live re-census; expect the missing count at ~0 (anchors that
# MAGIC              arrived after the stage are the residue).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify"])
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_map_title_seed_target")
dbutils.widgets.text("include_blocked_keys", "yes")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table")
INCLUDE_BLOCKED = dbutils.widgets.get("include_blocked_keys") == "yes"
CONFIRM = dbutils.widgets.get("confirm") == "yes"

REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
LWT = "openalex.works.locations_w_types"
WORKS = "openalex.works.openalex_works"
BAD_TITLES = "openalex.system.bad_titles"
# the title tier's guard: a key with more distinct ids than this is blocked for keyless records
N_IDS_GUARD = 3

import datetime, json, time

SUMMARY = {"mode": MODE}


def note(**kw):
    """Serverless notebook tasks return no stdout through the API; everything printed is also returned via notebook.exit."""
    SUMMARY.update(kw)
    print(kw)

print(dict(mode=MODE, target=TARGET, include_blocked_keys=INCLUDE_BLOCKED, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def bound_pairs_sql():
    """Every (work_id, title_author) a pinned anchor carries, under the title tier's own gate
    (byte-identical to MapWorkIds' pending cell: latest LWT row per anchor, LENGTH > 20, not a bad title)."""
    return f"""
    WITH lwt AS (
      SELECT provenance, native_id_namespace, native_id, title,
             NULLIF(merge_key.title_author, '') AS title_author
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY provenance, native_id_namespace, native_id
                                         ORDER BY updated_date DESC) AS rn
            FROM {LWT})
      WHERE rn = 1 AND NULLIF(merge_key.title_author, '') IS NOT NULL
    ),
    bad AS (SELECT DISTINCT TRIM(title) AS title FROM {BAD_TITLES})
    SELECT DISTINCT r.work_id, l.title_author
    FROM lwt l
    JOIN {REGISTRY} r
      ON r.provenance = l.provenance AND r.native_id_namespace = l.native_id_namespace AND r.native_id = l.native_id
    LEFT ANTI JOIN bad ON bad.title = TRIM(l.title)
    WHERE r.work_id IS NOT NULL AND LENGTH(l.title_author) > 20
    """


def missing_sql():
    """Bound pairs the map does not hold, with the key's current distinct-id count (0 = key unseen anywhere)."""
    return f"""
    WITH pairs AS ({bound_pairs_sql()}),
    missing AS (
      SELECT p.work_id, p.title_author FROM pairs p
      LEFT ANTI JOIN {MAP} m ON m.id = p.work_id AND m.title_author = p.title_author
    ),
    keyn AS (
      SELECT m.title_author, COUNT(DISTINCT m.id) AS n_ids
      FROM {MAP} m
      LEFT SEMI JOIN missing x ON x.title_author = m.title_author
      WHERE m.title_author IS NOT NULL GROUP BY m.title_author
    )
    SELECT x.work_id, x.title_author, COALESCE(k.n_ids, 0) AS n_ids_before
    FROM missing x LEFT JOIN keyn k ON k.title_author = x.title_author
    """


def print_census(table):
    for r in rows(f"""
        SELECT CASE WHEN n_ids_before = 0 THEN 'key unseen anywhere'
                    WHEN n_ids_before < {N_IDS_GUARD} THEN 'key bound to other work(s), stays under the guard'
                    ELSE 'pair pushes the key past the guard (blocked for keyless records)' END AS class,
               COUNT(*) AS pairs, COUNT(DISTINCT work_id) AS works, COUNT(DISTINCT title_author) AS keys
        FROM {table} GROUP BY 1 ORDER BY 2 DESC"""):
        print(r)


def pred():
    """rows of the target this run applies to (alias t)"""
    guard = "" if INCLUDE_BLOCKED else f" AND t.n_ids_before < {N_IDS_GUARD}"
    return f"t.executed_at IS NULL{guard}"


def scope():
    return f"{TARGET} t WHERE {pred()}"

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  SELECT *, current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM ({missing_sql()})""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (title_author, work_id)")
    note(staged_seconds=int(time.time() - t0), target=TARGET,
         bound_pairs=one(f"SELECT COUNT(*) AS n FROM ({bound_pairs_sql()})")["n"],
         missing=one(f"SELECT COUNT(*) AS pairs, COUNT(DISTINCT work_id) AS works FROM {TARGET}"))
    print_census(TARGET)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    plan = one(f"""
        SELECT COUNT(*) AS rows_to_insert, COUNT(DISTINCT t.work_id) AS works, COUNT(DISTINCT t.title_author) AS keys,
               SUM(CASE WHEN t.n_ids_before = 0 THEN 1 ELSE 0 END) AS keys_unseen_anywhere,
               SUM(CASE WHEN t.n_ids_before >= {N_IDS_GUARD} THEN 1 ELSE 0 END) AS pairs_pushing_key_past_guard,
               MAX(t.n_ids_before) AS max_n_ids_before
        FROM {scope()}""")
    held = one(f"""SELECT COUNT(*) AS pairs_left_out_blocked_keys FROM {TARGET} t
                   WHERE t.executed_at IS NULL AND t.n_ids_before >= {N_IDS_GUARD}""") if not INCLUDE_BLOCKED else {}
    note(**plan, **held, include_blocked_keys=INCLUDE_BLOCKED)
    if (plan["rows_to_insert"] or 0) == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "nothing to do"}, default=str))
    print("sample bindings (key unseen anywhere) — the work and the key it will gain:")
    for r in rows(f"""SELECT t.work_id, t.title_author, LEFT(w.title, 80) AS title,
                             (SELECT array_join(collect_set(m.title_author), ' | ') FROM {MAP} m WHERE m.id = t.work_id AND m.title_author IS NOT NULL) AS keys_held
                      FROM {TARGET} t LEFT JOIN {WORKS} w ON w.id = t.work_id
                      WHERE {pred()} AND t.n_ids_before = 0 ORDER BY RAND(1) LIMIT 8"""):
        print("  ", r)
    print("sample bindings (key already bound to another work — the pinned-split class):")
    for r in rows(f"""SELECT t.work_id, t.title_author, t.n_ids_before, LEFT(w.title, 60) AS title,
                             (SELECT MIN(m.id) FROM {MAP} m WHERE m.title_author = t.title_author) AS key_resolves_to_today
                      FROM {TARGET} t LEFT JOIN {WORKS} w ON w.id = t.work_id
                      WHERE {pred()} AND t.n_ids_before BETWEEN 1 AND {N_IDS_GUARD - 1} ORDER BY RAND(2) LIMIT 8"""):
        print("  ", r)
    print("sample bindings pushing a key past the guard:")
    for r in rows(f"""SELECT t.work_id, t.title_author, t.n_ids_before FROM {TARGET} t
                      WHERE t.executed_at IS NULL AND t.n_ids_before >= {N_IDS_GUARD} ORDER BY RAND(3) LIMIT 5"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if one(f"SELECT COUNT(*) AS n FROM {TARGET} WHERE executed_at IS NOT NULL")["n"] > 0:
        raise Exception(f"{TARGET} already executed (executed_at set): stage again into a new target_table")

    t0 = time.time()
    # anti-joined to the map once more: the nightly cell may have registered some of these since the stage
    spark.sql(f"""INSERT INTO {MAP} (id, doi, pmid, arxiv, title_author, created_date, updated_date)
                  SELECT t.work_id, NULL, NULL, NULL, t.title_author, current_date(), current_timestamp()
                  FROM {scope()}
                  AND NOT EXISTS (SELECT 1 FROM {MAP} m WHERE m.id = t.work_id AND m.title_author = t.title_author)""")
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE {pred()}")
    note(executed_seconds=int(time.time() - t0),
         executed=one(f"SELECT COUNT(*) AS pairs, MIN(executed_at) AS executed_at FROM {TARGET} WHERE executed_at IS NOT NULL"))

# COMMAND ----------

if MODE == "verify":
    t0 = time.time()
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW missing_now AS {missing_sql()}")
    note(census_seconds=int(time.time() - t0),
         missing_now=one("SELECT COUNT(*) AS pairs, COUNT(DISTINCT work_id) AS works FROM missing_now"))
    print_census("missing_now")
    if spark.catalog.tableExists(TARGET):
        note(executed_pairs_still_missing=one(f"""SELECT COUNT(*) AS n FROM {TARGET} t JOIN missing_now x
                          ON x.work_id = t.work_id AND x.title_author = t.title_author WHERE t.executed_at IS NOT NULL""")["n"])
    print("the #1256 example pair (W4406840771 registered under _safety;c only; W7128278553 minted on _phan;l):")
    for r in rows(f"SELECT id, title_author FROM {MAP} WHERE id IN (4406840771, 7128278553) AND title_author IS NOT NULL ORDER BY 1, 2"):
        print("  ", r)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
