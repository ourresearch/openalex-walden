# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.33-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # RekeyPlaceholderOaiIds — move stored placeholder-host OAI ids onto the endpoint's own key (oxjob #1407 Phase B)
# MAGIC
# MAGIC `repo_works` is keyed on the OAI header id. Installs that never set their repository identifier
# MAGIC emit a platform placeholder (`oai:ojs.pkp.sfu.ca:article/1`), so unrelated journals share one id
# MAGIC space and `apply_changes` keeps one record per id (~335K records lost, 2026-09-28). Since Phase A
# MAGIC (2026-09-30) Repo.py / RepoBackfill.py key such ids on the endpoint's own pmh_url host + install
# MAGIC path (`openalex.dlt.oai_ids.endpoint_hosts`) for every endpoint EXCEPT
# MAGIC `oai_ids_excluded.PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS`: the endpoints whose placeholder records were
# MAGIC already stored. This notebook moves those stored records; the commit that empties the exclusion list
# MAGIC ships right after `execute`.
# MAGIC
# MAGIC **Order-independent by construction.** Work pins (`location_work_ids`), enrichments and denylist
# MAGIC rows are COPIED to the new key, never renamed: whichever key MapWorkIds sees on a given night, it
# MAGIC finds a pin, so no anchor re-resolves or mints. The old-key rows are dead once the old keys are
# MAGIC gone from repo_works and are removed by `cleanup` after `verify`.
# MAGIC
# MAGIC Modes (`job` names every artifact: `openalex.repo.<job>_*`):
# MAGIC - `setup`       idempotent DDL: native_id_rekey_map, repo_replay, the sidecar view. Empty tables; no
# MAGIC                 behaviour change. Run BEFORE the walden deploy that reads them.
# MAGIC - `stage`       `<job>_hosts` (endpoint -> key host, from the registry via the library) and `<job>_map`
# MAGIC                 (old -> new key for every placeholder key stored in repo_works). Staging tables only.
# MAGIC - `dry_run`     what `execute` would do, the guards, and the Guardrails estimate. Read-only.
# MAGIC - `execute`     `confirm = yes`, outside 03:00-08:00 UTC: freeze `<job>_audit_*`, then the map,
# MAGIC                 pin/enrichment/denylist copies, replay rows, and the RepoBackfill replay. The Repo
# MAGIC                 pipeline applies the replay on its next update.
# MAGIC - `verify`      after a Repo update and one End 2 End: old keys gone, new keys present, pins kept.
# MAGIC - `cleanup`     `confirm = yes`: delete old-key pin/enrichment/denylist rows whose key left repo_works.
# MAGIC - `undo_copies` `confirm = yes`: delete the rows `execute` inserted (from the audit tables).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["setup", "stage", "dry_run", "execute", "verify", "cleanup", "undo_copies"])
dbutils.widgets.text("job", "oxjob1407")
dbutils.widgets.text("confirm", "no")
# Validation only: write every table this notebook creates or changes into openalex.<schema_override>
# instead of production (sources such as repo_items, the registry and curations are still read from
# production), and restrict `stage` to `only_endpoints`. Leave both empty for the real run.
dbutils.widgets.text("schema_override", "")
dbutils.widgets.text("only_endpoints", "")

MODE = dbutils.widgets.get("mode")
JOB = dbutils.widgets.get("job").strip()
CONFIRM = dbutils.widgets.get("confirm") == "yes"
assert JOB.replace("_", "").isalnum(), JOB

import datetime
from pyspark.sql import functions as F
from openalex.dlt.oai_ids import (
    DEFAULT_OAI_HOSTS, ENDPOINT_REGISTRY_TABLE, REKEY_MAP_TABLE, REPLAY_TABLE,
    endpoint_hosts, is_placeholder_id_sql, rekey_native_id_sql, to_replay_rows)
from openalex.dlt.oai_ids_excluded import PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS
from openalex.dlt.oai_ids_overrides import KEEP_PLACEHOLDER_ENDPOINTS
from openalex.dlt.sequencing import dedupe_by_sequence

SIDECAR_VIEW = "openalex.works.locations_parsed_pmh_rekeyed"
REPO_WORKS = "openalex.repo.repo_works"
REPO_PARSED = "openalex.repo.repo_parsed"
REGISTRY = "openalex.works.location_work_ids"
ENRICH = "openalex.works.location_enrichments"
DENYLIST = "openalex.works.location_denylist"
CURATIONS = "openalex.curations.approved_curations"

OVERRIDE = dbutils.widgets.get("schema_override").strip()
ONLY_ENDPOINTS = [x.strip() for x in dbutils.widgets.get("only_endpoints").split(",") if x.strip()]
WORK_SCHEMA = f"openalex.{OVERRIDE}" if OVERRIDE else "openalex.repo"
if OVERRIDE:
    assert OVERRIDE.startswith("repo_dev_"), "schema_override must name a repo_dev_* scratch schema"
    _dev = lambda t: f"openalex.{OVERRIDE}.{t.split('.')[-1]}"
    (REKEY_MAP_TABLE, REPLAY_TABLE, SIDECAR_VIEW, REPO_WORKS, REPO_PARSED, REGISTRY, ENRICH, DENYLIST) = map(
        _dev, (REKEY_MAP_TABLE, REPLAY_TABLE, SIDECAR_VIEW, REPO_WORKS, REPO_PARSED, REGISTRY, ENRICH, DENYLIST))
    print("VALIDATION RUN: writing to", WORK_SCHEMA)
SCOPE = set(PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS)
if ONLY_ENDPOINTS:
    SCOPE &= set(ONLY_ENDPOINTS)
SCOPE_SQL = "endpoint_id IN (" + ", ".join(f"'{x}'" for x in sorted(SCOPE)) + ")"

STAGE_HOSTS = f"{WORK_SCHEMA}.{JOB}_hosts"
STAGE_MAP = f"{WORK_SCHEMA}.{JOB}_map"
SOURCE_IDS = f"{WORK_SCHEMA}.{JOB}_source_ids"
AUDIT = {t: f"{WORK_SCHEMA}.{JOB}_audit_{t.split('.')[-1]}" for t in (REGISTRY, ENRICH, DENYLIST)}
PLACEHOLDER = is_placeholder_id_sql
REKEY = lambda id_sql: rekey_native_id_sql(id_sql, "h.id_host", "h.keep_placeholder_host")

print({"mode": MODE, "job": JOB, "confirm": CONFIRM, "schema_override": OVERRIDE, "scope_endpoints": len(SCOPE),
       "placeholder_hosts": DEFAULT_OAI_HOSTS})


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def show(sql, n=50):
    spark.sql(sql).show(n, truncate=False)


def require_confirm_outside_nightly():
    if not CONFIRM:
        raise Exception(f"mode={MODE} writes production tables: set confirm=yes")
    h = datetime.datetime.now(datetime.timezone.utc).hour
    if not OVERRIDE and 3 <= h < 8:
        raise Exception(f"{h:02d}h UTC is inside the 03:00-08:00 End 2 End window; run before 03:00 or after 08:00")

# COMMAND ----------

# The sidecar view (the single definition; read by CreateSuperLocations and CreateRepoSuperAuthorships).
# Landing-page and PDF rows carry the pmh id they were fetched for inside `ids`. For an incumbent whose
# key moved, translate that id so the re-keyed repo record keeps its landing page, PDF, abstract,
# license and author sidecars. Rows without a map entry pass through unchanged.
SIDECAR_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {SIDECAR_VIEW}
COMMENT 'oxjob #1407: locations_parsed with the pmh id in landing_page/pdf rows translated through {REKEY_MAP_TABLE}'
AS
SELECT /*+ BROADCAST(m) */
  lp.* EXCEPT (ids),
  CASE WHEN m.old_native_id IS NULL THEN lp.ids
       ELSE transform(lp.ids, x -> IF(x.namespace = 'pmh' AND x.id = m.old_native_id,
                                      named_struct('id', m.new_native_id, 'namespace', x.namespace,
                                                   'relationship', x.relationship),
                                      x))
  END AS ids
FROM openalex.works.locations_parsed lp
LEFT JOIN (SELECT old_native_id, new_native_id FROM {REKEY_MAP_TABLE} WHERE is_incumbent) m
  ON lp.provenance IN ('landing_page', 'pdf')
 AND m.old_native_id = get(filter(lp.ids, x -> x.namespace = 'pmh').id, 0)
"""

if MODE == "setup":
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {REKEY_MAP_TABLE} (
        old_native_id STRING NOT NULL,
        new_native_id STRING NOT NULL,
        endpoint_id STRING,
        rule STRING NOT NULL,
        is_incumbent BOOLEAN NOT NULL,
        job STRING NOT NULL,
        built_at TIMESTAMP NOT NULL)
      COMMENT 'oxjob #1407: old -> new native_id for keys already stored. is_incumbent = held the key in repo_works at execute time.'""")
    if not spark.catalog.tableExists(REPLAY_TABLE):
        # _change_type is a CDF-reserved column name; the replay flow derives it from replay_op
        cols = [c for c in spark.table(REPO_PARSED).columns if c != "_change_type"]
        spark.sql(f"""
          CREATE TABLE {REPLAY_TABLE}
          COMMENT 'oxjob #1407: append-only corrections streamed into repo_works by the Repo pipeline replay flow'
          TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.appendOnly = true)
          AS SELECT {", ".join("`" + c + "`" for c in cols)},
                    CAST(NULL AS STRING) AS replay_job, CAST(NULL AS STRING) AS replay_op,
                    CAST(NULL AS STRING) AS replay_provenance, CAST(NULL AS TIMESTAMP) AS replay_loaded_at
          FROM {REPO_PARSED} WHERE false""")
    spark.sql(SIDECAR_VIEW_SQL)
    for t in (REKEY_MAP_TABLE, REPLAY_TABLE):
        print(t, spark.table(t).count(), "rows")
    print("created", SIDECAR_VIEW)

# COMMAND ----------

# STAGE. Scope: the excluded endpoints (the ones whose placeholder records are stored) that still have a
# placeholder-host record in the live harvest, the backfill or repo_works. The key host comes from the
# SAME library function Repo.py uses once the exclusion list is emptied (registry pmh_url host + install
# path, KEY_HOST_OVERRIDES applied), so live harvests after the cutover land on exactly these keys.
# Endpoints no longer in the registry get no host and are left on their placeholder keys (retired: they
# collide with nobody once the active ones move). keep_placeholder_host = KEEP_PLACEHOLDER_ENDPOINTS.

if MODE == "stage":
    spark.sql(f"""
      CREATE OR REPLACE TABLE {SOURCE_IDS} AS
      SELECT DISTINCT repository_id AS endpoint_id, `ns0:header`.`ns0:identifier` AS raw_id
      FROM openalex.repo.repo_items WHERE {PLACEHOLDER('`ns0:header`.`ns0:identifier`')}
        AND {SCOPE_SQL.replace('endpoint_id', 'repository_id')}
      UNION
      SELECT DISTINCT endpoint_id, pmh_id FROM openalex.repo.repo_items_backfill WHERE {PLACEHOLDER('pmh_id')}
        AND {SCOPE_SQL}
      UNION
      SELECT DISTINCT endpoint_id, native_id FROM {REPO_WORKS} WHERE {PLACEHOLDER('native_id')} AND {SCOPE_SQL}""")
    staged = {r.endpoint_id for r in spark.table(SOURCE_IDS).select("endpoint_id").distinct().collect()}
    registry = [(r.id, r.pmh_url) for r in spark.table(ENDPOINT_REGISTRY_TABLE).select("id", "pmh_url").collect()]
    hosts = endpoint_hosts(registry, excluded=set())
    host_rows = [(e, hosts[e], e in KEEP_PLACEHOLDER_ENDPOINTS) for e in sorted(staged) if e in hosts]
    (spark.createDataFrame(host_rows, "endpoint_id string, id_host string, keep_placeholder_host boolean")
          .write.mode("overwrite").saveAsTable(STAGE_HOSTS))
    print(f"staged endpoints: {len(staged):,}; with a key host: {len(host_rows):,}; "
          f"not in the registry (left on placeholder keys): {len(staged) - len(host_rows):,}")
    # the map: every key STORED in repo_works that moves, with the endpoint that holds it
    spark.sql(f"""
      CREATE OR REPLACE TABLE {STAGE_MAP} AS
      SELECT w.native_id AS old_native_id, {REKEY('w.native_id')} AS new_native_id,
             w.endpoint_id, 'default_host' AS rule, true AS is_incumbent
      FROM {REPO_WORKS} w JOIN {STAGE_HOSTS} h USING (endpoint_id)
      WHERE {PLACEHOLDER('w.native_id')}""")
    show(f"SELECT keep_placeholder_host, count(*) endpoints FROM {STAGE_HOSTS} GROUP BY 1")
    show(f"SELECT count(*) keys, count(DISTINCT new_native_id) new_keys, count(DISTINCT endpoint_id) endpoints FROM {STAGE_MAP}")

# COMMAND ----------

def guards():
    """Every condition that must hold before execute. Returns (counts, failures)."""
    g = one(f"""
      SELECT
        (SELECT count(*) FROM (SELECT old_native_id FROM {STAGE_MAP} GROUP BY 1 HAVING count(*) > 1)) AS dup_old,
        (SELECT count(*) FROM (SELECT new_native_id FROM {STAGE_MAP} GROUP BY 1 HAVING count(*) > 1)) AS dup_new,
        (SELECT count(*) FROM {STAGE_MAP} WHERE old_native_id = new_native_id) AS unchanged,
        (SELECT count(*) FROM {STAGE_MAP} m JOIN {REPO_WORKS} w ON w.native_id = m.new_native_id) AS new_key_exists,
        (SELECT count(*) FROM {STAGE_MAP} m JOIN {REGISTRY} r
           ON r.provenance = 'repo' AND r.native_id_namespace = 'pmh' AND r.native_id = m.new_native_id) AS new_key_pinned,
        (SELECT count(*) FROM {CURATIONS} c JOIN {STAGE_MAP} m ON c.entity_id = concat('pmh:', m.old_native_id)) AS curations,
        (SELECT count(*) FROM {REPLAY_TABLE} WHERE replay_job = '{JOB}') AS replay_rows_already,
        (SELECT count(*) FROM (
           SELECT s.endpoint_id, {REKEY('s.raw_id')} AS nk
           FROM {SOURCE_IDS} s JOIN {STAGE_HOSTS} h USING (endpoint_id)
           GROUP BY 1, 2 HAVING count(DISTINCT s.raw_id) > 1)) AS merging_raw_ids
    """)
    return g, [f"{k}={v:,}" for k, v in g.items() if v]


def with_new_self_id(df, old_col, new_col):
    """Point the record's own pmh entry in `ids` at the new key."""
    return df.withColumn("ids", F.expr(
        f"transform(ids, x -> IF(x.namespace = 'pmh' AND x.id = {old_col}, "
        f"named_struct('id', {new_col}, 'namespace', x.namespace, 'relationship', x.relationship), x))"))


def replay_live_upserts():
    """Latest repo_parsed version of every placeholder-host record of a staged endpoint, re-keyed.
    Incumbents keep their ingested_at (their sidecars travel through the view; no re-fetch). Recovered
    records -- never stored, never fetched -- get ingested_at = now, so taxicab's ingested_at window
    seeds them."""
    parsed = spark.sql(f"""
      SELECT p.*, {REKEY('p.native_id')} AS _new_id
      FROM {REPO_PARSED} p JOIN {STAGE_HOSTS} h USING (endpoint_id)
      WHERE {PLACEHOLDER('p.native_id')}""")
    latest = dedupe_by_sequence(
        parsed, keys=["endpoint_id", "native_id"],
        order_by=[F.col("updated_date").desc_nulls_last(), F.col("ingested_at").desc_nulls_last()])
    inc = spark.table(STAGE_MAP).select(F.col("new_native_id").alias("_new_id"),
                                       F.lit(True).alias("_incumbent")).distinct()
    out = with_new_self_id(latest.join(inc, "_new_id", "left"), "native_id", "_new_id")
    return (out.withColumn("ingested_at", F.when(F.col("_incumbent"), F.col("ingested_at"))
                                           .otherwise(F.current_timestamp()))
               .withColumn("native_id", F.col("_new_id"))
               .drop("_new_id", "_incumbent"))


def replay_deletes():
    """One delete per old key. Pre-image = the stored row with ingested_at + 1 microsecond: equal on
    updated_date, >= on the provenance rank ('repo'), strictly greater on ingested_at -> it outranks
    the stored _sequence (#1418's finding). Deletes never seed taxicab."""
    return spark.sql(f"""
      SELECT w.* EXCEPT (ingested_at),
             coalesce(w.ingested_at, TIMESTAMP '1970-01-01 00:00:00') + INTERVAL 1 MICROSECOND AS ingested_at
      FROM {REPO_WORKS} w JOIN {STAGE_MAP} m ON m.old_native_id = w.native_id""")


if MODE in ("dry_run", "execute"):
    assert spark.catalog.tableExists(STAGE_MAP), "run mode=stage first"
    g, fails = guards()
    print("guards:", g)
    est = one(f"""
      SELECT
        (SELECT count(*) FROM {STAGE_MAP}) AS keys_moving,
        (SELECT count(*) FROM {STAGE_MAP} m JOIN {REGISTRY} r
           ON r.provenance = 'repo' AND r.native_id_namespace = 'pmh' AND r.native_id = m.old_native_id) AS pins_to_copy,
        (SELECT count(*) FROM {STAGE_MAP} m JOIN {ENRICH} e
           ON e.provenance = 'repo' AND e.native_id_namespace = 'pmh' AND e.native_id = m.old_native_id) AS enrichments_to_copy,
        (SELECT count(*) FROM {STAGE_MAP} m JOIN {DENYLIST} d
           ON d.provenance = 'repo' AND d.native_id_namespace = 'pmh' AND d.native_id = m.old_native_id) AS denylist_to_copy,
        (SELECT count(DISTINCT r.work_id) FROM {STAGE_MAP} m JOIN {REGISTRY} r
           ON r.provenance = 'repo' AND r.native_id_namespace = 'pmh' AND r.native_id = m.old_native_id) AS works_restamped_by_pmh_id
    """)
    print("estimate:", est)
    live = replay_live_upserts().cache()
    n_live = live.count()
    n_recovered = live.filter(F.col("ingested_at") >= F.expr("current_timestamp() - INTERVAL 1 HOUR")).count()
    print(f"live replay upserts: {n_live:,} (recovered, never stored: {n_recovered:,}). Backfill-origin records come "
          f"from the RepoBackfill replay (placeholder-only parse) and are not in this count.")
    print(f"Guardrails check 1 estimate (live part): ~{est['works_restamped_by_pmh_id'] + n_recovered:,} works "
          f"stamped vs 7,500,000. Deleted locations: ~{est['keys_moving']:,} old ES ids vs guard_fraction 0.5%.")
    show(f"SELECT endpoint_id, count(*) n FROM {STAGE_MAP} GROUP BY 1 ORDER BY n DESC", 25)
    if fails:
        print("GUARDS FAILING:", fails)

# COMMAND ----------

if MODE == "execute":
    require_confirm_outside_nightly()
    if fails:
        raise Exception(f"guards failing, nothing written: {fails}")
    for t in (REGISTRY, ENRICH, DENYLIST):
        if spark.catalog.tableExists(AUDIT[t]):
            raise Exception(f"{AUDIT[t]} exists: {JOB} was already executed")

    # 1. freeze the audit: the rows about to be COPIED to their new key
    for t in (REGISTRY, ENRICH, DENYLIST):
        spark.sql(f"""
          CREATE TABLE {AUDIT[t]} AS
          SELECT m.new_native_id AS copied_to, s.*
          FROM {t} s JOIN {STAGE_MAP} m
            ON s.provenance = 'repo' AND s.native_id_namespace = 'pmh' AND s.native_id = m.old_native_id""")
        print(AUDIT[t], spark.table(AUDIT[t]).count())

    # 2. the map (read by the sidecar view and CreateLocationsWithTypes from the next End 2 End)
    spark.sql(f"""
      INSERT INTO {REKEY_MAP_TABLE}
      SELECT old_native_id, new_native_id, endpoint_id, rule, is_incumbent, '{JOB}', current_timestamp()
      FROM {STAGE_MAP}""")

    # 3. copy pins, enrichments, denylist rows to the new key (old rows stay until cleanup)
    for t in (REGISTRY, ENRICH, DENYLIST):
        cols = spark.table(t).columns
        sel = ", ".join("copied_to" if c == "native_id" else f"`{c}`" for c in cols)
        spark.sql(f"INSERT INTO {t} ({', '.join('`' + c + '`' for c in cols)}) SELECT {sel} FROM {AUDIT[t]}")
        print("copied into", t)

    # 4. replay rows: live upserts, then one delete per old key
    replay_schema = spark.table(REPLAY_TABLE).schema
    for df, op in ((replay_live_upserts(), "upsert"), (replay_deletes(), "delete")):
        to_replay_rows(df, replay_schema, JOB, op, "repo").write.format("delta").mode("append").saveAsTable(REPLAY_TABLE)

    # 5. backfill-origin records: RepoBackfill's own parser, placeholder records only
    if OVERRIDE:
        print("VALIDATION RUN: skipping the RepoBackfill replay (it writes the production replay table)")
    else:
        print(dbutils.notebook.run("../ingest/RepoBackfill", 4 * 3600, {"replay_job": JOB, "rebuild": "false"}))

    show(f"""SELECT replay_op, replay_provenance, count(*) n FROM {REPLAY_TABLE}
             WHERE replay_job = '{JOB}' GROUP BY 1,2 ORDER BY 1,2""")
    print("Done. NOW push the commit that empties oai_ids_excluded for these endpoints, then run a Repo "
          "pipeline update (it applies the replay); then the 05:00 End 2 End; then mode=verify.")

# COMMAND ----------

if MODE == "verify":
    show(f"""
      SELECT
        (SELECT count(*) FROM {REKEY_MAP_TABLE} m JOIN {REPO_WORKS} w ON w.native_id = m.old_native_id
          WHERE m.job = '{JOB}') AS old_keys_still_in_repo_works,
        (SELECT count(*) FROM {REKEY_MAP_TABLE} m LEFT ANTI JOIN {REPO_WORKS} w ON w.native_id = m.new_native_id
          WHERE m.job = '{JOB}') AS new_keys_missing_from_repo_works,
        (SELECT count(*) FROM {REPO_WORKS} w JOIN {STAGE_HOSTS} h USING (endpoint_id)
          WHERE {PLACEHOLDER('w.native_id')}) AS placeholder_keys_left_on_staged_endpoints,
        (SELECT count(*) FROM {AUDIT[REGISTRY]} a JOIN {REGISTRY} r
           ON r.provenance = 'repo' AND r.native_id_namespace = 'pmh' AND r.native_id = a.copied_to
          WHERE r.work_id <=> a.work_id) AS pins_kept,
        (SELECT count(*) FROM {AUDIT[REGISTRY]}) AS pins_copied,
        (SELECT count(*) FROM {REPLAY_TABLE} WHERE replay_job = '{JOB}' AND replay_op = 'upsert') AS replay_upserts
    """)

# COMMAND ----------

if MODE == "cleanup":
    require_confirm_outside_nightly()
    for t in (REGISTRY, ENRICH, DENYLIST):
        spark.sql(f"""
          DELETE FROM {t}
          WHERE provenance = 'repo' AND native_id_namespace = 'pmh'
            AND native_id IN (SELECT old_native_id FROM {REKEY_MAP_TABLE} WHERE job = '{JOB}')
            AND native_id NOT IN (SELECT native_id FROM {REPO_WORKS})""")
        print("cleaned", t)

if MODE == "undo_copies":
    require_confirm_outside_nightly()
    for t in (REGISTRY, ENRICH, DENYLIST):
        spark.sql(f"""
          DELETE FROM {t}
          WHERE provenance = 'repo' AND native_id_namespace = 'pmh'
            AND native_id IN (SELECT copied_to FROM {AUDIT[t]})""")
        print("removed copies from", t)
    print("Replay rows cannot be un-appended (append-only CDF). To reverse the key move itself, restore the "
          "endpoints to oai_ids_excluded and replay the inverse map.")
