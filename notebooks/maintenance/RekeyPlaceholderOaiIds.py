# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.30-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # RekeyPlaceholderOaiIds — re-key placeholder-host OAI ids and moved hosts (oxjob #1407)
# MAGIC
# MAGIC `repo_works` is keyed on the OAI header id. Installs that never set their repository identifier
# MAGIC emit a platform placeholder (`oai:ojs.pkp.sfu.ca:article/1` from 198 endpoints), so
# MAGIC `apply_changes` keeps one record per id: 335 endpoints, 1,267,040 harvested records on 886,136
# MAGIC ids, ~335K distinct records lost (2026-09-28). Repo.py / RepoBackfill.py re-key those ids to the
# MAGIC endpoint's own host (`openalex.dlt.oai_ids`) for every endpoint listed in
# MAGIC `openalex.repo.endpoint_id_host`. This notebook moves what is ALREADY stored, and applies explicit
# MAGIC old -> new renames for hosts that moved their OAI ids (`host_move`: UCM, USK, polibotanica).
# MAGIC
# MAGIC **Order-independent by construction.** Work pins (`location_work_ids`), enrichments and denylist
# MAGIC rows are COPIED to the new key, never renamed: whichever key MapWorkIds sees on a given night, it
# MAGIC finds a pin, so no anchor re-resolves or mints. The old-key rows are dead once the old keys are
# MAGIC gone from repo_works and are removed by `cleanup` after `verify`.
# MAGIC
# MAGIC Modes (`job` names every artifact: `openalex.repo.<job>_*`):
# MAGIC - `setup`           idempotent DDL: endpoint_id_host, native_id_rekey_map, host_move_ids, repo_replay,
# MAGIC                     the sidecar view. Empty tables; no behaviour change. Run BEFORE the walden deploy
# MAGIC                     that reads them.
# MAGIC - `add_endpoints`   Phase A: give `endpoint_ids` (endpoints with no stored placeholder records, e.g. a
# MAGIC                     new OJS journal) an id_host. `confirm = yes`.
# MAGIC - `load_host_moves` `host_move_prefixes` (JSON {endpoint_id: [old_prefix, new_prefix]}) and/or
# MAGIC                     `host_move_csv` (endpoint_id,old_native_id,new_native_id) -> host_move_ids.
# MAGIC - `stage`           `<job>_endpoint_id_host_stage` and `<job>_map` from live data (staging tables only).
# MAGIC - `dry_run`         what `execute` would do, the guards, and the Guardrails estimate. Read-only.
# MAGIC - `execute`         `confirm = yes`, outside 03:00-08:00 UTC: freeze `<job>_audit_*`, then
# MAGIC                     endpoint_id_host, native_id_rekey_map, pin/enrichment/denylist copies, replay rows,
# MAGIC                     and the RepoBackfill replay. The Repo pipeline applies the replay on its next update.
# MAGIC - `verify`          after a Repo update and one End 2 End: old keys gone, new keys present, pins kept.
# MAGIC - `cleanup`         `confirm = yes`: delete old-key pin/enrichment/denylist rows whose key left repo_works.
# MAGIC - `undo_copies`     `confirm = yes`: delete the rows `execute` inserted (from the audit tables).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["setup", "add_endpoints", "load_host_moves", "stage", "dry_run",
                                             "execute", "verify", "cleanup", "undo_copies"])
dbutils.widgets.text("job", "oxjob1407")
dbutils.widgets.text("endpoint_ids", "")
dbutils.widgets.text("host_move_prefixes", "")
dbutils.widgets.text("host_move_csv", "")
dbutils.widgets.text("host_move_requested_by", "")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
JOB = dbutils.widgets.get("job").strip()
CONFIRM = dbutils.widgets.get("confirm") == "yes"
assert JOB.replace("_", "").isalnum(), JOB

import datetime, json
from pyspark.sql import functions as F
from openalex.dlt.oai_ids import (
    DEFAULT_OAI_HOSTS, ENDPOINT_ID_HOST_TABLE, REKEY_MAP_TABLE, REPLAY_TABLE,
    is_placeholder_id_sql, pmh_url_id_host_sql, rekey_native_id_sql, to_replay_rows)
from openalex.dlt.sequencing import dedupe_by_sequence
from openalex.dlt.repo_filters import ENDPOINT_SETSPEC_KEEP

HOST_MOVE_TABLE = "openalex.repo.host_move_ids"
SIDECAR_VIEW = "openalex.works.locations_parsed_pmh_rekeyed"
REPO_WORKS = "openalex.repo.repo_works"
REPO_PARSED = "openalex.repo.repo_parsed"
ENDPOINTS = "openalex_sources.public.oai_pmh_endpoint"
DELETION_AUDIT = "openalex_sources.public.endpoint_deletion_audit"
REGISTRY = "openalex.works.location_work_ids"
ENRICH = "openalex.works.location_enrichments"
DENYLIST = "openalex.works.location_denylist"
CURATIONS = "openalex.curations.approved_curations"

STAGE_HOSTS = f"openalex.repo.{JOB}_endpoint_id_host_stage"
STAGE_MAP = f"openalex.repo.{JOB}_map"
SOURCE_IDS = f"openalex.repo.{JOB}_source_ids"
AUDIT = {t: f"openalex.repo.{JOB}_audit_{t.split('.')[-1]}" for t in (REGISTRY, ENRICH, DENYLIST)}
PLACEHOLDER = is_placeholder_id_sql
# the registry/enrichment/denylist key for a repo location (UnionAll maps repo_backfill -> 'repo')
REPO_KEY = "provenance = 'repo' AND native_id_namespace = 'pmh'"

print({"mode": MODE, "job": JOB, "confirm": CONFIRM, "placeholder_hosts": DEFAULT_OAI_HOSTS})


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
    if 3 <= h < 8:
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
      CREATE TABLE IF NOT EXISTS {ENDPOINT_ID_HOST_TABLE} (
        endpoint_id STRING NOT NULL,
        id_host STRING NOT NULL,
        keep_placeholder_host BOOLEAN NOT NULL,
        assigned_at TIMESTAMP NOT NULL,
        assigned_by STRING,
        note STRING)
      COMMENT 'oxjob #1407: frozen, insert-only. Repo.py/RepoBackfill.py replace a placeholder OAI host with id_host for these endpoints.'""")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {REKEY_MAP_TABLE} (
        old_native_id STRING NOT NULL,
        new_native_id STRING NOT NULL,
        endpoint_id STRING,
        rule STRING NOT NULL,
        is_incumbent BOOLEAN NOT NULL,
        job STRING NOT NULL,
        built_at TIMESTAMP NOT NULL)
      COMMENT 'oxjob #1407: old -> new native_id for keys already stored (rule default_host | host_move). is_incumbent = held the key in repo_works at execute time.'""")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {HOST_MOVE_TABLE} (
        endpoint_id STRING NOT NULL,
        old_native_id STRING NOT NULL,
        new_native_id STRING NOT NULL,
        requested_by STRING,
        source STRING,
        loaded_at TIMESTAMP NOT NULL)
      COMMENT 'oxjob #1407: explicit renames for hosts that moved their OAI ids (checked same article per id)'""")
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
    for t in (ENDPOINT_ID_HOST_TABLE, REKEY_MAP_TABLE, HOST_MOVE_TABLE, REPLAY_TABLE):
        print(t, spark.table(t).count(), "rows")
    print("created", SIDECAR_VIEW)

# COMMAND ----------

def endpoint_hosts_sql(endpoints_sql):
    """endpoint_id -> id_host from the live mirror, else the deletion audit, else ep-<endpoint_id>."""
    return f"""
      WITH live AS (SELECT id AS endpoint_id, {pmh_url_id_host_sql('pmh_url')} AS h FROM {ENDPOINTS}),
      dead AS (SELECT endpoint_id, {pmh_url_id_host_sql('pmh_url')} AS h FROM {DELETION_AUDIT}
               QUALIFY row_number() OVER (PARTITION BY endpoint_id ORDER BY executed_at DESC) = 1)
      SELECT e.endpoint_id,
             coalesce(nullif(l.h, ''), nullif(d.h, ''), concat('ep-', e.endpoint_id)) AS id_host,
             CASE WHEN nullif(l.h, '') IS NOT NULL THEN 'oai_pmh_endpoint'
                  WHEN nullif(d.h, '') IS NOT NULL THEN 'endpoint_deletion_audit' ELSE 'fallback' END AS id_host_source
      FROM ({endpoints_sql}) e
      LEFT JOIN live l ON l.endpoint_id = e.endpoint_id
      LEFT JOIN dead d ON d.endpoint_id = e.endpoint_id"""


def endpoint_list(widget="endpoint_ids"):
    return [x.strip() for x in dbutils.widgets.get(widget).split(",") if x.strip()]


if MODE == "add_endpoints":
    require_confirm_outside_nightly()
    ids = endpoint_list()
    assert ids, "endpoint_ids is empty"
    in_list = ", ".join(f"'{x}'" for x in ids)
    stored = one(f"SELECT count(*) n FROM {REPO_WORKS} WHERE endpoint_id IN ({in_list}) AND {PLACEHOLDER('native_id')}")["n"]
    if stored:
        raise Exception(f"{stored:,} placeholder rows are already stored for these endpoints: "
                        "they need the stage/execute path, not add_endpoints")
    spark.sql(f"""
      MERGE INTO {ENDPOINT_ID_HOST_TABLE} t
      USING ({endpoint_hosts_sql(f"SELECT explode(array({in_list})) AS endpoint_id")}) s
      ON t.endpoint_id = s.endpoint_id
      WHEN NOT MATCHED THEN INSERT (endpoint_id, id_host, keep_placeholder_host, assigned_at, assigned_by, note)
        VALUES (s.endpoint_id, s.id_host, false, current_timestamp(), '{JOB}', concat('add_endpoints; ', s.id_host_source))""")
    show(f"SELECT * FROM {ENDPOINT_ID_HOST_TABLE} WHERE endpoint_id IN ({in_list})")

# COMMAND ----------

if MODE == "load_host_moves":
    require_confirm_outside_nightly()
    by = dbutils.widgets.get("host_move_requested_by").strip() or None
    prefixes = dbutils.widgets.get("host_move_prefixes").strip()
    csv_path = dbutils.widgets.get("host_move_csv").strip()
    frames = []
    if prefixes:
        # {endpoint_id: [old_prefix, new_prefix]}: every STORED key of that endpoint under old_prefix
        for ep, (old_p, new_p) in json.loads(prefixes).items():
            assert old_p.startswith("oai:") and old_p.endswith(":") and new_p.endswith(":"), (old_p, new_p)
            frames.append(spark.sql(f"""
              SELECT endpoint_id, native_id AS old_native_id,
                     concat('{new_p}', substr(native_id, {len(old_p) + 1})) AS new_native_id,
                     'prefix {old_p} -> {new_p}' AS source
              FROM {REPO_WORKS} WHERE endpoint_id = '{ep}' AND startswith(native_id, '{old_p}')"""))
    if csv_path:
        frames.append(spark.read.option("header", True).csv(csv_path)
                      .select("endpoint_id", "old_native_id", "new_native_id")
                      .withColumn("source", F.lit(csv_path)))
    assert frames, "give host_move_prefixes and/or host_move_csv"
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    df = (df.withColumn("requested_by", F.lit(by)).withColumn("loaded_at", F.current_timestamp())
            .dropDuplicates(["endpoint_id", "old_native_id"]))
    # host_move replay rows come from repo_works, which carries no set_spec: a keep-list endpoint
    # would drop them in apply_endpoint_filters
    bad = [r.endpoint_id for r in df.select("endpoint_id").distinct().collect() if r.endpoint_id in ENDPOINT_SETSPEC_KEEP]
    assert not bad, f"host_move endpoints on the setSpec keep-list are not supported: {bad}"
    df.createOrReplaceTempView("hm_new")
    spark.sql(f"""
      MERGE INTO {HOST_MOVE_TABLE} t USING hm_new s
      ON t.endpoint_id = s.endpoint_id AND t.old_native_id = s.old_native_id
      WHEN NOT MATCHED THEN INSERT (endpoint_id, old_native_id, new_native_id, requested_by, source, loaded_at)
        VALUES (s.endpoint_id, s.old_native_id, s.new_native_id, s.requested_by, s.source, s.loaded_at)""")
    show(f"SELECT endpoint_id, requested_by, source, count(*) n FROM {HOST_MOVE_TABLE} GROUP BY 1,2,3 ORDER BY 1")

# COMMAND ----------

# STAGE. Scope: every endpoint with a placeholder-host record in the live harvest, the backfill or
# repo_works, plus `endpoint_ids`. keep_placeholder_host is set for every endpoint where the plain
# re-key would map two distinct raw ids onto one key (aggregators re-emitting ojs.localhost:N and
# ojs.pkp.sfu.ca:N for different articles: 12 endpoints on 2026-09-29), so the re-key never merges.

if MODE == "stage":
    spark.sql(f"""
      CREATE OR REPLACE TABLE {SOURCE_IDS} AS
      SELECT DISTINCT repository_id AS endpoint_id, `ns0:header`.`ns0:identifier` AS raw_id
      FROM openalex.repo.repo_items WHERE {PLACEHOLDER('`ns0:header`.`ns0:identifier`')}
      UNION
      SELECT DISTINCT endpoint_id, pmh_id FROM openalex.repo.repo_items_backfill WHERE {PLACEHOLDER('pmh_id')}
      UNION
      SELECT DISTINCT endpoint_id, native_id FROM {REPO_WORKS} WHERE {PLACEHOLDER('native_id')}""")
    eps = f"SELECT DISTINCT endpoint_id FROM {SOURCE_IDS}" + "".join(
        f" UNION SELECT '{x}'" for x in endpoint_list())
    spark.sql(f"CREATE OR REPLACE TABLE openalex.repo.{JOB}_hosts_plain AS {endpoint_hosts_sql(eps)}")
    # rows already frozen in endpoint_id_host (Phase A) win over anything derived now
    spark.sql(f"""
      CREATE OR REPLACE TABLE {STAGE_HOSTS} AS
      WITH plain AS (
        SELECT h.endpoint_id, coalesce(f.id_host, h.id_host) AS id_host, h.id_host_source,
               f.keep_placeholder_host AS frozen_keep, f.endpoint_id IS NOT NULL AS frozen
        FROM openalex.repo.{JOB}_hosts_plain h LEFT JOIN {ENDPOINT_ID_HOST_TABLE} f USING (endpoint_id)),
      k AS (SELECT s.endpoint_id, s.raw_id, {rekey_native_id_sql('s.raw_id', 'p.id_host', 'false')} AS nk
            FROM {SOURCE_IDS} s JOIN plain p USING (endpoint_id)),
      merging AS (SELECT nk FROM k GROUP BY nk HAVING count(DISTINCT raw_id) > 1),
      keep_eps AS (SELECT DISTINCT endpoint_id FROM k JOIN merging USING (nk))
      SELECT p.endpoint_id, p.id_host,
             coalesce(p.frozen_keep, ke.endpoint_id IS NOT NULL) AS keep_placeholder_host,
             p.id_host_source, p.frozen
      FROM plain p LEFT JOIN keep_eps ke USING (endpoint_id)""")
    # the map: every key STORED in repo_works that moves, with the endpoint that holds it
    spark.sql(f"""
      CREATE OR REPLACE TABLE {STAGE_MAP} AS
      SELECT w.native_id AS old_native_id,
             {rekey_native_id_sql('w.native_id', 'h.id_host', 'h.keep_placeholder_host')} AS new_native_id,
             w.endpoint_id, 'default_host' AS rule, true AS is_incumbent
      FROM {REPO_WORKS} w JOIN {STAGE_HOSTS} h USING (endpoint_id)
      WHERE {PLACEHOLDER('w.native_id')}
      UNION ALL
      SELECT m.old_native_id, m.new_native_id, m.endpoint_id, 'host_move', true
      FROM {HOST_MOVE_TABLE} m
      JOIN {REPO_WORKS} w ON w.native_id = m.old_native_id AND w.endpoint_id = m.endpoint_id""")
    show(f"SELECT id_host_source, keep_placeholder_host, frozen, count(*) endpoints FROM {STAGE_HOSTS} GROUP BY 1,2,3")
    show(f"SELECT rule, count(*) n, count(DISTINCT new_native_id) new_keys FROM {STAGE_MAP} GROUP BY 1")

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
           ON r.{REPO_KEY.replace(' AND ', ' AND r.')} AND r.native_id = m.new_native_id) AS new_key_pinned,
        (SELECT count(*) FROM {CURATIONS} c JOIN {STAGE_MAP} m ON c.entity_id = concat('pmh:', m.old_native_id)) AS curations,
        (SELECT count(*) FROM {REPLAY_TABLE} WHERE replay_job = '{JOB}') AS replay_rows_already
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
      SELECT p.*, {rekey_native_id_sql('p.native_id', 'h.id_host', 'h.keep_placeholder_host')} AS _new_id
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


def replay_host_move_upserts():
    """host_move incumbents: the stored repo_works row under its new key (content as published)."""
    rw = spark.sql(f"""
      SELECT w.*, m.new_native_id AS _new_id
      FROM {REPO_WORKS} w JOIN {STAGE_MAP} m ON m.old_native_id = w.native_id AND m.rule = 'host_move'""")
    return (with_new_self_id(rw, "native_id", "_new_id")
            .withColumn("native_id", F.col("_new_id")).drop("_new_id"))


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
    show(f"SELECT rule, endpoint_id, count(*) n FROM {STAGE_MAP} GROUP BY 1,2 ORDER BY n DESC", 25)
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

    # 2. frozen endpoint hosts (insert-only) and the map. From here Repo.py re-keys new harvests.
    spark.sql(f"""
      MERGE INTO {ENDPOINT_ID_HOST_TABLE} t USING {STAGE_HOSTS} s ON t.endpoint_id = s.endpoint_id
      WHEN NOT MATCHED THEN INSERT (endpoint_id, id_host, keep_placeholder_host, assigned_at, assigned_by, note)
        VALUES (s.endpoint_id, s.id_host, s.keep_placeholder_host, current_timestamp(), '{JOB}', s.id_host_source)""")
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

    # 4. replay rows: live upserts, host_move upserts, then one delete per old key
    replay_schema = spark.table(REPLAY_TABLE).schema
    for df, op in ((replay_live_upserts(), "upsert"), (replay_host_move_upserts(), "upsert"), (replay_deletes(), "delete")):
        to_replay_rows(df, replay_schema, JOB, op, "repo").write.format("delta").mode("append").saveAsTable(REPLAY_TABLE)

    # 5. backfill-origin records: RepoBackfill's own parser, placeholder records only
    print(dbutils.notebook.run("../ingest/RepoBackfill", 4 * 3600, {"replay_job": JOB, "rebuild": "false"}))

    show(f"""SELECT replay_op, replay_provenance, count(*) n FROM {REPLAY_TABLE}
             WHERE replay_job = '{JOB}' GROUP BY 1,2 ORDER BY 1,2""")
    print("Done. A Repo pipeline update applies the replay (scheduled 02:20/10:20/18:20 UTC, or run one now); "
          "then the 05:00 End 2 End; then mode=verify.")

# COMMAND ----------

if MODE == "verify":
    show(f"""
      SELECT
        (SELECT count(*) FROM {REKEY_MAP_TABLE} m JOIN {REPO_WORKS} w ON w.native_id = m.old_native_id
          WHERE m.job = '{JOB}') AS old_keys_still_in_repo_works,
        (SELECT count(*) FROM {REKEY_MAP_TABLE} m LEFT ANTI JOIN {REPO_WORKS} w ON w.native_id = m.new_native_id
          WHERE m.job = '{JOB}') AS new_keys_missing_from_repo_works,
        (SELECT count(*) FROM {REPO_WORKS} w JOIN {ENDPOINT_ID_HOST_TABLE} h USING (endpoint_id)
          WHERE {PLACEHOLDER('w.native_id')}) AS placeholder_keys_left_on_listed_endpoints,
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
    print("Replay rows cannot be un-appended (append-only CDF). To reverse the key move itself, remove this "
          "job's endpoint_id_host rows and replay the inverse map.")
