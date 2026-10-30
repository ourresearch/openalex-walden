# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merged author, source and work IDs: one loser -> winner map per entity (oxjob #1124, works #1037)
# MAGIC
# MAGIC A merged-away ID resolves to the survivor, best effort with no permanence promise (Jason, 2026-10-26 "Kyle's way";
# MAGIC works and the no-promise wording 2026-10-27, #1037). This notebook
# MAGIC builds the map from the merge ledgers we already keep, follows chains to a live winner, writes
# MAGIC `openalex.authors.merged_author_ids` / `openalex.sources.merged_source_ids` (the snapshot's
# MAGIC `merged_ids.csv.gz`, see `notebooks/snapshot/export_merged_ids`), and mirrors it into the
# MAGIC Elasticsearch index `merge-authors` / `merge-sources`. openalex-elastic-api reads that index to
# MAGIC answer a loser ID with a 301 to the winner and to resolve loser IDs in filters (same doc shape
# MAGIC as `merge-funders`: `{id, merge_into_id}` keyed by the loser's full URL).
# MAGIC
# MAGIC **Ledgers**
# MAGIC - authors: profkit's merge decisions `openalex.authors.oxjob1305_apply_decisions` (rows not
# MAGIC   reverted); profkit seat-move runs (`openalex.authors.profkit_seatmv_runs`, not reverted), where
# MAGIC   a profile whose every moved seat went to one profile counts as merged into it; and the pre-2026
# MAGIC   snapshot `merged_ids` files (`s3://openalex/legacy-data/merged_ids/authors/`, frozen 2025-10-14)
# MAGIC   minus the "deleted author" target A5317838346 and the null author A9999999999; and renamed (re-minted)
# MAGIC   profiles whose seats all moved to one new ID (`openalex.authors.author_rename_edges`, #1932).
# MAGIC - sources: the registry's `openalex_sources.public.source_merge` plus the legacy files.
# MAGIC - works (2026-10-27, #1037): `openalex.works.merged_work_ids` plus the legacy files minus the "Deleted Work"
# MAGIC   placeholder W4285719527. Map table `openalex.works.merged_work_ids_map` (the ledger owns the plain name).
# MAGIC
# MAGIC **Rules.** One edge per loser (the latest). A loser that is live again (the matcher refilled it)
# MAGIC is left out: its own record answers. A chain whose end is not live after 8 hops is left out:
# MAGIC that ID stays 404, like a deleted (not merged) ID. Deleted IDs are never mapped.
# MAGIC
# MAGIC Seat-move prestate tables live in `openalex.transient` (purged eventually), so each run's
# MAGIC per-profile seat counts are copied once into `openalex.authors.author_seat_move_counts`.

# COMMAND ----------

from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers

dbutils.widgets.dropdown("entity", "authors", ["authors", "sources", "works"])
dbutils.widgets.text("max_shrink_fraction", "0.02")
dbutils.widgets.text("shrink_guard_override", "false")
dbutils.widgets.text("full_resync", "false")

ENTITY = dbutils.widgets.get("entity")
MAX_SHRINK = float(dbutils.widgets.get("max_shrink_fraction"))
SHRINK_OVERRIDE = dbutils.widgets.get("shrink_guard_override").lower() == "true"
FULL_RESYNC = dbutils.widgets.get("full_resync").lower() == "true"
MAX_HOPS = 8

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CFG = {
    "authors": {
        "prefix": "A",
        "live": "openalex.authors.openalex_authors",
        "map": "openalex.authors.merged_author_ids",
        "synced": "openalex.authors.merged_author_ids_in_es",
        "legacy_table": "openalex.authors.author_merge_edges_legacy",
        "legacy_s3": "s3://openalex/legacy-data/merged_ids/authors/",
        "legacy_exclude": ("A5317838346", "A9999999999"),
        "es_index": "merge-authors",
        "shards": 2,
        "partitions": 512,
    },
    "sources": {
        "prefix": "S",
        "live": "openalex.sources.sources_api",
        "map": "openalex.sources.merged_source_ids",
        "synced": "openalex.sources.merged_source_ids_in_es",
        "legacy_table": "openalex.sources.source_merge_edges_legacy",
        "legacy_s3": "s3://openalex/legacy-data/merged_ids/sources/",
        "legacy_exclude": (),
        "es_index": "merge-sources",
        "shards": 1,
        "partitions": 8,
    },
    # works (Jason 2026-10-27 17:33 CT, #1037): best effort, no permanence promise. The ledger table is already
    # called merged_work_ids, so the map takes a _map suffix. Legacy rows into W4285719527 ("Deleted Work"
    # placeholder, 11.2M) are deletions, not merges.
    "works": {
        "prefix": "W",
        "live": "openalex.works.openalex_works",
        "map": "openalex.works.merged_work_ids_map",
        "synced": "openalex.works.merged_work_ids_map_in_es",
        "legacy_table": "openalex.works.work_merge_edges_legacy",
        "legacy_s3": "s3://openalex/legacy-data/merged_ids/works/",
        "legacy_exclude": ("W4285719527",),
        "es_index": "merge-works",
        "shards": 2,
        "partitions": 512,
    },
}[ENTITY]
P = CFG["prefix"]
print(f"entity={ENTITY} live={CFG['live']} map={CFG['map']} es={CFG['es_index']}")

# COMMAND ----------

# MAGIC %md #### Legacy edges (copied once from the frozen pre-2026 snapshot files)

# COMMAND ----------

if not spark.catalog.tableExists(CFG["legacy_table"]):
    excl = ", ".join(f"'{x}'" for x in CFG["legacy_exclude"]) or "''"
    spark.sql(f"""
      CREATE TABLE {CFG['legacy_table']}
      COMMENT 'Pre-2026 snapshot merged_ids/{ENTITY} rows (frozen 2025-10-14), copied by notebooks/elastic/sync_merged_ids (oxjob #1124)'
      AS SELECT DISTINCT
        CAST(substr(id, 2) AS BIGINT) AS loser_id,
        CAST(substr(merge_into_id, 2) AS BIGINT) AS winner_id,
        CAST(merge_date AS DATE) AS merge_date
      FROM read_files('{CFG['legacy_s3']}', format => 'csv', header => true)
      WHERE id LIKE '{P}%' AND merge_into_id LIKE '{P}%' AND merge_into_id NOT IN ({excl})
    """)
print(f"{CFG['legacy_table']}: {spark.table(CFG['legacy_table']).count():,} rows")

# COMMAND ----------

# MAGIC %md #### Authors: copy new seat-move runs' per-profile counts (prestate tables are transient)

# COMMAND ----------

SEAT_COUNTS = "openalex.authors.author_seat_move_counts"

if ENTITY == "authors":
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {SEAT_COUNTS} (
        run_id STRING, from_author_id BIGINT, to_author_id BIGINT, seats BIGINT, applied_at TIMESTAMP)
      COMMENT 'Seats moved per (run, from, to) by profkit apply_seat_moves.py; to NULL = blanked. Copied from each run prestate table by notebooks/elastic/sync_merged_ids (oxjob #1124)'
    """)
    done = {r.run_id for r in spark.sql(f"SELECT DISTINCT run_id FROM {SEAT_COUNTS}").collect()}
    runs = spark.sql("SELECT run_id, applied_at FROM openalex.authors.profkit_seatmv_runs").collect()
    for r in runs:
        if r.run_id in done:
            continue
        stem = r.run_id.replace("-", "_").replace(".", "_")
        src = next((t for t in (f"openalex.transient.profkit_seatmv_prestate_{stem}",
                                f"openalex.authors.profkit_seatmv_prestate_{stem}")
                    if spark.catalog.tableExists(t)), None)
        if src is None:
            print(f"WARNING: no prestate table for seat-move run {r.run_id}; its emptied profiles are not mapped")
            continue
        spark.sql(f"""
          INSERT INTO {SEAT_COUNTS}
          SELECT '{r.run_id}', from_author_id, to_author_id, COUNT(*), TIMESTAMP'{r.applied_at}'
          FROM {src} WHERE from_author_id IS NOT NULL
          GROUP BY from_author_id, to_author_id
        """)
        print(f"copied seat counts for {r.run_id} from {src}")

# COMMAND ----------

# MAGIC %md #### Authors: renamed (re-minted) profiles, copied from their finders' scratch tables
# MAGIC #1932 (2026-10-30): a profile whose every seat moved to one new ID is the same profile under another ID,
# MAGIC so it resolves like a merge. Scratch tables in openalex.transient get purged, so rows are kept here.

# COMMAND ----------

RENAMES = "openalex.authors.author_rename_edges"
RENAME_SOURCES = ["openalex.transient.oxjob1932_dead_author_ids"]  # dead_id, current_id, night, seats, dead_seats

if ENTITY == "authors":
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {RENAMES} (loser_id BIGINT, winner_id BIGINT, merge_date DATE, source STRING)
      COMMENT 'Renamed (re-minted) author IDs -> successor; whole moves only. Read by notebooks/elastic/sync_merged_ids (oxjob #1124, #1932)'
    """)
    for src in RENAME_SOURCES:
        if spark.catalog.tableExists(src):
            spark.sql(f"""
              MERGE INTO {RENAMES} t
              USING (SELECT dead_id AS loser_id, current_id AS winner_id, night AS merge_date, '{src}' AS source
                     FROM {src} WHERE seats = dead_seats) s
              ON t.loser_id = s.loser_id
              WHEN NOT MATCHED THEN INSERT *
            """)
    print(f"{RENAMES}: {spark.table(RENAMES).count():,} rows")

# COMMAND ----------

# MAGIC %md #### Edges: one per loser, the latest

# COMMAND ----------

if ENTITY == "authors":
    edges_sql = f"""
      SELECT loser_id, winner_id, CAST(applied_at AS DATE) AS merge_date, 'profkit_merge' AS ledger, 3 AS prio
      FROM openalex.authors.oxjob1305_apply_decisions WHERE reverted_at IS NULL
      UNION ALL
      SELECT from_author_id, max_by(to_author_id, seats), CAST(max(applied_at) AS DATE), 'profkit_seat_moves', 2
      FROM (
        SELECT c.from_author_id, c.to_author_id, SUM(c.seats) AS seats, MAX(c.applied_at) AS applied_at
        FROM {SEAT_COUNTS} c JOIN openalex.authors.profkit_seatmv_runs r
          ON c.run_id = r.run_id AND r.reverted_at IS NULL
        GROUP BY c.from_author_id, c.to_author_id)
      GROUP BY from_author_id
      -- merged = every moved seat went to one profile (none blanked, none split)
      HAVING COUNT(*) = 1 AND max_by(to_author_id, seats) IS NOT NULL
      UNION ALL
      SELECT loser_id, winner_id, merge_date, 'rematch_renames', 2 FROM {RENAMES}
      UNION ALL
      SELECT loser_id, winner_id, merge_date, 'legacy_merged_ids', 1 FROM {CFG['legacy_table']}
    """
elif ENTITY == "works":
    # every works merge path logs here (merge waves, version groups, #1349 dead-ID repoints); CreateWorksEnriched
    # already repoints citations through it
    edges_sql = f"""
      SELECT loser_work_id AS loser_id, winner_work_id AS winner_id, CAST(merged_at AS DATE) AS merge_date,
             'merged_work_ids' AS ledger, 2 AS prio
      FROM openalex.works.merged_work_ids
      UNION ALL
      SELECT loser_id, winner_id, merge_date, 'legacy_merged_ids', 1 FROM {CFG['legacy_table']}
    """
else:
    edges_sql = f"""
      SELECT loser_id, winner_id, CAST(merged_at AS DATE) AS merge_date, 'source_merge' AS ledger, 2 AS prio
      FROM openalex_sources.public.source_merge
      UNION ALL
      SELECT loser_id, winner_id, merge_date, 'legacy_merged_ids', 1 FROM {CFG['legacy_table']}
    """

# materialized: the chain walk below reads it once per hop
EDGES = CFG["map"] + "_edges"
spark.sql(f"""
  CREATE OR REPLACE TABLE {EDGES}
  COMMENT 'One merge edge per loser (latest) from the {ENTITY} merge ledgers; input to {CFG["map"]} (oxjob #1124)'
  AS SELECT loser_id, winner_id, merge_date, ledger FROM (
    SELECT *, row_number() OVER (PARTITION BY loser_id ORDER BY merge_date DESC, prio DESC) AS rn
    FROM ({edges_sql}) WHERE loser_id IS NOT NULL AND winner_id IS NOT NULL AND loser_id <> winner_id)
  WHERE rn = 1
""")
spark.sql(f"CREATE OR REPLACE TEMP VIEW e AS SELECT * FROM {EDGES}")

live_n = spark.sql(f"SELECT COUNT(*) AS n FROM {CFG['live']}").first().n
if live_n == 0:
    raise Exception(f"ABORT: {CFG['live']} is empty; refusing to build the map.")
print(f"live {ENTITY}: {live_n:,}")
# the chain walk only needs liveness for IDs that appear in an edge (463M live works vs ~40M edges)
LIVE = CFG["map"] + "_live"
spark.sql(f"""
  CREATE OR REPLACE TABLE {LIVE} AS
  SELECT DISTINCT l.id FROM {CFG['live']} l
  LEFT SEMI JOIN (SELECT loser_id AS id FROM {EDGES} UNION SELECT winner_id FROM {EDGES}) x ON l.id = x.id
""")
spark.sql(f"CREATE OR REPLACE TEMP VIEW live AS SELECT id FROM {LIVE}")
spark.sql("SELECT ledger, COUNT(*) AS edges FROM e GROUP BY ledger ORDER BY ledger").show()

# COMMAND ----------

# MAGIC %md #### Follow chains to a live winner

# COMMAND ----------

hops = ["h0 AS (SELECT loser_id, winner_id AS w, merge_date, ledger FROM e)"]
for i in range(1, MAX_HOPS + 1):
    hops.append(f"""h{i} AS (
      SELECT h.loser_id, CASE WHEN lv.id IS NULL AND n.winner_id IS NOT NULL THEN n.winner_id ELSE h.w END AS w,
             h.merge_date, h.ledger
      FROM h{i-1} h LEFT JOIN live lv ON h.w = lv.id LEFT JOIN e n ON h.w = n.loser_id)""")
spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW resolved AS
  WITH {', '.join(hops)}
  SELECT h.loser_id, h.w, h.merge_date, h.ledger,
         lw.id IS NOT NULL AS winner_live, ll.id IS NOT NULL AS loser_live
  FROM h{MAX_HOPS} h LEFT JOIN live lw ON h.w = lw.id LEFT JOIN live ll ON h.loser_id = ll.id
""")
stats = spark.sql("""
  SELECT ledger, COUNT(*) AS edges,
         SUM(CASE WHEN loser_live THEN 1 ELSE 0 END) AS loser_live_left_out,
         SUM(CASE WHEN NOT loser_live AND NOT winner_live THEN 1 ELSE 0 END) AS winner_gone_left_out,
         SUM(CASE WHEN NOT loser_live AND winner_live AND loser_id <> w THEN 1 ELSE 0 END) AS mapped
  FROM resolved GROUP BY ledger ORDER BY ledger
""")
stats.show(truncate=False)

new_df = spark.sql(f"""
  SELECT loser_id AS id, w AS merge_into_id, merge_date, ledger
  FROM resolved WHERE NOT loser_live AND winner_live AND loser_id <> w
""")

# COMMAND ----------

# MAGIC %md #### Write the map (shrink guard first)

# COMMAND ----------

new_n = new_df.count()
old_n = spark.table(CFG["map"]).count() if spark.catalog.tableExists(CFG["map"]) else 0
print(f"{CFG['map']}: {old_n:,} -> {new_n:,}")
if old_n and new_n < (1 - MAX_SHRINK) * old_n and not SHRINK_OVERRIDE:
    raise Exception(f"ABORT: map would shrink {old_n:,} -> {new_n:,} (> {MAX_SHRINK:.0%}); "
                    "a ledger or the live table is probably incomplete. Re-run with shrink_guard_override=true if sanctioned.")

new_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG["map"])
spark.sql(f"COMMENT ON TABLE {CFG['map']} IS 'Merged-away {ENTITY} id -> live winner (chains followed). "
          f"Built nightly by notebooks/elastic/sync_merged_ids from the merge ledgers; served as 301s via ES {CFG['es_index']} "
          f"and published as {ENTITY}/merged_ids.csv.gz in the snapshot (oxjob #1124).'")

# COMMAND ----------

# MAGIC %md #### Mirror into Elasticsearch (diff against what the last run wrote)

# COMMAND ----------

ES_INDEX = CFG["es_index"]
client = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True)
created = False
if FULL_RESYNC and client.indices.exists(index=ES_INDEX):
    client.indices.delete(index=ES_INDEX)
    print(f"full_resync: deleted ES index {ES_INDEX}")
if not client.indices.exists(index=ES_INDEX):
    # replicas 0 for the first bulk load; set to 1 once it is in
    client.indices.create(
        index=ES_INDEX,
        settings={"number_of_shards": CFG["shards"], "number_of_replicas": 0, "refresh_interval": "30s"},
        mappings={"dynamic": "strict", "properties": {
            "id": {"type": "keyword"}, "merge_into_id": {"type": "keyword"}, "merge_date": {"type": "date"}}},
    )
    created = True
    print(f"created ES index {ES_INDEX}")

new_tbl = spark.table(CFG["map"]).select("id", "merge_into_id", "merge_date")
if created or not spark.catalog.tableExists(CFG["synced"]):
    upserts = new_tbl
    deletes = spark.createDataFrame([], "id BIGINT")
else:
    old_tbl = spark.table(CFG["synced"]).select("id", "merge_into_id", "merge_date")
    upserts = new_tbl.join(old_tbl, ["id", "merge_into_id", "merge_date"], "left_anti")
    deletes = old_tbl.select("id").join(new_tbl.select("id"), "id", "left_anti")

ops = (upserts.select(F.lit("index").alias("op"), "id", "merge_into_id", F.col("merge_date").cast("string").alias("merge_date"))
       .unionByName(deletes.select(F.lit("delete").alias("op"), "id",
                                   F.lit(None).cast("bigint").alias("merge_into_id"), F.lit(None).cast("string").alias("merge_date"))))
n_up, n_del = upserts.count(), deletes.count()
print(f"ES {ES_INDEX}: {n_up:,} upserts, {n_del:,} deletes")

URL = "https://openalex.org/" + P

def send_partition(rows):
    es = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True)

    def actions():
        for r in rows:
            if r.op == "index":
                yield {"_op_type": "index", "_index": ES_INDEX, "_id": f"{URL}{r.id}",
                       "_source": {"id": f"{URL}{r.id}", "merge_into_id": f"{URL}{r.merge_into_id}",
                                   "merge_date": r.merge_date}}
            else:
                yield {"_op_type": "delete", "_index": ES_INDEX, "_id": f"{URL}{r.id}"}

    # thread_count 2: the cluster serves production; this is a background mirror
    for ok, info in helpers.parallel_bulk(es, actions(), chunk_size=2000, thread_count=2,
                                          raise_on_error=False, raise_on_exception=False):
        if not ok and info.get("delete", {}).get("status") != 404:
            raise Exception(f"ES bulk failure on {ES_INDEX}: {info}")

if n_up + n_del:
    n_parts = max(1, min(CFG["partitions"], (n_up + n_del) // 50_000 + 1))
    ops.repartition(n_parts).foreachPartition(send_partition)

if created:
    client.indices.put_settings(index=ES_INDEX, settings={"index": {"number_of_replicas": 1}})
client.indices.refresh(index=ES_INDEX)
es_n = client.count(index=ES_INDEX)["count"]
print(f"ES {ES_INDEX}: {es_n:,} docs; map {new_n:,} rows")
if es_n != new_n:
    raise Exception(f"ES {ES_INDEX} holds {es_n:,} docs but the map has {new_n:,}; re-run with full_resync=true")

spark.table(CFG["map"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG["synced"])
print(f"{CFG['synced']} updated")
