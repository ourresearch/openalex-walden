# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh `openalex.funders.funders_api`

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.funders.funders_api",
    "index_name": "funders-v3"
}

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )
    
    def generate_actions(op_type = "index"):
        for row in partition:
            yield {
                "_op_type": op_type,
                "_index": CONFIG["index_name"],
                "_id": row.id,
                "_source": row._source.asDict(True)
            }
    
    try:
        count = 0
        for success, info in helpers.parallel_bulk(
            client, 
            generate_actions(), 
            chunk_size=500,
            thread_count=4
        ):
            count += 1
            if not success:
                print(f"FAILED TO INDEX: {info}")
                raise Exception(f"Failed to index document: {info}")
        
        print(f"Successfully indexed {count} total documents to {index_name}")
        
    except Exception as e:
        log.error(f"Error indexing documents to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing documents to {index_name}: {e}")

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(f"{CONFIG['table_name']}")
        .withColumn("id", F.concat(F.lit("https://openalex.org/F"), F.col("id")))
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    df = df.repartition(8)
    print(f"Total records to process: {df.count()}")
    
    def send_partition_wrapper(partition):
        return send_partition_to_elastic(
            partition,
            CONFIG['index_name']
        )
    
    df.foreachPartition(send_partition_wrapper)
    
    print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")
    
except Exception as e:
    print(f"Failed to process {CONFIG['table_name']}: {e}")
    log.error(f"Failed to process {CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nIndexing operation completed!")

# COMMAND ----------

client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )

client.indices.refresh(index=CONFIG['index_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge redirects (`merge-funders`): upsert-only
# MAGIC
# MAGIC openalex-elastic-api `funders_id_get` answers a miss with a 301 to the winner when
# MAGIC `merge-funders` holds `{id: loser_url, merge_into_id: winner_url}`. Only
# MAGIC MergeFunders.ipynb used to write it, so merges made by SQL packets / the registry
# MAGIC (63 alias-review pairs + HRB on 2026-09-30) 404ed. This cell adds the MISSING redirects for
# MAGIC `openalex.funders.funders.merge_into_id` (the column CreateFundersAPI filters on), chains
# MAGIC resolved to the live root. It never deletes or changes an existing redirect doc (unmerge is a
# MAGIC manual step, plans/funders/funder-merge-runbook.md). A redirect is written only when its winner
# MAGIC doc is present in the funders index. Any unresolved merge (cycle, depth, root not live),
# MAGIC conflicting existing doc or winner missing from ES raises here, BEFORE the stale-doc delete,
# MAGIC so a loser keeps its old doc (200) rather than 404ing.

# COMMAND ----------

from elasticsearch.helpers import scan, bulk

MERGE_INDEX = "merge-funders"
F_URL = "https://openalex.org/F"


def plan_merge_redirects(merges, live_ids, existing, max_hops=10):
    """merges: {loser_id: merge_into_id}; live_ids: the funders_api id snapshot;
    existing: {doc_id: winner_url} already in merge-funders.
    Returns (desired {loser_url: root_url} for every resolved loser,
             upserts {loser_url: root_url} for MISSING docs only,
             conflicts {loser_url: (existing_url, computed_url)}, unresolved {loser_id: reason}).
    Existing docs are never planned for change or deletion. An existing doc whose target differs
    is accepted only when the redirect graph clients follow (existing docs + this run's upserts)
    leads from it to the same root without a cycle (A->B on file, B->C added now)."""
    def root(start):
        target, seen, hops = start, set(), 0
        while target in merges and hops < max_hops:
            if target in seen:
                return None
            seen.add(target)
            target, hops = merges[target], hops + 1
        return None if target in merges else target

    desired, unresolved = {}, {}
    for loser, first in merges.items():
        if loser in live_ids:
            unresolved[loser] = "loser still live in funders_api"
            continue
        target = root(first)
        if target is None or target == loser:
            unresolved[loser] = "merge chain cycle or too deep"
        elif target not in live_ids:
            unresolved[loser] = f"root F{target} not live in funders_api"
        else:
            desired[f"{F_URL}{loser}"] = f"{F_URL}{target}"
    upserts = {k: v for k, v in desired.items() if k not in existing}
    live_urls = {f"{F_URL}{i}" for i in live_ids}
    graph = dict(existing)
    graph.update(upserts)
    conflicts = {}
    for k, v in desired.items():
        if k not in existing or existing[k] == v:
            continue
        node, seen, ok = existing[k], {k}, True
        while node in graph and node not in seen and len(seen) <= max_hops:
            # every intermediate hop must be a merged (not live) loser resolving to the same root
            if node in live_urls or desired.get(node) != v:
                ok = False
                break
            seen.add(node)
            node = graph[node]
        if not ok or node != v or node in seen:
            conflicts[k] = (existing[k], v)
    return desired, upserts, conflicts, unresolved


def protected_loser_urls(unresolved, conflicts, missing_winner_losers):
    """Losers whose redirect is not proven: the stale-doc delete must leave their entity doc alone."""
    return ({f"{F_URL}{lid}" for lid in unresolved} | set(conflicts) | set(missing_winner_losers))


def check_redirect_plan(unresolved, conflicts, missing_winner_losers):
    """Raise (fail closed, before the stale-doc delete) when any merge could not be published."""
    problems = [f"F{lid}: {why}" for lid, why in sorted(unresolved.items())]
    problems += [f"{k}: existing redirect -> {old}, computed -> {new} (not overwritten)"
                 for k, (old, new) in sorted(conflicts.items())]
    problems += [f"{k}: winner {w} has no doc in the funders index"
                 for k, w in sorted(missing_winner_losers.items())]
    if problems:
        raise Exception(
            f"MERGE-FUNDERS REDIRECT SYNC FAILED: {len(problems)} merged funder(s) without a proven "
            f"redirect; stale-doc delete NOT run, their old docs stay. First: " + "; ".join(problems[:10]))


def missing_winners(client, index_name, desired):
    """{loser_url: winner_url} for every resolved loser whose winner doc is absent from index_name
    (an mget error counts as absent)."""
    winners = sorted(set(desired.values()))
    found = set()
    for i in range(0, len(winners), 500):
        resp = client.mget(index=index_name, ids=winners[i:i + 500], _source=False)
        found |= {d["_id"] for d in resp["docs"] if d.get("found") and not d.get("error")}
    return {k: w for k, w in desired.items() if w not in found}


# ONE funders_api snapshot, read BEFORE merges and reused by the stale-doc delete below
live_ids = {int(r.id) for r in spark.sql(f"SELECT id FROM {CONFIG['table_name']}").collect()}
API_SNAPSHOT_URLS = {f"{F_URL}{i}" for i in live_ids}
try:
    merges = {
        int(r.funder_id): int(r.merge_into_id)
        for r in spark.sql(
            "SELECT funder_id, merge_into_id FROM openalex.funders.funders WHERE merge_into_id IS NOT NULL"
        ).collect()
    }
except Exception as e:
    raise Exception(f"MERGE-FUNDERS REDIRECT SYNC FAILED: could not read merged funders "
                    f"({type(e).__name__}: {e}); merge-funders untouched") from e
existing = {}
if client.indices.exists(index=MERGE_INDEX):
    existing = {
        hit["_id"]: hit["_source"].get("merge_into_id")
        for hit in scan(client, index=MERGE_INDEX, query={"query": {"match_all": {}}})
    }
desired, upserts, conflicts, unresolved = plan_merge_redirects(merges, live_ids, existing)
missing = missing_winners(client, CONFIG["index_name"], desired)   # every loser, kept docs too
writable = {k: v for k, v in upserts.items() if k not in missing and k not in conflicts}

if writable:
    ok, errors = bulk(client, [{"_op_type": "create", "_index": MERGE_INDEX, "_id": k,
                                "_source": {"id": k, "merge_into_id": v}} for k, v in writable.items()],
                      chunk_size=500, raise_on_error=False)
    hard = []
    for e in errors:
        item = e.get("create", {})
        if item.get("status") == 409:              # created by someone else after our scan
            doc = client.get(index=MERGE_INDEX, id=item.get("_id"))
            on_file = doc["_source"].get("merge_into_id")
            if on_file != writable.get(item.get("_id")):
                conflicts[item.get("_id")] = (on_file, writable.get(item.get("_id")))
        else:
            hard.append(e)
    if hard:
        raise Exception(f"MERGE-FUNDERS REDIRECT SYNC FAILED: {len(hard)} merge-funders write "
                        f"failures; stale-doc delete NOT run. First: {hard[0]}")
    client.indices.refresh(index=MERGE_INDEX)
    print(f"merge-funders: {ok} redirects created")

REDIRECT_PROTECTED = protected_loser_urls(unresolved, conflicts, missing)
VERIFIED_LOSERS = set(desired) - REDIRECT_PROTECTED
MERGED_LOSER_URLS = {f"{F_URL}{l}" for l in merges}
print(f"merged rows: {len(merges)}, redirect docs: {len(existing)}, missing redirects: {len(upserts)}, "
      f"created: {len(writable)}, winner not in ES: {len(missing)}, conflicts: {len(conflicts)}, "
      f"unresolved: {len(unresolved)}, verified losers: {len(VERIFIED_LOSERS)}")
check_redirect_plan(unresolved, conflicts, missing)   # raises before the stale-doc delete

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete stale docs (merged funders)
# MAGIC
# MAGIC The sync above is a full-table upsert, so funders that leave `funders_api`
# MAGIC (merge losers get `openalex.funders.funders.merge_into_id` set and are filtered out by
# MAGIC CreateFundersAPI) would otherwise linger in the index forever and keep
# MAGIC resolving on the public API. Mirrors the sources pattern (oxjob #548 C1b):
# MAGIC delete any ES doc whose id is no longer in `funders_api`.

# COMMAND ----------

from elasticsearch.helpers import scan, bulk

# the same funders_api snapshot the merge-redirect cell planned and verified against
if "API_SNAPSHOT_URLS" not in globals():
    raise Exception("stale-doc delete needs API_SNAPSHOT_URLS from the merge-redirect cell; run the notebook in order")
table_ids = API_SNAPSHOT_URLS
es_ids = [
    hit["_id"]
    for hit in scan(client, index=CONFIG["index_name"],
                    query={"query": {"match_all": {}}}, _source=False)
]
# a merged loser is deleted only when its redirect + winner were verified this run
stale_ids = [i for i in es_ids if i not in table_ids and i not in REDIRECT_PROTECTED
             and (i not in MERGED_LOSER_URLS or i in VERIFIED_LOSERS)]
print(f"index docs: {len(es_ids)}, table rows: {len(table_ids)}, stale: {len(stale_ids)}")

# A broken/empty funders_api must never mass-delete the index. Expected volume is
# merge losers only (a handful at a time).
MAX_STALE = 2000
if len(stale_ids) > MAX_STALE:
    raise Exception(f"{len(stale_ids)} stale docs exceeds safety cap {MAX_STALE} — "
                    "funders_api looks wrong, refusing to delete")

if stale_ids:
    deleted, errors = bulk(
        client,
        ({"_op_type": "delete", "_index": CONFIG["index_name"], "_id": i} for i in stale_ids),
        chunk_size=1000,
        raise_on_error=False,
    )
    not_found = [e for e in errors if e.get("delete", {}).get("status") == 404]
    real_errors = [e for e in errors if e.get("delete", {}).get("status") != 404]
    print(f"deleted {deleted} stale docs ({len(not_found)} already gone)")
    if real_errors:
        raise Exception(f"{len(real_errors)} delete failures, first: {real_errors[0]}")
    client.indices.refresh(index=CONFIG["index_name"])
else:
    print("no stale docs")
