# Databricks notebook source
# MAGIC %md
# MAGIC # Delete Removed Sources from Elasticsearch
# MAGIC
# MAGIC The sources ES sync upserts but never deletes, so sources removed from the
# MAGIC registry (merged duplicates, oxjob #629; later the D1 tombstone purge) live on
# MAGIC in the index as ghosts. This notebook diffs the index against the
# MAGIC authoritative table and deletes documents that no longer exist there.
# MAGIC
# MAGIC Run on demand after a merge batch (not scheduled). Dry-run by default.
# MAGIC
# MAGIC Safeguards (per oxjob #629 ES-DELETE-PROPOSAL, mechanism per Casey 2026-07-17:
# MAGIC "detect and delete in the API... you have to guard it [so] a bad read
# MAGIC [doesn't] delete the whole thing"):
# MAGIC 1. Abort if the authoritative table has fewer than `min_table_count` rows
# MAGIC    (empty/partial-read detector).
# MAGIC 2. Abort if the diff exceeds `max_deletes` (mass-delete cap).
# MAGIC 3. `dry_run=true` by default — prints what it would delete, touches nothing.
# MAGIC 4. Every deleted id is logged, so a resync can restore any mistake.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

from datetime import datetime, timezone
from elasticsearch import Elasticsearch, helpers

dbutils.widgets.text("index_name", "sources-v3", "ES index name")
dbutils.widgets.text("table_name", "openalex.sources.sources_api", "Authoritative table")
dbutils.widgets.text("max_deletes", "2000", "Abort if diff exceeds this (mass-delete cap)")
dbutils.widgets.text("min_table_count", "250000", "Abort if table smaller than this (bad-read detector)")
dbutils.widgets.text("dry_run", "true", "true = print only, delete nothing")

INDEX_NAME = dbutils.widgets.get("index_name").strip()
TABLE_NAME = dbutils.widgets.get("table_name").strip()
MAX_DELETES = int(dbutils.widgets.get("max_deletes"))
MIN_TABLE_COUNT = int(dbutils.widgets.get("min_table_count"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() != "false"

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
es = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=60, max_retries=3)


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}")

# COMMAND ----------

# MAGIC %md ## 1. Authoritative ids (with bad-read guard)

# COMMAND ----------

# Same id shape the sync writes: https://openalex.org/S{id}
rows = spark.sql(f"SELECT id FROM {TABLE_NAME}").collect()
table_ids = {f"https://openalex.org/S{r.id}" for r in rows}
log(f"{TABLE_NAME}: {len(table_ids):,} ids")

if len(table_ids) < MIN_TABLE_COUNT:
    raise RuntimeError(
        f"Aborting: table has {len(table_ids):,} rows < min_table_count "
        f"{MIN_TABLE_COUNT:,} — refusing to diff against a possibly bad read"
    )

# COMMAND ----------

# MAGIC %md ## 2. Index ids

# COMMAND ----------

es_ids = set()
for doc in helpers.scan(
    es, index=INDEX_NAME, query={"query": {"match_all": {}}}, _source=False, size=5000
):
    es_ids.add(doc["_id"])
log(f"{INDEX_NAME}: {len(es_ids):,} docs")

# COMMAND ----------

# MAGIC %md ## 3. Diff (with mass-delete cap)

# COMMAND ----------

to_delete = sorted(es_ids - table_ids)
log(f"diff: {len(to_delete):,} docs in the index but not in {TABLE_NAME}")

if len(to_delete) > MAX_DELETES:
    raise RuntimeError(
        f"Aborting: {len(to_delete):,} deletions exceeds max_deletes {MAX_DELETES:,} "
        f"— raise the cap deliberately if this is a planned batch"
    )

for _id in to_delete:
    log(f"{'DRY-RUN would delete' if DRY_RUN else 'deleting'}: {_id}")

# COMMAND ----------

# MAGIC %md ## 4. Delete (skipped on dry run)

# COMMAND ----------

if DRY_RUN:
    dbutils.notebook.exit(f"dry_run: {len(to_delete)} would be deleted")

if not to_delete:
    dbutils.notebook.exit("nothing to delete")

ok = 0
for success, info in helpers.parallel_bulk(
    es,
    ({"_op_type": "delete", "_index": INDEX_NAME, "_id": _id} for _id in to_delete),
    chunk_size=500,
    thread_count=2,
    raise_on_error=False,
):
    if success:
        ok += 1
    else:
        # 404s mean it vanished between scan and delete — fine; log anything else
        log(f"delete issue: {info}")

es.indices.refresh(index=INDEX_NAME)
log(f"deleted {ok:,}/{len(to_delete):,}; index refreshed")
dbutils.notebook.exit(f"deleted {ok} of {len(to_delete)}")
