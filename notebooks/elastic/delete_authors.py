# Databricks notebook source
%pip install elasticsearch==8.19.0
%restart_python

# COMMAND ----------
# notebooks/elastic/delete_authors — ES delete pass for authors-v19 (mirrors notebooks/elastic/delete_works, oxjob #784).
# Second task in jobs/sync_authors_to_elasticsearch.yaml, after sync_authors, same job cluster.
# Consumes openalex.authors.deleted_authors rows with es_deleted_at IS NULL; 404 = already gone = done;
# non-404 failures stay unstamped and retry next run.
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers

ELASTIC_INDEX = "authors-v19"
ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
ID_PREFIX = "https://openalex.org/A"
LEDGER = "openalex.authors.deleted_authors"
AUTHORS = "openalex.authors.openalex_authors"

dbutils.widgets.text("guard_fraction", "0.02")          # authors backlog runs at ~1.5%/night; works uses 0.5%
dbutils.widgets.text("deleted_authors_guard_override", "false")
GUARD_FRACTION = float(dbutils.widgets.get("guard_fraction"))
GUARD_OVERRIDE = dbutils.widgets.get("deleted_authors_guard_override").lower() == "true"

client = Elasticsearch(hosts=[ELASTIC_URL], request_timeout=180, max_retries=5, retry_on_timeout=True)

# COMMAND ----------
pending_df = spark.sql(f"SELECT author_id FROM {LEDGER} WHERE es_deleted_at IS NULL")
pending = pending_df.count()
authors_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {AUTHORS}").collect()[0].n
live = client.count(index=ELASTIC_INDEX)["count"]
print(f"pending deletes: {pending:,}; live docs: {live:,}; {AUTHORS}: {authors_rows:,}")

if pending:
    if authors_rows == 0:
        raise Exception(f"ABORT: {AUTHORS} is empty; refusing to trust the ledger.")
    if pending > GUARD_FRACTION * live and not GUARD_OVERRIDE:
        raise Exception(f"ABORT: {pending:,} pending > {GUARD_FRACTION:.1%} of {live:,} live docs; "
                        "re-run with deleted_authors_guard_override=true if sanctioned.")

    ids = [r.author_id for r in pending_df.collect()]

    def actions():
        for aid in ids:
            yield {"_op_type": "delete", "_index": ELASTIC_INDEX, "_id": f"{ID_PREFIX}{aid}"}

    deleted = missing = 0
    failed = []
    for ok, info in helpers.parallel_bulk(client, actions(), chunk_size=700, thread_count=4,
                                          raise_on_error=False, raise_on_exception=False):
        item = info.get("delete", {})
        if ok:
            deleted += 1
        elif item.get("status") == 404:
            missing += 1
        else:
            failed.append(int(item["_id"].rsplit("A", 1)[-1]))
        if (deleted + missing) % 200_000 == 0 and (deleted + missing):
            print(f"  ...{deleted + missing:,} / {len(ids):,}", flush=True)

    print(f"deleted {deleted:,}, already absent {missing:,}, failed {len(failed):,}")
    done = spark.createDataFrame([(i,) for i in ids if i not in set(failed)], "author_id BIGINT")
    done.createOrReplaceTempView("es_done")
    spark.sql(f"""MERGE INTO {LEDGER} l USING es_done d ON l.author_id = d.author_id AND l.es_deleted_at IS NULL
                  WHEN MATCHED THEN UPDATE SET es_deleted_at = current_timestamp()""")
    client.indices.refresh(index=ELASTIC_INDEX)
    if failed:
        raise Exception(f"{len(failed):,} deletes failed (non-404); left unstamped for retry.")

client.close()
