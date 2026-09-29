# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync `openalex.common.keywords_api` to Elasticsearch

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

# Delete ES docs whose id is no longer in keywords_api (oxjob #1322). Leave false until the works
# backfill after a vocabulary swap has finished, so not-yet-backfilled works still resolve their ids.
dbutils.widgets.text("delete_stale", "false")
DELETE_STALE = dbutils.widgets.get("delete_stale").lower() == "true"

CONFIG = {
    "table_name": "openalex.common.keywords_api",
    "index_name": "keywords-v1"
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
        raise

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(f"{CONFIG['table_name']}")
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
    raise

print("\nIndexing operation completed!")

# COMMAND ----------

client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )

client.indices.refresh(index=CONFIG['index_name'])

# COMMAND ----------

# Stale-doc sweep (oxjob #1322): the index is rewritten in full from the table above every run, so the
# table is the complete truth; anything in ES that is not in it is a retired keyword.
if DELETE_STALE:
    table_ids = {r.id for r in spark.table(CONFIG["table_name"]).select("id").collect()}
    if len(table_ids) < 500_000:
        raise Exception(f"{CONFIG['table_name']} has only {len(table_ids):,} rows; refusing to delete from {CONFIG['index_name']}")
    es_ids = [h["_id"] for h in helpers.scan(client, index=CONFIG["index_name"],
                                            query={"query": {"match_all": {}}}, _source=False)]
    stale = [i for i in es_ids if i not in table_ids]
    print(f"ES docs {len(es_ids):,}; table rows {len(table_ids):,}; stale {len(stale):,}")
    if len(stale) > 0.5 * len(es_ids):
        raise Exception(f"{len(stale):,} of {len(es_ids):,} docs would be deleted; refusing (delete by hand if intended)")
    ok, errors = helpers.bulk(client, ({"_op_type": "delete", "_index": CONFIG["index_name"], "_id": i} for i in stale),
                              chunk_size=1000, raise_on_error=False)
    print(f"Deleted {ok:,} stale docs; {len(errors)} errors")
    if errors:
        raise Exception(f"{len(errors)} delete errors, first: {errors[0]}")
    client.indices.refresh(index=CONFIG["index_name"])
else:
    print("delete_stale=false: stale-doc sweep skipped")
