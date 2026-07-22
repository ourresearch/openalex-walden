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
# MAGIC ### Delete stale docs (merged funders)
# MAGIC
# MAGIC The sync above is a full-table upsert, so funders that leave `funders_api`
# MAGIC (merge losers get `mid.funder.merge_into_id` set and are filtered out by
# MAGIC CreateFundersAPI) would otherwise linger in the index forever and keep
# MAGIC resolving on the public API. Mirrors the sources pattern (oxjob #548 C1b):
# MAGIC delete any ES doc whose id is no longer in `funders_api`.

# COMMAND ----------

from elasticsearch.helpers import scan, bulk

table_ids = {
    f"https://openalex.org/F{r.id}"
    for r in spark.sql(f"SELECT id FROM {CONFIG['table_name']}").collect()
}
es_ids = [
    hit["_id"]
    for hit in scan(client, index=CONFIG["index_name"],
                    query={"query": {"match_all": {}}}, _source=False)
]
stale_ids = [i for i in es_ids if i not in table_ids]
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
