# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh `openalex.sources.sources_api`

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
    "table_name": "openalex.publishers.publishers_api",
    "index_name": "publishers-v6"
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
        .withColumn("id", F.concat(F.lit("https://openalex.org/P"), F.col("id")))
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    df = df.repartition(32)
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

# delete index if exists
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=5,
)
#if client.indices.exists(index=ELASTIC_INDEX):
client.indices.refresh(index=CONFIG["index_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop the docs of merged publishers (oxjob #2299)
# MAGIC
# MAGIC The loop above only indexes publishers_api rows, and publishers_api leaves merged publishers out, so a merged
# MAGIC publisher kept serving its last doc (old name, old roles, old counts) until someone deleted it by hand. This cell
# MAGIC deletes the doc of every publisher whose row in openalex.publishers.publishers carries a merge_into_id and that
# MAGIC is not in publishers_api; live publishers are never touched. More than MAX_MERGED_DROPS docs to delete in one run
# MAGIC stops the cell instead: that many new merges in a day is a bug to look at, not a cleanup.

# COMMAND ----------

MAX_MERGED_DROPS = 500
merged_ids = [f"https://openalex.org/P{r.id}" for r in spark.sql(f"""
    SELECT p.id FROM openalex.publishers.publishers p
    LEFT ANTI JOIN {CONFIG['table_name']} a ON a.id = p.id
    WHERE p.merge_into_id IS NOT NULL
""").collect()]
present = []
for i in range(0, len(merged_ids), 500):
    resp = client.mget(index=CONFIG["index_name"], ids=merged_ids[i:i + 500], _source=False)
    present += [d["_id"] for d in resp["docs"] if d.get("found")]
print(f"Merged publishers: {len(merged_ids)}; their docs still in {CONFIG['index_name']}: {len(present)}")
if len(present) > MAX_MERGED_DROPS:
    raise Exception(f"{len(present)} merged-publisher docs to delete (over {MAX_MERGED_DROPS}); none deleted. First: {present[:10]}")
if present:
    deleted, errors = helpers.bulk(
        client,
        ({"_op_type": "delete", "_index": CONFIG["index_name"], "_id": doc_id} for doc_id in present),
        raise_on_error=False,
    )
    print(f"Deleted {deleted} merged-publisher docs: {present[:50]}")
    if errors:
        raise Exception(f"Merged-publisher doc deletes failed: {errors[:5]}")
    client.indices.refresh(index=CONFIG["index_name"])
