# Databricks notebook source
# NOTE: Like sync_indexes, this tiny entity (one row per external source list) builds its
# API table and syncs it to Elasticsearch in one notebook.
#
# "source-lists" (oxjob #1205) is the vocabulary behind sources.listed_in — the external
# journal lists a source appears on (cwts-core, doaj, doyens, ...). Unlike indexes, the
# vocabulary is NOT hardcoded here: the registry table openalex_sources.public.source_list
# is the single source of truth (id, display_name, maintainer, url, scope, list_version),
# so a list loaded there by hand (jobs.load_source_list) shows up here on the next run.
# Non-normative by design: a list's row says who maintains it and what it covers, never
# that OpenAlex endorses it.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists("openalex.common.source_lists_api"):
    spark.sql("""
        CREATE OR REPLACE TABLE openalex.common.source_lists_api_hash AS
        SELECT id, updated_date,
            xxhash64(CONCAT_WS('|',
                COALESCE(display_name, ''),
                COALESCE(description, ''),
                COALESCE(maintainer, ''),
                COALESCE(url, ''),
                COALESCE(CAST(list_version AS STRING), ''),
                COALESCE(CAST(sources_count AS STRING), ''),
                COALESCE(CAST(works_count AS STRING), ''),
                COALESCE(CAST(cited_by_count AS STRING), '')
            )) AS content_hash
        FROM openalex.common.source_lists_api
    """)
else:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS openalex.common.source_lists_api_hash (
            id STRING, updated_date TIMESTAMP, content_hash BIGINT
        )
    """)
print("Hash snapshot complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rebuild source_lists_api. Vocabulary + metadata come from the registry; counts from
# MAGIC -- the mirrored sources table and the works table.
# MAGIC -- works_count follows the filter the doc links to (primary_location.source.listed_in)
# MAGIC -- and is core-corpus only (is_xpac IS NOT TRUE), same reasoning as sync_indexes.
# MAGIC CREATE OR REPLACE TABLE openalex.common.source_lists_api AS
# MAGIC WITH vocab AS (
# MAGIC     SELECT id, display_name, maintainer, url, scope, list_version
# MAGIC     FROM openalex_sources.public.source_list
# MAGIC ),
# MAGIC source_counts AS (
# MAGIC     SELECT list_id, COUNT(*) AS sources_count
# MAGIC     FROM openalex.sources.sources_api
# MAGIC     LATERAL VIEW explode(listed_in) AS list_id
# MAGIC     GROUP BY list_id
# MAGIC ),
# MAGIC work_counts AS (
# MAGIC     SELECT list_id,
# MAGIC         COUNT(*) AS works_count,
# MAGIC         SUM(cited_by_count) AS cited_by_count
# MAGIC     FROM openalex.works.openalex_works w
# MAGIC     LATERAL VIEW explode(w.primary_location.source.listed_in) AS list_id
# MAGIC     WHERE w.is_xpac IS NOT TRUE
# MAGIC     GROUP BY list_id
# MAGIC )
# MAGIC SELECT
# MAGIC     CONCAT('https://openalex.org/source-lists/', v.id) AS id,
# MAGIC     v.display_name,
# MAGIC     v.scope AS description,
# MAGIC     v.maintainer,
# MAGIC     v.url,
# MAGIC     v.list_version,
# MAGIC     COALESCE(sc.sources_count, 0) AS sources_count,
# MAGIC     COALESCE(wc.works_count, 0) AS works_count,
# MAGIC     COALESCE(wc.cited_by_count, 0) AS cited_by_count,
# MAGIC     CONCAT('https://api.openalex.org/sources?filter=listed_in:', v.id) AS sources_api_url,
# MAGIC     CONCAT('https://api.openalex.org/works?filter=primary_location.source.listed_in:', v.id) AS works_api_url,
# MAGIC     TIMESTAMP '2026-09-16 00:00:00' AS created_date,
# MAGIC     CAST(NULL AS TIMESTAMP) AS updated_date
# MAGIC FROM vocab v
# MAGIC LEFT JOIN source_counts sc ON sc.list_id = v.id
# MAGIC LEFT JOIN work_counts wc ON wc.list_id = v.id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set updated_date based on content hash comparison
# MAGIC WITH new_hashes AS (
# MAGIC     SELECT id,
# MAGIC         xxhash64(CONCAT_WS('|',
# MAGIC             COALESCE(display_name, ''),
# MAGIC             COALESCE(description, ''),
# MAGIC             COALESCE(maintainer, ''),
# MAGIC             COALESCE(url, ''),
# MAGIC             COALESCE(CAST(list_version AS STRING), ''),
# MAGIC             COALESCE(CAST(sources_count AS STRING), ''),
# MAGIC             COALESCE(CAST(works_count AS STRING), ''),
# MAGIC             COALESCE(CAST(cited_by_count AS STRING), '')
# MAGIC         )) AS content_hash
# MAGIC     FROM openalex.common.source_lists_api
# MAGIC )
# MAGIC MERGE INTO openalex.common.source_lists_api AS target
# MAGIC USING (
# MAGIC     SELECT n.id,
# MAGIC         CASE
# MAGIC             WHEN p.id IS NULL THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             WHEN n.content_hash <> p.content_hash THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
# MAGIC             ELSE p.updated_date
# MAGIC         END AS new_updated_date
# MAGIC     FROM new_hashes n
# MAGIC     LEFT JOIN openalex.common.source_lists_api_hash p ON n.id = p.id
# MAGIC ) AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET target.updated_date = source.new_updated_date

# COMMAND ----------

from pyspark.sql import functions as F
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.common.source_lists_api",
    "index_name": "source-lists-v1"
}

# Created here on first run (the index is too small to justify a separate mapping
# step, and dynamic mapping would lock list_version/url in as text). Shape copied
# from oa-statuses / indexes: keyword id with a .lower subfield, searchable
# display_name with autocomplete.
INDEX_BODY = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "refresh_interval": "30s",
            "analysis": {
                "normalizer": {"lower": {"filter": ["lowercase"]}},
                "analyzer": {
                    "search_analyzer": {
                        "filter": ["lowercase", "kstem", "stop"],
                        "tokenizer": "standard",
                    }
                },
            },
        }
    },
    "mappings": {
        "properties": {
            "id": {
                "type": "keyword",
                "fields": {"lower": {"type": "keyword", "normalizer": "lower"}},
            },
            "display_name": {
                "type": "text",
                "analyzer": "search_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "search_as_you_type",
                        "doc_values": False,
                        "max_shingle_size": 2,
                    },
                    "keyword": {"type": "keyword"},
                },
            },
            "description": {
                "type": "text",
                "analyzer": "search_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "search_as_you_type",
                        "doc_values": False,
                        "max_shingle_size": 2,
                    }
                },
            },
            "maintainer": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "url": {"type": "keyword", "index": False},
            "list_version": {"type": "date"},
            "sources_count": {"type": "long"},
            "works_count": {"type": "long"},
            "cited_by_count": {"type": "long"},
            "sources_api_url": {"type": "keyword", "index": False},
            "works_api_url": {"type": "keyword", "index": False},
            "created_date": {"type": "date"},
            "updated_date": {"type": "date"},
        }
    },
}

def ensure_index(index_name):
    client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)
    try:
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body=INDEX_BODY)
            print(f"Created index {index_name} with explicit mapping")
    finally:
        client.close()

def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )

    def generate_actions():
        for row in partition:
            yield {
                "_op_type": "index",
                "_index": index_name,
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

        print(f"Successfully indexed {count} documents to {index_name}")

    except Exception as e:
        log.error(f"Error indexing to {index_name}: {e}", stack_info=True, exc_info=True)
        print(f"Error indexing to {index_name}: {e}")

# COMMAND ----------

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    ensure_index(CONFIG['index_name'])

    df = (spark.table(CONFIG['table_name'])
        .select("id", F.struct(F.col("*")).alias("_source"))
    )
    df = df.repartition(1)
    print(f"Total records: {df.count()}")

    def send_partition_wrapper(partition):
        return send_partition_to_elastic(partition, CONFIG['index_name'])

    df.foreachPartition(send_partition_wrapper)

    # A list removed from the registry should disappear here too (tiny index: one
    # search covers it).
    client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)
    try:
        client.indices.refresh(index=CONFIG['index_name'])
        live_ids = {r.id for r in spark.table(CONFIG['table_name']).select("id").collect()}
        hits = client.search(index=CONFIG['index_name'], size=1000, _source=False,
                             query={"match_all": {}})["hits"]["hits"]
        stale = [h["_id"] for h in hits if h["_id"] not in live_ids]
        for doc_id in stale:
            client.delete(index=CONFIG['index_name'], id=doc_id)
        print(f"Deleted {len(stale)} stale documents from {CONFIG['index_name']}")
    finally:
        client.close()

    print(f"Completed indexing {CONFIG['table_name']} to {CONFIG['index_name']}")

except Exception as e:
    print(f"Failed to process {CONFIG['table_name']}: {e}")
    log.error(f"Failed: {CONFIG['table_name']}: {e}", stack_info=True, exc_info=True)

print("\nIndexing operation completed!")
