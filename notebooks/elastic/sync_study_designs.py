# Databricks notebook source
# NOTE: Like sync_source_lists, this tiny closed-vocabulary entity builds its API table and
# syncs it to Elasticsearch in one notebook.
#
# "study-designs" (oxjob #1312) is the vocabulary behind works.study_designs: the seven
# "Study Characteristics" values of PubMed's Publication Types that OpenAlex serves. The
# vocabulary lives in utils/study_design.py (VALUE_ID, SERVED_CLASSES, DISPLAY_NAME,
# DESCRIPTION, PUBMED_MAP); CreateWorksEnriched hard-codes the same slug -> display_name map.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "study_design.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "study_design.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

TABLE = "openalex.common.study_designs_api"
HASH = "openalex.common.study_designs_api_hash"
HASH_EXPR = """xxhash64(CONCAT_WS('|',
    COALESCE(display_name, ''),
    COALESCE(description, ''),
    COALESCE(TO_JSON(pubmed_publication_types), '[]'),
    COALESCE(CAST(works_count AS STRING), ''),
    COALESCE(CAST(cited_by_count AS STRING), '')
))"""

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists(TABLE):
    spark.sql(f"CREATE OR REPLACE TABLE {HASH} AS SELECT id, updated_date, {HASH_EXPR} AS content_hash FROM {TABLE}")
else:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HASH} (id STRING, updated_date TIMESTAMP, content_hash BIGINT)")
print("Hash snapshot complete")

# COMMAND ----------

# Rebuild study_designs_api. works_count follows the filter the record links to
# (study_designs.id) and is core-corpus only (is_xpac IS NOT TRUE), as in sync_source_lists.
# Before the first End 2 End that carries the column, openalex_works has no study_designs:
# counts are 0 until then instead of failing the common-entities job.
vocab = spark.createDataFrame(
    [(sd.VALUE_ID[c], sd.DISPLAY_NAME[c], sd.DESCRIPTION[c],
      sorted(t for t, v in sd.PUBMED_MAP.items() if v == c)) for c in sd.SERVED_CLASSES],
    "slug STRING, display_name STRING, description STRING, pubmed_publication_types ARRAY<STRING>",
)
vocab.createOrReplaceTempView("study_design_vocab")

if "study_designs" in spark.table("openalex.works.openalex_works").columns:
    counts_sql = """
        SELECT sd.id AS id, COUNT(*) AS works_count, SUM(w.cited_by_count) AS cited_by_count
        FROM openalex.works.openalex_works w
        LATERAL VIEW explode(w.study_designs) AS sd
        WHERE w.is_xpac IS NOT TRUE
        GROUP BY sd.id"""
else:
    print("openalex_works has no study_designs column yet: counts are 0")
    counts_sql = "SELECT CAST(NULL AS STRING) AS id, CAST(0 AS BIGINT) AS works_count, CAST(0 AS BIGINT) AS cited_by_count WHERE FALSE"

spark.sql(f"""
CREATE OR REPLACE TABLE {TABLE} AS
WITH counts AS ({counts_sql})
SELECT
    CONCAT('https://openalex.org/study-designs/', v.slug) AS id,
    v.display_name,
    v.description,
    v.pubmed_publication_types,
    COALESCE(c.works_count, 0) AS works_count,
    COALESCE(c.cited_by_count, 0) AS cited_by_count,
    CONCAT('https://api.openalex.org/works?filter=study_designs.id:', v.slug) AS works_api_url,
    TIMESTAMP '2026-09-25 00:00:00' AS created_date,
    CAST(NULL AS TIMESTAMP) AS updated_date
FROM study_design_vocab v
LEFT JOIN counts c ON c.id = CONCAT('https://openalex.org/study-designs/', v.slug)
""")
display(spark.table(TABLE).select("id", "display_name", "works_count"))

# COMMAND ----------

# Set updated_date based on content hash comparison
spark.sql(f"""
WITH new_hashes AS (SELECT id, {HASH_EXPR} AS content_hash FROM {TABLE})
MERGE INTO {TABLE} AS target
USING (
    SELECT n.id,
        CASE
            WHEN p.id IS NULL THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
            WHEN n.content_hash <> p.content_hash THEN DATE_TRUNC('SECOND', CURRENT_TIMESTAMP())
            ELSE p.updated_date
        END AS new_updated_date
    FROM new_hashes n
    LEFT JOIN {HASH} p ON n.id = p.id
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.updated_date = source.new_updated_date
""")

# COMMAND ----------

from elasticsearch import Elasticsearch, helpers  # noqa: E402
import logging  # noqa: E402

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
INDEX_NAME = "study-designs-v1"

# Created here on first run; shape copied from source-lists-v1.
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
            "description": {"type": "text", "analyzer": "search_analyzer"},
            "pubmed_publication_types": {"type": "keyword"},
            "works_count": {"type": "long"},
            "cited_by_count": {"type": "long"},
            "works_api_url": {"type": "keyword", "index": False},
            "created_date": {"type": "date"},
            "updated_date": {"type": "date"},
        }
    },
}

client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)
try:
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(index=INDEX_NAME, body=INDEX_BODY)
        print(f"Created index {INDEX_NAME} with explicit mapping")
    rows = [r.asDict(True) for r in spark.table(TABLE).collect()]   # seven rows
    actions = [{"_op_type": "index", "_index": INDEX_NAME, "_id": r["id"], "_source": r} for r in rows]
    ok, errors = helpers.bulk(client, actions, raise_on_error=False)
    if errors:
        raise RuntimeError(f"{len(errors)} documents failed to index: {errors[:3]}")
    client.indices.refresh(index=INDEX_NAME)
    live_ids = {r["id"] for r in rows}
    hits = client.search(index=INDEX_NAME, size=1000, _source=False, query={"match_all": {}})["hits"]["hits"]
    stale = [h["_id"] for h in hits if h["_id"] not in live_ids]
    for doc_id in stale:
        client.delete(index=INDEX_NAME, id=doc_id)
    print(f"Indexed {ok} documents to {INDEX_NAME}; deleted {len(stale)} stale")
finally:
    client.close()
