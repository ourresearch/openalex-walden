# Databricks notebook source
# NOTE: Like sync_relationships, this tiny closed-vocabulary entity builds its API table and syncs it to Elasticsearch
# in one notebook.
#
# "author types" (oxjob #2437) is the vocabulary of authorships[].author.type on works and of type on authors: who or
# what the byline credits. A seat is typed only where a source states it or a rule that passed its bar infers it
# (CreateWorkAuthorships, from openalex.works.work_author_types); NULL means not stated, so works_count counts only
# works with a typed author. Each value lists its standard_terms: the same class in standard vocabularies (W3C PROV-O,
# schema.org, Wikidata, ActivityStreams), so RDF users get their class while the API keeps one plain slug.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

TABLE = "openalex.common.author_types_api"
HASH = "openalex.common.author_types_api_hash"
HASH_EXPR = """xxhash64(CONCAT_WS('|',
    COALESCE(display_name, ''),
    COALESCE(description, ''),
    COALESCE(CAST(works_count AS STRING), ''),
    COALESCE(to_json(standard_terms), '')
))"""

# key, display_name, description. Keep in step with openalex-elastic-api (tests) and the help page.
VOCAB = [
    ("human", "Human", "The author is a person."),
    ("organization", "Organization",
     "The author is a group credited as one author: a consortium, committee, working group, company or institution."),
    ("software-agent", "Software agent",
     "The author is running software, such as an AI research agent or another bot."),
]

# The same class in standard vocabularies (vocabulary, term, url), per value. Keep in step with the help page.
STANDARD_TERMS = {
    "human": [
        ("W3C PROV-O", "Person", "http://www.w3.org/ns/prov#Person"),
        ("schema.org", "Person", "https://schema.org/Person"),
        ("Wikidata", "human (Q5)", "https://www.wikidata.org/wiki/Q5"),
    ],
    "organization": [
        ("W3C PROV-O", "Organization", "http://www.w3.org/ns/prov#Organization"),
        ("schema.org", "Organization", "https://schema.org/Organization"),
        ("Wikidata", "organization (Q43229)", "https://www.wikidata.org/wiki/Q43229"),
    ],
    "software-agent": [
        ("W3C PROV-O", "SoftwareAgent", "http://www.w3.org/ns/prov#SoftwareAgent"),
        ("Wikidata", "software agent (Q2297769)", "https://www.wikidata.org/wiki/Q2297769"),
        ("W3C ActivityStreams 2.0", "Application", "https://www.w3.org/ns/activitystreams#Application"),
    ],
}
assert set(STANDARD_TERMS) == {v[0] for v in VOCAB}, "every author type lists its standard terms"

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists(TABLE):
    spark.sql(f"CREATE OR REPLACE TABLE {HASH} AS SELECT id, updated_date, {HASH_EXPR} AS content_hash FROM {TABLE}")
else:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HASH} (id STRING, updated_date TIMESTAMP, content_hash BIGINT)")
print("Hash snapshot complete")

# COMMAND ----------

# Rebuild author_types_api. works_count = works with at least one author of that type (one scan of openalex_works).
# Before the first End 2 End that carries authorships[].author.type, counts are 0 instead of failing the job.
spark.createDataFrame(
    [(slug, name, desc, [dict(zip(("vocabulary", "term", "url"), t)) for t in STANDARD_TERMS[slug]])
     for slug, name, desc in VOCAB],
    "slug STRING, display_name STRING, description STRING, "
    "standard_terms ARRAY<STRUCT<vocabulary: STRING, term: STRING, url: STRING>>",
).createOrReplaceTempView("author_type_vocab")

author_fields = (spark.table("openalex.works.openalex_works").schema["authorships"].dataType.elementType["author"]
                 .dataType.fieldNames())
if "type" in author_fields:
    slugs = ", ".join(f"'{slug}'" for slug, _, _ in VOCAB)
    per_slug = ",\n               ".join(
        f"COUNT_IF(EXISTS(authorships, a -> a.author.type = '{slug}')) AS `{slug}`" for slug, _, _ in VOCAB)
    scan = spark.sql(f"""
        SELECT {per_slug},
               COUNT_IF(EXISTS(authorships, a -> a.author.type IS NOT NULL AND a.author.type NOT IN ({slugs}))) AS other
        FROM openalex.works.openalex_works""").first()
    # A type the pipeline serves but this vocabulary does not describe would be a work pointing at a 404.
    assert scan["other"] == 0, f"{scan['other']} works carry an author type with no vocabulary row"
    counts = [(slug, int(scan[slug])) for slug, _, _ in VOCAB]
else:
    print("openalex_works has no authorships[].author.type yet: counts are 0")
    counts = [(slug, 0) for slug, _, _ in VOCAB]
spark.createDataFrame(counts, "slug STRING, works_count BIGINT").createOrReplaceTempView("author_type_counts")

spark.sql(f"""
CREATE OR REPLACE TABLE {TABLE} AS
SELECT
    CONCAT('https://openalex.org/author-types/', v.slug) AS id,
    v.display_name,
    v.description,
    v.standard_terms,
    COALESCE(c.works_count, 0) AS works_count,
    CONCAT('https://api.openalex.org/works?filter=authorships.author.type:', v.slug) AS works_api_url,
    TIMESTAMP '2026-11-21 00:00:00' AS created_date,
    CAST(NULL AS TIMESTAMP) AS updated_date
FROM author_type_vocab v
LEFT JOIN author_type_counts c ON c.slug = v.slug
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
INDEX_NAME = "author-types-v1"

# standard_terms: displayed, not searched.
STANDARD_TERMS_MAPPING = {
    "properties": {
        "vocabulary": {"type": "keyword"},
        "term": {"type": "keyword"},
        "url": {"type": "keyword", "index": False},
    }
}

# Created here on first run; shape copied from work-relationships-v1.
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
            "standard_terms": STANDARD_TERMS_MAPPING,
            "works_count": {"type": "long"},
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
    rows = [r.asDict(True) for r in spark.table(TABLE).collect()]   # three rows
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
