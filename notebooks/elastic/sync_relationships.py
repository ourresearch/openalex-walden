# Databricks notebook source
# NOTE: Like sync_study_designs, this tiny closed-vocabulary entity builds its API table and
# syncs it to Elasticsearch in one notebook.
#
# "relationships" (oxjob #2065) is the vocabulary behind works.parent_works[].relationship: the kind of
# part-of link between a work and its parent. Four values. The values themselves are written by
# notebooks/end2end/CreateWorkParents; this notebook only describes and counts them.
#
# Naming rule (Jason, 15 Nov 2026, oxjob #1490): a relationship value is a NOUN that names the link itself, not one
# end of it (citation, retraction, review), so it reads the same from either work; the description reads from the work
# that carries the link ("The work is ..."). Each value lists its standard_terms: the same link in standard
# vocabularies (Dublin Core, schema.org, DataCite, Crossref, DCAT), so RDF users get their predicate while the API
# stays in nouns. A standard term can be broader than ours: all four part-of kinds are isPartOf.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

TABLE = "openalex.common.work_relationships_api"
HASH = "openalex.common.work_relationships_api_hash"
def hash_expr(terms_sql="to_json(standard_terms)"):
    return f"""xxhash64(CONCAT_WS('|',
    COALESCE(display_name, ''),
    COALESCE(description, ''),
    COALESCE(CAST(works_count AS STRING), ''),
    COALESCE({terms_sql}, '')
))"""
HASH_EXPR = hash_expr()

# key, display_name, description. Keep in step with openalex-elastic-api (tests) and the help page.
VOCAB = [
    ("volume", "Volume",
     "The work is in a book or proceedings volume: a chapter, entry, front matter or paper of that volume."),
    ("component", "Component",
     "The work is a piece of another work that does not stand alone: a figure, table, file or supplement."),
    ("data-series", "Data series",
     "The work is one record of a data series: datasets published separately that share an instrument and method."),
    ("part", "Part",
     "The work is declared part of another work in some other way: an article in an issue, a dataset in a "
     "collection, a component of a project."),
]

# The same link in standard vocabularies (vocabulary, term, url), per value. Keep in step with the help page.
_DCTERMS = ("Dublin Core", "isPartOf", "http://purl.org/dc/terms/isPartOf")
_SCHEMA = ("schema.org", "isPartOf", "https://schema.org/isPartOf")
_DATACITE = ("DataCite", "IsPartOf",
             "https://datacite-metadata-schema.readthedocs.io/en/4.6/appendices/appendix-1/relationType/#ispartof")
_CROSSREF = ("Crossref", "isPartOf",
             "https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/")
_PART_OF = [_DCTERMS, _SCHEMA, _DATACITE, _CROSSREF]
STANDARD_TERMS = {
    "volume": _PART_OF,
    "component": _PART_OF,
    "data-series": _PART_OF + [("DCAT", "inSeries", "https://www.w3.org/ns/dcat#inSeries")],
    "part": _PART_OF,
}
assert set(STANDARD_TERMS) == {v[0] for v in VOCAB}, "every relationship lists its standard terms"

# COMMAND ----------

# Snapshot existing hashes for change detection
if spark.catalog.tableExists(TABLE):
    # Tables built before standard_terms existed hash it as empty, so adding it bumps updated_date once.
    old_terms = "to_json(standard_terms)" if "standard_terms" in spark.table(TABLE).columns else "CAST(NULL AS STRING)"
    spark.sql(f"CREATE OR REPLACE TABLE {HASH} AS SELECT id, updated_date, {hash_expr(old_terms)} AS content_hash FROM {TABLE}")
else:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HASH} (id STRING, updated_date TIMESTAMP, content_hash BIGINT)")
print("Hash snapshot complete")

# COMMAND ----------

# Rebuild work_relationships_api. works_count = works with a parent of that kind, every work counted (children are
# often outside the default view, and the filter the record links to shows them all), once per kind.
# Before the first End 2 End that carries the column, openalex_works has no parent_works: counts are 0 until then
# instead of failing the common-entities job.
spark.createDataFrame(
    [(slug, name, desc, [dict(zip(("vocabulary", "term", "url"), t)) for t in STANDARD_TERMS[slug]])
     for slug, name, desc in VOCAB],
    "slug STRING, display_name STRING, description STRING, "
    "standard_terms ARRAY<STRUCT<vocabulary: STRING, term: STRING, url: STRING>>",
).createOrReplaceTempView("work_relationship_vocab")

if "parent_works" in spark.table("openalex.works.openalex_works").columns:
    counts_sql = """
        SELECT relationship AS slug, COUNT(*) AS works_count
        FROM (SELECT DISTINCT w.id, p.relationship
              FROM openalex.works.openalex_works w
              LATERAL VIEW explode(w.parent_works) AS p)
        GROUP BY relationship"""
else:
    print("openalex_works has no parent_works column yet: counts are 0")
    counts_sql = "SELECT CAST(NULL AS STRING) AS slug, CAST(0 AS BIGINT) AS works_count WHERE FALSE"

spark.sql(f"""
CREATE OR REPLACE TABLE {TABLE} AS
WITH counts AS ({counts_sql})
SELECT
    CONCAT('https://openalex.org/relationships/', v.slug) AS id,
    v.display_name,
    v.description,
    v.standard_terms,
    COALESCE(c.works_count, 0) AS works_count,
    CONCAT('https://api.openalex.org/works?filter=parent_works.relationship:', v.slug) AS works_api_url,
    TIMESTAMP '2026-11-14 00:00:00' AS created_date,
    CAST(NULL AS TIMESTAMP) AS updated_date
FROM work_relationship_vocab v
LEFT JOIN counts c ON c.slug = v.slug
""")
display(spark.table(TABLE).select("id", "display_name", "works_count"))

# A relationship the pipeline writes but this vocabulary does not describe would be a work pointing at a 404.
unknown = spark.sql(f"""
    WITH counts AS ({counts_sql})
    SELECT slug FROM counts WHERE slug NOT IN (SELECT slug FROM work_relationship_vocab)""").collect()
assert not unknown, f"parent_works carries relationships with no vocabulary row: {[r.slug for r in unknown]}"

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
INDEX_NAME = "work-relationships-v1"

# standard_terms (oxjob #1490): displayed, not searched.
STANDARD_TERMS_MAPPING = {
    "properties": {
        "vocabulary": {"type": "keyword"},
        "term": {"type": "keyword"},
        "url": {"type": "keyword", "index": False},
    }
}

# Created here on first run; shape copied from study-designs-v1.
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
    else:
        # Fields added after the index was created (standard_terms, oxjob #1490); a no-op once present.
        client.indices.put_mapping(index=INDEX_NAME, properties={"standard_terms": STANDARD_TERMS_MAPPING})
    rows = [r.asDict(True) for r in spark.table(TABLE).collect()]   # four rows
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
