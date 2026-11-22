# Databricks notebook source
# NOTE: Like sync_study_designs, this tiny closed-vocabulary entity builds its API table and
# syncs it to Elasticsearch in one notebook.
#
# "relationships" (link types from the /link-types rename, oxjob #2481) is the vocabulary of work-to-work links:
# citation (a work's referenced_works, oxjob #1490), the four part-of kinds of works.parent_works[].relationship
# (oxjob #2065), written by notebooks/end2end/CreateWorkParents, review (oxjob #2481, rows of work_links built from
# work_review_links), and the three pull notices retraction, withdrawal and removal (oxjob #2264, rows of work_links
# built from work_pull_notice_links), and translation (oxjob #2536, rows of work_links built from
# work_translation_links). This notebook only describes and counts them; the API's /links serves the links themselves.
# It writes the same records to two indexes: work-relationships-v1 (ids https://openalex.org/relationships/<slug>,
# served at /relationships) and link-types-v1 (ids https://openalex.org/link-types/<slug>, served at /link-types).
# /relationships redirects to /link-types after the notice period (oxjob #2481); then the first index can go.
#
# Naming rule (Jason, 15 Nov 2026, oxjob #1490): a relationship value is a NOUN that names the link itself, not one
# end of it (citation, retraction, review), so it reads the same from either work; the description reads from the work
# that carries the link ("The work is ..."). Each value lists its standard_terms: the same link in standard
# vocabularies (Dublin Core, schema.org, DataCite, Crossref, DCAT), so RDF users get their predicate while the API
# stays in nouns. A standard term can be broader than ours: all four part-of kinds are isPartOf. Since oxjob #2264
# (Jason, 22 Nov 2026) each standard term says how close it is in `match`, with SKOS's mapping levels: exact (the same
# link), close (the same idea said another way: the inverse link, or the notice's own type), broad (theirs also covers
# more), narrow (theirs covers only part of ours), related (a status or a date, not a link).

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
    ("citation", "Citation", "The work cites another work: that work is in its reference list."),
    ("volume", "Volume",
     "The work is in a book or proceedings volume: a chapter, entry, front matter or paper of that volume."),
    ("component", "Component",
     "The work is a piece of another work that does not stand alone: a figure, table, file or supplement."),
    ("data-series", "Data series",
     "The work is one record of a data series: datasets published separately that share an instrument and method."),
    ("part", "Part",
     "The work is declared part of another work in some other way: an article in an issue, a dataset in a "
     "collection, a component of a project."),
    ("review", "Review",
     "The work reviews another work: an evaluation of that specific work by someone other than its authors, such as "
     "a peer review, preprint review or book review (a literature review article is the work type review, not this "
     "link)."),
    # oxjob #2264 (Jason approved 22 Nov 2026): the pull notices. Crossref's update types name them but define none; these
    # definitions are ours (crosswalk of 15 vocabularies: oxjobs #2264 work/pull_notice_crosswalk.md). When a source does
    # not say which of the three it is (Retraction Watch, PubMed), the link is a retraction, as those sources call it.
    ("retraction", "Retraction",
     "The work is a notice that another work should no longer be relied on. The work it retracts usually stays online, "
     "marked as retracted."),
    ("withdrawal", "Withdrawal",
     "The work is a notice that another work was withdrawn, usually soon after it appeared and for a publishing reason "
     "such as a duplicate or an editorial error. Its content may be gone."),
    ("removal", "Removal",
     "The work is a notice that another work's content was taken down, usually for legal or safety reasons, while its "
     "record stays."),
    # oxjob #2536 (crosswalk of 26 vocabularies: that job's work/translation_crosswalk.md). Same shape as summary
    # (oxjob #2535): a work made from another work, linked from it. The line between them (Jason, 22 Nov 2026: a
    # translation is "a summary at compression equals 0 and language equals some other language"; FRBR, Z39.14): full
    # length in another language is a translation, anything shorter a summary, in any language.
    ("translation", "Translation",
     "The work is a translation of another work: that work's full text in another language, published as its own work "
     "(such as an article in an English edition of a Russian journal). A shorter version in another language is a "
     "summary; a translated title or abstract inside one record, or a free adaptation, is neither."),
]

# The same link in standard vocabularies (vocabulary, term, url, match), per value. Keep in step with the help page.
# match is the SKOS mapping level of their term against ours (see the header): exact, close, broad, narrow or related.
_DCTERMS = ("Dublin Core", "isPartOf", "http://purl.org/dc/terms/isPartOf", "broad")
_SCHEMA = ("schema.org", "isPartOf", "https://schema.org/isPartOf", "broad")
_DATACITE_DOCS = "https://datacite-metadata-schema.readthedocs.io/en/4.7/appendices/appendix-1/relationType/"
_DATACITE = ("DataCite", "IsPartOf", _DATACITE_DOCS + "#ispartof", "broad")
_CROSSREF = ("Crossref", "isPartOf",
             "https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/", "broad")
_PART_OF = [_DCTERMS, _SCHEMA, _DATACITE, _CROSSREF]
# oxjob #2264: sources for the pull-notice crosswalk.
_CROSSMARK = "https://www.crossref.org/documentation/crossmark/participating-in-crossmark/"
_CITO_RETRACTS = ("CiTO", "retracts", "http://purl.org/spar/cito/retracts")
_RW_GUIDE = "https://retractionwatch.com/retraction-watch-database-user-guide/"
_RW_REASONS = ("https://retractionwatch.com/retraction-watch-database-user-guide/"
               "retraction-watch-database-user-guide-appendix-b-reasons/")
_NLM = "https://www.nlm.nih.gov/bsd/policy/errata.html"
_WOS = "https://webofscience.help.clarivate.com/en-us/Content/document-types.html"
_EPMC = "https://europepmc.org/AboutArticleStatusMonitor"
_ELSEVIER = "https://www.elsevier.com/about/policies-and-standards/article-withdrawal"
_NISO_CREC = "https://www.niso.org/publications/rp-45-2024-crec"
_COPE = "https://publicationethics.org/guidance/guideline/retraction-guidelines"
_WD_P5824 = ("Wikidata", "P5824", "https://www.wikidata.org/wiki/Property:P5824")
_FABIO = ("FaBiO", "RetractionNotice", "http://purl.org/spar/fabio/RetractionNotice")
_MESH = ("MeSH", "Retraction Notice", "http://id.nlm.nih.gov/mesh/D016440")
STANDARD_TERMS = {
    "citation": [
        ("Dublin Core", "references", "http://purl.org/dc/terms/references", "broad"),
        ("schema.org", "citation", "https://schema.org/citation", "exact"),
        ("DataCite", "References", _DATACITE_DOCS + "#references", "exact"),
        ("DataCite", "Cites", _DATACITE_DOCS + "#cites", "exact"),
        ("Crossref", "references", _CROSSREF[2], "exact"),
        ("CiTO", "cites", "http://purl.org/spar/cito/cites", "exact"),
    ],
    # DataCite's IsPublishedIn ("A is published inside B, but is independent of other things published inside B")
    # fits a chapter in its book better than IsPartOf.
    "volume": [_DCTERMS, _SCHEMA, ("DataCite", "IsPublishedIn", _DATACITE_DOCS + "#ispublishedin", "close"), _CROSSREF],
    "component": _PART_OF,
    "data-series": _PART_OF + [("DCAT", "inSeries", "https://www.w3.org/ns/dcat#inSeries", "exact")],
    "part": _PART_OF,
    # oxjob #2481 (crosswalk of 24 vocabularies: that job's work/review_crosswalk.md): the four exact equivalents first,
    # then two broader terms. CiTO's reviews types a citation, so a literature review may use it for every paper it
    # surveys; schema.org's itemReviewed also covers product and service reviews.
    "review": [
        ("Crossref", "isReviewOf", _CROSSREF[2], "exact"),
        ("DataCite", "Reviews", _DATACITE_DOCS + "#reviews", "exact"),
        ("Wikidata", "P6977", "https://www.wikidata.org/wiki/Property:P6977", "exact"),
        ("LC Relationship", "reviewof", "http://id.loc.gov/vocabulary/relationship/reviewof", "exact"),
        ("schema.org", "itemReviewed", "https://schema.org/itemReviewed", "broad"),
        ("CiTO", "reviews", "http://purl.org/spar/cito/reviews", "broad"),
    ],
    # oxjob #2264: exact and close terms first, then broader, narrower and related ones. Most indexes and ontologies
    # fold withdrawals and removals into retraction (broad); Crossref alone has all three. Wikidata's P5824 "retracted
    # by" is the same link read from the retracted work (close). Notice types (MeSH, FaBiO, Web of Science) name the
    # notice, not the link.
    "retraction": [
        ("Crossref", "retraction", _CROSSMARK, "exact"),
        _CITO_RETRACTS + ("exact",),
        ("JATS", "retracted-article",
         "https://jats.nlm.nih.gov/archiving/tag-library/1.4/attribute/related-article-type.html", "exact"),
        ("NISO CREC", "retraction", _NISO_CREC, "close"),
        ("COPE", "retraction", _COPE, "close"),
        ("ICMJE", "retraction",
         "https://www.icmje.org/recommendations/browse/publishing-and-editorial-issues/corrections-and-version-control.html",
         "close"),
        ("Elsevier", "article retraction", _ELSEVIER, "close"),
        ("Web of Science", "Retraction", _WOS, "close"),
        _WD_P5824 + ("close",),
        ("Retraction Watch", "Retraction", _RW_GUIDE, "broad"),
        ("PubMed", "RetractionOf", _NLM, "broad"),
        ("Scopus", "Erratum", "https://supportcontent.elsevier.com/Support%20Hub/Scopus/Scopus_ContentCoverage_Guide_WEB.pdf",
         "broad"),
        ("Europe PMC", "Retraction of", _EPMC, "broad"),
        _FABIO + ("broad",),
        _MESH + ("broad",),
    ],
    "withdrawal": [
        ("Crossref", "withdrawal", _CROSSMARK, "exact"),
        ("Web of Science", "Item Withdrawal", _WOS, "close"),
        _CITO_RETRACTS + ("broad",),
        ("Retraction Watch", "Retraction", _RW_GUIDE, "broad"),
        ("PubMed", "RetractionOf", _NLM, "broad"),
        _WD_P5824 + ("broad",),
        _FABIO + ("broad",),
        _MESH + ("broad",),
        ("Elsevier", "article withdrawal", _ELSEVIER, "narrow"),
        ("Europe PMC", "preprint-withdrawal", _EPMC, "narrow"),
        ("DataCite", "Withdrawn",
         "https://datacite-metadata-schema.readthedocs.io/en/4.7/appendices/appendix-1/dateType/#withdrawn", "related"),
    ],
    "removal": [
        ("Crossref", "removal", _CROSSMARK, "exact"),
        ("NISO CREC", "removal", _NISO_CREC, "close"),
        ("COPE", "retraction with removal", _COPE, "close"),
        ("Elsevier", "article removal", _ELSEVIER, "close"),
        ("Retraction Watch", "Removed", _RW_REASONS, "close"),
        _CITO_RETRACTS + ("broad",),
        ("PubMed", "RetractionOf", _NLM, "broad"),
        _FABIO + ("broad",),
        _MESH + ("broad",),
        ("Europe PMC", "preprint-removal", _EPMC, "narrow"),
        ("DataCite", "tombstone", "https://support.datacite.org/docs/tombstone-pages", "related"),
    ],
    # oxjob #2536 (crosswalk of 26 vocabularies, work/translation_crosswalk.md): every standard with this link names it
    # "translation (of)" and points it from the translation to the original. Five exact terms, then two broader ones:
    # Wikidata's P629 (what its scholarly items mostly use) also covers editions; Dublin Core isVersionOf covers
    # versions and adaptations.
    "translation": [
        ("DataCite", "IsTranslationOf", _DATACITE_DOCS + "#istranslationof", "exact"),
        ("Crossref", "isTranslationOf", _CROSSREF[2], "exact"),
        ("Wikidata", "P9745", "https://www.wikidata.org/wiki/Property:P9745", "exact"),
        ("schema.org", "translationOfWork", "https://schema.org/translationOfWork", "exact"),
        ("LC Relationship", "translationof", "http://id.loc.gov/vocabulary/relationship/translationof", "exact"),
        ("Wikidata", "P629", "https://www.wikidata.org/wiki/Property:P629", "broad"),
        ("Dublin Core", "isVersionOf", "http://purl.org/dc/terms/isVersionOf", "broad"),
    ],
}
assert set(STANDARD_TERMS) == {v[0] for v in VOCAB}, "every relationship lists its standard terms"
MATCH_LEVELS = ("exact", "close", "broad", "narrow", "related")
assert all(len(t) == 4 and t[3] in MATCH_LEVELS for ts in STANDARD_TERMS.values() for t in ts), \
    "every standard term is (vocabulary, term, url, match) with a SKOS match level"

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

# Rebuild work_relationships_api. works_count = works that carry a link of that kind (citation: works with a reference
# list match; part-of: works with a parent of that kind), every work counted (children are often outside the default
# view, and the filter the record links to shows them all), once per kind.
# Before the first End 2 End that carries the column, openalex_works has no parent_works: counts are 0 until then
# instead of failing the common-entities job.
spark.createDataFrame(
    [(slug, name, desc, [dict(zip(("vocabulary", "term", "url", "match"), t)) for t in STANDARD_TERMS[slug]])
     for slug, name, desc in VOCAB],
    "slug STRING, display_name STRING, description STRING, "
    "standard_terms ARRAY<STRUCT<vocabulary: STRING, term: STRING, url: STRING, match: STRING>>",
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

citation_sql = """
    SELECT 'citation' AS slug, COUNT(*) AS works_count
    FROM openalex.works.openalex_works WHERE referenced_works_count > 0"""

# review (oxjob #2481): the works that review another work, from the links table the Work Links job builds.
review_sql = """
    SELECT 'review' AS slug, COUNT(DISTINCT origin_work_id) AS works_count
    FROM openalex.works.work_links WHERE relationship = 'review'"""

# pull notices (oxjob #2264): the notices of each kind, from the same links table (the works they pull carry the list
# pull_notice_works).
pull_notice_sql = """
    SELECT relationship AS slug, COUNT(DISTINCT origin_work_id) AS works_count
    FROM openalex.works.work_links WHERE relationship IN ('retraction', 'withdrawal', 'removal')
    GROUP BY relationship"""

# translation (oxjob #2536): the works that are a translation of another work, from the same links table.
translation_sql = """
    SELECT 'translation' AS slug, COUNT(DISTINCT origin_work_id) AS works_count
    FROM openalex.works.work_links WHERE relationship = 'translation'"""

spark.sql(f"""
CREATE OR REPLACE TABLE {TABLE} AS
WITH counts AS ({counts_sql} UNION ALL {citation_sql} UNION ALL {review_sql} UNION ALL {pull_notice_sql}
                UNION ALL {translation_sql})
SELECT
    CONCAT('https://openalex.org/relationships/', v.slug) AS id,
    v.display_name,
    v.description,
    v.standard_terms,
    COALESCE(c.works_count, 0) AS works_count,
    -- review: no works filter selects the reviews themselves; works reviewed are reviewed_by_count:>0. The pull
    -- notices likewise: the works they pull are pull_notice_works.link_type:<slug> (oxjob #2264).
    CASE v.slug
      WHEN 'citation' THEN 'https://api.openalex.org/works?filter=has_references:true'
      WHEN 'review' THEN CAST(NULL AS STRING)
      WHEN 'retraction' THEN CAST(NULL AS STRING)
      WHEN 'withdrawal' THEN CAST(NULL AS STRING)
      WHEN 'removal' THEN CAST(NULL AS STRING)
      WHEN 'translation' THEN CAST(NULL AS STRING)
      ELSE CONCAT('https://api.openalex.org/works?filter=parent_works.relationship:', v.slug)
    END AS works_api_url,
    -- review joined the vocabulary on 21 Nov 2026 (oxjob #2481); the pull notices on 23 Nov (oxjob #2264); the rest on 14 Nov.
    CASE WHEN v.slug = 'review' THEN TIMESTAMP '2026-11-21 00:00:00'
         WHEN v.slug IN ('retraction', 'withdrawal', 'removal') THEN TIMESTAMP '2026-11-23 00:00:00'
         -- translation (oxjob #2536): set to the day it ships.
         WHEN v.slug = 'translation' THEN TIMESTAMP '2026-11-23 00:00:00'
         ELSE TIMESTAMP '2026-11-14 00:00:00' END AS created_date,
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
LINK_TYPES_INDEX = "link-types-v1"   # oxjob #2481: /link-types

# standard_terms (oxjob #1490): displayed, not searched. match (oxjob #2264): the SKOS mapping level.
STANDARD_TERMS_MAPPING = {
    "properties": {
        "vocabulary": {"type": "keyword"},
        "term": {"type": "keyword"},
        "url": {"type": "keyword", "index": False},
        "match": {"type": "keyword"},
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

def as_link_type(row):
    """The same record under its /link-types id; part-of works_api_urls use the parent_works.link_type filter."""
    row = dict(row)
    row["id"] = row["id"].replace("https://openalex.org/relationships/", "https://openalex.org/link-types/")
    if row.get("works_api_url"):
        row["works_api_url"] = row["works_api_url"].replace("parent_works.relationship:", "parent_works.link_type:")
    return row


def write_index(client, index_name, rows):
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=INDEX_BODY)
        print(f"Created index {index_name} with explicit mapping")
    else:
        # Fields added after the index was created (standard_terms, oxjob #1490; standard_terms.match, oxjob #2264);
        # a no-op once present.
        client.indices.put_mapping(index=index_name, properties={"standard_terms": STANDARD_TERMS_MAPPING})
    actions = [{"_op_type": "index", "_index": index_name, "_id": r["id"], "_source": r} for r in rows]
    ok, errors = helpers.bulk(client, actions, raise_on_error=False)
    if errors:
        raise RuntimeError(f"{len(errors)} documents failed to index into {index_name}: {errors[:3]}")
    client.indices.refresh(index=index_name)
    live_ids = {r["id"] for r in rows}
    hits = client.search(index=index_name, size=1000, _source=False, query={"match_all": {}})["hits"]["hits"]
    stale = [h["_id"] for h in hits if h["_id"] not in live_ids]
    for doc_id in stale:
        client.delete(index=index_name, id=doc_id)
    print(f"Indexed {ok} documents to {index_name}; deleted {len(stale)} stale")


client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)
try:
    rows = [r.asDict(True) for r in spark.table(TABLE).collect()]   # nine rows
    write_index(client, INDEX_NAME, rows)
    write_index(client, LINK_TYPES_INDEX, [as_link_type(r) for r in rows])
finally:
    client.close()
