# Databricks notebook source
# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync `openalex.institutions.affiliation_strings_lookup_with_counts` to Elasticsearch v2
# MAGIC
# MAGIC This notebook syncs raw affiliation strings with their resolved institution IDs and works counts
# MAGIC to Elasticsearch for the affiliations dashboard. Uses the canonical MV as source of truth for
# MAGIC institution IDs and fresh counts from OpenAlex_works.
# MAGIC
# MAGIC **Source table**: `openalex.institutions.affiliation_strings_lookup_with_counts`
# MAGIC **Target index**: `raw-affiliation-strings-v2`

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.WARNING, format='[%(asctime)s]: %(message)s')
log = logging.getLogger(__name__)

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")

CONFIG = {
    "table_name": "openalex.institutions.affiliation_strings_lookup_with_counts",
    "index_name": "raw-affiliation-strings-v2"
}

def create_index_if_not_exists(client, index_name):
    """Create index with accent-folding analyzer if it doesn't exist."""
    if client.indices.exists(index=index_name):
        print(f"Index {index_name} already exists, skipping creation")
        return

    client.indices.create(
        index=index_name,
        body={
            "settings": {
                "analysis": {
                    "analyzer": {
                        "affiliation_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "raw_affiliation_string": {
                        "type": "text",
                        "analyzer": "affiliation_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "institution_ids_final": {"type": "keyword"},
                    "institution_ids_from_model": {"type": "keyword", "index": False},
                    "institution_ids_override": {"type": "keyword", "index": False},
                    "countries": {"type": "keyword"},
                    "works_count": {"type": "integer"}
                }
            }
        }
    )
    print(f"Created index {index_name} with accent-folding analyzer")


def send_partition_to_elastic(partition, index_name):
    client = Elasticsearch(
        hosts=[ELASTIC_URL],
        max_retries=3,
        request_timeout=180
    )

    def generate_actions(op_type="index"):
        for row in partition:
            yield {
                "_op_type": op_type,
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
            thread_count=2  # Reduced from 4 to limit ES load
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

# Create the index with custom mappings (if it doesn't exist)
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=3,
    request_timeout=180
)
create_index_if_not_exists(client, CONFIG['index_name'])

# COMMAND ----------

def format_institution_id(id_val):
    """Format institution ID with I prefix for consistency."""
    if id_val is None:
        return None
    return f"I{id_val}"

format_id_udf = F.udf(format_institution_id, StringType())

print(f"\n=== Processing {CONFIG['table_name']} ===")

try:
    df = (spark.table(CONFIG['table_name'])
        # Create a hashed ID from raw_affiliation_string
        .withColumn("id", F.sha2(F.col("raw_affiliation_string"), 256))
        # Format institution IDs with I prefix
        .withColumn("institution_ids_final_formatted",
            F.transform(F.col("institution_ids_final"), lambda x: F.concat(F.lit("I"), x.cast("string"))))
        .withColumn("institution_ids_from_model_formatted",
            F.transform(F.col("institution_ids_from_model"), lambda x: F.concat(F.lit("I"), x.cast("string"))))
        .withColumn("institution_ids_override_formatted",
            F.transform(F.col("institution_ids_override"), lambda x: F.concat(F.lit("I"), x.cast("string"))))
        # Select only the columns we need
        .select(
            "id",
            F.struct(
                F.col("raw_affiliation_string"),
                F.col("institution_ids_final_formatted").alias("institution_ids_final"),
                F.col("institution_ids_from_model_formatted").alias("institution_ids_from_model"),
                F.col("institution_ids_override_formatted").alias("institution_ids_override"),
                F.col("countries"),
                F.col("works_count").cast("int").alias("works_count")
            ).alias("_source")
        )
        # Filter out any rows where the affiliation string is null
        .filter(F.col("id").isNotNull())
    )

    df = df.repartition(8)  # Reduced from 32 to limit ES load
    record_count = df.count()
    print(f"Total records to process: {record_count}")

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

# Refresh the index to make documents searchable immediately
client = Elasticsearch(
    hosts=[ELASTIC_URL],
    max_retries=3,
    request_timeout=180
)

client.indices.refresh(index=CONFIG['index_name'])
print(f"Index {CONFIG['index_name']} refreshed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the index

# COMMAND ----------

# Quick verification - count docs and sample search
result = client.count(index=CONFIG['index_name'])
print(f"Total documents in index: {result['count']}")

# Verify our test RAS
test_result = client.search(
    index=CONFIG['index_name'],
    body={
        "query": {
            "term": {
                "raw_affiliation_string.keyword": "Department of Mathematics, Simon Fraser University, Burnaby, British Columbia, Canada V5A 1S6"
            }
        },
        "size": 1
    }
)
if test_result['hits']['hits']:
    doc = test_result['hits']['hits'][0]['_source']
    print(f"\nTest RAS verification:")
    print(f"  works_count: {doc.get('works_count')}")
    print(f"  institution_ids_final: {doc.get('institution_ids_final')}")
else:
    print("Test RAS not found in index")
