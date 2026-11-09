# Databricks notebook source
# MAGIC %md
# MAGIC ### Mappings for grants on author profiles (oxjob #1847). One-off, run BEFORE the first Nightly Grants night that serves authors.
# MAGIC
# MAGIC **awards-v4**: `author {id, display_name}` on `lead_investigator`, `co_lead_investigator` and `investigators[]`, and the
# MAGIC new `investigators_full` (every person on the award: lead, co-lead and investigators[]), object-mapped with keyword ids like
# MAGIC `institution_awarded_full`, so elastic-api can filter `lead_investigator.author.id` and `investigators.author.id` with plain
# MAGIC term queries. Without this PUT, dynamic mapping would make the ids `text` and a term filter on a full id would never match
# MAGIC (the oxjob #1901 trap).
# MAGIC
# MAGIC **authors-v22**: `awards_count` (integer, for `filter=awards_count:>0` and sorting) and `awards_api_url` (not indexed).
# MAGIC
# MAGIC Adding properties to existing objects is allowed on a live index; nothing is re-indexed here. Awards pick the fields up as
# MAGIC Nightly Grants re-sends each award whose content changed (every award that gains an author does); authors as their
# MAGIC `updated_date` moves (every author whose `awards_count` changes does).

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import json
from elasticsearch import Elasticsearch

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
AWARDS_INDEX, AUTHORS_INDEX = "awards-v4", "authors-v22"
client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)

author_props = {"properties": {
    "id":           {"type": "keyword"},
    "display_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
}}
person_props = {
    "given_name":  {"type": "text"},
    "family_name": {"type": "text"},
    "orcid":       {"type": "keyword"},
    "author":      author_props,
}

# investigators[] came from awards-v3's dynamic mapping, so it is an object today. A nested investigators would need
# nested queries in elastic-api: stop rather than patch around it.
current = client.indices.get_mapping(index=AWARDS_INDEX)[AWARDS_INDEX]["mappings"]["properties"]
for slot in ("lead_investigator", "co_lead_investigator", "investigators"):
    assert current.get(slot, {}).get("type", "object") == "object", f"{slot} is {current[slot].get('type')}; stop"
    print(slot, json.dumps(current.get(slot, {}).get("properties", {}).get("orcid")))
# a re-run is fine: the patch is additive and identical; an investigators_full mapped another way stops the run
full = current.get("investigators_full", {}).get("properties", {}).get("author", {}).get("properties", {}).get("id", {}).get("type")
assert full in (None, "keyword"), f"investigators_full.author.id is mapped as {full}; stop and compare"

awards_patch = {"properties": {
    "lead_investigator":    {"properties": {"author": author_props}},
    "co_lead_investigator": {"properties": {"author": author_props}},
    "investigators":        {"properties": {"author": author_props}},
    "investigators_full":   {"properties": person_props},      # NOT nested: denormalized object for term filters
}}
print(json.dumps(awards_patch, indent=2))
client.indices.put_mapping(index=AWARDS_INDEX, body=awards_patch)

authors_patch = {"properties": {
    "awards_count":   {"type": "integer"},
    "awards_api_url": {"type": "keyword", "index": False},
}}
print(json.dumps(authors_patch, indent=2))
client.indices.put_mapping(index=AUTHORS_INDEX, body=authors_patch)

# COMMAND ----------

after = client.indices.get_mapping(index=AWARDS_INDEX)[AWARDS_INDEX]["mappings"]["properties"]
assert after["investigators_full"]["properties"]["author"]["properties"]["id"]["type"] == "keyword"
assert after["lead_investigator"]["properties"]["author"]["properties"]["id"]["type"] == "keyword"
assert client.indices.get_mapping(index=AUTHORS_INDEX)[AUTHORS_INDEX]["mappings"]["properties"]["awards_count"]["type"] == "integer"
print("mappings in place")
