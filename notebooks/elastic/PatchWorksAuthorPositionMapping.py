# Databricks notebook source
# MAGIC %md
# MAGIC ### Author position fields on works (oxjob #1474). One-off; a re-run is a no-op.
# MAGIC
# MAGIC Adds six keyword fields to the live works index's mapping, each mapped exactly like `corresponding_author_ids`
# MAGIC (keyword + a `lower` subfield): `first_author_ids`, `first_author_institution_ids`, `first_author_countries`,
# MAGIC `last_author_ids`, `last_author_institution_ids`, `last_author_countries`. They pair an author position with the
# MAGIC author, institution or country on the same authorship, which no query can do on the flat `authorships` array.
# MAGIC
# MAGIC Adding properties to a live index is allowed; nothing is re-indexed here. `sync_works` fills the fields once the
# MAGIC index maps them (it leaves them out until then): the nightly for the works it re-sends, and the next works rebuild
# MAGIC for every work, because `IndexRebuild` copies the live mapping verbatim. Run it outside the End 2 End's ES sync.
# MAGIC
# MAGIC `dry_run=true` (the default) checks and prints the patch and the current fill counts; `dry_run=false` applies it.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import json
from elasticsearch import Elasticsearch

dbutils.widgets.text("dry_run", "true")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() != "false"

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
WORKS_ALIAS, MODEL_FIELD = "works", "corresponding_author_ids"
FIELDS = [f"{position}_author{suffix}" for position in ("first", "last")
          for suffix in ("_ids", "_institution_ids", "_countries")]
client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)

mappings = client.indices.get_mapping(index=WORKS_ALIAS).body
assert len(mappings) == 1, f"the works alias points at {sorted(mappings)}: run when it names one index"
INDEX = next(iter(mappings))
props = mappings[INDEX]["mappings"]["properties"]
model = props[MODEL_FIELD]
assert model.get("type") == "keyword", f"{INDEX} maps {MODEL_FIELD} as {model.get('type')}; stop and compare"
assert model.get("fields", {}).get("lower", {}).get("normalizer") == "lower", f"{MODEL_FIELD} has no `lower` subfield; stop"
settings = client.indices.get_settings(index=INDEX)[INDEX]["settings"]["index"]
assert "lower" in settings.get("analysis", {}).get("normalizer", {}), f"{INDEX} has no `lower` normalizer; stop"
for f in FIELDS:   # absent, or already exactly the model mapping (a re-run)
    assert props.get(f) in (None, model), f"{INDEX} already maps {f} differently: {json.dumps(props.get(f))}; stop"

patch = {"properties": {f: model for f in FIELDS}}
todo = [f for f in FIELDS if props.get(f) is None]
print(INDEX, "patch:", json.dumps(patch, indent=2))
print("to add:", todo or "nothing (already mapped)")
if todo and not DRY_RUN:
    client.indices.put_mapping(index=INDEX, body=patch)
    print("PUT", INDEX, "_mapping: done")

# COMMAND ----------

after = client.indices.get_mapping(index=INDEX)[INDEX]["mappings"]["properties"]
mapped = [f for f in FIELDS if after.get(f) == model]
print("mapped like", MODEL_FIELD, ":", mapped)
if not DRY_RUN:
    assert mapped == FIELDS, f"{INDEX} does not map all six after the PUT"
total = client.count(index=INDEX)["count"]
for f in (mapped or []):
    n = client.count(index=INDEX, query={"exists": {"field": f}})["count"]
    print(f"{f}: {n:,} of {total:,} works carry it")
