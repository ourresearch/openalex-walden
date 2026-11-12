# Databricks notebook source
# MAGIC %md
# MAGIC ### Keywords and SDGs on grants (oxjob #2125). One-off, run BEFORE the first Nightly Grants night that serves them.
# MAGIC
# MAGIC Copies the live works index's mapping of `keywords` and `sustainable_development_goals` onto awards-v4 unchanged (object,
# MAGIC not nested; `id` keyword), so elastic-api filters `keywords.id` and `sustainable_development_goals.id` on awards exactly as on
# MAGIC works. Without this PUT, dynamic mapping would make the ids `text` and a term filter on a full id would never match (the
# MAGIC oxjob #1901 trap); Nightly Grants' sync refuses to write until both ids are keyword.
# MAGIC
# MAGIC Adding properties to a live index is allowed; nothing is re-indexed here. Awards pick the fields up as Nightly Grants re-sends
# MAGIC each award whose content changed (every award that gains keywords or SDGs does). A re-run is a no-op.

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import json
from elasticsearch import Elasticsearch

ELASTIC_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
AWARDS_INDEX, WORKS_ALIAS, FIELDS = "awards-v4", "works", ("keywords", "sustainable_development_goals")
client = Elasticsearch(hosts=[ELASTIC_URL], max_retries=3, request_timeout=180)

works = client.indices.get_mapping(index=WORKS_ALIAS)
assert len(works) == 1, f"the works alias points at {sorted(works)}: run when it names one index"
works_props = next(iter(works.values()))["mappings"]["properties"]
patch = {"properties": {f: works_props[f] for f in FIELDS}}
for f in FIELDS:
    assert "type" not in works_props[f], f"works maps {f} as {works_props[f].get('type')}; stop and compare"
    assert works_props[f]["properties"]["id"]["type"] == "keyword", f"works {f}.id is not keyword; stop"

# the `lower` subfields need the index's `lower` normalizer (awards-v4 has it since oxjob #256)
settings = client.indices.get_settings(index=AWARDS_INDEX)[AWARDS_INDEX]["settings"]["index"]
assert "lower" in settings.get("analysis", {}).get("normalizer", {}), "awards-v4 has no `lower` normalizer; stop"
current = client.indices.get_mapping(index=AWARDS_INDEX)[AWARDS_INDEX]["mappings"]["properties"]
for f in FIELDS:   # absent, or already exactly the works mapping (a re-run)
    assert current.get(f) in (None, works_props[f]), f"awards-v4 already maps {f} differently: {json.dumps(current.get(f))}; stop"

print(json.dumps(patch, indent=2))
client.indices.put_mapping(index=AWARDS_INDEX, body=patch)

# COMMAND ----------

after = client.indices.get_mapping(index=AWARDS_INDEX)[AWARDS_INDEX]["mappings"]["properties"]
assert all(after[f] == works_props[f] for f in FIELDS), "awards-v4 does not match works after the PUT"
print("mapped on", AWARDS_INDEX, "as on", next(iter(works)))
