# Databricks notebook source
# MAGIC %md
# MAGIC # Nightly grants
# MAGIC Builds the grant tables every night: stable award ids (`award_entities` / `award_keys`), `openalex_awards`, `award_id_aliases`,
# MAGIC `work_awards` (links on merged-away works move to the surviving work) and `awards_api`, then publishes changed award
# MAGIC documents, redirects and capped deletes to search. Replaces the RefreshAwards release chain (09-30).
# MAGIC Every check runs before the first registry state change or table swap; a failed night leaves yesterday's tables in place
# MAGIC and the next night simply runs again. Config: `config.prod.json` next to this notebook; the `overrides_json` parameter
# MAGIC merges on top (e.g. `{"stop_before_apply": true, "sync": {"mode": "observe"}}` for a check run that changes nothing public).

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import json, os, sys

dbutils.widgets.text("overrides_json", "")
dbutils.widgets.text("databricks_run_id", "")
PACKAGE_ROOT = os.getcwd()                            # this notebook's folder in the git checkout
config = json.load(open(os.path.join(PACKAGE_ROOT, "config.prod.json")))
overrides = json.loads(dbutils.widgets.get("overrides_json") or "{}")
for key, value in overrides.items():
    config[key] = {**config[key], **value} if isinstance(value, dict) and isinstance(config.get(key), dict) else value
config["package_root"] = PACKAGE_ROOT
sys.path.insert(0, os.path.join(PACKAGE_ROOT, "lib"))

from nightly_runtime import Nightly
import build_awards, work_awards, create_api, sync_awards

spark.conf.set("spark.sql.ansi.enabled", "false")   # the deployed chain ran with ANSI off
c = Nightly(spark, config, dbutils.widgets.get("databricks_run_id") or None)
mode = config["sync"]["mode"]
assert mode in ("none", "observe", "elasticsearch"), "SYNC_MODE"
print("run", c.run_id, "overrides", json.dumps(overrides))

# COMMAND ----------

try:
    c.start()
    c.bind_inputs()
    build_awards.run(c)
    work_awards.run(c)
    create_api.run(c)
    search = sync_awards.Search(c, dbutils) if mode != "none" else None
    if search:
        sync_awards.stage(c, search)
    if config.get("stop_before_apply"):
        c.finish("CHECKED")                           # everything built and checked; registry state and public tables untouched
    else:
        build_awards.apply(c)                         # logs each status/redirect transition, then applies it
        # carried state first: every id that becomes public below has its bindings carried
        c.artifact("bindings_final_carry", f"SELECT observation_key,stable_id,family,coalesce(payload.provenance,family) source FROM {c.r}bindings_final")
        c.swap({c.p + "award_bindings_last": c.r + "bindings_final_carry", c.p + "award_merge_doi_pairs": c.r + "merge_doi_pairs"})
        o = c.outputs
        c.swap({o["awards"]: c.r + "awards_candidate", o["aliases"]: c.r + "aliases_candidate",
                o["work_awards"]: c.r + "work_awards_candidate", o["api"]: c.r + "api_candidate"})
        if mode == "elasticsearch":
            sync_awards.publish(c, search)
        build_awards.mark_published(c)
        c.finish("SUCCEEDED" if mode == "elasticsearch" else "SUCCEEDED_NO_SEARCH")
except BaseException as exc:
    try:
        c.finish("FAILED", f"{type(exc).__name__}: {exc}"[:4000])
    finally:
        raise

# COMMAND ----------

print(json.dumps(dict(run_id=c.run_id, counts=c.counts), default=str)[:20000])
