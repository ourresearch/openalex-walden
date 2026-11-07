# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords nightly 1/4: build the tagger queue (oxjob #1322)
# MAGIC
# MAGIC Every work in `openalex_works` that has never been tagged: no row in the tagged-ids record (`<state_prefix>tagged`,
# MAGIC seeded from the full-corpus retag + the 2026-09-30 catch-up) and no row in the served table (`target`). Then (oxjob #2085)
# MAGIC works whose text changed since they were tagged: the tagged record's `text_key` (hash of the title + abstract[:6000] it was
# MAGIC tagged on) differs from today's, newest first, at most `max_retag` and within `max_works` (`retag = true`; AppendKeywords
# MAGIC updates, inserts or deletes their served row). Their new keywords re-stamp the work at the next End 2 End. A NULL
# MAGIC `text_key` is never re-queued; venue is not in the key. A work tagged with no keywords is re-tagged only when its text
# MAGIC changes. xpac included, as in the corpus run. Fields are the corpus
# MAGIC run's: title, abstract cut to 6,000 characters, venue = primary location's source name. Shards `pmod(hash(id), n)`
# MAGIC with n = ceil(works / shard_works); over `max_works` the newest works go first and the rest wait for the next run.
# MAGIC
# MAGIC Stops before any GPU spend if the tagged record is missing or not seeded (`min_tagged_rows`: an empty record would
# MAGIC re-queue the ~53M works that were tagged and came out empty), or if a rule table / the kid0 UDF is missing.
# MAGIC If today's End 2 End is still before Guardrails, waits for it (up to `wait_e2e_hours`), so the queue and the
# MAGIC normalisation read a finished `openalex_works`.

# COMMAND ----------

import datetime as dt
import math
import time

for name, default in [("state_prefix", "openalex.works.work_keywords_v2_"), ("target", "openalex.works.work_keywords_v2"),
                      ("rules_prefix", "openalex.common.keywords_v2_"), ("synmap", "openalex.common.keywords_v2_synmap"),
                      ("kid0_udf", "openalex.common.keywords_v2_kid0"), ("max_works", "2000000"), ("shard_works", "25000"),
                      ("min_tagged_rows", "400000000"), ("force_ids_table", ""), ("force_retag", "false"), ("max_retag", "300000"),
                      ("wait_e2e_hours", "6"), ("e2e_job_id", "616701029470182"),
                      ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
RULES = dbutils.widgets.get("rules_prefix").strip()
SYNMAP = dbutils.widgets.get("synmap").strip()
KID0 = dbutils.widgets.get("kid0_udf").strip()
MAX_WORKS = int(dbutils.widgets.get("max_works"))
SHARD_WORKS = int(dbutils.widgets.get("shard_works"))
MIN_TAGGED = int(dbutils.widgets.get("min_tagged_rows"))
FORCE = dbutils.widgets.get("force_ids_table").strip()
FORCE_RETAG = dbutils.widgets.get("force_retag").strip().lower() == "true"
MAX_RETAG = int(dbutils.widgets.get("max_retag"))
WAIT_H = float(dbutils.widgets.get("wait_e2e_hours"))
E2E_JOB = int(dbutils.widgets.get("e2e_job_id"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, TAGGED = f"{P}queue", f"{P}tagged"
# The tagger's input text, as the key a work is tagged on (oxjob #2085). Keep in step with the queue's title / abstract below.
TEXT_KEY = "xxhash64(coalesce({w}.title, ''), coalesce(substr({w}.abstract, 1, 6000), ''))"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

# COMMAND ----------

# Wait while today's End 2 End has not yet passed Guardrails (CreateWorksEnriched is still rewriting openalex_works).
from databricks.sdk import WorkspaceClient  # noqa: E402

w = WorkspaceClient()
deadline = time.time() + WAIT_H * 3600
while True:
    active = [r for r in w.jobs.list_runs(job_id=E2E_JOB, active_only=True)]
    pending = []
    for r in active:
        run = w.jobs.get_run(r.run_id)
        g = [t for t in (run.tasks or []) if t.task_key == "Guardrails"]
        if not g or g[0].end_time in (None, 0):
            pending.append(r.run_id)
    if not pending:
        log("no End 2 End run before Guardrails; go")
        break
    if time.time() > deadline:
        log(f"End 2 End run(s) {pending} still before Guardrails after {WAIT_H} h; going anyway (inputs are a consistent Delta snapshot)")
        break
    log(f"End 2 End run(s) {pending} still before Guardrails; waiting 5 min")
    time.sleep(300)

# COMMAND ----------

# Guards before any spend: the tagged record exists and is seeded; every normalisation input exists.
if not spark.catalog.tableExists(TAGGED):
    raise RuntimeError(f"{TAGGED} does not exist: seed it first (every work the corpus retag / catch-up tagged), or the whole corpus re-queues")
n_tagged = spark.table(TAGGED).count()
if n_tagged < MIN_TAGGED:
    raise RuntimeError(f"{TAGGED} holds {n_tagged:,} rows < min_tagged_rows {MIN_TAGGED:,}: not seeded? Refusing to queue.")
for t in [f"{RULES}{x}" for x in ("fold", "purge", "country_kids", "boiler", "templated")] + [SYNMAP, TARGET, "openalex.common.keywords_v2",
          "openalex.works.works_study_design", "openalex.sources.sources_api"]:
    spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
assert spark.sql(f"SELECT {KID0}('Machine Learning') AS k").collect()[0].k, f"{KID0} returned nothing"
log(f"tagged record {TAGGED}: {n_tagged:,} works; rule tables and {KID0} present")
if "text_key" not in {c.name for c in spark.table(TAGGED).schema}:   # oxjob #2085; NULL keys are never re-queued
    spark.sql(f"ALTER TABLE {TAGGED} ADD COLUMNS (text_key BIGINT COMMENT 'xxhash64(title, abstract[:6000]) the work was tagged on (oxjob #2085)')")

# COMMAND ----------

# new = never tagged; retag = tagged before (oxjob #2085) and its text changed since (the tagged record's key).
# FORCE (test / backfill): only these works (column id): those not served (as before #2085) plus served ones whose text changed;
# with force_retag, every one of them.
changed = "tagged_key IS NOT NULL AND tagged_key <> text_key"
pick = ("true" if FORCE_RETAG else f"NOT served OR ({changed})") if FORCE else f"NOT retag OR ({changed})"
cand = f"""SELECT * FROM (
  SELECT w.*, {TEXT_KEY.format(w='w')} AS text_key, (t.work_id IS NOT NULL OR k.work_id IS NOT NULL) AS retag, t.text_key AS tagged_key,
         k.work_id IS NOT NULL AS served
  FROM openalex.works.openalex_works w
  LEFT JOIN (SELECT work_id, text_key FROM {TAGGED}) t ON t.work_id = w.id
  LEFT JOIN (SELECT work_id FROM {TARGET}) k ON k.work_id = w.id""" + (f"""
  WHERE w.id IN (SELECT id FROM {FORCE})""" if FORCE else "") + f"""
) WHERE {pick}"""
cnt = spark.sql(f"SELECT count_if(NOT retag) AS new, count_if(retag) AS changed FROM ({cand})").collect()[0]
take_new = min(cnt.new, MAX_WORKS)
take_re = min(cnt.changed, MAX_RETAG, MAX_WORKS - take_new)
take = take_new + take_re
n_shards = max(1, math.ceil(take / SHARD_WORKS))
build_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
log(f"untagged works: {cnt.new:,}; tagged works to tag again (text changed{', forced' if FORCE else ''}): {cnt.changed:,}"
    + (f" (from {FORCE})" if FORCE else "") + f"; queueing {take_new:,} new + {take_re:,} re-tags in {n_shards} shards; build {build_id}")
if cnt.new > MAX_WORKS:
    log(f"over max_works: the newest {MAX_WORKS:,} go now, {cnt.new - MAX_WORKS:,} wait for the next run")
if cnt.changed > take_re:
    log(f"re-tags capped (max_retag {MAX_RETAG:,}, max_works {MAX_WORKS:,}): {cnt.changed - take_re:,} wait for the next run")

spark.sql(f"""
CREATE OR REPLACE TABLE {QUEUE} CLUSTER BY (shard)
COMMENT 'Keywords nightly queue (oxjob #1322): works never tagged, then (#2085) tagged works whose text changed (retag); rebuilt every run by BuildKeywordQueue; read by the Modal workers'
AS SELECT pmod(hash(c.id), {n_shards}) AS shard, c.id, c.title, substr(c.abstract, 1, 6000) AS abstract,
          c.primary_location.source.display_name AS journal, COALESCE(c.is_xpac, false) AS is_xpac, c.created_date, '{build_id}' AS build_id,
          c.text_key, c.retag
FROM ({cand}) c
QUALIFY row_number() OVER (PARTITION BY c.retag ORDER BY c.created_date DESC, c.id DESC) <= CASE WHEN c.retag THEN {take_re} ELSE {take_new} END
""")
q = spark.sql(f"""SELECT count(*) AS n, count(DISTINCT id) AS ids, count_if(retag) AS re, count(DISTINCT shard) AS shards, count_if(is_xpac) AS xpac,
  min(created_date) AS oldest FROM {QUEUE}""").collect()[0]
if q.n != take or q.ids != take or q.re != take_re:
    raise RuntimeError(f"queue holds {q.n:,} rows / {q.ids:,} ids / {q.re:,} re-tags, expected {take:,} / {take_re:,}")
log(f"queue {QUEUE}: {q.n:,} works ({q.re:,} re-tags), {q.shards} shards, {q.xpac:,} xpac, oldest created {q.oldest}")
dbutils.jobs.taskValues.set(key="build_id", value=build_id)
dbutils.jobs.taskValues.set(key="queued", value=int(q.n))
