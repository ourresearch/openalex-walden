# Databricks notebook source
# MAGIC %md
# MAGIC # Affiliation matcher: new-card sweep (oxjob #1393)
# MAGIC
# MAGIC When OpenAlex mints institutions from a new ROR dump (job Institutions, 22:30 CT; `institutions_api` at 00:56 CT),
# MAGIC the nightly matcher with live cards gives them to new strings. This sweep gives them to the strings that already
# MAGIC exist: old works, and new works that reuse an old string.
# MAGIC
# MAGIC Job `Affiliation Matcher New Institutions Sweep` (jobs/affiliation_matcher_sweep.yaml), four tasks:
# MAGIC `step=check` (serverless, seconds): records institutions never seen before in `affiliation_matcher_cards_seen`
# MAGIC and sets task value `pending`; the rest runs only if it is > 0. `step=search`: candidate strings for the pending
# MAGIC institutions by ES (`utils/affiliation_matcher/sweep.py`) into `_sweep_queue`. Then `AffiliationMatcherNightly`
# MAGIC with `queue_table`, `shard` k/n, `target_table`, `sweep_ids_table` and live cards answers them into
# MAGIC `_sweep_answers_<k>`, n tasks on n GPU clusters (Jev is the limit: ≈ 6.5M calls for a September-sized dump).
# MAGIC `step=apply`: a new answer is written only if it names a pending institution and differs from the current one. Every write is logged in `affiliation_matcher_sweep_log` (old and new ids: the
# MAGIC revert). A run that would change more than `max_works` works writes nothing and fails (Guardrails trip at 7.5M).
# MAGIC Restart-safe: institutions are marked swept only after their answers are written; a rerun resumes the staging table.

# COMMAND ----------

import json
import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, REPO_ROOT)

dbutils.widgets.text("step", "check", "check | search | apply")
dbutils.widgets.text("answers_table", "openalex.institutions.affiliation_matcher_answers", "answers table")
dbutils.widgets.text("prefix", "openalex.institutions.affiliation_matcher", "state tables: <prefix>_cards_seen, _sweep_queue, _sweep_answers, _sweep_log")
dbutils.widgets.text("max_institutions", "5000", "institutions per run (a big ROR dump drains over days)")
dbutils.widgets.text("max_works", "2000000", "write nothing if applying would change more works than this")
dbutils.widgets.text("shards", "6", "the match step runs as this many tasks on separate GPU clusters, shard k writing <prefix>_sweep_answers_<k>")
dbutils.widgets.text("apply", "true", "false = count what would change, write nothing")

STEP = dbutils.widgets.get("step").strip()
ANSWERS = dbutils.widgets.get("answers_table").strip()
P = dbutils.widgets.get("prefix").strip()
SEEN, QUEUE, STAGING, LOG = f"{P}_cards_seen", f"{P}_sweep_queue", f"{P}_sweep_answers", f"{P}_sweep_log"
SWEEP_IDS, CHANGES = f"{P}_sweep_ids", f"{P}_sweep_changes"
MAX_INST = int(dbutils.widgets.get("max_institutions"))
MAX_WORKS = int(dbutils.widgets.get("max_works"))
APPLY = dbutils.widgets.get("apply").strip().lower() == "true"
SHARDS = int(dbutils.widgets.get("shards"))
SHARD_TABLES = [f"{STAGING}_{k}" for k in range(SHARDS)]
T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.0f}s] {msg}", flush=True)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SEEN} (
  institution_id BIGINT NOT NULL, ror STRING, first_seen_at TIMESTAMP, sweep_started_at TIMESTAMP, swept_at TIMESTAMP,
  sweep_run STRING, n_candidate_strings BIGINT, n_applied_strings BIGINT
) COMMENT 'oxjob #1393: every institution the matcher has cards for, and when its strings were swept'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG} (
  sweep_run STRING, raw_affiliation_string STRING, old_ids ARRAY<BIGINT>, new_ids ARRAY<BIGINT>, swept_ids ARRAY<BIGINT>,
  old_tier STRING, works_count BIGINT, applied_at TIMESTAMP, reverted_at TIMESTAMP
) COMMENT 'oxjob #1393: every answer the new-card sweep wrote; revert = write old_ids back'
""")

if spark.table(SEEN).limit(1).count() == 0:
    # Seed: the corpus run (#1385) used the frozen cards, which equal institutions_api on 2026-09-27 (140,266 ids),
    # so every current institution counts as swept.
    spark.sql(f"""
INSERT INTO {SEEN}
SELECT id, ror, current_timestamp(), NULL, current_timestamp(), 'seed: corpus run cards (#1385)', NULL, NULL
FROM openalex.institutions.institutions_api
""")
    log(f"seeded {SEEN}")

if STEP == "check":
    spark.sql(f"""
INSERT INTO {SEEN}
SELECT i.id, i.ror, current_timestamp(), NULL, NULL, NULL, NULL, NULL
FROM openalex.institutions.institutions_api i
LEFT ANTI JOIN {SEEN} s ON s.institution_id = i.id
WHERE coalesce(i.status, 'active') <> 'withdrawn'
""")
    pending = spark.sql(f"SELECT count(*) AS n FROM {SEEN} WHERE swept_at IS NULL").collect()[0].n
    dbutils.jobs.taskValues.set("pending", int(pending))
    dbutils.notebook.exit(json.dumps({"pending": int(pending)}))

assert STEP in ("search", "apply"), STEP

# COMMAND ----------

from utils.affiliation_matcher import nightly as nm  # noqa: E402
from utils.affiliation_matcher import sweep  # noqa: E402
from utils.affiliation_matcher.retrieve import Index  # noqa: E402

if STEP == "search":
    pend = spark.sql(f"""
SELECT institution_id, sweep_started_at FROM {SEEN} WHERE swept_at IS NULL
ORDER BY first_seen_at, institution_id LIMIT {MAX_INST}""").collect()
    ids = {int(r.institution_id) for r in pend}
    resume = (bool(pend) and all(r.sweep_started_at is not None for r in pend) and spark.catalog.tableExists(QUEUE)
              and spark.catalog.tableExists(SWEEP_IDS)
              and {int(r.institution_id) for r in spark.table(SWEEP_IDS).collect()} == ids)
    if resume:
        dbutils.notebook.exit(f"resuming: {len(ids):,} institutions, queue and staging kept")
    for t in SHARD_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.createDataFrame([(i,) for i in sorted(ids)], "institution_id BIGINT").write.mode("overwrite").saveAsTable(SWEEP_IDS)
    WORK = "/local_disk0/tmp/affiliation_matcher"
    os.makedirs(WORK, exist_ok=True)
    CARDS = f"{WORK}/institutions.jsonl.gz"
    log(f"live cards: {nm.write_cards(spark, CARDS):,}")
    ix = Index(CARDS)
    with_card = {i for i in ids if i in ix.inst}
    log(f"{len(ids):,} institutions to sweep; {len(with_card):,} have a card (the rest are withdrawn)")
    t = time.time()
    found, stats = sweep.search(ix, with_card, dbutils.secrets.get(scope="elastic", key="elastic_url"), log=log)
    log(f"search {time.time() - t:.0f}s: {stats}")
    rows = sorted({x for v in found.values() for x in v})
    spark.createDataFrame([(x,) for x in rows], "raw_affiliation_string STRING").write.mode("overwrite").saveAsTable(QUEUE)
    spark.createDataFrame([(i, len(v)) for i, v in found.items()] or [(0, 0)], "institution_id BIGINT, n BIGINT").createOrReplaceTempView("cand_counts")
    spark.sql(f"""MERGE INTO {SEEN} t USING (SELECT i.institution_id, coalesce(c.n, 0) AS n FROM {SWEEP_IDS} i
                  LEFT JOIN cand_counts c ON c.institution_id = i.institution_id) c
                  ON t.institution_id = c.institution_id
                  WHEN MATCHED THEN UPDATE SET sweep_started_at = current_timestamp(), n_candidate_strings = c.n""")
    dbutils.notebook.exit(json.dumps({"institutions": len(ids), "queue": len(rows), **stats}))

# COMMAND ----------

# step apply
ids = {int(r.institution_id) for r in spark.table(SWEEP_IDS).collect()}
# the shards' answers as one view (a shard whose queue was empty never created its table)
have = [t for t in SHARD_TABLES if spark.catalog.tableExists(t)]
spark.sql(f"""CREATE OR REPLACE TEMP VIEW sweep_staging AS
{" UNION ALL ".join(f"SELECT * FROM {t}" for t in have) if have else
 "SELECT CAST(NULL AS STRING) AS raw_affiliation_string, CAST(NULL AS ARRAY<BIGINT>) AS institution_ids, CAST(NULL AS MAP<BIGINT, DOUBLE>) AS scores, CAST(NULL AS STRING) AS decider, CAST(NULL AS STRING) AS matcher_version WHERE FALSE"}""")
STAGING = "sweep_staging"
left = spark.sql(f"SELECT count(*) AS n FROM {QUEUE} q LEFT ANTI JOIN {STAGING} s USING (raw_affiliation_string)").collect()[0].n
if left:
    raise RuntimeError(f"{left:,} queued strings not answered yet (matcher time budget); the next run resumes")
RUN = "sweep-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
ID_ARRAY = "array(" + ",".join(f"CAST({i} AS BIGINT)" for i in sorted(ids)) + ")"
# materialized: after the MERGE below, a view would compare the new answers with themselves
spark.sql(f"""
CREATE OR REPLACE TABLE {CHANGES} AS
WITH cur AS (
  SELECT s.raw_affiliation_string, s.institution_ids AS new_ids, s.scores, s.decider, s.matcher_version, a.tier AS old_tier,
         CASE WHEN a.raw_affiliation_string IS NOT NULL THEN a.institution_ids
              WHEN l.institution_ids_override != array() THEN l.institution_ids_override
              WHEN SIZE(l.institution_ids) > 0 AND l.institution_ids[0] IS NULL THEN array()
              ELSE FILTER(COALESCE(l.institution_ids, array()), x -> x IS NOT NULL AND x != -1) END AS old_ids
  FROM {STAGING} s
  LEFT JOIN {ANSWERS} a ON a.raw_affiliation_string = s.raw_affiliation_string
  LEFT JOIN openalex.institutions.affiliation_strings_lookup l ON l.raw_affiliation_string = s.raw_affiliation_string
  WHERE s.decider <> 'no_swept_candidate' AND arrays_overlap(s.institution_ids, {ID_ARRAY})
)
SELECT '{RUN}' AS sweep_run, c.*, array_intersect(c.new_ids, {ID_ARRAY}) AS swept_ids, coalesce(w.works_count, 0) AS works_count
FROM cur c
LEFT JOIN openalex.institutions.affiliation_strings_lookup_with_counts w ON w.raw_affiliation_string = c.raw_affiliation_string
WHERE array_sort(c.new_ids) <> array_sort(coalesce(c.old_ids, array()))
""")
agg = spark.sql(f"SELECT count(*) AS strings, coalesce(sum(works_count), 0) AS works FROM {CHANGES}").collect()[0]
n_inst = spark.sql(f"SELECT count(DISTINCT x) AS n FROM {CHANGES} LATERAL VIEW explode(swept_ids) t AS x").collect()[0].n
log(f"would change {agg.strings:,} strings, {agg.works:,} works, naming {n_inst:,} new institutions")
if not APPLY:
    dbutils.notebook.exit(json.dumps({"dry_run": True, "strings": agg.strings, "works": agg.works, "institutions": n_inst}))
if agg.works > MAX_WORKS:
    raise RuntimeError(f"sweep would change {agg.works:,} works (> max_works {MAX_WORKS:,}); nothing written. "
                       f"Read {CHANGES} and ask before raising max_works.")

spark.sql(f"""
INSERT INTO {LOG}
SELECT sweep_run, raw_affiliation_string, old_ids, new_ids, swept_ids, old_tier, works_count, current_timestamp(), NULL
FROM {CHANGES}
""")
spark.sql(f"""
MERGE INTO {ANSWERS} t
USING (SELECT raw_affiliation_string, new_ids AS institution_ids, CAST(array() AS ARRAY<STRING>) AS countries, scores,
              decider, 'sweep' AS tier, matcher_version, current_timestamp() AS run_at FROM {CHANGES}) s
ON t.raw_affiliation_string = s.raw_affiliation_string
WHEN MATCHED THEN UPDATE SET institution_ids = s.institution_ids, countries = s.countries, scores = s.scores,
  decider = s.decider, tier = s.tier, matcher_version = s.matcher_version, run_at = s.run_at
WHEN NOT MATCHED THEN INSERT *
""")
spark.sql(f"""
MERGE INTO {SEEN} t
USING (SELECT i.institution_id, count(c.raw_affiliation_string) AS n
       FROM {SWEEP_IDS} i LEFT JOIN (SELECT raw_affiliation_string, explode(swept_ids) AS institution_id FROM {CHANGES}) c
         ON c.institution_id = i.institution_id GROUP BY i.institution_id) s
ON t.institution_id = s.institution_id
WHEN MATCHED THEN UPDATE SET swept_at = current_timestamp(), sweep_run = '{RUN}', n_applied_strings = s.n
""")
log(f"applied {RUN}: {agg.strings:,} strings, {agg.works:,} works; {len(ids):,} institutions marked swept")
