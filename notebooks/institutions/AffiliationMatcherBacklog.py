# Databricks notebook source
# MAGIC %md
# MAGIC # Affiliation matcher: daily backlog (oxjob #1386)
# MAGIC
# MAGIC The nightly matcher (End 2 End, `AffiliationMatcherNightly`) answers about 40K new strings a night, newest first,
# MAGIC inside a 26-minute window. New strings run 20K–60K on an ordinary night and 150K–300K on backfill nights, so some
# MAGIC wait and their works publish with the 2023 model's ids meanwhile (25 Oct 2026: 943K strings had aged out unanswered).
# MAGIC This job answers every string the nightly hasn't reached, off the End 2 End path, so a spike never waits for a session.
# MAGIC
# MAGIC Job `Affiliation Matcher Backlog` (jobs/affiliation_matcher_backlog.yaml), daily:
# MAGIC `step=check` (serverless, seconds): queue = lookup strings created since the corpus snapshot (27 Sep 2026) with no
# MAGIC answers row; sets task values `pending` and `shards` (one GPU shard per `per_shard` strings, at most `max_shards`).
# MAGIC Then `AffiliationMatcherNightly` with `queue_table`, `shard` k/n and `target_table` answers shard k into
# MAGIC `<prefix>_backlog_answers_<k>` on its own GPU cluster, with the nightly's production settings. `step=apply`:
# MAGIC checks the shard tables and inserts their answers into the answers table exactly as the nightly does (insert-only
# MAGIC MERGE: a string answered meanwhile keeps its answer), and records every inserted string in `<prefix>_backlog_log`.
# MAGIC
# MAGIC Revert one run: `MERGE INTO <answers> a USING (SELECT * FROM <prefix>_backlog_log WHERE backlog_run = '<run>') l
# MAGIC ON a.raw_affiliation_string = l.raw_affiliation_string AND a.run_at = l.run_at WHEN MATCHED THEN DELETE`; the
# MAGIC strings return to the queue and their works to the legacy ids at the next End 2 End. Stop it: pause the schedule.
# MAGIC A run whose answers would change more than `max_works` works writes nothing and fails (a bug fence like the sweep's;
# MAGIC for a real backlog that big, pre-clear the End 2 End and rerun apply with a higher `max_works`).

# COMMAND ----------

import json
import math
import time

dbutils.widgets.text("step", "check", "check | apply")
dbutils.widgets.text("answers_table", "openalex.institutions.affiliation_matcher_answers", "answers table")
dbutils.widgets.text("prefix", "openalex.institutions.affiliation_matcher", "state tables: <prefix>_backlog_queue, _backlog_answers_<k>, _backlog_answers, _backlog_log")
dbutils.widgets.text("since", "2026-09-27", "queue floor: strings created before the corpus snapshot are the corpus run's to answer")
dbutils.widgets.text("per_shard", "150000", "strings per GPU shard (≈ 60-90 min each with Jev to its deadline)")
dbutils.widgets.text("max_shards", "6", "GPU shards the job defines (match_0 .. match_<n-1>)")
dbutils.widgets.text("max_works", "2000000", "apply writes nothing and fails if more works than this could change")
dbutils.widgets.text("backlog_run", "", "run id recorded in the log (the job passes {{job.run_id}})")

STEP = dbutils.widgets.get("step").strip()
ANSWERS = dbutils.widgets.get("answers_table").strip()
P = dbutils.widgets.get("prefix").strip()
QUEUE, ALL, LOG = f"{P}_backlog_queue", f"{P}_backlog_answers", f"{P}_backlog_log"
SINCE = dbutils.widgets.get("since").strip()
PER_SHARD = int(dbutils.widgets.get("per_shard"))
MAX_SHARDS = int(dbutils.widgets.get("max_shards"))
MAX_WORKS = int(dbutils.widgets.get("max_works"))
RUN = dbutils.widgets.get("backlog_run").strip() or f"manual-{int(time.time())}"
SHARD_TABLES = [f"{P}_backlog_answers_{k}" for k in range(MAX_SHARDS)]
LOOKUP = "openalex.institutions.affiliation_strings_lookup"
T0 = time.time()
assert STEP in ("check", "apply"), STEP


def log(msg):
    print(f"[{time.time() - T0:7.0f}s] {msg}", flush=True)

# COMMAND ----------

if STEP == "check":
    # Shard tables from an earlier run: apply has already inserted what it could (insert-only), so start clean; any
    # string a failed run left unapplied is still unanswered and comes back in today's queue.
    for t in SHARD_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"""
CREATE OR REPLACE TABLE {QUEUE}
COMMENT 'oxjob #1386 backlog: lookup strings since the corpus snapshot with no answers row (rebuilt each run)' AS
SELECT DISTINCT l.raw_affiliation_string
FROM {LOOKUP} l
LEFT ANTI JOIN {ANSWERS} a ON a.raw_affiliation_string = l.raw_affiliation_string
WHERE l.created_datetime >= TIMESTAMP '{SINCE}'
  AND l.raw_affiliation_string IS NOT NULL AND trim(l.raw_affiliation_string) <> ''
""")
    pending = spark.table(QUEUE).count()
    shards = 0 if pending == 0 else min(MAX_SHARDS, math.ceil(pending / PER_SHARD))
    oldest = spark.sql(f"SELECT min(l.created_datetime) AS t FROM {QUEUE} q JOIN {LOOKUP} l USING (raw_affiliation_string)").collect()[0].t
    log(f"queue: {pending:,} unanswered strings since {SINCE} (oldest created {oldest}); shards: {shards}")
    dbutils.jobs.taskValues.set("pending", int(pending))
    dbutils.jobs.taskValues.set("shards", int(shards))
    dbutils.notebook.exit(json.dumps({"pending": int(pending), "shards": int(shards), "oldest": str(oldest)}))

# COMMAND ----------

present = [t for t in SHARD_TABLES if spark.catalog.tableExists(t)]
if not present:
    dbutils.notebook.exit(json.dumps({"inserted": 0, "note": "no shard tables"}))
spark.sql(f"CREATE OR REPLACE TABLE {ALL} COMMENT 'oxjob #1386 backlog: the last run''s shard answers in one table' AS "
          + " UNION ALL ".join(f"SELECT * FROM {t}" for t in present))
n, nd, nnull, tiers = spark.sql(f"""SELECT count(*), count(DISTINCT raw_affiliation_string),
    sum(CASE WHEN institution_ids IS NULL THEN 1 ELSE 0 END), collect_set(tier) FROM {ALL}""").collect()[0]
log(f"{len(present)} shard tables: {n:,} answers, {nd:,} distinct strings, {nnull or 0} NULL ids, tiers {tiers}")
assert n == nd, "a string answered twice across shards"
assert not nnull, "NULL institution_ids"
assert n == 0 or list(tiers) == ["nightly"], f"unexpected tiers {tiers}"
if n == 0:
    dbutils.notebook.exit(json.dumps({"inserted": 0, "note": "shard tables empty"}))

# Works whose institutions can change: strings whose answer differs from today's published ids (the view), as the
# view computes them for an answered string (answer ∪ curated adds − curated removes).
chg, works, empty_new = spark.sql(f"""
WITH x AS (
  SELECT s.raw_affiliation_string, coalesce(m.institution_ids, array()) AS old_ids,
         array_except(array_union(s.institution_ids, coalesce(m.curated_add_ids, array())),
                      coalesce(m.curated_remove_ids, array())) AS new_ids
  FROM {ALL} s
  LEFT ANTI JOIN {ANSWERS} a ON a.raw_affiliation_string = s.raw_affiliation_string
  LEFT JOIN openalex.institutions.raw_affiliation_strings_institutions_mv m ON m.raw_affiliation_string = s.raw_affiliation_string)
SELECT count(*), coalesce(sum(coalesce(w.works_count, 0)), 0), coalesce(sum(CASE WHEN size(new_ids) = 0 THEN 1 ELSE 0 END), 0)
FROM x LEFT JOIN openalex.institutions.affiliation_string_works_counts w ON w.raw_aff_string = x.raw_affiliation_string
WHERE NOT (array_sort(new_ids) <=> array_sort(old_ids))""").collect()[0]
log(f"strings whose published ids change: {chg:,}; works on them (upper bound for re-stamps): {works:,}; new answer empty: {empty_new:,}")
dbutils.jobs.taskValues.set("works_upper", int(works))
if works > MAX_WORKS:
    raise Exception(f"{works:,} works could change, over max_works {MAX_WORKS:,}: nothing written. If the backlog is real, "
                    f"pre-clear the End 2 End (scripts/preclear_e2e.py) and rerun apply with a higher max_works.")

res = spark.sql(f"""MERGE INTO {ANSWERS} t USING {ALL} s ON t.raw_affiliation_string = s.raw_affiliation_string
                    WHEN NOT MATCHED THEN INSERT *""").collect()
log(f"MERGE: {res}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG} (backlog_run STRING, raw_affiliation_string STRING, run_at TIMESTAMP,
              applied_at TIMESTAMP) COMMENT 'oxjob #1386 backlog: every answer the daily backlog job inserted (the revert list)'""")
spark.sql(f"""MERGE INTO {LOG} t
USING (SELECT '{RUN}' AS backlog_run, s.raw_affiliation_string, s.run_at FROM {ALL} s
       JOIN {ANSWERS} a ON a.raw_affiliation_string = s.raw_affiliation_string AND a.run_at = s.run_at
                        AND a.matcher_version <=> s.matcher_version) s
ON t.backlog_run = s.backlog_run AND t.raw_affiliation_string = s.raw_affiliation_string
WHEN NOT MATCHED THEN INSERT (backlog_run, raw_affiliation_string, run_at, applied_at)
  VALUES (s.backlog_run, s.raw_affiliation_string, s.run_at, current_timestamp())""")
inserted = spark.sql(f"SELECT count(*) AS n FROM {LOG} WHERE backlog_run = '{RUN}'").collect()[0].n
dups = spark.sql(f"""SELECT count(*) AS n FROM (SELECT a.raw_affiliation_string FROM {ANSWERS} a
                     JOIN {ALL} s ON s.raw_affiliation_string = a.raw_affiliation_string
                     GROUP BY a.raw_affiliation_string HAVING count(*) > 1)""").collect()[0].n
left = spark.sql(f"SELECT count(*) AS n FROM {QUEUE} q LEFT ANTI JOIN {ANSWERS} a USING (raw_affiliation_string)").collect()[0].n
log(f"inserted {inserted:,} of {n:,} answers (the rest were answered meanwhile); duplicates {dups}; queue strings still unanswered {left:,}")
assert dups == 0, "duplicate answers rows"
dbutils.jobs.taskValues.set("inserted", int(inserted))
dbutils.notebook.exit(json.dumps({"run": RUN, "answers": int(n), "inserted": int(inserted), "ids_change": int(chg),
                                  "works_upper": int(works), "still_unanswered": int(left)}))
