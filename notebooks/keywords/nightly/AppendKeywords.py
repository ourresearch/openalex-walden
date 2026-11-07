# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords nightly 4/4: append the new works, replace the re-tagged ones (oxjobs #1322, #2085)
# MAGIC
# MAGIC Checks `<state_prefix>append_rows` (one row per work, none already in the target except re-tagged works, no NULLs, the served
# MAGIC struct type, every keyword id + name in `openalex.common.keywords_v2`, every work in this build's queue) and only then MERGEs
# MAGIC into `target`: new works' rows are inserted; a re-tagged work (queue `retag`: its text changed since it was tagged, oxjob #2085)
# MAGIC gets its served row replaced, inserted, or deleted when it now has no keyword. Every row a re-tag replaces or deletes is first
# MAGIC copied into `<state_prefix>replaced` with the build id (restore a build by MERGE from there). No other row is touched. Any
# MAGIC failed check fails the task and writes nothing. Then every work of the build (with or without keywords) is recorded in the
# MAGIC tagged-ids record `<state_prefix>tagged` with the `text_key` it was tagged on (so it is queued again only if its text
# MAGIC changes), and one line goes into `<state_prefix>runs`. A repair re-runs the same idempotent MERGEs.
# MAGIC The next End 2 End merges work_keywords_v2 into `openalex_works` (one re-stamp per new or re-tagged work).

# COMMAND ----------

import datetime as dt
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "keywords_nightly.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "keywords_nightly.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import keywords_nightly as kn  # noqa: E402

for name, default in [("state_prefix", "openalex.works.work_keywords_v2_"), ("target", "openalex.works.work_keywords_v2"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW, ROWS, TAGGED, RUNS, REPLACED = f"{P}queue", f"{P}raw", f"{P}append_rows", f"{P}tagged", f"{P}runs", f"{P}replaced"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def task_value(task, key, default):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task, key=key, default=default, debugValue=default)
    except Exception:
        return default

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (build_id STRING, queued BIGINT, tagged BIGINT, appended BIGINT, no_keywords BIGINT,
  assignments BIGINT, usd_est DOUBLE, modal_wall_s DOUBLE, target STRING, finished_at TIMESTAMP)
  COMMENT 'Keywords nightly (oxjob #1322): one row per finished run'""")
if "retagged" not in {c.name for c in spark.table(RUNS).schema}:
    spark.sql(f"ALTER TABLE {RUNS} ADD COLUMNS (retagged BIGINT COMMENT 'works tagged again because their text changed (oxjob #2085)')")
if "text_key" not in {c.name for c in spark.table(TAGGED).schema}:
    spark.sql(f"ALTER TABLE {TAGGED} ADD COLUMNS (text_key BIGINT COMMENT 'xxhash64(title, abstract[:6000]) the work was tagged on (oxjob #2085)')")
RUN_COLS = "build_id, queued, tagged, appended, no_keywords, assignments, usd_est, modal_wall_s, target, finished_at, retagged"
q = spark.sql(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}").collect()[0]
n, build_id = int(q.n), q.b
if DRY:
    dbutils.notebook.exit("dry run")
if n == 0:
    spark.sql(f"INSERT INTO {RUNS} ({RUN_COLS}) VALUES (NULL, 0, 0, 0, 0, 0, 0, 0, '{TARGET}', current_timestamp(), 0)")
    dbutils.notebook.exit("empty queue: nothing to append")
tagged_n = spark.sql(f"SELECT count(*) AS n FROM {RAW} WHERE build_id = '{build_id}'").collect()[0].n
if tagged_n != n:
    raise RuntimeError(f"{RAW} holds {tagged_n:,} rows for build {build_id}, queue {n:,}")
# re-tagged works (oxjob #2085); a queue built before #2085 has none and no text_key
qcols = {c.name for c in spark.table(QUEUE).schema}
RETAG = f"(SELECT id FROM {QUEUE} WHERE retag)" if "retag" in qcols else kn.NO_RETAG
QKEY = "q.text_key" if "text_key" in qcols else "CAST(NULL AS BIGINT)"
n_retag = spark.sql(f"SELECT count(*) AS n FROM {RETAG}").collect()[0].n

# COMMAND ----------

res = {k: spark.sql(sql).collect()[0].asDict() for k, sql in kn.check_statements(ROWS, TARGET, QUEUE, retag=RETAG).items()}
print(json.dumps(res, indent=1, default=str))
rows_n, already = int(res["shape"]["works"]), int(res["already_in_target"]["n"])
rows_new = spark.sql(f"SELECT count(*) AS n FROM {ROWS} r LEFT ANTI JOIN {RETAG} q ON q.id = r.work_id").collect()[0].n
repair = bool(rows_new) and already == rows_new
if repair:
    # a repair of this task after its MERGE went through: accept only if the new works' served rows are these rows
    same = spark.sql(f"""SELECT count(*) AS n FROM {ROWS} r JOIN {TARGET} t ON t.work_id = r.work_id AND to_json(t.keywords) = to_json(r.keywords)
      LEFT ANTI JOIN {RETAG} q ON q.id = r.work_id""").collect()[0].n
    if same != rows_new:
        raise RuntimeError(f"all {rows_new:,} new work_ids are already in {TARGET} but only {same:,} with these keywords; NOTHING written")
    log(f"all {rows_new:,} new rows of build {build_id} are already in {TARGET} (an earlier attempt wrote them); the MERGE below repeats them")
    res["already_in_target"]["n"] = 0
ok, bad = kn.checks_pass(res)
if not ok:
    raise RuntimeError("pre-append checks failed, NOTHING written: " + "; ".join(bad))
log(f"checks pass: {rows_n:,} rows for build {build_id} ({n_retag:,} works re-tagged)")

# Keep every served row a re-tag replaces or deletes (once per build: a repair finds them saved), then one MERGE.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {REPLACED} (build_id STRING, replaced_at TIMESTAMP, work_id BIGINT,
  keywords ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT>>, tagger_version STRING, updated_at TIMESTAMP)
  COMMENT 'Keywords nightly (oxjob #2085): served rows replaced or deleted by a re-tag, one per (build, work); restore a build by MERGE from here'""")
spark.sql(f"""INSERT INTO {REPLACED}
SELECT '{build_id}', current_timestamp(), t.work_id, t.keywords, t.tagger_version, t.updated_at
FROM {TARGET} t JOIN {RETAG} q ON q.id = t.work_id
LEFT ANTI JOIN (SELECT work_id FROM {REPLACED} WHERE build_id = '{build_id}') d ON d.work_id = t.work_id""")
unsaved = spark.sql(f"""SELECT count(*) AS n FROM {TARGET} t JOIN {RETAG} q ON q.id = t.work_id
  LEFT ANTI JOIN (SELECT work_id FROM {REPLACED} WHERE build_id = '{build_id}') d ON d.work_id = t.work_id""").collect()[0].n
if unsaved:
    raise RuntimeError(f"{unsaved:,} served rows of re-tagged works are not saved in {REPLACED}; NOTHING written")
before = spark.table(TARGET).count()
spark.sql(f"""MERGE INTO {TARGET} t
USING (SELECT COALESCE(r.work_id, q.id) AS work_id, r.keywords, r.tagger_version FROM {ROWS} r FULL OUTER JOIN {RETAG} q ON q.id = r.work_id) s
ON t.work_id = s.work_id
WHEN MATCHED AND s.keywords IS NULL THEN DELETE
WHEN MATCHED THEN UPDATE SET t.keywords = s.keywords, t.tagger_version = s.tagger_version, t.updated_at = current_timestamp()
WHEN NOT MATCHED AND s.keywords IS NOT NULL THEN INSERT (work_id, keywords, tagger_version, updated_at)
  VALUES (s.work_id, s.keywords, s.tagger_version, current_timestamp())""")
after = spark.table(TARGET).count()
chk = spark.sql(f"""SELECT
  (SELECT count(*) FROM {ROWS} r JOIN {TARGET} t ON t.work_id = r.work_id AND to_json(t.keywords) = to_json(r.keywords)) AS served,
  (SELECT count(*) FROM {RETAG} q JOIN {TARGET} t ON t.work_id = q.id LEFT ANTI JOIN {ROWS} r ON r.work_id = q.id) AS emptied_still_served""").collect()[0]
appended = rows_new if not repair else 0
log(f"merged build {build_id} into {TARGET}: {before:,} -> {after:,} rows; {chk.served:,} of {rows_n:,} rows served as staged")
if chk.served != rows_n or chk.emptied_still_served:
    raise RuntimeError(f"after the MERGE {chk.served:,} of {rows_n:,} rows are served as staged and {chk.emptied_still_served:,} re-tagged works "
                       "without keywords still have a row (another writer?)")

# COMMAND ----------

spark.sql(f"""MERGE INTO {TAGGED} t
USING (SELECT CAST(substr(r.id, 2) AS BIGINT) AS work_id, r.tagger_version, r.tagged_at, r.build_id AS run, {QKEY} AS text_key
       FROM {RAW} r LEFT JOIN {QUEUE} q ON concat('W', CAST(q.id AS STRING)) = r.id WHERE r.build_id = '{build_id}'
       QUALIFY row_number() OVER (PARTITION BY r.id ORDER BY r.tagged_at DESC) = 1) s
ON t.work_id = s.work_id
WHEN MATCHED THEN UPDATE SET t.tagger_version = s.tagger_version, t.tagged_at = s.tagged_at, t.run = s.run, t.text_key = s.text_key
WHEN NOT MATCHED THEN INSERT (work_id, tagger_version, tagged_at, run, text_key) VALUES (s.work_id, s.tagger_version, s.tagged_at, s.run, s.text_key)""")
a = spark.sql(f"SELECT sum(size(keywords)) AS s FROM {ROWS}").collect()[0].s or 0
spark.sql(f"""INSERT INTO {RUNS} ({RUN_COLS}) VALUES ('{build_id}', {n}, {tagged_n}, {appended}, {n - rows_n}, {a},
  {float(task_value('tag', 'usd_est', 0.0))}, {float(task_value('tag', 'modal_wall_s', 0.0))}, '{TARGET}', current_timestamp(), {n_retag})""")
log(f"build {build_id}: {n:,} queued ({n_retag:,} re-tags), {rows_n:,} with keywords, {n - rows_n:,} with none, recorded in {TAGGED}")
