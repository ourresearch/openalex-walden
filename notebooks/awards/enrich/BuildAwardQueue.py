# Databricks notebook source
# MAGIC %md
# MAGIC # Awards enrich 1/4: the queue of awards to score (topics, keywords, SDGs)
# MAGIC
# MAGIC Job AwardTopics (`jobs/award_topics.yaml`), beside Nightly Grants, not a step inside it. Queue = awards in `awards_table` with
# MAGIC usable text (`lib/award_text.py`: title >= 4 words or description >= 20 words; CJK by characters, >= 8 in the title or
# MAGIC >= 40 in the description) that are missing from any of the three served side tables, or whose text changed since they were
# MAGIC scored there (`text_key`, as oxjob #2085 does for works), newest first (highest id), at most `max_awards`.
# MAGIC `force_ids_table` (samples / backfills): queue exactly the usable awards in that table (column `id`), served or not.
# MAGIC
# MAGIC Writes `<state_prefix>queue` and, for Jason's Modal apps, `modal_queue_table` (the same rows; the apps only read
# MAGIC `openalex.(works|transient).*`, so with dev state the copy is the one table outside dev). One table serves both apps:
# MAGIC topics reads `shard, work_id, title, abstract, venue`, keywords reads `id, title, abstract, journal WHERE shard = n`.
# MAGIC `venue` / `journal` are NULL ("(unknown)" in the prompt). Shards `pmod(hash(award_id), n)`, n = ceil(awards / shard_works).

# COMMAND ----------

import datetime as dt
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import award_text as T  # noqa: E402

for name, default in [("state_prefix", "openalex_dev.rohan_lab.awt_"), ("awards_table", "openalex.awards.awards_api"),
                      ("modal_queue_table", "openalex.transient.awt_queue"), ("topics_target", "openalex_dev.rohan_lab.awt_award_topics_v2"),
                      ("keywords_target", "openalex_dev.rohan_lab.awt_award_keywords"), ("sdgs_target", "openalex_dev.rohan_lab.awt_award_sdgs"),
                      ("max_awards", "2000000"), ("shard_works", "25000"), ("force_ids_table", ""), ("force_ids_file", ""),
                      ("dry_run", "true")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
AWARDS = dbutils.widgets.get("awards_table").strip()
MQ = dbutils.widgets.get("modal_queue_table").strip()
TARGETS = [dbutils.widgets.get(k).strip() for k in ("topics_target", "keywords_target", "sdgs_target")]
MAX = int(dbutils.widgets.get("max_awards"))
SHARD = int(dbutils.widgets.get("shard_works"))
FORCE = dbutils.widgets.get("force_ids_table").strip()
FORCE_FILE = dbutils.widgets.get("force_ids_file").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE = f"{P}queue"
for t in [P + "x", AWARDS, MQ] + TARGETS + ([FORCE] if FORCE else []):
    assert t.replace(".", "").replace("_", "").isalnum() and t.count(".") == 2, f"bad table name {t!r}"
if FORCE_FILE:   # a CSV with a column id (the 10k sample: sample/awt_sample_ids.csv), read into a temp view: no table is written
    assert not FORCE, "pass force_ids_table or force_ids_file, not both"
    import pandas as pd
    ids = pd.read_csv(FORCE_FILE, usecols=["id"]).drop_duplicates()
    spark.createDataFrame(ids.astype({"id": "int64"})).createOrReplaceTempView("awt_force_ids")
    FORCE = "awt_force_ids"
    print(f"force_ids_file {FORCE_FILE}: {len(ids):,} ids", flush=True)


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

# COMMAND ----------

# served = (award_id, text_key) in all three targets; a missing target counts as empty (first run)
served = [f"(SELECT award_id, text_key FROM {t})" for t in TARGETS if spark.catalog.tableExists(t)]
if FORCE:
    cand = f"SELECT u.*, false AS rescore FROM ({T.usable_awards_sql(AWARDS)}) u WHERE u.id IN (SELECT id FROM {FORCE})"
elif len(served) < 3:
    cand = f"SELECT u.*, false AS rescore FROM ({T.usable_awards_sql(AWARDS)}) u"
    log(f"{3 - len(served)} target(s) missing: every usable award is a candidate (a first run; cap max_awards {MAX:,})")
else:
    # in every target with today's text = done; anything else is (re)queued
    joins = " ".join(f"LEFT JOIN {s} s{i} ON s{i}.award_id = u.id" for i, s in enumerate(served))
    done = " AND ".join(f"s{i}.text_key <=> u.text_key" for i in range(3))
    seen = " OR ".join(f"s{i}.award_id IS NOT NULL" for i in range(3))
    cand = f"SELECT u.*, ({seen}) AS rescore FROM ({T.usable_awards_sql(AWARDS)}) u {joins} WHERE NOT ({done})"
cnt = spark.sql(f"SELECT count(*) AS n, count_if(rescore) AS re, count_if(cjk_title) AS cjk FROM ({cand})").collect()[0]
take = min(cnt.n, MAX)
n_shards = max(1, math.ceil(take / SHARD))
build_id = "awt-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
log(f"candidates {cnt.n:,} ({cnt.re:,} text changed, {cnt.cjk:,} CJK titles); queueing {take:,} in {n_shards} shards; build {build_id}")
if DRY:   # no queue is written: the scorer tasks' dry runs read these task values, never a (missing or stale) queue table
    dbutils.jobs.taskValues.set(key="build_id", value=build_id)
    dbutils.jobs.taskValues.set(key="queued", value=int(take))
    dbutils.jobs.taskValues.set(key="shards", value=int(n_shards))
    dbutils.notebook.exit(f"dry run: {cnt.n:,} candidates, would queue {take:,} in {n_shards} shards")

spark.sql(f"""CREATE OR REPLACE TABLE {QUEUE} CLUSTER BY (shard)
COMMENT 'Awards enrich queue: usable awards missing from or changed since the side tables; rebuilt every run by BuildAwardQueue'
AS SELECT CAST(pmod(hash(c.id), {n_shards}) AS INT) AS shard, c.id AS award_id, c.id AS work_id, c.id AS id,
  c.display_name AS title, substr(c.description, 1, {T.ABSTRACT_CUT}) AS abstract, CAST(NULL AS STRING) AS venue, CAST(NULL AS STRING) AS journal,
  c.provenance, c.cjk_title, c.text_key, c.rescore, '{build_id}' AS build_id, current_timestamp() AS queued_at
FROM ({cand}) c QUALIFY row_number() OVER (ORDER BY c.id DESC) <= {take}""")
q = spark.sql(f"SELECT count(*) AS n, count(DISTINCT award_id) AS ids, count(DISTINCT shard) AS shards FROM {QUEUE}").collect()[0]
if q.n != take or q.ids != take:
    raise RuntimeError(f"queue holds {q.n:,} rows / {q.ids:,} ids, expected {take:,}")
if MQ != QUEUE:
    spark.sql(f"""CREATE OR REPLACE TABLE {MQ} CLUSTER BY (shard)
      COMMENT 'Awards enrich: copy of {QUEUE} for the Modal apps (they read openalex.(works|transient) only); rebuilt every run'
      AS SELECT shard, work_id, id, title, abstract, venue, journal, build_id FROM {QUEUE}""")
log(f"queue {QUEUE}: {q.n:,} awards, {q.shards} shards" + (f"; Modal copy {MQ}" if MQ != QUEUE else ""))
dbutils.jobs.taskValues.set(key="build_id", value=build_id)
dbutils.jobs.taskValues.set(key="queued", value=int(q.n))
dbutils.jobs.taskValues.set(key="shards", value=int(q.shards))
