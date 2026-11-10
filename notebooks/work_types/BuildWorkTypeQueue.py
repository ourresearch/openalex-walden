# Databricks notebook source
# MAGIC %md
# MAGIC # Work types 1/2: build the scorer queue (oxjob #1976; the topics nightly's shape, notebooks/topics/nightly)
# MAGIC
# MAGIC Runs after End 2 End (job `Classify_Work_Types`). Queue = the live works (non-xpac unless `include_xpac`; one backfill chunk
# MAGIC `pmod(id, chunks) = chunk`, or all) whose pack inputs changed since their score: every live work's `input_key`
# MAGIC (`sql/02_queue_cand.sql`: the fingerprint of exactly what its pack is assembled from, tags and source types included) is
# MAGIC compared with its latest-seen row in `openalex.works.work_type_scores` for the policy's model; no row = new, another key =
# MAGIC changed. New works first, newest first (highest id), at most `max_works`; the rest wait for the next run.
# MAGIC
# MAGIC The candidates' packs are assembled (`lib/pack_assembly.py`, the s2r3 text and its hash `input_hash`). A text the work
# MAGIC already has a score for (its inputs came back, or changed only outside the text) is not re-scored: that row is touched
# MAGIC (`input_key`, `seen_at`, the stage-0 signals), so it is the work's current row again. The rest is the queue table
# MAGIC `openalex.transient.work_type_queue` (rebuilt each build; shards of `shard_works`), which the Modal app exports.
# MAGIC Scratch (the candidates, the packs) is parquet under `<volume_dir>/_build/<build_id>/`, never a table.
# MAGIC
# MAGIC A retry reuses an unfinished queue (no score row carries its build id) younger than `reuse_hours`, so the score task
# MAGIC resumes the same Modal call instead of scoring twice.

# COMMAND ----------

import datetime as dt
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import pandas as pd  # noqa: E402
import pack_assembly as A  # noqa: E402
import sql_templates as T  # noqa: E402
import work_type_cell as C  # noqa: E402

for name, default in [("schema", "openalex.works"), ("queue", "openalex.transient.work_type_queue"),
                      ("policy_id", "pol5-4ed2aed64c8e"), ("volume_dir", "/Volumes/openalex/works/work_types"),
                      ("max_works", "3000000"), ("shard_works", "50000"), ("chunk", "0"), ("chunks", "1"),
                      ("include_xpac", "false"), ("reuse_hours", "20"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)  # noqa: F821
W = {k: dbutils.widgets.get(k).strip() for k in ("schema", "queue", "policy_id", "volume_dir", "max_works", "shard_works",  # noqa: F821
                                                  "chunk", "chunks", "include_xpac", "reuse_hours", "dry_run")}
S, QUEUE, VOL = W["schema"], W["queue"], W["volume_dir"].rstrip("/")
MAX_WORKS, SHARD_WORKS, REUSE_H = int(W["max_works"]), int(W["shard_works"]), float(W["reuse_hours"])
CHUNK, CHUNKS = int(W["chunk"]), int(W["chunks"])
DRY = W["dry_run"].lower() == "true"
POL = T.load_policy(W["policy_id"])                     # recomputes the policy and model ids; refuses a tampered file
if POL["model_id"] not in C.SCORED_MODELS:
    raise RuntimeError(f"{POL['policy_id']}: its model {POL['model_id']} isn't scored by the Modal app ({C.SCORED_MODELS})")
for t in (S + ".x", QUEUE):
    assert t.startswith("openalex.") and t.replace(".", "").replace("_", "").isalnum(), f"bad table name {t!r}"
assert VOL.startswith("/Volumes/openalex/") and ".." not in VOL, f"bad volume_dir {VOL!r}"
assert W["include_xpac"] in ("true", "false") and CHUNKS >= 1 and 0 <= CHUNK < CHUNKS and 1 <= SHARD_WORKS <= 60000
P = {"S": S, "model_id": POL["model_id"], "render_version": POL["render_version"], "pack_version": A.bp.PACK_VERSION,
     "include_xpac": W["include_xpac"], "chunk": str(CHUNK), "chunks": str(CHUNKS)}


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def run_sql(name, **more):
    for st in T.statements(T.render(name, **dict(P, **more))):
        spark.sql(st)  # noqa: F821


def one(sql):
    return spark.sql(sql).collect()[0]  # noqa: F821

# COMMAND ----------

# The scores table is created by the setup SQL, never here (an empty one would queue the whole corpus).
if not spark.catalog.tableExists(f"{S}.work_type_scores"):  # noqa: F821
    raise RuntimeError(f"{S}.work_type_scores does not exist: run sql/01_setup.sql first (RUN-GOLINE1.md)")

# Resume an unfinished build (task or job retry) instead of re-queueing under a new build id.
if spark.catalog.tableExists(QUEUE) and not DRY:  # noqa: F821
    prev = one(f"SELECT max(build_id) AS b, count(*) AS n, max(queued_at) AS at FROM {QUEUE}")
    if prev.n:
        loaded = one(f"SELECT count(*) AS n FROM {S}.work_type_scores WHERE build_id = '{prev.b}'").n
        age_h = (dt.datetime.utcnow() - prev.at).total_seconds() / 3600 if prev.at else 1e9
        if loaded == 0 and age_h < REUSE_H:
            log(f"reusing unfinished build {prev.b} ({prev.n:,} works, queued {age_h:.1f} h ago)")
            dbutils.jobs.taskValues.set(key="build_id", value=prev.b)  # noqa: F821
            dbutils.jobs.taskValues.set(key="queued", value=int(prev.n))  # noqa: F821
            dbutils.notebook.exit(f"reused build {prev.b}")  # noqa: F821

# COMMAND ----------

build_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + ("" if CHUNKS == 1 else f"-c{CHUNK:02d}of{CHUNKS:02d}")
scratch = f"{VOL}/_build/{build_id}"
run_sql("02_queue_keys.sql")
run_sql("02_pack_rows.sql")                    # every live work's pack inputs (the keys only are kept)
run_sql("02_queue_cand.sql")
cnt = one("SELECT count_if(new) AS new, count_if(NOT new) AS changed FROM wt_cand")
take_new = min(cnt.new, MAX_WORKS)
take_ch = min(cnt.changed, MAX_WORKS - take_new)
log(f"build {build_id}: {cnt.new:,} works with no score, {cnt.changed:,} whose inputs changed; taking {take_new:,} + {take_ch:,}")
if DRY:
    dbutils.notebook.exit(f"dry run: {cnt.new:,} new, {cnt.changed:,} changed")  # noqa: F821
(spark.sql(f"""SELECT work_id, input_key, new FROM wt_cand  -- noqa
               QUALIFY row_number() OVER (PARTITION BY new ORDER BY work_id DESC) <= CASE WHEN new THEN {take_new} ELSE {take_ch} END""")
 .write.mode("overwrite").parquet(f"{scratch}/cand"))

# COMMAND ----------

# The candidates' packs: the same pack statement over them, read ONCE into the scratch, so the text and the key stored with
# it come from the same inputs (the sources aren't one snapshot across two reads: Codex 11-09 #3). Assembled in Python
# (s2r3 text, input_hash, stage-0 signals); the key is recomputed over these rows (02_queue_cand.sql's wt_keys).
spark.read.parquet(f"{scratch}/cand").createOrReplaceTempView("wt_work_set")  # noqa: F821
run_sql("02_pack_rows.sql")
spark.table("wt_pack_rows").write.mode("overwrite").parquet(f"{scratch}/rows")  # noqa: F821
spark.read.parquet(f"{scratch}/rows").createOrReplaceTempView("wt_pack_rows")  # noqa: F821
keys_st = [st for st in T.statements(T.render("02_queue_cand.sql", **P)) if "TEMP VIEW wt_keys AS" in T.code_of(st)]
assert len(keys_st) == 1, "02_queue_cand.sql must define wt_keys once"
spark.sql(keys_st[0])  # noqa: F821


def _assemble(it):
    for b in it:
        yield pd.DataFrame([A.assemble_row(w, r, t, x) for w, r, t, x in zip(b.work_j, b.recs, b.tags, b.b3)],
                           columns=["work_id", "input_hash", "render_version", "pack_version", "text", "s0_doi", "s0_names", "s0_urls"])


(spark.table("wt_pack_rows")  # noqa: F821
 .mapInPandas(_assemble, "work_id BIGINT, input_hash STRING, render_version STRING, pack_version STRING, text STRING, "
                         "s0_doi STRING, s0_names ARRAY<STRING>, s0_urls ARRAY<STRING>")
 .join(spark.table("wt_keys"), "work_id")  # noqa: F821   (the key of exactly these inputs)
 .join(spark.read.parquet(f"{scratch}/cand").select("work_id", "new"), "work_id")  # noqa: F821
 .write.mode("overwrite").parquet(f"{scratch}/packs"))
spark.read.parquet(f"{scratch}/packs").createOrReplaceTempView("wt_packs")  # noqa: F821
g = one(f"""SELECT count(*) AS n, count(DISTINCT work_id) AS ids, count_if(text IS NULL OR input_hash IS NULL) AS nulls,
  count_if(render_version <> '{POL['render_version']}') AS bad_render,
  (SELECT count(*) FROM wt_cand_files c LEFT ANTI JOIN wt_packs p ON p.work_id = c.work_id) AS unpacked
  FROM wt_packs""".replace("wt_cand_files", f"parquet.`{scratch}/cand`"))
if not (g.n == g.ids == take_new + take_ch and g.nulls == 0 and g.bad_render == 0 and g.unpacked == 0):
    raise RuntimeError(f"packs do not match the candidates: {g.asDict()}")

# COMMAND ----------

# A text the work already has a score for is not re-scored: touch that row (it is the work's current one again).
t = spark.sql(f"""MERGE INTO {S}.work_type_scores s  -- noqa
USING wt_packs p
ON s.work_id = p.work_id AND s.input_hash = p.input_hash AND s.model_version = '{POL['model_id']}'
WHEN MATCHED THEN UPDATE SET s.input_key = p.input_key, s.seen_at = current_timestamp(),
  s.s0_doi = p.s0_doi, s.s0_names = p.s0_names, s.s0_urls = p.s0_urls""").collect()[0]
touched = int(t.num_updated_rows)
n_q = one(f"""SELECT count(*) AS n FROM wt_packs p LEFT ANTI JOIN
  (SELECT work_id, input_hash FROM {S}.work_type_scores WHERE model_version = '{POL['model_id']}') s
  ON s.work_id = p.work_id AND s.input_hash = p.input_hash""").n
n_shards = max(1, math.ceil(n_q / SHARD_WORKS))
spark.sql(f"""CREATE OR REPLACE TABLE {QUEUE} CLUSTER BY (shard)  -- noqa
COMMENT 'Work types (oxjob #1976): the texts to score, rebuilt every build by BuildWorkTypeQueue; exported once by the Modal app openalex-worktypes-nightly'
AS SELECT CAST(pmod(hash(p.work_id), {n_shards}) AS INT) AS shard, p.work_id, p.input_hash, p.text, p.input_key, p.new,
          p.s0_doi, p.s0_names, p.s0_urls, p.render_version, p.pack_version, '{POL['model_id']}' AS model_version,
          '{build_id}' AS build_id, current_timestamp() AS queued_at
FROM wt_packs p
LEFT ANTI JOIN (SELECT work_id, input_hash FROM {S}.work_type_scores WHERE model_version = '{POL['model_id']}') s
  ON s.work_id = p.work_id AND s.input_hash = p.input_hash""")
q = one(f"SELECT count(*) AS n, count(DISTINCT work_id) AS ids, count(DISTINCT shard) AS shards FROM {QUEUE}")
if not (q.n == q.ids == n_q):
    raise RuntimeError(f"queue holds {q.n:,} rows / {q.ids:,} ids, expected {n_q:,}")
log(f"queue {QUEUE}: {q.n:,} texts to score in {q.shards} shards; {touched:,} works back on an already-scored text (touched)")
dbutils.jobs.taskValues.set(key="build_id", value=build_id)  # noqa: F821
dbutils.jobs.taskValues.set(key="queued", value=int(q.n))  # noqa: F821
dbutils.jobs.taskValues.set(key="touched", value=touched)  # noqa: F821
