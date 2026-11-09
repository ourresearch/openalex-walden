# Databricks notebook source
# MAGIC %md
# MAGIC # Awards enrich 3/4: keywords on Modal (Jason's app `oxjob1322-keywords-nightly`, student_qwen4b_v3fix), into `award_keywords`
# MAGIC
# MAGIC POSTs `modal_queue_table` + its shard list to the app's `/start` exactly as `notebooks/keywords/nightly/TagKeywordsModal.py`
# MAGIC does, checks every shard file landed, appends the raw output to `<state_prefix>keywords_raw`, then normalises it with the
# MAGIC shipped closed-vocabulary SQL (`utils/keywords_nightly.py`, rendered, not edited). That SQL reads works for four per-mention
# MAGIC fates; for awards it reads a works-shaped view of the queue instead (`openalex.works.openalex_works` and
# MAGIC `openalex.works.works_study_design` are swapped for views, nothing else changes):
# MAGIC - venue_name / boilerplate / templated: no venue (NULL source), so they never fire;
# MAGIC - discipline_lazy: type 'grant', title = award title;
# MAGIC - country_no_evidence: countries = the award's recipient countries (+ the text regex, as for works).
# MAGIC The homonym-sense step (Jev, oxjob #1476) is not run for awards in this version: a split keyword keeps its main sense.
# MAGIC Every queued award gets a row in `target` (an empty list when none survives), carrying the `text_key` it was tagged on.
# MAGIC `dry_run = true` (the default): logs the request and the estimate, reads no secret, calls nothing, writes nothing.

# COMMAND ----------

import datetime as dt
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import modal_call as M  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)
from utils import keywords_nightly as kn  # noqa: E402

for name, default in [("state_prefix", "openalex_dev.rohan_lab.awt_"), ("modal_queue_table", "openalex.transient.awt_queue"),
                      ("target", "openalex_dev.rohan_lab.awt_award_keywords"), ("awards_table", "openalex.awards.awards_api"),
                      ("volume_dir", "/Volumes/openalex/authors/oxjob_tmp/oxjob1322_god/nightly/awards"),
                      ("endpoint", M.APPS["keywords"]["endpoint"]), ("secret_scope", "modal"), ("secret_key", M.APPS["keywords"]["secret_key"]),
                      ("warehouse_id", ""), ("approved_warehouse", ""), ("rules_prefix", "openalex.common.keywords_v2_"), ("synmap", "openalex.common.keywords_v2_synmap"),
                      ("kid0_udf", "openalex.common.keywords_v2_kid0"), ("max_usd", "2"), ("max_minutes", "60"), ("dry_run", "true")]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ("state_prefix", "modal_queue_table", "target", "awards_table", "volume_dir", "endpoint",
                                                  "secret_scope", "secret_key", "warehouse_id", "approved_warehouse", "rules_prefix", "synmap", "kid0_udf")}
P, MQ, TARGET, VOL, RULES = W["state_prefix"], W["modal_queue_table"], W["target"], W["volume_dir"].rstrip("/"), W["rules_prefix"]
MAX_USD, MAX_MIN = float(dbutils.widgets.get("max_usd")), float(dbutils.widgets.get("max_minutes"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW, ROWS = f"{P}queue", f"{P}keywords_raw", f"{P}keywords_rows"
DOCS, NO_SD = "awt_award_docs", "awt_no_study_design"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def one(sql):
    return spark.sql(sql).collect()[0]

def planned():
    """(awards, shards, build_id) for a dry run, from build_queue's task values (its dry run writes no queue). Run by hand
    (no job), they fall back to the queue table if one exists, else 0."""
    tv = lambda k, d: dbutils.jobs.taskValues.get(taskKey="build_queue", key=k, default=d, debugValue=d)
    n = int(tv("queued", -1))
    if n < 0 and spark.catalog.tableExists(QUEUE):
        r = spark.sql(f"SELECT count(*) AS n, count(DISTINCT shard) AS s, max(build_id) AS b FROM {QUEUE}").collect()[0]
        return int(r.n), int(r.s), r.b or "awt-dry"
    return max(n, 0), int(tv("shards", 1)), tv("build_id", "awt-dry")

# COMMAND ----------

if DRY:
    n, n_shards, build_id = planned()
    shards = list(range(n_shards))   # pmod(hash(id), n) shard ids
else:
    q = one(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}")
    n, build_id = int(q.n), q.b
    shards = [int(r.shard) for r in spark.sql(f"SELECT DISTINCT shard FROM {QUEUE} ORDER BY shard").collect()]
if n == 0:
    dbutils.notebook.exit("empty queue: nothing to tag")
est = M.estimate_usd("keywords", n, len(shards))
body = M.start_body("keywords", MQ, f"{VOL}/{build_id}", build_id, W["warehouse_id"], shards)
bad = M.check_request("keywords", body, W["approved_warehouse"])
log(f"build {build_id}: {n:,} awards in {len(shards)} shards; estimate ${est:.2f} (max_usd {MAX_USD}); request {json.dumps(body)[:600]}")
if bad:
    log("the app / our rules would refuse this request: " + "; ".join(bad))
if DRY:
    dbutils.notebook.exit(json.dumps({"dry_run": True, "awards": n, "usd_est": round(est, 2), "request": body, "refusals": bad}))
if bad:
    raise RuntimeError("refusing to call the keywords app: " + "; ".join(bad))
if est > MAX_USD:
    raise RuntimeError(f"estimate ${est:.2f} over max_usd {MAX_USD}; raise it on purpose to tag this queue")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RAW} (id STRING, keywords ARRAY<STRING>, lp ARRAY<DOUBLE>, model STRING, shard BIGINT,
  build_id STRING, tagger_version STRING, tagged_at TIMESTAMP) CLUSTER BY (build_id)
  COMMENT 'Awards enrich: raw keyword-student output per queued award (id W<award id>), append-only, one build per run'""")
if one(f"SELECT count(*) AS n FROM {RAW} WHERE build_id = '{build_id}'").n != n:
    token = dbutils.secrets.get(scope=W["secret_scope"], key=W["secret_key"])
    res = M.ModalCall("keywords", W["endpoint"], token, dbutils.fs, log).run(body, MAX_MIN)
    log(f"Modal done: {res['shards_done']}/{res['shards']} shards, {res['works']:,} awards, {res['shard_seconds']:.0f} shard-s, "
        f"~${res['usd_est']:.2f} (excl. cold starts), wall {res['wall_s']:.0f} s, errors {len(res['errors'])}")
    if res["errors"] or res["shards_done"] != len(shards) or res["works"] != n:
        dbutils.fs.rm(f"{VOL}/{build_id}/_call_id.txt")
        raise RuntimeError(f"incomplete tagging: {json.dumps({k: v for k, v in res.items() if k != 'results'})[:2000]}")
    dbutils.jobs.taskValues.set(key="usd_est", value=float(res["usd_est"]))
    from pyspark.sql import functions as F  # noqa: E402
    raw = (spark.read.schema("id STRING, keywords ARRAY<STRING>, lp ARRAY<DOUBLE>, model STRING, shard BIGINT").json(f"{VOL}/{build_id}/*.jsonl.gz")
           .withColumn("build_id", F.lit(build_id)).withColumn("tagger_version", F.lit(kn.TAGGER_VERSION)).withColumn("tagged_at", F.current_timestamp()))
    raw.createOrReplaceTempView("awt_kw_files")
    c = one(f"""SELECT count(*) AS rows, count(DISTINCT id) AS ids,
        (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN awt_kw_files r ON r.id = concat('W', CAST(q.award_id AS STRING))) AS missing,
        count_if(keywords IS NULL OR lp IS NULL OR size(keywords) <> size(lp)) AS bad FROM awt_kw_files""")
    if not (c.rows == c.ids == n and c.missing == 0 and c.bad == 0):
        raise RuntimeError(f"shard files do not match the queue: {c.asDict()}")
    spark.sql(f"DELETE FROM {RAW} WHERE build_id = '{build_id}'")
    spark.sql(f"INSERT INTO {RAW} SELECT id, keywords, lp, model, shard, build_id, tagger_version, tagged_at FROM awt_kw_files")

# COMMAND ----------

# Normalise: the shipped templates with the two works tables swapped for award views (asserted present, so a template change fails loudly).
spark.sql(f"""CREATE OR REPLACE TEMP VIEW {DOCS} AS
SELECT q.award_id AS id, 'grant' AS type, q.title, q.abstract,
  named_struct('source', named_struct('id', CAST(NULL AS STRING), 'display_name', CAST(NULL AS STRING),
                                      'host_organization_name', CAST(NULL AS STRING))) AS primary_location,
  array(named_struct('countries', coalesce(transform(a.recipient_countries, c -> c.country_code), array()))) AS authorships
FROM {QUEUE} q LEFT JOIN {W['awards_table']} a ON a.id = q.award_id""")
spark.sql(f"CREATE OR REPLACE TEMP VIEW {NO_SD} AS SELECT CAST(NULL AS BIGINT) AS work_id, CAST(NULL AS ARRAY<STRING>) AS study_designs WHERE false")
spark.sql(f"CREATE OR REPLACE TABLE {P}kw_nrm_raw AS SELECT id, keywords, lp FROM {RAW} WHERE build_id = '{build_id}'")
served = f"(SELECT award_id AS work_id FROM {TARGET})" if spark.catalog.tableExists(TARGET) else "(SELECT CAST(NULL AS BIGINT) AS work_id WHERE false)"
values = dict(RAW=f"{P}kw_nrm_raw", STAGE=f"{P}kw_nrm_", ROWS=ROWS, TARGET=served, KID0=W["kid0_udf"],
              RETAG=f"(SELECT award_id AS id FROM {QUEUE})",   # every queued award may replace its served row
              FOLD=f"{RULES}fold", SYNMAP=W["synmap"], PURGE=f"{RULES}purge", COUNTRY_KIDS=f"{RULES}country_kids",
              BOILER=f"{RULES}boiler", TEMPLATED=f"{RULES}templated", VOCAB=kn.VOCAB_FROM_KEYWORDS_V2, TAGGER_VERSION=kn.TAGGER_VERSION,
              COMMENT=f"Awards enrich: keyword rows for build {build_id}")
for step, sql in kn.normalise_statements(values):
    used = "openalex.works.openalex_works" in sql or "openalex.works.works_study_design" in sql
    sql = sql.replace("openalex.works.openalex_works", DOCS).replace("openalex.works.works_study_design", NO_SD)
    assert "openalex.works." not in sql, f"step {step}: an unexpected works table in the template"
    t0 = time.time()
    spark.sql(sql)
    log(f"{step} done in {time.time() - t0:.0f} s" + (" (award views)" if used else ""))

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (award_id BIGINT, keywords ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT>>,
  tagger_version STRING, updated_at TIMESTAMP, text_key BIGINT)
  COMMENT 'Awards enrich: closed-vocabulary keywords per award (shape of openalex_works.keywords); [] = tagged, none kept'""")
c = one(f"""SELECT count(*) AS rows, count(DISTINCT work_id) AS ids,
  (SELECT count(*) FROM {ROWS} r LEFT ANTI JOIN {QUEUE} q ON q.award_id = r.work_id) AS not_in_queue,
  (SELECT count(*) FROM (SELECT explode(keywords) AS k FROM {ROWS}) e LEFT ANTI JOIN openalex.common.keywords_v2 kv
     ON kv.id = e.k.id AND kv.display_name = e.k.display_name) AS not_in_vocab,
  max(typeof(keywords)) AS typ FROM {ROWS}""")
log(f"rows check: {c.asDict()}")
if c.rows != c.ids or c.not_in_queue or c.not_in_vocab or (c.rows and c.typ != kn.SERVED_TYPE):
    raise RuntimeError(f"refusing to merge, NOTHING written to the target: {c.asDict()}")
spark.sql(f"""MERGE INTO {TARGET} v USING (
  SELECT q.award_id, coalesce(r.keywords, CAST(ARRAY() AS ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT>>)) AS keywords,
         '{kn.TAGGER_VERSION}' AS tagger_version, current_timestamp() AS ts, q.text_key
  FROM {QUEUE} q LEFT JOIN {ROWS} r ON r.work_id = q.award_id) s ON v.award_id = s.award_id
WHEN MATCHED THEN UPDATE SET v.keywords = s.keywords, v.tagger_version = s.tagger_version, v.updated_at = s.ts, v.text_key = s.text_key
WHEN NOT MATCHED THEN INSERT (award_id, keywords, tagger_version, updated_at, text_key) VALUES (s.award_id, s.keywords, s.tagger_version, s.ts, s.text_key)""")
e = one(f"""SELECT count(*) AS n, count_if(size(keywords) = 0) AS empty, round(avg(size(keywords)), 2) AS kw
  FROM {TARGET} WHERE award_id IN (SELECT award_id FROM {QUEUE})""")
if e.n != n:
    raise RuntimeError(f"{TARGET} holds {e.n:,} of the {n:,} queued awards after the merge")
log(f"build {build_id}: {n:,} awards in {TARGET}; {e.empty:,} with no keyword kept; {e.kw} keywords per award")
