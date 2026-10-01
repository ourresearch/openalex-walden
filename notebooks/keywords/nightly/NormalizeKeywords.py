# Databricks notebook source
# MAGIC %md
# MAGIC # Keywords nightly 3/4: normalise against the closed vocabulary (oxjob #1322)
# MAGIC
# MAGIC The same SQL that built the shipped per-work keywords (`utils/keywords_nightly.py`, generated from build_vocab.py with the
# MAGIC production flags): raw string -> kid0 UDF -> plural fold -> synonym map -> per-mention fates (malformed, boilerplate, loop,
# MAGIC discipline_lazy, venue_name, purge / dual-use, country evidence) -> templated sources capped at 2 -> only kids already in
# MAGIC `openalex.common.keywords_v2` (nothing is added), position = first mention, score = round(exp(lp), 3), lp >= -1.2.
# MAGIC Reads this build's rows of `<state_prefix>raw`; writes the per-run tables `<state_prefix>nrm_*` and the rows to append,
# MAGIC `<state_prefix>append_rows`, in the exact `work_keywords_v2` shape. Works left with no keyword get no row.

# COMMAND ----------

import datetime as dt
import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "keywords_nightly.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "keywords_nightly.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import keywords_nightly as kn  # noqa: E402

for name, default in [("state_prefix", "openalex.works.work_keywords_v2_"), ("target", "openalex.works.work_keywords_v2"),
                      ("rules_prefix", "openalex.common.keywords_v2_"), ("synmap", "openalex.common.keywords_v2_synmap"),
                      ("kid0_udf", "openalex.common.keywords_v2_kid0"), ("senses", kn.SENSES), ("work_senses", kn.WORK_SENSES), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
RULES = dbutils.widgets.get("rules_prefix").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
QUEUE, RAW, ROWS = f"{P}queue", f"{P}raw", f"{P}append_rows"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

# COMMAND ----------

q = spark.sql(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}").collect()[0]
n, build_id = int(q.n), q.b
if n == 0 or DRY:
    spark.sql(f"DROP TABLE IF EXISTS {ROWS}")   # so the append task can never pick up an older build's rows
    dbutils.notebook.exit("empty queue" if n == 0 else "dry run")
spark.sql(f"CREATE OR REPLACE TABLE {P}nrm_raw AS SELECT id, keywords, lp FROM {RAW} WHERE build_id = '{build_id}'")
m = spark.table(f"{P}nrm_raw").count()
if m != n:
    raise RuntimeError(f"{RAW} holds {m:,} rows for build {build_id}, queue {n:,}: run the tag task first")

values = dict(RAW=f"{P}nrm_raw", STAGE=f"{P}nrm_", ROWS=ROWS, TARGET=TARGET, KID0=dbutils.widgets.get("kid0_udf").strip(),
              FOLD=f"{RULES}fold", SYNMAP=dbutils.widgets.get("synmap").strip(), PURGE=f"{RULES}purge", COUNTRY_KIDS=f"{RULES}country_kids",
              BOILER=f"{RULES}boiler", TEMPLATED=f"{RULES}templated", VOCAB=kn.VOCAB_FROM_KEYWORDS_V2, TAGGER_VERSION=kn.TAGGER_VERSION,
              COMMENT=f"Keywords nightly (oxjob #1322): rows to append for build {build_id}")
for step, sql in kn.normalise_statements(values):
    t0 = time.time()
    spark.sql(sql)
    log(f"{step} done in {time.time() - t0:.0f} s")

# homonym senses (oxjob #1476): a keyword with distinct meanings moves to its sense heading where a per-work decision exists
# ("inflation" on a cosmology paper -> "inflation (cosmology)"). Skipped while the sense tables don't exist.
SENSES, WORK_SENSES = dbutils.widgets.get("senses").strip(), dbutils.widgets.get("work_senses").strip()
if spark.catalog.tableExists(SENSES) and spark.catalog.tableExists(WORK_SENSES) and spark.catalog.tableExists(ROWS):
    t0 = time.time()
    spark.sql(kn.sense_statement(ROWS, f"{P}nrm_sensed", WORK_SENSES, SENSES))
    spark.sql(f"CREATE OR REPLACE TABLE {ROWS} AS SELECT * FROM {P}nrm_sensed")
    log(f"senses done in {time.time() - t0:.0f} s")

# COMMAND ----------

display(spark.sql(f"SELECT fate, count(*) AS mentions, round(count(*) / sum(count(*)) OVER (), 5) AS share FROM {P}nrm_mentions_kept GROUP BY fate ORDER BY mentions DESC"))
r = spark.sql(f"SELECT count(*) AS works, sum(size(keywords)) AS assignments FROM {ROWS}").collect()[0]
log(f"build {build_id}: {r.works:,} of {n:,} tagged works have >= 1 keyword ({r.assignments or 0:,} assignments); {n - r.works:,} end with none")
