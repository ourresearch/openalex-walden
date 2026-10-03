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
                      ("kid0_udf", "openalex.common.keywords_v2_kid0"), ("senses", kn.SENSES), ("work_senses", kn.WORK_SENSES), ("sense_max_pairs", "500000"),
                      ("jev_threads", "64"), ("jev_rps", "150"), ("dry_run", "false")]:
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

# homonym senses (oxjob #1476): a keyword with distinct meanings moves to its sense heading where a per-work decision says so
# ("inflation" on a cosmology paper -> "inflation (cosmology)"). First Jev classifies tonight's (work, split keyword) pairs that have no
# decision yet (same question as the validated corpus run), then the rows are rewritten. Skipped while the sense tables don't exist.
SENSES, WORK_SENSES = dbutils.widgets.get("senses").strip(), dbutils.widgets.get("work_senses").strip()
if spark.catalog.tableExists(SENSES) and spark.catalog.tableExists(WORK_SENSES) and spark.catalog.tableExists(ROWS):
    import concurrent.futures as cf
    from utils import study_design as sd  # noqa: E402  (JevClient: paced, retrying, pinned jev-1.13.0)
    t0 = time.time()
    spark.sql(kn.sense_pairs_statement(ROWS, QUEUE, f"{P}nrm_sense_pairs", WORK_SENSES, SENSES))
    pairs = spark.table(f"{P}nrm_sense_pairs").collect()
    cap = int(dbutils.widgets.get("sense_max_pairs"))
    if len(pairs) > cap:
        log(f"senses: {len(pairs):,} undecided pairs > cap {cap:,}; classifying the first {cap:,}, the rest keep the main sense")
        pairs = pairs[:cap]
    opts = {}
    for r in spark.table(SENSES).orderBy("heading", "opt").collect():
        opts.setdefault(r.heading, []).append(r)
    for h in opts:
        opts[h].sort(key=lambda r: int(r.opt[1:]))
    client = sd.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=int(dbutils.widgets.get("jev_threads")),
                          rps=float(dbutils.widgets.get("jev_rps")))

    def classify(r):
        o = opts[r.heading]
        res = client.decide(kn.sense_state(r.heading, r.title, r.abstract, r.venue), kn.sense_question([x.label for x in o]))
        if not res["ok"]:
            return None
        opt, prob = kn.sense_decision(res["answers"]["sense"]["probabilities"])
        moved = opt not in ("s0", "none") and prob >= kn.SENSE_T
        sid = next((x.sense_id for x in o if x.opt == opt), None) if moved else None
        return (int(r.work_id), r.heading, opt, prob, sid, res.get("model"))

    with cf.ThreadPoolExecutor(int(dbutils.widgets.get("jev_threads"))) as ex:
        dec = [d for d in ex.map(classify, pairs) if d is not None]
    if dec:
        spark.createDataFrame(dec, "work_id bigint, heading string, opt string, prob double, sense_id string, model string") \
             .createOrReplaceTempView("new_sense_decisions")
        spark.sql(f"""INSERT INTO {WORK_SENSES} (work_id, heading, opt, prob, sense_id, model, decided_at)
                      SELECT work_id, heading, opt, prob, sense_id, model, current_timestamp() FROM new_sense_decisions""")
    log(f"senses: {len(dec):,} of {len(pairs):,} pairs classified ({len(pairs) - len(dec):,} failed, keep the main sense), "
        f"{sum(d[4] is not None for d in dec):,} move; Jev ${client.usd:.2f}; {time.time() - t0:.0f} s")
    if client.out_of_credit:  # the failed pairs stay undecided and are re-asked next night (oxjob #1523)
        log(f"senses: !!! JEV OUT OF CREDIT ({client.out_of_credit}); undecided pairs retry next night. "
            "Top up at console.typesafe.ai.")
    t0 = time.time()
    spark.sql(kn.sense_statement(ROWS, f"{P}nrm_sensed", WORK_SENSES, SENSES))
    spark.sql(f"CREATE OR REPLACE TABLE {ROWS} AS SELECT * FROM {P}nrm_sensed")
    log(f"senses done in {time.time() - t0:.0f} s")

# COMMAND ----------

display(spark.sql(f"SELECT fate, count(*) AS mentions, round(count(*) / sum(count(*)) OVER (), 5) AS share FROM {P}nrm_mentions_kept GROUP BY fate ORDER BY mentions DESC"))
r = spark.sql(f"SELECT count(*) AS works, sum(size(keywords)) AS assignments FROM {ROWS}").collect()[0]
log(f"build {build_id}: {r.works:,} of {n:,} tagged works have >= 1 keyword ({r.assignments or 0:,} assignments); {n - r.works:,} end with none")
