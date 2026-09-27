# Databricks notebook source
# MAGIC %md
# MAGIC # Affiliation matcher, nightly (oxjob #1386)
# MAGIC
# MAGIC Answers every new affiliation string with the new matcher (oxjobs #1363 / #1385) and appends the answers to
# MAGIC `openalex.institutions.affiliation_matcher_answers`, which `raw_affiliation_strings_institutions_mv` reads before
# MAGIC the legacy model + rules. Code: `utils/affiliation_matcher/` (retrieve + decider vendored from #1363 unchanged;
# MAGIC the chooser as JSON trees, `gbt.py`).
# MAGIC
# MAGIC Per string: retrieve (lex2, ES neighbours, dense chunks, stored TF top 5) -> no-Jev chooser -> Jev on the strings
# MAGIC it is unsure about, most unsure first, until `jev_deadline_min` -> Jev chooser for those. A string the deadline
# MAGIC cuts keeps its no-Jev answer (`decider = 'no_jev'`), still well above the legacy model.
# MAGIC
# MAGIC Queue: lookup strings created in the last `since_days` days with no answers row. Does nothing until the corpus
# MAGIC answers are loaded (`scripts/affiliation_matcher_swap.py load`), so it can ship before the swap.
# MAGIC
# MAGIC Restart-safe: works in chunks of `chunk_strings`, each MERGEd (insert-only) before the next starts.

# COMMAND ----------

import gzip
import hashlib
import json
import multiprocessing as mp
import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "affiliation_matcher", "nightly.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "nightly.py" in filenames and dirpath.endswith(os.path.join("utils", "affiliation_matcher")):
                REPO_ROOT = os.path.dirname(os.path.dirname(dirpath))
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402  (JevClient: paced, retrying, pinned jev-1.13.0)
from utils.affiliation_matcher import gbt  # noqa: E402
from utils.affiliation_matcher import nightly as nm  # noqa: E402
from utils.affiliation_matcher.decider import Features  # noqa: E402
from utils.affiliation_matcher.retrieve import Index  # noqa: E402

dbutils.widgets.text("answers_table", "openalex.institutions.affiliation_matcher_answers", "where answers go")
dbutils.widgets.text("since_days", "14", "queue: lookup strings created in the last N days with no answer")
dbutils.widgets.text("max_strings", "150000", "queue cap per run")
dbutils.widgets.text("chunk_strings", "20000", "strings per chunk (one MERGE each)")
dbutils.widgets.text("artifacts", "/Volumes/openalex/works/models/affiliation_matcher/v1", "chooser JSON + name-embedding cache")
dbutils.widgets.text("cards", "frozen", "frozen = the decider's institutions + lineage snapshot (#1363 FROZEN_DECIDER.md); live = rebuild from walden tables")
dbutils.widgets.text("jev", "true", "false = no-Jev chooser only")
dbutils.widgets.text("jev_min_uncertainty", "0.05", "Jev only strings unsure at this margin (#1363 hybrid b)")
dbutils.widgets.text("jev_deadline_min", "22", "stop starting Jev batches after this many minutes of Jev")
dbutils.widgets.text("jev_rps", "330", "Jev requests/s (account cap 400)")
dbutils.widgets.text("jev_threads", "96", "Jev threads")
dbutils.widgets.text("max_usd", "25", "stop starting Jev batches past this spend this run")
dbutils.widgets.text("require_swap", "true", "true = do nothing until the answers table holds corpus rows")
dbutils.widgets.text("dry_run", "false", "true = write to <answers_table>_dryrun (overwritten) instead")

ANSWERS = dbutils.widgets.get("answers_table").strip()
SINCE_DAYS = int(dbutils.widgets.get("since_days"))
MAX_STRINGS = int(dbutils.widgets.get("max_strings"))
CHUNK = int(dbutils.widgets.get("chunk_strings"))
ART = dbutils.widgets.get("artifacts").rstrip("/")
CARDS_MODE = dbutils.widgets.get("cards").strip().lower()
USE_JEV = dbutils.widgets.get("jev").strip().lower() == "true"
JEV_B = float(dbutils.widgets.get("jev_min_uncertainty"))
JEV_DEADLINE_S = float(dbutils.widgets.get("jev_deadline_min")) * 60
JEV_RPS = float(dbutils.widgets.get("jev_rps"))
JEV_THREADS = int(dbutils.widgets.get("jev_threads"))
MAX_USD = float(dbutils.widgets.get("max_usd"))
REQUIRE_SWAP = dbutils.widgets.get("require_swap").strip().lower() == "true"
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
TARGET = ANSWERS + "_dryrun" if DRY_RUN else ANSWERS
LOOKUP = "openalex.institutions.affiliation_strings_lookup"

T0 = time.time()
def log(msg):
    print(f"[{time.time() - T0:7.0f}s] {msg}", flush=True)

# COMMAND ----------

has_answers = spark.catalog.tableExists(ANSWERS)
corpus_rows = (spark.sql(f"SELECT count(*) AS n FROM {ANSWERS} WHERE tier IS NULL OR tier <> 'nightly'").collect()[0].n
               if has_answers else 0)
if REQUIRE_SWAP and corpus_rows == 0:
    dbutils.notebook.exit(f"{ANSWERS} holds no corpus answers yet (swap not loaded); nothing to do")
done_join = (f"LEFT ANTI JOIN {ANSWERS} a ON a.raw_affiliation_string = l.raw_affiliation_string" if has_answers else "")
if DRY_RUN:
    done_join = ""  # a dry run answers the queue as if nothing were answered yet

queue = spark.sql(f"""
SELECT l.raw_affiliation_string AS s, to_json(l.model_response) AS mr
FROM {LOOKUP} l
{done_join}
WHERE l.created_datetime >= current_timestamp() - INTERVAL {SINCE_DAYS} DAYS
  AND l.raw_affiliation_string IS NOT NULL AND trim(l.raw_affiliation_string) <> ''
ORDER BY l.created_datetime
LIMIT {MAX_STRINGS}
""").collect()
strings = [r.s for r in queue]
top5 = [nm.top5_from_model_response(r.mr) for r in queue]
log(f"queue: {len(strings):,} strings (corpus answers: {corpus_rows:,}); target {TARGET}")
if not strings:
    dbutils.notebook.exit("queue empty")

# COMMAND ----------

# Cards + lineage: by default the frozen snapshot the decider was scored and the corpus was run with (same features,
# same candidates dropped). `live` rebuilds them from walden tables so a new ROR institution is a candidate the next
# night; switch only together with a re-scored chooser (after ship: the new-card sweep).
WORK = "/local_disk0/tmp/affiliation_matcher"
os.makedirs(WORK, exist_ok=True)
if CARDS_MODE == "live":
    CARDS, LINEAGE = f"{WORK}/institutions.jsonl.gz", f"{WORK}/lineage.jsonl.gz"
    log(f"live cards: {nm.write_cards(spark, CARDS):,}; lineage rows: {nm.write_lineage(spark, LINEAGE):,}")
else:
    CARDS, LINEAGE = f"{ART}/institutions.jsonl.gz", f"{ART}/lineage.jsonl.gz"
    log(f"frozen cards: {CARDS} (sha256 {hashlib.sha256(open(CARDS, 'rb').read()).hexdigest()[:12]}…)")
ix = Index(CARDS)
F = Features(ix, LINEAGE)
dec_jev = gbt.load_decider(f"{ART}/chooser_jev.json", F)
dec_nojev = gbt.load_decider(f"{ART}/chooser_nojev.json", F)
chooser_sha = hashlib.sha256(open(f"{ART}/chooser_jev.json", "rb").read() + open(f"{ART}/chooser_nojev.json", "rb").read()).hexdigest()[:10]
MATCHER_VERSION = f"v1/{chooser_sha}/{CARDS_MODE}"
log(f"index: {len(ix.inst):,} institutions, {len(ix.variants):,} name variants; matcher_version {MATCHER_VERSION}")

names = nm.names_for_dense(ix)
names_key = hashlib.sha256(("intfloat/multilingual-e5-base|query: |128\n" + "\n".join(f"{i}\t{t}" for i, t in names)).encode()).hexdigest()[:16]
EMB = f"{ART}/names_me5b_{names_key}.pt"
import torch  # noqa: E402
name_emb = torch.load(EMB) if os.path.exists(EMB) else None
log(f"dense names: {len(names):,} ({'cached' if name_emb is not None else 'embedding this run'})")

ES_URL = dbutils.secrets.get(scope="elastic", key="elastic_url")
jev_client = sd.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=JEV_THREADS, rps=JEV_RPS) if USE_JEV else None

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  raw_affiliation_string STRING NOT NULL, institution_ids ARRAY<BIGINT> NOT NULL, countries ARRAY<STRING>,
  scores MAP<BIGINT, DOUBLE>, decider STRING, tier STRING, matcher_version STRING, run_at TIMESTAMP
) CLUSTER BY (raw_affiliation_string)
""")
if DRY_RUN:
    spark.sql(f"TRUNCATE TABLE {TARGET}")

# COMMAND ----------

ctx = mp.get_context("fork")
n_proc = max(1, (os.cpu_count() or 2) - 1)
nm._IX = ix  # forked workers share the driver's index (copy-on-write) instead of rebuilding it
lex_pool = ctx.Pool(n_proc)
log(f"lexical workers: {n_proc}")

jev_used_s = 0.0
totals = {"strings": 0, "jev": 0, "no_jev": 0, "empty_pool": 0, "es_failed": 0}

for c0 in range(0, len(strings), CHUNK):
    S, T5 = strings[c0:c0 + CHUNK], top5[c0:c0 + CHUNK]
    t = time.time()
    lex = lex_pool.map(nm.lex2, S, chunksize=100)
    log(f"chunk {c0 // CHUNK}: {len(S):,} strings; lex2 {time.time() - t:.0f}s")
    t = time.time()
    nb = nm.neighbour_all(S, ES_URL, threads=32)
    totals["es_failed"] += sum(x is None for x in nb)
    log(f"  neighbour {time.time() - t:.0f}s ({sum(x is None for x in nb)} ES failures)")
    t = time.time()
    dense, name_emb_now = nm.dense_chunks_all(S, names, name_emb=name_emb)
    if name_emb is None:
        name_emb = name_emb_now
        torch.save(name_emb, EMB)
    log(f"  dense {time.time() - t:.0f}s")

    ranks = [{"lex2": lex[k], "neighbour": nb[k] or [], "dense_me5b_chunks": dense[k], "top5": T5[k]} for k in range(len(S))]
    cands = [nm.candidates(ix, r) for r in ranks]

    # No-Jev chooser on everything; uncertainty orders the Jev queue.
    out = {}
    for k, s in enumerate(S):
        if not cands[k]:
            out[k] = ([], {}, "empty_pool")
            continue
        ids, probs = dec_nojev.decide(s, {i: 1.0 for i in cands[k]}, ranks[k])
        out[k] = (ids, probs, "no_jev")

    if jev_client is not None:
        unsure = sorted((k for k in out if out[k][2] == "no_jev" and nm.uncertainty(out[k][1]) > JEV_B),
                        key=lambda k: -nm.uncertainty(out[k][1]))
        t = time.time()
        done_k = 0
        for b0 in range(0, len(unsure), 1000):
            if jev_used_s + (time.time() - t) >= JEV_DEADLINE_S or jev_client.usd >= MAX_USD:
                break
            batch = [(k, S[k], cands[k]) for k in unsure[b0:b0 + 1000]]
            got = nm.jev_strings(jev_client, ix, batch, threads=JEV_THREADS)
            for k, js in got.items():
                ids, probs = dec_jev.decide(S[k], js, ranks[k])
                out[k] = (ids, probs, "jev")
            done_k += len(got)
        jev_used_s += time.time() - t
        log(f"  jev: {done_k:,} of {len(unsure):,} unsure strings ({len(S):,} in chunk) in {time.time() - t:.0f}s; "
            f"${jev_client.usd:.2f} this run; retries {jev_client.n_retry}, failures {jev_client.n_fail}")

    rows = [(S[k], [int(i) for i in sorted(out[k][0])], [], {int(i): float(p) for i, p in out[k][1].items()},
             out[k][2], "nightly", MATCHER_VERSION) for k in range(len(S))]
    for k in range(len(S)):
        totals[out[k][2]] += 1
    totals["strings"] += len(S)
    df = spark.createDataFrame(rows, "raw_affiliation_string STRING, institution_ids ARRAY<BIGINT>, countries ARRAY<STRING>, "
                                     "scores MAP<BIGINT, DOUBLE>, decider STRING, tier STRING, matcher_version STRING")
    df.createOrReplaceTempView("am_chunk")
    # Insert-only: one row per string, ever (the MV refresh fails on duplicates).
    spark.sql(f"""
MERGE INTO {TARGET} t
USING (SELECT *, current_timestamp() AS run_at FROM am_chunk) s
ON t.raw_affiliation_string = s.raw_affiliation_string
WHEN NOT MATCHED THEN INSERT *
""")
    log(f"  wrote chunk; totals {totals}")

lex_pool.close()
log(f"done: {totals}; Jev ${jev_client.usd if jev_client else 0:.2f}")
