# Databricks notebook source
# MAGIC %md
# MAGIC # Affiliation matcher, nightly (oxjob #1386)
# MAGIC
# MAGIC Answers every new affiliation string with the new matcher (oxjobs #1363 / #1385) and appends the answers to
# MAGIC `openalex.institutions.affiliation_matcher_answers`, which `raw_affiliation_strings_institutions_mv` reads before
# MAGIC the legacy model + rules. Code: `utils/affiliation_matcher/` (retrieve + decider vendored from #1363 unchanged;
# MAGIC the chooser as JSON trees, `gbt.py`).
# MAGIC
# MAGIC Per string: retrieve (lex2, ES neighbours, dense chunks, stored TF top 5) -> first-pass chooser -> Jev on the
# MAGIC strings it is unsure about, most unsure first, until `jev_deadline_min` -> Jev chooser for those. First pass is
# MAGIC `decider_mode`: `student` = #1363's frozen decider v1.1 (cross-encoder student p + chooser with ROR relationships,
# MAGIC 89.6% exact on test v2; Jev on 18.6% of strings at b = 0.2 lifts it to 90.4%), or `no_model` = the no-Jev chooser (82.5%; Jev
# MAGIC on 62.9% at b = 0.05 lifts it to ≈ 89.9% on test v1). A string the deadline cuts keeps its first-pass answer.
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
dbutils.widgets.text("max_strings", "60000", "queue cap per run (a backlog drains over nights; the rest keep the legacy answer meanwhile)")
dbutils.widgets.text("max_minutes", "26", "stop starting chunks after this many minutes: End 2 End has ≈ 37 min of slack including cluster start")
dbutils.widgets.text("chunk_strings", "20000", "strings per chunk (one MERGE each)")
dbutils.widgets.text("artifacts", "/Volumes/openalex/works/models/affiliation_matcher/v1", "chooser JSON + name-embedding cache")
dbutils.widgets.text("live_works_counts", "frozen", "with cards=live: frozen = institutions in the frozen snapshot keep its works_count (the chooser's training distribution), new ones take their live count; live = every count live")
dbutils.widgets.text("cards", "frozen", "frozen = the decider's institutions + lineage snapshot (#1363 FROZEN_DECIDER.md); live = rebuild from walden tables")
dbutils.widgets.text("decider_mode", "student", "student = #1363 decider v1 (student p + v1 chooser) first; no_model = the no-Jev chooser first")
dbutils.widgets.text("jev", "true", "false = first-pass chooser only")
dbutils.widgets.text("jev_min_uncertainty", "", "Jev only strings unsure at this margin (#1363 hybrid b); default 0.2 for student, 0.05 for no_model")
dbutils.widgets.text("jev_deadline_min", "12", "stop starting Jev batches after this many minutes of Jev")
dbutils.widgets.text("jev_rps", "250", "Jev requests/s (account cap 400; leaves room for a broker pass from desk)")
dbutils.widgets.text("jev_threads", "96", "Jev threads")
dbutils.widgets.text("max_usd", "25", "stop starting Jev batches past this spend this run")
dbutils.widgets.text("require_swap", "true", "true = do nothing until the answers table holds corpus rows")
dbutils.widgets.text("dry_run", "false", "true = write to <answers_table>_dryrun (overwritten) instead")
dbutils.widgets.text("queue_table", "", "answer the strings in this table (column raw_affiliation_string) instead of the nightly queue; needs target_table")
dbutils.widgets.text("shard", "", "with queue_table: k/n answers only the strings with pmod(xxhash64(string), n) = k (the sweep runs n shards on n clusters)")
dbutils.widgets.text("target_table", "", "with queue_table: where answers go (never the answers table); strings already there are skipped (restart-safe)")
dbutils.widgets.text("vote_ids", "final", "ES neighbour votes: final = each neighbour's institution_ids_final (live answers); legacy = pre-swap ids from vote_ids_table for the strings the swap changed (#1386 charter NOW row 7)")
dbutils.widgets.text("vote_ids_table", "openalex.institutions.oxjob1386_legacy_vote_ids", "with vote_ids=legacy: raw_affiliation_string -> institution_ids voted instead")
dbutils.widgets.text("cards_exclude_table", "", "simulation only (#1393): drop these institutions (column institution_id) from the cards, e.g. to rebuild the world before a ROR dump")
dbutils.widgets.text("sweep_ids_table", "", "with queue_table: new-card sweep (#1393) — institutions being swept (column institution_id); a string none of them reaches in lex2/dense is written with decider 'no_swept_candidate' and not matched further")

ANSWERS = dbutils.widgets.get("answers_table").strip()
SINCE_DAYS = int(dbutils.widgets.get("since_days"))
MAX_STRINGS = int(dbutils.widgets.get("max_strings"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
CHUNK = int(dbutils.widgets.get("chunk_strings"))
ART = dbutils.widgets.get("artifacts").rstrip("/")
CARDS_MODE = dbutils.widgets.get("cards").strip().lower()
VOTE_IDS = dbutils.widgets.get("vote_ids").strip().lower()
VOTE_IDS_TABLE = dbutils.widgets.get("vote_ids_table").strip()
assert VOTE_IDS in ("final", "legacy"), VOTE_IDS
USE_JEV = dbutils.widgets.get("jev").strip().lower() == "true"
DECIDER_MODE = dbutils.widgets.get("decider_mode").strip().lower()
assert DECIDER_MODE in ("student", "no_model"), DECIDER_MODE
JEV_B = float(dbutils.widgets.get("jev_min_uncertainty") or (0.2 if DECIDER_MODE == "student" else 0.05))
JEV_DEADLINE_S = float(dbutils.widgets.get("jev_deadline_min")) * 60
JEV_RPS = float(dbutils.widgets.get("jev_rps"))
JEV_THREADS = int(dbutils.widgets.get("jev_threads"))
MAX_USD = float(dbutils.widgets.get("max_usd"))
REQUIRE_SWAP = dbutils.widgets.get("require_swap").strip().lower() == "true"
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
TARGET = ANSWERS + "_dryrun" if DRY_RUN else ANSWERS
QUEUE_TABLE = dbutils.widgets.get("queue_table").strip()
SWEEP_IDS_TABLE = dbutils.widgets.get("sweep_ids_table").strip()
SHARD = dbutils.widgets.get("shard").strip()
SHARD_SQL = ""
if SHARD:
    _k, _n = (int(x) for x in SHARD.split("/"))
    assert 0 <= _k < _n and QUEUE_TABLE, "shard k/n needs 0 <= k < n and a queue_table"
    SHARD_SQL = f"AND pmod(xxhash64(q.raw_affiliation_string), {_n}) = {_k}"
if QUEUE_TABLE:
    TARGET = dbutils.widgets.get("target_table").strip()
    assert TARGET and TARGET != ANSWERS, "queue_table needs a target_table other than the answers table"
assert not SWEEP_IDS_TABLE or QUEUE_TABLE, "sweep_ids_table needs queue_table"
LOOKUP = "openalex.institutions.affiliation_strings_lookup"

T0 = time.time()
def log(msg):
    print(f"[{time.time() - T0:7.0f}s] {msg}", flush=True)

# COMMAND ----------

has_answers = spark.catalog.tableExists(ANSWERS)
corpus_rows = (spark.sql(f"SELECT count(*) AS n FROM {ANSWERS} WHERE tier IS NULL OR tier <> 'nightly'").collect()[0].n
               if has_answers else 0)
if REQUIRE_SWAP and corpus_rows == 0 and not QUEUE_TABLE:
    dbutils.notebook.exit(f"{ANSWERS} holds no corpus answers yet (swap not loaded); nothing to do")
done_join = (f"LEFT ANTI JOIN {ANSWERS} a ON a.raw_affiliation_string = l.raw_affiliation_string" if has_answers else "")
if DRY_RUN:
    done_join = ""  # a dry run answers the queue as if nothing were answered yet

if QUEUE_TABLE:
    # a given list (test sets, the new-card sweep): every string in it, except those the target already holds
    done_t = (f"LEFT ANTI JOIN {TARGET} t ON t.raw_affiliation_string = q.raw_affiliation_string"
              if spark.catalog.tableExists(TARGET) else "")
    queue = spark.sql(f"""
SELECT q.raw_affiliation_string AS s, to_json(l.model_response) AS mr
FROM (SELECT DISTINCT raw_affiliation_string FROM {QUEUE_TABLE}) q
LEFT JOIN {LOOKUP} l ON l.raw_affiliation_string = q.raw_affiliation_string
{done_t}
WHERE q.raw_affiliation_string IS NOT NULL AND trim(q.raw_affiliation_string) <> '' {SHARD_SQL}
LIMIT {MAX_STRINGS}
""").collect()
else:
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
    # oxjob #1393: ROR relationships go live with the cards, or a new record would get no ROR features
    CARDS, LINEAGE, ROR_REL = f"{WORK}/institutions.jsonl.gz", f"{WORK}/lineage.jsonl.gz", f"{WORK}/ror_rel.jsonl.gz"
    LIVE_COUNTS = dbutils.widgets.get("live_works_counts").strip().lower()
    assert LIVE_COUNTS in ("frozen", "live"), LIVE_COUNTS
    log(f"live cards ({LIVE_COUNTS} works counts): "
        f"{nm.write_cards(spark, CARDS, f'{ART}/institutions.jsonl.gz' if LIVE_COUNTS == 'frozen' else None):,}; lineage rows: {nm.write_lineage(spark, LINEAGE):,}; "
        f"ROR relationship rows: {nm.write_ror_rel(spark, ROR_REL):,}")
else:
    CARDS, LINEAGE, ROR_REL = f"{ART}/institutions.jsonl.gz", f"{ART}/lineage.jsonl.gz", f"{ART}/ror_rel.jsonl.gz"
    log(f"frozen cards: {CARDS} (sha256 {hashlib.sha256(open(CARDS, 'rb').read()).hexdigest()[:12]}…)")
EXCLUDE_TABLE = dbutils.widgets.get("cards_exclude_table").strip()
if EXCLUDE_TABLE:
    assert QUEUE_TABLE, "cards_exclude_table is for simulations on a queue_table"
    excl = {int(r.institution_id) for r in spark.table(EXCLUDE_TABLE).collect()}
    src, CARDS = CARDS, f"{WORK}/institutions_excluded.jsonl.gz"
    with gzip.open(src, "rt") as fi, gzip.open(CARDS, "wt") as fo:
        for line in fi:
            if int(json.loads(line)["id"]) not in excl:
                fo.write(line)
    log(f"cards without the {len(excl):,} institutions in {EXCLUDE_TABLE}")
ix = Index(CARDS)
# decider v1.1 (#1363 FROZEN_DECIDER.md): the student-mode choosers add ROR parent/child/related features
F = Features(ix, LINEAGE, ror_rel=ROR_REL)
JEV_CHOOSER = "chooser_jev_ror" if DECIDER_MODE == "student" else "chooser_jev"
dec_jev = gbt.load_decider(f"{ART}/{JEV_CHOOSER}.json", F)
FIRST = "chooser_me5b_full_ror" if DECIDER_MODE == "student" else "chooser_nojev"
dec_first = gbt.load_decider(f"{ART}/{FIRST}.json", F)
FIRST_NAME = "student" if DECIDER_MODE == "student" else "no_jev"
student = None
if DECIDER_MODE == "student":
    from utils.affiliation_matcher import student as st  # noqa: E402
    student = st.load(f"{ART}/student_me5b_full", base_dir=f"{ART}/base_multilingual-e5-base")
    log(f"student loaded on {student[2]}")
chooser_sha = hashlib.sha256(open(f"{ART}/{JEV_CHOOSER}.json", "rb").read() + open(f"{ART}/{FIRST}.json", "rb").read()).hexdigest()[:10]
MATCHER_VERSION = f"v1/{DECIDER_MODE}/{chooser_sha}/{CARDS_MODE}" + (f"/{time.strftime('%Y-%m-%d')}" if CARDS_MODE == "live" else "") + ("/livecounts" if CARDS_MODE == "live" and dbutils.widgets.get("live_works_counts").strip().lower() == "live" else "") + ("/legacyvotes" if VOTE_IDS == "legacy" else "")
log(f"index: {len(ix.inst):,} institutions, {len(ix.variants):,} name variants; matcher_version {MATCHER_VERSION}")

names = nm.names_for_dense(ix)
import torch  # noqa: E402
if CARDS_MODE == "live":
    # one rolling per-text cache: only names that changed since the last run are embedded (a whole-list key would
    # re-embed 266K names and leave a new ≈ 400 MB file in the Volume on every night a name changes)
    name_emb, n_new = nm.name_embeddings_cached(names, f"{ART}/names_me5b_live_cache.pt" if not EXCLUDE_TABLE
                                                else f"{ART}/names_me5b_live_cache_sim.pt")
    EMB = None
    log(f"dense names: {len(names):,} ({n_new:,} embedded this run)")
else:
    names_key = hashlib.sha256(("intfloat/multilingual-e5-base|query: |128\n" + "\n".join(f"{i}\t{t}" for i, t in names)).encode()).hexdigest()[:16]
    EMB = f"{ART}/names_me5b_{names_key}.pt"
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
if DRY_RUN and not QUEUE_TABLE:
    spark.sql(f"TRUNCATE TABLE {TARGET}")
SWEEP = ({int(r.institution_id) for r in spark.sql(f"SELECT DISTINCT institution_id FROM {SWEEP_IDS_TABLE}").collect()}
         if SWEEP_IDS_TABLE else None)
if SWEEP is not None:
    log(f"sweep: {len(SWEEP):,} institutions; {len(SWEEP - set(ix.inst)):,} of them have no card (withdrawn or not minted)")

# COMMAND ----------

def legacy_ids_of(names):
    """vote_ids=legacy: the pre-swap ids of the listed neighbour strings (runs in the neighbour thread, beside lex2/dense)."""
    from pyspark.sql import functions as F
    t = time.time()
    df = spark.createDataFrame([(n,) for n in names], "raw_affiliation_string STRING")
    rows = spark.table(VOTE_IDS_TABLE).join(F.broadcast(df), "raw_affiliation_string").collect()
    out = {r.raw_affiliation_string: [int(i) for i in (r.institution_ids or [])] for r in rows}
    log(f"  legacy votes: {len(out):,} of {len(names):,} neighbour strings changed at the swap ({time.time() - t:.0f}s)")
    return out


ctx = mp.get_context("fork")
n_proc = max(1, (os.cpu_count() or 2) - 1)
nm._IX = ix  # forked workers share the driver's index (copy-on-write) instead of rebuilding it
lex_pool = ctx.Pool(n_proc)
from concurrent.futures import ThreadPoolExecutor  # noqa: E402
bg = ThreadPoolExecutor(1)
log(f"lexical workers: {n_proc}")

jev_used_s = 0.0
totals = {"strings": 0, "jev": 0, "student": 0, "no_jev": 0, "empty_pool": 0, "es_failed": 0, "no_swept_candidate": 0}

for c0 in range(0, len(strings), CHUNK):
    if (time.time() - T0) / 60 >= MAX_MINUTES:
        log(f"stop: max_minutes {MAX_MINUTES:.0f} reached; {len(strings) - c0:,} strings wait for the next night")
        break
    S, T5 = strings[c0:c0 + CHUNK], top5[c0:c0 + CHUNK]
    # ES neighbours (network-bound threads) run beside lexical (CPU processes) and dense (GPU); in a sweep too (for
    # strings the filter below drops they are wasted, ≈ 17%, but waiting for the filter cost 100-180 s per 10K).
    t = time.time()
    nb_future = bg.submit(nm.neighbour_all, S, ES_URL, 32, legacy_ids_of if VOTE_IDS == "legacy" else None)
    lex = lex_pool.map(nm.lex2, S, chunksize=100)
    log(f"chunk {c0 // CHUNK}: {len(S):,} strings; lex2 {time.time() - t:.0f}s")
    t = time.time()
    dense, name_emb_now = nm.dense_chunks_all(S, names, name_emb=name_emb)
    if name_emb is None:
        name_emb = name_emb_now
        torch.save(name_emb, EMB)
    log(f"  dense {time.time() - t:.0f}s")
    skipped = []
    if SWEEP is not None:
        # new-card sweep (#1393): a string whose lex2 top 10 and dense top 10 hold no swept institution has the same
        # pool as before, so its answer can't change because of the new cards: record it and move on
        keep = [k for k in range(len(S)) if SWEEP & set(nm.pool({"lex2": lex[k], "dense_me5b_chunks": dense[k]}))]
        kept = set(keep)
        skipped = [S[k] for k in range(len(S)) if k not in kept]
        S, T5, lex, dense = [S[k] for k in keep], [T5[k] for k in keep], [lex[k] for k in keep], [dense[k] for k in keep]
        log(f"  sweep filter: {len(S):,} strings reach a swept institution, {len(skipped):,} don't")
    t = time.time()
    nb = nb_future.result()
    if SWEEP is not None:
        nb = [nb[k] for k in keep]
    totals["es_failed"] += sum(x is None for x in nb)
    log(f"  neighbour: waited {time.time() - t:.0f}s more ({sum(x is None for x in nb)} ES failures)")

    ranks = [{"lex2": lex[k], "neighbour": nb[k] or [], "dense_me5b_chunks": dense[k], "top5": T5[k]} for k in range(len(S))]
    cands = [nm.candidates(ix, r) for r in ranks]

    # First-pass chooser on everything (student p + v1 chooser, or the no-model chooser); its uncertainty orders
    # the Jev queue.
    if student is not None and S:
        t = time.time()
        sp = st.p_for(student, ix, [(k, S[k], i) for k in range(len(S)) for i in cands[k]], workers=8)
        log(f"  student: {len(sp):,} pairs in {time.time() - t:.0f}s")
    out = {k: ([], {}, "empty_pool") for k in range(len(S)) if not cands[k]}
    todo = [k for k in range(len(S)) if cands[k]]
    t = time.time()
    first = nm.decide_many(dec_first, [(S[k], {i: sp[(k, i)] for i in cands[k]} if student is not None
                                              else {i: 1.0 for i in cands[k]}, ranks[k]) for k in todo])
    for k, (ids, probs) in zip(todo, first):
        out[k] = (ids, probs, FIRST_NAME)
    log(f"  first-pass chooser {time.time() - t:.0f}s")

    if jev_client is not None:
        unsure = sorted((k for k in out if out[k][2] == FIRST_NAME and nm.uncertainty(out[k][1]) > JEV_B),
                        key=lambda k: -nm.uncertainty(out[k][1]))
        t = time.time()
        done_k = 0
        for b0 in range(0, len(unsure), 1000):
            if jev_used_s + (time.time() - t) >= JEV_DEADLINE_S or jev_client.usd >= MAX_USD:
                break
            batch = [(k, S[k], cands[k]) for k in unsure[b0:b0 + 1000]]
            got = nm.jev_strings(jev_client, ix, batch, threads=JEV_THREADS)
            keys = list(got)
            for k, (ids, probs) in zip(keys, nm.decide_many(dec_jev, [(S[k], got[k], ranks[k]) for k in keys])):
                out[k] = (ids, probs, "jev")
            done_k += len(got)
        jev_used_s += time.time() - t
        log(f"  jev: {done_k:,} of {len(unsure):,} unsure strings ({len(S):,} in chunk) in {time.time() - t:.0f}s; "
            f"${jev_client.usd:.2f} this run; retries {jev_client.n_retry}, failures {jev_client.n_fail}")

    rows = [(S[k], [int(i) for i in sorted(out[k][0])], [], {int(i): float(p) for i, p in out[k][1].items()},
             out[k][2], "sweep" if SWEEP is not None else "nightly", MATCHER_VERSION) for k in range(len(S))]
    rows += [(x, [], [], {}, "no_swept_candidate", "sweep", MATCHER_VERSION) for x in skipped]
    for k in range(len(S)):
        totals[out[k][2]] += 1
    totals["no_swept_candidate"] += len(skipped)
    totals["strings"] += len(S) + len(skipped)
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
