"""#1348 production decision job (2026-09-25): one night's vector decisions on a Databricks CPU node, vectorized per block, with the Jev zone
call inside (decision 6). Inputs (built by shadow_night_sql.py on the warehouse): oxjob1348_shadow_night_<d8> (the night's seats + 256-d
vectors) and oxjob1348_shadow_pool_<d8> (CAP most-recent stored seats of every profile in the touched non-mega blocks). Per block: decode
the pool once, per-profile centroid + per-seat x per-profile best cosine (sorted-group reduceat), first-name mask, masked top-10 profiles,
hub-pref within HUB of the best (most pool seats), band(tier) + cen >= CEN -> 'band'; else 'zone' -> Jev Choice over the top-3 (cards from
oxjob1342_all_comp / the daily table), auto at p >= JEV_BAR -> 'jev'; else 'zone_unresolved' (mint). Mega -> 'mega_cascade'; no pool -> 'mint'.
Output: openalex.authors.oxjob1348_nightly_decisions (run_date-partitioned): work_id, author_sequence, block_key, tier, rule, vec_author_id
(the assignment when rule in band/jev), band_pid, band_best, band_cen, band_n, jev_pid, jev_p, jev_choice, top3, n_prof, n_masked,
name_author_id, orcid_author_id, match_outcome. Rules final 2026-09-25: small 0.93, medium 0.95, hub 0.03, cen 0.85, Jev >= 0.9.
    argv: <home> --date 2026-09-25 [--band-small 0.93 --band-medium 0.95 --cen 0.85 --hub 0.03 --jev-bar 0.9 --jev-limit 0 --jev-concurrency 48 --skip-jev]"""
import json, os, re, sys, time, unicodedata, glob, asyncio, numpy as np
from collections import defaultdict
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
HOME = sys.argv[1]
try: _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError: _HERE = HOME  # spark_python_task runs the file without __file__ (run 107981729893761)
sys.path.insert(0, HOME); sys.path.insert(0, _HERE)  # jevx.py beside this file (walden) or on the Volume (tests)
def arg(k, d): return next((sys.argv[i + 1] for i, x in enumerate(sys.argv) if x == k), d)
DATE = arg("--date", None); D8 = DATE.replace("-", ""); BAND = {"small": float(arg("--band-small", 0.93)), "medium": float(arg("--band-medium", 0.95))}
CEN = float(arg("--cen", 0.85)); HUB = float(arg("--hub", 0.03)); JEV_BAR = float(arg("--jev-bar", 0.9)); JEV_LIMIT = int(arg("--jev-limit", 0)); JEV_CONC = int(arg("--jev-concurrency", 48))
K = 10; DIM = 256
A = "openalex.authors"; NT = arg("--night-table", f"{A}.oxjob1348_shadow_night_{D8}"); PT = arg("--pool-table", f"{A}.oxjob1348_shadow_pool_{D8}"); OUT = arg("--out-table", f"{A}.oxjob1348_nightly_decisions"); DAILY = arg("--cards-table", f"{A}.oxjob1348_daily_{D8}")
NPROC = int(arg("--procs", 0)) or max(1, (os.cpu_count() or 2) - 2)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, LongType, DoubleType
spark = SparkSession.builder.getOrCreate()
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)
def fold(x): return unicodedata.normalize("NFKD", x or "").encode("ascii", "ignore").decode().lower()
def first_token(name):
    n = fold(name); n = n.split(",", 1)[1] if "," in n else (" ".join(n.split()[:-1]) if len(n.split()) > 1 else "")
    t = re.findall(r"[a-z]+", n); return t[0] if t else ""
def norm_first(x):
    t = re.findall(r"[a-z]+", fold(x)); return t[0] if t else ""
def compatible(x, y):
    if not x or not y: return True
    if len(x) == 1 or len(y) == 1: return x[0] == y[0]
    return x == y or x.startswith(y) or y.startswith(x)
def dump(name, tbl, nparts):
    d = f"{HOME}/nightly/{D8}/{name}"
    if os.path.exists(f"{d}/_SUCCESS"): log("skip (exists):", d); return d
    t0 = time.time(); spark.table(tbl).repartition(nparts, "block_key").sortWithinPartitions("block_key").write.mode("overwrite").parquet(d); log(f"dumped {name} ({time.time()-t0:.0f}s)"); return d
def bin_matrix(col, scale):
    """binary column of 256 int8 bytes per row -> unit-norm float32 (n, 256), decoded from the contiguous data buffer, no per-row Python"""
    arr = col.combine_chunks(); n = len(arr)
    if n == 0: return np.zeros((0, DIM), dtype=np.float32)
    otype = np.int64 if pa.types.is_large_binary(arr.type) else np.int32  # offsets buffer (BinaryArray has no .offsets in this pyarrow)
    off0 = int(np.frombuffer(arr.buffers()[1], dtype=otype)[arr.offset])
    raw = np.frombuffer(arr.buffers()[2], dtype=np.int8)[off0:off0 + n * DIM].reshape(n, DIM).astype(np.float32)
    X = raw * np.asarray(scale, dtype=np.float32)[:, None]; return X / np.linalg.norm(X, axis=1, keepdims=True).clip(1e-6)
def segments(keys):
    """sorted string column -> list of (key, lo, hi)"""
    codes = pc.dictionary_encode(keys.combine_chunks()); idx = codes.indices.to_numpy(zero_copy_only=False); vals = codes.dictionary.to_pylist()
    cuts = np.flatnonzero(np.diff(idx)) + 1; los = np.concatenate([[0], cuts]); his = np.concatenate([cuts, [len(idx)]])
    return [(vals[idx[lo]], int(lo), int(hi)) for lo, hi in zip(los, his)]
# ---- night seats by block -------------------------------------------------------------------------------------------------------------
nd = dump("night", NT, 16); pd_ = dump("pool", PT, 128); t0 = time.time(); night = defaultdict(list); nseats = 0
NCOLS = ["work_id", "author_sequence", "block_key", "tier", "raw_name", "pn_first", "match_outcome", "name_author_id", "orcid_author_id", "existing_author_id"]
for fn in sorted(glob.glob(f"{nd}/*.parquet")):
    T = pq.read_table(fn, columns=NCOLS + ["ft_q", "ft_s"]); V = bin_matrix(T.column("ft_q"), T.column("ft_s").to_numpy()); D = {c: T.column(c).to_pylist() for c in NCOLS}
    for i in range(T.num_rows):
        r = {c: D[c][i] for c in NCOLS}; r["v"] = V[i]; r["qf"] = norm_first(r["pn_first"] or "") or first_token(r["raw_name"]); night[r["block_key"]].append(r); nseats += 1
log(f"night: {nseats} seats in {len(night)} blocks ({time.time()-t0:.0f}s)")
# do-not-reassign (#1370, requested 2026-09-25): a seat Unmix removed from a profile must never go back to it. Candidates listed for the seat in
# openalex.authors.profile_move_dnr are dropped before ranking. Tolerant: the table does not exist until #1370 creates it.
DNR_T = arg("--dnr-table", f"{A}.profile_move_dnr"); dnr = defaultdict(set)
if spark.catalog.tableExists(DNR_T):
    spark.table(NT).select("work_id", "author_sequence").createOrReplaceTempView("night_keys")
    for r in spark.sql(f"SELECT d.work_id, d.author_sequence, d.author_id FROM {DNR_T} d JOIN night_keys n USING (work_id, author_sequence)").collect():
        dnr[(int(r[0]), int(r[1]))].add(int(r[2]))
    log(f"dnr: {sum(len(v) for v in dnr.values())} (seat, profile) exclusions for {len(dnr)} tonight's seats")
else: log(f"dnr: {DNR_T} does not exist yet, no exclusions")
# ---- decide ------------------------------------------------------------------------------------------------------------------------------
out_rows = []; stats = defaultdict(int); nseen = set()
def emit(r, rule, pid=None, band=None, top=None, n_prof=0, n_masked=0):
    stats[(r["tier"], rule)] += 1
    out_rows.append({"run_date": DATE, "work_id": r["work_id"], "author_sequence": r["author_sequence"], "block_key": r["block_key"], "tier": r["tier"], "rule": rule, "vec_author_id": pid,
                     "band_pid": band["pid"] if band else None, "band_best": band["best"] if band else None, "band_cen": band["cen"] if band else None, "band_n": band["n"] if band else None,
                     "jev_pid": None, "jev_p": None, "jev_choice": None, "top3": top or [], "n_prof": n_prof, "n_masked": n_masked,
                     "name_author_id": r["name_author_id"], "orcid_author_id": r["orcid_author_id"], "match_outcome": r["match_outcome"]})
def decide_block(bk, P, profs, pfirst, pwc, wids, seqs):
    seats = night[bk]; uniq, first_idx, pidx = np.unique(profs, return_index=True, return_inverse=True); u = len(uniq)
    order = np.argsort(pidx, kind="stable"); starts = np.flatnonzero(np.diff(np.concatenate([[-1], pidx[order]]))); n_p = np.bincount(pidx, minlength=u)
    Cen = np.zeros((u, DIM), dtype=np.float32); np.add.at(Cen, pidx, P); Cen /= np.linalg.norm(Cen, axis=1, keepdims=True).clip(1e-9)
    pf = [norm_first(pfirst[i] or "") for i in first_idx]; wc = [pwc[i] for i in first_idx]
    Q = np.stack([s["v"] for s in seats]); C = Q @ P.T; B = np.maximum.reduceat(C[:, order], starts, axis=1); CC = Q @ Cen.T  # (m,u)
    compat_cache = {}
    for i, r in enumerate(seats):
        qf = r["qf"]
        if qf not in compat_cache: compat_cache[qf] = np.fromiter((compatible(qf, p) for p in pf), dtype=bool, count=u)
        mask = compat_cache[qf]
        ex = dnr.get((int(r["work_id"]), int(r["author_sequence"])))
        if ex: mask = mask & ~np.isin(uniq, np.fromiter(ex, dtype=np.int64))
        n_masked = int(mask.sum())
        if r["tier"] == "mega": emit(r, "mega_cascade", n_prof=u, n_masked=n_masked); continue
        if n_masked == 0: emit(r, "mint", n_prof=u, n_masked=0); continue
        b = np.where(mask, B[i], -2.0); kk = min(K, n_masked); top_idx = np.argpartition(-b, kk - 1)[:kk]; top_idx = top_idx[np.argsort(-b[top_idx])]
        cand = []
        for j in top_idx:
            j = int(j); rows = order[starts[j]:(starts[j + 1] if j + 1 < u else len(order))]; kb = rows[int(np.argmax(C[i, rows]))]
            cand.append({"pid": int(uniq[j]), "best": float(B[i, j]), "cen": float(CC[i, j]), "n": int(n_p[j]), "wc": wc[j], "best_seat": [int(wids[kb]), int(seqs[kb])]})
        c0 = cand[0]; near = [c for c in cand if c["best"] >= c0["best"] - HUB]; c0 = max(near, key=lambda c: (c["n"], c["best"]))
        if c0["best"] >= BAND.get(r["tier"], 9) and c0["cen"] >= CEN: emit(r, "band", c0["pid"], c0, cand[:3], u, n_masked)
        else: emit(r, "zone", None, c0, cand[:3], u, n_masked)
PCOLS = ["block_key", "author_id", "prof_first", "works_count", "work_id", "author_sequence"]
def work_file(args):
    """one pool parquet file -> jsonl part + (blocks seen, stats); runs in a forked worker (night dict shared copy-on-write)"""
    i, fn = args; out_rows.clear(); stats.clear(); seen = set(); nb = 0
    T = pq.read_table(fn, columns=PCOLS + ["ft_q", "ft_s"]); segs = [s for s in segments(T.column("block_key")) if s[0] in night]
    if segs:
        P_all = bin_matrix(T.column("ft_q"), T.column("ft_s").to_numpy()); profs = T.column("author_id").to_numpy(); wids = T.column("work_id").to_numpy(); seqs = T.column("author_sequence").to_numpy()
        pfirst = T.column("prof_first").to_pylist(); pwc = T.column("works_count").to_pylist()
        for bk, lo, hi in segs:
            decide_block(bk, P_all[lo:hi], profs[lo:hi], pfirst[lo:hi], pwc[lo:hi], wids[lo:hi], seqs[lo:hi]); seen.add(bk); nb += 1
    part = f"/tmp/nightly_{D8}_part{i:03d}.jsonl"
    with open(part, "w") as f:
        for r in out_rows: f.write(json.dumps(r) + "\n")
    return part, seen, dict(stats), nb, len(out_rows)
t0 = time.time(); files = sorted(glob.glob(f"{pd_}/*.parquet")); parts = []; tot_stats = defaultdict(int); done = 0
from multiprocessing import get_context
with get_context("fork").Pool(NPROC) as pool:
    for part, seen, st, nb, ns in pool.imap_unordered(work_file, list(enumerate(files))):
        parts.append(part); nseen |= seen; done += 1
        for k, v in st.items(): tot_stats[k] += v
        if done % 8 == 0 or done == len(files): log(f"  {done}/{len(files)} files, {len(nseen)} blocks decided ({time.time()-t0:.0f}s, {NPROC} procs)")
out_rows.clear(); stats.clear()
for bk, rs in night.items():
    if bk in nseen: continue
    for r in rs: emit(r, "mega_cascade" if r["tier"] == "mega" else "mint")
for k, v in stats.items(): tot_stats[k] += v
log("decisions:", json.dumps({f"{t}/{r}": v for (t, r), v in sorted(tot_stats.items())}))
# ---- write decisions (typed) ------------------------------------------------------------------------------------------------------------
os.makedirs(f"{HOME}/results", exist_ok=True); fn = f"/tmp/nightly_{D8}.jsonl"
with open(fn, "w") as f:
    for part in parts:
        with open(part) as g: shutil_copyfileobj = f.write(g.read())
    for r in out_rows: f.write(json.dumps(r) + "\n")
import shutil; shutil.copy(fn, f"{HOME}/results/nightly_{D8}.jsonl")
top3_t = ArrayType(StructType([StructField("best", DoubleType()), StructField("best_seat", ArrayType(LongType())), StructField("cen", DoubleType()), StructField("n", LongType()), StructField("pid", LongType()), StructField("wc", LongType())]))
df = spark.read.json(f"{HOME}/results/nightly_{D8}.jsonl")
for c, t in {"work_id": "bigint", "author_sequence": "bigint", "vec_author_id": "bigint", "band_pid": "bigint", "band_best": "double", "band_cen": "double", "band_n": "bigint", "jev_pid": "bigint", "jev_p": "double", "jev_choice": "string",
             "n_prof": "bigint", "n_masked": "bigint", "name_author_id": "bigint", "orcid_author_id": "bigint"}.items(): df = df.withColumn(c, F.col(c).cast(t))
df = df.withColumn("top3", F.from_json(F.to_json(F.col("top3")), top3_t)).select("run_date", "work_id", "author_sequence", "block_key", "tier", "rule", "vec_author_id", "band_pid", "band_best", "band_cen", "band_n", "jev_pid", "jev_p", "jev_choice", "top3", "n_prof", "n_masked", "name_author_id", "orcid_author_id", "match_outcome")
if spark.catalog.tableExists(OUT): spark.sql(f"DELETE FROM {OUT} WHERE run_date = '{DATE}'")
df.write.mode("append").partitionBy("run_date").saveAsTable(OUT); log("decisions written to", OUT)
log("NIGHTLY DECIDE DONE", DATE)
