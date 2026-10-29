# Databricks notebook source
# MAGIC %md
# MAGIC # Related works for new and re-embedded works (nightly, oxjob #1648)
# MAGIC
# MAGIC Gives every work that has a Qwen3 vector but no related works (new works, ~120K a day) its list, and recomputes the
# MAGIC lists of works whose vector changed since their list was made (re-embedded after a title / abstract / venue change),
# MAGIC up to `reembed_cap` a night, oldest first. Runs in `jobs/embed_qwen3_nightly.yaml` right after the embed task, so the
# MAGIC next Walden End 2 End (`CreateWorksEnriched` reads `openalex.works.work_related_works`) serves the lists.
# MAGIC
# MAGIC Same method and policy as the 27 Oct backfill (policy v1.2): the 10 nearest works by cosine on the Qwen3 vector; never
# MAGIC paratext / erratum / retraction / peer-review / book-review / libguides / supplementary-materials or a retracted work;
# MAGIC never the work's own title and no title twice; paper-like works get paper-like works only; other works at most 3 from
# MAGIC their own venue. Lists can be shorter than 10 or empty.
# MAGIC
# MAGIC How (IVF, no GPU, no external index): `openalex.vector_search.work_vectors_ivf` holds every work's vector (float16)
# MAGIC tagged with one of 131,072 lists (k-means centroids in the volume, from the backfill; `BuildRelatedWorksIvf`).
# MAGIC 1. Queries: the night's works; each gets its 32 nearest lists (`nprobe`) from the centroids, on the driver.
# MAGIC 2. Their vectors are appended to the vector table first (so tonight's new works can find each other).
# MAGIC 3. One scan of the vector table (`mapInArrow`): each list's rows are scored exactly against the queries that probe it.
# MAGIC    A random `exact_sample` of queries is also scored against every row: the exact top 10 gives tonight's recall@10.
# MAGIC 4. The best 64 per query, checked against `openalex_works` as it is now (live, type, retracted, title, venue),
# MAGIC    then policy v1.2 -> 10. Fails before writing if recall@10 < `min_recall`.
# MAGIC 5. MERGE into `work_related_works` (`mode=write`); `mode=dry_run` stops at the staging table.
# MAGIC
# MAGIC Each work whose list lands takes one `updated_date` stamp in the next End 2 End (related_works is in the content hash).
# MAGIC Idempotent: a failed run is re-run; the vector append skips rows already there.

# COMMAND ----------

dbutils.widgets.text("mode", "write")               # write | dry_run
dbutils.widgets.text("reembed_cap", "500000")       # re-embedded works per night (new works are never capped)
dbutils.widgets.text("max_queries", "3000000")      # guardrail: refuse an absurd night before scanning
dbutils.widgets.text("nprobe", "32")
dbutils.widgets.text("exact_sample", "200")         # queries also searched exactly, for tonight's recall@10
dbutils.widgets.text("min_recall", "0.85")          # dry run 29 Oct: 0.927 on 2,000 (backfill r2 GPU, 8-bit codes: 0.823);
                                                    # a 200-query sample moves ~0.015, so 0.85 trips only on a real fault
dbutils.widgets.text("append_vectors", "true")
dbutils.widgets.text("check_sample", "0")           # tests only: recompute this many backfill works (never written) and
                                                    # report how many of their 10 match the backfill's list

MODE = dbutils.widgets.get("mode")
assert MODE in ("write", "dry_run"), MODE
REEMBED_CAP = int(dbutils.widgets.get("reembed_cap"))
MAX_QUERIES = int(dbutils.widgets.get("max_queries"))
NPROBE = int(dbutils.widgets.get("nprobe"))
N_EXACT = int(dbutils.widgets.get("exact_sample"))
MIN_RECALL = float(dbutils.widgets.get("min_recall"))
APPEND = dbutils.widgets.get("append_vectors").lower() == "true"
N_CHECK = int(dbutils.widgets.get("check_sample"))
BACKFILL_ALGO, BACKFILL_VEC_AT = "qwen3-knn-v1.2", "2026-10-27 16:20:00"   # the 27 Oct backfill and its vectors' time

E = "openalex.vector_search.work_embeddings_qwen3"
W = "openalex.works.openalex_works"
R = "openalex.works.work_related_works"
B = "openalex.vector_search.work_vectors_ivf"
QT = "openalex.vector_search.related_works_nightly_queries"
PARTS = "openalex.vector_search.related_works_nightly_parts"
OUT = "openalex.vector_search.related_works_nightly_out"
LOG = "openalex.vector_search.related_works_nightly_log"
VOL = "/Volumes/openalex/vector_search/related_works_ivf"
ALGO = "qwen3-knn-v1.2-ivf-nightly"
D, NLIST, LISTS_PER_GROUP, K_LIST, POOL, K_EXACT, KEEP, VENUE_CAP = 1024, 131072, 128, 48, 64, 16, 10, 3
PAPERLIKE = ['article', 'review', 'book-chapter', 'book', 'preprint', 'dissertation', 'conference-paper', 'conference-abstract',
             'letter', 'editorial', 'report', 'data-paper', 'software-paper', 'standard', 'reference-entry']
JUNK = ['paratext', 'erratum', 'retraction', 'peer-review', 'book-review', 'libguides', 'supplementary-materials']
# the backfill export's title key (q_full_export.sql): Unicode letters and digits only, lower case; empty -> NULL (never equal)
TITLE_H = r"""CAST(CASE WHEN length(regexp_replace(lower({t}), '[^\\p{{L}}\\p{{N}}]+', '')) > 0
  THEN xxhash64(regexp_replace(lower({t}), '[^\\p{{L}}\\p{{N}}]+', '')) END AS STRING)"""   # string: pandas keeps it exact

import json, os, time, uuid
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

RUN_ID = time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
RUN = f"{VOL}/runs/{RUN_ID}"
os.makedirs(RUN, exist_ok=True)
stats = {"run_id": RUN_ID, "mode": MODE, "nprobe": NPROBE, "reembed_cap": REEMBED_CAP}
T0 = time.time()
def step(msg):
    print(f"+{time.time() - T0:7.0f}s {msg}", flush=True)
step(f"run {RUN_ID} mode={MODE} reembed_cap={REEMBED_CAP:,} nprobe={NPROBE} exact_sample={N_EXACT}")

# COMMAND ----------

# 1. Queries: works with a vector and no list, then re-embedded works (vector newer than their list), capped, oldest first.
@F.pandas_udf("binary")
def fp16b(s: pd.Series) -> pd.Series:
    return pd.Series([None if x is None else np.asarray(x, dtype=np.float16).tobytes() for x in s])
spark.udf.register("rw_fp16b", fp16b)

# Pick the ids first from (work_id, embedded_at) only; then read just those vectors, from files embedded since the oldest
# pick (data skipping on embedded_at), never the whole 1.9 TB table.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rw_pick AS
WITH ev AS (SELECT CAST(work_id AS BIGINT) AS work_id, embedded_at FROM {E}),
new AS (
  SELECT ev.work_id, ev.embedded_at, 'new' AS reason
  FROM ev JOIN {W} w ON w.id = ev.work_id
  LEFT ANTI JOIN {R} r ON r.work_id = ev.work_id
),
re AS (
  SELECT ev.work_id, ev.embedded_at, 'reembed' AS reason
  FROM ev JOIN {R} r ON r.work_id = ev.work_id
  JOIN {W} w ON w.id = ev.work_id
  WHERE ev.embedded_at > r.computed_at
  ORDER BY ev.embedded_at, ev.work_id
  LIMIT {REEMBED_CAP}
)
SELECT * FROM new UNION ALL SELECT * FROM re
""")
spark.sql("CACHE TABLE rw_pick_c AS SELECT * FROM rw_pick")
lo = spark.sql("SELECT MIN(embedded_at) m, COUNT(*) n FROM rw_pick_c").first()
step(f"picked {lo.n:,} ids; oldest vector {lo.m}")
lo_ts = (lo.m or __import__("datetime").datetime(2100, 1, 1)).strftime("%Y-%m-%d %H:%M:%S.%f")
# check rows (tests): backfill works whose vector is the one the backfill used, read from the vector table's own copy
# (16 list groups only, so partition pruning keeps the read small); staged, never appended or written
check_sql = "" if N_CHECK <= 0 else f"""
  UNION ALL
  SELECT * FROM (
    SELECT b.work_id, 'check' AS reason, b.vec_at AS embedded_at, b.v
    FROM {B} b JOIN {R} r ON r.work_id = b.work_id
    WHERE b.lg IN ({",".join(str(x) for x in range(0, 1024, 64))}) AND b.vec_at = TIMESTAMP '{BACKFILL_VEC_AT}'
      AND r.algorithm = '{BACKFILL_ALGO}' AND pmod(xxhash64(b.work_id, 1648), 1000) < 2
    ORDER BY xxhash64(b.work_id, 16481) LIMIT {N_CHECK})"""
spark.sql(f"""
CREATE OR REPLACE TABLE {QT} AS
WITH q AS (
  SELECT p.work_id, p.reason, p.embedded_at, rw_fp16b(e.embedding) AS v
  FROM rw_pick_c p
  JOIN (SELECT CAST(work_id AS BIGINT) AS work_id, embedding, embedded_at FROM {E}
        WHERE embedded_at >= TIMESTAMP '{lo_ts}') e ON e.work_id = p.work_id AND e.embedded_at = p.embedded_at
  {check_sql}
)
SELECT CAST(ROW_NUMBER() OVER (ORDER BY q.reason, q.work_id) - 1 AS INT) AS qi, q.work_id, q.reason, q.embedded_at, q.v,
       w.type, COALESCE(w.is_retracted, false) AS is_retracted
FROM q JOIN {W} w ON w.id = q.work_id
""")
cnt = {r.reason: r.n for r in spark.sql(f"SELECT reason, COUNT(*) n FROM {QT} GROUP BY 1").collect()}
NQ = sum(cnt.values())
stats.update({"n_new": cnt.get("new", 0), "n_reembed": cnt.get("reembed", 0), "n_check": cnt.get("check", 0), "n_queries": NQ})
step(f"queries: {NQ:,} ({cnt})")
if NQ > MAX_QUERIES:
    raise RuntimeError(f"{NQ:,} queries > max_queries={MAX_QUERIES:,}: refusing unattended; re-run with a larger max_queries if intended")

# COMMAND ----------

def finish(note):
    stats["note"] = note
    stats["minutes"] = round((time.time() - T0) / 60, 1)
    row = spark.createDataFrame([(RUN_ID, MODE, json.dumps(stats))], "run_id string, mode string, stats string") \
        .withColumn("logged_at", F.current_timestamp())
    row.write.mode("append").saveAsTable(LOG)
    step(f"logged: {note}")

if NQ == 0:
    finish("no queries")
    dbutils.notebook.exit("no queries")

# COMMAND ----------

# 2. Probe lists on the driver: each query's NPROBE nearest centroids (inner product; vectors and centroids are unit length).
qp = spark.table(QT).select("qi", "work_id", "v", "type").toPandas().sort_values("qi")
assert (qp.qi.values == np.arange(NQ)).all()
Q16 = np.frombuffer(b"".join(qp.v.values), dtype=np.float16).reshape(NQ, D)
qwid = qp.work_id.values.astype(np.int64)
qpaper = qp.type.isin(PAPERLIKE).values
cent = np.load(f"{VOL}/centroids.npy")
assert cent.shape == (NLIST, D)
probes = np.empty((NQ, NPROBE), dtype=np.int32)
for i in range(0, NQ, 4096):
    sc = np.asarray(Q16[i:i + 4096], dtype=np.float32) @ cent.T
    p = np.argpartition(-sc, NPROBE - 1, axis=1)[:, :NPROBE]
    o = np.argsort(-np.take_along_axis(sc, p, axis=1), axis=1)
    probes[i:i + 4096] = np.take_along_axis(p, o, axis=1)        # column 0 = the nearest list (where the work is filed)
del sc
flat_l = probes.ravel()
flat_q = np.repeat(np.arange(NQ, dtype=np.int32), NPROBE)
o = np.argsort(flat_l, kind="stable")
qix = flat_q[o]
ptr = np.searchsorted(flat_l[o], np.arange(NLIST + 1)).astype(np.int64)
rng = np.random.default_rng(int(RUN_ID[:8]))
S = np.sort(rng.choice(NQ, min(N_EXACT, NQ), replace=False)).astype(np.int64)
for name, arr in (("q", Q16), ("qwid", qwid), ("qpaper", qpaper), ("qix", qix), ("ptr", ptr), ("s", S)):
    np.save(f"{RUN}/{name}.npy", arr)
step(f"probes done; {len(S)} exact-sample queries; files in {RUN}")

# COMMAND ----------

# 3. File tonight's vectors in the table before the scan (skip rows already filed: re-runs are idempotent).
if APPEND:
    spark.createDataFrame(pd.DataFrame({"qi": np.arange(NQ, dtype=np.int32), "list_id": probes[:, 0]})) \
        .createOrReplaceTempView("rw_home")
    before = spark.table(B).count()
    spark.sql(f"""
INSERT INTO {B} (lg, list_id, work_id, v, type, is_retracted, vec_at)
SELECT CAST(h.list_id DIV {LISTS_PER_GROUP} AS INT), CAST(h.list_id AS INT), q.work_id, q.v, q.type, q.is_retracted, q.embedded_at
FROM (SELECT * FROM {QT} WHERE reason <> 'check') q JOIN rw_home h ON h.qi = q.qi
LEFT ANTI JOIN {B} b ON b.work_id = q.work_id AND b.vec_at = q.embedded_at
DISTRIBUTE BY CAST(h.list_id DIV {LISTS_PER_GROUP} AS INT)""")
    stats["appended"] = spark.table(B).count() - before
    step(f"vectors appended: {stats['appended']:,}")
    # compact the nightly small files now and then (only files under 64 MB: the big list-ordered files stay as they are)
    nfiles = spark.sql(f"DESCRIBE DETAIL {B}").first().numFiles
    if nfiles > 12 * 1024:
        spark.conf.set("spark.databricks.delta.optimize.minFileSize", str(64 * 1024 * 1024))
        spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(256 * 1024 * 1024))
        spark.sql(f"OPTIMIZE {B}")
        step(f"compacted {nfiles:,} files -> {spark.sql(f'DESCRIBE DETAIL {B}').first().numFiles:,}")

# COMMAND ----------

# 4. One scan of the vector table. Per list: exact scores against the queries that probe it, best K_LIST per query.
#    Exact sample: every row against the sampled queries, best K_EXACT. Candidate-only rules are applied here (type,
#    retracted, paper-like for paper-like queries, not itself) so they never use up slots; the rest after.
import pyarrow as pa
import pyarrow.compute as pc

RUN_DIR, JUNK_A, PAPER_A = RUN, JUNK, PAPERLIKE

def scan(batches):
    import numpy as np, pyarrow as pa, pyarrow.compute as pc
    try:
        from threadpoolctl import threadpool_limits
        threadpool_limits(1)
    except Exception:
        pass
    Q = np.load(f"{RUN_DIR}/q.npy", mmap_mode="r")
    qwid = np.load(f"{RUN_DIR}/qwid.npy"); qpaper = np.load(f"{RUN_DIR}/qpaper.npy")
    qix = np.load(f"{RUN_DIR}/qix.npy"); ptr = np.load(f"{RUN_DIR}/ptr.npy"); S = np.load(f"{RUN_DIR}/s.npy")
    Qs = np.asarray(Q[S], dtype=np.float32); s_paper = qpaper[S]; s_wid = qwid[S]
    junk_set, paper_set = pa.array(JUNK_A), pa.array(PAPER_A)
    out_q, out_c, out_s = [], [], []
    ex_s = np.full((len(S), K_EXACT), -np.inf, np.float32); ex_c = np.full((len(S), K_EXACT), -1, np.int64)
    for b in batches:
        n = b.num_rows
        if n == 0:
            continue
        lid = b.column("list_id").to_numpy(zero_copy_only=False)
        wid = b.column("work_id").to_numpy(zero_copy_only=False)
        col = b.column("v")
        off = np.frombuffer(col.buffers()[1], dtype=np.int32)[col.offset: col.offset + n + 1]
        assert col.null_count == 0 and (np.diff(off) == 2 * D).all(), "vector not 2048 bytes"
        V16 = np.frombuffer(col.buffers()[2], dtype=np.uint8)[off[0]: off[-1]].view(np.float16).reshape(n, D)
        ty = b.column("type")
        bad = (pc.fill_null(pc.is_in(ty, value_set=junk_set), False).to_numpy(zero_copy_only=False)
               | pc.fill_null(b.column("is_retracted"), False).to_numpy(zero_copy_only=False))
        paper = pc.fill_null(pc.is_in(ty, value_set=paper_set), False).to_numpy(zero_copy_only=False)
        order = np.argsort(lid, kind="stable")
        ls = lid[order]
        starts = np.flatnonzero(np.r_[True, ls[1:] != ls[:-1]])
        for a, e in zip(starts, np.r_[starts[1:], n]):
            l = ls[a]
            qs = qix[ptr[l]:ptr[l + 1]]
            if len(qs) == 0:
                continue
            rows = order[a:e]
            rows = rows[~bad[rows]]
            if len(rows) == 0:
                continue
            sc = V16[rows].astype(np.float32) @ np.asarray(Q[qs], dtype=np.float32).T      # (rows, queries)
            pq = qpaper[qs]
            if pq.any():
                sc[np.ix_(~paper[rows], pq)] = -np.inf
            sc[wid[rows][:, None] == qwid[qs][None, :]] = -np.inf
            k = min(K_LIST, len(rows))
            idx = np.argpartition(-sc, k - 1, axis=0)[:k] if len(rows) > k else np.repeat(np.arange(len(rows))[:, None], len(qs), 1)
            ts = np.take_along_axis(sc, idx, axis=0)
            keep = np.isfinite(ts)
            out_q.append(np.broadcast_to(qs[None, :], idx.shape)[keep]); out_c.append(wid[rows][idx][keep]); out_s.append(ts[keep])
        if len(S):
            sc = V16.astype(np.float32) @ Qs.T                                              # (rows, sample)
            sc[bad] = -np.inf
            if s_paper.any():
                sc[np.ix_(~paper, s_paper)] = -np.inf
            sc[wid[:, None] == s_wid[None, :]] = -np.inf
            k = min(K_EXACT, n)
            idx = np.argpartition(-sc, k - 1, axis=0)[:k] if n > k else np.repeat(np.arange(n)[:, None], len(S), 1)
            cs = np.concatenate([ex_s, np.take_along_axis(sc, idx, axis=0).T], 1)
            cc = np.concatenate([ex_c, wid[idx].T], 1)
            o = np.argsort(-cs, axis=1)[:, :K_EXACT]
            ex_s = np.take_along_axis(cs, o, 1); ex_c = np.take_along_axis(cc, o, 1)
    if out_q:
        q = np.concatenate(out_q).astype(np.int32); c = np.concatenate(out_c); s = np.concatenate(out_s).astype(np.float32)
        o = np.lexsort((-s, q)); q, c, s = q[o], c[o], s[o]
        first = np.r_[0, np.flatnonzero(q[1:] != q[:-1]) + 1]
        rank = np.arange(len(q)) - np.repeat(first, np.diff(np.r_[first, len(q)]))
        m = rank < POOL
        yield pa.RecordBatch.from_arrays([pa.array(np.zeros(m.sum(), np.int8)), pa.array(q[m]), pa.array(c[m]), pa.array(s[m])],
                                         names=["kind", "qi", "cand", "score"])
    if len(S):
        keep = np.isfinite(ex_s)
        yield pa.RecordBatch.from_arrays([pa.array(np.ones(keep.sum(), np.int8)),
                                          pa.array(np.broadcast_to(np.arange(len(S), dtype=np.int32)[:, None], ex_s.shape)[keep]),
                                          pa.array(ex_c[keep]), pa.array(ex_s[keep])],
                                         names=["kind", "qi", "cand", "score"])

t = time.time()
(spark.table(B).select("list_id", "work_id", "v", "type", "is_retracted")
 .mapInArrow(scan, "kind tinyint, qi int, cand bigint, score float")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PARTS))
stats["scan_min"] = round((time.time() - t) / 60, 1)
step(f"scan done in {stats['scan_min']} min")

# COMMAND ----------

# 5. Recall@10 on the exact sample: the scan's top 10 (same candidate rules, before title / venue rules) vs exact top 10.
#    kind=1 rows carry the sample's position in S as qi.
spark.createDataFrame(pd.DataFrame({"si": np.arange(len(S), dtype=np.int32), "qi": S.astype(np.int32)})) \
    .createOrReplaceTempView("rw_smap")
rec = spark.sql(f"""
WITH ivf AS (SELECT qi, cand FROM (SELECT qi, cand, MAX(score) s FROM {PARTS} WHERE kind = 0 GROUP BY qi, cand)
             QUALIFY ROW_NUMBER() OVER (PARTITION BY qi ORDER BY s DESC) <= 10),
ex AS (SELECT m.qi, x.cand FROM (SELECT qi AS si, cand, MAX(score) s FROM {PARTS} WHERE kind = 1 GROUP BY qi, cand) x
       JOIN rw_smap m ON m.si = x.si
       QUALIFY ROW_NUMBER() OVER (PARTITION BY m.qi ORDER BY x.s DESC) <= 10)
SELECT COUNT(DISTINCT ex.qi) n, COUNT(ivf.cand) / (10 * COUNT(DISTINCT ex.qi)) recall10
FROM ex LEFT JOIN ivf ON ivf.qi = ex.qi AND ivf.cand = ex.cand
""")
r = rec.first()
stats["recall10"] = None if r.recall10 is None else round(float(r.recall10), 4)
stats["recall_n"] = int(r.n)
step(f"recall@10 = {stats['recall10']} on {r.n} sampled queries (bar {MIN_RECALL})")

# COMMAND ----------

# 6. Policy v1.2 on the best POOL per query, with candidates as they are now in openalex_works (gone = dropped).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rw_pool AS
SELECT p.qi, p.cand, p.score, q.work_id AS q_work_id, qw.type AS q_type, {TITLE_H.format(t='qw.title')} AS q_title_h,
       qw.primary_location.source.id AS q_venue,
       cw.type AS c_type, COALESCE(cw.is_retracted, false) AS c_retracted, {TITLE_H.format(t='cw.title')} AS c_title_h,
       cw.primary_location.source.id AS c_venue
FROM (SELECT qi, cand, s AS score FROM (SELECT qi, cand, MAX(score) s FROM {PARTS} WHERE kind = 0 GROUP BY qi, cand)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY qi ORDER BY s DESC) <= {POOL}) p
JOIN {QT} q ON q.qi = p.qi
JOIN {W} qw ON qw.id = q.work_id
JOIN {W} cw ON cw.id = p.cand
""")

PAPER_SET, JUNK_SET = frozenset(PAPERLIKE), frozenset(JUNK)

def policy(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("score", ascending=False, kind="stable")
    src = pdf.iloc[0]
    q_paper = src.q_type in PAPER_SET
    q_title = None if pd.isna(src.q_title_h) else src.q_title_h
    q_venue = None if pd.isna(src.q_venue) else src.q_venue
    ids, scores, seen, same_venue = [], [], set(), 0
    for r in pdf.itertuples(index=False):
        if r.cand == r.q_work_id or r.c_type in JUNK_SET or r.c_retracted:
            continue
        if q_paper and r.c_type not in PAPER_SET:
            continue
        t = None if pd.isna(r.c_title_h) else r.c_title_h
        if t is not None:
            if t == q_title or t in seen:
                continue
            seen.add(t)
        if not q_paper and q_venue is not None and r.c_venue == q_venue:
            same_venue += 1
            if same_venue > VENUE_CAP:
                continue
        ids.append(f"https://openalex.org/W{int(r.cand)}"); scores.append(float(r.score))
        if len(ids) == KEEP:
            break
    return pd.DataFrame({"work_id": [int(src.q_work_id)], "related_works": [ids], "scores": [scores]})

lists = (spark.table("rw_pool").groupBy("qi")
         .applyInPandas(policy, "work_id bigint, related_works array<string>, scores array<float>"))
# queries whose pool emptied out entirely still get a row (an empty list), so they are not re-queried every night
(spark.table(QT).select("work_id", "reason").join(lists, "work_id", "left")
 .select("work_id", F.coalesce("related_works", F.array().cast("array<string>")).alias("related_works"),
         F.coalesce("scores", F.array().cast("array<float>")).alias("scores"),
         F.lit(ALGO).alias("algorithm"), F.current_timestamp().alias("computed_at"), "reason")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUT))
o = spark.sql(f"""SELECT COUNT(*) n, COUNT(DISTINCT work_id) d, COUNT_IF(size(related_works) > 0) nonempty,
  COUNT_IF(size(related_works) = 10) full10, ROUND(AVG(size(related_works)), 2) avg_len FROM {OUT}""").first()
stats.update({"out_rows": o.n, "out_nonempty": o.nonempty, "out_full10": o.full10, "out_avg_len": float(o.avg_len or 0)})
step(f"lists: {o.n:,} rows ({o.d:,} distinct), {o.nonempty:,} non-empty, {o.full10:,} with 10, mean length {o.avg_len}")
assert o.n == o.d == NQ, "one row per query expected"
if N_CHECK > 0:
    c = spark.sql(f"""SELECT COUNT(*) n, AVG(size(array_intersect(o.related_works, r.related_works))) overlap,
      AVG(size(r.related_works)) backfill_len, AVG(size(o.related_works)) new_len
      FROM {OUT} o JOIN {R} r ON r.work_id = o.work_id WHERE o.reason = 'check'""").first()
    stats.update({"check_n": c.n, "check_overlap": round(float(c.overlap or 0), 3), "check_backfill_len": round(float(c.backfill_len or 0), 3),
                  "check_new_len": round(float(c.new_len or 0), 3)})
    step(f"check vs backfill on {c.n} works: {stats['check_overlap']} of the backfill's {stats['check_backfill_len']} also listed "
         f"(new length {stats['check_new_len']})")

# COMMAND ----------

# 7. Write (only if tonight's recall passed).
if stats["recall10"] is not None and stats["recall_n"] >= min(50, NQ) and stats["recall10"] < MIN_RECALL:
    finish(f"STOP: recall@10 {stats['recall10']} < {MIN_RECALL}; nothing written to {R}")
    raise RuntimeError(f"recall@10 {stats['recall10']} below {MIN_RECALL}: lists not written")
if MODE == "write":
    m = spark.sql(f"""
MERGE INTO {R} t USING (SELECT * FROM {OUT} WHERE reason <> 'check') s ON t.work_id = s.work_id
WHEN MATCHED THEN UPDATE SET t.related_works = s.related_works, t.scores = s.scores, t.algorithm = s.algorithm,
     t.computed_at = s.computed_at
WHEN NOT MATCHED THEN INSERT (work_id, related_works, scores, algorithm, computed_at)
     VALUES (s.work_id, s.related_works, s.scores, s.algorithm, s.computed_at)""").first()
    stats["merge"] = m.asDict() if m is not None else None
    step(f"merged into {R}: {stats['merge']}")
finish("ok" if MODE == "write" else "dry run: lists in staging only")

# COMMAND ----------

# 8. Keep the volume small: drop run folders older than 7 days.
import shutil
cut = time.strftime("%Y%m%d", time.gmtime(time.time() - 7 * 86400))
for d in os.listdir(f"{VOL}/runs"):
    if d[:8] < cut:
        shutil.rmtree(f"{VOL}/runs/{d}", ignore_errors=True)
