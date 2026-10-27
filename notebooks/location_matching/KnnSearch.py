# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 2/6: embedding neighbours on GPUs (oxjob #1668)
# MAGIC
# MAGIC Exact kNN of the night's seed vectors (`location_work_match_seed_vecs`) against every work vector in
# MAGIC `openalex.vector_search.work_embeddings_qwen3`, inside Databricks (no Elasticsearch). Reads the GPU-ready fp16 export
# MAGIC (`ExportVectorShards`, daily after the 15:00 UTC embedding job) when it is at least as new as the table's last data change;
# MAGIC otherwise reads the table itself (same exact search, slower). One pass over the corpus: each task
# MAGIC keeps the top K of its corpus slice per query (fp16 matmul selects, fp32 dot scores); a query's global top K is the top K of
# MAGIC the task tops, written to `location_work_match_knn_raw` (+ `knn_q`, query index -> seed). `sql/knn_rank.sql` (features task)
# MAGIC keeps the works that existed before the run and ranks 1-10. Runs on the job's GPU cluster (1-GPU workers, two tasks per
# MAGIC GPU), alongside the candidate lookup. Checked 2026-10-24 against SQL (max |diff| 1.7e-7) and Elasticsearch (90% of its top 10).

# COMMAND ----------

import datetime as dt
import json
import os
import time

import numpy as np
import pyarrow as pa

dbutils.widgets.text("night", "")
dbutils.widgets.text("parts", "80")
dbutils.widgets.text("volume_dir", "/Volumes/openalex/works/data/location_work_match")
dbutils.widgets.text("vectors_dir", "/Volumes/openalex/works/data/location_work_match/vectors")
NIGHT = dbutils.widgets.get("night").strip()   # set only for a standalone run; in the job the prepare task names the night
if not NIGHT:
    NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue="")
PARTS = int(dbutils.widgets.get("parts"))
T = "openalex.works.location_work_match_"
K, DIM, BLOCK, QCH, RCH = 20, 1024, 32768, 32768, 4096
DIR = f"{dbutils.widgets.get('volume_dir').rstrip('/')}/knn_{NIGHT}"
QPATH = f"{DIR}/q.npy"
VEC_ROOT = dbutils.widgets.get("vectors_dir").rstrip("/")
SRC = "openalex.vector_search.work_embeddings_qwen3"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)

# COMMAND ----------

os.makedirs(DIR, exist_ok=True)
pdf = spark.table(f"{T}seed_vecs").where(f"size(embedding) = {DIM}").select("seed_id", "embedding").toPandas()
Q = np.stack(pdf.embedding.values).astype(np.float32)
Q /= np.linalg.norm(Q, axis=1, keepdims=True)
np.save(QPATH, Q)
spark.createDataFrame(pdf[["seed_id"]].assign(qi=np.arange(len(pdf), dtype=np.int32))).write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{T}knn_q")
log(f"{len(pdf):,} queries")
del Q, pdf

# COMMAND ----------


def _searcher():
    """Per-task GPU state: the queries (fp32 for scoring, fp16 for selection) and the running top K. `add(c16, ids)` folds one
    corpus block (unit vectors, fp16 on the GPU) in; `result()` yields the task tops as Arrow batches."""
    import torch
    from pyspark import TaskContext
    gpu = TaskContext.get().resources().get("gpu")
    dev = torch.device(f"cuda:{gpu.addresses[0]}" if gpu else "cuda:0")
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction = False
    q32 = torch.from_numpy(np.load(QPATH)).to(dev)
    q16 = q32.half()
    nq = q32.shape[0]
    top_s = torch.full((nq, K), -2.0, device=dev)
    top_i = torch.full((nq, K), -1, dtype=torch.int64, device=dev)

    def add(c16, ids):
        c32 = c16.float()
        k = min(K, c16.shape[0])
        for q0 in range(0, nq, QCH):
            q1 = min(nq, q0 + QCH)
            j = (q16[q0:q1] @ c16.T).topk(k, dim=1).indices
            s = torch.cat([(q32[r0:min(q1, r0 + RCH)].unsqueeze(1) * c32[j[r0 - q0:min(q1, r0 + RCH) - q0]]).sum(-1)
                           for r0 in range(q0, q1, RCH)])
            cs, ci = torch.cat([top_s[q0:q1], s], 1), torch.cat([top_i[q0:q1], ids[j]], 1)
            ts, tj = cs.topk(K, dim=1)
            top_s[q0:q1], top_i[q0:q1] = ts, ci.gather(1, tj)

    def result():
        qi = torch.arange(nq, dtype=torch.int32, device=dev).unsqueeze(1).expand(nq, K)
        keep = top_i >= 0
        qa, wid, cos = qi[keep].cpu().numpy(), top_i[keep].cpu().numpy(), top_s[keep].cpu().numpy()
        for i in range(0, len(qa), 2_000_000):
            yield pa.RecordBatch.from_arrays([pa.array(qa[i:i + 2_000_000]), pa.array(wid[i:i + 2_000_000]),
                                              pa.array(cos[i:i + 2_000_000])], names=["qi", "wid", "cos"])
    return add, result, dev


def knn_table(batches):
    """Corpus = the table's rows (array<float>), normalised on the GPU."""
    import torch
    add, result, dev = _searcher()
    wids, embs, n = [], [], 0

    def flush():
        c = torch.nn.functional.normalize(torch.from_numpy(np.concatenate(embs)).to(dev), dim=1).half()
        add(c, torch.from_numpy(np.concatenate(wids)).to(dev))
    for b in batches:
        wids.append(b.column(0).to_numpy())
        embs.append(b.column(1).flatten().to_numpy().reshape(-1, DIM))
        n += b.num_rows
        if n >= BLOCK:
            flush(); wids, embs, n = [], [], 0
    if n:
        flush()
    yield from result()


def knn_export(batches):
    """Corpus = the export's parts (unit-normalised fp16 + int64 ids), read straight into GPU blocks."""
    import torch
    add, result, dev = _searcher()
    for b in batches:
        for part in b.column(0).to_pylist():
            V = np.load(f"{BUILD_DIR}/part-{part:05d}.vec.npy")
            I = np.load(f"{BUILD_DIR}/part-{part:05d}.ids.npy")
            for s0 in range(0, len(I), BLOCK):
                add(torch.from_numpy(V[s0:s0 + BLOCK]).to(dev), torch.from_numpy(I[s0:s0 + BLOCK]).to(dev))
    yield from result()


def current_export():
    """The export's manifest when it is at least as new as the table's last data change, else (None, reason)."""
    try:
        cur = json.load(open(f"{VEC_ROOT}/current.json"))
    except Exception as e:
        return None, f"no export ({e!r})"
    last = spark.sql(f"""SELECT max(version) AS v FROM (DESCRIBE HISTORY {SRC})
      WHERE operation NOT IN ('OPTIMIZE', 'VACUUM START', 'VACUUM END', 'SET TBLPROPERTIES', 'ANALYZE')""").collect()[0].v
    if cur["version"] < last:
        return None, f"export at v{cur['version']} but the table changed at v{last}"
    return json.load(open(f"{VEC_ROOT}/{cur['build']}/manifest.json")), "ok"


t = time.time()
manifest, why = current_export()
if manifest:
    BUILD_DIR = f"{VEC_ROOT}/{manifest['build']}"
    corpus = f"export {manifest['build']} ({manifest['rows']:,} vectors)"
    parts = spark.createDataFrame([(p["part"],) for p in manifest["parts"]], "part INT").repartition(PARTS)
    raw = parts.mapInArrow(knn_export, "qi INT, wid BIGINT, cos FLOAT")
else:
    corpus = f"table {SRC} ({why})"
    raw = spark.sql(f"""
      SELECT CAST(work_id AS BIGINT) AS wid, transform(embedding, x -> CAST(x AS FLOAT)) AS emb
      FROM {SRC} WHERE size(embedding) = {DIM}""").coalesce(PARTS).mapInArrow(knn_table, "qi INT, wid BIGINT, cos FLOAT")
log(f"corpus: {corpus}")
raw.write.mode("overwrite").saveAsTable(f"{T}knn_raw")
log(f"search {time.time() - t:.0f}s")
dbutils.jobs.taskValues.set(key="corpus", value=corpus[:300])

# COMMAND ----------

dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
