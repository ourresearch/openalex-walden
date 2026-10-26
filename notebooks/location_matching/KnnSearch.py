# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 2/6: embedding neighbours on GPUs (oxjob #1668)
# MAGIC
# MAGIC Exact kNN of the night's seed vectors (`location_work_match_seed_vecs`) against every work vector in
# MAGIC `openalex.vector_search.work_embeddings_qwen3`, inside Databricks (no Elasticsearch). One pass over the corpus: each task
# MAGIC keeps the top K of its corpus slice per query (fp16 matmul selects, fp32 dot scores); a query's global top K is the top K of
# MAGIC the task tops, written to `location_work_match_knn_raw` (+ `knn_q`, query index -> seed). `sql/knn_rank.sql` (features task)
# MAGIC keeps the works that existed before the run and ranks 1-10. Runs on the job's GPU cluster (1-GPU workers, two tasks per
# MAGIC GPU), alongside the candidate lookup. Checked 2026-10-24 against SQL (max |diff| 1.7e-7) and Elasticsearch (90% of its top 10).

# COMMAND ----------

import datetime as dt
import os
import time

import numpy as np
import pyarrow as pa

dbutils.widgets.text("night", "")
dbutils.widgets.text("parts", "80")
dbutils.widgets.text("volume_dir", "/Volumes/openalex/works/data/location_work_match")
NIGHT = dbutils.jobs.taskValues.get(taskKey="prepare", key="night", debugValue=dbutils.widgets.get("night"))
PARTS = int(dbutils.widgets.get("parts"))
T = "openalex.works.location_work_match_"
K, DIM, BLOCK, QCH, RCH = 20, 1024, 32768, 32768, 4096
DIR = f"{dbutils.widgets.get('volume_dir').rstrip('/')}/knn_{NIGHT}"
QPATH = f"{DIR}/q.npy"
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


def knn_part(batches):
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

    def flush(wids, embs):
        c32 = torch.nn.functional.normalize(torch.from_numpy(np.concatenate(embs)).to(dev), dim=1)
        c16, ids = c32.half(), torch.from_numpy(np.concatenate(wids)).to(dev)
        k = min(K, c32.shape[0])
        for q0 in range(0, nq, QCH):
            q1 = min(nq, q0 + QCH)
            j = (q16[q0:q1] @ c16.T).topk(k, dim=1).indices
            s = torch.cat([(q32[r0:min(q1, r0 + RCH)].unsqueeze(1) * c32[j[r0 - q0:min(q1, r0 + RCH) - q0]]).sum(-1)
                           for r0 in range(q0, q1, RCH)])
            cs, ci = torch.cat([top_s[q0:q1], s], 1), torch.cat([top_i[q0:q1], ids[j]], 1)
            ts, tj = cs.topk(K, dim=1)
            top_s[q0:q1], top_i[q0:q1] = ts, ci.gather(1, tj)

    wids, embs, n = [], [], 0
    for b in batches:
        wids.append(b.column(0).to_numpy())
        embs.append(b.column(1).flatten().to_numpy().reshape(-1, DIM))
        n += b.num_rows
        if n >= BLOCK:
            flush(wids, embs); wids, embs, n = [], [], 0
    if n:
        flush(wids, embs)
    qi = torch.arange(nq, dtype=torch.int32, device=dev).unsqueeze(1).expand(nq, K)
    keep = top_i >= 0
    qi, wid, cos = qi[keep].cpu().numpy(), top_i[keep].cpu().numpy(), top_s[keep].cpu().numpy()
    for i in range(0, len(qi), 2_000_000):
        yield pa.RecordBatch.from_arrays([pa.array(qi[i:i + 2_000_000]), pa.array(wid[i:i + 2_000_000]),
                                          pa.array(cos[i:i + 2_000_000])], names=["qi", "wid", "cos"])


t = time.time()
spark.sql(f"""
  SELECT CAST(work_id AS BIGINT) AS wid, transform(embedding, x -> CAST(x AS FLOAT)) AS emb
  FROM openalex.vector_search.work_embeddings_qwen3 WHERE size(embedding) = {DIM}""") \
    .coalesce(PARTS).mapInArrow(knn_part, "qi INT, wid BIGINT, cos FLOAT") \
    .write.mode("overwrite").saveAsTable(f"{T}knn_raw")
log(f"search {time.time() - t:.0f}s")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
