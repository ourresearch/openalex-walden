# Databricks notebook source
# MAGIC %md
# MAGIC # Location work match vectors: GPU-ready copy of the work embeddings (oxjob #1668)
# MAGIC
# MAGIC The nightly kNN (`KnnSearch`) spent most of its 40 minutes reading `openalex.vector_search.work_embeddings_qwen3` (1.95 TB,
# MAGIC 481.5M vectors stored as 8-byte doubles) and converting it, not on the GPU math. This job writes the table once a day, after
# MAGIC the 15:00 UTC embedding job, as fp16 numpy shards in a Unity Catalog volume (~1 TB): `<volume_dir>/<build>/part-NNNNN.vec.npy`
# MAGIC (unit-normalised float16, shape n x 1024) + `part-NNNNN.ids.npy` (int64 work ids) + `manifest.json` (source table version,
# MAGIC rows per part). `<volume_dir>/current.json` names the newest complete build; older builds beyond `keep` are deleted. The
# MAGIC export reads the table at one version, so a build is one consistent snapshot.

# COMMAND ----------

import datetime as dt
import json
import os
import time

import numpy as np
import pyarrow as pa

for name, default in [("volume_dir", "/Volumes/openalex/works/data/location_work_match/vectors"), ("shards", "1024"), ("keep", "2")]:
    dbutils.widgets.text(name, default)
ROOT = dbutils.widgets.get("volume_dir").rstrip("/")
SHARDS, KEEP, DIM = int(dbutils.widgets.get("shards")), int(dbutils.widgets.get("keep")), 1024
SRC = "openalex.vector_search.work_embeddings_qwen3"
T0 = time.time()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


VER = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 1").collect()[0].version
BUILD = f"{dt.datetime.utcnow():%Y%m%dT%H%M}_v{VER}"
OUT = f"{ROOT}/{BUILD}"
os.makedirs(OUT, exist_ok=True)
log(f"exporting {SRC} version {VER} -> {OUT}")

# COMMAND ----------


def write_shard(batches):
    from pyspark import TaskContext
    pid = TaskContext.get().partitionId()
    ids, vecs = [], []
    for b in batches:
        v = b.column(1).flatten().to_numpy().reshape(-1, DIM)
        v = v / np.linalg.norm(v, axis=1, keepdims=True)
        ids.append(b.column(0).to_numpy()); vecs.append(v.astype(np.float16))
    n = 0
    if ids:
        I, V = np.concatenate(ids), np.concatenate(vecs)
        n = len(I)
        np.save(f"{OUT}/part-{pid:05d}.ids.npy", I)
        np.save(f"{OUT}/part-{pid:05d}.vec.npy", V)
    yield pa.RecordBatch.from_arrays([pa.array([pid], pa.int32()), pa.array([n], pa.int64())], names=["part", "n"])


parts = spark.sql(f"""
  SELECT CAST(work_id AS BIGINT) AS wid, transform(embedding, x -> CAST(x AS FLOAT)) AS emb
  FROM {SRC} VERSION AS OF {VER} WHERE size(embedding) = {DIM}""") \
    .coalesce(SHARDS).mapInArrow(write_shard, "part INT, n BIGINT").collect()
parts = sorted((r.part, r.n) for r in parts if r.n)
manifest = {"build": BUILD, "source": SRC, "version": VER, "dim": DIM, "dtype": "float16", "normalised": True,
            "rows": sum(n for _, n in parts), "parts": [{"part": p, "n": n} for p, n in parts],
            "built_at": dt.datetime.utcnow().isoformat() + "Z", "seconds": round(time.time() - T0, 1)}
with open(f"{OUT}/manifest.json", "w") as f:
    json.dump(manifest, f)
with open(f"{ROOT}/current.json", "w") as f:
    json.dump({"build": BUILD, "version": VER, "rows": manifest["rows"]}, f)
log(f"{manifest['rows']:,} vectors in {len(parts)} parts, {manifest['seconds']:.0f}s; current = {BUILD}")

# COMMAND ----------

builds = sorted(d.name.rstrip("/") for d in dbutils.fs.ls(ROOT) if d.isDir())
for old in builds[:-KEEP]:
    if old != BUILD:
        dbutils.fs.rm(f"{ROOT}/{old}", recurse=True)
        log(f"removed old build {old}")
dbutils.notebook.exit(json.dumps({k: manifest[k] for k in ("build", "version", "rows", "seconds")}))
