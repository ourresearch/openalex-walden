"""Worked example (oxjob #2000, 12-13 Nov 2026): 1.76B reference texts embedded with Qwen3-Embedding-0.6B on Modal H100s and
loaded into openalex.vector_search.reference_embeddings_qwen3. Input shard parquet: (text_hash BIGINT, text STRING);
output: (text_hash, text, v = 1,024 float16 unit vector as 2,048 bytes), the format of the works table
work_reference_style_embeddings_qwen3, so the two compare directly.

Measured (#2000): 4 vLLM engines per H100 ~4,000-4,200 texts/s (one engine is CPU-bound at ~2,000); 12 CPU / 48 GB per container
is as fast as 32 / 128 and 24% cheaper; ~$0.45 per 1M texts compute + ~$0.08 egress; vectors match the Databricks endpoint
databricks-qwen3-embedding-0-6b to cosine 0.9999+. max_model_len 8,192: 1,000 characters of some scripts exceed 2,048 tokens.

    modal run embed_qwen3_app.py::model          # once: copy the weights to the volume (containers then skip the HF download)
    modal run embed_qwen3_app.py::ident          # OIDC sub for role.yaml
    modal run embed_qwen3_app.py::backfill --shards 0-255
Runbook: docs/runbooks/modal-gpu-batch-via-s3.md
"""
import json
import os
import time

import modal

APP_NAME = "myjob-embed"
BUCKET, PREFIX = "openalex-sandbox", "myjob/embed"
ROLE_ARN = "arn:aws:iam::639657227068:role/myjob-modal-s3"
VOLUME = "myjob-models"
MODEL_ID = "Qwen/Qwen3-Embedding-0.6B"
MODEL = "/vol/models/Qwen3-Embedding-0.6B"
# Query-side instruction #2000 used for reference strings; documents (works) were embedded bare. Must match how the
# vectors you compare against were made.
INSTRUCTION = "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: "
ENGINES = 4

app = modal.App(APP_NAME)
vol = modal.Volume.from_name(VOLUME, create_if_missing=True)
image = (modal.Image.from_registry("nvidia/cuda:12.9.1-devel-ubuntu22.04", add_python="3.12")
         .pip_install("vllm==0.30.0", "pyarrow", "boto3", "huggingface_hub[hf_xet]")
         .env({"HF_HOME": "/root/hf", "TOKENIZERS_PARALLELISM": "false", "PYTHONUNBUFFERED": "1", "HF_XET_HIGH_PERFORMANCE": "1",
               "CUDA_HOME": "/usr/local/cuda"})
         .add_local_python_source("s3io")
         .add_local_file(__file__, "/root/embed_engine.py"))  # the engine children import engine_loop from here


def engine_loop(k, util, inq, outq):
    """One vLLM engine in a child process (several share the GPU). Gets (tag, texts), saves unit fp16 rows, returns the path."""
    import numpy as np
    from vllm import LLM
    llm = LLM(model=MODEL, runner="pooling", dtype="bfloat16", max_model_len=8192, gpu_memory_utilization=util,
              enable_prefix_caching=False)
    llm.embed([INSTRUCTION + "warm up"] * 64, use_tqdm=False)
    outq.put(("ready", k))
    while True:
        job = inq.get()
        if job is None:
            return
        tag, texts = job
        parts = []
        for i in range(0, len(texts), 20000):
            outs = llm.embed([INSTRUCTION + t for t in texts[i:i + 20000]], use_tqdm=False)
            V = np.asarray([o.outputs.embedding for o in outs], dtype=np.float32)
            V /= np.linalg.norm(V, axis=1, keepdims=True)
            parts.append(V.astype(np.float16))
        path = f"/tmp/out_{tag}_{k}.npy"
        np.save(path, np.concatenate(parts) if parts else np.zeros((0, 1024), np.float16))
        outq.put((tag, path))


@app.cls(image=image, gpu="H100", volumes={"/vol": vol}, cpu=12, memory=48 * 1024, timeout=2 * 3600, scaledown_window=120,
         max_containers=50)
class Embedder:
    @modal.enter()
    def start(self):
        import multiprocessing as mp
        import queue
        import sys
        sys.path.insert(0, "/root")
        import embed_engine
        ctx = mp.get_context("spawn")
        self.inq = [ctx.Queue() for _ in range(ENGINES)]
        self.outq = ctx.Queue()
        self.ps = [ctx.Process(target=embed_engine.engine_loop, args=(k, round(0.85 / ENGINES, 3), self.inq[k], self.outq))
                   for k in range(ENGINES)]
        for p in self.ps:
            p.start()
        t0, ready = time.time(), 0
        while ready < ENGINES:  # fail fast if an engine dies while starting
            try:
                self.outq.get(timeout=30)
                ready += 1
            except queue.Empty:
                dead = [k for k, p in enumerate(self.ps) if not p.is_alive()]
                if dead or time.time() - t0 > 1200:
                    raise RuntimeError(f"engines failed to start: dead {dead}, {time.time() - t0:.0f}s")

    def embed(self, texts, tag):
        import queue
        import numpy as np
        n = len(texts)
        cuts = [n * k // ENGINES for k in range(ENGINES + 1)]
        for k in range(ENGINES):
            self.inq[k].put((tag, texts[cuts[k]:cuts[k + 1]]))
        got = {}
        while len(got) < ENGINES:
            try:
                _, path = self.outq.get(timeout=30)
            except queue.Empty:
                dead = [k for k, p in enumerate(self.ps) if not p.is_alive()]
                if dead:
                    raise RuntimeError(f"engines died mid-job: {dead}")
                continue
            got[int(path.rsplit("_", 1)[1][:-4])] = path
        V = np.concatenate([np.load(got[k]) for k in range(ENGINES)])
        for p in got.values():
            os.remove(p)
        assert len(V) == n, (len(V), n)
        return V

    @modal.method()
    def shard(self, s: int):
        import pyarrow as pa
        import s3io
        c = s3io.client(ROLE_ARN, APP_NAME)
        if s3io.done(c, BUCKET, PREFIX, s):
            return {"shard": s, "skipped": True}
        t0 = time.time()
        t = s3io.read_shard(c, BUCKET, PREFIX, s, columns=["text_hash", "text"])
        if t is None:
            return {"shard": s, "error": "no input files"}
        V = self.embed(t.column("text").to_pylist(), f"s{s}")
        t1 = time.time()
        out = pa.table({"text_hash": t.column("text_hash").cast(pa.int64()), "text": t.column("text"),
                        "v": pa.array([r.tobytes() for r in V], type=pa.binary())})
        s3io.write_shard(ROLE_ARN, BUCKET, PREFIX, s, out)
        return {"shard": s, "rows": t.num_rows, "texts_per_s": round(t.num_rows / max(t1 - t0, 1e-9)),
                "seconds": round(time.time() - t0)}

    @modal.exit()
    def stop(self):
        for q in self.inq:
            q.put(None)


@app.function(image=image, volumes={"/vol": vol}, timeout=1800)
def fetch_model():
    from huggingface_hub import snapshot_download
    snapshot_download(MODEL_ID, local_dir=MODEL, max_workers=16)
    vol.commit()
    return sorted(os.listdir(MODEL))


@app.function(image=modal.Image.debian_slim().pip_install("boto3").add_local_python_source("s3io"), timeout=120)
def whoami():
    import s3io
    return s3io.whoami_claims()


@app.local_entrypoint()
def model():
    print("MODEL " + str(fetch_model.remote()))


@app.local_entrypoint()
def ident():
    print("WHOAMI " + json.dumps(whoami.remote()))


@app.local_entrypoint()
def backfill(shards: str = "0-7"):
    import s3io
    todo = s3io.parse_ranges(shards)
    t0, rows, n = time.time(), 0, 0
    for r in Embedder().shard.map(todo, order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception) or "error" in r:
            print("SHARD_ERROR " + repr(r)[:500], flush=True)
            continue
        n += 1
        rows += r.get("rows", 0)
        print("SHARD " + json.dumps(r) + f" | {n}/{len(todo)} shards, {rows:,} texts, {rows / max(time.time() - t0, 1):,.0f}/s",
              flush=True)
