"""Topics nightly scorer on Modal (oxjob #1531). The GPU side of the Databricks job "Topics" (jobs/topics.yaml): the job's
`topics_score_modal` task POSTs the queue table here and polls; vLLM does not run on Databricks (FIPS).

- `api` (bearer token): POST /start validates the request, spawns `run_all` and returns its call id; POST /status says running / done
  (with the per-shard results) / failed.
- `run_all`: exports the queue table ONCE through the Databricks SQL statement API (EXTERNAL_LINKS) into one file per shard on the
  Modal Volume `openalex-topics-nightly` (/data/<build_id>/in/<shard>.jsonl.gz), then maps `Scorer.score` over the shards. GPU workers
  never query the warehouse; each one PUTs `<out_dir>/<shard>.parquet` to the Unity Catalog Volume.
- `Scorer`: the #1531 corpus pass class verbatim (oxjobs topic-model-release scratch/corpus/modal_corpus.py): the #1485 student q8b_2m
  (Qwen3-8B causal LM, last-token pooling, linear head over 4,516 topics + 1 not-classifiable class), vLLM FP8 on one H100, input text
  `Title / Abstract[:4000] / Venue` ("(none)" / "(unknown)"; no Abstract line when there is none), HF tokenizer right truncation at 384,
  top 10 classes. Output parquet = the scratch/corpus/modal_load.py schema that built openalex.transient.topics_v2_scores:
  work_id BIGINT, topic_ids ARRAY<INT> (numeric topic ids, -1 = not classifiable), probs ARRAY<FLOAT>, not_classifiable BOOLEAN
  (top class is not classifiable), shard STRING. Class order: q8b_2m_class_order.json (index i -> topic id; index 4516 -> -1).
  Max 8 GPUs (affiliations / authors work keeps priority on the shared 50-GPU workspace).

Volumes: `openalex-text-topics` at /vol (model: models/q8b_2m/causal + head.pt, read only); `openalex-topics-nightly` at /data
(exported queue shards, one folder per build).
Secrets: `openalex-topics-nightly-token` (TOPICS_NIGHTLY_TOKEN; the same value is in Databricks secret scope `modal`, key
`topics1531_nightly_token`), `oxjob-databricks` (DATABRICKS_HOST / DATABRICKS_TOKEN: the identity that reads the queue and writes the
UC Volume).
Deploy from a checkout:  modal deploy notebooks/topics/nightly/modal_topics_nightly.py
Endpoint: https://wordslikethis--openalex-topics-nightly-api.modal.run (/start, /status)"""
import gzip
import json
import os
import re
import time

import modal

APP = "openalex-topics-nightly"
VERSION_MARK = "topics-nightly v1 2026-10-04"
app = modal.App(APP)
mvol = modal.Volume.from_name("openalex-text-topics")
dvol = modal.Volume.from_name("openalex-topics-nightly", create_if_missing=True, version=2)
HERE = os.path.dirname(os.path.abspath(__file__))
cpu_image = modal.Image.debian_slim(python_version="3.11").pip_install("httpx", "fastapi[standard]")
vllm_image = (modal.Image.debian_slim(python_version="3.11")
              .pip_install("vllm>=0.10,<0.11", "transformers>=4.53,<4.56", "numpy")   # = the corpus pass image
              .pip_install("httpx", "pyarrow")
              .env({"HF_HOME": "/vol/hf", "VLLM_CACHE_ROOT": "/tmp/vllm_cache", "TOKENIZERS_PARALLELISM": "true"})
              .add_local_file(f"{HERE}/q8b_2m_class_order.json", "/cfg/q8b_2m_class_order.json"))
TOKEN = modal.Secret.from_name("openalex-topics-nightly-token")
DBX = modal.Secret.from_name("oxjob-databricks")

TAG = "q8b_2m"
NC = 4516                 # class index of "not classifiable" -> topic id -1
MAXC = 8                  # max H100 containers
DEFAULT_WAREHOUSE = "3996dc0a9b183ce3"   # DEV-WH (X-Large serverless), as the corpus export
PRICE_PER_S = (3.95 + 16 * 0.0473 + 64 * 0.008) / 3600   # H100 + 16 cpu + 64 GiB, $/container-second (Modal billing is the truth)
TABLE_RX = re.compile(r"^openalex\.(works|transient)\.[A-Za-z0-9_]{1,120}$")
OUT_RX = re.compile(r"^/Volumes/openalex/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.=-]+){1,6}$")
WH_RX = re.compile(r"^[0-9a-f]{16}$")
BUILD_RX = re.compile(r"^[0-9A-Za-z_-]{6,40}$")


def text(r):   # = modal_corpus.text (the corpus pass and #1485 training input)
    p = [f"Title: {r['ti'] or '(none)'}"]
    if r["ab"]: p.append(f"Abstract: {r['ab'][:4000]}")
    p.append(f"Venue: {r['ve'] or '(unknown)'}"); return "\n".join(p)


def _dbx_host():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    return host if host.startswith("http") else "https://" + host


@app.cls(image=vllm_image, gpu="H100", volumes={"/vol": mvol, "/data": dvol}, secrets=[DBX], timeout=3600, startup_timeout=1200, cpu=16,
         memory=65536, max_containers=MAXC, scaledown_window=120, retries=modal.Retries(max_retries=2, initial_delay=10.0))
class Scorer:
    @modal.enter()
    def load(self):   # = modal_corpus.Scorer.load
        import torch, torch.nn as nn
        from vllm import LLM
        from transformers import AutoTokenizer
        print(VERSION_MARK, "scorer load", flush=True)
        d = f"/vol/models/{TAG}"; self.tok = AutoTokenizer.from_pretrained(f"{d}/causal")
        kw = dict(model=f"{d}/causal", dtype="bfloat16", max_model_len=512, gpu_memory_utilization=0.85, enable_prefix_caching=False, quantization="fp8")
        try: self.llm = LLM(runner="pooling", convert="embed", override_pooler_config={"pooling_type": "LAST", "normalize": False}, **kw)
        except TypeError: self.llm = LLM(task="embed", override_pooler_config={"pooling_type": "LAST", "normalize": False}, **kw)
        sd = torch.load(f"{d}/head.pt"); self.head = nn.Linear(sd["weight"].shape[1], sd["weight"].shape[0]).cuda(); self.head.load_state_dict(sd); self.head.eval()
        import numpy as np
        order = json.load(open("/cfg/q8b_2m_class_order.json"))
        assert len(order) == NC and sd["weight"].shape[0] == NC + 1, (len(order), tuple(sd["weight"].shape))
        self.tid = np.array([int(t[1:]) for t in order] + [-1], dtype=np.int32)

    def run(self, rows, k=10, block=20000):   # = modal_corpus.Scorer.run
        import torch, numpy as np
        from vllm.inputs import TokensPrompt
        from concurrent.futures import ThreadPoolExecutor
        tokf = lambda rs: self.tok([text(r) for r in rs], truncation=True, max_length=384)["input_ids"]
        blocks = [rows[s:s + block] for s in range(0, len(rows), block)]; I, P = [], []
        with ThreadPoolExecutor(1) as ex:   # tokenize the next block while the GPU embeds this one
            fut = ex.submit(tokf, blocks[0]) if blocks else None
            for j in range(len(blocks)):
                ids = fut.result()
                if j + 1 < len(blocks): fut = ex.submit(tokf, blocks[j + 1])
                outs = self.llm.embed([TokensPrompt(prompt_token_ids=x) for x in ids], use_tqdm=False)
                H = torch.tensor(np.array([o.outputs.embedding for o in outs]), dtype=torch.float32, device="cuda")
                with torch.no_grad(): v, i = torch.softmax(self.head(H), 1).topk(k, 1)
                I.append(i.cpu().numpy().astype(np.int16)); P.append(v.cpu().numpy().astype(np.float16))
        return np.concatenate(I), np.concatenate(P)

    @modal.method()
    def score(self, build_id: str, shard: int, out_dir: str):
        """Score /data/<build_id>/in/<shard>.jsonl.gz and PUT <out_dir>/<shard>.parquet (modal_load.upload schema) to the UC Volume."""
        import io, httpx, numpy as np, pyarrow as pa, pyarrow.parquet as pq
        t0 = time.time(); dvol.reload()
        rows = [json.loads(l) for l in gzip.open(f"/data/{build_id}/in/{shard:04d}.jsonl.gz", "rt")]
        t_read = time.time() - t0
        top, prob = self.run(rows)
        t_gpu = time.time() - t0 - t_read
        top, prob = top.astype(np.int64), prob.astype(np.float32)   # = modal_load: int16 class idx / fp16 prob, widened
        n, k = top.shape
        offs = pa.array(np.arange(0, n * k + 1, k, dtype=np.int32))
        sh = f"{shard:04d}"
        t = pa.table({"work_id": pa.array(np.array([r["id"] for r in rows], dtype=np.int64), pa.int64()),
                      "topic_ids": pa.ListArray.from_arrays(offs, pa.array(self.tid[top].ravel(), pa.int32())),
                      "probs": pa.ListArray.from_arrays(offs, pa.array(prob.ravel(), pa.float32())),
                      "not_classifiable": pa.array(top[:, 0] == NC),
                      "shard": pa.array([sh] * n, pa.string())})
        buf = io.BytesIO(); pq.write_table(t, buf, compression="zstd"); body = buf.getvalue()
        H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}", "Content-Type": "application/octet-stream"}
        for att in range(5):
            try:
                r = httpx.put(f"{_dbx_host()}/api/2.0/fs/files{out_dir}/{sh}.parquet", params={"overwrite": "true"}, content=body, headers=H, timeout=600)
                r.raise_for_status(); break
            except Exception as e:
                print("PUT retry", shard, repr(e)[:200], flush=True); time.sleep(10 * (att + 1))
        else:
            raise RuntimeError(f"shard {shard}: PUT failed")
        return {"shard": shard, "works": n, "nc": int((top[:, 0] == NC).sum()), "read_s": round(t_read, 1), "gpu_s": round(t_gpu, 1),
                "total_s": round(time.time() - t0, 1), "wps": round(n / max(t_gpu, 1e-6), 1), "bytes": len(body)}


def export_queue(table_in, build_id, warehouse):
    """One statement over the whole queue (EXTERNAL_LINKS), rows streamed into one gzip file per shard on /data/<build_id>/in/."""
    import httpx
    from concurrent.futures import ThreadPoolExecutor
    host = _dbx_host(); H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    d = f"/data/{build_id}/in"; os.makedirs(d, exist_ok=True)
    t0 = time.time()
    st = httpx.post(f"{host}/api/2.0/sql/statements", headers=H, timeout=120, json={
        "warehouse_id": warehouse, "wait_timeout": "0s", "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY",
        "statement": f"SELECT shard, work_id, title, abstract, venue FROM {table_in}"}).json()
    if "statement_id" not in st: raise RuntimeError(f"statement not accepted: {json.dumps(st)[:500]}")
    sid = st["statement_id"]; print("export statement", sid, flush=True)
    while st["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - t0 > 1800:
            httpx.post(f"{host}/api/2.0/sql/statements/{sid}/cancel", headers=H, timeout=60); raise TimeoutError("queue export not done in 30 min")
        time.sleep(5); st = httpx.get(f"{host}/api/2.0/sql/statements/{sid}", headers=H, timeout=120).json()
    if st["status"]["state"] != "SUCCEEDED": raise RuntimeError(json.dumps(st["status"])[:1000])
    total = int(st["manifest"].get("total_row_count", 0)); n_chunks = int(st["manifest"].get("total_chunk_count", 0))
    print(f"export: {total} rows in {n_chunks} chunks, query {time.time() - t0:.0f}s", flush=True)

    def fetch(k):
        for att in range(6):
            try:
                links = httpx.get(f"{host}/api/2.0/sql/statements/{sid}/result/chunks/{k}", headers=H, timeout=120).json()["external_links"]
                out = []
                for l in links: out += httpx.get(l["external_link"], timeout=600).json()
                return out
            except Exception as e:
                print("chunk retry", k, repr(e)[:200], flush=True); time.sleep(10 * (att + 1))
        raise RuntimeError(f"chunk {k} failed")

    files, counts, n = {}, {}, 0
    try:
        with ThreadPoolExecutor(4) as ex:   # map keeps chunk order and at most a few chunks in memory
            for rows in ex.map(fetch, range(n_chunks)):
                for shard, wid, ti, ab, ve in rows:
                    s = int(shard)
                    if s not in files: files[s] = gzip.open(f"{d}/{s:04d}.jsonl.gz", "wt", compresslevel=3)
                    files[s].write(json.dumps({"id": int(wid), "ti": ti or "", "ab": ab or "", "ve": ve or ""}, ensure_ascii=False) + "\n")
                    counts[s] = counts.get(s, 0) + 1; n += 1
    finally:
        for f in files.values(): f.close()
    if n != total: raise RuntimeError(f"export wrote {n} rows, statement said {total}")
    res = {"rows": n, "shards": {str(k): v for k, v in sorted(counts.items())}, "statement": sid, "seconds": round(time.time() - t0, 1)}
    json.dump(res, open(f"/data/{build_id}/export_done.json", "w")); dvol.commit()
    return res


@app.function(image=cpu_image, volumes={"/data": dvol}, secrets=[DBX], timeout=6 * 3600, cpu=4, memory=16384)
def run_all(table_in: str, out_dir: str, build_id: str, warehouse: str):
    print(VERSION_MARK, "run_all", table_in, out_dir, build_id, flush=True)
    t0 = time.time(); dvol.reload()
    done = f"/data/{build_id}/export_done.json"
    exp = json.load(open(done)) if os.path.exists(done) else export_queue(table_in, build_id, warehouse)
    shards = sorted(int(s) for s in exp["shards"])
    t_export = time.time() - t0
    results, errors = [], []
    for r in Scorer().score.starmap([(build_id, s, out_dir) for s in shards], order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception): errors.append(repr(r)[:500])
        else: results.append(r); print(r, flush=True)
    shard_s = sum(r["total_s"] for r in results)
    return {"shards": len(shards), "shards_done": len(results), "errors": errors, "works": sum(r["works"] for r in results),
            "exported": exp["rows"], "nc": sum(r["nc"] for r in results), "export_s": round(t_export, 1), "shard_seconds": round(shard_s, 1),
            "usd_est": round(shard_s * PRICE_PER_S, 3), "wall_s": round(time.time() - t0, 1), "version": VERSION_MARK,
            "results": sorted(results, key=lambda r: r["shard"])}


@app.function(image=cpu_image, secrets=[TOKEN])
@modal.asgi_app()
def api():
    """POST /start {table_in, out_dir, build_id, warehouse?} -> {call_id}; POST /status {call_id} -> {state: running | done | failed,
    result | error}. Bearer token = TOPICS_NIGHTLY_TOKEN. Every field is validated: table_in and warehouse go into SQL, out_dir into a URL,
    build_id into a Volume path."""
    import hmac
    from fastapi import FastAPI, HTTPException, Request
    web = FastAPI()

    def auth(request):
        got = request.headers.get("authorization", "")
        if not hmac.compare_digest(got.encode(), f"Bearer {os.environ['TOPICS_NIGHTLY_TOKEN']}".encode()):
            raise HTTPException(status_code=401, detail="unauthorized")

    @web.post("/start")
    async def start(request: Request):
        auth(request)
        b = await request.json()
        table_in, out_dir = str(b.get("table_in", "")), str(b.get("out_dir", "")).rstrip("/")
        build_id, warehouse = str(b.get("build_id", "")), str(b.get("warehouse", DEFAULT_WAREHOUSE))
        if not TABLE_RX.match(table_in): raise HTTPException(400, "table_in must be openalex.works.* or openalex.transient.*")
        if not OUT_RX.match(out_dir) or ".." in out_dir: raise HTTPException(400, "out_dir must be a folder under /Volumes/openalex/<schema>/<volume>/")
        if not BUILD_RX.match(build_id) or not WH_RX.match(warehouse): raise HTTPException(400, "build_id / warehouse not allowed")
        call = run_all.spawn(table_in, out_dir, build_id, warehouse)
        return {"call_id": call.object_id, "version": VERSION_MARK}

    @web.post("/status")
    async def status(request: Request):
        auth(request)
        call_id = str((await request.json()).get("call_id", ""))
        if not re.match(r"^fc-[A-Za-z0-9]{10,40}$", call_id): raise HTTPException(400, "bad call_id")
        fc = modal.FunctionCall.from_id(call_id)
        try:
            return {"state": "done", "result": fc.get(timeout=0)}
        except TimeoutError:
            return {"state": "running"}
        except Exception as e:   # the call failed or was cancelled
            return {"state": "failed", "error": repr(e)[:1000]}

    return web
