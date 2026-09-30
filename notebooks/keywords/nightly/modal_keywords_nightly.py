"""Keywords nightly tagger on Modal (oxjob #1322). The GPU side of the Databricks job "Keywords nightly" (jobs/keywords_nightly.yaml):
vLLM will not run on Databricks (FIPS), so the job's `tag` task POSTs here and polls.

- `api` (bearer token): POST /start validates the request, spawns `run_all` and returns its call id; POST /status says running / done
  (with the per-shard results) / failed.
- `run_all`: `Tagger.run_shard.map` over the queue's shards; each worker reads its shard from the Databricks queue table through the SQL
  statement API, tags it and PUTs `<out_dir>/<shard>.jsonl.gz` ({id: 'W<n>', keywords, lp, model, shard}) to the Unity Catalog Volume.
- `Tagger`: identical to the v3fix full-corpus retag (oxjobs keyword-tagger-optimization scratch/god/corpus_v3/modal_corpus_v3.py, via the
  2026-09-30 new-works catch-up copy): same student, prompt, 560-token input cut, greedy decoding (temperature 0, max 192 tokens, stop "\n"),
  vLLM settings and per-keyword confidence (mean token log-prob, special tokens skipped). The queue table, output folder and warehouse are
  parameters; nothing else differs. Max 10 GPUs (affiliations / authors work has priority on the shared 50-GPU workspace).

Secrets: `oxjob1322-nightly-token` (KW_NIGHTLY_TOKEN; the same value is in Databricks secret scope `modal`, key `kw1322_nightly_token`),
`oxjob-databricks` (DATABRICKS_HOST / DATABRICKS_TOKEN: the identity the workers read the queue and write the Volume as).
Deploy from a checkout:  modal deploy notebooks/keywords/nightly/modal_keywords_nightly.py
Endpoint: https://wordslikethis--oxjob1322-keywords-nightly-api.modal.run (/start, /status)"""
import json
import os
import re
import time

import modal

app = modal.App("oxjob1322-keywords-nightly")
vol = modal.Volume.from_name("oxjob1322")
image = (modal.Image.from_registry("nvidia/cuda:12.8.1-devel-ubuntu22.04", add_python="3.11").pip_install("vllm>=0.10", "transformers>=4.51,<5.0", "httpx")
         .env({"HF_HOME": "/vol/hf", "CUDA_HOME": "/usr/local/cuda", "VLLM_USE_FLASHINFER_SAMPLER": "0"}))
web_image = modal.Image.debian_slim(python_version="3.11").pip_install("fastapi[standard]", "httpx")
TOKEN = modal.Secret.from_name("oxjob1322-nightly-token")
DBX = modal.Secret.from_name("oxjob-databricks")

DEFAULT_WAREHOUSE = "3996dc0a9b183ce3"   # DEV-WH (X-Large serverless): a shared Medium warehouse queued concurrent shard reads into timeouts (2026-09-27)
PRICE_PER_S = (6.25 + 0.9) / 3600      # B200 + cpu/mem, $/container-second (estimate; Modal's billing report is the truth)
ALLOWED_MODELS = {"/vol/models/student_qwen4b_v3fix"}
TABLE_RX = re.compile(r"^openalex\.(works|transient)\.[A-Za-z0-9_]{1,120}$")
OUT_RX = re.compile(r"^/Volumes/openalex/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.=-]+){1,6}$")
WH_RX = re.compile(r"^[0-9a-f]{16}$")


def user_text(w):   # = god/prompt_v3.user_text (the student's training prompt)
    return "\n".join([f"Title: {w.get('title') or '(none)'}", f"Abstract: {(w.get('abstract') or '(none)')[:6000]}", f"Venue: {w.get('journal') or '(unknown)'}"])


def keyword_lps(tok, token_ids, logprobs, special=frozenset()):
    """Split the generated text on ';' and average the sampled-token log-probs per keyword (separator tokens count toward the keyword they close).
    Special tokens (the <|im_end|> that ends every list) are skipped: vLLM keeps the stop token in token_ids (bug in the first 330 shards, 2026-09-26)."""
    kws, lps, cur, cur_lp = [], [], "", []
    for tid, d in zip(token_ids, logprobs):
        if tid in special: continue
        piece = tok.decode([tid]); lp = d[tid].logprob if d and tid in d else 0.0
        parts = piece.split(";")
        cur += parts[0]; cur_lp.append(lp)
        for p in parts[1:]:
            if cur.strip(): kws.append(cur.strip()); lps.append(round(sum(cur_lp) / len(cur_lp), 4))
            cur, cur_lp = p, []
    if cur.strip() and cur_lp: kws.append(cur.strip()); lps.append(round(sum(cur_lp) / len(cur_lp), 4))
    return kws, lps


@app.cls(image=image, gpu="B200", volumes={"/vol": vol}, secrets=[DBX], timeout=900, memory=65536, cpu=8,
         max_containers=10, scaledown_window=60, retries=3)   # Jason 2026-09-27 22:32: max 10 GPUs so 40 stay free for affiliations / authors work
class Tagger:
    model: str = modal.parameter(default="/vol/models/student_qwen4b_v3fix")
    quant: str = modal.parameter(default="fp8")
    out_dir: str = modal.parameter(default="")      # Unity Catalog Volume folder for this run's shard files
    table_in: str = modal.parameter(default="")     # queue table: shard, id, title, abstract, journal
    warehouse: str = modal.parameter(default=DEFAULT_WAREHOUSE)

    @modal.enter()
    def load(self):
        from vllm import LLM, SamplingParams
        from transformers import AutoTokenizer
        kw = dict(model=self.model, dtype="bfloat16", max_model_len=1024, gpu_memory_utilization=0.9, enable_prefix_caching=False,
                  max_num_seqs=512, max_num_batched_tokens=32768)   # 4B on a 180 GB B200: 256 left decode under-batched; 1024 coincided with hung workers (2026-09-26)
        if self.quant: kw["quantization"] = self.quant
        self.llm = LLM(**kw); self.tok = AutoTokenizer.from_pretrained(self.model); self.special = frozenset(self.tok.all_special_ids)
        self.sp = SamplingParams(temperature=0.0, max_tokens=192, stop=["\n"], logprobs=1)

    def _tag(self, texts, chunk=16384):
        """Chunks keep the GIL-bound tokenise / detokenise steps short so Modal's heartbeat gets through (one 118K-prompt call starved it, 2026-09-26)."""
        out = []
        for i in range(0, len(texts), chunk):
            prompts = [{"prompt_token_ids": self.tok(t + "\nKeywords:", add_special_tokens=False)["input_ids"][:560]} for t in texts[i:i + chunk]]
            out += [keyword_lps(self.tok, o.outputs[0].token_ids, o.outputs[0].logprobs, self.special) for o in self.llm.generate(prompts, self.sp, use_tqdm=False)]
        return out

    @modal.method()
    def run_shard(self, shard: int):
        import httpx, gzip
        t0 = time.time(); host = os.environ["DATABRICKS_HOST"].rstrip("/"); H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
        with httpx.Client(timeout=300, headers=H) as h:
            st = h.post(f"{host}/api/2.0/sql/statements", json={"warehouse_id": self.warehouse, "wait_timeout": "30s", "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY",
                        "statement": f"SELECT id, title, abstract, journal FROM {self.table_in} WHERE shard = {int(shard)}"}).json()
            t_q = time.time()
            while st["status"]["state"] in ("PENDING", "RUNNING"):
                if time.time() - t_q > 300:   # a queued / stuck read: cancel it server-side and fail fast so Modal retries (no silent 900 s timeouts)
                    h.post(f"{host}/api/2.0/sql/statements/{st['statement_id']}/cancel"); raise TimeoutError(f"shard {shard}: statement not done in 300 s")
                time.sleep(3); st = h.get(f"{host}/api/2.0/sql/statements/{st['statement_id']}").json()
            if st["status"]["state"] != "SUCCEEDED": raise RuntimeError(f"shard {shard}: {json.dumps(st['status'])[:400]}")
            cols = [c["name"] for c in st["manifest"]["schema"]["columns"]]; rows = []
            for i in range(st["manifest"].get("total_chunk_count", 0)):
                ch = h.get(f"{host}/api/2.0/sql/statements/{st['statement_id']}/result/chunks/{i}").json()
                for l in ch.get("external_links", []): rows += [dict(zip(cols, r)) for r in httpx.get(l["external_link"], timeout=300).json()]
        t_fetch = time.time() - t0
        out = self._tag([user_text(w) for w in rows]); t_gen = time.time() - t0 - t_fetch
        model = os.path.basename(self.model) + (f"+{self.quant}" if self.quant else "")
        body = gzip.compress("".join(json.dumps({"id": f"W{w['id']}", "keywords": k, "lp": lp, "model": model, "shard": shard}, ensure_ascii=False) + "\n"
                                     for w, (k, lp) in zip(rows, out)).encode(), 6)
        r = httpx.put(f"{host}/api/2.0/fs/files{self.out_dir}/{shard:04d}.jsonl.gz?overwrite=true", headers={**H, "Content-Type": "application/octet-stream"}, content=body, timeout=600)
        if r.status_code >= 300: raise RuntimeError(f"shard {shard}: PUT {r.status_code} {r.text[:300]}")
        return {"shard": shard, "works": len(rows), "fetch_s": round(t_fetch, 1), "gen_s": round(t_gen, 1), "total_s": round(time.time() - t0, 1), "bytes": len(body)}


@app.function(image=web_image, timeout=4 * 3600)
def run_all(table_in: str, out_dir: str, shards: list, model: str, quant: str, warehouse: str):
    t0 = time.time(); results, errors = [], []
    T = Tagger(model=model, quant=quant, out_dir=out_dir, table_in=table_in, warehouse=warehouse)
    for r in T.run_shard.map(shards, order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception): errors.append(repr(r)[:500])
        else: results.append(r)
    return {"shards": len(shards), "shards_done": len(results), "errors": errors, "works": sum(r["works"] for r in results),
            "shard_seconds": round(sum(r["total_s"] for r in results), 1), "usd_est": round(sum(r["total_s"] for r in results) * PRICE_PER_S, 3),
            "wall_s": round(time.time() - t0, 1), "results": sorted(results, key=lambda r: r["shard"])}


@app.function(image=web_image, secrets=[TOKEN])
@modal.asgi_app()
def api():
    """POST /start {table_in, out_dir, shards: [int], model?, quant?, warehouse?} -> {call_id}; POST /status {call_id} -> {state: running | done |
    failed, result | error}. Bearer token = KW_NIGHTLY_TOKEN. Every field is validated: table_in and warehouse go into SQL, out_dir into a URL."""
    import hmac
    from fastapi import FastAPI, HTTPException, Request
    web = FastAPI()

    def auth(request):
        got = request.headers.get("authorization", "")
        if not hmac.compare_digest(got.encode(), f"Bearer {os.environ['KW_NIGHTLY_TOKEN']}".encode()):
            raise HTTPException(status_code=401, detail="unauthorized")

    @web.post("/start")
    async def start(request: Request):
        auth(request)
        b = await request.json()
        table_in, out_dir = str(b.get("table_in", "")), str(b.get("out_dir", "")).rstrip("/")
        model, quant, warehouse = str(b.get("model", "/vol/models/student_qwen4b_v3fix")), str(b.get("quant", "fp8")), str(b.get("warehouse", DEFAULT_WAREHOUSE))
        shards = b.get("shards")
        if not TABLE_RX.match(table_in): raise HTTPException(400, "table_in must be openalex.works.* or openalex.transient.*")
        if not OUT_RX.match(out_dir) or ".." in out_dir: raise HTTPException(400, "out_dir must be a folder under /Volumes/openalex/<schema>/<volume>/")
        if model not in ALLOWED_MODELS or quant not in ("", "fp8") or not WH_RX.match(warehouse): raise HTTPException(400, "model / quant / warehouse not allowed")
        if not isinstance(shards, list) or not shards or len(shards) > 2000 or not all(isinstance(x, int) and 0 <= x < 100000 for x in shards):
            raise HTTPException(400, "shards must be a list of 1..2000 non-negative ints")
        call = run_all.spawn(table_in, out_dir, sorted(set(shards)), model, quant, warehouse)
        return {"call_id": call.object_id, "shards": len(set(shards))}

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
