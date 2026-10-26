"""Location -> work matcher, Qwen stage on Modal (oxjob #1668). The GPU side of the Databricks job "Match Locations To Works"
(jobs/match_locations_to_works_shadow.yaml): its score_qwen task POSTs the night's prompt table here and polls. The same pattern as
notebooks/topics/nightly/modal_topics_nightly.py.

- `api` (bearer token): POST /start validates the request, spawns `run_all` and returns its call id; POST /status says running /
  done (with per-shard results) / failed.
- `run_all`: exports the prompt table once through the Databricks SQL statement API (EXTERNAL_LINKS) into one file per shard on
  the Modal Volume `location-work-matcher` (/data/<build_id>/in/<shard>.jsonl.gz), then maps `Scorer.score` over the shards.
- `Scorer`: Qwen/Qwen3.5-9B + LoRA adapter ep2_9b (oxjob #1668: 2 epochs on 37,828 Opus pick-one records; bind at p >= 0.887),
  merged, bf16, HF transformers, one GPU per container. A row = the record and its shortlist (prefix) + one slot per candidate
  ("c<i> =>" then " ?"); p = softmax over the ' yes' / ' no' logits one position before the slot. One forward pass per record.
  Output parquet per shard: seed_id BIGINT, cid BIGINT, p DOUBLE, shard STRING, PUT to the Unity Catalog Volume.

Volumes: `oxjob1668-qwen` at /vol (adapter models/ep2_9b, read only); `location-work-matcher` at /data (exported prompt shards).
Secrets: `location-work-matcher-token` (LOCATION_MATCH_TOKEN; the same value is in Databricks secret scope `modal`, key
`location_matcher_token`), `oxjob-databricks` (DATABRICKS_HOST / DATABRICKS_TOKEN: reads the prompt table, writes the UC Volume).
Deploy from a checkout:  modal deploy notebooks/location_matching/modal_location_matcher.py"""
import gzip
import json
import os
import re
import time

import modal

APP = "location-work-matcher"
VERSION_MARK = "location-work-matcher v1 2026-10-26"
BASE, ADAPTER = "Qwen/Qwen3.5-9B", "/vol/models/ep2_9b"
app = modal.App(APP)
mvol = modal.Volume.from_name("oxjob1668-qwen")
dvol = modal.Volume.from_name("location-work-matcher", create_if_missing=True, version=2)
cpu_image = modal.Image.debian_slim(python_version="3.12").pip_install("httpx", "fastapi[standard]")
gpu_image = (modal.Image.debian_slim(python_version="3.12")
             .pip_install("torch==2.14.1", "triton>=3.7.1", "transformers==5.18.0", "peft==0.21.1", "accelerate", "huggingface_hub[hf_xet]",
                          "flash-linear-attention", "numpy", "pillow", "torchvision", "httpx", "pyarrow")   # = the oxjob1668-qwen image
             .env({"HF_HOME": "/root/hf", "TOKENIZERS_PARALLELISM": "false", "PYTHONUNBUFFERED": "1"}))
TOKEN = modal.Secret.from_name("location-work-matcher-token")
DBX = modal.Secret.from_name("oxjob-databricks")

MAXC = 8                  # max GPU containers
MAXLEN = 12000            # tokens per record (training cap)
DEFAULT_WAREHOUSE = "3996dc0a9b183ce3"
PRICE_PER_S = (4.54 + 8 * 0.0473 + 96 * 0.008) / 3600   # H200 + 8 cpu + 96 GiB, $/container-second (Modal billing is the truth)
TABLE_RX = re.compile(r"^openalex\.(works|transient)\.[A-Za-z0-9_]{1,120}$")
OUT_RX = re.compile(r"^/Volumes/openalex/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.=-]+){1,6}$")
WH_RX = re.compile(r"^[0-9a-f]{16}$")
BUILD_RX = re.compile(r"^[0-9A-Za-z_-]{6,40}$")


def ids_for(tok, row, maxlen=MAXLEN):   # = oxjob #1668 qwen/common.py ids_for
    T = lambda s: tok(s, add_special_tokens=False)["input_ids"]
    q, yes, no = T(" ?"), T(" yes"), T(" no")
    assert len(q) == len(yes) == len(no) == 1, (q, yes, no)
    ids = T(row["prefix"]); slots = []
    for s in row["seats"]:
        t = T(s["line"] + " =>")
        if len(ids) + len(t) + 2 > maxlen: break
        ids += t; slots.append((len(ids), s["key"])); ids += q + T("\n")
    return ids, slots, yes[0], no[0]


def _dbx_host():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    return host if host.startswith("http") else "https://" + host


@app.cls(image=gpu_image, gpu=["H200", "H100"], volumes={"/vol": mvol, "/data": dvol}, secrets=[DBX], timeout=3600, startup_timeout=1200,
         cpu=8, memory=96 * 1024, max_containers=MAXC, scaledown_window=120, retries=modal.Retries(max_retries=2, initial_delay=10.0))
class Scorer:
    @modal.enter()
    def load(self):   # = oxjob #1668 qwen/score.py
        import torch
        from huggingface_hub import snapshot_download
        from peft import PeftModel
        from transformers import AutoModelForImageTextToText, AutoTokenizer
        print(VERSION_MARK, "scorer load", flush=True)
        snapshot_download(BASE, max_workers=32)
        self.tok = AutoTokenizer.from_pretrained(BASE)
        model = AutoModelForImageTextToText.from_pretrained(BASE, dtype=torch.bfloat16, attn_implementation="sdpa").to("cuda")
        self.model = PeftModel.from_pretrained(model, ADAPTER).merge_and_unload().eval()

    @modal.method()
    def score(self, build_id: str, shard: int, out_dir: str):
        """Score /data/<build_id>/in/<shard>.jsonl.gz and PUT <out_dir>/<shard>.parquet to the UC Volume."""
        import io, httpx, torch, pyarrow as pa, pyarrow.parquet as pq
        t0 = time.time(); dvol.reload()
        rows = [json.loads(l) for l in gzip.open(f"/data/{build_id}/in/{shard:04d}.jsonl.gz", "rt")]
        seeds, cids, ps = [], [], []
        with torch.no_grad():
            for r in rows:
                ids, slots, Y, N = ids_for(self.tok, r)
                keep = torch.tensor([pos - 1 for pos, _ in slots], device="cuda")
                lg = self.model(input_ids=torch.tensor([ids], device="cuda"), logits_to_keep=keep).logits[0][:, [Y, N]].float()
                for (_, key), two in zip(slots, lg):
                    seeds.append(int(r["seed_id"])); cids.append(int(key)); ps.append(torch.softmax(two, -1)[0].item())
        t_gpu = time.time() - t0
        sh = f"{shard:04d}"
        t = pa.table({"seed_id": pa.array(seeds, pa.int64()), "cid": pa.array(cids, pa.int64()), "p": pa.array(ps, pa.float64()),
                      "shard": pa.array([sh] * len(seeds), pa.string())})
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
        return {"shard": shard, "records": len(rows), "pairs": len(seeds), "gpu_s": round(t_gpu, 1), "total_s": round(time.time() - t0, 1),
                "rps": round(len(rows) / max(t_gpu, 1e-6), 2), "bytes": len(body)}


def export_rows(table_in, build_id, warehouse):
    """One statement over the whole prompt table (EXTERNAL_LINKS), rows streamed into one gzip file per shard on /data/<build_id>/in/."""
    import httpx
    from concurrent.futures import ThreadPoolExecutor
    host = _dbx_host(); H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    d = f"/data/{build_id}/in"; os.makedirs(d, exist_ok=True)
    t0 = time.time()
    st = httpx.post(f"{host}/api/2.0/sql/statements", headers=H, timeout=120, json={
        "warehouse_id": warehouse, "wait_timeout": "0s", "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY",
        "statement": f"SELECT shard, seed_id, prefix, seats FROM {table_in}"}).json()
    if "statement_id" not in st: raise RuntimeError(f"statement not accepted: {json.dumps(st)[:500]}")
    sid = st["statement_id"]; print("export statement", sid, flush=True)
    while st["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - t0 > 1800:
            httpx.post(f"{host}/api/2.0/sql/statements/{sid}/cancel", headers=H, timeout=60); raise TimeoutError("prompt export not done in 30 min")
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
        with ThreadPoolExecutor(4) as ex:
            for rows in ex.map(fetch, range(n_chunks)):
                for shard, seed_id, prefix, seats in rows:
                    s = int(shard)
                    if s not in files: files[s] = gzip.open(f"{d}/{s:04d}.jsonl.gz", "wt", compresslevel=3)
                    files[s].write(json.dumps({"seed_id": seed_id, "prefix": prefix, "seats": json.loads(seats)}, ensure_ascii=False) + "\n")
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
    exp = json.load(open(done)) if os.path.exists(done) else export_rows(table_in, build_id, warehouse)
    shards = sorted(int(s) for s in exp["shards"])
    t_export = time.time() - t0
    results, errors = [], []
    for r in Scorer().score.starmap([(build_id, s, out_dir) for s in shards], order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception): errors.append(repr(r)[:500])
        else: results.append(r); print(r, flush=True)
    shard_s = sum(r["total_s"] for r in results)
    return {"shards": len(shards), "shards_done": len(results), "errors": errors, "records": sum(r["records"] for r in results),
            "exported": exp["rows"], "pairs": sum(r["pairs"] for r in results), "export_s": round(t_export, 1),
            "shard_seconds": round(shard_s, 1), "usd_est": round(shard_s * PRICE_PER_S, 3), "wall_s": round(time.time() - t0, 1),
            "version": VERSION_MARK, "results": sorted(results, key=lambda r: r["shard"])}


@app.function(image=cpu_image, secrets=[TOKEN])
@modal.asgi_app()
def api():
    """POST /start {table_in, out_dir, build_id, warehouse?} -> {call_id}; POST /status {call_id} -> {state: running | done | failed,
    result | error}. Bearer token = LOCATION_MATCH_TOKEN. Every field is validated: table_in and warehouse go into SQL, out_dir into
    a URL, build_id into a Volume path."""
    import hmac
    from fastapi import FastAPI, HTTPException, Request
    web = FastAPI()

    def auth(request):
        got = request.headers.get("authorization", "")
        if not hmac.compare_digest(got.encode(), f"Bearer {os.environ['LOCATION_MATCH_TOKEN']}".encode()):
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
        except Exception as e:
            return {"state": "failed", "error": repr(e)[:1000]}

    return web
