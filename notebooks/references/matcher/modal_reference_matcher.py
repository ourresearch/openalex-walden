"""Reference matcher, GPU stages on Modal (oxjob #2000; nightly from #2296). The Databricks job "Match References"
(jobs/match_references.yaml, notebook MatchReferences) POSTs here and polls, as the location matcher does
(notebooks/location_matching/modal_location_matcher.py).

- `api` (bearer token LOCATION_MATCH_TOKEN): POST /start {stage: search | reader, table_in, out_dir, build_id, warehouse?}
  spawns `run_search` or `run_reader` and returns its call id; POST /status {call_id} says running / done (result) / failed.
- `run_search`: the night's reference vectors (table_in: text_hash, v) -> the best 30 works each by inner product over the
  reference-style work vectors (openalex.vector_search.work_reference_style_embeddings_qwen3), searching the 256 nearest of
  its 16,384 lists (#2000's corpus run: nprobe 256, pool 30). The lists live on the Modal Volume as built on 13 Nov 2026
  (431.6M works) or as last rebuilt; works embedded since then (new or changed) are exported each night into the delta and
  replace their old rows in the lists they belong to, so the search always sees the current vectors. A night that sees
  more than REBUILD_AT re-embedded works starts `rebuild_lists` in the background (a fresh copy, ~20 min, then the search
  switches to it through /data/current.json). Output parquet
  (text_hash, cand, score, rank, probe_rank) PUT to out_dir on a Unity Catalog Volume.
- `run_reader`: the reader (Qwen/Qwen3.5-2B + LoRA r2b_c5, vLLM) on the night's prompt table (table_in: shard, seed_id, prefix,
  seats): p(yes) per candidate slot, one parquet per shard (seed_id, cid, p) to out_dir. #2000's corpus run's scoring, unchanged.

Volumes: `oxjob2000-refsearch` at /data (index, delta, nightly queries), `oxjob2000-qwen` at /vol (adapter models/r2b_c5).
Secrets: `location-work-matcher-token` (LOCATION_MATCH_TOKEN, shared with the location matcher's app: the same Databricks jobs
call both; the value is in Databricks secret scope `modal`, key `location_matcher_token`), `oxjob-databricks` (DATABRICKS_HOST /
DATABRICKS_TOKEN: reads tables, writes the UC Volume).
Deploy from a checkout:  modal deploy notebooks/references/matcher/modal_reference_matcher.py"""
import io
import json
import math
import os
import queue
import re
import subprocess
import sys
import threading
import time

import modal

APP = "reference-matcher"
VERSION_MARK = "reference-matcher v1 2026-11-16 (nprobe 256, pool 30, delta lists; reader r2b_c5)"
app = modal.App(APP)
svol = modal.Volume.from_name("oxjob2000-refsearch", version=2)
qvol = modal.Volume.from_name("oxjob2000-qwen")
DBX = modal.Secret.from_name("oxjob-databricks")
TOKEN = modal.Secret.from_name("location-work-matcher-token")
cpu_image = modal.Image.debian_slim(python_version="3.12").pip_install("httpx", "pyarrow", "numpy", "fastapi[standard]")
gpu_image = modal.Image.debian_slim(python_version="3.12").pip_install("torch==2.14.1", "numpy", "pyarrow", "httpx")
reader_image = (modal.Image.from_registry("nvidia/cuda:12.9.1-devel-ubuntu22.04", add_python="3.12")
                .pip_install("vllm==0.30.0", "peft", "huggingface_hub[hf_xet]", "httpx", "pyarrow")
                .env({"HF_HOME": "/root/hf", "TOKENIZERS_PARALLELISM": "false", "PYTHONUNBUFFERED": "1", "HF_XET_HIGH_PERFORMANCE": "1",
                      "VLLM_USE_FLASHINFER_SAMPLER": "0", "CUDA_HOME": "/usr/local/cuda"}))

INDEX = "openalex.vector_search.work_reference_style_embeddings_qwen3"
D, N_LG, LISTS_PER_GROUP = 1024, 1024, 16
NPROBE, POOL, SUB = 256, 30, 1_000_000
# Which lists and delta the search reads: /data/current.json, written by rebuild_lists. Without it, the lists #2000 built,
# exported 13 Nov 2026 14:40-15:21 UTC; works embedded from the start of that day are re-read into the delta (an overlap is
# harmless: a delta row replaces the list row of the same work).
CURRENT = "/data/current.json"
FIRST = {"index": "/data/index", "delta": "/data/delta", "delta_start": "2026-11-13 00:00:00"}
DELTA_MARGIN_H = 6           # each night re-reads the last hours before the previous night's newest row (late commits)
REBUILD_AT = 20_000_000      # a night that sees more re-embedded works than this starts a rebuild of the lists (~10 days)
MAX_DELTA_ROWS = 40_000_000  # past this the search refuses (memory): the rebuild has not happened
ADAPTER, BASE = "r2b_c5", "Qwen/Qwen3.5-2B"
READER_GPUS = 8              # leave room on the shared Modal GPU cap for other production jobs
DEFAULT_WAREHOUSE = "3996dc0a9b183ce3"
TABLE_RX = re.compile(r"^openalex\.(works|transient)\.[A-Za-z0-9_]{1,120}$")
OUT_RX = re.compile(r"^/Volumes/openalex/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.=-]+){1,6}$")
WH_RX = re.compile(r"^[0-9a-f]{16}$")
BUILD_RX = re.compile(r"^[0-9A-Za-z_-]{6,40}$")


# ---------- Databricks access (SQL statement API + Files API), as #2000's refsearch_modal.py ----------

def _host():
    h = os.environ["DATABRICKS_HOST"].rstrip("/")
    return h if h.startswith("http") else "https://" + h


def _H():
    return {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}


def _statement(statement, warehouse, fmt, timeout=3600):
    import httpx
    t0 = time.time()
    st = httpx.post(f"{_host()}/api/2.0/sql/statements", headers=_H(), timeout=120, json={
        "warehouse_id": warehouse, "wait_timeout": "0s", "disposition": "EXTERNAL_LINKS", "format": fmt, "statement": statement}).json()
    if "statement_id" not in st:
        raise RuntimeError(f"statement not accepted: {json.dumps(st)[:500]}")
    sid = st["statement_id"]
    while st["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - t0 > timeout:
            httpx.post(f"{_host()}/api/2.0/sql/statements/{sid}/cancel", headers=_H(), timeout=60)
            raise TimeoutError(sid)
        time.sleep(3)
        st = httpx.get(f"{_host()}/api/2.0/sql/statements/{sid}", headers=_H(), timeout=120).json()
    if st["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(json.dumps(st["status"])[:1000])
    return sid, st


def _chunk_links(sid, k):
    import httpx
    return httpx.get(f"{_host()}/api/2.0/sql/statements/{sid}/result/chunks/{k}", headers=_H(), timeout=120).json()["external_links"]


def sql_arrow(statement, warehouse):
    """Yield one pyarrow Table per result chunk (EXTERNAL_LINKS, ARROW_STREAM)."""
    import httpx, pyarrow as pa
    from concurrent.futures import ThreadPoolExecutor
    sid, st = _statement(statement, warehouse, "ARROW_STREAM")
    n_chunks = int(st["manifest"].get("total_chunk_count", 0))

    def fetch(k):
        for att in range(6):
            try:
                return pa.concat_tables([pa.ipc.open_stream(httpx.get(l["external_link"], timeout=900).content).read_all()
                                         for l in _chunk_links(sid, k)])
            except Exception as e:
                print("chunk retry", k, repr(e)[:200], flush=True)
                time.sleep(10 * (att + 1))
        raise RuntimeError(f"chunk {k} failed")

    with ThreadPoolExecutor(16) as ex:
        yield from ex.map(fetch, range(n_chunks))


def sql_rows(statement, warehouse):
    """All rows of a statement (EXTERNAL_LINKS, JSON_ARRAY)."""
    import httpx
    sid, st = _statement(statement, warehouse, "JSON_ARRAY")
    out = []
    for k in range(int(st["manifest"].get("total_chunk_count", 0))):
        for att in range(6):
            try:
                part = []
                for l in _chunk_links(sid, k):
                    part += httpx.get(l["external_link"], timeout=600).json()
                out += part
                break
            except Exception as e:
                print("chunk retry", k, repr(e)[:200], flush=True)
                time.sleep(10 * (att + 1))
        else:
            raise RuntimeError(f"chunk {k} failed")
    return out


def put_file(path, body):
    import httpx
    for att in range(5):
        try:
            r = httpx.put(f"{_host()}/api/2.0/fs/files{path}", params={"overwrite": "true"}, content=body,
                          headers={**_H(), "Content-Type": "application/octet-stream"}, timeout=900)
            r.raise_for_status()
            return
        except Exception as e:
            print("PUT retry", path, repr(e)[:200], flush=True)
            time.sleep(10 * (att + 1))
    raise RuntimeError(f"PUT failed: {path}")


def vec16(col):
    """BINARY(2048) column -> (n, 1024) float16."""
    import numpy as np, pyarrow as pa
    col = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    n = len(col)
    if col.null_count:
        raise ValueError("null vector")
    off = np.frombuffer(col.buffers()[1], dtype=np.int32 if col.type == pa.binary() else np.int64)[col.offset: col.offset + n + 1]
    if not (np.diff(off) == 2 * D).all():
        raise ValueError("vector not 2048 bytes")
    return np.frombuffer(col.buffers()[2], dtype=np.uint8)[off[0]: off[-1]].view(np.float16).reshape(n, D)


def npy(path):
    """np.load through readinto (releases the GIL, so reader threads overlap)."""
    import numpy as np
    with open(path, "rb") as f:
        ver = np.lib.format.read_magic(f)
        shape, fortran, dtype = (np.lib.format.read_array_header_1_0 if ver == (1, 0) else np.lib.format.read_array_header_2_0)(f)
        a = np.empty(shape, dtype)
        mv, got = memoryview(a).cast("B"), 0
        while got < mv.nbytes:
            n = f.readinto(mv[got:])
            if not n:
                raise IOError(f"short read {path}")
            got += n
    return a


# ---------- search ----------

@app.function(image=cpu_image, volumes={"/data": svol}, secrets=[DBX], timeout=3 * 3600, cpu=4, memory=64 * 1024)
def export_queries(table: str, build_id: str, warehouse: str):
    """The night's reference vectors -> /data/nightly/<build_id>/q.h.npy (text_hash) + q.v.npy (float16)."""
    import numpy as np
    t0 = time.time()
    hs, vs = [], []
    for t in sql_arrow(f"SELECT text_hash, v FROM {table}", warehouse):
        for b in t.to_batches():
            hs.append(b.column("text_hash").to_numpy(zero_copy_only=False).astype(np.int64))
            vs.append(np.array(vec16(b.column("v"))))
    h = np.concatenate(hs) if hs else np.zeros(0, np.int64)
    V = np.concatenate(vs) if vs else np.zeros((0, D), np.float16)
    base = f"/data/nightly/{build_id}"
    os.makedirs(base, exist_ok=True)
    np.save(f"{base}/q.h.npy", h)
    np.save(f"{base}/q.v.npy", V)
    svol.commit()
    return {"queries": int(len(h)), "seconds": round(time.time() - t0, 1)}


@app.function(image=cpu_image, volumes={"/data": svol}, secrets=[DBX], timeout=3 * 3600, cpu=4, memory=96 * 1024, nonpreemptible=True)
def refresh_delta(warehouse: str):
    """Works embedded since the last export (new or changed vectors) -> one more delta file; manifest /data/delta/manifest.json."""
    import numpy as np
    svol.reload()
    t0 = time.time()
    cur = _current()
    dd = cur["delta"]
    os.makedirs(dd, exist_ok=True)
    mpath = f"{dd}/manifest.json"
    man = json.load(open(mpath)) if os.path.exists(mpath) else {"newest": None, "files": []}
    since = (f"timestamp'{man['newest']}' - INTERVAL {DELTA_MARGIN_H} HOURS" if man["newest"] else f"timestamp'{cur['delta_start']}'")
    ids, lst, vs, newest = [], [], [], None
    for t in sql_arrow(f"SELECT work_id, list_id, v, CAST(embedded_at AS STRING) e FROM {INDEX} WHERE embedded_at >= {since}", warehouse):
        for b in t.to_batches():
            ids.append(b.column("work_id").to_numpy(zero_copy_only=False).astype(np.int64))
            lst.append(b.column("list_id").to_numpy(zero_copy_only=False).astype(np.int32))
            vs.append(np.array(vec16(b.column("v"))))
            e = max(b.column("e").to_pylist())
            newest = e if newest is None or e > newest else newest
    n = int(sum(len(x) for x in ids))
    if n:
        name = f"d{len(man['files']):04d}"
        np.save(f"{dd}/{name}.ids.npy", np.concatenate(ids))
        np.save(f"{dd}/{name}.lst.npy", np.concatenate(lst))
        np.save(f"{dd}/{name}.v.npy", np.concatenate(vs))
        man["files"].append({"name": name, "rows": n, "since": since, "newest": newest})
        man["newest"] = newest
        json.dump(man, open(mpath, "w"))
        svol.commit()
    return {"exported": n, "files": len(man["files"]), "newest": man["newest"], "lists": cur["index"], "seconds": round(time.time() - t0, 1)}


def _current():
    return json.load(open(CURRENT)) if os.path.exists(CURRENT) else dict(FIRST)


def _load_delta(dd):
    """Every delta file, the latest row per work kept -> (ids sorted, lst, v) and the rows grouped by list group."""
    import numpy as np
    mpath = f"{dd}/manifest.json"
    man = json.load(open(mpath)) if os.path.exists(mpath) else {"files": []}
    if not man["files"]:
        return np.zeros(0, np.int64), {}, 0
    ids = np.concatenate([npy(f"{dd}/{f['name']}.ids.npy") for f in man["files"]])
    lst = np.concatenate([npy(f"{dd}/{f['name']}.lst.npy") for f in man["files"]])
    V = np.concatenate([npy(f"{dd}/{f['name']}.v.npy") for f in man["files"]])
    # later files win: unique over the reversed order keeps each work's newest row
    _, first = np.unique(ids[::-1], return_index=True)
    keep = np.sort(len(ids) - 1 - first)
    ids, lst, V = ids[keep], lst[keep], V[keep]
    lg = lst // LISTS_PER_GROUP
    groups = {}
    order = np.argsort(lg, kind="stable")
    bounds = np.searchsorted(lg[order], np.arange(N_LG + 1))
    for g in range(N_LG):
        sel = order[bounds[g]:bounds[g + 1]]
        if len(sel):
            sel = sel[np.argsort(lst[sel], kind="stable")]
            groups[g] = (ids[sel], lst[sel], np.ascontiguousarray(V[sel]))
    return np.sort(ids), groups, len(ids)


@app.function(image=gpu_image, gpu="H100", volumes={"/data": svol}, secrets=[DBX], timeout=4 * 3600, cpu=16, memory=200 * 1024)
def search_gpu(build_id: str, out_dir: str, nprobe: int = NPROBE, pool: int = POOL, readers: int = 16):
    """Best `pool` works per query. The lists stream past the GPU once (#2000's search_chunk loop); in each list group the
    built rows of works re-embedded since are masked out (score -inf) and the group's delta rows are scanned as a second block.
    Nothing is copied on the CPU: the cost stays reading the lists (~0.9 TB)."""
    import numpy as np, pyarrow as pa, pyarrow.parquet as pq, torch
    svol.reload()
    T = {"start": time.time()}
    dev = "cuda"
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction = False
    cur = _current()
    ix = cur["index"]
    cent = torch.from_numpy(np.load(f"{ix}/centroids.npy")).float().to(dev)
    L = cent.shape[0]
    H = npy(f"/data/nightly/{build_id}/q.h.npy")
    V = npy(f"/data/nightly/{build_id}/q.v.npy")
    n = len(H)
    delta_ids, delta_groups, n_delta = _load_delta(cur["delta"])
    if n_delta > MAX_DELTA_ROWS:
        raise RuntimeError(f"{n_delta:,} re-embedded works since the lists were built (> {MAX_DELTA_ROWS:,}): rebuild the lists")
    T["load"] = time.time()
    subs = []
    for a in range(0, n, SUB):
        Q = torch.from_numpy(V[a:a + SUB]).to(dev)
        m = Q.shape[0]
        probe = torch.empty((m, nprobe), dtype=torch.int64, device=dev)
        for i in range(0, m, 32768):
            probe[i:i + 32768] = torch.topk(Q[i:i + 32768].float() @ cent.T, nprobe, dim=1, sorted=True).indices
        flat = probe.flatten()
        del probe
        order = torch.argsort(flat, stable=True)
        qix = (order // nprobe).to(torch.int32)
        rix = (order % nprobe).to(torch.int16)
        ptr = torch.searchsorted(flat[order], torch.arange(L + 1, device=dev)).cpu().numpy()
        del flat, order
        subs.append({"a": a, "Q": Q, "qix": qix, "rix": rix, "ptr": ptr,
                     "bs": torch.full((m, pool), float("-inf"), dtype=torch.float32, device=dev),
                     "bi": torch.full((m, pool), -1, dtype=torch.int64, device=dev),
                     "br": torch.zeros((m, pool), dtype=torch.int16, device=dev)})
    del V
    T["probes"] = time.time()
    qq = queue.Queue(maxsize=readers)

    def load(g):
        ids, lst, Vg = npy(f"{ix}/lg{g:04d}.ids.npy"), npy(f"{ix}/lg{g:04d}.lst.npy"), npy(f"{ix}/lg{g:04d}.v.npy")
        # works re-embedded since the lists were built: their rows here are stale
        stale = np.isin(ids, delta_ids, assume_unique=True) if len(delta_ids) else None
        blocks = [(ids, lst, Vg, stale if stale is not None and stale.any() else None)]
        if g in delta_groups:
            di, dl, dv = delta_groups[g]
            blocks.append((di, dl, dv, None))
        return g, blocks

    err = []

    def reader():
        from concurrent.futures import ThreadPoolExecutor, as_completed
        try:
            with ThreadPoolExecutor(readers) as ex:
                it = iter(range(N_LG))
                pending = {ex.submit(load, g) for g in [next(it) for _ in range(readers)]}
                while pending:
                    d = next(as_completed(pending))
                    pending.remove(d)
                    qq.put(d.result())
                    g = next(it, None)
                    if g is not None:
                        pending.add(ex.submit(load, g))
        except Exception as e:
            err.append(repr(e))
        qq.put(None)

    threading.Thread(target=reader, daemon=True).start()
    pairs, rows_seen, LIM = 0, 0, 1 << 28
    while True:
        item = qq.get()
        if item is None:
            break
        g, blocks = item
        for ids, lst, Vg, stale in blocks:
            if len(ids) == 0:
                continue
            rows_seen += len(ids) - (int(stale.sum()) if stale is not None else 0)
            Vt = torch.from_numpy(Vg).to(dev)
            It = torch.from_numpy(ids).to(dev)
            St = torch.from_numpy(stale).to(dev) if stale is not None else None
            starts = np.flatnonzero(np.r_[True, lst[1:] != lst[:-1]])
            for a, e in zip(starts, np.r_[starts[1:], len(lst)]):
                l, nl = int(lst[a]), int(e - a)
                kk = min(pool, nl)
                Vl, Il = Vt[a:e], It[a:e]
                Ml = St[a:e] if St is not None and bool(stale[a:e].any()) else None
                for s in subs:
                    q0, q1 = int(s["ptr"][l]), int(s["ptr"][l + 1])
                    if q1 == q0:
                        continue
                    bq = max(1, min(q1 - q0, LIM // nl))
                    for b in range(q0, q1, bq):
                        qb = s["qix"][b:min(b + bq, q1)].long()
                        rb = s["rix"][b:min(b + bq, q1)]
                        S = s["Q"][qb] @ Vl.T
                        if Ml is not None:
                            S.masked_fill_(Ml[None, :], float("-inf"))
                        tv, ti = torch.topk(S, kk, dim=1, sorted=False)
                        pairs += S.numel()
                        cs = torch.cat([s["bs"][qb], tv.float()], 1)
                        ci = torch.cat([s["bi"][qb], Il[ti]], 1)
                        cr = torch.cat([s["br"][qb], rb[:, None].expand(-1, kk)], 1)
                        top = torch.topk(cs, pool, dim=1, sorted=False).indices
                        s["bs"][qb] = cs.gather(1, top)
                        s["bi"][qb] = ci.gather(1, top)
                        s["br"][qb] = cr.gather(1, top)
    if err:
        raise RuntimeError(f"list reader failed: {err[0][:500]}")
    torch.cuda.synchronize()
    T["scan"] = time.time()
    rows = 0
    for j, s in enumerate(subs):
        sc, o = torch.sort(s["bs"], dim=1, descending=True)
        bi, br = s["bi"].gather(1, o).cpu().numpy(), s["br"].gather(1, o).cpu().numpy()
        sc = sc.cpu().numpy()
        keep = (bi >= 0) & np.isfinite(sc)
        m = sc.shape[0]
        th = np.broadcast_to(H[s["a"]:s["a"] + m][:, None], sc.shape)
        rk = np.broadcast_to(np.arange(1, pool + 1, dtype=np.int16)[None, :], sc.shape)
        tab = pa.table({"text_hash": th[keep], "cand": bi[keep], "score": sc[keep], "rank": rk[keep], "probe_rank": br[keep]})
        buf = io.BytesIO()
        pq.write_table(tab, buf, compression="zstd")
        put_file(f"{out_dir}/part-{j:03d}.parquet", buf.getvalue())
        rows += tab.num_rows
    T["write"] = time.time()
    st = {"queries": int(n), "rows": int(rows), "lists": ix, "delta_works": int(n_delta), "list_rows_seen": int(rows_seen), "pairs_scored": int(pairs),
          "gpu": torch.cuda.get_device_name(0), "nprobe": nprobe, "pool": pool, "parts": len(subs)}
    prev = T["start"]
    for key in ("load", "probes", "scan", "write"):
        st[f"{key}_s"] = round(T[key] - prev, 1)
        prev = T[key]
    st["total_s"] = round(T["write"] - T["start"], 1)
    print(st, flush=True)
    return st


# the two orchestrators are non-preemptible: a restarted one would start its GPU stage a second time
@app.function(image=cpu_image, volumes={"/data": svol}, secrets=[DBX], timeout=5 * 3600, cpu=1, memory=2048, nonpreemptible=True)
def run_search(table_in: str, out_dir: str, build_id: str, warehouse: str):
    t0 = time.time()
    dq = refresh_delta.spawn(warehouse)
    q = export_queries.remote(table_in, build_id, warehouse)
    d = dq.get()
    if q["queries"] == 0:
        return {"stage": "search", "queries": 0, "delta": d, "rows": 0, "wall_s": round(time.time() - t0, 1), "version": VERSION_MARK}
    s = search_gpu.remote(build_id, out_dir)
    if s["delta_works"] > REBUILD_AT:
        s["rebuild"] = _start_rebuild(warehouse)
    # container-seconds x list price (H100 $3.95/h + 16 cpu + 160 GiB, plus the two CPU exports); Modal billing is the truth
    usd = s["total_s"] * (3.95 + 16 * 0.0473 + 200 * 0.008) / 3600 + (q["seconds"] + d["seconds"]) * (4 * 0.0473 + 80 * 0.008) / 3600
    return {"stage": "search", "queries": q["queries"], "export_s": q["seconds"], "delta": d, **s, "usd_est": round(usd, 2),
            "wall_s": round(time.time() - t0, 1), "version": VERSION_MARK}


# ---------- rebuilding the lists ----------

@app.function(image=cpu_image, volumes={"/data": svol}, secrets=[DBX], timeout=3 * 3600, cpu=4, memory=40 * 1024,
              max_containers=48, retries=modal.Retries(max_retries=2, initial_delay=30.0))
def index_range(out: str, lo: int, hi: int, warehouse: str):
    """List groups [lo, hi) of the works' vectors -> one set of files per group in `out`, rows sorted by list (#2000's index_range)."""
    import numpy as np
    svol.reload()
    todo = [g for g in range(lo, hi) if not os.path.exists(f"{out}/lg{g:04d}.v.npy")]
    if not todo:
        return {"lo": lo, "hi": hi, "skipped": True}
    t0 = time.time()
    parts = {}
    for t in sql_arrow(f"SELECT lg, list_id, work_id, v FROM {INDEX} WHERE lg >= {min(todo)} AND lg <= {max(todo)}", warehouse):
        for b in t.to_batches():
            lgs = b.column("lg").to_numpy(zero_copy_only=False)
            lst = b.column("list_id").to_numpy(zero_copy_only=False).astype(np.int32)
            ids = b.column("work_id").to_numpy(zero_copy_only=False).astype(np.int64)
            V = vec16(b.column("v"))
            for g in np.unique(lgs):
                m = lgs == g
                parts.setdefault(int(g), []).append((lst[m], ids[m], np.array(V[m])))
    rows = 0
    for g in todo:
        got = parts.pop(g, [])
        lst = np.concatenate([x[0] for x in got]) if got else np.zeros(0, np.int32)
        ids = np.concatenate([x[1] for x in got]) if got else np.zeros(0, np.int64)
        V = np.concatenate([x[2] for x in got]) if got else np.zeros((0, D), np.float16)
        o = np.argsort(lst, kind="stable")
        np.save(f"{out}/lg{g:04d}.ids.npy", ids[o])
        np.save(f"{out}/lg{g:04d}.lst.npy", lst[o])
        np.save(f"{out}/lg{g:04d}.v.npy", np.ascontiguousarray(V[o]))
        rows += len(ids)
    svol.commit()
    return {"lo": lo, "hi": hi, "rows": rows, "seconds": round(time.time() - t0, 1)}


@app.function(image=cpu_image, volumes={"/data": svol}, secrets=[DBX], timeout=8 * 3600, cpu=2, memory=4096)
def rebuild_lists(warehouse: str):
    """A fresh copy of the lists from the works' vectors as they are now, then /data/current.json points at it with an empty
    delta starting a few hours before the copy began. The previous lists stay until the next rebuild removes them."""
    import shutil, httpx
    svol.reload()
    start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - DELTA_MARGIN_H * 3600))
    stamp = time.strftime("%Y%m%d%H%M", time.gmtime())
    out, dd = f"/data/index_{stamp}", f"/data/delta_{stamp}"
    os.makedirs(out, exist_ok=True)
    r = httpx.get(f"{_host()}/api/2.0/fs/files/Volumes/openalex/vector_search/work_reference_style_index/centroids.npy", headers=_H(), timeout=600)
    r.raise_for_status()
    open(f"{out}/centroids.npy", "wb").write(r.content)
    svol.commit()
    res = list(index_range.starmap([(out, a, min(a + 8, N_LG), warehouse) for a in range(0, N_LG, 8)], order_outputs=False))
    svol.reload()
    missing = [g for g in range(N_LG) if not os.path.exists(f"{out}/lg{g:04d}.v.npy")]
    if missing:
        raise RuntimeError(f"rebuild left {len(missing)} list groups missing")
    old = _current()
    json.dump({"index": out, "delta": dd, "delta_start": start, "built": stamp, "rows": sum(x.get("rows", 0) for x in res)}, open(CURRENT, "w"))
    svol.commit()
    # remove lists older than the ones just replaced (a search that started before the switch may still read those)
    for name in os.listdir("/data"):
        p = f"/data/{name}"
        if name.startswith(("index_", "delta_")) and p not in (out, dd, old["index"], old["delta"]):
            shutil.rmtree(p, ignore_errors=True)
    os.remove("/data/rebuild.lock") if os.path.exists("/data/rebuild.lock") else None
    svol.commit()
    return {"lists": out, "rows": sum(x.get("rows", 0) for x in res)}


def _start_rebuild(warehouse):
    """Start rebuild_lists unless one started in the last 12 hours."""
    svol.reload()
    lock = "/data/rebuild.lock"
    if os.path.exists(lock) and time.time() - os.path.getmtime(lock) < 12 * 3600:
        return "already running"
    open(lock, "w").write(time.strftime("%Y-%m-%d %H:%M:%S"))
    svol.commit()
    return rebuild_lists.spawn(warehouse).object_id


# ---------- reader ----------

def ids_for(tok, row, maxlen=12000):
    """#1668 / #2000 prompt layout: prefix, then per candidate 'c<k> =>' and a ' ?' slot whose next-token yes / no is read."""
    T = lambda s: tok(s, add_special_tokens=False)["input_ids"]
    q, yes, no = T(" ?"), T(" yes"), T(" no")
    assert len(q) == len(yes) == len(no) == 1, (q, yes, no)
    ids = T(row["prefix"])
    slots = []
    for s in row["seats"]:
        t = T(s["line"] + " =>")
        if len(ids) + len(t) + 2 > maxlen:
            break
        ids += t
        slots.append((len(ids), s["key"], s.get("y")))
        ids += q + T("\n")
    return ids, slots, yes[0], no[0]


def merge(adapter_dir, out, base_id):
    import shutil, torch
    from huggingface_hub import snapshot_download
    from peft import PeftModel
    from transformers import AutoModelForImageTextToText
    base = snapshot_download(base_id, max_workers=32)
    m = AutoModelForImageTextToText.from_pretrained(base, dtype=torch.bfloat16).to("cuda")
    m = PeftModel.from_pretrained(m, adapter_dir).merge_and_unload()
    m.save_pretrained(out, max_shard_size="10GB")
    del m
    torch.cuda.empty_cache()
    for fn in os.listdir(base):
        if not fn.endswith(".safetensors") and fn != "model.safetensors.index.json" and not os.path.exists(f"{out}/{fn}"):
            shutil.copy(f"{base}/{fn}", out)


@app.cls(image=reader_image, gpu="H100", volumes={"/vol": qvol}, secrets=[DBX], timeout=2 * 3600, memory=64 * 1024, cpu=16,
         ephemeral_disk=524288, scaledown_window=120, max_containers=READER_GPUS, retries=modal.Retries(max_retries=2, initial_delay=30.0))
class Reader:
    @modal.enter()
    def load(self):
        t0 = time.time()
        out = "/tmp/merged"
        # the merge runs in its own process so its CUDA memory is gone before vLLM starts
        subprocess.run([sys.executable, "-c", f"import modal_reference_matcher as m; m.merge('/vol/models/{ADAPTER}', '{out}', '{BASE}')"],
                       check=True, cwd="/root")
        from vllm import LLM
        self.llm = LLM(model=out, dtype="bfloat16", max_model_len=4096, max_num_batched_tokens=32768, gpu_memory_utilization=0.90,
                       enable_prefix_caching=False, limit_mm_per_prompt={"image": 0, "video": 0}, max_logprobs=20)
        self.tok = self.llm.get_tokenizer()
        self.cold_s = time.time() - t0

    @modal.method()
    def score_shard(self, table: str, shard: int, out_dir: str, warehouse: str):
        import pyarrow as pa, pyarrow.parquet as pq
        from vllm import SamplingParams
        from vllm.inputs import TokensPrompt
        t0 = time.time()
        raw = sql_rows(f"SELECT seed_id, prefix, seats FROM {table} WHERE shard = {int(shard)}", warehouse)
        rows = [{"seed_id": s, "prefix": p, "seats": json.loads(z)} for s, p, z in raw]
        t1 = time.time()
        sp = SamplingParams(max_tokens=1, prompt_logprobs=20, temperature=0, flat_logprobs=True, detokenize=False)
        built = [ids_for(self.tok, r) for r in rows]
        out, miss = [], 0
        for b0 in range(0, len(rows), 400):
            chunk = built[b0:b0 + 400]
            res = self.llm.generate([TokensPrompt(prompt_token_ids=b[0]) for b in chunk], sp, use_tqdm=False)
            by = {}
            for o in res:
                by.setdefault(tuple(o.prompt_token_ids), []).append(o)
            for r, (ids, slots, Y, N) in zip(rows[b0:b0 + 400], chunk):
                o = (by.get(tuple(ids)) or [None]).pop(0)
                if o is None:
                    miss += len(slots)
                    continue
                pl = o.prompt_logprobs
                for pos, key, _ in slots:
                    d = pl[pos] or {}
                    if Y not in d and N not in d:
                        miss += 1
                        continue
                    ly = d[Y].logprob if Y in d else -1e9
                    ln = d[N].logprob if N in d else -1e9
                    mx = max(ly, ln)
                    out.append((r["seed_id"], int(key), math.exp(ly - mx) / (math.exp(ly - mx) + math.exp(ln - mx))))
        t2 = time.time()
        tab = pa.table({"seed_id": pa.array([o[0] for o in out], pa.string()), "cid": pa.array([o[1] for o in out], pa.int64()),
                        "p": pa.array([o[2] for o in out], pa.float64())})
        buf = io.BytesIO()
        pq.write_table(tab, buf, compression="zstd")
        put_file(f"{out_dir}/shard={shard:05d}.parquet", buf.getvalue())
        return {"shard": shard, "rows": len(rows), "slots": len(out), "missing_slots": miss, "fetch_s": round(t1 - t0, 1),
                "gpu_s": round(t2 - t1, 1), "cold_s": round(self.cold_s, 1)}


@app.function(image=cpu_image, secrets=[DBX], timeout=4 * 3600, cpu=1, memory=2048, nonpreemptible=True)
def run_reader(table_in: str, out_dir: str, build_id: str, warehouse: str):
    t0 = time.time()
    shards = [int(r[0]) for r in sql_rows(f"SELECT DISTINCT shard FROM {table_in} ORDER BY shard", warehouse)]
    if not shards:
        return {"stage": "reader", "shards": 0, "prompts": 0, "wall_s": round(time.time() - t0, 1), "version": VERSION_MARK}
    res, errors = [], []
    for r in Reader().score_shard.starmap([(table_in, s, out_dir, warehouse) for s in shards], order_outputs=False, return_exceptions=True):
        (errors if isinstance(r, Exception) else res).append(repr(r)[:300] if isinstance(r, Exception) else r)
    gpu_s = sum(r["gpu_s"] + r["fetch_s"] for r in res) + sum(r["cold_s"] for r in res[:READER_GPUS])
    usd = gpu_s * (3.95 + 16 * 0.0473 + 64 * 0.008) / 3600
    return {"stage": "reader", "shards": len(shards), "shards_done": len(res), "errors": errors, "prompts": sum(r["rows"] for r in res),
            "slots": sum(r["slots"] for r in res), "missing_slots": sum(r["missing_slots"] for r in res), "usd_est": round(usd, 2),
            "wall_s": round(time.time() - t0, 1), "version": VERSION_MARK}


# ---------- API ----------

@app.function(image=cpu_image, secrets=[TOKEN])
@modal.asgi_app()
def api():
    """POST /start {stage, table_in, out_dir, build_id, warehouse?} -> {call_id}; POST /status {call_id} -> {state, result | error}.
    Bearer token = LOCATION_MATCH_TOKEN (the location matcher's). table_in and warehouse go into SQL, out_dir into a URL, build_id into a Volume path:
    each is validated."""
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
        stage = str(b.get("stage", ""))
        table_in, out_dir = str(b.get("table_in", "")), str(b.get("out_dir", "")).rstrip("/")
        build_id, warehouse = str(b.get("build_id", "")), str(b.get("warehouse", DEFAULT_WAREHOUSE))
        if stage not in ("search", "reader"): raise HTTPException(400, "stage must be search or reader")
        if not TABLE_RX.match(table_in): raise HTTPException(400, "table_in must be openalex.works.* or openalex.transient.*")
        if not OUT_RX.match(out_dir) or ".." in out_dir: raise HTTPException(400, "out_dir must be a folder under /Volumes/openalex/<schema>/<volume>/")
        if not BUILD_RX.match(build_id) or not WH_RX.match(warehouse): raise HTTPException(400, "build_id / warehouse not allowed")
        fn = run_search if stage == "search" else run_reader
        call = await fn.spawn.aio(table_in, out_dir, build_id, warehouse)
        return {"call_id": call.object_id, "version": VERSION_MARK}

    @web.post("/status")
    async def status(request: Request):
        auth(request)
        call_id = str((await request.json()).get("call_id", ""))
        if not re.match(r"^fc-[A-Za-z0-9]{10,40}$", call_id): raise HTTPException(400, "bad call_id")
        fc = modal.FunctionCall.from_id(call_id)
        try:
            return {"state": "done", "result": await fc.get.aio(timeout=0)}
        except TimeoutError:
            return {"state": "running"}
        except Exception as e:
            return {"state": "failed", "error": repr(e)[:1000]}

    return web
