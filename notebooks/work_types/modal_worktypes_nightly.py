"""Work-type nightly scorer on Modal: the topics-nightly SHAPE (worktype-stage2, 2026-11-09; v5/BRIEF-RECUT.md). It copies the
contract of walden notebooks/topics/nightly/modal_topics_nightly.py (Jason, oxjob #1531) exactly, with the FROZEN stage-2 v6c Scorer.
NOT DEPLOYED. Deploy, secrets and the Databricks credential are Rohan's call.

- `api` (bearer token): POST /start {table_in, out_dir, build_id, warehouse?} validates the request, spawns `run_all` and returns its
  call id; POST /status {call_id} says running / done (with the per-shard results) / failed. Same fields, same answers as topics.
- `run_all`: exports the queue table ONCE through the Databricks SQL statement API (EXTERNAL_LINKS) into one file per shard on the
  Modal Volume `openalex-worktypes-nightly` (/data/<build_id>/in/<shard>.jsonl.gz), then maps `Scorer.score` over the shards. GPU
  workers never query the warehouse; each one PUTs `<out_dir>/<shard>.parquet` to the Unity Catalog Volume (Files API).
- `Scorer`: stage 2 v6c, model s2-0bc40dd7f812bdfe = q1.7b-v6c-final (v6 labels), T 1.0, render s2r3 cut at 384 tokens, 25 classes.
  It is prodpath/modal/s2_scorer.py's S2ScorerV6c (byte-identical to the stored scores on all 1,700 DEV works), with its checks:
    - at load: every weight file's sha256 vs the manifest, T vs the manifest, the pinned environment's alias (the exact 163-package
      vLLM lock below, as modal_prod.py's vllm_image_pinned);
    - per shard: each row's input_hash = sha256("s2r3\n" + text)[:32] (the queue renders the text in Databricks with the vendored
      render_s2r3; a mismatch fails the shard: renderer drift), no duplicate work_id, one well-formed output row per input row.
  Output parquet (one row per work; the scores table's key is (work_id, input_hash, model_version)):
    work_id BIGINT, input_hash STRING, model_version STRING (= s2-0bc40dd7f812bdfe), render_version STRING (= s2r3),
    class_idx INT, confidence DOUBLE (softmax(logits / T) of the top class, 4 dp), logits ARRAY<FLOAT> (25, class order = the
    manifest's `classes`), shard STRING.
  Max 8 GPUs, as topics (affiliations / authors work keeps priority on the shared workspace).

Queue table contract (written by worktype-student-v2's queue builder): SELECT shard, work_id, input_hash, text FROM <table_in>,
  shard INT (~50K rows a shard), text = render_s2r3(enriched pack) and input_hash = its hash, both computed in Databricks.

Volumes: `oxjob1976-worktype-s2` at /vol (models/q1.7b-v6c-final: causal/ + head.pt + meta.json; read only, sha-checked);
`openalex-worktypes-nightly` at /data (exported queue shards, one folder per build).
Secrets (NAMES only; created by Rohan, never read here):
  `openalex-worktypes-nightly-token`   WORKTYPES_NIGHTLY_TOKEN, the bearer token (the same value in the Databricks secret scope)
  `openalex-worktypes-dbx`             DATABRICKS_HOST / DATABRICKS_TOKEN of a DEDICATED service principal (Rohan creates it, 11-09
                                       decision): SELECT on the queue table + CAN USE on the warehouse (statement API) and WRITE on the
                                       UC Volume path. Not topics' `oxjob-databricks`.
Deploy from a checkout (NOT DONE):  MODAL_PROFILE=wordslikethis modal deploy prodpath/modal/modal_worktypes_nightly.py
Endpoint after deploy: https://wordslikethis--openalex-worktypes-nightly-api.modal.run (/start, /status)
Never `from __future__ import annotations` here: Modal class parameters break with it."""
import gzip
import hashlib
import json
import math
import os
import re
import time

import modal

APP = "openalex-worktypes-nightly"
VERSION_MARK = "worktypes-nightly v1.1 2026-11-09"
app = modal.App(APP)
mvol = modal.Volume.from_name("oxjob1976-worktype-s2")
dvol = modal.Volume.from_name("openalex-worktypes-nightly", create_if_missing=True, version=2)
SKIP = {"pip", "setuptools", "wheel"}
# prodpath/modal/s2_scorer.py VLLM_LOCK verbatim (test_worktypes_nightly.py checks it), = stage2/modal_prod.py's pinned lock
VLLM_LOCK = ['agent-detector==2.0.0', 'aiohappyeyeballs==2.7.1', 'aiohttp==3.14.3', 'aiosignal==1.4.0', 'annotated-doc==0.0.5',
 'annotated-types==0.8.0', 'anyio==4.15.1', 'astor==0.8.1', 'attrs==26.1.0', 'blake3==1.0.10', 'cachetools==7.2.0',
 'cbor2==6.1.5', 'certifi==2026.7.22', 'cffi==2.1.1', 'charset-normalizer==3.5.2', 'click==8.5.0',
 'cloudpickle==3.1.2', 'compressed-tensors==0.11.0', 'cuda-pathfinder==1.8.3', 'cupy-cuda12x==14.2.0',
 'depyf==0.19.0', 'detect-installer==0.2.1', 'dill==0.4.1', 'diskcache==5.6.3', 'dnspython==2.8.0', 'einops==0.8.2',
 'email-validator==2.3.0', 'fastapi-cli==0.0.32', 'fastapi-cloud-cli==0.26.0', 'fastapi==0.142.2', 'fastar==0.12.0',
 'filelock==4.0.9', 'frozendict==2.4.7', 'frozenlist==1.8.0', 'fsspec==2026.9.0', 'gguf==0.19.0',
 'googleapis-common-protos==1.75.5', 'grpclib==0.4.8', 'h11==0.16.0', 'h2==4.2.0', 'hf-xet==1.6.0', 'hpack==4.1.0',
 'httpcore2==2.13.1', 'httpcore==1.0.9', 'httptools==0.8.0', 'httpx2==2.13.1', 'httpx==0.28.1',
 'huggingface_hub==0.36.2', 'hyperframe==6.1.0', 'idna==3.20', 'interegular==0.3.3', 'Jinja2==3.1.6', 'jiter==0.17.0',
 'jsonschema-specifications==2025.9.1', 'jsonschema==4.26.0', 'lark==1.2.2', 'llguidance==0.7.30', 'llvmlite==0.44.0',
 'lm-format-enforcer==0.11.3', 'markdown-it-py==4.2.0', 'MarkupSafe==3.0.4', 'mdurl==0.1.2', 'mistral_common==1.12.0',
 'mpmath==1.3.0', 'msgpack==1.2.3', 'msgspec==0.22.0', 'multidict==6.9.1', 'networkx==3.6.1', 'ninja==1.13.2',
 'numba==0.61.2', 'numpy==2.2.6', 'nvidia-cublas-cu12==12.8.4.1', 'nvidia-cuda-cupti-cu12==12.8.90',
 'nvidia-cuda-nvrtc-cu12==12.8.93', 'nvidia-cuda-runtime-cu12==12.8.90', 'nvidia-cudnn-cu12==9.10.2.21',
 'nvidia-cufft-cu12==11.3.3.83', 'nvidia-cufile-cu12==1.13.1.3', 'nvidia-curand-cu12==10.3.9.90',
 'nvidia-cusolver-cu12==11.7.3.90', 'nvidia-cusparse-cu12==12.5.8.93', 'nvidia-cusparselt-cu12==0.7.1',
 'nvidia-nccl-cu12==2.27.3', 'nvidia-nvjitlink-cu12==12.8.93', 'nvidia-nvtx-cu12==12.8.90', 'openai-harmony==0.0.8',
 'openai==3.24.0', 'opencv-python-headless==5.0.0.93', 'opentelemetry-api==1.45.0',
 'opentelemetry-exporter-http-transport==0.66b0', 'opentelemetry-exporter-otlp-common==0.66b0',
 'opentelemetry-exporter-otlp-proto-common==1.45.0', 'opentelemetry-exporter-otlp-proto-http==1.45.0',
 'opentelemetry-proto==1.45.0', 'opentelemetry-sdk==1.45.0', 'opentelemetry-semantic-conventions==0.66b0',
 'outlines_core==0.2.11', 'packaging==26.3', 'partial-json-parser==0.2.1.1.post7', 'pillow==12.3.0', 'pip==25.2',
 'prometheus-fastapi-instrumentator==8.1.0', 'prometheus_client==0.26.0', 'propcache==0.5.4', 'protobuf==7.36.2',
 'psutil==7.2.2', 'py-cpuinfo==9.0.0', 'pybase64==1.5.0', 'pycountry==26.2.16', 'pycparser==3.0',
 'pydantic-extra-types==2.11.1', 'pydantic-settings==2.15.0', 'pydantic==2.13.5', 'pydantic_core==2.46.5',
 'Pygments==2.21.0', 'python-dotenv==1.2.4', 'python-json-logger==4.2.0', 'python-multipart==0.0.32', 'PyYAML==6.0.3',
 'pyzmq==27.2.0', 'ray==2.59.0', 'referencing==0.37.0', 'regex==2026.9.29', 'requests==2.34.2',
 'rich-toolkit==0.20.5', 'rich==15.0.0', 'rignore==0.8.1', 'rpds-py==2026.6.3', 'safetensors==0.8.0', 'scipy==1.17.1',
 'sentencepiece==0.2.2', 'sentry-sdk==2.71.0', 'setproctitle==1.3.8', 'setuptools==65.5.1', 'shellingham==1.5.4',
 'sniffio==1.3.1', 'soundfile==0.14.0', 'soxr==1.1.0', 'starlette==1.7.0', 'sympy==1.14.0', 'tiktoken==0.14.0',
 'tokenizers==0.21.4', 'torch==2.8.0', 'torchaudio==2.8.0', 'torchvision==0.23.0', 'tqdm==4.70.1',
 'transformers==4.55.4', 'triton==3.4.0', 'truststore==0.10.4', 'typer==0.27.2', 'typing-inspection==0.4.4',
 'typing_extensions==4.16.0', 'urllib3==2.8.0', 'uv==0.8.17', 'uvicorn==0.54.0', 'uvloop==0.23.0', 'vllm==0.10.2',
 'watchfiles==1.3.0', 'websockets==17.1', 'wheel==0.45.1', 'xformers==0.0.32.post1', 'xgrammar==0.1.23',
 'yarl==1.25.1']
# prodpath/modal/s2_scorer.py MANIFEST_V6C verbatim (= make_policy.manifest() over the weight record; the test checks it)
MANIFEST = {'aliases': ['s2-q1.7b-v6c-final-T1.0-envc70dc591'],
 'classes': ['article', 'book', 'book-chapter', 'book-review', 'conference-abstract', 'conference-paper',
             'data-paper', 'dataset', 'dissertation', 'editorial', 'erratum', 'letter', 'other', 'paratext',
             'peer-review', 'preprint', 'reference-entry', 'report', 'retraction', 'review', 'software-paper',
             'software', 'standard', 'supplementary-materials', 'study-registration'],
 'files': {'causal/added_tokens.json': 'c0284b582e14987fbd3d5a2cb2bd139084371ed9acbae488829a1c900833c680',
           'causal/chat_template.jinja': 'a55ee1b1660128b7098723e0abcd92caa0788061051c62d51cbe87d9cf1974d8',
           'causal/config.json': '56d00e5e73be2735cf6d324466e039cdf19b2fab5efaf909d3af83ef76de12c7',
           'causal/generation_config.json': 'e01064dd848f648bd00b6d73acf204f1eb992ec84e21d0dfc51cb0e0fa6ef10d',
           'causal/merges.txt': '8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5',
           'causal/model.safetensors': 'e74aea063e920c3c584617c9b03f3f628e4f42d045250719990a9476ac8bc97b',
           'causal/special_tokens_map.json': '76862e765266b85aa9459767e33cbaf13970f327a0e88d1c65846c2ddd3a1ecd',
           'causal/tokenizer.json': 'adfb57087bf35878c5a8a3dd9d46a5fb97a47c3e4ff93330504adbea55772eda',
           'causal/tokenizer_config.json': '443bfa629eb16387a12edbf92a76f6a6f10b2af3b53d87ba1550adfcf45f7fa0',
           'causal/vocab.json': 'ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910',
           'head.pt': '3c835da308f1fbd90de06842ac8fd89071ec4eea860eb4681f7152c6c5377b83',
           'meta.json': 'a368b892aa4da642155b80f89d997abd412189de4ed081eecc91b4772a80beb1'},
 'id': 's2-0bc40dd7f812bdfe',
 'kind': 's2',
 'note': 'q1.7b-v6c-final weights (Modal volume), T 1.0, render s2r3',
 'render_version': 's2r3',
 'temperature': 1.0}
TAG = "q1.7b-v6c-final"
N_CLASSES = 25
MAXLEN = 384
RENDER_VERSION = "s2r3"
MAXC = 8                  # max H100 containers (= topics)
DEFAULT_WAREHOUSE = "3996dc0a9b183ce3"   # topics' default (DEV-WH = the jobs tier jobs-xl from 11-10: the scheduled nightly only)
PRICE_PER_S = (3.95 + 16 * 0.0473 + 64 * 0.008) / 3600   # = topics: H100 + 16 cpu + 64 GiB, $/container-second
TABLE_RX = re.compile(r"^openalex\.(works|transient)\.[A-Za-z0-9_]{1,120}$")
OUT_RX = re.compile(r"^/Volumes/openalex/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.=-]+){1,6}$")
WH_RX = re.compile(r"^[0-9a-f]{16}$")
BUILD_RX = re.compile(r"^[0-9A-Za-z_-]{6,40}$")
# the pinned scoring image: exactly s2_scorer.py's gpu_image (same steps, same order), so the environment alias matches the manifest
gpu_image = (modal.Image.debian_slim(python_version="3.11")
             .pip_install(*[x for x in VLLM_LOCK if x.split("==")[0].lower() not in SKIP])
             .env({"HF_HOME": "/vol/hf", "VLLM_CACHE_ROOT": "/tmp/vllm_cache", "TOKENIZERS_PARALLELISM": "true"}))
cpu_image = modal.Image.debian_slim(python_version="3.11").pip_install("httpx", "fastapi[standard]", "numpy==2.2.6",
                                                                        "pyarrow==21.0.0")
TOKEN = modal.Secret.from_name("openalex-worktypes-nightly-token")
DBX = modal.Secret.from_name("openalex-worktypes-dbx")   # a dedicated service principal's DATABRICKS_HOST / DATABRICKS_TOKEN
_PKGS = ["torch", "transformers", "accelerate", "vllm", "numpy", "tokenizers", "safetensors", "triton", "xformers",
         "flashinfer-python", "nvidia-cuda-runtime-cu12", "nvidia-cudnn-cu12", "huggingface-hub", "pydantic"]


def text_hash(text: str) -> str:   # = s2_scorer.text_hash
    return hashlib.sha256(f"{RENDER_VERSION}\n{text}".encode()).hexdigest()[:32]


def check_rows(rows: list) -> None:
    """Fail loud on a bad input_hash (renderer drift), an empty text or a duplicate work_id (= s2_scorer.prepare's checks)."""
    bad = [r["id"] for r in rows if text_hash(r["text"]) != r["h"]]
    if bad:
        raise ValueError(f"{len(bad)} rows: input_hash != sha256('s2r3\\n' + text)[:32] (renderer drift?) first: {bad[:5]}")
    if any(not r["text"] for r in rows):
        raise ValueError("empty text")
    ids = [r["id"] for r in rows]
    if len(set(ids)) != len(ids):
        raise ValueError(f"{len(ids) - len(set(ids))} duplicate work_ids")


def _env() -> dict:   # = s2_scorer._env (the environment record hashed into the manifest alias)
    import importlib.metadata as md
    import platform
    out = {"image_kind": "vllm", "image_id": os.environ.get("MODAL_IMAGE_ID"), "python": platform.python_version(),
           "packages": {}}
    for p in _PKGS:
        try:
            out["packages"][p] = md.version(p)
        except md.PackageNotFoundError:
            pass
    try:
        import torch
        out["torch_cuda"] = torch.version.cuda
        out["cudnn"] = torch.backends.cudnn.version()
    except Exception as e:  # noqa: BLE001
        out["torch_cuda"] = f"n/a ({type(e).__name__})"
    from vllm.engine.arg_utils import EngineArgs
    f = getattr(EngineArgs, "__dataclass_fields__", {})
    out["engine_args_has_runner"] = "runner" in f
    out["engine_args_has_convert"] = "convert" in f
    out["vllm_call_path"] = ('LLM(runner="pooling", convert="embed", override_pooler_config={pooling_type: LAST, '
                             'normalize: False}, dtype=bfloat16)' if ("runner" in f and "convert" in f)
                             else 'LLM(task="embed", ...) [TypeError fallback]')
    return out


def _dbx_host():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    return host if host.startswith("http") else "https://" + host


@app.cls(image=gpu_image, gpu="H100", volumes={"/vol": mvol, "/data": dvol}, secrets=[DBX], timeout=3600,
         startup_timeout=1200, cpu=16, memory=65536, max_containers=MAXC, scaledown_window=120,
         retries=modal.Retries(max_retries=2, initial_delay=10.0))
class Scorer:
    @modal.enter()
    def load(self):   # = s2_scorer.S2ScorerV6c.load
        import torch
        import torch.nn as nn
        from transformers import AutoTokenizer
        from vllm import LLM
        print(VERSION_MARK, "scorer load", flush=True)
        t0 = time.time()
        mvol.reload()
        d = f"/vol/models/{TAG}"
        got = {}
        for dp, _, fns in os.walk(d):
            for fn in fns:
                fp = os.path.join(dp, fn)
                h = hashlib.sha256()
                with open(fp, "rb") as fh:
                    for chunk in iter(lambda: fh.read(1 << 22), b""):
                        h.update(chunk)
                got[os.path.relpath(fp, d)] = h.hexdigest()
        if got != MANIFEST["files"]:
            bad = sorted(k for k in set(got) | set(MANIFEST["files"]) if got.get(k) != MANIFEST["files"].get(k))
            raise RuntimeError(f"{TAG}: files don't match manifest {MANIFEST['id']}: {bad}")
        self.meta = json.load(open(f"{d}/meta.json"))
        if int(self.meta.get("max_len", MAXLEN)) != MAXLEN:
            raise RuntimeError(f"{TAG} was trained at max_len {self.meta['max_len']}, not {MAXLEN}")
        if abs(float(self.meta["temperature"]) - float(MANIFEST["temperature"])) > 1e-12:
            raise RuntimeError(f"T {self.meta['temperature']} != manifest T {MANIFEST['temperature']}")
        self.env = _env()
        if not (self.env["engine_args_has_runner"] and self.env["engine_args_has_convert"]):
            raise RuntimeError("this vLLM lacks runner/convert: refusing the old task= API")
        env_sha = hashlib.sha256(json.dumps(self.env, sort_keys=True).encode()).hexdigest()[:8]
        alias = f"s2-{TAG}-T{self.meta['temperature']}-env{env_sha}"
        if alias not in MANIFEST.get("aliases", []):
            raise RuntimeError(f"environment alias {alias} is not one of manifest {MANIFEST['id']}'s aliases "
                               f"{MANIFEST.get('aliases')}: not the pinned scoring environment")
        self.version = MANIFEST["id"]
        self.tok = AutoTokenizer.from_pretrained(f"{d}/causal")
        self.llm = LLM(model=f"{d}/causal", runner="pooling", convert="embed", dtype="bfloat16",
                       max_model_len=max(512, MAXLEN + 16), gpu_memory_utilization=0.85, enable_prefix_caching=False,
                       seed=0, override_pooler_config={"pooling_type": "LAST", "normalize": False})
        sd = torch.load(f"{d}/head.pt")
        self.head = nn.Linear(sd["weight"].shape[1], sd["weight"].shape[0]).cuda()
        self.head.load_state_dict(sd)
        self.head.eval()
        assert sd["weight"].shape[0] == N_CLASSES == len(MANIFEST["classes"]), tuple(sd["weight"].shape)
        print(f"S2 V6C LOADED {self.version} alias {alias} in {time.time() - t0:.1f} s", flush=True)

    def run(self, rows):   # = S2ScorerV6c.score's GPU part: one work per sequence, the stored-score computation
        import numpy as np
        import torch
        from vllm.inputs import TokensPrompt
        enc = self.tok([r["text"] for r in rows], truncation=True, max_length=MAXLEN)["input_ids"]
        outs = self.llm.embed([TokensPrompt(prompt_token_ids=x) for x in enc], use_tqdm=False)
        if len(outs) != len(rows):
            raise RuntimeError(f"vLLM returned {len(outs)} embeddings for {len(rows)} inputs")
        H = torch.tensor(np.array([o.outputs.embedding for o in outs]), dtype=torch.float32, device="cuda")
        with torch.no_grad():
            z = self.head(H)
            p = torch.softmax(z / self.meta["temperature"], 1)
        conf, idx = p.max(1)
        return idx.cpu().numpy(), conf.cpu().numpy(), z.cpu().numpy()

    @modal.method()
    def score(self, build_id: str, shard: int, out_dir: str):
        """Score /data/<build_id>/in/<shard>.jsonl.gz into /data/<build_id>/out/<shard>.npz (put_shard uploads the parquet)."""
        import numpy as np
        t0 = time.time()
        dvol.reload()
        rows = [json.loads(l) for l in gzip.open(f"/data/{build_id}/in/{shard:04d}.jsonl.gz", "rt")]
        check_rows(rows)
        t_read = time.time() - t0
        idx, conf, z = self.run(rows)
        t_gpu = time.time() - t0 - t_read
        n = len(rows)
        if not (len(idx) == len(conf) == z.shape[0] == n and z.shape[1] == N_CLASSES and np.isfinite(z).all()
                and ((conf >= 0) & (conf <= 1)).all()):
            raise RuntimeError(f"shard {shard}: malformed output")
        # The pinned scoring image has no pyarrow (and adding it would change the image id the alias check hashes), so the
        # GPU side saves the raw arrays with numpy; put_shard (cpu_image) writes the parquet and PUTs it (oxjob #1976 smoke).
        os.makedirs(f"/data/{build_id}/out", exist_ok=True)
        np.savez(f"/data/{build_id}/out/{shard:04d}.npz", work_id=np.array([r["id"] for r in rows], dtype=np.int64),
                 input_hash=np.array([r["h"] for r in rows], dtype=str), class_idx=idx.astype(np.int32),
                 confidence=np.array([round(float(c), 4) for c in conf], dtype=np.float64),
                 logits=np.round(z.astype(np.float64), 4).astype(np.float32))
        dvol.commit()
        return {"shard": shard, "works": n, "model_version": self.version, "read_s": round(t_read, 1), "gpu_s": round(t_gpu, 1),
                "total_s": round(time.time() - t0, 1), "wps": round(n / max(t_gpu, 1e-6), 1)}


@app.function(image=cpu_image, volumes={"/data": dvol}, secrets=[DBX], timeout=1800, cpu=2, memory=4096)
def put_shard(build_id: str, shard: int, out_dir: str, version: str):
    """/data/<build_id>/out/<shard>.npz (Scorer.score) -> <out_dir>/<shard>.parquet in the UC Volume (Files API). Same columns
    and types the score load reads (03_scores_load.sql)."""
    import io
    import httpx
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    dvol.reload()
    a = np.load(f"/data/{build_id}/out/{shard:04d}.npz")
    n, sh = len(a["work_id"]), f"{shard:04d}"
    if not (len(a["input_hash"]) == len(a["class_idx"]) == len(a["confidence"]) == a["logits"].shape[0] == n
            and a["logits"].shape[1] == N_CLASSES):
        raise RuntimeError(f"shard {shard}: malformed arrays")
    offs = pa.array(np.arange(0, n * N_CLASSES + 1, N_CLASSES, dtype=np.int32))
    t = pa.table({"work_id": pa.array(a["work_id"], pa.int64()),
                  "input_hash": pa.array(a["input_hash"].tolist(), pa.string()),
                  "model_version": pa.array([version] * n, pa.string()),
                  "render_version": pa.array([RENDER_VERSION] * n, pa.string()),
                  "class_idx": pa.array(a["class_idx"], pa.int32()),
                  "confidence": pa.array(a["confidence"], pa.float64()),
                  "logits": pa.ListArray.from_arrays(offs, pa.array(a["logits"].ravel(), pa.float32())),
                  "shard": pa.array([sh] * n, pa.string())})
    buf = io.BytesIO()
    pq.write_table(t, buf, compression="zstd")
    body = buf.getvalue()
    H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}", "Content-Type": "application/octet-stream"}
    for att in range(5):
        try:
            r = httpx.put(f"{_dbx_host()}/api/2.0/fs/files{out_dir}/{sh}.parquet", params={"overwrite": "true"}, content=body,
                          headers=H, timeout=600)
            r.raise_for_status()
            break
        except Exception as e:  # noqa: BLE001
            print("PUT retry", shard, repr(e)[:200], flush=True)
            time.sleep(10 * (att + 1))
    else:
        raise RuntimeError(f"shard {shard}: PUT failed")
    return {"shard": shard, "bytes": len(body)}


def export_queue(table_in, build_id, warehouse):
    """One statement over the whole queue (EXTERNAL_LINKS), rows streamed into one gzip file per shard on /data/<build_id>/in/."""
    import httpx
    from concurrent.futures import ThreadPoolExecutor
    host = _dbx_host()
    H = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    d = f"/data/{build_id}/in"
    os.makedirs(d, exist_ok=True)
    t0 = time.time()
    st = httpx.post(f"{host}/api/2.0/sql/statements", headers=H, timeout=120, json={
        "warehouse_id": warehouse, "wait_timeout": "0s", "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY",
        "statement": f"SELECT shard, work_id, input_hash, text FROM {table_in}"}).json()
    if "statement_id" not in st:
        raise RuntimeError(f"statement not accepted: {json.dumps(st)[:500]}")
    sid = st["statement_id"]
    print("export statement", sid, flush=True)
    while st["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - t0 > 1800:
            httpx.post(f"{host}/api/2.0/sql/statements/{sid}/cancel", headers=H, timeout=60)
            raise TimeoutError("queue export not done in 30 min")
        time.sleep(5)
        st = httpx.get(f"{host}/api/2.0/sql/statements/{sid}", headers=H, timeout=120).json()
    if st["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(json.dumps(st["status"])[:1000])
    total = int(st["manifest"].get("total_row_count", 0))
    n_chunks = int(st["manifest"].get("total_chunk_count", 0))
    print(f"export: {total} rows in {n_chunks} chunks, query {time.time() - t0:.0f}s", flush=True)

    def fetch(k):
        for att in range(6):
            try:
                links = httpx.get(f"{host}/api/2.0/sql/statements/{sid}/result/chunks/{k}", headers=H, timeout=120).json()["external_links"]
                out = []
                for l in links:
                    out += httpx.get(l["external_link"], timeout=600).json()
                return out
            except Exception as e:  # noqa: BLE001
                print("chunk retry", k, repr(e)[:200], flush=True)
                time.sleep(10 * (att + 1))
        raise RuntimeError(f"chunk {k} failed")

    files, counts, n = {}, {}, 0
    try:
        with ThreadPoolExecutor(4) as ex:   # map keeps chunk order and at most a few chunks in memory
            for rows in ex.map(fetch, range(n_chunks)):
                for shard, wid, h, txt in rows:
                    s = int(shard)
                    if s not in files:
                        files[s] = gzip.open(f"{d}/{s:04d}.jsonl.gz", "wt", compresslevel=3)
                    files[s].write(json.dumps({"id": int(wid), "h": h or "", "text": txt or ""}, ensure_ascii=False) + "\n")
                    counts[s] = counts.get(s, 0) + 1
                    n += 1
    finally:
        for f in files.values():
            f.close()
    if n != total:
        raise RuntimeError(f"export wrote {n} rows, statement said {total}")
    res = {"rows": n, "shards": {str(k): v for k, v in sorted(counts.items())}, "statement": sid, "seconds": round(time.time() - t0, 1)}
    json.dump(res, open(f"/data/{build_id}/export_done.json", "w"))
    dvol.commit()
    return res


@app.function(image=cpu_image, volumes={"/data": dvol}, secrets=[DBX], timeout=6 * 3600, cpu=4, memory=16384)
def run_all(table_in: str, out_dir: str, build_id: str, warehouse: str):
    print(VERSION_MARK, "run_all", table_in, out_dir, build_id, flush=True)
    t0 = time.time()
    dvol.reload()
    done = f"/data/{build_id}/export_done.json"
    exp = json.load(open(done)) if os.path.exists(done) else export_queue(table_in, build_id, warehouse)
    shards = sorted(int(s) for s in exp["shards"])
    t_export = time.time() - t0
    scored, errors = [], []
    for r in Scorer().score.starmap([(build_id, s, out_dir) for s in shards], order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception):
            errors.append(repr(r)[:500])
        else:
            scored.append(r)
            print(r, flush=True)
    by_shard = {r["shard"]: r for r in scored}
    results = []
    for p in put_shard.starmap([(build_id, r["shard"], out_dir, r["model_version"]) for r in scored], order_outputs=False,
                               return_exceptions=True):
        if isinstance(p, Exception):
            errors.append(repr(p)[:500])
        else:
            results.append(dict(by_shard[p["shard"]], bytes=p["bytes"]))
    shard_s = sum(r["total_s"] for r in scored)
    return {"shards": len(shards), "shards_done": len(results), "errors": errors, "works": sum(r["works"] for r in results),
            "exported": exp["rows"], "model_version": MANIFEST["id"], "export_s": round(t_export, 1),
            "shard_seconds": round(shard_s, 1), "usd_est": round(shard_s * PRICE_PER_S, 3), "wall_s": round(time.time() - t0, 1),
            "version": VERSION_MARK, "results": sorted(results, key=lambda r: r["shard"])}


@app.function(image=cpu_image, secrets=[TOKEN])
@modal.asgi_app()
def api():
    """POST /start {table_in, out_dir, build_id, warehouse?} -> {call_id}; POST /status {call_id} -> {state: running | done | failed,
    result | error}. Bearer token = WORKTYPES_NIGHTLY_TOKEN. Every field is validated: table_in and warehouse go into SQL, out_dir into
    a URL, build_id into a Volume path. (= modal_topics_nightly.api)"""
    import hmac
    from fastapi import FastAPI, HTTPException, Request
    web = FastAPI()

    def auth(request):
        got = request.headers.get("authorization", "")
        if not hmac.compare_digest(got.encode(), f"Bearer {os.environ['WORKTYPES_NIGHTLY_TOKEN']}".encode()):
            raise HTTPException(status_code=401, detail="unauthorized")

    @web.post("/start")
    async def start(request: Request):
        auth(request)
        b = await request.json()
        table_in, out_dir = str(b.get("table_in", "")), str(b.get("out_dir", "")).rstrip("/")
        build_id, warehouse = str(b.get("build_id", "")), str(b.get("warehouse", DEFAULT_WAREHOUSE))
        if not TABLE_RX.match(table_in):
            raise HTTPException(400, "table_in must be openalex.works.* or openalex.transient.*")
        if not OUT_RX.match(out_dir) or ".." in out_dir:
            raise HTTPException(400, "out_dir must be a folder under /Volumes/openalex/<schema>/<volume>/")
        if not BUILD_RX.match(build_id) or not WH_RX.match(warehouse):
            raise HTTPException(400, "build_id / warehouse not allowed")
        call = run_all.spawn(table_in, out_dir, build_id, warehouse)
        return {"call_id": call.object_id, "version": VERSION_MARK}

    @web.post("/status")
    async def status(request: Request):
        auth(request)
        call_id = str((await request.json()).get("call_id", ""))
        if not re.match(r"^fc-[A-Za-z0-9]{10,40}$", call_id):
            raise HTTPException(400, "bad call_id")
        fc = modal.FunctionCall.from_id(call_id)
        try:
            return {"state": "done", "result": fc.get(timeout=0)}
        except TimeoutError:
            return {"state": "running"}
        except Exception as e:  # noqa: BLE001   (the call failed or was cancelled)
            return {"state": "failed", "error": repr(e)[:1000]}

    return web
