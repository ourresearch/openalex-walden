"""Template for a Modal GPU batch over S3 shards. Copy it, set the four constants, write transform(), then:

    modal run app.py::ident                 # prints the OIDC sub; its workspace id goes in role.yaml (ModalWorkspaceId)
    modal run app.py::backfill --shards 0-255
    (or launch.py, which also exports the inputs and loads the outputs into Databricks)

Each call handles one shard: in/shard=S/*.parquet -> transform -> out/shard=S/part-0.parquet. A shard whose output exists is
skipped, so a stopped or failed run is simply rerun. For a vLLM model, see embed_qwen3_app.py (4 engines per H100).
Runbook: docs/runbooks/modal-gpu-batch-via-s3.md
"""
import json
import time

import modal

APP_NAME = "myjob-gpu"                                                  # = role.yaml AppName
BUCKET, PREFIX = "openalex-sandbox", "myjob/task"                       # = role.yaml Bucket, Prefix
ROLE_ARN = "arn:aws:iam::639657227068:role/myjob-modal-s3"              # = role.yaml output RoleArn
GPU, CPU, MEMORY_GB, MAX_CONTAINERS = "H100", 12, 48, 50                # size from a one-container test (runbook step 4)

app = modal.App(APP_NAME)
image = (modal.Image.debian_slim(python_version="3.12")
         .pip_install("pyarrow", "boto3")
         .add_local_python_source("s3io"))


@app.cls(image=image, gpu=GPU, cpu=CPU, memory=MEMORY_GB * 1024, timeout=2 * 3600, scaledown_window=120,
         max_containers=MAX_CONTAINERS)
class Worker:
    @modal.enter()
    def start(self):
        pass  # load the model once per container here

    def transform(self, table):
        """pyarrow.Table in -> pyarrow.Table out (keep the key column so the load can check rows). Replace this."""
        return table

    @modal.method()
    def shard(self, s: int):
        import s3io
        c = s3io.client(ROLE_ARN, APP_NAME)
        if s3io.done(c, BUCKET, PREFIX, s):
            return {"shard": s, "skipped": True}
        t0 = time.time()
        t = s3io.read_shard(c, BUCKET, PREFIX, s)
        if t is None:
            return {"shard": s, "error": "no input files"}
        out = self.transform(t)
        assert out.num_rows == t.num_rows, (out.num_rows, t.num_rows)
        s3io.write_shard(ROLE_ARN, BUCKET, PREFIX, s, out)
        return {"shard": s, "rows": t.num_rows, "seconds": round(time.time() - t0)}


@app.function(image=image, timeout=120)
def whoami():
    import s3io
    return s3io.whoami_claims()


@app.local_entrypoint()
def ident():
    print("WHOAMI " + json.dumps(whoami.remote()))


@app.local_entrypoint()
def backfill(shards: str = "0-7"):
    import s3io
    todo = s3io.parse_ranges(shards)
    t0, rows, n = time.time(), 0, 0
    for r in Worker().shard.map(todo, order_outputs=False, return_exceptions=True):
        if isinstance(r, Exception) or "error" in r:
            print("SHARD_ERROR " + repr(r)[:500], flush=True)
            continue
        n += 1
        rows += r.get("rows", 0)
        print("SHARD " + json.dumps(r) + f" | {n}/{len(todo)} shards, {rows:,} rows, {rows / max(time.time() - t0, 1):,.0f}/s",
              flush=True)
