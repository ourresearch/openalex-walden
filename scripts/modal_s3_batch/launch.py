"""Steps 2-5 in one go: export the shards, run the Modal app over them, then load the output into Databricks and wait.
Run from this folder with a Python that has databricks-sdk (walden .venv), after the role exists (role.yaml).

    python launch.py --batches 0-31 --batch-size 8 \
      --source openalex.transient.myjob_input --cols text_hash,text --staging openalex.transient.myjob_modal_in \
      --s3 s3://openalex-sandbox/myjob/embed --app embed_qwen3_app.py \
      --dest openalex.vector_search.myjob_vectors \
      --select "CAST(text_hash AS BIGINT) AS text_hash, text, v, current_timestamp() AS embedded_at" \
      --bad "v IS NULL OR length(v) != 2048" --write-id-prefix myjob_embed_b --progress openalex.transient.myjob_progress
    add --load-only to (re)load what Modal has already written.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time

from s3io import parse_ranges

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--batches", required=True)
ap.add_argument("--batch-size", type=int, default=8)
for k in ("source", "cols", "staging", "s3", "app", "dest", "select", "bad", "write-id-prefix", "progress"):
    ap.add_argument(f"--{k}", required=k not in ("select", "bad", "cols", "staging", "app"))
ap.add_argument("--key", default="text_hash")
ap.add_argument("--run", default="modal")
ap.add_argument("--load-only", action="store_true")
ap.add_argument("--workers", type=int, default=8, help="load cluster workers (~2 min per 12.6M-row batch on 8)")
a = ap.parse_args()
if not a.load_only and not (a.cols and a.staging and a.app):
    ap.error("--cols, --staging and --app are needed unless --load-only")


def ranges(xs):
    xs, out = sorted(xs), []
    for x in xs:
        if out and x == out[-1][1] + 1:
            out[-1][1] = x
        else:
            out.append([x, x])
    return ",".join(f"{p}-{q}" if p != q else f"{p}" for p, q in out)


batches = parse_ranges(a.batches)
shards = ranges([a.batch_size * b + i for b in batches for i in range(a.batch_size)])
if not a.load_only:
    subprocess.run([sys.executable, f"{HERE}/export.py", "--source", a.source, "--cols", a.cols, "--staging", a.staging,
                    "--s3", a.s3, "--shards", shards], check=True, cwd=HERE)
    subprocess.run(["modal", "run", f"{a.app}::backfill", "--shards", shards], check=True, cwd=HERE)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(profile="DEFAULT")
nb = f"/Users/{w.current_user.me().user_name}/modal_s3_batch/load_dbx"
w.workspace.mkdirs(os.path.dirname(nb))
w.workspace.import_(path=nb, format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True,
                    content=base64.b64encode(open(f"{HERE}/load_dbx.py", "rb").read()).decode())
params = {"source": a.source, "key": a.key, "s3": a.s3, "dest": a.dest, "select": a.select or "*", "bad": a.bad or "false",
          "batch_size": str(a.batch_size), "batches": ranges(batches), "write_id_prefix": a.write_id_prefix, "progress": a.progress,
          "run": a.run, "wait_minutes": "0"}
spec = {"run_name": f"modal_s3_batch load {a.dest} batches {ranges(batches)}",
        "tasks": [{"task_key": "load", "notebook_task": {"notebook_path": nb, "base_parameters": params},
                   "new_cluster": {"spark_version": "16.4.x-scala2.13", "node_type_id": "rd-fleet.4xlarge",
                                   "driver_node_type_id": "rd-fleet.4xlarge", "num_workers": a.workers,
                                   "aws_attributes": {"availability": "ON_DEMAND", "zone_id": "auto"},
                                   "data_security_mode": "SINGLE_USER", "runtime_engine": "STANDARD"},
                   "timeout_seconds": 14400}]}
r = subprocess.run(["databricks", "jobs", "submit", "--profile", "DEFAULT", "--no-wait", "--json", json.dumps(spec)],
                   capture_output=True, text=True, check=True)
run_id = json.loads(r.stdout)["run_id"]
print(f"load run {run_id} submitted", flush=True)
while True:
    time.sleep(60)
    st = json.loads(subprocess.run(["databricks", "jobs", "get-run", str(run_id), "--profile", "DEFAULT", "-o", "json"],
                                   capture_output=True, text=True, check=True).stdout)
    if st["state"].get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
        out = subprocess.run(["databricks", "jobs", "get-run-output", str(st["tasks"][0]["run_id"]), "--profile", "DEFAULT", "-o", "json"],
                             capture_output=True, text=True).stdout
        print(f"load run {run_id}: {st['state'].get('result_state')} "
              f"{json.loads(out or '{}').get('notebook_output', {}).get('result', '')}", flush=True)
        break
