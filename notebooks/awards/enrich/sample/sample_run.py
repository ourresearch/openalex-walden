"""One-off runs of job AwardTopics' notebooks on a fixed id list: `check` (local, the default) or `go` (one Databricks run).

    python3 sample_run.py check [sample|tiny]          # local only: no network, no token, no SQL, no Modal
    AWT_OK='<who> <date>' AWT_WAREHOUSE=<16-hex id> python3 sample_run.py go [sample|tiny]
    (a warehouse on the grants-hub never-use list, e.g. Jason's DEV-WH, also needs AWT_WAREHOUSE_OK=<the same id>)

Runs (RUNS below): `sample` = the 10k sample (run 831985739633851, 2026-11-09, all four tasks); `tiny` = 100 awards through the
two Modal apps only (no SDGs), own dev tables and own queue copy, to test a warehouse for the apps' export (e.g. shared-2xl).

`check` proves the run is ready and says what blocks it: the 10k ids file (count + sha256), the two Modal /start bodies validated
against the rules in Jason's app sources (TABLE_RX / OUT_RX / WH_RX / ALLOWED_MODELS), the spend estimates against the max_usd caps,
and the list of every write the run would make. It never reads a secret.

`go` (only with AWT_OK set by Jason's or Rohan's direct OK, and a warehouse named for the apps' export) copies this branch's
enrich folder, utils/keywords_nightly.py and the SDG head into /Workspace/Users/<me>/awt-sample/<commit>/ (repo layout kept, so the
notebooks' imports work), then submits ONE run (`databricks jobs submit`) of the four notebooks with dry_run=false on the sample ids,
and polls it. Nothing is scheduled and no job is created.
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ENRICH = os.path.dirname(HERE)
REPO = os.path.abspath(os.path.join(ENRICH, "..", "..", ".."))
sys.path.insert(0, os.path.join(ENRICH, "lib"))
import modal_call as M  # noqa: E402

PROFILE = "dbc-ce570f73-0362"
BASE = {   # job parameters shared by the runs (jobs/award_topics.yaml defaults, except dry_run / warehouse / ids)
    "awards_table": "openalex.awards.awards_api",
    "topics_volume_dir": "/Volumes/openalex/works/data/topics_nightly/awards",
    "keywords_volume_dir": "/Volumes/openalex/authors/oxjob_tmp/oxjob1322_god/nightly/awards",
}
RUNS = {
    "sample": dict(BASE, ids="awt_sample_ids.csv", n=10000, tasks=("build_queue", "topics", "keywords", "sdgs"),
                   state_prefix="openalex_dev.rohan_lab.awt_", modal_queue_table="openalex.transient.awt_queue",
                   topics_target="openalex_dev.rohan_lab.awt_award_topics_v2", keywords_target="openalex_dev.rohan_lab.awt_award_keywords",
                   sdgs_target="openalex_dev.rohan_lab.awt_award_sdgs", topics_max_usd="2", keywords_max_usd="2", sdgs_max_usd="1"),
    "tiny": dict(BASE, ids="awt_tiny_ids.csv", n=100, tasks=("build_queue", "topics", "keywords"),
                 state_prefix="openalex_dev.rohan_lab.awt_tiny_", modal_queue_table="openalex.transient.awt_tiny_queue",
                 topics_target="openalex_dev.rohan_lab.awt_tiny_award_topics_v2", keywords_target="openalex_dev.rohan_lab.awt_tiny_award_keywords",
                 sdgs_target="openalex_dev.rohan_lab.awt_tiny_award_sdgs", topics_max_usd="1", keywords_max_usd="1", sdgs_max_usd="1"),
}
WRITES = [
    "{modal_queue_table}  ({n:,} rows, PROD catalog scratch: the apps read openalex.(works|transient) only)",
    "{state_prefix}queue, {state_prefix}topics_raw, {state_prefix}keywords_raw, {state_prefix}kw_nrm_*, {state_prefix}keywords_rows",
    "{topics_target}, {keywords_target}" + " (+ {sdgs_target})  (dev side tables)",
    "{topics_volume_dir}/<build>/*.parquet and {keywords_volume_dir}/<build>/*.jsonl.gz  (PROD-catalog Volumes: the apps' OUT_RX)",
    "Modal Volume openalex-topics-nightly:/data/<build>/in/  (the topics app's export, Jason's)",
    "/Workspace/Users/<me>/awt-sample/<commit>/  (the notebooks of this branch)",
]


def sha256(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


# What `go` copies to the workspace (repo layout kept): every file the notebooks read at run time, incl. the app sources
# lib/modal_call.app_rules() parses, utils/keywords_nightly.py and the SDG head.
UPLOADS = ["notebooks/awards/enrich", "utils", "notebooks/sdg", M.APPS["topics"]["source"], M.APPS["keywords"]["source"]]


def check(warehouse, run="sample"):
    PARAMS = RUNS[run]
    N_EXPECTED, IDS = PARAMS["n"], os.path.join(HERE, PARAMS["ids"])
    gates, ok = [], True
    approved = os.environ.get("AWT_WAREHOUSE_OK", "").strip()
    if os.path.exists(IDS):
        ids = [l.split(",")[0] for l in open(IDS).read().splitlines()[1:]]
        want = open(IDS + ".sha256").read().split()[0]
        ids_ok = len(ids) == len(set(ids)) == N_EXPECTED and sha256(IDS) == want
        gates.append((f"{run} ids", ids_ok, f"{len(set(ids)):,} distinct ids, sha256 {'matches' if sha256(IDS) == want else 'DIFFERS'}"))
    else:
        gates.append((f"{run} ids", False, f"{PARAMS['ids']} is not in this checkout (the 10k ids live on branch awards-topics-sample)"))
    shards = [0]   # <= 25,000 awards = one shard
    build = "awt-YYYYMMDDTHHMMSSZ"
    for app, vol in (("topics", PARAMS["topics_volume_dir"]), ("keywords", PARAMS["keywords_volume_dir"])):
        body = M.start_body(app, PARAMS["modal_queue_table"], f"{vol}/{build}", build, warehouse, shards)
        bad = M.check_request(app, body, approved)   # the same rule the notebooks apply, with the same approved id
        dev_bad = M.check_request(app, dict(body, table_in=PARAMS["state_prefix"] + "queue", out_dir=f"/Volumes/openalex_dev/rohan_lab/awt_files/{app}/{build}"))
        est = M.estimate_usd(app, N_EXPECTED, len(shards))
        cap = float(PARAMS[f"{app}_max_usd"])
        gates.append((f"{app}: request accepted by the app", not bad, "; ".join(bad) or json.dumps(body)))
        gates.append((f"{app}: all-dev request (queue + Volume in openalex_dev)", not dev_bad, "; ".join(dev_bad) or "accepted"))
        gates.append((f"{app}: spend estimate under max_usd", est <= cap, f"${est:.2f} <= ${cap:.2f} (Jason's guard formula)"))
    if "sdgs" in PARAMS["tasks"]:
        gates.append(("sdgs: spend estimate under max_usd", True, f"${N_EXPECTED / 1e6 * 3.4:.3f} endpoint (measured 2026-11-08: 15 s, 10,000 awards)"))
    ok_go = bool(os.environ.get("AWT_OK", "").strip())
    gates.append(("direct OK from Jason or Rohan (AWT_OK)", ok_go, os.environ.get("AWT_OK", "") or "not set: his apps, his token (secret scope modal), his GPU slot"))
    for name, passed, detail in gates:
        print(f"[{'PASS' if passed else 'BLOCK'}] {name}: {detail}")
    print("\nthe run would write:")
    for w in WRITES:
        if "sdgs" not in PARAMS["tasks"]:
            w = w.replace(" (+ {sdgs_target})", "")
        print("  -", w.format(**PARAMS))
    blocking = [g for g in gates if not g[1] and "all-dev" not in g[0]]
    print(f"\n{'READY' if not blocking else 'BLOCKED'}: {len(blocking)} blocking gate(s)" + ("" if not blocking else ": " + ", ".join(g[0] for g in blocking)))
    return not blocking


def sh(*args, capture=True):
    print("$", " ".join(args), flush=True)
    r = subprocess.run(args, capture_output=capture, text=True)
    if r.returncode:
        raise SystemExit(f"failed ({r.returncode}): {(r.stderr or '')[:2000]}")
    return r.stdout


def go(warehouse, run="sample"):
    PARAMS = RUNS[run]
    if not check(warehouse, run):
        raise SystemExit("not starting: a gate blocks (see above)")
    commit = sh("git", "-C", REPO, "rev-parse", "--short", "HEAD").strip()
    if sh("git", "-C", REPO, "status", "--porcelain", "--", *UPLOADS).strip():
        raise SystemExit("uncommitted changes in the files the run uploads: commit first, so the run is reproducible")
    me = json.loads(sh("databricks", "current-user", "me", "-p", PROFILE, "-o", "json"))["userName"]
    root = f"/Workspace/Users/{me}/awt-sample/{commit}"
    for rel in UPLOADS:
        if os.path.isdir(os.path.join(REPO, rel)):
            sh("databricks", "workspace", "import-dir", os.path.join(REPO, rel), f"{root}/{rel}", "--overwrite", "-p", PROFILE)
        else:   # a single file (an app source): imported as a workspace file, not a notebook
            sh("databricks", "workspace", "mkdirs", f"{root}/{os.path.dirname(rel)}", "-p", PROFILE)
            sh("databricks", "workspace", "import", f"{root}/{rel}", "--file", os.path.join(REPO, rel), "--format", "RAW", "--overwrite", "-p", PROFILE)
    nb = f"{root}/notebooks/awards/enrich"
    common = dict(state_prefix=PARAMS["state_prefix"], dry_run="false")
    tasks = [
        ("build_queue", "BuildAwardQueue", [], dict(common, awards_table=PARAMS["awards_table"], modal_queue_table=PARAMS["modal_queue_table"],
                                                    topics_target=PARAMS["topics_target"], keywords_target=PARAMS["keywords_target"],
                                                    sdgs_target=PARAMS["sdgs_target"], force_ids_file=f"{nb}/sample/{PARAMS['ids']}")),
        ("topics", "ScoreAwardTopics", ["build_queue"], dict(common, modal_queue_table=PARAMS["modal_queue_table"], target=PARAMS["topics_target"],
                                                             volume_dir=PARAMS["topics_volume_dir"], warehouse_id=warehouse, max_usd=PARAMS["topics_max_usd"],
                                                             approved_warehouse=os.environ.get("AWT_WAREHOUSE_OK", "").strip())),
        ("keywords", "ScoreAwardKeywords", ["build_queue"], dict(common, modal_queue_table=PARAMS["modal_queue_table"], target=PARAMS["keywords_target"],
                                                                 awards_table=PARAMS["awards_table"], volume_dir=PARAMS["keywords_volume_dir"],
                                                                 warehouse_id=warehouse, max_usd=PARAMS["keywords_max_usd"],
                                                                 approved_warehouse=os.environ.get("AWT_WAREHOUSE_OK", "").strip())),
        ("sdgs", "ScoreAwardSdgs", ["build_queue"], dict(common, target=PARAMS["sdgs_target"], awards_table=PARAMS["awards_table"],
                                                         max_usd=PARAMS["sdgs_max_usd"])),
    ]
    tasks = [t for t in tasks if t[0] in PARAMS["tasks"]]
    spec = {"run_name": f"awt-{run} {commit} ({os.environ['AWT_OK']})", "timeout_seconds": 7200,
            "environments": [{"environment_key": "Default", "spec": {"environment_version": "4"}}],
            "tasks": [{"task_key": k, "depends_on": [{"task_key": d} for d in deps], "environment_key": "Default",
                       "notebook_task": {"notebook_path": f"{nb}/{path}", "source": "WORKSPACE", "base_parameters": p}}
                      for k, path, deps, p in tasks]}
    spec_file = os.path.join(tempfile.gettempdir(), f"awt-{run}-run-{commit}.json")   # outside the checkout: it stays clean
    json.dump(spec, open(spec_file, "w"), indent=1)
    run = json.loads(sh("databricks", "jobs", "submit", "--no-wait", "--json", f"@{spec_file}", "-p", PROFILE, "-o", "json"))
    run_id = run["run_id"]
    print(f"submitted run {run_id}; spec {spec_file}", flush=True)
    while True:
        r = json.loads(sh("databricks", "jobs", "get-run", str(run_id), "-p", PROFILE, "-o", "json"))
        st = r["state"]
        print(time.strftime("%H:%M:%S"), st.get("life_cycle_state"), st.get("result_state", ""),
              " ".join(f"{t['task_key']}={t['state'].get('result_state', t['state'].get('life_cycle_state'))}" for t in r.get("tasks", [])), flush=True)
        if st.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            print(r.get("run_page_url", ""))
            raise SystemExit(0 if st.get("result_state") == "SUCCESS" else 1)
        time.sleep(60)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    which = sys.argv[2] if len(sys.argv) > 2 else "sample"
    wh = os.environ.get("AWT_WAREHOUSE", "").strip()
    if which not in RUNS or mode not in ("check", "go"):
        raise SystemExit("usage: sample_run.py check|go [sample|tiny]")
    check(wh, which) if mode == "check" else go(wh, which)
