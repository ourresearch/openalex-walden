#!/usr/bin/env python3
"""Fail when a notebook that a job runs on serverless compute caches a DataFrame (oxjob #1642, #1231).

Serverless compute refuses Spark's caching APIs: `df.cache()` / `df.persist()` raise
'[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute', and so does SQL `CACHE TABLE`.
A notebook tested on a classic cluster or by hand passes, then fails the first time its job runs. 27 Nov 2026: the new
OpenAPC job (#1231) failed all three attempts of its first scheduled run on one `.cache()`, an hour before the End 2 End
that was to read its table (walden ba839863 dropped it). This check fails the commit instead.

A task runs on serverless when its notebook_task names no warehouse_id and the task names no job_cluster_key,
existing_cluster_id or new_cluster. Drop the cache (Spark recomputes a deterministic plan for each action), or write the
intermediate to a table (openalex.transient) and read it back.

usage: check_serverless_no_cache.py [REPO_ROOT]   (default: .)
       check_serverless_no_cache.py --self-test
"""
import glob
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_warehouse_notebooks_sql_only import TASK, notebook_file, notebook_task_keys  # noqa: E402

CACHE = re.compile(r"\.(cache|persist)\(|^\s*(?:--\s*MAGIC\s+|#\s*MAGIC\s+)?CACHE\s+(?:LAZY\s+)?TABLE\b", re.I)
CLUSTER = re.compile(r"^\s*(job_cluster_key|existing_cluster_id|new_cluster):", re.M)


def serverless_notebook_tasks(yaml_text):
    """[(task_key, notebook_path)] for every notebook task in a job YAML that runs on serverless compute."""
    out = []
    found = list(TASK.finditer(yaml_text))
    if not found:
        return out
    indent = min(len(m.group(0)) - len(m.group(0).lstrip()) for m in found)
    starts = [m for m in found if len(m.group(0)) - len(m.group(0).lstrip()) == indent]
    for i, m in enumerate(starts):
        chunk = yaml_text[m.end():starts[i + 1].start() if i + 1 < len(starts) else len(yaml_text)]
        keys = notebook_task_keys(chunk)
        if keys.get("notebook_path") and not keys.get("warehouse_id") and not CLUSTER.search(chunk):
            out.append((m.group(1), keys["notebook_path"]))
    return out


def cache_lines(text):
    """[(line number, line)] of code lines that cache. Comment lines (# or --, MAGIC lines aside) and markdown are skipped."""
    out = []
    if text.lstrip().startswith("{"):  # .ipynb: number lines across code cells as one text
        import json
        lines = []
        for c in json.loads(text).get("cells", []):
            if c.get("cell_type") == "code":
                src = c.get("source", "")
                lines += ["# COMMAND ----------"] + ("".join(src) if isinstance(src, list) else src).splitlines()
    else:
        lines = text.splitlines()
    in_md = False
    for n, ln in enumerate(lines, 1):
        s = ln.strip()
        if re.match(r"^(#|--) COMMAND -+$", s):  # a new cell
            in_md = False
            continue
        if re.match(r"^(#|--)\s*MAGIC\s+%md", s) or s.startswith("%md"):
            in_md = True
        if in_md or (s.startswith(("#", "--")) and not re.match(r"^(#|--)\s*MAGIC\s", s)):
            continue
        code = re.sub(r"\s#.*$", "", ln) if not s.startswith(("--", "#")) else ln
        if CACHE.search(code):
            out.append((n, s[:120]))
    return out


def problems(root):
    out = []
    for yml in sorted(glob.glob(os.path.join(root, "jobs", "*.yaml")) + glob.glob(os.path.join(root, "jobs", "*.yml"))):
        for key, path in serverless_notebook_tasks(open(yml, encoding="utf-8").read()):
            f = notebook_file(root, path)
            if not f:
                continue
            for n, ln in cache_lines(open(f, encoding="utf-8").read()):
                out.append(f"{os.path.relpath(f, root)} line {n}: '{ln}': {os.path.relpath(yml, root)} task {key} runs "
                           f"this notebook on serverless, which refuses cache/persist (PERSIST TABLE). Drop the cache, or "
                           f"write the intermediate to a table and read it back.")
    return out


def self_test():
    job = ("resources:\n  jobs:\n    j:\n      tasks:\n"
           "        - task_key: A\n          notebook_task:\n            notebook_path: notebooks/x/A\n"
           "          environment_key: Default\n"
           "        - task_key: B\n          job_cluster_key: c\n          notebook_task:\n            notebook_path: notebooks/x/A\n"
           "        - task_key: C\n          depends_on:\n            - task_key: A\n"
           "          notebook_task:\n            notebook_path: notebooks/x/C\n            warehouse_id: abc\n"
           "        - task_key: D\n          notebook_task:\n            notebook_path: notebooks/x/D\n")
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "jobs"))
        os.makedirs(os.path.join(d, "notebooks", "x"))
        open(os.path.join(d, "jobs", "j.yaml"), "w").write(job)
        # A: serverless (task A) and classic (task B): .cache() on a code line is caught once, for A; the comment is not
        open(os.path.join(d, "notebooks", "x", "A.py"), "w").write(
            "# Databricks notebook source\n# No .cache(): serverless\ndf = spark.table('t').cache()\n")
        # C: on a warehouse: not this check's business
        open(os.path.join(d, "notebooks", "x", "C.sql"), "w").write("CACHE TABLE t\n")
        # D: serverless, clean, plus a markdown mention
        open(os.path.join(d, "notebooks", "x", "D.py"), "w").write(
            "# Databricks notebook source\n# MAGIC %md\n# MAGIC avoid .cache() here\n\n# COMMAND ----------\n\nx = 1\n")
        got = problems(d)
        assert len(got) == 1, got
        assert got[0].startswith("notebooks/x/A.py line 3:") and "task A " in got[0], got
    print("self-test OK")


def main():
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    bad = problems(root)
    for b in bad:
        print(f"FAIL {b}")
    if bad:
        return 1
    print("serverless notebooks: no cache/persist")
    return 0


if __name__ == "__main__":
    sys.exit(main())
