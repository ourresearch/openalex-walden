#!/usr/bin/env python3
"""Fail when a notebook that a job runs on a SQL warehouse holds a cell that is not SQL (oxjob #1642).

A notebook task with a `warehouse_id` runs on a Databricks SQL warehouse, which executes only SQL cells. Any other cell
(%python, %scala, %r, %sh, %pip, or a code cell in a notebook whose default language is Python) fails the whole task at
once: 'Unsupported cell during execution. SQL warehouses only support executing SQL cells.' (test run 174750944669027).
It happened twice in a week, both times to CreateWorksEnriched, which Works_Enriched runs on a warehouse: #1156 / #1444's
byline cell (17 Nov 2026, held by 50ae3fca) and #2075's contributor_roles guard (20 Nov 2026, rewritten as SQL scripting
by 1ea01389). Both cells were dry-run on serverless compute, where Python works, so only the End 2 End shepherd's
pre-flight caught them, hours before the night. This check fails the commit instead.

Write the cell in SQL (SQL scripting has BEGIN / DECLARE / IF, as PublishWorks uses), or give it its own task on Python
compute. %sql, %md and %run cells are fine.

usage: check_warehouse_notebooks_sql_only.py [REPO_ROOT]   (default: .)
       check_warehouse_notebooks_sql_only.py --self-test
"""
import glob
import json
import os
import re
import sys
import tempfile

OK_MAGIC = {"sql", "md", "run"}
# Python notebooks that already named a warehouse when this check landed (20 Nov 2026), outside the End 2 End: listed
# so the check can block new cases now. Each is either a task that fails when it runs or one whose warehouse_id should
# move under base_parameters; drop its line once its job is fixed.
KNOWN = {("refresh_work_awards.yaml", "Create_Awards"), ("refresh_work_awards.yaml", "Work_Awards"),
         ("sync_awards_to_elasticsearch.yaml", "create_awards_api")}
TASK = re.compile(r"^[ \t]*- task_key:[ \t]*(\S+)", re.M)


def warehouse_notebook_tasks(yaml_text):
    """[(task_key, notebook_path)] for every notebook task in a job YAML that names a warehouse_id."""
    out = []
    found = list(TASK.finditer(yaml_text))
    if not found:
        return out
    # a depends_on entry is also '- task_key: X', indented deeper than the task definitions: split on the shallowest
    indent = min(len(m.group(0)) - len(m.group(0).lstrip()) for m in found)
    starts = [m for m in found if len(m.group(0)) - len(m.group(0).lstrip()) == indent]
    for i, m in enumerate(starts):
        chunk = yaml_text[m.end():starts[i + 1].start() if i + 1 < len(starts) else len(yaml_text)]
        keys = notebook_task_keys(chunk)
        if keys.get("notebook_path") and keys.get("warehouse_id"):
            out.append((m.group(1), keys["notebook_path"]))
    return out


def notebook_task_keys(chunk):
    """The direct children of the chunk's notebook_task mapping, {key: value}. Only direct children count: a
    `warehouse_id` under base_parameters is a parameter of a Python notebook, not the compute it runs on."""
    lines = chunk.splitlines()
    for j, ln in enumerate(lines):
        if re.match(r"^\s*notebook_task:\s*$", ln):
            base, child, keys = len(ln) - len(ln.lstrip()), None, {}
            for c in lines[j + 1:]:
                if not c.strip() or c.lstrip().startswith("#"):
                    continue
                ind = len(c) - len(c.lstrip())
                if ind <= base:
                    break
                child = ind if child is None else child
                kv = re.match(r"^\s*([A-Za-z_]+):\s*['\"]?([^'\"#]*?)['\"]?\s*(?:#.*)?$", c)
                if ind == child and kv:
                    keys[kv.group(1)] = kv.group(2).strip()
            return keys
    return {}


def notebook_file(root, path):
    p = re.sub(r"^.*?(notebooks/)", r"\1", path.lstrip("./"))
    for cand in (p, p + ".ipynb", p + ".sql", p + ".py"):
        f = os.path.join(root, cand)
        if os.path.isfile(f):
            return f
    return None


def bad_cells(text, filename):
    """[(cell index, language)] for the cells a warehouse cannot run."""
    out = []
    if filename.endswith(".ipynb"):
        nb = json.loads(text)
        default = ((nb.get("metadata") or {}).get("application/vnd.databricks.v1+notebook") or {}).get("language") \
            or ((nb.get("metadata") or {}).get("language_info") or {}).get("name") or "python"
        code = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]
        for i, c in enumerate(code):
            src = c.get("source", "")
            src = "".join(src) if isinstance(src, list) else src
            if not src.strip():
                continue
            m = re.match(r"\s*%(\w+)", src)
            lang = m.group(1).lower() if m else default.lower()
            if lang not in OK_MAGIC:
                out.append((i, lang))
    else:  # Databricks source notebook: cells split by COMMAND lines, magics on '-- MAGIC' / '# MAGIC' lines
        default = "sql" if filename.endswith(".sql") else "python"
        for i, cell in enumerate(re.split(r"^(?:--|#) COMMAND -+\s*$", text, flags=re.M)):
            body = "\n".join(ln for ln in cell.splitlines() if ln.strip() and not ln.startswith(("# Databricks notebook",
                                                                                                    "-- Databricks notebook")))
            if not body.strip():
                continue
            m = re.match(r"\s*(?:--|#) MAGIC %(\w+)", body)
            lang = m.group(1).lower() if m else default
            if lang not in OK_MAGIC:
                out.append((i, lang))
    return out


def problems(root):
    out = []
    for yml in sorted(glob.glob(os.path.join(root, "jobs", "*.yaml")) + glob.glob(os.path.join(root, "jobs", "*.yml"))):
        for key, path in warehouse_notebook_tasks(open(yml, encoding="utf-8").read()):
            f = notebook_file(root, path)
            if not f or (os.path.basename(yml), key) in KNOWN:
                continue
            for i, lang in bad_cells(open(f, encoding="utf-8").read(), f):
                out.append(f"{os.path.relpath(f, root)} code cell {i}: {lang} cell, but {os.path.relpath(yml, root)} task "
                           f"{key} runs this notebook on a SQL warehouse, which runs only SQL cells (the task fails). "
                           f"Write it in SQL (SQL scripting: BEGIN / DECLARE / IF), or give it a task on Python compute.")
    return out


def self_test():
    def nb(lang, cells):
        return json.dumps({"metadata": {"application/vnd.databricks.v1+notebook": {"language": lang}},
                           "cells": [{"cell_type": t, "source": s} for t, s in cells]})
    job = ("resources:\n  jobs:\n    j:\n      tasks:\n"
           "        - task_key: A\n          notebook_task:\n            notebook_path: notebooks/x/A\n"
           "            warehouse_id: abc\n"
           "        - task_key: B\n          notebook_task:\n            notebook_path: notebooks/x/B\n"
           "        - task_key: C\n          depends_on:\n            - task_key: B\n"
           "          notebook_task:\n            notebook_path: notebooks/x/C\n"
           "            source: GIT\n            warehouse_id: abc\n"
           "        - task_key: D\n          depends_on:\n            - task_key: C\n"
           "          notebook_task:\n            notebook_path: notebooks/x/B\n"
           "        - task_key: E\n          notebook_task:\n            notebook_path: notebooks/x/B\n"
           "            base_parameters:\n              warehouse_id: abc\n")
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "jobs"))
        os.makedirs(os.path.join(d, "notebooks", "x"))
        open(os.path.join(d, "jobs", "j.yaml"), "w").write(job)
        # A: SQL notebook with a %python cell (bad) and %md / %sql / plain SQL cells (fine)
        open(os.path.join(d, "notebooks", "x", "A.ipynb"), "w").write(nb("sql", [
            ("markdown", "# t"), ("code", "SELECT 1"), ("code", "%sql\nSELECT 2"), ("code", ["%py", "thon\nprint(1)"]),
            ("code", "%md\nnote"), ("code", "BEGIN\n  DECLARE x INT;\nEND")]))
        # B: Python cells but no warehouse (fine)
        open(os.path.join(d, "notebooks", "x", "B.ipynb"), "w").write(nb("python", [("code", "print(1)")]))
        # C: Python-default source notebook on a warehouse: a plain cell is Python (bad), a %sql cell is fine
        open(os.path.join(d, "notebooks", "x", "C.py"), "w").write(
            "# Databricks notebook source\nprint(1)\n\n# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT 1\n")
        got = problems(d)
        assert len(got) == 2, got
        assert got[0].startswith("notebooks/x/A.ipynb code cell 2: python"), got
        assert got[1].startswith("notebooks/x/C.py code cell 0: python"), got
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
    print("warehouse notebooks: every cell is SQL")
    return 0


if __name__ == "__main__":
    sys.exit(main())
