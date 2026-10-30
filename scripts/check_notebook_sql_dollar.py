#!/usr/bin/env python3
"""Fail when notebook SQL holds a dollar sign that a Databricks job run would blank (oxjob #1932).

A job run treats `$1`, `$name` and `${name}` in notebook SQL as legacy widget parameters and sends ''
in their place, while an interactive or API run sends the text as written. So '$2 $1' in a
regexp_replace works when you test it and becomes ' ' every night (MatchAuthors' 'Last, First' flip,
17 Jul - 30 Oct 2026: weekly author-id churn on ~25K profiles; ProduceNihPubmedRelinks' NIH key
build; #1910's title cleaner). System query history shows what the warehouse received.

Write the backreference so the SQL text has no such dollar: concat(chr(36), '2 ', chr(36), '1')
is folded to '$2 $1' by the optimizer. A backslash before the dollar ('\\\\$1') also survives.

Checked: SQL cells of .ipynb notebooks (SQL notebooks and %sql cells), Databricks SQL source
notebooks (.sql), %sql cells of Python source notebooks, and plain .sql files under notebooks/.
Skipped: SQL comments, `$$` delimiters, Python comments inside a `$$ ... $$` function body.

usage: check_notebook_sql_dollar.py [ROOT ...]   (default: notebooks)
       check_notebook_sql_dollar.py --self-test
"""
import json
import os
import re
import sys

BAD_NEXT = re.compile(r"[0-9A-Za-z_{]")
SKIP_MAGIC = ("%md", "%python", "%py", "%scala", "%r", "%sh", "%pip", "%run", "%fs", "%conda")


def scan_sql(text):
    """Yield (line_no, snippet) for each dollar a job run would blank. Line numbers are 1-based within text."""
    i, n, line = 0, len(text), 1
    state = "code"            # code | sq | dq | bt | line_comment | block_comment | body
    body_state = "code"       # inside $$ ... $$: code | sq | dq | comment (Python)
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if c == "\n":
            line += 1
            if state == "line_comment":
                state = "code"
            if state == "body" and body_state == "comment":
                body_state = "code"
            i += 1
            continue
        if state == "line_comment":
            i += 1
            continue
        if state == "block_comment":
            if c == "*" and nxt == "/":
                state, i = "code", i + 2
            else:
                i += 1
            continue
        if c == "$" and nxt == "$":           # $$ delimiter opens or closes a function body
            if state == "code":
                state, body_state = "body", "code"
            elif state == "body" and body_state == "code":
                state = "code"
            i += 2
            continue
        if c == "$" and BAD_NEXT.match(nxt) and not (i > 0 and text[i - 1] == "\\"):
            if not (state == "body" and body_state == "comment"):
                start = text.rfind("\n", 0, i) + 1
                end = text.find("\n", i)
                yield line, text[start:end if end >= 0 else n].strip()[:160]
            i += 1
            continue
        if state == "body":
            if body_state == "code":
                if c == "#":
                    body_state = "comment"
                elif c in "'\"":
                    body_state, quote = "str", c
            elif body_state == "str":
                if c == "\\":
                    i += 2
                    continue
                if c == quote:
                    body_state = "code"
            i += 1
            continue
        if state == "code":
            if c == "-" and nxt == "-":
                state, i = "line_comment", i + 2
                continue
            if c == "/" and nxt == "*":
                state, i = "block_comment", i + 2
                continue
            if c == "'":
                state = "sq"
            elif c == '"':
                state = "dq"
            elif c == "`":
                state = "bt"
            i += 1
            continue
        # inside a quoted string or identifier
        if c == "\\" and state in ("sq", "dq"):
            i += 2
            continue
        if (state == "sq" and c == "'") or (state == "dq" and c == '"') or (state == "bt" and c == "`"):
            state = "code"
        i += 1


def sql_cells(path):
    """Yield (label, sql_text) for each SQL cell in a notebook or SQL file."""
    with open(path, encoding="utf-8", errors="replace") as f:
        txt = f.read()
    if path.endswith(".ipynb"):
        nb = json.loads(txt)
        md = nb.get("metadata", {})
        lang = (md.get("application/vnd.databricks.v1+notebook", {}).get("language")
                or md.get("language_info", {}).get("name") or "python")
        for k, cell in enumerate(nb.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            src = cell.get("source", "")
            src = "".join(src) if isinstance(src, list) else src
            head = src.lstrip()
            if head.startswith("%sql"):
                yield f"cell {k}", src.split("\n", 1)[1] if "\n" in src else ""
            elif lang == "sql" and not head.startswith(SKIP_MAGIC):
                yield f"cell {k}", src
    elif path.endswith(".sql"):
        if txt.startswith("-- Databricks notebook source"):
            for k, cell in enumerate(txt.split("-- COMMAND ----------")):
                lines = cell.split("\n")
                magic = [l for l in lines if l.startswith("-- MAGIC")]
                if not magic or magic[0].strip() == "-- MAGIC %sql":
                    yield f"cell {k}", "\n".join(l for l in lines if not l.startswith("-- MAGIC"))
        else:
            yield "file", txt
    elif path.endswith(".py") and txt.startswith("# Databricks notebook source"):
        for k, cell in enumerate(txt.split("# COMMAND ----------")):
            lines = cell.split("\n")
            magic = [l for l in lines if l.startswith("# MAGIC")]
            if magic and magic[0].strip() == "# MAGIC %sql":
                body = [l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lines]
                yield f"cell {k}", "\n".join(b for b in body if b.strip() != "%sql")


def check(roots):
    problems = []
    for root in roots:
        for dp, dn, fn in os.walk(root):
            dn[:] = [d for d in dn if not d.startswith(".")]
            for f in sorted(fn):
                if not f.endswith((".ipynb", ".sql", ".py")):
                    continue
                p = os.path.join(dp, f)
                try:
                    cells = list(sql_cells(p))
                except (ValueError, UnicodeError) as e:
                    problems.append(f"{p}: could not read ({e})")
                    continue
                for label, sql in cells:
                    per_line = {}
                    for ln, snippet in scan_sql(sql):
                        per_line.setdefault(ln, [snippet, 0])[1] += 1
                    for ln, (snippet, k) in per_line.items():
                        where = f"{label} line {ln}" if label != "file" else f"line {ln}"
                        problems.append(f"{p}: {where}: {k} x: {snippet}")
    return problems


def self_test():
    cases = {
        "SELECT regexp_replace(x, '^([^,]+),(.*)$', '$2 $1')": 2,
        "SELECT regexp_replace(x, p, concat(chr(36), '2 ', chr(36), '1'))": 0,
        "SELECT '${env}' AS e": 1,
        "SELECT x -- costs $1 to run\nFROM t": 0,
        "/* $1 */ SELECT 1": 0,
        "SELECT regexp_replace(n, '([.])', '\\\\\\\\$1')": 0,
        "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$\n# MARC $5 field\nreturn s  # $a\n$$": 0,
        "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$\nreturn re.sub('(a)', '$1', s)\n$$": 1,
        "SELECT get_json_object(j, '$.a'), '$' AS d, 'x$' AS e": 0,
        "SELECT 'it''s $1'": 1,
        "COMMENT 'MARC fields: $a kept'": 1,
    }
    bad = [(sql, want, len(list(scan_sql(sql)))) for sql, want in cases.items() if len(list(scan_sql(sql))) != want]
    for sql, want, got in bad:
        print(f"self-test FAIL: want {want} got {got}: {sql!r}")
    print(f"self-test: {len(cases) - len(bad)}/{len(cases)} passed")
    return not bad


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(0 if self_test() else 1)
    probs = check(sys.argv[1:] or ["notebooks"])
    for p in probs:
        print(p)
    if probs:
        print(f"\n{len(probs)} line(s) of notebook SQL hold a dollar sign that a job run would blank. Build the text at run time "
              f"instead, e.g. concat(chr(36), '2 ', chr(36), '1') for '$2 $1' (oxjob #1932).")
        sys.exit(1)
    print("notebook SQL: no dollar sign a job run would blank")
