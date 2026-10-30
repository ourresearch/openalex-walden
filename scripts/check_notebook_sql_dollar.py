#!/usr/bin/env python3
r"""Fail when notebook SQL holds a dollar sign that a Databricks job run would blank (oxjob #1932).

A job run treats `$1`, `$name` and `${name}` in notebook SQL as legacy widget parameters and sends ''
in their place, while an interactive or API run sends the text as written. So '$2 $1' in a
regexp_replace works when you test it and becomes ' ' every night (MatchAuthors' 'Last, First' flip,
seen in every End 2End from 17 Oct 2026 on: weekly author-id churn on ~25K profiles;
ProduceNihPubmedRelinks' NIH key build; #1910's title cleaner). System query history shows what the
warehouse received. Databricks documents no switch to turn this off.

Write the backreference so the SQL text has no such dollar: concat(chr(36), '2 ', chr(36), '1')
is folded to '$2 $1' by the optimizer. A backslash right before the dollar also survives (the
awards notebooks' '\\\\$1').

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
from collections import Counter

DOLLAR = re.compile(r"\$[0-9A-Za-z_{]")


def scan_sql(text):
    """Yield (line_no, snippet) for each dollar a job run would blank. Line numbers are 1-based within text."""
    # state: code (SQL) | body (Python inside $$ ... $$) | str | comment | block; `after` is where a str or comment returns to
    state, after, quote = "code", "code", ""
    i, n = 0, len(text)
    while i < n:
        c, nxt = text[i], text[i + 1:i + 2]
        if state == "comment":
            if c == "\n":
                state = after
            i += 1
            continue
        if state == "block":
            state, i = ("code", i + 2) if text.startswith("*/", i) else ("block", i + 1)
            continue
        if c == "$" and nxt == "$":               # $$ opens or closes a function body; inside a string it is text
            state = {"code": "body", "body": "code"}.get(state, state)
            i += 2
            continue
        if DOLLAR.match(text, i) and text[i - 1:i] != "\\":
            start, end = text.rfind("\n", 0, i) + 1, text.find("\n", i)
            yield text.count("\n", 0, i) + 1, text[start:end if end >= 0 else n].strip()[:160]
            i += 1
            continue
        if state == "str":
            if c == "\\" and quote != "`":
                i += 2
                continue
            if c == quote:
                state = after
            i += 1
            continue
        # state is code or body
        if state == "code" and text.startswith("--", i) or state == "body" and c == "#":
            state, after = "comment", state
        elif state == "code" and text.startswith("/*", i):
            state, i = "block", i + 1
        elif c in "'\"" or (state == "code" and c == "`"):
            state, after, quote = "str", state, c
        i += 1


def source_cells(txt, prefix):
    """Cells of a Databricks source-format notebook (prefix '--' for .sql, '#' for .py) that hold SQL."""
    magic = f"{prefix} MAGIC "
    for k, cell in enumerate(txt.split(f"{prefix} COMMAND ----------")):
        lines = cell.split("\n")
        magics = [l[len(magic):].strip() for l in lines if l.startswith(f"{prefix} MAGIC")]
        if magics and magics[0] == "%sql":
            body = [l[len(magic):] if l.startswith(magic) else "" for l in lines]
            yield f"cell {k}", "\n".join(b for b in body if b.strip() != "%sql")
        elif not magics and prefix == "--":
            yield f"cell {k}", cell


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
            elif lang == "sql" and not head.startswith("%"):
                yield f"cell {k}", src
    elif path.endswith(".sql"):
        if txt.startswith("-- Databricks notebook source"):
            yield from source_cells(txt, "--")
        else:
            yield "file", txt
    elif path.endswith(".py") and txt.startswith("# Databricks notebook source"):
        yield from source_cells(txt, "#")


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
                    if not DOLLAR.search(sql):    # every finding needs this pattern; skips ~99% of cells
                        continue
                    for (ln, snippet), k in Counter(scan_sql(sql)).items():
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
    failed = 0
    for sql, want in cases.items():
        got = len(list(scan_sql(sql)))
        if got != want:
            failed += 1
            print(f"self-test FAIL: want {want} got {got}: {sql!r}")
    print(f"self-test: {len(cases) - failed}/{len(cases)} passed")
    return not failed


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
