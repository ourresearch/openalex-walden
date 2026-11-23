#!/usr/bin/env python3
"""Fail when notebook SQL calls a function Databricks doesn't have (oxjob #2342).

Night 2026-11-21's End 2 End lost Sync_Work_Field_Curations, and with it every task after Works_Base, to
`[UNRESOLVED_ROUTINE] Cannot resolve routine strpos`: f717f727 (#2303) used Postgres' strpos, which Databricks
doesn't have (instr / locate / position are its names). Nothing runs a notebook's SQL between the push and the
night, so the first warehouse to see the cell was the 05:00 UTC run. Only a hand repair at 01:39 CT kept the
night from publishing nothing. This check fails the commit instead.

A call is an unqualified name followed by `(` in SQL code (strings, comments, backtick names and $$ ... $$ function
bodies don't count). It passes when the name is one of:
  - a Databricks built-in (scripts/notebook_sql_functions.txt, `builtin` lines: SHOW SYSTEM FUNCTIONS),
  - SQL syntax that looks like a call (SYNTAX below: CAST-like forms, types such as DECIMAL(10, 2), keywords),
  - a function in catalog openalex (`openalex` lines of the same file, any schema: notebooks may USE a schema),
  - a function some notebook or SQL file in the repo creates (CREATE [OR REPLACE] [TEMPORARY] FUNCTION, or a Python
    cell's spark.udf.register("name", ...)),
  - a name the statement itself declares right before `(` (CREATE TABLE t (...), INSERT INTO t (...), a CTE's
    column list, a named window).
Qualified calls (schema.fn(...)) aren't checked: the catalog decides those.

If a new Databricks built-in trips it, refresh the list:  check_notebook_sql_functions.py --refresh
(needs DATABRICKS_CONFIG_FILE / DATABRICKS_CONFIG_PROFILE with SQL warehouse access, ~$0.10 on desk-default).

usage: check_notebook_sql_functions.py [ROOT ...]   (default: notebooks)
       check_notebook_sql_functions.py --self-test
       check_notebook_sql_functions.py --refresh
"""
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_notebook_sql_dollar import sql_cells  # noqa: E402  (same cell extraction as the dollar check)

LIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebook_sql_functions.txt")

# SQL that is written like a call but isn't in SHOW SYSTEM FUNCTIONS: special forms, data types, and keywords a
# parenthesis can follow.
SYNTAX = set("""
try_cast timestampadd timestampdiff identifier extract trim overlay substring position
decimal dec numeric varchar char array map struct interval
in as values over exists using on and or not from join select where by tblproperties options rollup cube sets
pivot unpivot tablesample lateral window table returns then else when case is like ilike rlike regexp any all some
union intersect except minus distinct having limit between escape within group rows range preceding following
do while elseif repeat until loop call key references check default generated constraint matched insert update
set return recursive is_member is_account_group_member partitioned partition cluster zorder location comment
for into to with lambda filter nulls first last order asc desc qualify mask row columns column replace
immediate var variable environment
""".split())

DECLARERS = {"table", "view", "into", "function", "exists", "with", "window", "procedure"}

CREATE_FN = re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                       r"(?:[`\w]+\.){0,2}`?(\w+)`?", re.I)
PY_UDF = re.compile(r"udf\.register\(\s*[\\]?[\"'](\w+)")   # a Python cell's spark.udf.register("name", ...)
CALL = re.compile(r"(?<![\w.`$}])([A-Za-z_]\w*)\s*\(")   # after } is a templated name: {P}award_runs (
WORD = re.compile(r"([A-Za-z_]\w*)\s*$")


def code_only(text):
    """text with strings, comments, backtick names and $$ bodies blanked to spaces (newlines kept)."""
    out, i, n = [], 0, len(text)
    state, quote = "code", ""
    while i < n:
        c = text[i]
        if state == "code":
            if text.startswith("--", i):
                state = "comment"
            elif text.startswith("/*", i):
                state = "block"
            elif text.startswith("$$", i):
                state = "body"
                out.append("  ")
                i += 2
                continue
            elif c in "'\"`":
                state, quote = "str", c
            out.append(c if state == "code" else " ")
            i += 1
            continue
        if state == "comment" and c == "\n":
            state = "code"
        elif state == "block" and text.startswith("*/", i):
            out.append("  ")
            state, i = "code", i + 2
            continue
        elif state == "body" and text.startswith("$$", i):
            out.append("  ")
            state, i = "code", i + 2
            continue
        elif state == "str":
            if c == "\\" and quote != "`":
                out.append("  " if text[i + 1:i + 2] != "\n" else " \n")
                i += 2
                continue
            if c == quote:
                if text[i + 1:i + 2] == quote:      # '' inside a string
                    out.append("  ")
                    i += 2
                    continue
                state = "code"
        out.append("\n" if c == "\n" else " ")
        i += 1
    return "".join(out)


def calls(sql, known=frozenset()):
    """Yield (line_no, name) for each unqualified call to a name not in known that its own statement doesn't declare."""
    code = code_only(sql)
    for m in CALL.finditer(code):
        name = m.group(1)
        if name.lower() in known:
            continue
        before = WORD.search(code, 0, m.start())
        prev = before.group(1).lower() if before else ""
        if prev in DECLARERS or prev == "as" or code[:m.start()].rstrip().endswith(")"):
            continue    # declared here, or a table alias's column list: VALUES ... AS t(a, b) / (...) v(a, b)
        # a CTE with a column list, `name (a, b) AS (`, declares name
        tail, depth, j = code[m.end():m.end() + 2000], 1, 0
        while j < len(tail) and depth:
            depth += {"(": 1, ")": -1}.get(tail[j], 0)
            j += 1
        if not depth and re.match(r"\s*AS\s*\(", tail[j:], re.I):
            continue
        yield code.count("\n", 0, m.start()) + 1, name


def known_names(roots):
    names = set(SYNTAX)
    with open(LIST, encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            kind, name = line.split()
            names.add(name.rsplit(".", 1)[-1].lower())
    for root in roots:
        for dp, dn, fn in os.walk(root):
            dn[:] = [d for d in dn if not d.startswith(".")]
            for f in fn:
                if f.endswith((".ipynb", ".sql", ".py")):
                    with open(os.path.join(dp, f), encoding="utf-8", errors="replace") as h:
                        txt = h.read().replace("\\n", "\n")
                        names.update(m.group(1).lower() for m in CREATE_FN.finditer(txt))
                        names.update(m.group(1).lower() for m in PY_UDF.finditer(txt))
    return names


def check(roots, repo_roots=("notebooks", "jobs")):
    known = known_names([r for r in repo_roots if os.path.isdir(r)] + list(roots))
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
                    for (ln, name), k in Counter(calls(sql, known)).items():
                        where = f"{label} line {ln}" if label != "file" else f"line {ln}"
                        problems.append(f"{p}: {where}: {name}(  ({k}x)")
    return problems


def refresh():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    wh = os.environ.get("WAREHOUSE_ID", "1e51e877ff5308ca")   # desk-default

    def rows(q):
        r = w.statement_execution.execute_statement(statement=q, warehouse_id=wh, wait_timeout="50s")
        if str(r.status.state).split(".")[-1] != "SUCCEEDED":
            sys.exit(f"refresh failed: {r.status}")
        return r.result.data_array or []
    builtin = sorted({r[0].rsplit(".", 1)[-1].lower() for r in rows("SHOW SYSTEM FUNCTIONS") if re.match(r"[A-Za-z_]", r[0])})
    mine = sorted({f"{s}.{n}".lower() for s, n in rows("SELECT routine_schema, routine_name FROM system.information_schema.routines "
                                                         "WHERE routine_catalog = 'openalex'")})
    with open(LIST, encoding="utf-8") as f:
        head = "".join(l for l in f if l.startswith("#"))
    with open(LIST, "w", encoding="utf-8") as f:
        f.write(head + "".join(f"builtin {x}\n" for x in builtin) + "".join(f"openalex {x}\n" for x in mine))
    print(f"{LIST}: {len(builtin)} built-ins, {len(mine)} openalex functions")


def self_test():
    cases = {
        "SELECT strpos(a, 'x') FROM t": ["strpos"],                                    # night 11-21
        "SELECT instr(a, 'x'), locate('x', a) FROM t": [],
        "SELECT TRY_CAST(x AS INT), CAST(y AS DECIMAL(10, 2)) FROM t": [],
        "WITH a AS (SELECT 1), b (x, y) AS (SELECT 1, 2) SELECT * FROM a JOIN b ON (a.x = b.x)": [],
        "CREATE OR REPLACE TABLE openalex.works.t (id BIGINT) AS SELECT 1": [],
        "CREATE TABLE t2 (id BIGINT)": [],
        "INSERT INTO t (a, b) VALUES (1, 2)": [],
        "SELECT 'strpos(x)' AS s -- strpos(y)\n/* strpos(z) */ FROM t": [],
        "SELECT `strpos(x)` FROM t": [],
        "SELECT openalex.common.extract_grant_doi(x), common.f(x) FROM t": [],
        "SELECT names_compatible(a, b) FROM t": [],                                        # openalex.authors UDF
        "CREATE TEMPORARY FUNCTION my_fn(x INT) RETURNS INT RETURN x + 1;\nSELECT my_fn(1)": [],
        "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$\nreturn strpos(s)\n$$": [],
        "SELECT TRANSFORM(a, x -> string_to_array(x, ',')) FROM t": ["string_to_array"],  # Postgres again
        "SELECT IF(a, 1, 2), COUNT(*) FILTER (WHERE b) OVER w FROM t WINDOW w AS (PARTITION BY c)": [],
        "SELECT x FROM t WHERE x IN (1, 2) AND EXISTS (SELECT 1)": [],
        "SELECT * EXCEPT (a) FROM t": [],
        "SELECT 'it''s', strpos(a, 'b') FROM t": ["strpos"],
        "MERGE INTO t USING s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)": [],
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t": [],
        "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS v(id, name)": [],                  # alias column lists
        "SELECT * FROM VALUES ('x', 1) v(k, fid)": [],
        "CREATE TABLE IF NOT EXISTS {P}award_runs (id STRING)": [],                     # templated table name
        "EXECUTE IMMEDIATE ('SELECT 1'); SET VAR (a, b) = (SELECT 1, 2)": [],
        "SELECT regexp_matches(a, 'x') FROM t": ["regexp_matches"],                     # Postgres
    }
    failed = 0
    old = known_names([])

    for sql, want in cases.items():
        extra = {m.group(1).lower() for m in CREATE_FN.finditer(sql)}
        got = [n for _, n in calls(sql, old | extra)]
        if got != want:
            failed += 1
            print(f"self-test FAIL: want {want} got {got}: {sql!r}")
    print(f"self-test: {len(cases) - failed}/{len(cases)} passed")
    return not failed


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(0 if self_test() else 1)
    if sys.argv[1:] == ["--refresh"]:
        refresh()
        sys.exit(0)
    probs = check(sys.argv[1:] or ["notebooks"])
    for p in probs:
        print(p)
    if probs:
        print(f"\n{len(probs)} call(s) in notebook SQL name a function Databricks doesn't have (Postgres' strpos is instr "
              f"here). If it is a new Databricks built-in, run this script with --refresh; if it's ours, create it in a "
              f"notebook or qualify it (oxjob #2342).")
        sys.exit(1)
    print("notebook SQL: every function called exists")
