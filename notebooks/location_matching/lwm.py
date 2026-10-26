"""Match Locations To Works (oxjob #1668): shared helpers for the job's notebooks. SQL files run on a SQL warehouse through the
statement API (PROD-WH by default, the warehouse End 2 End's SQL tasks use), one statement at a time (split on a line holding
only `;`), with __KEY__ placeholders filled from pins."""
import datetime as dt
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

PROD_WH = "4d37bf76372a2901"
_w = None


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def client():
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def run_statement(stmt, warehouse=PROD_WH, max_rows=60):
    """Run one statement, poll until done, raise on failure; return up to max_rows result rows as dicts."""
    w, t = client(), time.time()
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=warehouse, wait_timeout="0s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        for attempt in range(6):   # the API's read timeout is transient; the statement keeps running server-side
            try:
                r = w.statement_execution.get_statement(r.statement_id)
                break
            except Exception as e:
                log(f"poll retry {attempt + 1}: {str(e)[:120]}")
                time.sleep(30)
        else:
            raise RuntimeError(f"gave up polling statement {r.statement_id}")
    body = "\n".join(l for l in stmt.splitlines() if not l.lstrip().startswith("--")).lstrip()
    log(f"{r.status.state.value} {time.time() - t:.0f}s  {body[:90]!r}")
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"statement failed: {r.status.error}\n{body[:500]}")
    rows = []
    if r.result and r.result.data_array:
        cols = [c.name for c in r.manifest.schema.columns]
        rows = [dict(zip(cols, row)) for row in r.result.data_array[:max_rows]]
    return rows


def run_sql_file(path, pins, warehouse=PROD_WH):
    """Run every statement of a SQL file with __KEY__ placeholders filled; print the rows of SELECT statements."""
    text = open(path).read()
    for k, v in pins.items():
        text = text.replace(f"__{k}__", str(v))
    left = re.findall(r"__[A-Z0-9]+__", "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--")))
    assert not left, f"{path}: unfilled {sorted(set(left))}"
    out = []
    for stmt in [s.strip() for s in text.split("\n;\n") if s.strip()]:
        rows = run_statement(stmt, warehouse)
        for row in rows:
            print("   ", row)
        out = rows
    return out
