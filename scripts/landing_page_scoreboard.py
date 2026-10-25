#!/usr/bin/env python3
"""Run the landing-page scoreboard SQL from a laptop or desk (oxjob #1789): DDL once, backfills by hand.
The nightly job runs the same SQL files from notebooks/monitoring/LandingPageScoreboard.py.

    python3 scripts/landing_page_scoreboard.py ddl        # tables, host-key functions, Zyte prices, alarm acks (idempotent)
    python3 scripts/landing_page_scoreboard.py prefixes   # rebuild the DOI-prefix fold; then re-run funnel for 14+ days
    python3 scripts/landing_page_scoreboard.py funnel --start 2026-09-01 --end 2026-10-24 [--chunk-days 7]
    python3 scripts/landing_page_scoreboard.py alerts --start 2026-09-08 --end 2026-10-24

Needs DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID (desk: ~/.zshenv). Stdlib only.
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(HERE, "..", "notebooks", "monitoring", "landing_pages")


def statements(text):
    """Split a SQL file on lines ending with ';' (our files keep one statement per block)."""
    out, cur = [], []
    for line in text.splitlines():
        if not cur and (not line.strip() or line.strip().startswith("--")):
            continue
        cur.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(cur).rstrip().rstrip(";"))
            cur = []
    if "".join(cur).strip():
        out.append("\n".join(cur))
    return out


def run(sql):
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    host = host if host.startswith("http") else "https://" + host

    def call(method, path, body=None):
        req = urllib.request.Request(host + path, method=method, data=json.dumps(body).encode() if body else None,
                                     headers={"Authorization": "Bearer " + os.environ["DATABRICKS_TOKEN"],
                                              "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.load(r)

    t0 = time.time()
    res = call("POST", "/api/2.0/sql/statements", {"statement": sql, "warehouse_id": os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
                                                   "wait_timeout": "50s", "format": "JSON_ARRAY", "disposition": "INLINE"})
    sid = res["statement_id"]
    while res["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - t0 > 3600:
            call("POST", f"/api/2.0/sql/statements/{sid}/cancel")  # don't leave a write running on the warehouse
            sys.exit(f"cancelled statement {sid} after 1 h")
        time.sleep(5)
        res = call("GET", "/api/2.0/sql/statements/" + sid)
    print(f"{res['status']['state']} {time.time() - t0:.0f}s | {sql.split(chr(10), 1)[0][:100]}", flush=True)
    if res["status"]["state"] != "SUCCEEDED":
        sys.exit(json.dumps(res["status"]))
    return res.get("result", {}).get("data_array") or []


def sql_file(name, **kw):
    text = open(os.path.join(SQL_DIR, name)).read()
    return [s.format(**kw) if kw else s for s in statements(text)]


def windows(start, end, chunk_days):
    d = start
    while d < end:
        e = min(d + dt.timedelta(days=chunk_days), end)
        yield d, e
        d = e


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["ddl", "prefixes", "funnel", "alerts", "host_state"])
    ap.add_argument("--start", type=dt.date.fromisoformat)
    ap.add_argument("--end", type=dt.date.fromisoformat, help="exclusive")
    ap.add_argument("--chunk-days", type=int, default=30, help="each funnel chunk rescans taxicab_results once")
    a = ap.parse_args()
    if a.what in ("ddl", "prefixes"):
        for f in ("tables.sql", "host_key.sql", "zyte_prices.sql", "alert_acks.sql") if a.what == "ddl" else ("doi_prefixes.sql",):
            for s in sql_file(f):
                run(s)
        return
    for s_, e_ in windows(a.start, a.end, a.chunk_days):
        for s in sql_file(f"{a.what}.sql", start=s_.isoformat(), end=e_.isoformat()):
            run(s)


if __name__ == "__main__":
    main()
