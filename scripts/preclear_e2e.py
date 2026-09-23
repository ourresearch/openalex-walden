#!/usr/bin/env python3
"""Pre-clear the next scheduled Walden End 2 End run for a planned bulk change.

Writes one row per flag to `openalex.works.e2e_overrides`. Every override gate in
`walden_end2end` resolves its flag through `openalex.works.e2e_override_active(flag, param)`,
which is true when the job parameter is "true" OR a row for that flag has a window covering
the moment the gate runs. So a row here makes the SCHEDULED nightly behave exactly like a
manual `run-now` with the parameter set, with no second run, nothing to pause and nothing to
cancel. Runbook: docs/runbooks/end2end-guardrails-override.md.

Rules (they do not bend): run this only after Jason's explicit yes for this specific run, and
only for the flag class that will actually fire. `guardrails_override` never authorises a mass
delete or an oversized feed; those have their own flags.

    scripts/preclear_e2e.py --reason "oxjob #1309 fallback cleanup, 10.9M works" --by jason
    scripts/preclear_e2e.py --flags guardrails_override,deleted_works_guard_override --reason ... --by ...
    scripts/preclear_e2e.py --night 2026-09-24 --reason ... --by ...   # CT evening date; default = next 05:00 UTC
    scripts/preclear_e2e.py --list                                     # rows whose window has not ended
    scripts/preclear_e2e.py --cancel                                   # end every open window now

Needs the `databricks` CLI authenticated on this machine. Warehouse: $DATABRICKS_SQL_WAREHOUSE_ID
or the serverless default.
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone

TABLE = "openalex.works.e2e_overrides"
FLAGS = (
    "guardrails_override",
    "deleted_works_guard_override",
    "deleted_locations_guard_override",
    "wunpaywall_guard_override",
)
NIGHTLY_UTC_HOUR = 5          # walden_end2end schedule: 0 0 5 * * ? UTC
WINDOW_AFTER_START_H = 18     # a full run is ~7 h; 18 h also covers a morning repair run, and ends before the next nightly
DEFAULT_WAREHOUSE = "69a583ace3bdc8d0"


def sql(statement, warehouse):
    body = json.dumps({"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"})
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body],
                         capture_output=True, text=True)
    try:
        d = json.loads(out.stdout)
    except json.JSONDecodeError:
        sys.exit(f"databricks api failed: {out.stdout[:300]} {out.stderr[:300]}")
    sid = d.get("statement_id")
    for _ in range(60):
        if d.get("status", {}).get("state") in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break
        time.sleep(3)
        o = subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}"], capture_output=True, text=True)
        d = json.loads(o.stdout)
    st = d.get("status", {})
    if st.get("state") != "SUCCEEDED":
        sys.exit(f"SQL {st.get('state')}: {(st.get('error') or {}).get('message', '')[:400]}")
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    return cols, d.get("result", {}).get("data_array", []) or []


def next_run_start(night):
    """05:00 UTC of the run that follows the CT evening `night` (or the next one from now)."""
    if night:
        d = date.fromisoformat(night) + timedelta(days=1)
        return datetime(d.year, d.month, d.day, NIGHTLY_UTC_HOUR, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    start = now.replace(hour=NIGHTLY_UTC_HOUR, minute=0, second=0, microsecond=0)
    return start if start > now else start + timedelta(days=1)


def q(s):
    return "'" + s.replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flags", default="guardrails_override", help="comma-separated; default guardrails_override")
    ap.add_argument("--reason", help="what wave, how many works, oxjob id")
    ap.add_argument("--by", help="who gave the per-run yes")
    ap.add_argument("--night", help="CT evening date (YYYY-MM-DD) the run belongs to; default = the next 05:00 UTC")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--cancel", action="store_true", help="end every open window now")
    ap.add_argument("--warehouse", default=None)
    a = ap.parse_args()
    import os
    wh = a.warehouse or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or DEFAULT_WAREHOUSE

    if a.list:
        cols, rows = sql(f"SELECT flag, valid_from, valid_until, reason, requested_by, created_at FROM {TABLE} "
                         f"WHERE valid_until > current_timestamp() ORDER BY valid_from", wh)
        print(" | ".join(cols)) if rows else print("no open windows")
        for r in rows:
            print(" | ".join(str(x) for x in r))
        return
    if a.cancel:
        cols, rows = sql(f"UPDATE {TABLE} SET valid_until = current_timestamp() WHERE valid_until > current_timestamp()", wh)
        print(f"closed {rows[0][0] if rows else 0} window(s)")
        return

    flags = [f.strip() for f in a.flags.split(",") if f.strip()]
    bad = [f for f in flags if f not in FLAGS]
    if bad:
        sys.exit(f"unknown flag(s) {bad}; known: {', '.join(FLAGS)}")
    if not a.reason or not a.by:
        sys.exit("--reason and --by are required (the reason is what the morning reader sees in Guardrails' output)")

    start = next_run_start(a.night)
    until = start + timedelta(hours=WINDOW_AFTER_START_H)
    now = datetime.now(timezone.utc)
    if until <= now:
        sys.exit(f"that run's window ({until:%Y-%m-%d %H:%M} UTC) has already ended")
    vals = ", ".join(
        f"(current_timestamp(), TIMESTAMP '{until:%Y-%m-%d %H:%M:%S}', {q(f)}, {q(a.reason)}, {q(a.by)}, current_timestamp())"
        for f in flags
    )
    sql(f"INSERT INTO {TABLE} (valid_from, valid_until, flag, reason, requested_by, created_at) VALUES {vals}", wh)
    print(f"pre-cleared {', '.join(flags)} for the End 2 End run starting {start:%Y-%m-%d %H:%M} UTC "
          f"({(start - timedelta(hours=5)):%Y-%m-%d %H:%M} CDT); window ends {until:%Y-%m-%d %H:%M} UTC")
    print("nothing to pause, nothing to cancel; check `SELECT ... FROM openalex.works.guardrails_history` in the morning "
          "(note = 'override:table').")


if __name__ == "__main__":
    main()
