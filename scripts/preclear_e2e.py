#!/usr/bin/env python3
"""Pre-clear one Walden End 2 End run for a planned change: name how far it may move each check.

Writes one row per flag to `openalex.works.e2e_overrides`, bound to one run: the window opens at the run's 05:00 UTC
start and closes 18 h later (the run plus a same-morning repair). Every capped gate in `walden_end2end` reads the row
through one function, `openalex.works.e2e_cap(flag, metric)` / `e2e_gate(...)` (notebooks/end2end/e2e_gate.sql).

A row raises only the checks it names, to the numbers it names (oxjob #2005, 4 Nov 2026). Every other check keeps its
own line. Until then a capped guardrails_override row raised Check 1 and switched Guardrails' loss checks and the
curation decline gates off. So name every planned change on the same command line:

    --max-changed N   works this run may stamp (Guardrails Check 1; its line is 7.5M)
    --max-loss works=N,abstracts=N,...   losses (each check's line is 2M works; citations 5%; curations 10 rows)
    --max-deleted N   works that may disappear (TrackDeletedWorks / delete_works); also names works=N on the
                      night's guardrails_override row (filed or raised in place for you)
    --current-run     a repair of the run in progress: the row stays open until the next run starts

Loss names (short or full): works (works_total), abstracts (both abstract checks), abstracts_any, abstracts_ii,
affiliations, institutions, fulltext, citations (total_citations), missing_author_ids, work_author_curations,
location_curations. Numbers take K/M suffixes (6.5M). A merge of N works can drop each "works with X" count by up to N:
count the losers that carry X and name it, or Guardrails fails that check. Name the night's total with headroom (x1.2).

Rules (they do not bend): whoever pilots the change runs this (Jason, 2026-10-14): for Jason's changes the job session
that owns the change runs it on his yes, `--by jason`, and tells Casey in #dev; Casey runs it for his own. Only for the
flag class that will actually fire. A night has ONE row per flag: the first change files it, every later change raises
it in place with --raise and the night's new totals. `guardrails_override` never authorises a mass delete or an
oversized feed; those have their own flags.

    scripts/preclear_e2e.py --max-changed 12000000 --reason "oxjob #1309 fallback cleanup, 10.9M works" --by jason
    scripts/preclear_e2e.py --max-changed 22M --max-loss abstracts=6M --reason "#1800 stand-in abstracts ..." --by jason
    scripts/preclear_e2e.py --flags guardrails_override,deleted_works_guard_override --max-changed 21.5M \
        --max-deleted 7.5M --max-loss abstracts=7M,affiliations=3M,institutions=3M --reason ... --by casey
    scripts/preclear_e2e.py --raise --max-changed 34M --max-loss abstracts=7.2M --reason "+ #1528 ..." --by jason
    scripts/preclear_e2e.py --current-run --raise --max-loss works=2.5M --reason ... --by ...  # this morning's repair
    scripts/preclear_e2e.py --night 2026-09-24 --max-changed ... --reason ... --by ...  # CT evening date of a later run
    scripts/preclear_e2e.py --list                                     # rows whose window has not ended
    scripts/preclear_e2e.py --cancel                                   # end every open window now
    add --dry-run to print the SQL instead of running it

Needs the `databricks` CLI authenticated on this machine. Warehouse: $DATABRICKS_SQL_WAREHOUSE_ID
or the serverless default.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
TABLE = "openalex.works.e2e_overrides"
FLAGS = (
    "guardrails_override",
    "deleted_works_guard_override",
    "deleted_locations_guard_override",
    "wunpaywall_guard_override",
    "keep_replicas",     # sync_works: a large incremental sync keeps number_of_replicas at 1 instead of dropping to 0 (#1638)
    "hash_rebaseline",   # CreateWorksEnriched: changed works take the new content_hash without a new updated_date (#1386)
)
# The metrics a guardrails_override row may name in max_loss (Guardrails' guardrails_history names, and the curation
# decline gates). Short names a person types -> the metric names the gates read.
LOSS_METRICS = {
    "works": ["works_total"],
    "abstracts": ["works_with_abstract_any", "works_with_abstract_inverted_index"],
    "abstracts_any": ["works_with_abstract_any"],
    "abstracts_ii": ["works_with_abstract_inverted_index"],
    "affiliations": ["works_with_affiliation_strings"],
    "institutions": ["works_with_institution_ids"],
    "fulltext": ["works_with_fulltext"],
    "citations": ["total_citations"],
    "missing_author_ids": ["recent_works_missing_author_ids"],
    "work_author_curations": ["work_author_curations"],
    "location_curations": ["location_curations"],
}
for _names in list(LOSS_METRICS.values()):
    for _m in _names:
        LOSS_METRICS.setdefault(_m, [_m])
WORKS_LOSS_LINE = 2_000_000          # Guardrails Checks 3-8
STAMPS_LINE = 7_500_000              # Guardrails Check 1
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


def run_start(night, current_run):
    """05:00 UTC start of the run this row is for: --current-run = the run that started most recently (a repair);
    --night = the run that follows that CT evening; otherwise the next run."""
    now = datetime.now(timezone.utc)
    today = now.replace(hour=NIGHTLY_UTC_HOUR, minute=0, second=0, microsecond=0)
    if current_run:
        return today if today <= now else today - timedelta(days=1)
    if night:
        d = date.fromisoformat(night) + timedelta(days=1)
        return datetime(d.year, d.month, d.day, NIGHTLY_UTC_HOUR, tzinfo=timezone.utc)
    return today if today > now else today + timedelta(days=1)


def count(s):
    """6000000, 6M, 6.5M, 250K -> int"""
    m = re.fullmatch(r"\s*([0-9][0-9_,]*(?:\.[0-9]+)?)\s*([kKmM]?)\s*", s or "")
    if not m:
        raise argparse.ArgumentTypeError(f"not a count: {s!r} (use 6000000, 6M or 250K)")
    n = float(m.group(1).replace(",", "").replace("_", ""))
    return int(round(n * {"": 1, "k": 1_000, "m": 1_000_000}[m.group(2).lower()]))


def losses(s):
    """'works=4.3M,abstracts=6M' -> {'works_total': 4300000, 'works_with_abstract_any': 6000000, ...}"""
    out = {}
    for part in filter(None, (p.strip() for p in (s or "").split(","))):
        if "=" not in part:
            raise argparse.ArgumentTypeError(f"--max-loss takes name=count pairs, got {part!r}")
        name, n = (x.strip() for x in part.split("=", 1))
        if name not in LOSS_METRICS:
            raise argparse.ArgumentTypeError(f"unknown loss {name!r}; known: {', '.join(sorted(LOSS_METRICS))}")
        for metric in LOSS_METRICS[name]:
            out[metric] = count(n)
    return out


def q(s):
    return "'" + s.replace("'", "''") + "'"


def map_sql(m):
    if not m:
        return "CAST(NULL AS MAP<STRING, BIGINT>)"
    return "map(" + ", ".join(f"{q(k)}, {int(v)}L" for k, v in sorted(m.items())) + ")"


def show_map(m):
    return ", ".join(f"{k} {v:,}" for k, v in sorted(m.items())) if m else "no loss named"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flags", default="guardrails_override", help="comma-separated; default guardrails_override")
    ap.add_argument("--reason", help="what change, how many works, oxjob id")
    ap.add_argument("--by", help="who gave the yes")
    ap.add_argument("--night", help="CT evening date (YYYY-MM-DD) the run belongs to; default = the next 05:00 UTC")
    ap.add_argument("--current-run", action="store_true", help="the run that started at the last 05:00 UTC (a repair)")
    ap.add_argument("--max-changed", type=count, help="guardrails_override: works this run may stamp (Check 1); default the 7.5M line")
    ap.add_argument("--max-deleted", type=count, help="deleted_works_guard_override: works that may disappear; also names works=N")
    ap.add_argument("--max-loss", type=losses, default={}, help="guardrails_override: losses by name, e.g. works=4.3M,abstracts=6M")
    ap.add_argument("--raise", dest="raise_", action="store_true",
                    help="raise the run's existing row in place to these totals, or file it if there is none "
                         "(a night has one row per flag)")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--cancel", action="store_true", help="end every open window now")
    ap.add_argument("--dry-run", action="store_true", help="print the SQL writes instead of running them")
    ap.add_argument("--warehouse", default=None)
    a = ap.parse_args()
    wh = a.warehouse or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or DEFAULT_WAREHOUSE

    def write(statement):
        if a.dry_run:
            print(f"[dry run] {statement}")
            return [], []
        return sql(statement, wh)

    _, desc = sql(f"DESCRIBE {TABLE}", wh)
    have = {r[0] for r in desc}
    for col, typ in (("max_changed", "BIGINT"), ("max_loss", "MAP<STRING, BIGINT>")):
        if col not in have and not a.list:
            write(f"ALTER TABLE {TABLE} ADD COLUMNS ({col} {typ})")
            have.add(col) if not a.dry_run else None
    loss_json = "to_json(max_loss)" if "max_loss" in have else "CAST(NULL AS STRING)"

    if a.list:
        # Column order is read by #1642's shepherd (preflight.py): flag | from | until | max_changed | ... | requested_by | created_at
        cols, rows = sql(f"SELECT flag, valid_from, valid_until, max_changed, {loss_json} AS max_loss, reason, requested_by, "
                         f"created_at FROM {TABLE} WHERE valid_until > current_timestamp() ORDER BY valid_from", wh)
        print(" | ".join(cols)) if rows else print("no open windows")
        for r in rows:
            print(" | ".join(str(x) for x in r))
        return
    if a.cancel:
        cols, rows = write(f"UPDATE {TABLE} SET valid_until = current_timestamp() WHERE valid_until > current_timestamp()")
        print(f"closed {rows[0][0] if rows else 0} window(s)")
        return

    flags = [f.strip() for f in a.flags.split(",") if f.strip()]
    bad = [f for f in flags if f not in FLAGS]
    if bad:
        sys.exit(f"unknown flag(s) {bad}; known: {', '.join(FLAGS)}")
    if not a.reason or not a.by:
        sys.exit("--reason and --by are required (the reason is what the morning reader sees in Guardrails' output)")
    if a.current_run and a.night:
        sys.exit("--current-run and --night name different runs; give one")
    if a.max_changed is not None and "guardrails_override" not in flags:
        sys.exit("--max-changed caps guardrails_override; add it to --flags")
    if a.max_loss and "guardrails_override" not in flags:
        sys.exit("--max-loss names Guardrails' and the curation gates' losses, which ride guardrails_override; add it to --flags")
    if a.max_deleted is not None and "deleted_works_guard_override" not in flags:
        sys.exit("--max-deleted caps deleted_works_guard_override; add it to --flags")

    loss = dict(a.max_loss)
    implicit = set()          # flags this command files or raises on its own (raise-or-file, never refused)
    works_from_deletions = False
    if a.max_deleted is not None:
        # Works deleted are works_total lost: the same number, named once, on the night's guardrails_override row too
        # (a deletion-only row would pass the deletion guard and then fail Guardrails' works-count check).
        if "guardrails_override" not in flags:
            flags.insert(0, "guardrails_override")
            implicit.add("guardrails_override")
        if "works_total" not in loss:
            loss["works_total"] = a.max_deleted
            works_from_deletions = True
        if a.max_deleted > WORKS_LOSS_LINE:
            unnamed = [m for m in ("works_with_abstract_any", "works_with_abstract_inverted_index",
                                   "works_with_affiliation_strings", "works_with_institution_ids", "works_with_fulltext")
                       if m not in loss]
            if unnamed:
                print(f"NOTE: a merge of {a.max_deleted:,} works can drop each of {', '.join(unnamed)} by up to that many. "
                      "Count the losers that carry each and name the ones over 2M (--max-loss), or Guardrails fails them.")

    start = run_start(a.night, a.current_run)
    # A planned night's row closes 18 h after its start (the run and a morning repair). A --current-run row (a repair of
    # the run in progress, possibly late in the day) stays open until the next run starts, never into it.
    until = start + timedelta(hours=24 if a.current_run else WINDOW_AFTER_START_H)
    now = datetime.now(timezone.utc)
    if until <= now:
        sys.exit(f"that run's window ({until:%Y-%m-%d %H:%M} UTC) has already ended")
    caps = {"guardrails_override": a.max_changed, "deleted_works_guard_override": a.max_deleted}
    stamp = datetime.now(CT).strftime("%Y-%m-%d %H:%M CT")

    # Check every flag before writing any, so a refusal never leaves the night half written.
    plan = []
    for f in flags:
        _, existing = sql(f"SELECT cast(created_at AS STRING), max_changed, {loss_json}, reason FROM {TABLE} "
                          f"WHERE flag = {q(f)} AND valid_until > TIMESTAMP '{start:%Y-%m-%d %H:%M:%S}' "
                          f"AND valid_from < TIMESTAMP '{until:%Y-%m-%d %H:%M:%S}' "
                          f"AND valid_until > current_timestamp()", wh)
        raise_f = a.raise_ or f in implicit
        if existing and not raise_f:
            print(f"refused for {f}: this run already has a row (one row per flag per night):")
            for r in existing:
                print(f"   created {r[0]} UTC, max_changed {r[1]}, max_loss {r[2]}: {r[3][:160]}")
            sys.exit("raise it in place with the night's new totals: add --raise (same command)")
        if len(existing) > 1:
            sys.exit(f"--raise for {f}: this run has {len(existing)} rows; close the extras first (one row per flag per night)")
        if existing and f == "guardrails_override" and existing[0][1] is None and caps[f] is None:
            sys.exit("this run's guardrails_override row has no stamp cap (an uncapped legacy row): "
                     "give --max-changed with the night's total stamps")
        plan.append((f, existing[0] if existing else None))

    for f, row in plan:
        row_loss = loss if f == "guardrails_override" else {}
        if row:
            created, old_changed, old_loss_json, _ = row
            old_loss = json.loads(old_loss_json) if old_loss_json else {}
            new_loss = dict(old_loss)
            new_loss.update(row_loss)
            if works_from_deletions and f == "guardrails_override":
                # derived from --max-deleted (the night's deletions): never lower a works_total someone named for more
                new_loss["works_total"] = max(int(old_loss.get("works_total", 0)), a.max_deleted)
            sets = [f"max_loss = {map_sql(new_loss)}",
                    f"reason = concat(reason, {q(f' | RAISED {stamp} by {a.by}: {a.reason}')})"]
            if caps.get(f) is not None:
                sets.insert(0, f"max_changed = {caps[f]}")
            write(f"UPDATE {TABLE} SET {', '.join(sets)} WHERE flag = {q(f)} AND created_at = TIMESTAMP '{created}'")
            print(f"raised {f} in place (row of {created} UTC): max_changed {old_changed} -> "
                  f"{caps.get(f) if caps.get(f) is not None else old_changed}; losses {show_map(new_loss)}")
            continue
        cap = caps.get(f)
        if f == "guardrails_override" and cap is None:
            cap = STAMPS_LINE     # a row names stamps explicitly; at the line it raises nothing (never NULL = uncapped)
        write(f"INSERT INTO {TABLE} (valid_from, valid_until, flag, reason, requested_by, created_at, max_changed, max_loss) "
              f"VALUES (TIMESTAMP '{start:%Y-%m-%d %H:%M:%S}', TIMESTAMP '{until:%Y-%m-%d %H:%M:%S}', {q(f)}, {q(a.reason)}, "
              f"{q(a.by)}, current_timestamp(), {cap if cap is not None else 'NULL'}, {map_sql(row_loss)})")
        named = f"stamps {cap:,}; {show_map(row_loss)}" if f == "guardrails_override" else (
            f"{cap:,} deletions" if cap is not None else "on")
        print(f"pre-cleared {f} ({named})")

    print(f"for the End 2 End run starting {start:%Y-%m-%d %H:%M} UTC ({start.astimezone(CT):%Y-%m-%d %H:%M} CT); "
          f"window {start:%H:%M} UTC to {until:%Y-%m-%d %H:%M} UTC"
          + (" (guardrails_override filed or raised for works_total, from --max-deleted)" if implicit else ""))
    print("Checks a row doesn't name keep their own line. Morning check: openalex.works.guardrails_history "
          "(threshold, preclear columns; note = 'preclear').")


if __name__ == "__main__":
    main()
