#!/usr/bin/env python3
"""Monitoring dry run (oxjob #1116): replay a component's checks over its metric history locally.

Read-only. Pulls the component's metrics through utils.databricks_sql.run_query, runs the
same engine the notebook uses over every day in order, and prints what WOULD have fired.
Use it to tune bands before the job is deployed, and to check a YAML edit.

  .venv/bin/python scripts/monitoring_dryrun.py author_matching [--days 60] [--show watch|critical]
"""

import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import date

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
from utils.databricks_sql import run_query  # noqa: E402
from utils.monitoring_engine import MetricStore, backfill  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("component")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--show", default="critical", choices=["critical", "watch", "all"])
    ap.add_argument("--size", default="medium")
    a = ap.parse_args()

    with open(os.path.join(ROOT, "monitoring", "checks", f"{a.component}.yaml")) as fh:
        spec = yaml.safe_load(fh)
    rows = run_query(f"""
        SELECT snapshot_date, metric, dimension, value
        FROM openalex.monitoring.metrics
        WHERE component = '{spec["component"]}'
          AND snapshot_date >= current_date() - INTERVAL {a.days} DAYS""", size=a.size)
    if spec.get("metrics_table"):   # a legacy tall table registered alongside the sink
        rows += run_query(f"""
            SELECT snapshot_date, metric, dimension, CAST(value AS DOUBLE) AS value
            FROM {spec["metrics_table"]}
            WHERE snapshot_date >= current_date() - INTERVAL {a.days} DAYS""", size=a.size)
    store = MetricStore(rows)
    days = store.dates()
    print(f"{a.component}: {len(rows)} metric rows, {len(days)} days ({days[0]}..{days[-1]})")

    findings = backfill(spec, store, days)
    by_day = defaultdict(Counter)
    for f in findings:
        by_day[f["snapshot_date"]][f["status"]] += 1
    print("\nper-day status counts:")
    for d in days:
        c = by_day[d]
        print(f"  {d}  ok={c['ok']:>3} watch={c['watch']:>3} critical={c['critical']:>3} "
              f"hist={c['insufficient_history']:>3} n={c['insufficient_n']:>2} nodata={c['no_data']:>3}")

    want = {"critical"} if a.show == "critical" else ({"watch", "critical"} if a.show == "watch" else None)
    print(f"\nfindings ({a.show}):")
    per_check = Counter()
    for f in findings:
        if want is None or f["status"] in want:
            per_check[(f["check_id"], f["status"])] += 1
            dim = f" [{f['dimension']}]" if f["dimension"] else ""
            v = f"{f['value']:.4g}" if f["value"] is not None else "-"
            print(f"  {f['snapshot_date']} {f['status']:<8} {f['section_id']}/{f['check_id']}{dim}: {v}  {f['detail']}")
    print("\nby check:")
    for (cid, st), n in sorted(per_check.items(), key=lambda x: -x[1]):
        print(f"  {n:>4}  {st:<8} {cid}")


if __name__ == "__main__":
    main()
