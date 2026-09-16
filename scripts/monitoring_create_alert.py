#!/usr/bin/env python3
"""Create or update the Monitoring daily-email alert (oxjob #1116) from its JSON spec.

Alerts v2 are hand-managed here, not bundle resources: a DABs alert must be owned by the
deploying identity (github-actions-deploy), which has no access to openalex.monitoring or
the warehouse, and the bundle-level run_as (terraform_user) is rejected for alerts. Run as
yourself; the alert then runs and reads as you.

  .venv/bin/python scripts/monitoring_create_alert.py            # create or update
  .venv/bin/python scripts/monitoring_create_alert.py --dry-run  # run the query only
"""

import argparse
import json
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as S

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPEC = os.path.join(ROOT, "monitoring", "alerts", "monitoring_daily_email.json")


def build(spec: dict) -> S.AlertV2:
    ev, n = spec["evaluation"], spec["evaluation"]["notification"]
    return S.AlertV2(
        display_name=spec["display_name"],
        query_text=spec["query_text"],
        warehouse_id=spec["warehouse_id"],
        custom_summary=spec["custom_summary"],
        custom_description=spec["custom_description"],
        evaluation=S.AlertV2Evaluation(
            comparison_operator=S.ComparisonOperator(ev["comparison_operator"]),
            source=S.AlertV2OperandColumn(name=ev["source"]["name"], display=ev["source"]["display"]),
            threshold=S.AlertV2Operand(value=S.AlertV2OperandValue(**ev["threshold"]["value"])),
            empty_result_state=S.AlertEvaluationState(ev["empty_result_state"]),
            notification=S.AlertV2Notification(
                notify_on_ok=n["notify_on_ok"],
                retrigger_seconds=n["retrigger_seconds"],
                subscriptions=[S.AlertV2Subscription(user_email=s["user_email"]) for s in n["subscriptions"]],
            ),
        ),
        schedule=S.CronSchedule(
            pause_status=S.SchedulePauseStatus(spec["schedule"]["pause_status"]),
            quartz_cron_schedule=spec["schedule"]["quartz_cron_schedule"],
            timezone_id=spec["schedule"]["timezone_id"],
        ),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    spec = json.load(open(SPEC))
    w = WorkspaceClient(profile="DEFAULT")

    r = w.statement_execution.execute_statement(warehouse_id=spec["warehouse_id"], statement=spec["query_text"], wait_timeout="50s")
    if r.status.state.value != "SUCCEEDED":
        sys.exit(f"query failed: {r.status.error}")
    cols = [c.name for c in r.manifest.schema.columns]
    row = dict(zip(cols, (r.result.data_array or [[None] * len(cols)])[0]))
    print("query OK ->", {k: (v[:80] + "…" if isinstance(v, str) and len(v) > 80 else v) for k, v in row.items()})
    if a.dry_run:
        return

    existing = [x for x in w.alerts_v2.list_alerts() if x.display_name == spec["display_name"]]
    alert = build(spec)
    if existing:
        out = w.alerts_v2.update_alert(existing[0].id, alert,
                                       "display_name,query_text,warehouse_id,custom_summary,custom_description,evaluation,schedule")
        print("updated", out.id)
    else:
        out = w.alerts_v2.create_alert(alert)
        print("created", out.id)
    print("owner:", out.owner_user_name, "| run as:", out.effective_run_as, "| state:", out.lifecycle_state)
    print(f"url: {w.config.host}/sql/alerts/{out.id}")


if __name__ == "__main__":
    main()
