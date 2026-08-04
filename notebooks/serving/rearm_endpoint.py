# Databricks notebook source
# Scale management for GPU serving endpoints (oxjob #709).
#
# The scale-to-zero timer only arms on a redeploy that changes the capacity
# envelope AND lands on a workload_size config; any request then consumes it
# permanently. Two modes:
#
#   mode=up     pre-run: PUT min0/max<up_max>. The fresh deployment comes up
#               provisioned, so this doubles as the warm-up (no cold start),
#               and inference can fan out to <up_max> concurrency.
#   mode=rearm  post-run: land on workload_size Small via a capacity-changing
#               transition, arming scale-to-zero; the endpoint sleeps ~40 min
#               later. From min/max != 0-4 a single PUT qualifies; from Small
#               it hops via min0/max8 first.
#
# These tasks must NEVER fail the pipeline: a failed re-arm costs ~$34/day
# until the next run and a failed scale-up just means Small-speed inference.
# Every path exits 0.

dbutils.widgets.text("endpoint", "")
dbutils.widgets.text("mode", "rearm")
dbutils.widgets.text("up_max", "16")

ENDPOINT = dbutils.widgets.get("endpoint")
MODE = dbutils.widgets.get("mode")
UP_MAX = int(dbutils.widgets.get("up_max"))

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

result = MODE

try:
    w = WorkspaceClient()

    def get():
        ep = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{ENDPOINT}")
        e = (ep.get("config") or {}).get("served_entities", [{}])[0]
        return e, ep.get("state", {}).get("config_update")

    def wait_settled():
        for _ in range(30):
            e, cu = get()
            if cu == "NOT_UPDATING":
                return e
            time.sleep(30)
        raise TimeoutError("endpoint stuck updating")

    entity = wait_settled()
    base = {
        "name": entity["name"],
        "entity_name": entity["entity_name"],
        "entity_version": entity["entity_version"],
        "workload_type": entity.get("workload_type", "GPU_MEDIUM"),
        "scale_to_zero_enabled": True,
    }
    routes = {"routes": [{"served_model_name": entity["name"], "traffic_percentage": 100}]}

    def put(served):
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/config",
                        body={"served_entities": [served], "traffic_config": routes})

    def put_and_wait(served, prev_ts):
        put(served)
        for _ in range(40):
            e, cu = get()
            if e.get("creation_timestamp") != prev_ts and cu == "NOT_UPDATING":
                return e
            time.sleep(30)
        raise TimeoutError("rollout did not complete")

    if MODE == "up":
        if entity.get("max_provisioned_concurrency") == UP_MAX:
            print(f"{ENDPOINT}: already at min0/max{UP_MAX}")
        else:
            entity = put_and_wait(
                {**base, "min_provisioned_concurrency": 0,
                 "max_provisioned_concurrency": UP_MAX},
                entity.get("creation_timestamp"))
            print(f"{ENDPOINT}: scaled up to min0/max{UP_MAX}, deployment warm")
    elif MODE == "rearm":
        msg = (entity.get("state") or {}).get("deployment_state_message") or ""
        if msg == "Scaled to zero":
            print(f"{ENDPOINT}: already scaled to zero, nothing to re-arm")
            result = "already-zero"
        elif "workload_size" in entity:
            entity = put_and_wait(
                {**base, "min_provisioned_concurrency": 0,
                 "max_provisioned_concurrency": 8},
                entity.get("creation_timestamp"))
            put_and_wait({**base, "workload_size": "Small"},
                         entity.get("creation_timestamp"))
            print(f"{ENDPOINT}: re-armed via hop — sleep expected in ~40 min")
        else:
            put_and_wait({**base, "workload_size": "Small"},
                         entity.get("creation_timestamp"))
            print(f"{ENDPOINT}: re-armed via single transition — sleep expected in ~40 min")
    else:
        print(f"unknown mode {MODE!r}, doing nothing")
except Exception as e:
    print(f"{ENDPOINT}: {MODE} FAILED (pipeline unaffected): {type(e).__name__}: {e}")
    result = f"failed: {type(e).__name__}"

dbutils.notebook.exit(result)
