# Databricks notebook source
# Re-arms scale-to-zero on a GPU serving endpoint (oxjob #709).
#
# The scale-to-zero timer only arms on a redeploy that changes the capacity
# envelope AND lands on a workload_size config; any request then consumes it
# permanently. This hop (Small -> min0/max8 -> Small) re-arms it after the
# nightly run, so the endpoint sleeps ~40 min later instead of billing 24/7.
#
# This task must NEVER fail the pipeline: a failed re-arm costs ~$34/day until
# the next run, which is not worth blocking end2end over. Every path exits 0.

dbutils.widgets.text("endpoint", "")
ENDPOINT = dbutils.widgets.get("endpoint")

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

OK = "rearmed"

try:
    w = WorkspaceClient()

    def get():
        ep = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{ENDPOINT}")
        e = (ep.get("config") or {}).get("served_entities", [{}])[0]
        return e, ep.get("state", {}).get("config_update")

    entity, config_update = get()
    msg = (entity.get("state") or {}).get("deployment_state_message") or ""

    if msg == "Scaled to zero":
        print(f"{ENDPOINT}: already scaled to zero, nothing to re-arm")
        OK = "already-zero"
    else:
        for _ in range(30):
            _, cu = get()
            if cu == "NOT_UPDATING":
                break
            time.sleep(30)

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

        def wait_replaced(prev_ts):
            for _ in range(40):
                e, cu = get()
                if e.get("creation_timestamp") != prev_ts and cu == "NOT_UPDATING":
                    return e.get("creation_timestamp")
                time.sleep(30)
            raise TimeoutError("rollout did not complete")

        ts0 = entity.get("creation_timestamp")
        put({**base, "min_provisioned_concurrency": 0, "max_provisioned_concurrency": 8})
        ts1 = wait_replaced(ts0)
        put({**base, "workload_size": "Small"})
        wait_replaced(ts1)
        print(f"{ENDPOINT}: re-armed — capacity hop complete, sleep expected in ~40 min")
except Exception as e:
    print(f"{ENDPOINT}: re-arm FAILED (pipeline unaffected): {type(e).__name__}: {e}")
    OK = f"failed: {type(e).__name__}"

dbutils.notebook.exit(OK)
