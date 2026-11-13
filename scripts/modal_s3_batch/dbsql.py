"""Run one SQL statement on a Databricks SQL warehouse with the SDK (reads return rows; writes return the statement id).

Defaults to shared-xl (sessions and one-off work). Use shared-m 69a583ace3bdc8d0 for repeated status checks, and move a big scan
up per wh-fit (CLAUDE.md "Which SQL warehouse"). Profile DEFAULT, as everywhere on the team machines.
"""
import json
import os
import time

WAREHOUSE = os.environ.get("MODAL_S3_BATCH_WAREHOUSE", "78c15381e3518e6e")  # shared-xl


def sql(statement, write=False, warehouse=None, profile="DEFAULT"):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    w = WorkspaceClient(profile=profile)
    st = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse or WAREHOUSE, wait_timeout="30s",
                                                 disposition=Disposition.INLINE, format=Format.JSON_ARRAY)
    while st.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        st = w.statement_execution.get_statement(st.statement_id)
    if st.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{st.status.state}: {st.status.error}")
    if write:
        return st.statement_id
    cols = [(c.name, c.type_name.value if c.type_name else "") for c in st.manifest.schema.columns]
    rows, res, chunk = [], st.result, 0
    while res is not None:
        for r in res.data_array or []:
            rows.append({n: (json.loads(v) if v is not None and t in ("ARRAY", "STRUCT", "MAP") else v) for (n, t), v in zip(cols, r)})
        chunk += 1
        res = w.statement_execution.get_statement_result_chunk_n(st.statement_id, chunk) if res.next_chunk_index is not None else None
    return rows
