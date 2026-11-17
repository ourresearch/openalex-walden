# Databricks notebook source
# MAGIC %md
# MAGIC # Oxjob #780: make D1 `content_index` match `primary_pair_sql` for a list of works (one-off, branch only)
# MAGIC
# MAGIC The nightly D1 sync writes only works whose pair changed between two `locations_mapped` versions, both computed
# MAGIC with today's `manifest_excluded_pdfs`, so a new exclusion never reaches D1 on its own (#780 EXPLORE § 6). For each
# MAGIC listed work: if its D1 row differs from the pair `primary_pair_sql` picks today, save the old row to
# MAGIC `openalex.pdf.content_index_removed_rows` (oxjob 780), then INSERT OR REPLACE the new pair, or DELETE the row when
# MAGIC the work has no file left. Same rule as #1959's `d1_cleanup.py`.
# MAGIC
# MAGIC Undo: INSERT OR REPLACE the saved (work_id, pdf_uuid, grobid_uuid) rows back into D1.

# COMMAND ----------

import json
import os
import sys
import time

import requests

dbutils.widgets.text("work_ids", "", "comma-separated bare work ids")
dbutils.widgets.text("dry_run", "true", "true = plan only")
WORK_IDS = [int(x) for x in dbutils.widgets.get("work_ids").replace(" ", "").split(",") if x]
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
assert WORK_IDS and len(WORK_IDS) <= 500, "give 1-500 work ids"

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.content_files import primary_pair_sql  # noqa: E402

D1_URL = ("https://api.cloudflare.com/client/v4/accounts/a452eddbbe06eb7d02f4879cee70d29c"
          "/d1/database/c2e1cc17-1810-400b-a7c8-c5103ab366de/query")
HEADERS = {"Authorization": f"Bearer {dbutils.secrets.get(scope='cloudflare', key='d1_api_token')}",
           "Content-Type": "application/json"}


def d1(sql, params=None):
    for attempt in range(8):
        r = requests.post(D1_URL, headers=HEADERS, json={"sql": sql, **({"params": params} if params else {})}, timeout=120)
        if r.status_code not in (429, 500, 502, 503, 504) or attempt == 7:
            r.raise_for_status()
            return r.json()["result"][0]["results"]
        time.sleep(min(60, 2 ** attempt))


def d1_rows():
    rows = d1(f"SELECT work_id, pdf_uuid, grobid_uuid FROM content_index WHERE work_id IN ({','.join(map(str, WORK_IDS))})")
    return {int(r["work_id"]): (r["pdf_uuid"], r["grobid_uuid"]) for r in rows}

# COMMAND ----------

ids = ",".join(map(str, WORK_IDS))
want = {r.work_id: (r.pdf_uuid, r.grobid_uuid) for r in spark.sql(primary_pair_sql(f"AND work_id IN ({ids})")).collect()}
have = d1_rows()
plan = []
for w in WORK_IDS:
    old, new = have.get(w), want.get(w)
    if old == new:
        continue
    if old is None:
        plan.append({"work_id": w, "action": "added", "old": None, "new": new})
    else:
        plan.append({"work_id": w, "action": "replaced" if new else "deleted", "old": old, "new": new})
for p in plan:
    print(json.dumps(p))
print(f"{len(plan)} of {len(WORK_IDS)} works change")

# COMMAND ----------

if plan and not DRY_RUN:
    saved = [(p["work_id"], p["old"][0], p["old"][1], p["action"], (p["new"] or (None, None))[0], (p["new"] or (None, None))[1])
             for p in plan if p["old"] is not None]
    if saved:
        spark.createDataFrame(saved, "work_id bigint, pdf_uuid string, grobid_uuid string, action string, "
                                     "new_pdf_uuid string, new_grobid_uuid string") \
            .selectExpr("*", "current_timestamp() AS saved_at", "780 AS oxjob") \
            .write.mode("append").saveAsTable("openalex.pdf.content_index_removed_rows")
        n = spark.sql(f"SELECT count(*) n FROM openalex.pdf.content_index_removed_rows WHERE oxjob = 780 "
                      f"AND work_id IN ({ids}) AND saved_at > current_timestamp() - INTERVAL 10 MINUTES").first().n
        assert n >= len(saved), f"saved {n} rows, expected {len(saved)}: D1 untouched"
    for p in plan:
        if p["new"]:
            d1("INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES (?, ?, ?)",
               [p["work_id"], p["new"][0], p["new"][1]])
        else:
            d1("DELETE FROM content_index WHERE work_id = ?", [p["work_id"]])

after = d1_rows()
result = {"dry_run": DRY_RUN, "plan": plan, "after": {str(k): v for k, v in after.items()},
          "match": all(after.get(w) == want.get(w) for w in WORK_IDS) if not DRY_RUN else None}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result, default=str))
