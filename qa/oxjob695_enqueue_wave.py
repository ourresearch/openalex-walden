"""oxjob #695 — enqueue a drain wave into taxicab rescrape_queue with per-host caps.

Usage: oxjob695_enqueue_wave.py host=cap [pool:budget_host=count ...]
  e.g. oxjob695_enqueue_wave.py www.sciencedirect.com=150000 pool:wiley=5000 pool:oup=1500
host=cap draws from pdf_candidate_drain_queue by host_rank; pool:X=count draws the next
count randomized rows from pdf_drip_pool (rate-blocked publishers — keep counts small and
run those waves with harvest_workers turned down). Anti-joins resolved taxicab_results
attempts (NULL-outcome rows stay retryable) and the current queue, so re-runs never
double-fetch and blocked/timed-out rows re-enter automatically.
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from utils.databricks_sql import run_query

caps, pools, oa = [], [], []
for arg in sys.argv[1:]:
    host, cap = arg.rsplit("=", 1)
    if host.startswith("pool:"):
        budget = host[len("pool:"):]
        assert budget.isalnum(), f"bad budget host {budget}"
        pools.append((budget, int(cap)))
    elif host.startswith("oa:"):
        h = host[len("oa:"):]
        assert h.replace(".", "").replace("-", "").isalnum(), f"bad oa host {h}"
        oa.append(f"(q.url_host = '{h}' AND q.host_rank <= {int(cap)})")
    else:
        assert host.replace(".", "").replace("-", "").isalnum(), f"bad host {host}"
        caps.append(f"(d.url_host = '{host}' AND d.host_rank <= {int(cap)})")
assert caps or pools or oa, "no host=cap, pool:host=count or oa:host=cap args given"

repo = Path(__file__).resolve().parents[1]
load_dotenv(repo / ".env")
warehouse_id = os.environ["DATABRICKS_WAREHOUSE_XLARGE"].rstrip("/").rsplit("/", 1)[-1]
w = WorkspaceClient(profile="DEFAULT")

def run_stmt(stmt):
    r = w.statement_execution.execute_statement(
        statement=stmt, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"enqueue failed: {r.status}")


if caps:
    run_stmt(f"""
INSERT INTO openalex.taxicab.rescrape_queue (native_id, native_id_namespace, created_date, pdf_url)
SELECT
  CASE WHEN d.native_id LIKE 'https://doi.org/%' THEN SUBSTRING(d.native_id, 17) ELSE d.native_id END,
  CASE WHEN d.native_id LIKE 'https://doi.org/%' THEN 'doi' ELSE d.native_id_namespace END,
  current_timestamp(), d.pdf_url
FROM openalex.parseland.pdf_candidate_drain_queue d
WHERE ({' OR '.join(caps)})
AND NOT EXISTS (
  SELECT 1 FROM openalex.taxicab.taxicab_results r
  WHERE r.url = d.pdf_url AND (r.content_type IS NOT NULL OR r.s3_path IS NOT NULL)
)
AND NOT EXISTS (SELECT 1 FROM openalex.taxicab.rescrape_queue q WHERE q.pdf_url = d.pdf_url)
""")

if oa:
    run_stmt(f"""
INSERT INTO openalex.taxicab.rescrape_queue (native_id, native_id_namespace, created_date, pdf_url)
SELECT
  CASE WHEN q.native_id LIKE 'https://doi.org/%' THEN SUBSTRING(q.native_id, 17) ELSE q.native_id END,
  CASE WHEN q.native_id LIKE 'https://doi.org/%' THEN 'doi' ELSE q.native_id_namespace END,
  current_timestamp(), q.pdf_url
FROM openalex.parseland.pdf_oa_host_queue q
WHERE ({' OR '.join(oa)})
AND NOT EXISTS (
  SELECT 1 FROM openalex.taxicab.taxicab_results r
  WHERE r.url = q.pdf_url AND (r.content_type IS NOT NULL OR r.s3_path IS NOT NULL)
)
AND NOT EXISTS (SELECT 1 FROM openalex.taxicab.rescrape_queue rq WHERE rq.pdf_url = q.pdf_url)
""")

for budget, count in pools:
    run_stmt(f"""
INSERT INTO openalex.taxicab.rescrape_queue (native_id, native_id_namespace, created_date, pdf_url)
SELECT native_id, native_id_namespace, current_timestamp(), pdf_url
FROM (
  SELECT p.native_id, p.native_id_namespace, p.pdf_url
  FROM openalex.parseland.pdf_drip_pool p
  WHERE p.budget_host = '{budget}'
  AND NOT EXISTS (
    SELECT 1 FROM openalex.taxicab.taxicab_results r
    WHERE r.url = p.pdf_url AND (r.content_type IS NOT NULL OR r.s3_path IS NOT NULL)
  )
  AND NOT EXISTS (SELECT 1 FROM openalex.taxicab.rescrape_queue q WHERE q.pdf_url = p.pdf_url)
  ORDER BY p.rand_key
  LIMIT {count}
)
""")

rows = run_query("""
SELECT TRY_PARSE_URL(pdf_url,'HOST') host, COUNT(*) n
FROM openalex.taxicab.rescrape_queue GROUP BY 1 ORDER BY n DESC""", size="xlarge")
total = 0
for row in rows:
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    total += d["n"]
    print(f"{d['host']:<32} {d['n']:>8,}")
print(f"TOTAL queued: {total:,}")
