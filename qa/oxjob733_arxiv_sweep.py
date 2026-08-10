#!/usr/bin/env python3
"""oxjob #733 (locations-cleanup): finish out arXiv — sweep stale twins so the
works-base arXiv CASE can be stripped.

Classes (measured 2026-08-10):
  same_work  1,103,460  stale twin, live twin on SAME work        -> DELETE (works no-op)
  crosswork    416,370  stale twin, live twin on DIFFERENT work   -> DELETE + work_id_map repoint to live work
  orphan             9  native_id absent upstream entirely        -> DELETE + map entries deleted
Works that end up with zero locations (~123.6K crosswork + orphans) are drift-
duplicates of their live twin: their work_id_map entries repoint to the live work
(pipeline-level merge) and their openalex_works rows are deleted (#239 precedent).

Usage:
  .venv/bin/python qa/oxjob733_arxiv_sweep.py snapshot
  .venv/bin/python qa/oxjob733_arxiv_sweep.py preview
  .venv/bin/python qa/oxjob733_arxiv_sweep.py execute
  .venv/bin/python qa/oxjob733_arxiv_sweep.py verify
"""
import sys
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = "3996dc0a9b183ce3"
SNAP = "openalex.works.oxjob733_arxiv_sweep_snapshot"
WSNAP = "openalex.works.oxjob733_arxiv_widmap_snapshot"
LM = "openalex.works.locations_mapped"
LWT = "openalex.works.locations_w_types"
WIDMAP = "openalex.works.work_id_map"
OW = "openalex.works.openalex_works"


def sql(w, statement, quiet=False):
    t0 = time.time()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(resp.status.error.message if resp.status.error else resp.status.state)
    if not quiet:
        print(f"  [{time.time()-t0:.0f}s] ok")
    return resp.result.data_array if resp.result else []


def versions(w):
    for tbl in (LM, WIDMAP, OW):
        rows = sql(w, f"DESCRIBE HISTORY {tbl} LIMIT 1", quiet=True)
        print(f"  {tbl} @ v{rows[0][0]}")


def snapshot(w):
    print(f"creating {SNAP} ...")
    sql(w, f"""
    CREATE OR REPLACE TABLE {SNAP} AS
    WITH arx AS (
      SELECT native_id, native_id_namespace, provenance, merge_key, work_id
      FROM {LM}
      WHERE provenance IN ('repo','repo_backfill') AND lower(native_id) LIKE '%arxiv.org%'
    ),
    t AS (
      SELECT DISTINCT native_id, merge_key FROM {LWT}
      WHERE provenance = 'repo' AND lower(native_id) LIKE '%arxiv.org%'
    ),
    live AS (
      SELECT a.* FROM arx a JOIN t ON a.native_id = t.native_id AND a.merge_key <=> t.merge_key
    ),
    doomed AS (
      SELECT a.*
      FROM arx a LEFT JOIN t ON a.native_id = t.native_id AND a.merge_key <=> t.merge_key
      WHERE t.native_id IS NULL
    ),
    live_works AS (
      SELECT native_id, MIN(work_id) AS live_work_id, COLLECT_SET(work_id) AS live_work_ids
      FROM live GROUP BY native_id
    ),
    classed AS (
      -- same_work:       live twin already in mapped on the SAME work -> pure delete
      -- crosswork:       live twin in mapped on a DIFFERENT work -> delete + map repoint
      -- pending_refresh: identity in types but NOT yet in mapped (arrives in tonight's
      --                  merge) -> delete stale row ONLY; map entries stay so the
      --                  arriving row re-maps to the same work; work NOT deleted
      -- orphan:          native_id absent upstream entirely -> delete row + map entries
      SELECT d.*, lw.live_work_id,
        CASE WHEN d.native_id NOT IN (SELECT native_id FROM t) THEN 'orphan'
             WHEN lw.native_id IS NULL THEN 'pending_refresh'
             WHEN ARRAY_CONTAINS(lw.live_work_ids, d.work_id) THEN 'same_work'
             ELSE 'crosswork' END AS cls
      FROM doomed d LEFT JOIN live_works lw ON d.native_id = lw.native_id
    ),
    doomed_per_work AS (
      SELECT work_id, COUNT(*) AS doomed_on_work FROM classed GROUP BY work_id
    ),
    total_per_work AS (
      SELECT lm.work_id, COUNT(*) AS total_locs
      FROM {LM} lm JOIN (SELECT DISTINCT work_id FROM classed) x ON lm.work_id = x.work_id
      GROUP BY lm.work_id
    )
    SELECT c.*, COALESCE(tp.total_locs = dp.doomed_on_work, FALSE) AS work_becomes_empty,
           CURRENT_TIMESTAMP() AS snapshotted_at
    FROM classed c
    LEFT JOIN doomed_per_work dp ON c.work_id = dp.work_id
    LEFT JOIN total_per_work tp ON c.work_id = tp.work_id
    """)
    preview(w)


def preview(w):
    rows = sql(w, f"""
    SELECT cls, work_becomes_empty, COUNT(*) AS rows, COUNT(DISTINCT work_id) AS works,
           SUM(CASE WHEN cls = 'crosswork' AND live_work_id IS NULL THEN 1 ELSE 0 END) AS crosswork_missing_live
    FROM {SNAP} GROUP BY cls, work_becomes_empty ORDER BY cls, work_becomes_empty
    """, quiet=True)
    print("cls | empty | rows | works | crosswork_missing_live")
    for r in rows:
        print(" | ".join(str(x) for x in r))
    print("EXPECT totals: same_work 1103460, crosswork ~150214, pending_refresh ~266156, orphan 9; crosswork_missing_live 0")


def execute(w):
    print("pre-execute Delta versions (rollback points):")
    versions(w)

    print(f"creating {WSNAP} (work_id_map rows to repoint/delete) ...")
    sql(w, f"""
    CREATE OR REPLACE TABLE {WSNAP} AS
    WITH doomed AS (
      SELECT * FROM {SNAP} WHERE cls IN ('crosswork','orphan')
    ),
    matched AS (
      SELECT m.id AS map_id, m.paper_id AS old_paper_id, m.doi, m.pmid, m.arxiv, m.title_author,
             d.cls, d.live_work_id, d.work_becomes_empty,
             ROW_NUMBER() OVER (PARTITION BY m.id ORDER BY d.live_work_id NULLS LAST) AS rn
      FROM {WIDMAP} m
      JOIN doomed d
        ON m.paper_id = CAST(d.work_id AS STRING)
       AND ( d.work_becomes_empty
          OR (m.doi <=> d.merge_key.doi AND m.pmid <=> d.merge_key.pmid
              AND m.arxiv <=> d.merge_key.arxiv AND m.title_author <=> d.merge_key.title_author) )
    )
    SELECT map_id, old_paper_id, doi, pmid, arxiv, title_author, cls, live_work_id, work_becomes_empty,
      CASE
        WHEN cls = 'orphan' OR live_work_id IS NULL THEN 'DELETE'
        WHEN EXISTS (
          SELECT 1 FROM {WIDMAP} m2
          WHERE m2.paper_id = CAST(matched.live_work_id AS STRING)
            AND m2.doi <=> matched.doi AND m2.pmid <=> matched.pmid
            AND m2.arxiv <=> matched.arxiv AND m2.title_author <=> matched.title_author
        ) THEN 'DELETE'
        ELSE 'REPOINT'
      END AS map_action,
      CURRENT_TIMESTAMP() AS snapshotted_at
    FROM matched WHERE rn = 1
    """)
    rows = sql(w, f"SELECT map_action, COUNT(*) FROM {WSNAP} GROUP BY map_action", quiet=True)
    for r in rows:
        print(f"  work_id_map {r[0]}: {r[1]}")

    print("DELETE doomed rows from locations_mapped ...")
    sql(w, f"""
    MERGE INTO {LM} AS t
    USING {SNAP} AS s
    ON  t.provenance = s.provenance
    AND t.native_id = s.native_id
    AND t.native_id_namespace <=> s.native_id_namespace
    AND t.merge_key <=> s.merge_key
    WHEN MATCHED THEN DELETE
    """)

    print("work_id_map repoint/delete ...")
    sql(w, f"""
    MERGE INTO {WIDMAP} AS m
    USING {WSNAP} AS s
    ON m.id = s.map_id
    WHEN MATCHED AND s.map_action = 'REPOINT' THEN UPDATE SET
      m.paper_id = CAST(s.live_work_id AS STRING),
      m.openalex_updated_dt = CURRENT_TIMESTAMP()
    WHEN MATCHED AND s.map_action = 'DELETE' THEN DELETE
    """)

    print("DELETE now-empty works from openalex_works (crosswork/orphan only; pending_refresh works re-fill tonight) ...")
    sql(w, f"""
    DELETE FROM {OW}
    WHERE id IN (
      SELECT DISTINCT work_id FROM {SNAP} s
      WHERE s.work_becomes_empty AND s.cls IN ('crosswork','orphan')
        AND NOT EXISTS (SELECT 1 FROM {SNAP} p WHERE p.work_id = s.work_id AND p.cls = 'pending_refresh')
    )
    """)

    verify(w)


def verify(w):
    rows = sql(w, f"""
    WITH t AS (
      SELECT DISTINCT native_id, merge_key FROM {LWT}
      WHERE provenance = 'repo' AND lower(native_id) LIKE '%arxiv.org%'
    )
    SELECT COUNT(*)
    FROM {LM} a LEFT JOIN t ON a.native_id = t.native_id AND a.merge_key <=> t.merge_key
    WHERE a.provenance IN ('repo','repo_backfill') AND lower(a.native_id) LIKE '%arxiv.org%'
      AND t.native_id IS NULL
    """, quiet=True)
    print(f"remaining doomed arXiv rows in locations_mapped: {rows[0][0]} (expect 0)")
    rows = sql(w, f"""
    SELECT COUNT(*) FROM {LM}
    WHERE provenance IN ('repo','repo_backfill') AND lower(native_id) LIKE '%arxiv.org%'
      AND (pdf_url IS DISTINCT FROM CONCAT('https://arxiv.org/pdf/', SPLIT_PART(native_id, ':', 3))
           OR (GET(FILTER(urls, x -> x.content_type = 'html').url, 0) IS NOT NULL
               AND landing_page_url IS DISTINCT FROM GET(FILTER(urls, x -> x.content_type = 'html').url, 0)))
    """, quiet=True)
    print(f"arXiv rows where works-base CASE would still change output: {rows[0][0]} (expect 2 pre-nightly, 0 after)")
    rows = sql(w, f"""
    SELECT COUNT(*) FROM {OW}
    WHERE id IN (
      SELECT DISTINCT work_id FROM {SNAP} s
      WHERE s.work_becomes_empty AND s.cls IN ('crosswork','orphan')
        AND NOT EXISTS (SELECT 1 FROM {SNAP} p WHERE p.work_id = s.work_id AND p.cls = 'pending_refresh')
    )
    """, quiet=True)
    print(f"empty-cohort works still in openalex_works: {rows[0][0]} (expect 0)")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "preview"
    w = WorkspaceClient(profile="DEFAULT")
    print(f"{datetime.now(timezone.utc).isoformat()} cmd={cmd}")
    {"snapshot": snapshot, "preview": preview, "execute": execute, "verify": verify}[cmd](w)
