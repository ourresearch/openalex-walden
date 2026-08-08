"""oxjob #747 — emit migrated unpaywall URL curations into a STAGING table.

Reads:  openalex.unpaywall.curation_requests_effective   (4,557 effective curations)
        openalex.unpaywall.oxjob747_resolution           (per curation x location match)
        openalex.unpaywall.oxjob747_curated_doi_locations (pre-migration location snapshot)
        openalex.unpaywall.oxjob747_works_oa_baseline    (pre-migration work OA state)
        openalex.unpaywall.oxjob747_work_sources         (per-work candidate source_id)
Writes: openalex.unpaywall.oxjob747_emit_staging         (approved_curations-shaped rows + qa_*)
        openalex.unpaywall.oxjob747_parked               (curations we accept losing, with reason)

Does NOT touch openalex.curations.approved_curations — the INSERT is a separate reviewed step.
"""
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = "3996dc0a9b183ce3"
ID_BASE = 100000
MODERATOR = "oxjob747-migration"

w = WorkspaceClient(profile="DEFAULT")


def sql(statement):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(resp.status.error.message if resp.status.error else resp.status.state)
    return resp.result.data_array if resp.result else []


STAGING = f"""
CREATE OR REPLACE TABLE openalex.unpaywall.oxjob747_emit_staging AS
WITH eff AS (
  SELECT doi, prev_url, new_url, action, email, submitted_date,
         REGEXP_REPLACE(LOWER(new_url), '^https?://', '') AS new_norm
  FROM openalex.unpaywall.curation_requests_effective
  WHERE action <> 'no-op'
),
res AS (
  SELECT r.*, e.submitted_date, e.new_norm
  FROM openalex.unpaywall.oxjob747_resolution r
  JOIN eff e ON e.doi = r.doi AND e.action = r.action
            AND COALESCE(e.prev_url,'') = COALESCE(r.prev_url,'')
),
curation_matched AS (
  SELECT doi, prev_url, action, MAX(CASE WHEN matched_field IS NOT NULL THEN 1 ELSE 0 END) AS any_match
  FROM res GROUP BY doi, prev_url, action
),

-- 1/2: update matched via a scalar field -> URL override on that location
upd AS (
  SELECT CONCAT(native_id_namespace, ':', native_id) AS entity_id,
         matched_field AS property, new_url AS property_value,
         FALSE AS create_new, email, submitted_date,
         'update' AS qa_class, doi AS qa_doi, work_id AS qa_work_id
  FROM res
  WHERE action = 'update' AND matched_field IN ('pdf_url','landing_page_url')
),

-- 3: nullify via pdf -> pdf_url NULL + is_oa false (old block set is_oa=false on pdf nullify)
nul_pdf AS (
  SELECT CONCAT(native_id_namespace, ':', native_id) AS entity_id,
         p.property, CAST(NULL AS STRING) AS property_value,
         FALSE AS create_new, email, submitted_date,
         'nullify_pdf' AS qa_class, doi AS qa_doi, work_id AS qa_work_id
  FROM res
  LATERAL VIEW EXPLODE(ARRAY('pdf_url','is_oa')) p AS property
  WHERE action = 'nullify' AND matched_field = 'pdf_url'
),
nul_pdf_fixed AS (
  SELECT entity_id, property,
         CASE WHEN property = 'is_oa' THEN 'false' ELSE property_value END AS property_value,
         create_new, email, submitted_date, qa_class, qa_doi, qa_work_id
  FROM nul_pdf
),

-- 4: nullify via landing -> landing_page_url NULL (old block did not touch is_oa here)
nul_landing AS (
  SELECT CONCAT(native_id_namespace, ':', native_id) AS entity_id,
         'landing_page_url' AS property, CAST(NULL AS STRING) AS property_value,
         FALSE AS create_new, email, submitted_date,
         'nullify_landing' AS qa_class, doi AS qa_doi, work_id AS qa_work_id
  FROM res
  WHERE action = 'nullify' AND matched_field = 'landing_page_url'
),

-- 5: add where the URL already exists on a location -> is_oa=true on that location.
-- OA-invariance guard: never emit is_oa=true against a work whose baseline is closed —
-- the old block's mark_oa evidently is not flipping these works today, so ours must not either.
mark_oa AS (
  SELECT CONCAT(native_id_namespace, ':', native_id) AS entity_id,
         'is_oa' AS property, 'true' AS property_value,
         FALSE AS create_new, email, submitted_date,
         'mark_oa' AS qa_class, doi AS qa_doi, work_id AS qa_work_id
  FROM res r
  WHERE action = 'add_or_mark_oa' AND matched_field IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM openalex.unpaywall.oxjob747_works_oa_baseline b
                    WHERE b.work_id = r.work_id AND b.is_oa = FALSE)
),

-- 6: unmatched updates downgraded to add semantics: mark_oa if new_url already on a location
upd_as_mark_oa AS (
  SELECT CONCAT(r.native_id_namespace, ':', r.native_id) AS entity_id,
         'is_oa' AS property, 'true' AS property_value,
         FALSE AS create_new, r.email, r.submitted_date,
         'update_unmatched_mark_oa' AS qa_class, r.doi AS qa_doi, r.work_id AS qa_work_id
  FROM res r
  JOIN curation_matched m ON m.doi = r.doi AND COALESCE(m.prev_url,'') = COALESCE(r.prev_url,'') AND m.action = r.action
  WHERE r.action = 'update' AND m.any_match = 0 AND r.work_id IS NOT NULL
    AND (REGEXP_REPLACE(LOWER(COALESCE(r.loc_pdf_url,'')), '^https?://', '') = r.new_norm
      OR REGEXP_REPLACE(LOWER(COALESCE(r.loc_landing_page_url,'')), '^https?://', '') = r.new_norm)
    AND NOT EXISTS (SELECT 1 FROM openalex.unpaywall.oxjob747_works_oa_baseline b
                    WHERE b.work_id = r.work_id AND b.is_oa = FALSE)
),

-- 7: create_new pairs: adds with no URL match anywhere + unmatched updates with no new_url match
create_new_pairs AS (
  SELECT r.doi, r.new_url, r.email, r.submitted_date, r.work_id
  FROM res r
  JOIN curation_matched m ON m.doi = r.doi AND COALESCE(m.prev_url,'') = COALESCE(r.prev_url,'') AND m.action = r.action
  WHERE m.any_match = 0 AND r.work_id IS NOT NULL
    AND (r.action = 'add_or_mark_oa'
         OR (r.action = 'update'
             AND NOT EXISTS (SELECT 1 FROM upd_as_mark_oa u WHERE u.qa_doi = r.doi)))
  GROUP BY r.doi, r.new_url, r.email, r.submitted_date, r.work_id
),
create_new_rows AS (
  SELECT
    CONCAT('openalex_curation:oxjob747-', p.work_id, '-', SUBSTRING(MD5(CONCAT(p.doi, p.new_url)), 1, 12)) AS entity_id,
    CAST(NULL AS STRING) AS property,
    TO_JSON(NAMED_STRUCT(
      'work_id', CONCAT('https://openalex.org/W', p.work_id),
      'pdf_url', CASE WHEN LOWER(p.new_url) LIKE '%.pdf%' OR LOWER(p.new_url) LIKE '%/pdf/%' THEN p.new_url END,
      'landing_page_url', CASE WHEN NOT (LOWER(p.new_url) LIKE '%.pdf%' OR LOWER(p.new_url) LIKE '%/pdf/%') THEN p.new_url END,
      'is_oa', TRUE,
      'version', 'publishedVersion',
      -- type on every create_new row: a type-less location that wins primary nulls
      -- work.type/is_paratext downstream
      'type', COALESCE(w.type, 'article'),
      -- source_id on every create_new row: source-less OA locations derive host_type NULL ->
      -- oa_status GOLD in works base (not bronze), so uniform green is the honest choice
      'source_id', CASE WHEN ws.source_id IS NOT NULL
                        THEN CONCAT('https://openalex.org/S', ws.source_id) END
    )) AS property_value,
    TRUE AS create_new, p.email, p.submitted_date,
    'create_new' AS qa_class, p.doi AS qa_doi, p.work_id AS qa_work_id
  FROM create_new_pairs p
  -- strict resolution guard: the target work must itself hold the curated DOI. Stale/misbound
  -- locations (mag ghosts, repo twins) can claim a DOI their work does not have — attaching
  -- there puts the curated URL on the wrong paper (64 such rows purged 2026-08-08).
  JOIN openalex.works.openalex_works w
    ON w.id = p.work_id
   AND LOWER(REGEXP_REPLACE(w.doi, '^https?://(dx\\\\.)?doi\\\\.org/', '')) = p.doi
  LEFT JOIN openalex.unpaywall.oxjob747_works_oa_baseline b ON b.work_id = p.work_id
  LEFT JOIN openalex.unpaywall.oxjob747_work_sources ws ON ws.work_id = p.work_id
  WHERE COALESCE(b.is_oa, TRUE) = TRUE OR b.oa_status IS NULL  -- exclude baseline-closed works (OA invariance)
),

unioned AS (
  SELECT * FROM upd
  UNION ALL SELECT * FROM nul_pdf_fixed
  UNION ALL SELECT * FROM nul_landing
  UNION ALL SELECT * FROM mark_oa
  UNION ALL SELECT * FROM upd_as_mark_oa
  UNION ALL SELECT * FROM create_new_rows
),
-- resolve (entity_id, property) conflicts: latest submitted wins (matches mechanism semantics)
deduped AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY entity_id, COALESCE(property,'~create~')
    ORDER BY submitted_date DESC NULLS LAST, qa_class
  ) AS conflict_rn
  FROM unioned
)
SELECT
  CAST({ID_BASE} + ROW_NUMBER() OVER (ORDER BY submitted_date ASC NULLS FIRST, entity_id, property) AS INT) AS id,
  'approved' AS status,
  'locations' AS entity,
  entity_id,
  property,
  property_value,
  email AS submitter_email,
  submitted_date,
  submitted_date AS moderated_date,
  CAST(NULL AS TIMESTAMP) AS live_date,
  '{MODERATOR}' AS moderator_email,
  FALSE AS is_live,
  create_new,
  CURRENT_TIMESTAMP() AS event_ts,
  qa_class, qa_doi, qa_work_id,
  conflict_rn > 1 AS qa_conflict_dropped
FROM deduped
"""

PARKED = """
CREATE OR REPLACE TABLE openalex.unpaywall.oxjob747_parked AS
WITH eff AS (
  SELECT doi, prev_url, new_url, action, email, submitted_date
  FROM openalex.unpaywall.curation_requests_effective WHERE action <> 'no-op'
),
per_curation AS (
  SELECT doi, prev_url, action,
    MAX(CASE WHEN matched_field IS NOT NULL THEN 1 ELSE 0 END) AS any_match,
    MAX(CASE WHEN matched_field IN ('pdf_url','landing_page_url') THEN 1 ELSE 0 END) AS scalar_match,
    MAX(CASE WHEN doi_has_locations THEN 1 ELSE 0 END) AS has_locations
  FROM openalex.unpaywall.oxjob747_resolution
  GROUP BY doi, prev_url, action
)
SELECT e.*, CASE
    WHEN p.has_locations = 0 THEN 'no_locations_for_doi'
    WHEN e.action IN ('update','nullify') AND p.any_match = 1 AND p.scalar_match = 0 THEN 'urls_array_only'
    WHEN e.action = 'nullify' AND p.any_match = 0 THEN 'organically_clean'
  END AS parked_reason
FROM eff e
JOIN per_curation p ON p.doi = e.doi AND COALESCE(p.prev_url,'') = COALESCE(e.prev_url,'') AND p.action = e.action
WHERE p.has_locations = 0
   OR (e.action IN ('update','nullify') AND p.any_match = 1 AND p.scalar_match = 0)
   OR (e.action = 'nullify' AND p.any_match = 0)

UNION ALL
-- add curations whose every URL-match sits on a baseline-closed work (mark_oa suppressed)
SELECT e2.doi, e2.prev_url, e2.new_url, e2.action, e2.email, e2.submitted_date,
       'baseline_closed_mark_oa' AS parked_reason
FROM (SELECT doi, prev_url, new_url, action, email, submitted_date
      FROM openalex.unpaywall.curation_requests_effective WHERE action = 'add_or_mark_oa') e2
WHERE EXISTS (SELECT 1 FROM openalex.unpaywall.oxjob747_resolution r
              JOIN openalex.unpaywall.oxjob747_works_oa_baseline b ON b.work_id = r.work_id
              WHERE r.doi = e2.doi AND r.action = 'add_or_mark_oa'
                AND r.matched_field IS NOT NULL AND b.is_oa = FALSE)
  AND e2.doi NOT IN (SELECT qa_doi FROM openalex.unpaywall.oxjob747_emit_staging
                     WHERE qa_class IN ('mark_oa','create_new'))
"""

if __name__ == "__main__":
    t0 = time.time()
    sql(STAGING)
    print(f"staging built in {time.time()-t0:.0f}s")
    sql(PARKED)

    print("\n--- staging by class ---")
    for row in sql("""SELECT qa_class, create_new, COUNT(*) n,
                      SUM(CASE WHEN qa_conflict_dropped THEN 1 ELSE 0 END) conflicts_dropped
                      FROM openalex.unpaywall.oxjob747_emit_staging GROUP BY 1,2 ORDER BY 1"""):
        print(row)
    print("\n--- totals ---")
    print(sql("""SELECT COUNT(*) rows, COUNT(DISTINCT qa_doi) dois, MIN(id) min_id, MAX(id) max_id
                 FROM openalex.unpaywall.oxjob747_emit_staging WHERE NOT qa_conflict_dropped"""))
    print("\n--- parked by reason ---")
    for row in sql("SELECT parked_reason, COUNT(*) FROM openalex.unpaywall.oxjob747_parked GROUP BY 1"):
        print(row)
    print("\n--- collisions with existing approved_curations ---")
    print(sql("""SELECT COUNT(*) FROM openalex.unpaywall.oxjob747_emit_staging s
                 JOIN openalex.curations.approved_curations a
                   ON a.entity='locations' AND a.entity_id = s.entity_id
                  AND COALESCE(a.property,'~') = COALESCE(s.property,'~')"""))
    print("\n--- create_new source_id coverage (green rule) ---")
    for row in sql("""SELECT b.oa_status,
                        SUM(CASE WHEN GET_JSON_OBJECT(s.property_value,'$.source_id') IS NOT NULL THEN 1 ELSE 0 END) with_source,
                        COUNT(*) n
                      FROM openalex.unpaywall.oxjob747_emit_staging s
                      LEFT JOIN openalex.unpaywall.oxjob747_works_oa_baseline b ON b.work_id = s.qa_work_id
                      WHERE s.qa_class='create_new' GROUP BY 1 ORDER BY n DESC"""):
        print(row)
