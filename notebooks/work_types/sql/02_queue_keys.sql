-- sql-template: rendered by lib/sql_templates.py, which fills every parameter before Spark runs it.
-- The queue builder's work set (worktype-student-v2, 2026-11-09, topics-shape re-cut). Statement 1: the live works of
-- this build, non-xpac unless include_xpac, one backfill chunk (pmod(id, chunks) = chunk) or all (chunks = 1). Then the
-- notebook runs 02_pack_rows.sql over them and 02_queue_cand.sql on its rows.
CREATE OR REPLACE TEMP VIEW wt_live AS
SELECT id AS work_id
FROM openalex.works.openalex_works_base
WHERE (${include_xpac} OR coalesce(is_xpac, false) = false)
  AND (${chunks} = 1 OR pmod(id, ${chunks}) = ${chunk})
  -- oxjob #2065: a Crossref component is typed by its record (supplementary-materials, or peer-review for a PLOS review
  -- letter) in CreateLocationsWithTypes; the student has only a made-up title to read, so it is never asked
  AND NOT coalesce(primary_location.provenance = 'crossref' AND primary_location.raw_type = 'component', false);

CREATE OR REPLACE TEMP VIEW wt_work_set AS SELECT work_id FROM wt_live;
