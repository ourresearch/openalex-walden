-- oxjob #2054 (with #835 B3 + B4): the starting point of parse_work_references' per-list filing, kept in
-- openalex.works.work_reference_lists (one row per list: its fingerprint when filed, the work its references sit under).
-- Run by notebooks/maintenance/WorkReferencesRefresh.py one step at a time on parse_referenced_works' SQL warehouse (the
-- fingerprints must come from the same engine); the __NAME__ placeholders are the tables, so a rehearsal can run this
-- same text on copies.
--
-- start: every list with a reference in locations_mapped as it stands now counts as filed, and no reference is touched,
-- so each night files only the lists that appear, change, move or go after this. The fingerprint, the filters and the
-- one row per record must stay identical to parse_work_references' work_reference_lists_current cell. A list is a
-- location's record (native_id, native_id_namespace, provenance).

-- @step start
CREATE OR REPLACE TABLE __LISTS__ CLUSTER BY (native_id) AS
SELECT
  native_id,
  native_id_namespace,
  provenance,
  work_id AS citing_work_id,
  xxhash64(transform(references, r -> named_struct('doi', r.doi, 'pmid', CASE WHEN r.doi IS NULL THEN r.pmid END,
    'arxiv', r.arxiv, 'title', r.title, 'authors', r.authors, 'year', r.year, 'raw', r.raw))) AS list_hash,
  current_timestamp() AS filed_at
FROM __LM__
WHERE work_id IS NOT NULL AND native_id IS NOT NULL AND native_id_namespace IS NOT NULL AND provenance IS NOT NULL
  AND exists(references, r -> coalesce(r.doi, r.pmid, r.arxiv, r.raw, r.title) IS NOT NULL)
QUALIFY row_number() OVER (PARTITION BY native_id, native_id_namespace, provenance ORDER BY work_id, list_hash) = 1;

-- @step counts
SELECT count(*) AS lists, count(list_hash) AS lists_with_hash FROM __LISTS__;

-- @step undo_drop
DROP TABLE IF EXISTS __LISTS__;

-- @step log
INSERT INTO __RUNS__ VALUES (__RUN_ROW__);
