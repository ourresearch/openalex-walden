-- oxjob #2054 (with #835 B3 + B4): the starting point of parse_work_references' per-list filing, kept in
-- openalex.works.work_reference_lists (one row per list: its fingerprint when filed, the work its references sit under).
-- Run by notebooks/maintenance/WorkReferencesRefresh.py one step at a time on parse_referenced_works' SQL warehouse (the
-- fingerprints must come from the same engine); the __NAME__ placeholders are the tables, so a rehearsal can run this
-- same text on copies.
--
-- start (phase 1): every list with a reference in locations_mapped as it stands now counts as filed, and no reference is touched,
-- so each night files only the lists that appear, change, move or go after this. The fingerprint, the filters and the
-- one row per record must stay identical to parse_work_references' work_reference_lists_current cell. A list is a
-- location's record (native_id, native_id_namespace, provenance).
--
-- refresh (phase 2): the fingerprints are taken again from the references actually stored, so every list whose stored
-- references differ from its current list (stale, never filed), sits under another work, or is gone is found and the
-- next parse_work_references run puts it right. The same fingerprint, built from the rows in reference order; pmid only
-- on a reference without a DOI (the DOI step overwrites pmid on DOI rows). A list whose rows sit under two works, or skip
-- or repeat a position, gets no fingerprint, so it is re-filed.

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

-- @step refresh
CREATE OR REPLACE TABLE __LISTS__ CLUSTER BY (native_id) AS
SELECT
  native_id,
  native_id_namespace,
  provenance,
  CASE WHEN count(DISTINCT citing_work_id) = 1 THEN max(citing_work_id) END AS citing_work_id,
  CASE WHEN count(DISTINCT citing_work_id) = 1 AND count(DISTINCT ref_ind) = count(*) AND min(ref_ind) = 0
        AND max(ref_ind) = count(*) - 1
       THEN xxhash64(transform(
         array_sort(collect_list(named_struct('i', ref_ind, 'doi', doi, 'pmid', CASE WHEN doi IS NULL THEN pmid END,
           'arxiv', arxiv, 'title', title, 'authors', authors, 'year', year, 'raw', raw))),
         r -> named_struct('doi', r.doi, 'pmid', r.pmid, 'arxiv', r.arxiv, 'title', r.title, 'authors', r.authors,
           'year', r.year, 'raw', r.raw))) END AS list_hash,
  current_timestamp() AS filed_at
FROM __WR__
WHERE native_id IS NOT NULL AND native_id_namespace IS NOT NULL AND provenance IS NOT NULL AND citing_work_id IS NOT NULL
GROUP BY native_id, native_id_namespace, provenance;

-- @step counts
SELECT count(*) AS lists, count(list_hash) AS lists_with_hash FROM __LISTS__;

-- @step undo_restore
RESTORE TABLE __WR__ TO VERSION AS OF __VERSION__;

-- @step undo_restore_lists
RESTORE TABLE __LISTS__ TO VERSION AS OF __LISTS_VERSION__;

-- @step undo_drop
DROP TABLE IF EXISTS __LISTS__;

-- @step log
INSERT INTO __RUNS__ VALUES (__RUN_ROW__);
