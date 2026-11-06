-- oxjob #1700: one-time removal of work_references rows that can never resolve and that lock their citing work out of
-- parse_work_references' insert (anti-join on citing_work_id). Run by notebooks/maintenance/WorkReferencesCleanup.py,
-- which substitutes the tables (job parameters, so a rehearsal points them at copies) and runs one step at a time.
--
-- Two kinds of row, the known cases only:
--   unmapped    citing_work_id IS NULL: rows of locations with no work, re-inserted every night because NULL never matches
--               the anti-join (#835 B1; 4,381,190,257 rows on 5 Nov 2026). Not backed up: the old cell 4 rebuilds exactly these from
--               locations_mapped every night, so reverting the parse_work_references commit is their undo.
--   placeholder a repository row whose seven reference fields are all NULL: the entry Repo.py / RepoBackfill.py wrote
--               on every record until walden b4e752b3 (5 Nov 2026). 294,535,260 rows under real works (5 Nov 2026).
--               Backed up to __REMOVED__ first; the undo puts them back.
-- Contentless rows of other provenances (~0.9M works, 3 Nov) are upstream values and stay; the Crossref parser fields
-- they lost are #835 R5.

-- @step counts
SELECT
  count_if(citing_work_id IS NULL) AS unmapped_rows,
  count_if(citing_work_id IS NOT NULL AND provenance IN ('repo', 'repo_backfill') AND cited_work_id IS NULL
           AND coalesce(doi, pmid, arxiv, title, authors, year, raw) IS NULL) AS placeholder_rows,
  count(*) AS total_rows
FROM __WR__;

-- @step backup
-- CREATE (not IF NOT EXISTS): the runner calls this step only when __REMOVED__ does not exist yet. A CTAS is atomic, so a
-- crash leaves either no table or the full set.
CREATE TABLE __REMOVED__ AS
SELECT *, current_timestamp() AS removed_at
FROM __WR__
WHERE citing_work_id IS NOT NULL AND provenance IN ('repo', 'repo_backfill') AND cited_work_id IS NULL
  AND coalesce(doi, pmid, arxiv, title, authors, year, raw) IS NULL;

-- @step removed_count
SELECT count(*) AS removed_rows, count(DISTINCT citing_work_id) AS removed_works FROM __REMOVED__;

-- @step delete
DELETE FROM __WR__
WHERE citing_work_id IS NULL
   OR (provenance IN ('repo', 'repo_backfill') AND cited_work_id IS NULL
       AND coalesce(doi, pmid, arxiv, title, authors, year, raw) IS NULL);

-- @step undo_unread
-- Rows inserted for the unblocked works after the cleanup (the re-read and later nights): taken out so the works return to
-- exactly their pre-cleanup rows.
DELETE FROM __WR__
WHERE created_timestamp > (SELECT max(finished_at) FROM __RUNS__ WHERE mode = 'write' AND status = 'ok')
  AND citing_work_id IN (SELECT DISTINCT citing_work_id FROM __REMOVED__);

-- @step undo_restored_count
SELECT count(*) AS placeholder_rows_present
FROM __WR__ w LEFT SEMI JOIN (SELECT DISTINCT citing_work_id FROM __REMOVED__) r ON r.citing_work_id = w.citing_work_id
WHERE w.provenance IN ('repo', 'repo_backfill') AND w.cited_work_id IS NULL
  AND coalesce(w.doi, w.pmid, w.arxiv, w.title, w.authors, w.year, w.raw) IS NULL;

-- @step undo_restore
INSERT INTO __WR__ SELECT * EXCEPT (removed_at) FROM __REMOVED__;

-- @step log
INSERT INTO __RUNS__ VALUES (__RUN_ROW__);
