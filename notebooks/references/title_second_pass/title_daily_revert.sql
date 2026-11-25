-- Revert the daily title second pass (oxjob #2596 fix 2; notebook TitleSecondPass, job Title Second Pass).
-- Before step 1: pause Title Second Pass in the Jobs UI (then set pause_status: PAUSED in jobs/title_second_pass.yaml and
-- push, or the next bundle deploy unpauses it), and make sure neither it nor Walden End 2 End is running: End 2 End's
-- parse_referenced_works reads these links and its MatchReferences copies them to same-text lines, so a pass already in
-- flight could put back what this removes. Run the three statements in order, between nightlies.
-- Conservative: it unlinks every line still holding a daily run's link (or a same-text copy of one), including a line the
-- matcher would also link to the same work; the 24 Nov backlog run title-2596-2026-11-24 has its own revert (#2596).

-- 1. Stop every later night from applying the daily runs (re-filed lists) or registering them again.
DELETE FROM openalex.works.reference_matcher_runs WHERE run_id LIKE 'title-daily-%';

-- 2. Unlink only lines that hold a daily link: same line, same printed text (hash), cited id = the run's id or the work it
--    has since been merged into. Lines relinked to anything else since are left alone. Same-text copies (linked_by
--    'same_text') of a daily link count too, unless another, non-copy link made the same choice for that text.
MERGE INTO openalex.works.work_references t
USING (
  WITH daily AS (
    SELECT text_hash, cited_work_id FROM openalex.works.reference_matcher_links WHERE run_id LIKE 'title-daily-%'
  ),
  owned AS (
    SELECT l.native_id, l.ref_ind, l.text_hash, l.cited_work_id
    FROM openalex.works.reference_matcher_links l
    WHERE l.run_id LIKE 'title-daily-%'
       OR (l.linked_by = 'same_text' AND (l.text_hash, l.cited_work_id) IN (SELECT text_hash, cited_work_id FROM daily)
           AND NOT EXISTS (SELECT 1 FROM openalex.works.reference_matcher_links o
                           WHERE o.text_hash = l.text_hash AND o.cited_work_id = l.cited_work_id
                             AND o.run_id NOT LIKE 'title-daily-%' AND coalesce(o.linked_by, '') <> 'same_text'))
  )
  -- one row per (line, text, id): the link's own id and its merge winner become separate rows, so no target row can
  -- match two source rows
  SELECT DISTINCT o.native_id, o.ref_ind, o.text_hash, e.match_id
  FROM owned o
  LEFT JOIN openalex.works.merged_work_ids_map m ON m.id = o.cited_work_id
  LATERAL VIEW explode(array_distinct(filter(array(o.cited_work_id, m.merge_into_id), x -> x IS NOT NULL))) e AS match_id
) s
ON t.native_id = s.native_id AND t.ref_ind = s.ref_ind AND t.cited_work_id = s.match_id
   AND xxhash64(left(trim(regexp_replace(regexp_replace(coalesce(nullif(trim(t.raw), ''), concat_ws('. ', t.authors, t.year, t.title)), '(?i)(https?://)?(dx\\.)?(doi\\.org/|doi:?\\s*)?10\\.\\d{4,9}/\\S+', ' '), '(?i)https?://\\S+|www\\.\\S+', ' ')), 1000)) = s.text_hash
WHEN MATCHED THEN UPDATE SET t.cited_work_id = NULL, t.updated_timestamp = current_timestamp();

-- 3. Remove the runs' links last (step 2 reads them), with the same-text copies taken from them (same rule as step 2;
--    a DELETE condition takes no multi-column IN, so EXISTS).
DELETE FROM openalex.works.reference_matcher_links l
WHERE l.run_id LIKE 'title-daily-%'
   OR (l.linked_by = 'same_text'
       AND EXISTS (SELECT 1 FROM openalex.works.reference_matcher_links d
                   WHERE d.run_id LIKE 'title-daily-%' AND d.text_hash = l.text_hash AND d.cited_work_id = l.cited_work_id)
       AND NOT EXISTS (SELECT 1 FROM openalex.works.reference_matcher_links o
                       WHERE o.text_hash = l.text_hash AND o.cited_work_id = l.cited_work_id
                         AND o.run_id NOT LIKE 'title-daily-%' AND coalesce(o.linked_by, '') <> 'same_text'));
-- The next End 2 End rebuilds citations from work_references, so citing and cited works re-stamp back on their own.
