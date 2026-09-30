-- Stage the nightly ES vector sync on a SQL warehouse (oxjobs #1275, #1433).
-- Runs as the `stage` task of jobs/embed_qwen3_nightly.yaml, right before sync_vector_index_qwen3 (incremental),
-- which reuses these tables instead of rebuilding them. Why here and not on the job cluster: the same query stalled
-- for hours on the 2-worker classic cluster (no file pruning once the 1024-float embedding column is in play).
--
-- works-vectors-v2 keeps its own copy of 14 filter fields, which semantic search filters on (#1433: after the
-- 2026-09-27 affiliation swap ~10% of works carried stale institutions there). So a work goes out when
--   embedded   it was embedded in the last 2 days (new, or its title/abstract/venue text changed);
--   changed    the hash of its 14 filter fields differs from vector_filter_fields_sent, the mirror of what the index
--              holds (the sync MERGEs it after a clean run), whatever moved them: a nightly re-stamp or a date-kept
--              backfill like the swap;
--   unmirrored it has a vector but no mirror row (a night whose sync failed before the MERGE).
-- Mirror rows whose work no longer exists go to vector_sync_deletes_qwen3 (the sync deletes them, max_deletes guard).
-- The projection and hash must stay identical to the ones the mirror was seeded with (oxjob #1433 PLAN): change a
-- field here and every work re-sends once. batch_id % 10 must match NUM_BATCHES (incremental) in the sync notebook.

CREATE OR REPLACE TABLE openalex.vector_search.vector_sync_staging_qwen3
PARTITIONED BY (batch_id) AS
WITH cur AS (
  SELECT
    CAST(w.id AS STRING) AS work_id,
    concat('https://openalex.org/W', CAST(w.id AS STRING)) AS id,
    w.publication_year,
    lower(w.type) AS type,
    w.open_access.is_oa AS is_oa,
    lower(w.language) AS language,
    array_compact(transform(w.authorships, a -> a.author.id)) AS author_ids,
    array_distinct(array_compact(flatten(transform(w.authorships, a -> transform(a.institutions, i -> i.id))))) AS institution_ids,
    array_distinct(array_compact(flatten(transform(w.authorships, a -> transform(a.institutions, i -> lower(i.country_code)))))) AS country_codes,
    w.is_retracted,
    w.primary_location.source.id AS source_id,
    w.cited_by_count,
    array_compact(transform(coalesce(w.funders, array()), f -> f.id)) AS funder_ids,
    w.fulltext IS NOT NULL AS has_fulltext,
    w.has_abstract,
    w.primary_location.license_id AS license_id
  FROM openalex.works.openalex_works w
  LEFT SEMI JOIN openalex.vector_search.work_embeddings_qwen3 e ON CAST(e.work_id AS BIGINT) = w.id
),
hashed AS (
  SELECT *, xxhash64(publication_year, type, is_oa, language, author_ids, institution_ids, country_codes, is_retracted, source_id, cited_by_count, funder_ids, has_fulltext, has_abstract, license_id) AS filter_hash FROM cur
),
recent AS (
  SELECT DISTINCT work_id FROM openalex.vector_search.work_embeddings_qwen3
  WHERE embedded_at >= current_timestamp() - INTERVAL 2 DAYS
),
todo AS (
  SELECT h.*,
         CASE WHEN r.work_id IS NOT NULL THEN 'embedded'
              WHEN m.id IS NULL THEN 'unmirrored'
              ELSE 'changed' END AS send_reason
  FROM hashed h
  LEFT JOIN openalex.vector_search.vector_filter_fields_sent m ON m.id = h.id
  LEFT JOIN recent r ON r.work_id = h.work_id
  WHERE r.work_id IS NOT NULL OR m.id IS NULL OR m.filter_hash <> h.filter_hash
)
SELECT
  t.id,
  e.embedding,
  t.publication_year,
  t.type,
  t.is_oa,
  t.language,
  t.author_ids,
  t.institution_ids,
  t.country_codes,
  t.is_retracted,
  t.source_id,
  t.cited_by_count,
  t.funder_ids,
  t.has_fulltext,
  t.has_abstract,
  t.license_id,
  t.filter_hash,
  t.send_reason,
  abs(hash(t.id)) % 10 AS batch_id
FROM todo t
JOIN openalex.vector_search.work_embeddings_qwen3 e ON e.work_id = t.work_id;

CREATE OR REPLACE TABLE openalex.vector_search.vector_sync_deletes_qwen3 AS
SELECT m.id
FROM openalex.vector_search.vector_filter_fields_sent m
LEFT ANTI JOIN openalex.works.openalex_works w ON w.id = CAST(substring(m.id, 23) AS BIGINT);

-- A missing or emptied mirror would make every work 'unmirrored' (474M sends): fail instead.
SELECT assert_true(
  (SELECT COUNT(*) FROM openalex.vector_search.vector_sync_staging_qwen3 WHERE send_reason = 'unmirrored') <= 5000000,
  'vector stage: more than 5M works with a vector have no mirror row; vector_filter_fields_sent looks missing or emptied (oxjob #1433)'
) AS mirror_ok;
