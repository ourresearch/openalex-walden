-- Stage the nightly ES vector sync on a SQL warehouse (oxjob #1275).
-- Runs as the `stage` task of jobs/embed_qwen3_nightly.yaml, right before sync_vector_index_qwen3 (incremental),
-- which reuses this table instead of rebuilding it. Why here and not on the job cluster: the same query stalled
-- for hours on the 2-worker classic cluster (no file pruning once the 1024-float embedding column is in play);
-- the serverless warehouse does it in ~2 min (2026-09-24). batch_id % 10 must match NUM_BATCHES (incremental) in
-- the sync notebook. Lookback 2 days: the embed task that precedes this stamps embedded_at minutes earlier.
-- oxjob #1433 (2026-09-29 22:00 CT): the filter-field diff version of this file (walden 996ab8a5) is parked. On the
-- 2-node vector cluster, re-sending tens of millions of docs in place makes merges evict the page cache kNN needs, and
-- semantic search failed 60-78% of requests during the one-off refresh. It comes back with more cluster capacity or a
-- per-night cap. The sync notebook's deletes and mirror steps stay inert with this file (no deletes table, no
-- filter_hash column).
CREATE OR REPLACE TABLE openalex.vector_search.vector_sync_staging_qwen3
PARTITIONED BY (batch_id) AS
WITH recent AS (
  SELECT * FROM openalex.vector_search.work_embeddings_qwen3
  WHERE embedded_at >= current_timestamp() - INTERVAL 2 DAYS
),
w AS (
  SELECT * FROM openalex.works.openalex_works
  WHERE id IN (SELECT CAST(work_id AS BIGINT) FROM recent)
)
SELECT
  concat('https://openalex.org/W', e.work_id) AS id,
  e.embedding,
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
  w.primary_location.license_id AS license_id,
  abs(hash(concat('https://openalex.org/W', e.work_id))) % 10 AS batch_id
FROM recent e
JOIN w ON w.id = CAST(e.work_id AS BIGINT)
