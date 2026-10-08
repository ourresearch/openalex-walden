-- Stage the quiet-hours filter-field sync of works-vectors-v2 (oxjob #1433). Runs as the `stage` task of
-- jobs/vector_filter_sync_qwen3.yaml (00:45 CT), before sync_vector_index_qwen3 sends the staged works in paced batches.
--
-- works-vectors-v2 keeps its own copy of 14 filter fields, which semantic search filters on. The 10:00 CT nightly
-- (embed_qwen3_nightly) sends only works embedded in the last 2 days, so a work whose fields move without a text change
-- (a nightly re-stamp, a date-kept backfill like the 27 Sep affiliation swap) would keep stale fields forever. This
-- stage picks those up by hash against vector_filter_fields_sent, the mirror of what the index holds:
--   changed    the hash of its 14 filter fields differs from the mirror row;
--   unmirrored it has a vector but no mirror row (new since the mirror was seeded and not yet sent by a run that
--              records hashes).
-- Mirror rows whose work no longer exists go to vector_filter_sync_deletes (the sync deletes them, max_deletes guard).
--
-- Why a separate quiet-hours job: re-sending millions of docs in place sets off merges that evict the page cache kNN
-- needs on the 2-node vector cluster; on 29 Sep that made semantic search fail 60-78% of requests. The sync notebook
-- paces the batches (merge + kNN guard) and stops for the night when the index is unhealthy.
--
-- IF NOT EXISTS: a night that leaves batches undone keeps its staging, checkpoint and deletes tables, and the next night
-- resumes them instead of re-staging. A clean run MERGEs the sent hashes into the mirror and drops all three, so the
-- next night stages a fresh diff.
--
-- The projection and hash must stay identical to the ones in stage_vector_sync_qwen3.sql and the mirror seed (oxjob
-- #1433 work/seed.sql): change a field here and every work re-sends once. batch_id range must match num_batches in the
-- job yaml. pmod, not abs(hash) %: abs overflows on the ANSI warehouse for the id that hashes to -2^31.

CREATE TABLE IF NOT EXISTS openalex.vector_search.vector_filter_sync_staging
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
todo AS (
  SELECT h.*, CASE WHEN m.id IS NULL THEN 'unmirrored' ELSE 'changed' END AS send_reason
  FROM hashed h
  LEFT JOIN openalex.vector_search.vector_filter_fields_sent m ON m.id = h.id
  WHERE m.id IS NULL OR m.filter_hash <> h.filter_hash
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
  pmod(hash(t.id), 20) AS batch_id
FROM todo t
JOIN openalex.vector_search.work_embeddings_qwen3 e ON e.work_id = t.work_id;

CREATE TABLE IF NOT EXISTS openalex.vector_search.vector_filter_sync_deletes AS
SELECT m.id
FROM openalex.vector_search.vector_filter_fields_sent m
LEFT ANTI JOIN openalex.works.openalex_works w ON w.id = CAST(substring(m.id, 23) AS BIGINT);

-- A missing or emptied mirror would make every work 'unmirrored' (≈ 475M sends): fail instead. 6 Oct 2026, a week
-- after the seed: 5.6M unmirrored (new works), 23.9M changed (15.7M of them a primary-source change). 7 Oct: 5.8M
-- unmirrored, 27.9M changed, 4.43M gone (#1540 merges). 8 Oct: 6.0M unmirrored, 36.2M changed, 10.75M gone
-- (+ #1581/#1099 dedup merges).
SELECT assert_true(
  (SELECT COUNT(*) FROM openalex.vector_search.vector_filter_sync_staging WHERE send_reason = 'unmirrored') <= 20000000,
  'vector filter sync: more than 20M works with a vector have no mirror row; vector_filter_fields_sent looks missing or emptied (oxjob #1433)'
) AS mirror_ok;
