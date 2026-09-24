# Semantic-search vectors: how they are built and kept current

Status 2026-09-24 (oxjob #1275). Replaces the old protocol for the Databricks Vector Search index behind
`/find/works`, which no longer exists (endpoint 404, no vector-search endpoints in the workspace).

## The stack

| piece | where | notes |
|---|---|---|
| corpus vectors | `openalex.vector_search.work_embeddings_qwen3` | 1024-d, `databricks-qwen3-embedding-0-6b` (pay-per-token), text = `Title / Abstract / Venue` capped at 2,000 chars, documents bare (no instruction prefix). Columns: `work_id, embedding, publication_year, type, is_oa, has_abstract, has_content_pdf, has_content_grobid_xml, embedded_at, bucket`. Clustered by `work_id`. |
| embedding text | `openalex.vector_search.work_text_qwen3_source` | what each vector was computed from; the nightly job diffs against it to find changed works |
| ES index | `works-vectors-v2` on the `openalex-vector-search` deployment | 8 shards, 0 replicas, `dense_vector` bfloat16 + `int8_hnsw`, 14 filter fields; created by `notebooks/elastic/sync_vector_index_qwen3.ipynb` (do not hand-create it) |
| query side | elastic-api `core/semantic_search.py` | same model with Qwen3's query instruction prefix, kNN k=50 in `core/vector_index.py`, citation-saturation boost |

The gte-large-en stack (`work_embeddings_v2`, `works_for_embedding`, `works-vectors-v1`, `ContinuousEmbeddings.py`,
`sync_vector_index.ipynb`) was retired 2026-09-24. Some March-2026 author-disambiguation notebooks in
`notebooks/vector_search/` still name `work_embeddings_v2`; they are unscheduled and would need re-pointing at
`work_embeddings_qwen3` (a different embedding space) before reuse.

## Nightly refresh (`jobs/embed_qwen3_nightly.yaml`, daily 15:00 UTC, after Walden End 2 End lands)

1. **embed** (serverless notebook `notebooks/vector_search/EmbedQwen3Incremental.py`): candidates = works updated in
   the last `lookback_days` (3) whose text is new or differs from `work_text_qwen3_source`, plus any titled work with
   no vector; MERGE the text into the source table; DELETE stale vectors; embed in 250K-row chunks, 4 in parallel,
   blind appends. Refuses to run past `max_works` (50M) so a text-format change cannot re-embed the corpus unattended.
   Returns a JSON summary (`dbutils.notebook.exit`).
2. **stage** (SQL-file task on the serverless warehouse `69a583ace3bdc8d0`, `notebooks/elastic/stage_vector_sync_qwen3.sql`):
   rows embedded in the last 2 days joined to their 14 filter fields → `vector_sync_staging_qwen3` (10 batches).
   This runs on the warehouse on purpose: the same query stalled for hours on the job cluster.
3. **sync** (`sync_vector_index_qwen3.ipynb`, `is_full_sync=false`, 2-worker job cluster): reuses the staging table
   and bulk-indexes it into `works-vectors-v2` (`index` ops, so re-sending is safe), checkpointing per batch, then
   drops the staging/checkpoint tables.

Typical night: ~300K works, ~25 min wall, ≈$1 of endpoint. Failure emails jason@ourresearch.org; a failed run is
simply re-run (every step is idempotent).

## Full rebuild (only if the model or the text format changes)

- Re-embed with the bulk runner in oxjobs #1275 (`scratch/embed_loop.py`: 500 hash buckets, parallel blind appends,
  ≈$2.1K and 15 h for 474M works on the pay-per-token endpoint).
- Build the index with job "Sync Vector Index to Elasticsearch (Qwen3, oxjob #1275)" (`is_full_sync=true`, 8 workers;
  do not shrink). It needs ~1.5 TB free on the vector deployment on top of whatever is serving; add a zone
  (`ec_add_zone.py` in the oxjob, Elastic Cloud control-plane key required) and remove it afterwards.
- Freeze `cluster.routing.rebalance.enable=none` for the build and unfreeze after, or ES will shuttle shards mid-build.
- Benchmark old vs new before flipping `WORKS_VECTOR_INDEX` in elastic-api (harness in oxjob #1275 / #1258).

## Checks

```bash
# index size and count
curl -s "$ES_VECTOR_SEARCH_URL/_cat/indices/works-vectors-v2?v&h=index,health,docs.count,store.size&bytes=gb"
# last nightly run
databricks jobs list-runs --job-id 760271376237273 --limit 1
# works without a vector (should be ~0 after each run)
SELECT count(*) FROM openalex.works.openalex_works w
LEFT ANTI JOIN openalex.vector_search.work_embeddings_qwen3 e ON e.work_id = CAST(w.id AS STRING)
WHERE w.title IS NOT NULL AND length(trim(w.title)) > 0
```
