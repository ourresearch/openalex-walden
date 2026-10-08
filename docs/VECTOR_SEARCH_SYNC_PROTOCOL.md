# Semantic-search vectors: how they are built and kept current

Status 2026-10-06 (oxjobs #1275, #1433). Replaces the old protocol for the Databricks Vector Search index behind
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
   builds `vector_sync_staging_qwen3` (10 batches) from every work with a vector that was **embedded** in the last
   2 days, with its 14 filter fields and their `filter_hash`. Runs on the warehouse on purpose: the same query stalled
   for hours on the job cluster.
3. **sync** (`sync_vector_index_qwen3.ipynb`, `is_full_sync=false`, 2–8-worker job cluster): bulk-indexes the staging
   table into `works-vectors-v2` (`index` ops, so re-sending is safe), checkpointing per batch; after a clean run MERGEs
   the staged hashes into the mirror `openalex.vector_search.vector_filter_fields_sent`; then drops the staging and
   checkpoint tables.

## Filter-field sync (`jobs/vector_filter_sync_qwen3.yaml`, 00:45 CT, paced)

1. **stage** (`notebooks/elastic/stage_vector_filter_sync.sql`, same warehouse): `CREATE TABLE IF NOT EXISTS`
   `vector_filter_sync_staging` (20 batches) = every work with a vector whose 14 filter fields **changed** (their
   `xxhash64` differs from the mirror) or that is **unmirrored**; and `vector_filter_sync_deletes` (mirror ids whose
   work no longer exists). Fails if more than 20M works are unmirrored (the mirror is broken). IF NOT EXISTS: an
   unfinished night's tables are resumed, not re-staged.
2. **sync** (same notebook, `resume=true`, `cleanup=true`, 2 workers, `spark.task.cpus=4`, paced as below): sends the
   staged works, then deletes the gone works (`max_deletes` 13M for the first catch-up; set it back to ≈ 1M once caught up), MERGEs the hashes into the mirror and drops the three
   tables, all only once every batch is done.

Sizes: the first catch-up (8 Oct 2026) ≈ 42M re-sent (36.2M changed, 6.0M unmirrored) and 10.75M deleted (the #1540,
#1581 and #1099 merges), 2-3 nights at the guard's pace; ≈ 1-3M a night after that.

**Why the filter-field diff (oxjob #1433):** semantic search pre-filters kNN on this index's own copy of the filter
fields. Until 2026-09-29 only works whose text changed were re-sent, so every metadata change (the 2026-09-27
affiliation swap, author moves, citations) left the copy stale: ≈ 10% of works had stale institutions. The API now
also re-checks filters on works-v34 when it hydrates hits (elastic-api `core/vector_index.py`), which stops wrong
hits but not misses. The mirror was seeded on 2026-09-29 from `openalex_works` version 16676, the version the
one-off refresh sent (160.4M works, paced over 1-6 Oct; then 4.58M deletes). **Both stage files' projection and hash must match the mirror's:** changing either re-sends
every work once.

Typical night: ~300K works, ~25 min wall, ≈$1 of endpoint. Failure emails jason@ourresearch.org; a failed run is
simply re-run (every step is idempotent).

## Big re-sends: pace them (oxjob #1433)

The vector cluster (2 nodes, 1.7 TB index, no replica) serves kNN from the page cache. Overwriting tens of millions
of docs in place sets off merges that evict it: on 2026-09-29 ≈ 40M docs in one evening made semantic search fail
60-78% of requests, and recovery took ≈ 90 min after the writes stopped (the damage lags the writes). So any re-send
bigger than a normal night goes through the sync notebook's pacing widgets: `max_batches` (per run), `max_minutes`,
and the guard (`knn_guard_ms` > 0: before each batch wait for ≤ `max_merges` merges on the index and a direct kNN probe
≤ `knn_guard_ms`, else stop). A run that leaves batches undone skips the deletes, the mirror and the cleanup; the next
run with `resume=true` carries on. Example: `jobs/vector_filter_sync_qwen3.yaml` (00:45 CT nightly, 2 workers,
`spark.task.cpus=4`); nights of the 160.4M refresh did 1-20 batches of 1.6M. Watch direct kNN (0.1-0.3 s healthy) and the Analytics Engine semantic 5xx rate, which was
already 5-15% an hour before any re-send.

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
