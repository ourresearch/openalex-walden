# Rebuild an Elasticsearch index and swap it in

This is the standard way OpenAlex reindexes an entity (works, authors, institutions, sources, locations, …) when a
change touches most documents. Never rewrite most of a live index in place: build a fresh copy beside it from the
Databricks source table, pinned to one Delta version, with no replicas, no refresh and no search traffic, so it loads
as fast as the cluster can index. Then flip the read alias in one atomic call. Rollback is flipping it back.

Proven on works, 2026-10-01 (oxjob #1456, `works-v34` → `works-v35`, the #1322 keywords vocabulary). The full run log
is in that oxjob's README; the numbers that worked are in § 1 so the next run can copy them.

Why not in place: an update rewrites the whole document on every copy and leaves a deleted copy for merges. On
2026-09-30 an in-place reload of 472M works at zero replicas took API search down for 2.5 hours (#1443), and the safe
in-place pace was days.

## 1. What worked for works (2026-10-01): the numbers

Copy these for the next works run; scale by document count and node count for other entities.

| | Value |
|---|---|
| Source | `openalex.works.openalex_works` pinned at Delta version **16745** (an OPTIMIZE commit at 11:58 UTC; last data commit 16722 at 09:00 UTC), 472,427,558 rows |
| Elastic Cloud | Elasticsearch 8.19, us-east-1, data nodes `aws.es.datahot.c6gd` (32 vCPU, 64 GB, ~1.85 TB each), 3 masters |
| Data nodes | **38 during the build** (scaled 22 → 38 on 2026-09-30 for #1443; about $80/node/day). Scaled back 38 → 30 → 22 on 2026-10-02, ~20 min per step at the default 40 MB/s recovery, cluster green throughout |
| New index settings | 72 primaries (same as old), **0 replicas, `refresh_interval: -1`, `total_shards_per_node: 2`** (= ceil(72 / 38)), everything else copied from the old index's live settings; mapping copied from the old index's live mapping (byte-equal) |
| Load job | one-off Databricks job from `jobs/index_rebuild_works.yaml`: `plan` → `create_index` → **16 id-range chunks in 4 lanes** on one cluster, with the watchdog beside them → `close_load`. DBR 16.4.x, `rd-fleet.8xlarge` driver and workers (32 vCPU each) |
| Writers | **4 workers × 32 cores = 128 bulk writers** shared by the 4 lanes. 8 workers (256 writers) was cancelled by the watchdog after 34 min (see step 3) |
| Watchdog | search queue > 50 on any data node for 3 consecutive 60 s checks → cancel; cluster disk > 75 % → cancel |
| Load rate / time | ≈ 25–29K docs/s average at 128 writers; **5 h 12 min** for 472.4M docs (14:46–19:58 UTC); chunks took 70–93 min in the first round, 41–69 min in the last; 0 `index_failed` |
| Cluster during the load | max search queue ≤ 2 (one spike to 7 at 256 writers before the cancel); data-node CPU avg ~20 %; API latency unchanged (0.7–1.1 s end to end, `db_response_time_ms` ~30) |
| Disk | new index 5.7 TB primaries (old was 8.06 TB with 24.5 % tombstones); 10.3–10.6 TB with 1 replica; peak node disk 52 % at 38 nodes during the load, 48 % after replicas, 45 % at 30 nodes |
| close_load | count −2 vs source at V (0 duplicate or null ids at V), passed with `close_tolerance` 10; 0 of 1,000 random ids missing |
| Replicas | `number_of_replicas: 1` with transient `indices.recovery.max_bytes_per_sec: 250mb` and `node_concurrent_recoveries: 4` → **green in 16 min** (5.7 TB at ≈ 10 GB/s cluster-wide). At the 40 MB/s default this would be hours |
| Catch-up | empty: the load started after the morning sync and finished before the next nightly, so no work had changed since V and the delete ledger had no rows since T |
| Warm-up | 14 representative queries (default list, filters, search, 5 group_bys, keyword and source and author filters, DOI term) run twice against the new index by name; second pass 3–474 ms |
| Swap | one `POST _aliases` at 21:02:38 UTC, 6 h 16 min after the load started. API unchanged the same second; the new keywords were in filters and group_by immediately |
| First nightly on the alias | 2026-10-02 run clean: `Sync_to_Elasticsearch` 38 min for ≈ 6.9M docs, 155 deletes, watermark row `works` advanced, Guardrails PASS |
| Old index | write-blocked 21:26 UTC, closed 13:21 UTC next day (0 reads in the prior minute), deleted 13:24 UTC, 16 h after the swap |
| Verify | old-vs-new `_source` diff of 1,000 docs: 88 % differed outside the intended fields, **all because the old index was stale** (see step 8). Keyword checks passed: all 4,634 sampled keyword ids in the vocabulary; 20 `keywords.id` filter counts within max(2, 0.1 %) of Delta |

## 2. When to use it

| Change | Do this |
|---|---|
| Fewer than ~10M documents change, no mapping change | The normal nightly / in-place sync |
| 10M or more documents change (e.g. a new field on every work) | **This playbook** |
| Any mapping, analyzer, index-sort or shard-count change | **This playbook** (in place is impossible) |
| Tombstones above ~25 % of documents, or stale docs piling up | **This playbook** (a rebuild drops them) |

## 3. Names and the one rule

- Versioned index: `{entity}-v{N}`. The new one is `{entity}-v{N+1}`, e.g. `works-v34` → `works-v35`.
- Alias: `{entity}` (e.g. `works`). **Every reader and every writer uses the alias, never the versioned name.** That
  is what makes a swap one call with no deploys. An alias that resolves to one index accepts index, bulk, update,
  delete, refresh, count, forcemerge and settings calls exactly like the concrete name (proven on a throwaway index
  2026-10-01). Two things differ, both useful: `DELETE /<alias>` is refused, and `_cat/indices/<alias>` matches nothing.
- Exception: the rebuild itself writes the new index **by its versioned name**, and the catch-up in step 7 does too,
  because the alias still points at the old index until step 10.
- **Works is done** (2026-10-01): openalex-api `settings.WORKS_INDEX = "works"` (Heroku v2065); walden `sync_works`,
  `delete_works` and `jobs/expunge_deletes_works.yaml` write `works`; openalex-users-api `RAS_WORKS_INDEX` defaults to
  `works`; openalex-api-proxy runbook tools resolve `GET _alias/works`. The one-off catch-up controllers
  (`KeywordsCatchup.py`, `AffiliationSwapCatchup.py`) keep a literal by design. Other entities still need step 0.

## 4. Roles and stop rules

- **Owner** (Casey for the Elastic cluster) approves steps 1, 3 (the chosen writer count), 6, 10 and 12, and is the
  only one who changes Elastic Cloud capacity (console or Cloud API; there is no `ecctl` or API key on the dev laptops).
- **Operator** (an agent or a person) runs the steps, logs each one in the job README, and stops on any stop rule.
- **Stop rules (any one: pause the writers and tell the owner):** a data node's search queue above 50 for 3 minutes
  (the watchdog does this automatically); API p95 latency 20 % above the same hour yesterday for 15 minutes; API 5xx
  above 1 %; cluster disk above 75 %; cluster not green for 30 minutes during the load.

## 5. Parameter sheet (fill in per run, copy into the job's PLAN.md)

| Parameter | Works, 2026-10-01 |
|---|---|
| Entity / alias | works / `works` |
| Old index → new index | `works-v34` → `works-v35` |
| Source table | `openalex.works.openalex_works` |
| Pinned Delta version V and its commit time T (UTC) | 16745, 2026-10-01 11:58:10 (`IndexRebuild plan` prints both) |
| Documents expected | 472,427,558 (`SELECT count(*) FROM <source> VERSION AS OF V`) |
| Shards | 72, same as old |
| Data nodes during the build | 38 |
| Writers | 128 (4 × rd-fleet.8xlarge) |
| Target load hours | under 12; actual 5.2 |
| Changes landing during the build that need replay | none (started after the morning sync, swapped before the next) |

## 6. The steps

### Step 0 (one time per entity): readers and writers use the alias
Point every reader and writer at the alias (API settings, sync and delete notebooks, jobs). Grep every repo for the
versioned name; it should remain only in comments and docs. Insert a watermark row keyed by the alias (step 11.1) so
the first nightly on the alias does not fall back to a 2-day window. Ship it and watch one nightly before the first
rebuild. Deploying the API on the alias before the swap is a no-op (the alias still points at the old index) and
means the swap takes effect for the API the instant the `_aliases` call lands.

### Step 1: pre-flight (no writes)
1. **Freeze window.** No other bulk or rebaselined writes to this entity until the swap. Nightly syncs keep running;
   step 7 replays them.
2. **Disk.** New index size at 0 replicas ≈ the old primary size × live / (live + deleted) documents (works: 8.06 TB ×
   0.80 ≈ 6.5 TB projected, 5.7 TB actual). Peak disk = now + new × 2 (after replicas) must stay under 70 % of the
   cluster, including keeping the old index until step 12. `GET _cat/allocation?h=disk.indices,disk.total`,
   `GET <old>/_stats/docs,store`.
3. **Capacity.** Average data-node CPU and the busiest node's search queue now: `GET _cat/nodes?h=name,cpu,load_1m`,
   `GET _cat/thread_pool/search?h=node_name,queue`. The owner decides whether to add nodes for the build. Works fit on
   38 nodes with headroom; 22 is the steady-state size.
4. **Mapping.** Start from the **live** mapping of the old index, never from the index template: templates drift
   (works: template 384 fields vs 503 live on 2026-09-30). Apply the job's intended mapping changes. Save the final
   mapping as a file in the job and diff it against the old index's mapping; every difference must be intended.
   `IndexRebuild create_index` does this and asserts byte-equality after the PUT.
5. **Settings.** Copy the old index's settings (analysis, sort, codec, merge policy, translog). Build-time overrides:
   `number_of_replicas: 0`, `refresh_interval: "-1"`, `index.routing.allocation.total_shards_per_node:
   ceil(shards / data nodes)` so primaries spread evenly. Record the old index's live values for step 6.
6. **Pin the source.** `IndexRebuild plan` prints V and T. The latest version is often an OPTIMIZE commit after the
   last data change; reading at V is still correct. Every load chunk reads `<source> VERSION AS OF V`, so a nightly
   rebuild of the source table mid-load changes nothing. Delta keeps about 30 days of history on the works table.
   Start the load right after a nightly's sync finishes: then nothing changes before the swap and step 7 is empty.

### Step 2: create the new index
`IndexRebuild create_index` (dry run first, then `dry_run=false`). Check: `GET <new>/_mapping` equals the saved
file; `GET _cat/shards/<new>` shows every primary STARTED and spread 1–2 per node.

### Step 3: calibrate (about 1 hour)
Load one slice (works: one id range of about 1/64 of the table, about 7.4M documents) into the new index at 64
writers for 15 minutes, then 128, then 256. At each level record: documents per second (`GET <new>/_stats/indexing`
twice, 60 s apart), write thread-pool queues, the busiest live search queue, API p95 and 5xx. Projected hours =
expected documents / rate. If over target, the owner adds nodes or accepts the time. The slice's documents are kept
(the full load overwrites them harmlessly).

**Calibration can pass a writer count that the full load cannot sustain.** On 2026-10-01 a 15-minute slice at 192
writers showed ≈ 50K docs/s with no stop rule tripped, so the full load started at 8 workers / 256 writers. It lasted
34 minutes: one data node's search queue climbed 264 → 138 → 55 and the watchdog cancelled the run at ≈ 80M docs. The
restart at 4 workers / 128 writers (≈ 25–29K docs/s) ran the full 5 h 12 min with max search queue ≤ 2 and no API
change. The slice does not stress the cluster the way 16 chunks across 4 lanes do: one hot node is enough to trip the
rule. **Start the full load one level below the highest passing calibration level**, or hold the calibrated level for a
full hour before committing. A cancel costs only the elapsed minutes (chunks are idempotent), so when in doubt it is
cheaper to try the higher level with the watchdog on than to guess low.

### Step 4: full load
Chunks by **id range** (works: 16 ranges from `approx_percentile(id)` at V, computed once by `IndexRebuild plan` and
published as task values), each reading the pinned version and writing the new index by name. Don't chunk with
`pmod(id, N)`: the works table is clustered by id, so a modulo filter makes every chunk scan the whole table. The
rebuild writer also skips the count query and the repartition. Chunks run in 4 lanes at the chosen writer count with
the watchdog beside them (search queue above 50 on any node for 3 minutes, or disk above 75 % → cancel; repair the
run to rerun unfinished chunks). Chunks are idempotent. Progress: `GET <new>/_stats/docs` against the expected
total. A chunk that had document-level errors still reports success (`sync_works` only prints them), so step 5's
count is the real check.

### Step 5: close the load
`IndexRebuild close_load`: refresh, count within `close_tolerance` (default 10) of the source count at V, and 1,000
random source ids all found by `_mget`. The first works run closed 2 short with no duplicate or null ids in the
source; the two were not worth a chunk repair, and the next content-hash bump re-sends them through the nightly.

### Step 6: replicas and live settings
1. `IndexRebuild live_settings dry_run=false`: `refresh_interval` back to the old index's value (works: `1h`) and
   `total_shards_per_node` back to the old index's (usually none). Doing this before replicas is fine.
2. `IndexRebuild add_replicas` (dry, then `dry_run=false`): sets transient `indices.recovery.max_bytes_per_sec`
   (250mb; the default 40mb would take hours) and `cluster.routing.allocation.node_concurrent_recoveries` 4, sets
   `number_of_replicas` to the old index's count, waits for green, then restores the recorded cluster settings. If
   the build-time shard cap is still on the index it raises it, since with replicas the index needs twice the slots.
   Works: green in 16 minutes.

**Alternative order used once (2026-10-01, to save time):** swap first (step 10) with the new index at 0 replicas, then
set the old index to 0 replicas (frees its replica's disk instantly), then add the new index's replica. It works, and
the recovery headroom is better, but the live index serves from a single copy per shard until green: a data-node
failure in that window makes 1–2 shards unavailable, and per-shard search capacity is halved. The notebook's `swap`
mode refuses while replica counts differ, so that swap was a hand-run `_aliases` call. The standard order stays
replicas before swap.

### Step 7: catch-up (changes since T)
1. **Updates.** `sync_works target_index=<new> since_ts=<T − 60 s> manage_replicas=false`. The watermark is untouched.
2. **Deletes.** `delete_works target_index=<new> since_ts=<same>`: replays every ledger row since then.
3. **Ad-hoc writes.** Replay anything that wrote the old index after T.
4. If a nightly runs between this step and the swap, repeat 1 and 2 right before step 10. Check first: `SELECT
   count(*) FROM <source> WHERE updated_date > <last sync start>` and the delete ledger since T; if both are 0, skip.

### Step 8: verify
`IndexRebuild verify` is read-only and can run any time after step 5. It checks: count vs the source now; 1,000 random
ids present in the new index; a field-by-field `_source` diff of those docs between old and new; and for keyword-type
changes, that sampled ids are all in the vocabulary and 20 filter counts match Delta.

**The old-vs-new document diff fails on every rebuild and that is expected.** The old index is stale relative to Delta
on every field the content hash excludes, because the nightly never re-sent them: `citation_normalized_percentile`,
`fwci`, `cited_by_percentile_year`, `institutions_distinct_count`, `source.listed_in`, location `updated` timestamps,
and `updated_date` where the old index carries a transient bump Delta later lost (#679). It also differs on null vs
empty-array representation (`study_designs`, `author.observed_orcids`, `source.issn`), which `exists` queries treat
the same. On 2026-10-01, 882 of 1,000 docs differed, and every checked field in the new index equalled Delta at V.
**Read the diff as "new = Delta?", not "new = old?"**; the oxjob's `scratch/tooling/diff_old_new.py` prints the
sub-field paths so the differences can be classified in minutes. TODO: make `verify` compare new vs Delta at V directly,
and build the 500-query replay set.

### Step 9: warm (15–30 minutes)
Run a sample of representative queries against the new index **by name** so its caches are hot: the default list sort,
a year + OA filter, title and abstract searches, group_bys on oa_status / year / type / institution / topic, keyword,
source, author and DOI filters. Two passes; the second should be mostly single- or double-digit milliseconds.

### Step 10: swap (one call)
`IndexRebuild swap dry_run=false`, which sends
```
POST _aliases
{"actions": [
  {"remove": {"index": "<old>", "alias": "<alias>"}},
  {"add":    {"index": "<new>", "alias": "<alias>"}}
]}
```
Atomic: no request sees neither index. It refuses unless the new index is green with the old index's replica count.
Watch the API for 30 minutes: the proxy's p95, 5xx, search queues. Measure latency server-side (`db_response_time_ms`
in the API's `meta`, or `took` on a direct query): end-to-end timings from a laptop can be the laptop's network.

### Step 11: after the swap
1. The nightly now writes the new index through the alias. The sync watermark (`openalex.works.elastic_sync_state`) is
   keyed by `ELASTIC_INDEX`; the alias row must exist before the first nightly (works: inserted 2026-10-01, copied
   from the `works-v34` row). The versioned row goes inert.
2. `IndexRebuild block_old dry_run=false`: `index.blocks.write: true` on the old index. Keep it 48 hours (works: 16).
3. **Rollback** (while the old index exists): `IndexRebuild rollback dry_run=false` lifts the block and moves the
   alias back, then replay writes made since the swap with `sync_works since_ts=<swap − 60 s> --rollback-replay` and
   `delete_works since_ts=<same>`, both through the alias.

### Step 12: retire
1. `POST <old>/_close` first and leave it closed for a while: any forgotten caller that still names the old index
   now fails loudly with `index_closed_exception` instead of silently reading stale data. Check
   `GET <old>/_stats/search` twice a minute apart before closing; 0 queries means nothing reads it.
2. `DELETE <old>` (owner). After this, rollback is a snapshot restore (hours).
3. Scale the cluster back (owner, Elastic Cloud console). Each step drains the removed nodes at the default 40 MB/s;
   works 38 → 30 took ~20 min with 34 shards relocating, green throughout.
4. `PUT _index_template/<entity>` with the live new-index mapping so the template stops drifting, then
   `POST _index_template/_simulate_index/<entity>-v<N+2>` and check the simulated mapping is byte-equal to the live
   one and no alias is attached. Works: done 2026-10-02 (345 → 463 fields).
5. Delete the one-off load job; write the run-history row below; fold anything learned into this file.

## 7. Tooling (walden)

- `notebooks/elastic/sync_works.ipynb` (and each entity's sync notebook): widgets `target_index` (write another index
  by name; refuses one that does not exist or is the live index), `source_version` (`VERSION AS OF`), `since_ts`
  (catch-up), `id_range` (load chunk). With `target_index` set, the watermark is never touched and replicas are never
  managed, and the run ends before the final refresh cell.
- `notebooks/elastic/delete_works.ipynb`: `target_index`, `since_ts`.
- `notebooks/elastic/IndexRebuild.py`: the control notebook, one mode per run, `dry_run` default true: `plan`,
  `create_index`, `watchdog`, `close_load` (`close_tolerance`), `add_replicas`, `live_settings`, `verify`
  (`expect_diff`, `keywords_table`), `swap`, `rollback`, `block_old`. Evidence JSON for each mode lands in
  `/Volumes/openalex/works/data/es_rebuild/<new index>/`.
- `jobs/index_rebuild_works.yaml`: the one-off load job, deliberately **not** in `databricks.yml` `include:` so a
  bundle deploy never creates it. Create it by hand for one run (`databricks jobs create --json`), run it once, delete
  it after the swap. Copy it for the next entity.
- `oxjobs/working/es-index-rebuild-and-swap/scratch/tooling/submit.py`: builds a `databricks jobs submit` payload for
  one manual run of any of the three notebooks (`--go` to submit; set `DATABRICKS_CONFIG_PROFILE`). Control runs use a
  single-node `m5d.2xlarge`; `sync_works` runs use `rd-fleet.8xlarge` workers (`--workers N` = N × 32 writers).

## 8. Run history

| Date | Entity | Old → new | Why | Docs | Nodes | Writers | Load rate | Load time | Swap | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-10-01 | works | v34 → v35 | new keywords (#1322) | 472.4M | 38 (then back to 22) | 256 → **128** (256 cancelled by the watchdog after 34 min) | ≈ 25–29K docs/s | 5 h 12 min | 21:02 UTC same day | replicas 16 min at 250 MB/s; close_load −2 (tolerance 10); verify diff explained by stale old index; swap-before-replicas order used once; dropped 127M tombstones + 140K stale works; old index deleted after 16 h |

## 9. Lessons

- **The cluster auto-creates missing indices** from the index template. A write to a new index name that doesn't exist
  yet would silently create it from the drifted template. `sync_works` and `delete_works` refuse a target that doesn't
  exist or that is the live index.
- **Pin V, but know what it is.** The latest Delta version is often an OPTIMIZE commit after the last data change.
  Reading at V is correct; `plan` prints both.
- **Catch-up by `updated_date` misses changes that don't bump it** (hash-rebaselined changes such as the keywords ship).
  The works table has Delta row tracking: `_metadata.row_commit_version > V` selects exactly the rows written after V.
  Add a `since_version` mode to `sync_works` before the next rebuild that follows a rebaseline.
- **Calibration under-predicts the full load's pressure** (step 3). Start one level lower.
- **The old index is not the truth** (step 8). Verify against Delta.
- **Replica recovery at the default 40 MB/s is the slowest step by far**; at 250 MB/s it is minutes. Always set the
  transient recovery settings and clear them after green.
- **The swap only helps once readers use the alias** (step 0). Without it the API keeps reading the old name and
  `block_old` would break the nightly. The watermark row keyed by the alias is part of step 0.
- **Close before delete** (step 12). A closed index is a tripwire for forgotten callers; a deleted one is silent.
- **Watch scripts need timeouts.** A laptop network blip made a watcher print blank lines for 15 minutes; `curl -m`
  on every call, and measure latency server-side.
