# OpenAlex Walden

Data pipeline for OpenAlex research metadata. Repo: `~/Documents/openalex-walden`

## Structure

- `notebooks/` — Code, organized by domain (end2end, ingest, maintenance, etc.)
- `jobs/` — YAML configs defining how to run notebooks (clusters, timeouts, params)
- `libraries/dlt_utils/` — Reusable DLT utilities
- `utils/` — Local utilities (including databricks_sql.py)
- `qa/issues/` — QA issue tracking
- `databricks.yml` — Bundle config. Uses shared state (`root_path: /Workspace/Shared/.bundle/`).

## Pipeline Overview

E2E stages: Ingest (DLT per source) → Union → Transform → Enrich → Output → RAS Dashboard
Main output: `openalex.works.openalex_works` → syncs to Elasticsearch, populates Wunpaywall.

## Querying Data

Activate venv: `source .venv/bin/activate`

```python
from utils.databricks_sql import run_query, run_query_df
results = run_query("SELECT * FROM openalex.works.openalex_works LIMIT 10")
df = run_query_df("SELECT * FROM openalex.works.openalex_works LIMIT 10")
```

Key schemas: `openalex.works`, `openalex.works_legacy`. Setup: `docs/DATABRICKS_SETUP.md`

## Notebook Deployment

Notebooks use `source: GIT` — Databricks fetches from GitHub `main` at runtime. Push to main = deployed on next job run. DAB deploy only for job YAML changes.

## Schedule

- **E2E workflow**: Daily 11:00pm CT (05:00 UTC), ~4hrs. Cron: `0 0 5 * * ?` UTC.

## End2End Job Conflicts

Before manually triggering E2E, check **LandingPageWork** (job ID: 281343722226857). Same DLT pipeline — concurrent runs fail with "Pipeline update already in progress."

LandingPageWork: every 4h from 01:00 UTC (cron: `56 0 1/4 * * ?`, ~40 min each).

Safe windows (allow 45 min after each start): 01:45–04:55, 05:45–08:55, 09:45–12:55, 13:45–16:55, 17:45–20:55, 21:45–00:55 UTC.

Check: `databricks jobs list-runs --job-id 281343722226857 --active-only --output json | jq 'if length > 0 then "LandingPageWork running — wait" else "Safe to start" end'`

## DAB Deploy

Primary: push to main → GitHub Actions validates + deploys with Slack notification to #dev.
Local (when needed): `./scripts/deploy.sh` — never run `databricks bundle deploy` directly.

**CRITICAL**: Always `git pull` before deploying. Shared state means deploys overwrite everyone's job definitions.

**DAB-managed jobs**: authors, sync_all_works_to_elasticsearch, sync_content_index_to_d1, taxicab, vacuum_optimize_tables, vector_embeddings, walden_end2end

**Migrating existing jobs to DAB**: Do NOT use `databricks bundle deployment bind` — it causes issues. Instead: add the job YAML to `jobs/` and `databricks.yml`, deploy (which creates a new job), then delete the old standalone job from the Databricks UI.

**`source: GIT` requires `git_source`**: Any job YAML with `source: GIT` in the notebook_task MUST include a `git_source` block at the job level. Without it, `databricks bundle validate` resolves `notebook_path` relative to the YAML file and fails. Copy from an existing job like `taxicab.yaml`.

## DLT Pipelines

**Streaming sources are append-only.** Tables feeding DLT streaming (`skipChangeCommits=true`) break on MERGE/UPDATE/DELETE. Checkpoint fails, blocks all downstream.

**Protected tables** (no in-place modification): `openalex.landing_page.taxicab_enriched_new`, any streaming source.

**Fix data in streaming source**: (1) append new records, or (2) coordinate with Casey for full refresh + checkpoint reset.

Other rules:
- Never full refresh tables calling external APIs — use checkpoint reset instead
- Table names in API = DLT function names, not full catalog paths
- Checkpoint reset: `databricks api post /api/2.0/pipelines/<id>/updates --json '{"reset_checkpoint_selection": ["table_name"]}'`

## Delta Gotchas

- `dropDuplicates()` before MERGE to avoid `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`
- Concurrent MERGEs to same partition fail — use `max_concurrent_runs=1`
- CLI lacks some features — use `databricks api post` for REST endpoints

## Bulk HTTP Jobs (Taxicab/Parseland)

- **Connection pooling**: Never use raw `requests.post()` in threads — limits to 10 connections. Use `BulkHttpClient` from `notebooks/maintenance/lib/`.
- **ECS auto-scaling**: Taxicab and Parseland now scale up based on request count, not CPU. No need to suspend auto-scaling before bulk jobs.

## Editing Notebooks

SQL warehouse jobs only support SQL cells. For Python, use compute cluster (no `warehouse_id`).

Notebooks (`.ipynb`) store cell sources as JSON arrays. `NotebookEdit` replaces entire cells.
- **Small changes**: Use `sed` via Bash for surgical edits
- **Temporary changes**: Run on Databricks, restore with `git checkout`
- **Permanent changes**: Verify diff with `git diff` before committing

## Affiliations Dashboard Pipeline

Two E2E tasks sync the `raw-affiliation-strings-v2` ES index:

1. **Refresh_RAS_Counts** — SQL notebook, rebuilds counts by exploding `authorships.raw_affiliation_strings`
2. **Sync_RAS_to_Elasticsearch** — Python notebook, syncs to ES (8 partitions, 2 threads, chunk_size=500)

Key tables: `openalex.institutions.affiliation_string_works_counts`, `affiliation_strings_lookup_with_counts`, `raw_affiliation_strings_institutions_mv`

## Task Management

Larger tasks tracked at `~/Documents/oax-jobs/`. See `oax-jobs/AGENTS.md` for job system reference.

## Graduated Learnings

### Databricks Operations

- **Bulk MERGE pattern**: For 100M+ rows, batch by grouping key. 227M rows in 1h04m across 50 batches. Template: `oax-jobs/active/endpoint-source-mapping/scripts/merge_repo_id.py`
- **Ad-hoc job submissions**: `data_security_mode: SINGLE_USER` required for Unity Catalog. Match production cluster config. `num_workers: 0` = too slow for big tables. Use `warehouse_id` inside `notebook_task` (not `sql_warehouse_id`).
- **Instance profiles with S3**: `databricks-s3-ingest-e9cd7-access-data-buckets`, `databricks-s3-ingest-94c89-access-data-buckets`
- **Delta ConcurrentAppendException**: Parallel `INSERT INTO` same unpartitioned table fails. Fix: separate staging tables per thread, or serialize.
- **Views over large tables**: VIEW on 480M rows too slow on single-node. Materialize first (`INSERT INTO ... SELECT`).
- **DAB deploy lock blocks CI**: Local deploy leaves lock, CI fails. Push to main and let CI handle it.
- **DLT new columns don't backfill**: Adding column to DLT definition doesn't populate existing rows. Fix: MERGE directly into target table.
- **E2E Guardrails override**: Launch-time parameter. Set `"true"` → deploy → trigger → revert → redeploy. For failed scheduled runs: repair with `rerun_tasks: ["Guardrails"]` + `rerun_dependent_tasks: true`.
- **SQL notebook gotchas**: `read_csv` TVF breaks on special chars (use PySpark). `LATERAL VIEW EXPLODE` + `LEFT JOIN` can't share FROM. `REFRESH MATERIALIZED VIEW` needs DBSQL Serverless/Pro.
- **ES upsert-only sync phantoms**: Upsert sync never deletes removed docs. Fix: delete index + full rebuild periodically.
