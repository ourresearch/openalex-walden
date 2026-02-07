# OpenAlex Walden

Data pipeline for OpenAlex research metadata. Main pipeline: `jobs/walden_end2end.yaml`

## Pipeline Overview

The Walden End2End job runs these stages:
1. **Ingest** - DLT pipelines for each source (Crossref, PubMed, DataCite, MAG, Repos, PDF, Landing Page)
2. **Union** - Combine all sources
3. **Transform** - Authorships, locations, parsed names, affiliations
4. **Enrich** - Topics, institutions matching, author matching
5. **Output** - Works enriched, Wunpaywall, Elasticsearch sync
6. **RAS Dashboard** - Refresh affiliation string counts, sync to ES for affiliations dashboard

Main output table: `openalex.works.openalex_works` - used to sync to Elasticsearch and populate Wunpaywall.

Notebooks are in `notebooks/end2end/` and `notebooks/ingest/`.

## Querying Data

Activate venv first: `source .venv/bin/activate`

```python
from utils.databricks_sql import run_query, run_query_df

# Returns list of dicts
results = run_query("SELECT * FROM openalex.works.openalex_works LIMIT 10")

# Returns pandas DataFrame
df = run_query_df("SELECT * FROM openalex.works.openalex_works LIMIT 10")
```

Key schemas: `openalex.works`, `openalex.works_legacy`

Setup: See `docs/DATABRICKS_SETUP.md`

## Databricks Asset Bundles (DAB)

Jobs are deployed via DAB (`databricks bundle deploy`). As of 2026-02-01, DAB uses a shared state location (`/Workspace/Shared/.bundle/openalex-walden`) so all team members deploy to the same jobs.

**Before deploying:** Always pull the latest changes first to avoid overwriting others' work:
```bash
git pull origin main && databricks bundle deploy
```

**DAB-managed jobs** (safe to deploy):
- `authors.yaml` — Authors job
- `refresh_stale_parser_responses.yaml` — Parser maintenance
- `sync_all_works_to_elasticsearch.yaml` — ES sync
- `sync_content_index_to_d1.yaml` — D1 sync
- `vacuum_optimize_tables.yaml` — Table maintenance
- `vector_embeddings.yaml` — Embeddings generator
- `walden_end2end.yaml` — E2E pipeline

**Binding existing jobs:** If a job already exists and needs to be added to DAB, bind it first:
`databricks bundle deployment bind <resource_name> <job_id> --auto-approve`

## Project Structure

- `jobs/` - Databricks job configs (YAML) deployed via DAB
- `notebooks/` - DLT pipelines and transformations
- `notebooks/maintenance/` - One-off fix/maintenance notebooks
- `libraries/dlt_utils/` - Reusable DLT utilities
- `utils/` - Local utilities (including databricks_sql.py)
- `qa/issues/` - QA issue tracking and documentation

## Editing Notebooks

**SQL Warehouse Constraint**: Jobs configured with `warehouse_id` run on a SQL warehouse, which only supports SQL cells. Use `%sql` magic or SQL cell types. For Python/Spark code, use a compute cluster instead (no `warehouse_id`).

Databricks notebooks (`.ipynb`) store cell sources as JSON arrays of strings (one per line). Using the `NotebookEdit` tool replaces the entire cell, which can change the JSON format and create large diffs.

**To minimize diffs when editing notebooks:**

1. **For small changes**: Use `sed` via Bash to make surgical edits to specific lines within the JSON structure, preserving the original format. The `Edit` tool cannot edit `.ipynb` files directly.

2. **For temporary changes** (e.g., one-time fixes run on Databricks): Make the change, run it on Databricks, then restore with `git checkout <file>` - no commit needed.

3. **For permanent changes**: If using `NotebookEdit`, verify the diff with `git diff` before committing to ensure only the intended changes are present.

## Affiliations Dashboard Pipeline

The affiliations dashboard (`raw-affiliation-strings-v2` ES index) is kept in sync by two end2end tasks:

1. **Refresh_RAS_Counts** — SQL notebook (`notebooks/end2end/RefreshRasWorksCounts.ipynb`)
   - Rebuilds `affiliation_string_works_counts` by exploding `authorships.raw_affiliation_strings` from `OpenAlex_works`
   - Rebuilds `affiliation_strings_lookup_with_counts` by joining the MV (which includes curations) with fresh counts
   - Runs on SQL warehouse after `Guardrails`

2. **Sync_RAS_to_Elasticsearch** — Python notebook (`notebooks/elastic/sync_affiliation_strings_to_elastic_v2.py`)
   - Syncs `affiliation_strings_lookup_with_counts` to ES index `raw-affiliation-strings-v2`
   - 8 partitions, 2 threads, chunk_size=500
   - Runs on `es_sync_cluster` after both `Refresh_RAS_Counts` and `Sync_to_Elasticsearch` (avoids concurrent ES writes)

**Key tables:**
- `openalex.institutions.affiliation_string_works_counts` — per-RAS work counts
- `openalex.institutions.affiliation_strings_lookup_with_counts` — RAS + institution IDs + counts (feeds ES)
- `openalex.institutions.raw_affiliation_strings_institutions_mv` — the MV with 3-layer priority (model → override → curation)

## Task Management

Larger tasks and migration projects are tracked in a separate repo: https://github.com/ourresearch/oax-jobs

Structure: `active/<job-name>/` with files:
- `job.yaml` - Status (exploring, in-progress, complete) and created date
- `README.md` - Problem statement, current state, log
- `PLAN.md` - Approach, tasks, completed work
- `EXPLORE.md` - Research and documentation
- `ACCEPTANCE.md` - Pass/fail criteria

Update these files when completing significant work on tracked tasks.