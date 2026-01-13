# OpenAlex Walden

Data pipeline for OpenAlex research metadata. Main pipeline: `jobs/walden_end2end.yaml`

## Pipeline Overview

The Walden End2End job runs these stages:
1. **Ingest** - DLT pipelines for each source (Crossref, PubMed, DataCite, MAG, Repos, PDF, Landing Page)
2. **Union** - Combine all sources
3. **Transform** - Authorships, locations, parsed names, affiliations
4. **Enrich** - Topics, institutions matching, author matching
5. **Output** - Works enriched, Wunpaywall, Elasticsearch sync

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

## Project Structure

- `jobs/` - Databricks job configs (YAML)
- `notebooks/` - DLT pipelines and transformations
- `libraries/dlt_utils/` - Reusable DLT utilities
- `utils/` - Local utilities (including databricks_sql.py)