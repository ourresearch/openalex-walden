# OpenAlex Walden

Data pipeline and transformation project for the OpenAlex research metadata system.

## Databricks SQL Access

You can query Databricks tables directly using the utility in `utils/databricks_sql.py`:

```python
from utils.databricks_sql import run_query, run_query_df

# Returns list of dicts
results = run_query("SELECT * FROM openalex.works.openalex_works LIMIT 10")

# Returns pandas DataFrame
df = run_query_df("SELECT * FROM openalex.works.openalex_works LIMIT 10")
```

Always activate the virtual environment first:
```bash
source .venv/bin/activate
```

Key catalogs/schemas available:
- `openalex.works` - scholarly works data (e.g., `openalex_works`)
- `openalex.default` - default schema

## Project Structure

- `notebooks/` - Databricks notebooks (DLT pipelines, transformations)
- `libraries/dlt_utils/` - Reusable DLT utilities library
- `utils/` - Local utility modules
- `scripts/local/` - Local data fetching scripts
- `jobs/` - Databricks job configuration (YAML)