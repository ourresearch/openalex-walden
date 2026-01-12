# OpenAlex Walden

Data pipeline and transformation project for the OpenAlex research metadata system.

## Databricks SQL Access

### Setup (one-time)

1. **Get credentials**: You need a Databricks service principal with read access to the catalog and permission to use a SQL warehouse. Get the client ID and secret from your admin.

2. **Create `.env` file** in project root:
   ```
   DATABRICKS_HOST=https://dbc-ce570f73-0362.cloud.databricks.com
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   DATABRICKS_CLIENT_ID=your-client-id
   DATABRICKS_CLIENT_SECRET=your-client-secret
   ```
   Find the HTTP path in Databricks: SQL Warehouses → your warehouse → Connection details

3. **Create virtual environment and install dependencies**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. **Test the connection**:
   ```bash
   source .venv/bin/activate
   python -c "from utils.databricks_sql import run_query; print(run_query('SELECT 1'))"
   ```

### Usage

Always activate the virtual environment first:
```bash
source .venv/bin/activate
```

Query tables using `utils/databricks_sql.py`:
```python
from utils.databricks_sql import run_query, run_query_df

# Returns list of dicts
results = run_query("SELECT * FROM openalex.works.openalex_works LIMIT 10")

# Returns pandas DataFrame
df = run_query_df("SELECT * FROM openalex.works.openalex_works LIMIT 10")
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