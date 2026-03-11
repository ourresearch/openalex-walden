# Databricks SQL Access Setup

One-time setup for local Databricks SQL access.

## Prerequisites

You need a Databricks service principal with read access to the catalog and permission to use a SQL warehouse. Get the client ID and secret from your admin.

## Setup Steps

1. **Create `.env` file** in project root:
   ```
   DATABRICKS_HOST=https://dbc-ce570f73-0362.cloud.databricks.com
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   DATABRICKS_CLIENT_ID=your-client-id
   DATABRICKS_CLIENT_SECRET=your-client-secret
   ```
   Find the HTTP path in Databricks: SQL Warehouses → your warehouse → Connection details

2. **Create virtual environment and install dependencies**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Test the connection**:
   ```bash
   source .venv/bin/activate
   python -c "from utils.databricks_sql import run_query; print(run_query('SELECT 1'))"
   ```