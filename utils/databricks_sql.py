import os
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
from dotenv import load_dotenv

load_dotenv()


def _credential_provider():
    """OAuth M2M credential provider for service principal authentication."""
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return oauth_service_principal(config)


def get_connection():
    """
    Create a connection to Databricks SQL warehouse using OAuth service principal.
    Requires these environment variables:
        - DATABRICKS_HOST: e.g., https://dbc-ce570f73-0362.cloud.databricks.com
        - DATABRICKS_HTTP_PATH: e.g., /sql/1.0/warehouses/abc123
        - DATABRICKS_CLIENT_ID: Service principal client ID
        - DATABRICKS_CLIENT_SECRET: Service principal secret
    """
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "").replace("http://", "")
    return sql.connect(
        server_hostname=host,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=_credential_provider,
    )


def run_query(query: str, params: dict = None):
    """
    Execute a SQL query and return results as a list of dicts.

    Args:
        query: SQL query string
        params: Optional dict of parameters for parameterized queries

    Returns:
        List of dicts, one per row, with column names as keys
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def run_query_df(query: str, params: dict = None):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        query: SQL query string
        params: Optional dict of parameters for parameterized queries

    Returns:
        pandas DataFrame with query results
    """
    import pandas as pd
    results = run_query(query, params)
    return pd.DataFrame(results)