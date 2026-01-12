import os
import re
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
from dotenv import load_dotenv

load_dotenv()


class ReadOnlyViolationError(Exception):
    """Raised when a query attempts to modify data."""
    pass


def _validate_read_only(query: str) -> None:
    """
    Validate that a query is read-only (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH).
    Raises ReadOnlyViolationError if the query could modify data.
    """
    # Normalize: remove comments and extra whitespace
    # Remove -- comments
    cleaned = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    # Remove /* */ comments
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    # Normalize whitespace
    cleaned = ' '.join(cleaned.split()).strip().upper()

    # List of allowed read-only statement prefixes
    read_only_prefixes = (
        'SELECT',
        'WITH',  # CTEs that lead to SELECT
        'SHOW',
        'DESCRIBE',
        'DESC',
        'EXPLAIN',
    )

    # List of forbidden keywords that indicate data modification
    forbidden_patterns = [
        r'\bINSERT\b',
        r'\bUPDATE\b',
        r'\bDELETE\b',
        r'\bDROP\b',
        r'\bCREATE\b',
        r'\bALTER\b',
        r'\bTRUNCATE\b',
        r'\bMERGE\b',
        r'\bREPLACE\b',
        r'\bGRANT\b',
        r'\bREVOKE\b',
        r'\bCOPY\b',
        r'\bUNLOAD\b',
        r'\bVACUUM\b',
        r'\bOPTIMIZE\b',
        r'\bREFRESH\b',
        r'\bMSCK\b',
        r'\bLOAD\b',
        r'\bSET\b(?!\s+)',  # SET without space after (not part of OFFSET)
    ]

    # Check if query starts with allowed prefix
    if not cleaned.startswith(read_only_prefixes):
        raise ReadOnlyViolationError(
            f"Query must start with SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN. "
            f"Got: {cleaned[:50]}..."
        )

    # Check for forbidden keywords anywhere in query
    for pattern in forbidden_patterns:
        if re.search(pattern, cleaned):
            keyword = re.search(pattern, cleaned).group()
            raise ReadOnlyViolationError(
                f"Query contains forbidden keyword '{keyword}'. Only read-only queries are allowed."
            )


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


def run_query(query: str, params: dict = None, allow_writes: bool = False):
    """
    Execute a SQL query and return results as a list of dicts.

    Args:
        query: SQL query string
        params: Optional dict of parameters for parameterized queries
        allow_writes: If False (default), validates query is read-only before executing

    Returns:
        List of dicts, one per row, with column names as keys

    Raises:
        ReadOnlyViolationError: If allow_writes=False and query attempts to modify data
    """
    if not allow_writes:
        _validate_read_only(query)

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