# Databricks notebook source
# MAGIC %md
# MAGIC # Re-crawl Elsevier DOIs stuck on linkinghub (v2 - FIXED)
# MAGIC
# MAGIC **Problem**: ~83K Elsevier DOIs from March-April 2025 resolved to `linkinghub.elsevier.com` instead of the actual `sciencedirect.com` article page due to a Taxicab bug that wasn't following JS redirects.
# MAGIC
# MAGIC **Solution**: Force re-crawl these DOIs. The bug has been fixed, so they should now resolve correctly.
# MAGIC
# MAGIC **v2 Fix**: Uses `BulkHttpClient` with proper connection pooling. The original used `requests.post()` directly which limits to 10 concurrent connections regardless of thread count!
# MAGIC
# MAGIC **Created**: 2026-02-02

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import datetime
from datetime import timezone
import time
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Add lib to path for BulkHttpClient
sys.path.insert(0, '/Workspace/Repos/openalex-walden/notebooks/maintenance')

# COMMAND ----------

# MAGIC %md
# MAGIC ## BulkHttpClient (inline for portability)
# MAGIC
# MAGIC This is also available at `lib/bulk_http_client.py` for reuse.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class BulkHttpConfig:
    """Configuration for bulk HTTP operations."""
    max_workers: int = 120
    pool_connections: int = 120
    pool_maxsize: int = 120
    timeout: int = 60
    retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: tuple = (500, 502, 503, 504)
    progress_interval: int = 1000


class BulkHttpClient:
    """
    High-throughput HTTP client for bulk operations.

    CRITICAL: This class correctly configures connection pooling.
    Using raw `requests.get/post` limits you to 10 concurrent connections!
    """

    def __init__(self, base_url: str, max_workers: int = 120, config: Optional[BulkHttpConfig] = None):
        self.base_url = base_url.rstrip('/')
        self.config = config or BulkHttpConfig(max_workers=max_workers)
        self.config.pool_connections = max(self.config.pool_connections, max_workers)
        self.config.pool_maxsize = max(self.config.pool_maxsize, max_workers)
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=self.config.retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=list(self.config.retry_statuses),
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        adapter = HTTPAdapter(
            pool_connections=self.config.pool_connections,
            pool_maxsize=self.config.pool_maxsize,
            max_retries=retries
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def post_many(
        self,
        endpoint: str,
        payloads: List[dict],
        result_mapper: Callable[[dict, dict], dict],
        error_mapper: Optional[Callable[[dict, Exception], dict]] = None
    ) -> List[dict]:
        results = []
        url = f"{self.base_url}{endpoint}"
        start_time = time.time()
        total = len(payloads)

        print(f"Starting bulk POST to {endpoint}")
        print(f"  Items: {total:,}")
        print(f"  Workers: {self.config.max_workers}")
        print(f"  Pool size: {self.config.pool_maxsize}")

        def process_one(payload: dict) -> dict:
            try:
                # Filter out underscore-prefixed keys (metadata) before sending to API
                api_payload = {k: v for k, v in payload.items() if not k.startswith("_")}
                response = self.session.post(url, json=api_payload, timeout=self.config.timeout)
                response.raise_for_status()
                return result_mapper(payload, response.json())
            except Exception as e:
                if error_mapper:
                    return error_mapper(payload, e)
                return {"error": str(e), "payload": payload}

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_payload = {executor.submit(process_one, p): p for p in payloads}
            completed = 0

            for future in as_completed(future_to_payload):
                completed += 1
                if completed % self.config.progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    remaining = (total - completed) / rate if rate > 0 else 0
                    print(f"Processed {completed:,}/{total:,} ({rate:.1f}/sec, ~{remaining/60:.1f} min remaining)")

                try:
                    results.append(future.result())
                except Exception as e:
                    payload = future_to_payload[future]
                    if error_mapper:
                        results.append(error_mapper(payload, e))

        elapsed = time.time() - start_time
        print(f"\nCompleted {len(results):,} requests in {elapsed:.1f}s ({len(results)/elapsed:.1f}/sec)")
        return results

# COMMAND ----------

# Configuration
TAXICAB_URL = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com"
MAX_WORKERS = 120

client = BulkHttpClient(TAXICAB_URL, max_workers=MAX_WORKERS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load DOIs to Re-crawl

# COMMAND ----------

linkinghub_dois = spark.sql("""
    SELECT
        native_id,
        native_id_namespace,
        url,
        resolved_url,
        created_date,
        processed_date,
        taxicab_id
    FROM openalex.taxicab.taxicab_results
    WHERE resolved_url LIKE '%linkinghub.elsevier%'
      AND processed_date BETWEEN '2025-03-01' AND '2025-05-01'
      AND native_id_namespace = 'doi'
""")

total_count = linkinghub_dois.count()
print(f"Found {total_count:,} DOIs to re-crawl")

# COMMAND ----------

# Convert to list of payloads
print("Converting to pandas...")
dois_pd = linkinghub_dois.toPandas()

payloads = []
for _, row in dois_pd.iterrows():
    native_id = row["native_id"]
    if native_id and "https://doi.org/" in native_id:
        native_id = native_id.replace("https://doi.org/", "")

    payloads.append({
        "url": row["url"],
        "native_id": native_id,
        "native_id_namespace": row["native_id_namespace"],
        # Keep metadata for result processing
        "_created_date": row["created_date"],
        "_old_taxicab_id": row["taxicab_id"]
    })

print(f"Prepared {len(payloads):,} payloads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process DOIs

# COMMAND ----------

def convert_to_datetime(value):
    """Convert various date/time formats to datetime with UTC."""
    if value is None:
        return datetime.datetime.now(timezone.utc)
    if hasattr(value, 'to_pydatetime'):
        dt = value.to_pydatetime()
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    if isinstance(value, datetime.datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time(0, 0, 0, tzinfo=timezone.utc))
    return datetime.datetime.now(timezone.utc)


def result_mapper(payload: dict, response: dict) -> dict:
    """Map successful response to result dict."""
    return {
        "taxicab_id": response.get("id"),
        "url": payload.get("url"),
        "status_code": response.get("status_code"),
        "resolved_url": response.get("resolved_url"),
        "content_type": response.get("content_type"),
        "native_id": response.get("native_id"),
        "native_id_namespace": response.get("native_id_namespace"),
        "s3_path": response.get("s3_path"),
        "is_soft_block": response.get("is_soft_block", False),
        "created_date": convert_to_datetime(payload.get("_created_date")),
        "error": None,
        "_old_taxicab_id": payload.get("_old_taxicab_id")
    }


def error_mapper(payload: dict, exception: Exception) -> dict:
    """Map failed request to result dict."""
    status_code = 0
    if hasattr(exception, 'response') and exception.response is not None:
        status_code = exception.response.status_code

    return {
        "taxicab_id": None,
        "url": payload.get("url"),
        "status_code": status_code,
        "resolved_url": None,
        "content_type": None,
        "native_id": payload.get("native_id"),
        "native_id_namespace": payload.get("native_id_namespace"),
        "s3_path": None,
        "is_soft_block": False,
        "created_date": convert_to_datetime(payload.get("_created_date")),
        "error": str(exception),
        "_old_taxicab_id": payload.get("_old_taxicab_id")
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test with 100 DOIs first

# COMMAND ----------

test_payloads = payloads[:100]
print(f"Testing with {len(test_payloads)} DOIs...")

test_results = client.post_many(
    endpoint="/taxicab",
    payloads=test_payloads,
    result_mapper=result_mapper,
    error_mapper=error_mapper
)

# Check test results
sciencedirect = sum(1 for r in test_results if r.get("resolved_url") and "sciencedirect" in r["resolved_url"])
journal = sum(1 for r in test_results if r.get("resolved_url") and "sciencedirect" not in r.get("resolved_url", "") and "linkinghub" not in r.get("resolved_url", ""))
linkinghub = sum(1 for r in test_results if r.get("resolved_url") and "linkinghub" in r["resolved_url"])
errors = sum(1 for r in test_results if r.get("error"))

print(f"\nTest Results:")
print(f"  sciencedirect.com: {sciencedirect}")
print(f"  journal domains:   {journal}")
print(f"  still linkinghub:  {linkinghub}")
print(f"  errors:            {errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Re-crawl (all DOIs)

# COMMAND ----------

print(f"Starting full re-crawl of {len(payloads):,} DOIs...")
print(f"Expected time at 15 req/sec: ~{len(payloads) / 15 / 60:.0f} minutes")

results = client.post_many(
    endpoint="/taxicab",
    payloads=payloads,
    result_mapper=result_mapper,
    error_mapper=error_mapper
)

# Add processed_date to all results
processed_date = datetime.datetime.now(timezone.utc)
for r in results:
    r["processed_date"] = processed_date

print(f"\nCompleted processing {len(results):,} URLs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

sciencedirect = [r for r in results if r.get("resolved_url") and "sciencedirect" in r["resolved_url"]]
journal_specific = [r for r in results if r.get("resolved_url") and "sciencedirect" not in r.get("resolved_url", "") and "linkinghub" not in r.get("resolved_url", "")]
still_linkinghub = [r for r in results if r.get("resolved_url") and "linkinghub" in r["resolved_url"]]
errors = [r for r in results if r.get("error")]
null_resolved = [r for r in results if not r.get("resolved_url") and not r.get("error")]

print(f"\n{'='*60}")
print(f"RE-CRAWL RESULTS")
print(f"{'='*60}")
print(f"✓ Resolved to sciencedirect.com:    {len(sciencedirect):,} ({100*len(sciencedirect)/len(results):.1f}%)")
print(f"✓ Resolved to journal domains:      {len(journal_specific):,} ({100*len(journal_specific)/len(results):.1f}%)")
print(f"✗ Still linkinghub (no improvement): {len(still_linkinghub):,} ({100*len(still_linkinghub)/len(results):.1f}%)")
print(f"! Errors:                            {len(errors):,} ({100*len(errors)/len(results):.1f}%)")
print(f"? NULL resolved_url:                 {len(null_resolved):,} ({100*len(null_resolved)/len(results):.1f}%)")
print(f"{'='*60}")
print(f"TOTAL SUCCESS: {len(sciencedirect) + len(journal_specific):,} ({100*(len(sciencedirect) + len(journal_specific))/len(results):.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

results_schema = T.StructType([
    T.StructField("taxicab_id", T.StringType(), True),
    T.StructField("url", T.StringType(), True),
    T.StructField("resolved_url", T.StringType(), True),
    T.StructField("status_code", T.IntegerType(), True),
    T.StructField("content_type", T.StringType(), True),
    T.StructField("native_id", T.StringType(), True),
    T.StructField("native_id_namespace", T.StringType(), True),
    T.StructField("s3_path", T.StringType(), True),
    T.StructField("is_soft_block", T.BooleanType(), True),
    T.StructField("created_date", T.TimestampType(), True),
    T.StructField("processed_date", T.TimestampType(), True),
    T.StructField("error", T.StringType(), True)
])

# Remove internal fields before saving
results_for_save = [{k: v for k, v in r.items() if not k.startswith("_")} for r in results]

results_df = spark.createDataFrame(results_for_save, schema=results_schema)
print(f"Created DataFrame with {results_df.count():,} records")

# COMMAND ----------

# Delete old records
old_taxicab_ids = [r.get("_old_taxicab_id") for r in results if r.get("_old_taxicab_id")]
print(f"Deleting {len(old_taxicab_ids):,} old records...")

old_ids_df = spark.createDataFrame([(id,) for id in old_taxicab_ids], ["taxicab_id"])
old_ids_df.createOrReplaceTempView("old_ids_to_delete")

spark.sql("""
    DELETE FROM openalex.taxicab.taxicab_results
    WHERE taxicab_id IN (SELECT taxicab_id FROM old_ids_to_delete)
""")
print("Old records deleted.")

# COMMAND ----------

# Save new results
results_df.write.mode("append").format("delta").saveAsTable("openalex.taxicab.taxicab_results")
print(f"Saved {results_df.count():,} new records to taxicab_results")

# COMMAND ----------

# Verify
remaining = spark.sql("""
    SELECT COUNT(*) as cnt
    FROM openalex.taxicab.taxicab_results
    WHERE resolved_url LIKE '%linkinghub.elsevier%'
      AND processed_date BETWEEN '2025-03-01' AND '2025-05-01'
""").collect()[0]["cnt"]

print(f"Remaining linkinghub DOIs from problem period: {remaining:,}")
