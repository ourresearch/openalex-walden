"""
Bulk HTTP Client for Taxicab/Parseland Jobs

Use this module for any job that makes many HTTP requests to internal services.
It handles connection pooling, retries, and progress reporting correctly.

Usage:
    from lib.bulk_http_client import BulkHttpClient

    client = BulkHttpClient(
        base_url="http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com",
        max_workers=120
    )

    results = client.post_many(
        endpoint="/taxicab",
        payloads=[{"url": "...", "native_id": "..."}, ...],
        result_mapper=lambda payload, response: {...}
    )
"""

import time
import datetime
from datetime import timezone
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
    progress_interval: int = 1000  # Print progress every N items


class BulkHttpClient:
    """
    High-throughput HTTP client for bulk operations.

    IMPORTANT: This class correctly configures connection pooling to allow
    max_workers concurrent requests. Using raw `requests.get/post` limits
    you to 10 concurrent connections per host!
    """

    def __init__(
        self,
        base_url: str,
        max_workers: int = 120,
        config: Optional[BulkHttpConfig] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.config = config or BulkHttpConfig(max_workers=max_workers)

        # Ensure pool size matches workers
        self.config.pool_connections = max(self.config.pool_connections, max_workers)
        self.config.pool_maxsize = max(self.config.pool_maxsize, max_workers)

        self.session = self._create_session()
        self._validate_config()

    def _create_session(self) -> requests.Session:
        """Create a properly configured session with connection pooling."""
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

    def _validate_config(self):
        """Warn if configuration looks wrong."""
        if self.config.max_workers > self.config.pool_maxsize:
            print(f"WARNING: max_workers ({self.config.max_workers}) > pool_maxsize ({self.config.pool_maxsize})")
            print("This will limit actual concurrency. Adjusting pool_maxsize.")
            self.config.pool_maxsize = self.config.max_workers
            self.session = self._create_session()

    def post(self, endpoint: str, payload: dict, timeout: Optional[int] = None) -> requests.Response:
        """Make a single POST request using the pooled session."""
        url = f"{self.base_url}{endpoint}"
        return self.session.post(url, json=payload, timeout=timeout or self.config.timeout)

    def get(self, endpoint: str, params: Optional[dict] = None, timeout: Optional[int] = None) -> requests.Response:
        """Make a single GET request using the pooled session."""
        url = f"{self.base_url}{endpoint}"
        return self.session.get(url, params=params, timeout=timeout or self.config.timeout)

    def post_many(
        self,
        endpoint: str,
        payloads: List[dict],
        result_mapper: Callable[[dict, dict], dict],
        error_mapper: Optional[Callable[[dict, Exception], dict]] = None
    ) -> List[dict]:
        """
        Make many POST requests in parallel with proper connection pooling.

        Args:
            endpoint: API endpoint (e.g., "/taxicab")
            payloads: List of request payloads
            result_mapper: Function(payload, response_json) -> result dict
            error_mapper: Optional function(payload, exception) -> result dict

        Returns:
            List of results from result_mapper/error_mapper
        """
        results = []
        url = f"{self.base_url}{endpoint}"
        start_time = time.time()
        total = len(payloads)

        print(f"Starting bulk POST to {endpoint}")
        print(f"  Items: {total:,}")
        print(f"  Workers: {self.config.max_workers}")
        print(f"  Pool size: {self.config.pool_maxsize}")
        print(f"  Timeout: {self.config.timeout}s")

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
                else:
                    return {"error": str(e), "payload": payload}

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_payload = {executor.submit(process_one, p): p for p in payloads}

            completed = 0
            last_checkpoint_time = start_time
            last_checkpoint_count = 0

            for future in as_completed(future_to_payload):
                completed += 1

                if completed % self.config.progress_interval == 0:
                    now = time.time()
                    elapsed = now - start_time
                    cumulative_rate = completed / elapsed

                    # Rolling rate since last checkpoint (more accurate for current speed)
                    interval_elapsed = now - last_checkpoint_time
                    interval_count = completed - last_checkpoint_count
                    rolling_rate = interval_count / interval_elapsed if interval_elapsed > 0 else cumulative_rate

                    # ETA based on rolling rate (more responsive to speedups/slowdowns)
                    remaining = (total - completed) / rolling_rate if rolling_rate > 0 else 0

                    print(f"Processed {completed:,}/{total:,} (recent: {rolling_rate:.1f}/sec, avg: {cumulative_rate:.1f}/sec, ~{remaining/60:.1f} min remaining)")

                    last_checkpoint_time = now
                    last_checkpoint_count = completed

                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    payload = future_to_payload[future]
                    print(f"Unexpected error for {payload}: {e}")
                    if error_mapper:
                        results.append(error_mapper(payload, e))

        elapsed = time.time() - start_time
        final_rate = len(results) / elapsed if elapsed > 0 else 0
        print(f"\nCompleted {len(results):,} requests in {elapsed:.1f}s ({final_rate:.1f}/sec)")

        return results


# Convenience instances for common services
TAXICAB_URL = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com"
PARSELAND_URL = "https://parseland.herokuapp.com"  # Update if different


def create_taxicab_client(max_workers: int = 120) -> BulkHttpClient:
    """Create a properly configured client for Taxicab bulk operations."""
    return BulkHttpClient(TAXICAB_URL, max_workers=max_workers)


def create_parseland_client(max_workers: int = 120) -> BulkHttpClient:
    """Create a properly configured client for Parseland bulk operations."""
    return BulkHttpClient(PARSELAND_URL, max_workers=max_workers)
