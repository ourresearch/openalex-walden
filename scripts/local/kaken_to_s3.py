#!/usr/bin/env python3
"""
KAKEN (JSPS Grants-in-Aid for Scientific Research) to S3 Data Pipeline
=======================================================================

This script downloads all Japanese research grant data from the KAKEN database,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://kaken.nii.ac.jp/
Funder: Japan Society for the Promotion of Science (JSPS)
Output: s3://openalex-ingest/awards/kaken/kaken_projects.parquet

What this script does:
1. Downloads project URLs from KAKEN sitemaps (~1M+ projects)
2. Scrapes project pages to extract metadata (title, PI, dates, amount, etc.)
3. Combines into a single DataFrame
4. Saves as parquet and uploads to S3

Features:
- Two-phase download: URLs first, then content
- Checkpointing: Progress is saved every 1000 projects; resume with --resume
- Rate limiting: Configurable delay to avoid overwhelming the server
- Retry logic: Failed pages are retried up to 3 times
- Ban detection: Detects 403/429/CAPTCHA patterns and backs off (30s to 5min max)
- ETA reporting: Shows estimated time remaining, including ban status

IMPORTANT: This script scrapes ~1M+ web pages. Expect 24-48 hours for full download.
Consider running with --max-projects for testing first.

Requirements:
    pip install pandas pyarrow requests lxml beautifulsoup4

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/kaken/

    For Zyte proxy support, set ZYTE_API_KEY environment variable.

Usage:
    python kaken_to_s3.py

    # Resume interrupted download:
    python kaken_to_s3.py --resume

    # Use Zyte proxy (recommended if getting blocked):
    python kaken_to_s3.py --resume --use-zyte

    # Test with limited projects:
    python kaken_to_s3.py --max-projects 1000 --skip-upload

Author: OpenAlex Team
"""

import argparse
import gzip
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from base64 import standard_b64decode
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# =============================================================================
# Configuration
# =============================================================================

# KAKEN sitemap settings
SITEMAP_INDEX_URL = "https://kaken.nii.ac.jp/sitemaps/kakenhi/sitemapindex.xml"
PROJECT_URL_BASE = "https://kaken.nii.ac.jp/en/grant/"

# Rate limiting - be respectful to the server
REQUEST_DELAY = 0.5  # Seconds between requests PER WORKER (was 0.2)
MAX_WORKERS = 5  # Parallel page fetches (was 10)
MAX_RETRIES = 3  # Max retries per page
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 1000  # Save checkpoint every N projects

# Ban detection thresholds - tuned for slow/flaky sites (not aggressive rate limiters)
BAN_ERROR_THRESHOLD = 25  # Consecutive errors before suspecting ban (was 10)
BAN_ERROR_WINDOW = 100  # Check error rate in last N requests (was 50)
BAN_ERROR_RATE_THRESHOLD = 0.6  # Error rate that triggers ban detection (was 0.5)
BAN_BACKOFF_INITIAL = 30  # Initial backoff when ban detected (seconds) (was 60)
BAN_BACKOFF_MAX = 300  # Max backoff (5 minutes) (was 3600/1 hour)

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kaken/kaken_projects.parquet"

# Zyte API configuration
ZYTE_API_URL = "https://api.zyte.com/v1/extract"
ZYTE_API_KEY = os.getenv("ZYTE_API_KEY", "")

# Global flag for Zyte usage (set by command line)
USE_ZYTE = False


# =============================================================================
# Progress Tracker
# =============================================================================

class ProgressTracker:
    """Track download progress with ETA calculation."""

    def __init__(self, total_items: int):
        self.total_items = total_items
        self.completed_items = 0
        self.errors = 0
        self.start_time = time.time()
        self.last_report_time = time.time()

    def update(self, is_error: bool = False):
        """Update progress counters."""
        self.completed_items += 1
        if is_error:
            self.errors += 1

    def get_eta(self) -> str:
        """Calculate and format ETA."""
        if self.completed_items == 0:
            return "calculating..."

        elapsed = time.time() - self.start_time
        items_per_second = self.completed_items / elapsed
        remaining_items = self.total_items - self.completed_items

        if items_per_second > 0:
            remaining_seconds = remaining_items / items_per_second
            eta = timedelta(seconds=int(remaining_seconds))
            return str(eta)
        return "unknown"

    def get_elapsed(self) -> str:
        """Format elapsed time."""
        elapsed = time.time() - self.start_time
        return str(timedelta(seconds=int(elapsed)))

    def get_rate(self) -> float:
        """Get items per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.completed_items / elapsed
        return 0.0

    def should_report(self, interval: int = 30) -> bool:
        """Check if we should print a progress report (every N seconds)."""
        now = time.time()
        if now - self.last_report_time >= interval:
            self.last_report_time = now
            return True
        return False

    def get_progress_line(self, ban_status: str = "") -> str:
        """Get formatted progress line."""
        pct = (self.completed_items / self.total_items) * 100 if self.total_items > 0 else 0
        rate = self.get_rate()
        line = (
            f"  [{self.completed_items:,}/{self.total_items:,} ({pct:.1f}%)] "
            f"[{rate:.2f}/s] "
            f"[Elapsed: {self.get_elapsed()}] "
            f"[ETA: {self.get_eta()}] "
            f"[Errors: {self.errors}]"
        )
        if ban_status:
            line += f" [{ban_status}]"
        return line


# =============================================================================
# Ban Detection
# =============================================================================

class BanDetector:
    """Detect if we're being banned/rate-limited and manage global backoff."""

    def __init__(self):
        self.recent_results: list[bool] = []  # True = success, False = error
        self.consecutive_errors = 0
        self.ban_detected = False
        self.current_backoff = 0
        self.total_bans_detected = 0
        self.last_ban_time: Optional[float] = None
        self._lock = __import__('threading').Lock()

    def record_result(self, success: bool, error_type: Optional[str] = None):
        """Record a request result."""
        with self._lock:
            self.recent_results.append(success)
            if len(self.recent_results) > BAN_ERROR_WINDOW:
                self.recent_results.pop(0)

            if success:
                self.consecutive_errors = 0
            else:
                self.consecutive_errors += 1

            # Check for ban signals - only explicit HTTP ban codes and captcha
            # (removed "connection_refused" - that's normal for slow/overloaded sites)
            if error_type in ("403", "429", "captcha"):
                self._check_ban_status(is_ban_signal=True)
            elif not success:
                self._check_ban_status(is_ban_signal=False)

    def _check_ban_status(self, is_ban_signal: bool):
        """Check if we should trigger ban detection."""
        # Strong ban signal (403/429/captcha) - require more consecutive errors
        # to avoid false positives from slow/flaky sites (was 3, now 10)
        if is_ban_signal and self.consecutive_errors >= 10:
            self._trigger_ban()
            return

        # Too many consecutive errors
        if self.consecutive_errors >= BAN_ERROR_THRESHOLD:
            self._trigger_ban()
            return

        # High error rate in recent window
        if len(self.recent_results) >= BAN_ERROR_WINDOW:
            error_rate = self.recent_results.count(False) / len(self.recent_results)
            if error_rate >= BAN_ERROR_RATE_THRESHOLD:
                self._trigger_ban()

    def _trigger_ban(self):
        """Trigger ban detection and set backoff."""
        if not self.ban_detected:
            self.ban_detected = True
            self.total_bans_detected += 1
            self.last_ban_time = time.time()
            # Exponential backoff based on number of bans
            self.current_backoff = min(
                BAN_BACKOFF_INITIAL * (2 ** (self.total_bans_detected - 1)),
                BAN_BACKOFF_MAX
            )
            print(f"\n  [BAN DETECTED] Consecutive errors: {self.consecutive_errors}, "
                  f"backing off for {self.current_backoff}s...")

    def wait_if_banned(self) -> bool:
        """Wait if ban is detected. Returns True if we waited."""
        if not self.ban_detected:
            return False

        print(f"  [BAN] Waiting {self.current_backoff}s before resuming...")
        time.sleep(self.current_backoff)

        # Reset state after waiting
        with self._lock:
            self.ban_detected = False
            self.consecutive_errors = 0
            self.recent_results.clear()

        print(f"  [BAN] Resuming after backoff...")
        return True

    def get_status(self) -> str:
        """Get current ban status for display."""
        if self.ban_detected:
            return f"BAN DETECTED - backoff {self.current_backoff}s"
        elif self.total_bans_detected > 0:
            return f"Bans: {self.total_bans_detected}"
        elif self.consecutive_errors >= 3:
            return f"Errors: {self.consecutive_errors} consecutive"
        return ""


# =============================================================================
# Checkpoint Management
# =============================================================================

class CheckpointManager:
    """Manage checkpointing for resumable downloads."""

    def __init__(self, output_dir: Path):
        self.checkpoint_file = output_dir / "kaken_checkpoint.json"
        self.urls_file = output_dir / "kaken_urls.json"
        self.projects_file = output_dir / "kaken_projects_partial.json"
        self.data = {
            "phase": "urls",  # "urls" or "content"
            "completed_urls": [],
            "failed_urls": [],
            "total_urls": 0,
            "last_updated": None,
        }
        self.urls = []
        self.projects = []

    def load(self) -> bool:
        """Load checkpoint from disk. Returns True if checkpoint exists."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    self.data = json.load(f)
                print(f"  [CHECKPOINT] Loaded: phase={self.data['phase']}, "
                      f"{len(self.data['completed_urls']):,} URLs completed")

                # Load URLs if available
                if self.urls_file.exists():
                    with open(self.urls_file, "r") as f:
                        self.urls = json.load(f)
                    print(f"  [CHECKPOINT] Loaded {len(self.urls):,} project URLs")

                # Load partial projects
                if self.projects_file.exists():
                    with open(self.projects_file, "r") as f:
                        self.projects = json.load(f)
                    print(f"  [CHECKPOINT] Loaded {len(self.projects):,} projects")

                return True
            except Exception as e:
                print(f"  [WARN] Failed to load checkpoint: {e}")
        return False

    def save(self):
        """Save checkpoint to disk."""
        self.data["last_updated"] = datetime.utcnow().isoformat()

        # Save checkpoint metadata
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

        # Save URLs if we have them
        if self.urls:
            with open(self.urls_file, "w") as f:
                json.dump(self.urls, f)

        # Save projects separately (can be large)
        if self.projects:
            with open(self.projects_file, "w") as f:
                json.dump(self.projects, f)

    def set_phase(self, phase: str):
        """Set current phase."""
        self.data["phase"] = phase

    def mark_completed(self, url: str):
        """Mark a URL as completed."""
        if url not in self.data["completed_urls"]:
            self.data["completed_urls"].append(url)

    def mark_failed(self, url: str):
        """Mark a URL as failed."""
        if url not in self.data["failed_urls"]:
            self.data["failed_urls"].append(url)

    def add_project(self, project: dict):
        """Add a project to the partial save."""
        self.projects.append(project)

    def get_remaining_urls(self) -> list[str]:
        """Get list of URLs that still need to be downloaded."""
        completed = set(self.data["completed_urls"])
        return [u for u in self.urls if u not in completed]

    def cleanup(self):
        """Remove checkpoint files after successful completion."""
        for f in [self.checkpoint_file, self.urls_file, self.projects_file]:
            if f.exists():
                f.unlink()
        print("  [CHECKPOINT] Cleaned up checkpoint files")


# =============================================================================
# Sitemap Functions
# =============================================================================

def download_sitemaps(session: requests.Session) -> list[str]:
    """
    Download all project URLs from KAKEN sitemaps.

    Returns:
        List of project URLs
    """
    print(f"\n{'='*60}")
    print("Phase 1: Downloading project URLs from sitemaps")
    print(f"{'='*60}")

    urls = []

    # Get sitemap index
    print("  [INFO] Fetching sitemap index...")
    response = session.get(SITEMAP_INDEX_URL, timeout=60)
    response.raise_for_status()

    # Parse sitemap index
    root = ET.fromstring(response.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemap_urls = [loc.text for loc in root.findall(".//sm:loc", ns)]

    print(f"  [INFO] Found {len(sitemap_urls)} sitemap files")

    # Download each sitemap
    for i, sitemap_url in enumerate(sitemap_urls):
        print(f"  [{i+1}/{len(sitemap_urls)}] Downloading {sitemap_url.split('/')[-1]}...")

        try:
            resp = session.get(sitemap_url, timeout=60)
            resp.raise_for_status()

            # Parse sitemap
            sitemap_root = ET.fromstring(resp.content)
            for loc in sitemap_root.findall(".//sm:loc", ns):
                url = loc.text
                # Only include PROJECT URLs (not AREA)
                if "KAKENHI-PROJECT" in url:
                    urls.append(url)

            time.sleep(0.2)  # Small delay between sitemaps

        except Exception as e:
            print(f"    [WARN] Failed to download sitemap: {e}")

    print(f"\n  [INFO] Total project URLs: {len(urls):,}")
    return urls


# =============================================================================
# Page Scraping Functions
# =============================================================================

def fetch_via_zyte(url: str, timeout: int = 60) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Fetch a URL via the Zyte API.

    Args:
        url: URL to fetch
        timeout: Request timeout in seconds

    Returns:
        Tuple of (html content or None, error message or None, error_type or None)
    """
    if not ZYTE_API_KEY:
        return None, "ZYTE_API_KEY not set", "config_error"

    zyte_params = {
        "url": url,
        "httpResponseBody": True,
        "httpResponseHeaders": True,
    }

    try:
        response = requests.post(
            ZYTE_API_URL,
            auth=(ZYTE_API_KEY, ""),
            json=zyte_params,
            timeout=timeout
        )
        response.raise_for_status()
        data = response.json()

        # Decode the base64 response body
        status_code = data.get("statusCode", 200)
        if status_code == 404:
            return None, None, None  # Page doesn't exist, not an error

        if status_code >= 400:
            return None, f"HTTP {status_code} via Zyte", str(status_code)

        body_b64 = data.get("httpResponseBody", "")
        if body_b64:
            html = standard_b64decode(body_b64).decode("utf-8", errors="replace")
            return html, None, None
        else:
            return None, "Empty response from Zyte", "empty_response"

    except requests.exceptions.Timeout:
        return None, "Zyte API timeout", "timeout"
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            return None, "Zyte rate limited", "429"
        elif response.status_code == 401:
            return None, "Zyte API key invalid", "auth_error"
        else:
            return None, f"Zyte HTTP {response.status_code}", "zyte_error"
    except Exception as e:
        return None, f"Zyte error: {type(e).__name__}: {str(e)}", "zyte_error"


def fetch_project_with_retry(
    url: str,
    max_retries: int = MAX_RETRIES
) -> tuple[str, Optional[dict], Optional[str], Optional[str]]:
    """
    Fetch and parse a single project page with retry logic.

    Args:
        url: Project page URL
        max_retries: Maximum retry attempts

    Returns:
        Tuple of (original_url, project dict or None, error message or None, error_type or None)
        error_type is one of: "403", "429", "connection_refused", "captcha", "timeout", "server_error", None
    """
    original_url = url  # Keep original for checkpoint tracking
    last_error = None
    error_type = None

    # Rate limit at the start of each request (per-worker throttling)
    time.sleep(REQUEST_DELAY)

    # Use English version of the page
    fetch_url = url
    if "/ja/" in fetch_url:
        fetch_url = fetch_url.replace("/ja/", "/en/")
    elif "/en/" not in fetch_url:
        fetch_url = fetch_url.replace("/grant/", "/en/grant/")

    for attempt in range(max_retries):
        try:
            # Fetch via Zyte or direct
            if USE_ZYTE:
                html, error, err_type = fetch_via_zyte(fetch_url)
                if error:
                    last_error = error
                    error_type = err_type
                    if attempt < max_retries - 1:
                        time.sleep(RETRY_BACKOFF ** attempt)
                    continue
                if html is None and error is None:
                    # 404 - page doesn't exist
                    return (original_url, None, None, None)
            else:
                headers = {
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": "OpenAlex-KAKEN-Ingest/1.0 (research data aggregator; contact@openalex.org)",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                response = requests.get(fetch_url, headers=headers, timeout=30)
                response.raise_for_status()
                html = response.text

            # Check for CAPTCHA or block page
            content_lower = html.lower()
            if any(signal in content_lower for signal in [
                "captcha", "robot", "blocked", "access denied",
                "too many requests", "rate limit", "please verify"
            ]):
                last_error = "CAPTCHA or block page detected"
                error_type = "captcha"
                if attempt < max_retries - 1:
                    time.sleep(RETRY_BACKOFF ** (attempt + 2))
                continue

            project = parse_project_page(html, fetch_url)
            return (original_url, project, None, None)

        except requests.exceptions.Timeout:
            last_error = f"Timeout (attempt {attempt + 1}/{max_retries})"
            error_type = "timeout"
        except requests.exceptions.ConnectionError as e:
            error_str = str(e).lower()
            if "refused" in error_str or "reset" in error_str:
                last_error = f"Connection refused (attempt {attempt + 1}/{max_retries})"
                error_type = "connection_refused"
            else:
                last_error = f"Connection error (attempt {attempt + 1}/{max_retries})"
                error_type = "connection_error"
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Rate limited
                wait_time = RETRY_BACKOFF ** (attempt + 2)
                last_error = f"Rate limited (429), waiting {wait_time:.1f}s"
                error_type = "429"
                time.sleep(wait_time)
            elif response.status_code == 403:  # Forbidden - likely banned
                last_error = f"Forbidden (403) - possible ban"
                error_type = "403"
            elif response.status_code == 404:
                # Project doesn't exist, skip
                return (original_url, None, None, None)
            elif response.status_code >= 500:
                last_error = f"Server error {response.status_code}"
                error_type = "server_error"
            else:
                return (original_url, None, f"HTTP {response.status_code}", None)
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"
            error_type = "other"

        # Wait before retry
        if attempt < max_retries - 1:
            time.sleep(RETRY_BACKOFF ** attempt)

    return (original_url, None, last_error, error_type)


def parse_project_page(html: str, url: str) -> Optional[dict]:
    """
    Parse a KAKEN project page to extract grant data.

    Args:
        html: Raw HTML content
        url: Page URL (for extracting project ID)

    Returns:
        Dictionary with project fields
    """
    soup = BeautifulSoup(html, "lxml")

    try:
        # Extract project ID from URL
        # URL format: https://kaken.nii.ac.jp/en/grant/KAKENHI-PROJECT-20H00001/
        project_id = url.rstrip("/").split("-")[-1]

        # Helper to find table header by partial text match
        def find_th_containing(text: str):
            """Find th element containing the given text (handles nested elements)."""
            for th in soup.find_all("th"):
                if text.lower() in th.get_text().lower():
                    return th
            return None

        # Helper to get table cell value
        def get_table_value(header_text: str) -> Optional[str]:
            th = find_th_containing(header_text)
            if th:
                td = th.find_next_sibling("td")
                if td:
                    return td.get_text(strip=True)
            return None

        # Title from h1
        title_elem = soup.find("h1")
        title = title_elem.get_text(strip=True) if title_elem else None

        # Research category
        category = get_table_value("Research Category")

        # Project period (dates)
        period_text = get_table_value("Project Period")
        start_date = None
        end_date = None
        if period_text:
            # Format: "2020-04-01 – 2023-03-31" or "2019"
            dates = re.findall(r"(\d{4}-\d{2}-\d{2}|\d{4})", period_text)
            if len(dates) >= 2:
                start_date = dates[0]
                end_date = dates[1]
            elif len(dates) == 1:
                # Single year project
                start_date = f"{dates[0]}-04-01"
                end_date = f"{dates[0]}-03-31"

        # Budget amount - look for th containing "Budget Amount"
        amount = None
        currency = "JPY"
        budget_th = find_th_containing("Budget Amount")
        if budget_th:
            budget_td = budget_th.find_next_sibling("td")
            if budget_td:
                # Get the h5 element which contains the total amount
                h5 = budget_td.find("h5")
                if h5:
                    budget_text = h5.get_text(strip=True)
                else:
                    budget_text = budget_td.get_text(strip=True)
                # Format: "¥42,380,000 (Direct Cost: ...)"
                amount_match = re.search(r"[¥￥]([\d,]+)", budget_text)
                if amount_match:
                    amount = float(amount_match.group(1).replace(",", ""))

        # Principal Investigator
        pi_given_name = None
        pi_family_name = None
        pi_affiliation = None
        pi_nrid = None

        # Find PI section - look for th containing "Principal Investigator"
        pi_th = find_th_containing("Principal Investigator")
        if pi_th:
            pi_td = pi_th.find_next_sibling("td")
            if pi_td:
                # Get PI name from link to nrid.nii.ac.jp
                pi_link = pi_td.find("a", href=lambda h: h and "nrid.nii.ac.jp" in h if h else False)
                if pi_link:
                    pi_name = pi_link.get_text(strip=True)
                    # Try to split name (format is usually "FAMILY Given" or "Family Given")
                    parts = pi_name.split()
                    if len(parts) >= 2:
                        pi_family_name = parts[0]
                        pi_given_name = " ".join(parts[1:])
                    else:
                        pi_family_name = pi_name

                    # Extract NRID from URL
                    href = pi_link.get("href", "")
                    nrid_match = re.search(r"(\d{10,})", href)
                    if nrid_match:
                        pi_nrid = nrid_match.group(1)

                # Get affiliation from h4 text after the link
                h4 = pi_td.find("h4")
                if h4:
                    h4_text = h4.get_text(strip=True)
                    # Remove PI name and extract affiliation
                    if pi_link:
                        h4_text = h4_text.replace(pi_link.get_text(strip=True), "")
                    # Clean up: format is usually "University, Department, Title (NRID)"
                    h4_text = re.sub(r"\(\d+\)$", "", h4_text)  # Remove NRID at end
                    h4_text = h4_text.strip().lstrip(",、 \u00a0").rstrip(",、 ")
                    if h4_text:
                        pi_affiliation = h4_text

        # Research institution
        institution = get_table_value("Research Institution")

        # Keywords
        keywords = get_table_value("Keywords")

        # Abstract (Outline of Research at the Start or Final Research Achievements)
        abstract = None
        for header in ["Outline of Research at the Start", "Outline of Final Research Achievements",
                       "Research Abstract"]:
            abstract = get_table_value(header)
            if abstract:
                break

        # Extract meta description as backup
        if not abstract:
            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc:
                abstract = meta_desc.get("content")

        return {
            "project_id": project_id,
            "title": title,
            "abstract": abstract,
            "category": category,
            "start_date": start_date,
            "end_date": end_date,
            "amount": amount,
            "currency": currency,
            "pi_given_name": pi_given_name,
            "pi_family_name": pi_family_name,
            "pi_affiliation": pi_affiliation,
            "pi_nrid": pi_nrid,
            "institution": institution,
            "keywords": keywords,
            "landing_page_url": url,
        }

    except Exception as e:
        print(f"    [WARN] Parse error for {url}: {e}")
        return None


# =============================================================================
# Download Functions
# =============================================================================

def download_all_projects(
    output_dir: Path,
    resume: bool = False,
    max_projects: Optional[int] = None
) -> list[dict]:
    """
    Download all projects from KAKEN with checkpointing.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_projects: Optional limit on projects (for testing)

    Returns:
        List of all project dictionaries
    """
    # Initialize checkpoint manager
    checkpoint = CheckpointManager(output_dir)

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "OpenAlex-KAKEN-Ingest/1.0 (research data aggregator; contact@openalex.org)",
        "Accept-Language": "en-US,en;q=0.9",
    })

    # Check for existing checkpoint
    if resume and checkpoint.load():
        urls = checkpoint.urls
        if checkpoint.data["phase"] == "urls" and not urls:
            # Need to download URLs still
            urls = download_sitemaps(session)
            checkpoint.urls = urls
            checkpoint.data["total_urls"] = len(urls)
            checkpoint.set_phase("content")
            checkpoint.save()
    else:
        # Start fresh
        urls = download_sitemaps(session)
        checkpoint.urls = urls
        checkpoint.data["total_urls"] = len(urls)
        checkpoint.set_phase("content")
        checkpoint.save()

    # Limit projects if requested
    if max_projects:
        checkpoint.urls = checkpoint.urls[:max_projects]
        print(f"  [INFO] Limited to {max_projects:,} projects")

    # Get remaining URLs to process
    urls_to_fetch = checkpoint.get_remaining_urls()
    print(f"\n{'='*60}")
    print("Phase 2: Downloading project details")
    print(f"{'='*60}")
    print(f"  [INFO] Total URLs: {len(checkpoint.urls):,}")
    print(f"  [INFO] Already completed: {len(checkpoint.data['completed_urls']):,}")
    print(f"  [INFO] Remaining: {len(urls_to_fetch):,}")

    if not urls_to_fetch:
        print("  [INFO] All URLs already downloaded!")
        return checkpoint.projects

    # Initialize progress tracker and ban detector
    progress = ProgressTracker(len(urls_to_fetch))
    ban_detector = BanDetector()

    # Track progress for checkpointing
    items_since_checkpoint = 0
    failed_urls = []

    print(f"\n  Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {progress.get_progress_line()}")

    # Download with controlled concurrency
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Process in batches
        batch_size = 500

        for batch_start in range(0, len(urls_to_fetch), batch_size):
            batch_urls = urls_to_fetch[batch_start:batch_start + batch_size]

            # Submit batch
            futures = {
                executor.submit(fetch_project_with_retry, url): url
                for url in batch_urls
            }

            # Process results
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result_url, project, error, error_type = future.result()

                    if error:
                        progress.update(is_error=True)
                        failed_urls.append((result_url, error))
                        checkpoint.mark_failed(result_url)
                        ban_detector.record_result(success=False, error_type=error_type)
                    elif project:
                        progress.update()
                        checkpoint.mark_completed(result_url)
                        checkpoint.add_project(project)
                        ban_detector.record_result(success=True)
                    else:
                        # Project page was empty or 404
                        progress.update()
                        checkpoint.mark_completed(result_url)
                        ban_detector.record_result(success=True)

                    items_since_checkpoint += 1

                    # Print progress periodically
                    if progress.should_report(30):
                        ban_status = ban_detector.get_status()
                        print(f"\r{progress.get_progress_line(ban_status)}", flush=True)

                    # Save checkpoint periodically
                    if items_since_checkpoint >= CHECKPOINT_INTERVAL:
                        checkpoint.save()
                        items_since_checkpoint = 0
                        print(f"\n  [CHECKPOINT] Saved: {progress.completed_items:,} items, "
                              f"{len(checkpoint.projects):,} projects")

                except Exception as e:
                    progress.update(is_error=True)
                    failed_urls.append((url, str(e)))
                    checkpoint.mark_failed(url)
                    ban_detector.record_result(success=False, error_type="other")

            # Check for ban and wait if needed before next batch
            if ban_detector.wait_if_banned():
                checkpoint.save()
                print(f"  [INFO] Saved checkpoint before resuming")

            # Save checkpoint after each batch
            checkpoint.save()

    # Final report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total processed: {progress.completed_items:,}")
    print(f"  Projects extracted: {len(checkpoint.projects):,}")
    print(f"  Total time: {progress.get_elapsed()}")
    if ban_detector.total_bans_detected > 0:
        print(f"  Ban events detected: {ban_detector.total_bans_detected}")

    if failed_urls:
        print(f"\n  [WARN] {len(failed_urls)} URLs failed:")
        for url, error in failed_urls[:10]:
            print(f"    - {url}: {error}")
        if len(failed_urls) > 10:
            print(f"    ... and {len(failed_urls) - 10} more")

        # Save failed URLs
        failed_file = output_dir / "kaken_failed_urls.json"
        with open(failed_file, "w") as f:
            json.dump(failed_urls, f, indent=2)

    return checkpoint.projects


# =============================================================================
# Processing Functions
# =============================================================================

def process_projects(projects: list[dict], output_dir: Path) -> Path:
    """
    Process projects into a parquet file.

    Args:
        projects: List of project dictionaries
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Phase 3: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    # Convert to DataFrame
    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    # Clean and validate dates
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            # Ensure dates are in YYYY-MM-DD format
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    # Ensure string columns are properly typed
    string_cols = ["project_id", "title", "abstract", "category", "currency",
                   "pi_given_name", "pi_family_name", "pi_affiliation", "pi_nrid",
                   "institution", "keywords", "landing_page_url"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("None", None).replace("nan", None)

    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path = output_dir / "kaken_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Define schema
    schema = pa.schema([
        ("project_id", pa.string()),
        ("title", pa.string()),
        ("abstract", pa.string()),
        ("category", pa.string()),
        ("start_date", pa.string()),
        ("end_date", pa.string()),
        ("amount", pa.float64()),
        ("currency", pa.string()),
        ("pi_given_name", pa.string()),
        ("pi_family_name", pa.string()),
        ("pi_affiliation", pa.string()),
        ("pi_nrid", pa.string()),
        ("institution", pa.string()),
        ("keywords", pa.string()),
        ("landing_page_url", pa.string()),
        ("ingested_at", pa.string()),
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")

    if "category" in df.columns:
        print(f"\n  Categories:")
        print(df["category"].value_counts().head(10).to_string())

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def find_aws_cli() -> Optional[str]:
    """Find AWS CLI executable path."""
    import shutil

    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path

    common_paths = [
        Path.home() / "Library/Python/3.9/bin/aws",
        Path.home() / "Library/Python/3.10/bin/aws",
        Path.home() / "Library/Python/3.11/bin/aws",
        Path.home() / "Library/Python/3.12/bin/aws",
        Path("/usr/local/bin/aws"),
        Path("/opt/homebrew/bin/aws"),
    ]

    for path in common_paths:
        if path.exists():
            return str(path)

    return None


def upload_to_s3(local_path: Path) -> bool:
    """Upload the parquet file to S3."""
    print(f"\n{'='*60}")
    print("Phase 4: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    try:
        result = subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print("  [SUCCESS] Upload complete!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download KAKEN (JSPS) grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./kaken_data"),
        help="Directory for downloaded/processed files (default: ./kaken_data)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing data)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--max-projects",
        type=int,
        default=None,
        help="Limit to N projects (for testing)"
    )
    parser.add_argument(
        "--use-zyte",
        action="store_true",
        help="Use Zyte API proxy for requests (requires ZYTE_API_KEY env var)"
    )
    args = parser.parse_args()

    # Set global Zyte flag
    global USE_ZYTE
    USE_ZYTE = args.use_zyte

    # Validate Zyte configuration
    if USE_ZYTE and not ZYTE_API_KEY:
        print("[ERROR] --use-zyte requires ZYTE_API_KEY environment variable")
        sys.exit(1)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("KAKEN (JSPS Grants-in-Aid) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    if USE_ZYTE:
        print(f"Proxy: Zyte API (key: {ZYTE_API_KEY[:8]}...)")
    if args.resume:
        print(f"Mode: RESUME (will continue from checkpoint)")
    if args.max_projects:
        print(f"Limit: {args.max_projects:,} projects")

    # Download
    if not args.skip_download:
        projects = download_all_projects(
            args.output_dir,
            resume=args.resume,
            max_projects=args.max_projects
        )

        if projects:
            checkpoint = CheckpointManager(args.output_dir)
            checkpoint.cleanup()
    else:
        existing_path = args.output_dir / "kaken_projects.parquet"
        if existing_path.exists():
            print(f"\n  [SKIP] Loading existing data from {existing_path}")
            df = pd.read_parquet(existing_path)
            projects = df.to_dict("records")
        else:
            print("[ERROR] No existing data found. Run without --skip-download")
            sys.exit(1)

    if not projects:
        print("[ERROR] No projects downloaded!")
        sys.exit(1)

    # Process
    parquet_path = process_projects(projects, args.output_dir)

    # Upload
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    if upload_success or args.skip_upload:
        print("Pipeline complete!")
    else:
        print("Pipeline FAILED - S3 upload unsuccessful")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateKAKENAwards.ipynb")


if __name__ == "__main__":
    main()
