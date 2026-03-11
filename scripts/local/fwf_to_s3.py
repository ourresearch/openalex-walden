#!/usr/bin/env python3
"""
FWF (Austrian Science Fund) to S3 Data Pipeline
================================================

This script downloads all FWF research grant data from the OpenAIRE API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: OpenAIRE API (https://api.openaire.eu)
- 19,600+ FWF projects available
- Projects from 1995 onwards
- Includes project codes, titles, dates, funding amounts, DOIs

Output: s3://openalex-ingest/awards/fwf/fwf_projects.parquet

What this script does:
1. Fetches all FWF projects from the OpenAIRE API (paginated JSON)
2. Parses JSON responses to extract project metadata
3. Extracts grant reference, funder, amount, dates, keywords
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved every 50 pages; resume with --resume
- Retry logic: Failed pages are retried up to 3 times with exponential backoff
- ETA reporting: Shows estimated time remaining based on current progress
- Error tracking: Failed pages are logged and can be retried manually

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/fwf/

Usage:
    python fwf_to_s3.py

    # Resume interrupted download:
    python fwf_to_s3.py --resume

    # Or with options:
    python fwf_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# OpenAIRE API settings
OPENAIRE_API_BASE = "https://api.openaire.eu/search/projects"
PAGE_SIZE = 100  # Results per page (max 100 for OpenAIRE)
REQUEST_DELAY = 0.5  # Seconds between requests (be polite to API)
MAX_RETRIES = 3  # Max retries per page
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 50  # Save checkpoint every N pages

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fwf/fwf_projects.parquet"


# =============================================================================
# Progress Tracker
# =============================================================================

class ProgressTracker:
    """Track download progress with ETA calculation."""

    def __init__(self, total_pages: int):
        self.total_pages = total_pages
        self.completed_pages = 0
        self.total_projects = 0
        self.errors = 0
        self.start_time = time.time()
        self.last_report_time = time.time()

    def update(self, projects_count: int, is_error: bool = False):
        """Update progress counters."""
        self.completed_pages += 1
        self.total_projects += projects_count
        if is_error:
            self.errors += 1

    def get_eta(self) -> str:
        """Calculate and format ETA."""
        if self.completed_pages == 0:
            return "calculating..."

        elapsed = time.time() - self.start_time
        rate = self.completed_pages / elapsed
        remaining_pages = self.total_pages - self.completed_pages

        if rate > 0:
            remaining_seconds = remaining_pages / rate
            eta = timedelta(seconds=int(remaining_seconds))
            return str(eta)
        return "unknown"

    def should_report(self, interval: float = 5.0) -> bool:
        """Check if we should report progress (every interval seconds)."""
        if time.time() - self.last_report_time >= interval:
            self.last_report_time = time.time()
            return True
        return False

    def report(self):
        """Print progress report."""
        pct = (self.completed_pages / self.total_pages) * 100 if self.total_pages > 0 else 0
        elapsed = timedelta(seconds=int(time.time() - self.start_time))
        print(f"[{elapsed}] Page {self.completed_pages}/{self.total_pages} "
              f"({pct:.1f}%) - {self.total_projects:,} projects - "
              f"ETA: {self.get_eta()} - Errors: {self.errors}")


# =============================================================================
# API Functions
# =============================================================================

def fetch_page(page: int, session: requests.Session) -> tuple[list[dict], bool]:
    """Fetch a single page of FWF projects from OpenAIRE API."""
    params = {
        "funder": "FWF",
        "format": "json",
        "size": PAGE_SIZE,
        "page": page
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(OPENAIRE_API_BASE, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data.get("response", {}).get("results", {}).get("result", [])

            if not results:
                return [], False

            projects = []
            for result in results:
                try:
                    project = result.get("metadata", {}).get("oaf:entity", {}).get("oaf:project", {})
                    if project:
                        projects.append(extract_project(project))
                except Exception as e:
                    print(f"  Warning: Failed to parse project: {e}")
                    continue

            return projects, False

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** attempt
                print(f"  Retry {attempt + 1}/{MAX_RETRIES} for page {page} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  ERROR: Failed to fetch page {page} after {MAX_RETRIES} attempts: {e}")
                return [], True

    return [], True


def extract_project(proj: dict) -> dict:
    """Extract relevant fields from OpenAIRE project data."""
    def get_value(d, default=None):
        """Get value from OpenAIRE nested dict format."""
        if d is None:
            return default
        if isinstance(d, dict):
            return d.get("$", default)
        return d

    # Extract funding program from fundingtree
    funding_program = None
    fundingtree = proj.get("fundingtree", [])
    if fundingtree and isinstance(fundingtree, list) and len(fundingtree) > 0:
        fl0 = fundingtree[0].get("funding_level_0", {})
        funding_program = get_value(fl0.get("name"))

    # Extract DOI from pid
    doi = None
    pid = proj.get("pid")
    if pid and isinstance(pid, dict):
        if pid.get("@classid") == "doi":
            doi = get_value(pid)

    # Handle funded_amount - can be dict with "$" or direct value, and may be string or number
    funded_amount = None
    fa_raw = proj.get("fundedamount")
    if fa_raw is not None:
        if isinstance(fa_raw, dict):
            fa_raw = fa_raw.get("$")
        if fa_raw is not None:
            try:
                funded_amount = float(fa_raw)
            except (ValueError, TypeError):
                funded_amount = None

    # Handle total_cost similarly
    total_cost = None
    tc_raw = get_value(proj.get("totalcost"))
    if tc_raw is not None:
        try:
            total_cost = float(tc_raw)
        except (ValueError, TypeError):
            total_cost = None

    return {
        "project_code": get_value(proj.get("code")),
        "title": get_value(proj.get("title")),
        "start_date": get_value(proj.get("startdate")),
        "end_date": get_value(proj.get("enddate")),
        "funded_amount": funded_amount,
        "total_cost": total_cost,
        "currency": get_value(proj.get("currency")),
        "keywords": get_value(proj.get("keywords")),
        "website_url": get_value(proj.get("websiteurl")),
        "doi": doi,
        "funding_program": funding_program,
        "openaire_id": get_value(proj.get("originalId")),
    }


def get_total_count(session: requests.Session) -> int:
    """Get total number of FWF projects."""
    params = {
        "funder": "FWF",
        "format": "json",
        "size": 1,
        "page": 1
    }
    response = session.get(OPENAIRE_API_BASE, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    total = data.get("response", {}).get("header", {}).get("total", {}).get("$", 0)
    return int(total)


# =============================================================================
# Checkpoint Functions
# =============================================================================

def save_checkpoint(output_dir: Path, projects: list[dict], last_page: int):
    """Save checkpoint with current progress."""
    checkpoint_path = output_dir / "fwf_checkpoint.json"
    data_path = output_dir / "fwf_partial.parquet"

    # Save partial data
    if projects:
        df = pd.DataFrame(projects)
        df.to_parquet(data_path, index=False)

    # Save checkpoint metadata
    checkpoint = {
        "last_page": last_page,
        "project_count": len(projects),
        "timestamp": datetime.now().isoformat()
    }
    with open(checkpoint_path, "w") as f:
        json.dump(checkpoint, f, indent=2)

    print(f"  Checkpoint saved: page {last_page}, {len(projects):,} projects")


def load_checkpoint(output_dir: Path) -> tuple[list[dict], int]:
    """Load checkpoint if it exists."""
    checkpoint_path = output_dir / "fwf_checkpoint.json"
    data_path = output_dir / "fwf_partial.parquet"

    if not checkpoint_path.exists() or not data_path.exists():
        return [], 0

    with open(checkpoint_path) as f:
        checkpoint = json.load(f)

    df = pd.read_parquet(data_path)
    projects = df.to_dict(orient="records")

    print(f"  Resumed from checkpoint: page {checkpoint['last_page']}, {len(projects):,} projects")
    return projects, checkpoint["last_page"]


def clear_checkpoint(output_dir: Path):
    """Remove checkpoint files."""
    checkpoint_path = output_dir / "fwf_checkpoint.json"
    data_path = output_dir / "fwf_partial.parquet"

    if checkpoint_path.exists():
        checkpoint_path.unlink()
    if data_path.exists():
        data_path.unlink()


# =============================================================================
# Main Download Function
# =============================================================================

def download_fwf_projects(output_dir: Path, resume: bool = False) -> pd.DataFrame:
    """Download all FWF projects from OpenAIRE API."""
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "OpenAlex-FWF-Downloader/1.0 (contact@openalex.org)"
    })

    # Get total count
    print("Fetching total project count...")
    total_count = get_total_count(session)
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"Found {total_count:,} FWF projects across {total_pages} pages")

    # Load checkpoint if resuming
    all_projects = []
    start_page = 1
    if resume:
        all_projects, start_page = load_checkpoint(output_dir)
        if start_page > 0:
            start_page += 1  # Start from next page

    if start_page > total_pages:
        print("All pages already downloaded!")
        return pd.DataFrame(all_projects)

    # Initialize progress tracker
    tracker = ProgressTracker(total_pages)
    tracker.completed_pages = start_page - 1
    tracker.total_projects = len(all_projects)

    print(f"\nDownloading pages {start_page} to {total_pages}...")

    for page in range(start_page, total_pages + 1):
        # Fetch page
        projects, is_error = fetch_page(page, session)
        all_projects.extend(projects)
        tracker.update(len(projects), is_error)

        # Report progress
        if tracker.should_report():
            tracker.report()

        # Save checkpoint periodically
        if page % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(output_dir, all_projects, page)

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Final report
    tracker.report()

    # Clear checkpoint on success
    clear_checkpoint(output_dir)

    print(f"\nDownload complete! {len(all_projects):,} projects retrieved.")

    return pd.DataFrame(all_projects)


# =============================================================================
# Upload to S3
# =============================================================================

def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    """Upload file to S3 using AWS CLI."""
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")

    try:
        result = subprocess.run(
            ["aws", "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Download FWF projects from OpenAIRE to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/fwf_download"),
                        help="Directory for output files")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint if available")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (for testing)")
    args = parser.parse_args()

    print("=" * 60)
    print("FWF (Austrian Science Fund) Data Download")
    print("=" * 60)
    print(f"Source: OpenAIRE API")
    print(f"Output: s3://{S3_BUCKET}/{S3_KEY}")
    print(f"Local dir: {args.output_dir}")
    print("=" * 60)

    # Download projects
    df = download_fwf_projects(args.output_dir, resume=args.resume)

    if df.empty:
        print("No projects downloaded!")
        return 1

    # Save to parquet
    output_path = args.output_dir / "fwf_projects.parquet"
    df.to_parquet(output_path, index=False)
    print(f"\nSaved to {output_path}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    # Show sample
    print("\nSample data:")
    print(df[["project_code", "title", "funded_amount", "start_date"]].head(3).to_string())

    # Summary stats
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    print(f"Total projects: {len(df):,}")
    print(f"Projects with funding amount: {df['funded_amount'].notna().sum():,}")
    if df['funded_amount'].notna().any():
        total_funding = df['funded_amount'].sum()
        print(f"Total funding: EUR {total_funding:,.0f}")
    print(f"Date range: {df['start_date'].min()} to {df['start_date'].max()}")

    # Upload to S3
    if not args.skip_upload:
        if upload_to_s3(output_path, S3_BUCKET, S3_KEY):
            print("\nSuccess! Data is ready for Databricks ingestion.")
            return 0
        else:
            print("\nUpload failed. Data saved locally.")
            return 1
    else:
        print("\nSkipped S3 upload (--skip-upload)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
