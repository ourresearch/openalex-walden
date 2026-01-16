#!/usr/bin/env python3
"""
DFG (Deutsche Forschungsgemeinschaft) to S3 Data Pipeline
==========================================================

This script downloads DFG GEPRIS funding data by crawling the GEPRIS website,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://gepris.dfg.de/gepris/
Output: s3://openalex-ingest/awards/dfg/dfg_projects.parquet

What this script does:
1. Crawls the GEPRIS project catalog (all funded projects since 2000s)
2. Extracts project metadata: ID, title, abstract, PI, institution, dates, programme
3. Saves as parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests beautifulsoup4 lxml

Usage:
    python dfg_to_s3.py

    # Skip upload (for testing):
    python dfg_to_s3.py --skip-upload

    # Limit to N projects (for testing):
    python dfg_to_s3.py --max-projects 1000 --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("BeautifulSoup not found. Installing...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "beautifulsoup4"], check=True)
    from bs4 import BeautifulSoup

# =============================================================================
# Configuration
# =============================================================================

# GEPRIS settings
GEPRIS_BASE = "https://gepris.dfg.de"
GEPRIS_SEARCH = f"{GEPRIS_BASE}/gepris/OCTOPUS"
REQUEST_DELAY = 0.2  # Be polite to the server
MAX_WORKERS = 3  # Parallel fetches (conservative)
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0
CHECKPOINT_INTERVAL = 500

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dfg/dfg_projects.parquet"


# =============================================================================
# Progress Tracker
# =============================================================================

class ProgressTracker:
    """Track download progress with ETA calculation."""

    def __init__(self, total_items: int):
        self.total_items = total_items
        self.completed = 0
        self.errors = 0
        self.start_time = time.time()
        self.last_report_time = time.time()

    def update(self, is_error: bool = False):
        self.completed += 1
        if is_error:
            self.errors += 1

    def get_eta(self) -> str:
        if self.completed == 0:
            return "calculating..."
        elapsed = time.time() - self.start_time
        rate = self.completed / elapsed
        remaining = self.total_items - self.completed
        if rate > 0:
            return str(timedelta(seconds=int(remaining / rate)))
        return "unknown"

    def get_elapsed(self) -> str:
        return str(timedelta(seconds=int(time.time() - self.start_time)))

    def should_report(self, interval: int = 30) -> bool:
        now = time.time()
        if now - self.last_report_time >= interval:
            self.last_report_time = now
            return True
        return False

    def get_progress_line(self) -> str:
        pct = (self.completed / self.total_items * 100) if self.total_items > 0 else 0
        return (
            f"[{self.completed:,}/{self.total_items:,} ({pct:.1f}%)] "
            f"[Elapsed: {self.get_elapsed()}] [ETA: {self.get_eta()}] "
            f"[Errors: {self.errors}]"
        )


# =============================================================================
# GEPRIS Crawler
# =============================================================================

class GEPRISCrawler:
    """Crawl DFG GEPRIS for funded project data."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OpenAlex-GEPRIS-Crawler/1.0 (research indexing)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        })
        self.checkpoint_file = output_dir / "dfg_checkpoint.json"
        self.projects_file = output_dir / "dfg_projects_partial.json"
        self.projects = []
        self.fetched_ids = set()

    def _init_session(self):
        """Initialize session by visiting the main page."""
        print("  [INFO] Initializing session...")
        try:
            resp = self.session.get(GEPRIS_SEARCH, params={
                "task": "showKatalog",
                "context": "projekt",
                "language": "en"
            }, timeout=30)
            resp.raise_for_status()
            print("  [INFO] Session initialized")
            return True
        except Exception as e:
            print(f"  [ERROR] Failed to initialize session: {e}")
            return False

    def _get_project_ids_from_search(self, offset: int = 0, count: int = 50) -> tuple[list[str], int]:
        """
        Get project IDs from the GEPRIS search catalog.

        Returns: (list of project IDs, total count)
        """
        params = {
            "task": "doKatalog",
            "context": "projekt",
            "oldsort": "",
            "sort": "vw_sort_date_start",
            "sortorder": "desc",
            "nurProjekteMitAB": "false",
            "language": "en",
            "index": str(offset),
            "anzahl": str(count),
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(GEPRIS_SEARCH, params=params, timeout=60)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Extract total count from results info
                total = 0
                results_info = soup.select_one('.result_info')
                if results_info:
                    text = results_info.get_text()
                    match = re.search(r'of\s+([\d,]+)\s+results', text)
                    if match:
                        total = int(match.group(1).replace(',', ''))

                # Extract project IDs from links
                project_ids = []
                for link in soup.select('a[href*="projekt/"]'):
                    href = link.get('href', '')
                    match = re.search(r'/projekt/(\d+)', href)
                    if match:
                        project_ids.append(match.group(1))

                # Deduplicate while preserving order
                seen = set()
                unique_ids = []
                for pid in project_ids:
                    if pid not in seen:
                        seen.add(pid)
                        unique_ids.append(pid)

                return unique_ids, total

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF ** attempt)
                else:
                    print(f"  [ERROR] Failed to get project list at offset {offset}: {e}")
                    return [], 0

        return [], 0

    def _fetch_project_details(self, project_id: str) -> Optional[dict]:
        """Fetch details for a single project."""
        url = f"{GEPRIS_BASE}/gepris/projekt/{project_id}"
        params = {"language": "en"}

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Extract project data
                project = {"project_id": project_id}

                # Title
                title_elem = soup.select_one('h1.facelift')
                if title_elem:
                    project["title"] = title_elem.get_text(strip=True)

                # Description/Abstract
                abstract_elem = soup.select_one('#projekttext')
                if abstract_elem:
                    project["abstract"] = abstract_elem.get_text(strip=True)

                # Extract from detail list
                for row in soup.select('.details tr'):
                    label_elem = row.select_one('th')
                    value_elem = row.select_one('td')
                    if not label_elem or not value_elem:
                        continue

                    label = label_elem.get_text(strip=True).lower()
                    value = value_elem.get_text(strip=True)

                    if 'applicant' in label or 'antragsteller' in label:
                        # Extract PI name and ORCID if present
                        project["pi_name"] = value
                        orcid_link = value_elem.select_one('a[href*="orcid.org"]')
                        if orcid_link:
                            project["pi_orcid"] = orcid_link.get('href')

                    elif 'institution' in label or 'einrichtung' in label:
                        project["institution"] = value

                    elif 'subject area' in label or 'fachgebiet' in label:
                        project["subject_area"] = value

                    elif 'dfg programme' in label or 'förderart' in label:
                        project["funding_programme"] = value

                    elif 'term' in label or 'laufzeit' in label:
                        # Parse date range like "2020-2023" or "from 2020 to 2023"
                        project["funding_period"] = value
                        dates = re.findall(r'\d{4}', value)
                        if dates:
                            project["start_year"] = dates[0]
                            if len(dates) > 1:
                                project["end_year"] = dates[-1]

                    elif 'funding' in label and 'amount' in label:
                        # Try to extract amount
                        amount_match = re.search(r'[\d,]+(?:\.\d+)?', value.replace(' ', ''))
                        if amount_match:
                            project["amount"] = amount_match.group().replace(',', '')

                return project

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF ** attempt)
                else:
                    return None

        return None

    def save_checkpoint(self):
        """Save progress to checkpoint files."""
        checkpoint_data = {
            "fetched_ids": list(self.fetched_ids),
            "last_updated": datetime.utcnow().isoformat(),
            "project_count": len(self.projects)
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        with open(self.projects_file, 'w') as f:
            json.dump(self.projects, f)

    def load_checkpoint(self) -> bool:
        """Load progress from checkpoint files."""
        if self.checkpoint_file.exists() and self.projects_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                self.fetched_ids = set(data.get("fetched_ids", []))
                with open(self.projects_file, 'r') as f:
                    self.projects = json.load(f)
                print(f"  [CHECKPOINT] Loaded {len(self.projects):,} projects from checkpoint")
                return True
            except Exception as e:
                print(f"  [WARN] Failed to load checkpoint: {e}")
        return False

    def cleanup_checkpoint(self):
        """Remove checkpoint files after success."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.projects_file.exists():
            self.projects_file.unlink()

    def crawl(self, max_projects: Optional[int] = None, resume: bool = False) -> list[dict]:
        """
        Crawl GEPRIS for all project data.

        Args:
            max_projects: Optional limit on projects to fetch
            resume: Whether to resume from checkpoint

        Returns:
            List of project dictionaries
        """
        print(f"\n{'='*60}")
        print("Step 1: Crawling GEPRIS for project data")
        print(f"{'='*60}")

        # Initialize session
        if not self._init_session():
            return []

        # Load checkpoint if resuming
        if resume:
            self.load_checkpoint()

        # Get initial project list to determine total count
        print("  [INFO] Getting project count...")
        _, total_count = self._get_project_ids_from_search(offset=0, count=1)

        if total_count == 0:
            print("  [ERROR] Could not determine total project count")
            # Try alternative: estimate based on known GEPRIS size (~80K projects)
            total_count = 80000
            print(f"  [INFO] Using estimated count: {total_count:,}")
        else:
            print(f"  [INFO] Total projects in GEPRIS: {total_count:,}")

        if max_projects:
            total_count = min(total_count, max_projects)
            print(f"  [INFO] Limiting to {total_count:,} projects")

        # Collect all project IDs first
        print("  [INFO] Collecting project IDs...")
        all_ids = []
        page_size = 50

        for offset in range(0, total_count, page_size):
            ids, _ = self._get_project_ids_from_search(offset=offset, count=page_size)
            new_ids = [pid for pid in ids if pid not in self.fetched_ids]
            all_ids.extend(new_ids)

            if offset % 500 == 0:
                print(f"    Collected {len(all_ids):,} new IDs (offset {offset:,})")

            if max_projects and len(all_ids) + len(self.fetched_ids) >= max_projects:
                all_ids = all_ids[:max_projects - len(self.fetched_ids)]
                break

            time.sleep(REQUEST_DELAY)

        print(f"  [INFO] Found {len(all_ids):,} new projects to fetch")

        if not all_ids:
            print("  [INFO] No new projects to fetch")
            return self.projects

        # Fetch project details
        progress = ProgressTracker(len(all_ids))
        projects_since_checkpoint = 0

        print(f"\n  Starting detail fetch at {datetime.now().strftime('%H:%M:%S')}")
        print(f"  {progress.get_progress_line()}")

        for i, project_id in enumerate(all_ids):
            project = self._fetch_project_details(project_id)

            if project:
                self.projects.append(project)
                self.fetched_ids.add(project_id)
                progress.update()
            else:
                progress.update(is_error=True)

            projects_since_checkpoint += 1

            if progress.should_report(30):
                print(f"  {progress.get_progress_line()}")

            if projects_since_checkpoint >= CHECKPOINT_INTERVAL:
                self.save_checkpoint()
                projects_since_checkpoint = 0
                print(f"  [CHECKPOINT] Saved {len(self.projects):,} projects")

            time.sleep(REQUEST_DELAY)

        print(f"\n  {'='*50}")
        print(f"  Crawl complete!")
        print(f"  Total projects: {len(self.projects):,}")
        print(f"  Time: {progress.get_elapsed()}")
        print(f"  Errors: {progress.errors}")

        return self.projects


# =============================================================================
# Processing Functions
# =============================================================================

def process_projects(projects: list[dict], output_dir: Path) -> Path:
    """Process crawled projects into a parquet file."""
    print(f"\n{'='*60}")
    print("Step 2: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    df = pd.DataFrame(projects)
    print(f"  [INFO] Total rows: {len(df):,}")
    print(f"  [INFO] Columns: {list(df.columns)}")

    # Convert years to dates
    if 'start_year' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_year'], format='%Y', errors='coerce').dt.strftime('%Y-01-01')
    if 'end_year' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_year'], format='%Y', errors='coerce').dt.strftime('%Y-12-31')

    # Convert amount to numeric
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates(subset=['project_id'], keep='first')
    print(f"  [INFO] Removed {original_count - len(df):,} duplicates")

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    df['source'] = 'gepris_crawl'

    # Save to parquet
    output_path = output_dir / "dfg_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  [INFO] Output file size: {size_mb:.1f} MB")

    # Summary
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    for col in ['title', 'abstract', 'pi_name', 'institution', 'funding_programme']:
        if col in df.columns:
            print(f"    - With {col}: {df[col].notna().sum():,}")

    if 'funding_programme' in df.columns:
        print(f"\n  Funding programmes (top 10):")
        print(df['funding_programme'].value_counts().head(10).to_string())

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
    for path in [
        Path.home() / "Library/Python/3.11/bin/aws",
        Path("/usr/local/bin/aws"),
        Path("/opt/homebrew/bin/aws"),
    ]:
        if path.exists():
            return str(path)
    return None


def upload_to_s3(local_path: Path) -> bool:
    """Upload the parquet file to S3."""
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found")
        return False

    try:
        subprocess.run([aws_cmd, "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print("  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Crawl DFG GEPRIS and upload to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./dfg_data"))
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--max-projects", type=int, default=None, help="Limit projects (for testing)")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip crawl, use existing data")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("DFG (Deutsche Forschungsgemeinschaft) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Crawl or load existing
    if args.skip_crawl:
        parquet_path = args.output_dir / "dfg_projects.parquet"
        if not parquet_path.exists():
            print("[ERROR] No existing parquet file found")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing data: {parquet_path}")
    else:
        crawler = GEPRISCrawler(args.output_dir)
        projects = crawler.crawl(max_projects=args.max_projects, resume=args.resume)

        if not projects:
            print("[ERROR] No projects crawled!")
            sys.exit(1)

        # Process
        parquet_path = process_projects(projects, args.output_dir)

        # Cleanup checkpoint on success
        crawler.cleanup_checkpoint()

    # Step 3: Upload
    if not args.skip_upload:
        upload_to_s3(parquet_path)

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")
    print(f"\nNext step: In Databricks, run notebooks/awards/CreateDFGAwards.ipynb")


if __name__ == "__main__":
    main()