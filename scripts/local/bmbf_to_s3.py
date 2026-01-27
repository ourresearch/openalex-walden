#!/usr/bin/env python3
"""
BMBF (Bundesministerium für Bildung und Forschung) Förderkatalog to S3 Data Pipeline
=====================================================================================

This script downloads German federal research funding data from the Förderkatalog,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://foerderportal.bund.de/foekat/
Output: s3://openalex-ingest/awards/bmbf/bmbf_projects.parquet

What this script does:
1. Searches for all projects in the Förderkatalog (HTML scraping)
2. Extracts FKZ (Förderkennzeichen) IDs from paginated results
3. Fetches detail pages for each project
4. Parses HTML to extract project metadata
5. Combines into a single DataFrame
6. Saves as parquet and uploads to S3

Note: The Förderkatalog includes projects from multiple ministries:
- BMFTR (formerly BMBF) - Education and Research (primary)
- BMWK - Economy and Climate
- BMUV - Environment
- BMEL - Agriculture
- etc.

We extract ALL projects since the funder can be filtered in Databricks.

Features:
- Checkpointing: Progress is saved every 100 projects; resume with --resume
- Retry logic: Failed requests are retried up to 3 times
- ETA reporting: Shows estimated time remaining
- Session management: Maintains HTTP session for efficiency

Requirements:
    pip install pandas pyarrow requests beautifulsoup4 lxml

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/bmbf/

Usage:
    python bmbf_to_s3.py

    # Resume interrupted download:
    python bmbf_to_s3.py --resume

    # Test with limited data:
    python bmbf_to_s3.py --max-projects 100

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
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# =============================================================================
# Configuration
# =============================================================================

# Förderkatalog settings
FOEKAT_BASE = "https://foerderportal.bund.de/foekat/jsp/"
SEARCH_URL = FOEKAT_BASE + "SucheAction.do"
REQUEST_DELAY = 0.2  # Seconds between requests (be polite)
MAX_WORKERS = 3  # Parallel detail page fetches (conservative)
MAX_RETRIES = 3  # Max retries per request
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N projects
PAGE_SIZE = 1000  # Results per page (maximum allowed)
MAX_PAGES = 350  # Safety limit: 300k results / 1000 per page = 300, add buffer

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bmbf/bmbf_projects.parquet"


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

    def should_report(self, interval: int = 10) -> bool:
        """Check if we should print a progress report (every N seconds)."""
        now = time.time()
        if now - self.last_report_time >= interval:
            self.last_report_time = now
            return True
        return False

    def get_progress_line(self) -> str:
        """Get formatted progress line."""
        pct = (self.completed_items / self.total_items) * 100 if self.total_items > 0 else 0
        rate = self.get_rate()
        return (
            f"  [{self.completed_items:,}/{self.total_items:,} ({pct:.1f}%)] "
            f"[{rate:.1f} proj/s] "
            f"[Elapsed: {self.get_elapsed()}] "
            f"[ETA: {self.get_eta()}] "
            f"[Errors: {self.errors}]"
        )


# =============================================================================
# Checkpoint Management
# =============================================================================

class CheckpointManager:
    """Manage checkpointing for resumable downloads."""

    def __init__(self, output_dir: Path):
        self.checkpoint_file = output_dir / "bmbf_checkpoint.json"
        self.projects_file = output_dir / "bmbf_projects_partial.json"
        self.data = {
            "completed_fkz": [],
            "failed_fkz": [],
            "total_fkz": 0,
            "last_updated": None,
            "projects_count": 0
        }
        self.projects = []

    def load(self) -> bool:
        """Load checkpoint from disk. Returns True if checkpoint exists."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    self.data = json.load(f)
                print(f"  [CHECKPOINT] Loaded checkpoint: {len(self.data['completed_fkz']):,} FKZs completed")

                if self.projects_file.exists():
                    with open(self.projects_file, "r") as f:
                        self.projects = json.load(f)
                    print(f"  [CHECKPOINT] Loaded {len(self.projects):,} projects from partial save")

                return True
            except Exception as e:
                print(f"  [WARN] Failed to load checkpoint: {e}")
        return False

    def save(self):
        """Save checkpoint to disk."""
        self.data["last_updated"] = datetime.utcnow().isoformat()
        self.data["projects_count"] = len(self.projects)

        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

        with open(self.projects_file, "w") as f:
            json.dump(self.projects, f)

    def mark_completed(self, fkz: str):
        """Mark an FKZ as completed."""
        if fkz not in self.data["completed_fkz"]:
            self.data["completed_fkz"].append(fkz)

    def mark_failed(self, fkz: str):
        """Mark an FKZ as failed."""
        if fkz not in self.data["failed_fkz"]:
            self.data["failed_fkz"].append(fkz)

    def add_project(self, project: dict):
        """Add a project to the partial save."""
        self.projects.append(project)

    def get_remaining_fkz(self, all_fkz: list[str]) -> list[str]:
        """Get list of FKZs that still need to be downloaded."""
        completed = set(self.data["completed_fkz"])
        return [fkz for fkz in all_fkz if fkz not in completed]

    def set_total_fkz(self, total: int):
        """Set total FKZ count."""
        self.data["total_fkz"] = total

    def cleanup(self):
        """Remove checkpoint files after successful completion."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.projects_file.exists():
            self.projects_file.unlink()
        print("  [CHECKPOINT] Cleaned up checkpoint files")


# =============================================================================
# HTML Parsing Functions
# =============================================================================

def clean_text(text: str) -> str:
    """Clean HTML text: unescape entities, normalize whitespace."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_amount(amount_str: str) -> Optional[float]:
    """Parse German-formatted amount like '798.894,00 €' to float."""
    if not amount_str:
        return None
    try:
        # Remove currency symbol and whitespace
        amount_str = re.sub(r'[€\s]', '', amount_str)
        # German format: 1.234.567,89 -> 1234567.89
        amount_str = amount_str.replace('.', '').replace(',', '.')
        return float(amount_str)
    except (ValueError, AttributeError):
        return None


def parse_date_range(date_str: str) -> tuple[Optional[str], Optional[str]]:
    """Parse date range like '01.01.1992 bis 31.12.1994' to ISO dates."""
    if not date_str:
        return None, None

    # Match patterns like "DD.MM.YYYY bis DD.MM.YYYY" or "DD.MM.YYYY - DD.MM.YYYY"
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*(?:bis|-)\s*(\d{2}\.\d{2}\.\d{4})', date_str)
    if match:
        start_de, end_de = match.groups()
        try:
            start = datetime.strptime(start_de, '%d.%m.%Y').strftime('%Y-%m-%d')
            end = datetime.strptime(end_de, '%d.%m.%Y').strftime('%Y-%m-%d')
            return start, end
        except ValueError:
            pass

    # Try single date
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', date_str)
    if match:
        try:
            date = datetime.strptime(match.group(1), '%d.%m.%Y').strftime('%Y-%m-%d')
            return date, None
        except ValueError:
            pass

    return None, None


def parse_detail_page(html: str, fkz: str) -> Optional[dict]:
    """
    Parse a project detail page to extract metadata.

    Args:
        html: HTML content of detail page
        fkz: The FKZ (funding ID) for this project

    Returns:
        Dictionary with project fields, or None if parsing fails
    """
    try:
        soup = BeautifulSoup(html, 'lxml')

        # Find the detail section
        detail_div = soup.find('div', class_='detailAnsicht')
        if not detail_div:
            return None

        # Extract fields from detailAnsichtItem divs
        project = {
            'fkz': fkz,
            'title': None,
            'amount': None,
            'currency': 'EUR',
            'ressort': None,
            'referat': None,
            'projekttraeger': None,
            'start_date': None,
            'end_date': None,
            'leistungsplan': None,
            'foerderart': None,
            'foerderprofil': None,
            'verbund': None,
            'zuwendungsempfaenger': None,
            'ort': None,
            'bundesland': None,
            'staat': None,
            'ausfuehrende_stelle': None,
            'ausfuehrende_stelle_ort': None,
        }

        items = detail_div.find_all('div', class_='detailAnsichtItem')

        current_field = None
        for item in items:
            divs = item.find_all('div', recursive=False)

            i = 0
            while i < len(divs):
                label_div = divs[i]
                label = clean_text(label_div.get_text())

                # Get value from next div
                if i + 1 < len(divs):
                    value_div = divs[i + 1]
                    value = clean_text(value_div.get_text())
                else:
                    value = None

                # Map German labels to our field names
                if 'Thema' in label:
                    project['title'] = value
                elif 'Fördersumme' in label or 'rdersumme' in label:
                    project['amount'] = parse_amount(value)
                elif label == 'Ressort':
                    # Parse "BMFTR, Referat F26"
                    if value:
                        parts = value.split(',')
                        project['ressort'] = parts[0].strip() if parts else value
                        if len(parts) > 1:
                            referat_match = re.search(r'Referat\s+(\S+)', parts[1])
                            if referat_match:
                                project['referat'] = referat_match.group(1)
                elif 'Projektträger' in label or 'Projekttr' in label:
                    project['projekttraeger'] = value
                elif 'Laufzeit' in label:
                    start, end = parse_date_range(value)
                    project['start_date'] = start
                    project['end_date'] = end
                elif 'Leistungsplansystematik' in label:
                    project['leistungsplan'] = value
                elif 'Förderart' in label or 'rderart' in label:
                    project['foerderart'] = value
                elif 'Förderprofil' in label or 'rderprofil' in label:
                    project['foerderprofil'] = value
                elif 'Verbund' in label:
                    project['verbund'] = value == 'Ja'
                elif 'Zuwendungsempfänger' in label or 'Zuwendungsempf' in label:
                    project['zuwendungsempfaenger'] = value
                elif label == 'Ort':
                    # Could be either ZE or AS ort
                    if project['zuwendungsempfaenger'] and not project['ort']:
                        project['ort'] = value
                    elif project['ausfuehrende_stelle'] and not project['ausfuehrende_stelle_ort']:
                        project['ausfuehrende_stelle_ort'] = value
                elif 'Bundesland' in label:
                    if value:
                        # Parse "Berlin, Deutschland"
                        parts = value.split(',')
                        project['bundesland'] = parts[0].strip() if parts else None
                        if len(parts) > 1:
                            project['staat'] = parts[1].strip()
                elif 'Ausführende Stelle' in label or 'Ausf' in label:
                    project['ausfuehrende_stelle'] = value

                i += 2  # Move to next label-value pair

        # Build landing page URL
        project['landing_page_url'] = f"https://foerderportal.bund.de/foekat/jsp/SucheAction.do?actionMode=view&fkz={fkz}"

        return project

    except Exception as e:
        print(f"  [WARN] Failed to parse detail page for {fkz}: {e}")
        return None


# =============================================================================
# API Functions
# =============================================================================

def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (OpenAlex Research Crawler; +https://openalex.org)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'de,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    return session


def search_all_projects(session: requests.Session, max_fkz: Optional[int] = None) -> tuple[int, list[str]]:
    """
    Search for all projects and extract FKZ IDs.

    Args:
        session: Requests session
        max_fkz: Optional limit on FKZs to return (for testing)

    Returns:
        Tuple of (total_count, list of FKZ IDs)
    """
    print("  [INFO] Searching for all projects...")
    if max_fkz:
        print(f"  [INFO] Will stop after finding {max_fkz:,} FKZs")

    all_fkz = []
    page = 1
    total_count = None

    while True:
        # Build search form data
        # Using wildcard search to get all projects
        # Pagination uses 1-indexed listrowfrom: page 1 = 1, page 2 = 1001, etc.
        form_data = {
            'actionMode': 'searchlist',
            'suche.detailSuche': 'false',
            'suche.schnellSuche': '%',  # Wildcard to match all
            'suche.listrowpersite': str(PAGE_SIZE),
            'suche.orderby': '1',
            'suche.order': 'asc',
            'suche.listrowfrom': str((page - 1) * PAGE_SIZE + 1),  # 1-indexed
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = session.post(SEARCH_URL, data=form_data, timeout=120)
                response.raise_for_status()
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"  [WARN] Search page {page} failed (attempt {attempt + 1}): {e}")
                    time.sleep(RETRY_BACKOFF ** attempt)
                else:
                    raise RuntimeError(f"Failed to fetch search page {page}: {e}")

        soup = BeautifulSoup(response.text, 'lxml')

        # Extract FKZ IDs from links
        # Pattern: SucheAction.do?actionMode=view&fkz=XXX
        fkz_links = soup.find_all('a', href=re.compile(r'actionMode=view.*fkz='))
        page_fkz = set()

        for link in fkz_links:
            href = link.get('href', '')
            match = re.search(r'fkz=([^&"]+)', href)
            if match:
                fkz = unquote(match.group(1))
                page_fkz.add(fkz)

        if not page_fkz:
            # No more results
            break

        all_fkz.extend(page_fkz)

        # Try to get total count from page
        if total_count is None:
            # Look for text like "Seite 1 von 113"
            page_info = soup.find(string=re.compile(r'Seite\s+\d+\s+von\s+\d+'))
            if page_info:
                match = re.search(r'von\s+(\d+)', page_info)
                if match:
                    total_pages = int(match.group(1))
                    total_count = total_pages * PAGE_SIZE  # Estimate

        print(f"  [INFO] Page {page}: found {len(page_fkz)} FKZs (total so far: {len(all_fkz):,})")

        # Check if we have enough FKZs for testing mode
        if max_fkz and len(all_fkz) >= max_fkz:
            print(f"  [INFO] Reached max_fkz limit ({max_fkz}), stopping search")
            all_fkz = all_fkz[:max_fkz]
            break

        # Check if we got a full page (more pages available)
        if len(page_fkz) < PAGE_SIZE:
            break

        # Safety: stop at max pages to prevent infinite loops
        if page >= MAX_PAGES:
            print(f"  [INFO] Reached MAX_PAGES limit ({MAX_PAGES}), stopping search")
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    # Deduplicate
    all_fkz = list(dict.fromkeys(all_fkz))  # Preserve order, remove duplicates

    print(f"  [INFO] Total unique FKZs found: {len(all_fkz):,}")
    return len(all_fkz), all_fkz


def fetch_project_detail(fkz: str, session: requests.Session) -> tuple[str, Optional[dict], Optional[str]]:
    """
    Fetch and parse a single project detail page.

    Args:
        fkz: Funding ID
        session: Requests session

    Returns:
        Tuple of (fkz, project dict or None, error message or None)
    """
    url = f"{SEARCH_URL}?actionMode=view&fkz={fkz}"

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()

            project = parse_detail_page(response.text, fkz)
            if project:
                return (fkz, project, None)
            else:
                return (fkz, None, "Failed to parse HTML")

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                return (fkz, None, "Timeout after retries")
        except requests.exceptions.HTTPError as e:
            return (fkz, None, f"HTTP {response.status_code}")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                return (fkz, None, str(e))

    return (fkz, None, "Unknown error")


# =============================================================================
# Download Functions
# =============================================================================

def download_all_projects(
    output_dir: Path,
    resume: bool = False,
    max_projects: Optional[int] = None
) -> list[dict]:
    """
    Download all projects from the Förderkatalog.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_projects: Optional limit on projects to download

    Returns:
        List of all project dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading projects from Förderkatalog")
    print(f"{'='*60}")

    checkpoint = CheckpointManager(output_dir)
    session = get_session()

    # First, get all FKZs (or limited number for testing)
    total_count, all_fkz = search_all_projects(session, max_fkz=max_projects)

    checkpoint.set_total_fkz(len(all_fkz))

    # Check for checkpoint
    if resume and checkpoint.load():
        fkz_to_fetch = checkpoint.get_remaining_fkz(all_fkz)
        print(f"  [RESUME] Resuming: {len(fkz_to_fetch):,} FKZs remaining")
    else:
        fkz_to_fetch = all_fkz
        checkpoint.projects = []
        print(f"  [INFO] Starting fresh download of {len(fkz_to_fetch):,} projects")

    if not fkz_to_fetch:
        print("  [INFO] All projects already downloaded!")
        return checkpoint.projects

    # Initialize progress
    progress = ProgressTracker(len(all_fkz))
    progress.completed_items = len(checkpoint.data["completed_fkz"])

    items_since_checkpoint = 0
    failed_fkz = []

    print(f"\n  Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {progress.get_progress_line()}")

    # Download with controlled concurrency
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        batch_size = 200

        for batch_start in range(0, len(fkz_to_fetch), batch_size):
            batch_fkz = fkz_to_fetch[batch_start:batch_start + batch_size]

            futures = {
                executor.submit(fetch_project_detail, fkz, session): fkz
                for fkz in batch_fkz
            }

            for future in as_completed(futures):
                fkz = futures[future]
                try:
                    fkz_result, project, error = future.result()

                    if error:
                        progress.update(is_error=True)
                        failed_fkz.append((fkz_result, error))
                        checkpoint.mark_failed(fkz_result)
                    elif project:
                        progress.update()
                        checkpoint.mark_completed(fkz_result)
                        checkpoint.add_project(project)
                    else:
                        progress.update(is_error=True)
                        checkpoint.mark_failed(fkz_result)

                    items_since_checkpoint += 1

                    if progress.should_report(10):
                        print(f"\r{progress.get_progress_line()}", flush=True)

                    if items_since_checkpoint >= CHECKPOINT_INTERVAL:
                        checkpoint.save()
                        items_since_checkpoint = 0
                        print(f"\n  [CHECKPOINT] Saved: {progress.completed_items:,} projects")

                except Exception as e:
                    progress.update(is_error=True)
                    failed_fkz.append((fkz, str(e)))
                    checkpoint.mark_failed(fkz)

                time.sleep(REQUEST_DELAY)

            checkpoint.save()

    # Final report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total projects: {len(checkpoint.projects):,}")
    print(f"  Total time: {progress.get_elapsed()}")
    print(f"  Average rate: {progress.get_rate():.1f} projects/second")

    if failed_fkz:
        print(f"\n  [WARN] {len(failed_fkz)} projects failed:")
        for fkz, error in failed_fkz[:10]:
            print(f"    - {fkz}: {error}")
        if len(failed_fkz) > 10:
            print(f"    ... and {len(failed_fkz) - 10} more")

        failed_file = output_dir / "bmbf_failed_fkz.json"
        with open(failed_file, "w") as f:
            json.dump(failed_fkz, f, indent=2)
        print(f"  [INFO] Failed FKZs saved to {failed_file}")

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
    print("Step 2: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    # Remove duplicates by FKZ
    original_count = len(df)
    df = df.drop_duplicates(subset=["fkz"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "bmbf_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")

    if df['amount'].notna().any():
        total_funding = df['amount'].sum()
        print(f"    - Total funding: €{total_funding:,.0f}")

    if 'ressort' in df.columns:
        print(f"\n  Ministries (Ressort):")
        print(df["ressort"].value_counts().head(10).to_string())

    if 'start_date' in df.columns:
        # Extract years
        df['_year'] = pd.to_datetime(df['start_date'], errors='coerce').dt.year
        print(f"\n  Year distribution (recent):")
        year_counts = df['_year'].value_counts().sort_index(ascending=False).head(10)
        print(year_counts.to_string())
        df.drop('_year', axis=1, inplace=True)

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
    """
    Upload the parquet file to S3.

    Args:
        local_path: Path to local parquet file

    Returns:
        True if upload succeeded
    """
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    print(f"  [INFO] Using AWS CLI: {aws_cmd}")

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
        description="Download BMBF Förderkatalog projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./bmbf_data"),
        help="Directory for downloaded/processed files (default: ./bmbf_data)"
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
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("BMBF Förderkatalog to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    if args.resume:
        print("Mode: RESUME")

    # Step 1: Download
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
        existing_path = args.output_dir / "bmbf_projects.parquet"
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

    # Step 2: Process
    parquet_path = process_projects(projects, args.output_dir)

    # Step 3: Upload
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
    print(f"  In Databricks, run: notebooks/awards/CreateBMBFAwards.ipynb")


if __name__ == "__main__":
    main()
