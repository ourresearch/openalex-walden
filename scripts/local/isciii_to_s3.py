#!/usr/bin/env python3
"""
ISCIII (Instituto de Salud Carlos III) to S3 Data Pipeline
===========================================================

This script downloads Spanish health research projects from Portal FIS,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://portalfis.isciii.es/
Output: s3://openalex-ingest/awards/isciii/isciii_projects.parquet

Portal FIS coverage: Health research projects from 2013-2024, including:
- PI (Proyectos de Investigación)
- AC (Acciones)
- FI (other project types)
- And more

What this script does:
1. Scrapes project listings from Portal FIS search results
2. Extracts project IDs and basic metadata from list pages
3. Fetches full details for each project
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved periodically; resume with --resume
- Retry logic: Failed requests are retried up to 3 times
- ETA reporting: Shows estimated time remaining
- Polite scraping: Delays between requests to avoid overloading server

Requirements:
    pip install pandas pyarrow requests beautifulsoup4 lxml

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/isciii/

Usage:
    python isciii_to_s3.py

    # Resume interrupted download:
    python isciii_to_s3.py --resume

    # Test with limited pages:
    python isciii_to_s3.py --max-projects 100

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
from bs4 import BeautifulSoup

# =============================================================================
# Configuration
# =============================================================================

PORTAL_FIS_BASE = "https://portalfis.isciii.es"
LIST_URL = f"{PORTAL_FIS_BASE}/lista"
PROJECT_URL = f"{PORTAL_FIS_BASE}/proyecto"

# Search prefixes to use for discovering all projects
# These cover the major project types in Portal FIS
SEARCH_PREFIXES = ["PI", "AC", "FI", "CP", "DTS", "AES", "COV", "RD"]

REQUEST_DELAY = 0.3  # Seconds between requests (be polite)
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N projects
MAX_WORKERS = 3  # Parallel project fetches (keep low for HTML scraping)

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/isciii/isciii_projects.parquet"

# OpenAlex funder info
FUNDER_ID = 4320334923
FUNDER_DISPLAY_NAME = "Instituto de Salud Carlos III"
FUNDER_ROR_ID = "https://ror.org/00ca2c886"
FUNDER_DOI = "10.13039/501100004587"


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
        """Check if we should print a progress report."""
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
            f"[{rate:.2f}/s] "
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
        self.checkpoint_file = output_dir / "isciii_checkpoint.json"
        self.projects_file = output_dir / "isciii_projects_partial.json"
        self.data = {
            "discovered_ids": [],
            "completed_ids": [],
            "failed_ids": [],
            "last_updated": None,
        }
        self.projects = []

    def load(self) -> bool:
        """Load checkpoint from disk."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    self.data = json.load(f)
                print(f"  [CHECKPOINT] Loaded: {len(self.data['completed_ids']):,} projects completed")

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

        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

        with open(self.projects_file, "w") as f:
            json.dump(self.projects, f)

    def add_discovered_ids(self, ids: list[str]):
        """Add discovered project IDs."""
        for pid in ids:
            if pid not in self.data["discovered_ids"]:
                self.data["discovered_ids"].append(pid)

    def mark_completed(self, project_id: str):
        """Mark a project as completed."""
        if project_id not in self.data["completed_ids"]:
            self.data["completed_ids"].append(project_id)

    def mark_failed(self, project_id: str):
        """Mark a project as failed."""
        if project_id not in self.data["failed_ids"]:
            self.data["failed_ids"].append(project_id)

    def add_project(self, project: dict):
        """Add a project to the partial save."""
        self.projects.append(project)

    def get_remaining_ids(self) -> list[str]:
        """Get list of IDs that still need to be fetched."""
        completed = set(self.data["completed_ids"])
        failed = set(self.data["failed_ids"])
        return [pid for pid in self.data["discovered_ids"] if pid not in completed and pid not in failed]

    def cleanup(self):
        """Remove checkpoint files after successful completion."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.projects_file.exists():
            self.projects_file.unlink()
        print("  [CHECKPOINT] Cleaned up checkpoint files")


# =============================================================================
# Scraping Functions
# =============================================================================

def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OpenAlex-ISCIII-Ingest/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    })
    return session


def search_projects(session: requests.Session, search_term: str, page: int = 1) -> tuple[list[str], int]:
    """
    Search for projects and extract project IDs.

    Returns:
        Tuple of (list of project IDs, total results count)
    """
    data = {
        "txtBuscar": search_term,
        "pagina": str(page),
        "chkCoincidenciaExacta": ""
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(LIST_URL, data=data, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            # Extract total count
            total_elem = soup.find("span", id="lblTotalResultados")
            total_count = 0
            if total_elem:
                match = re.search(r"(\d+)\s+resultados", total_elem.text)
                if match:
                    total_count = int(match.group(1))

            # Extract project IDs from links like: javascript:goToDetail('PI20/01361')
            project_ids = []
            for link in soup.find_all("a", class_="enlaceProyecto"):
                href = link.get("href", "")
                match = re.search(r"goToDetail\('([^']+)'\)", href)
                if match:
                    project_ids.append(match.group(1))

            return project_ids, total_count

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                print(f"  [WARN] Search failed for '{search_term}' page {page}: {e}")
                return [], 0

    return [], 0


def fetch_project_details(session: requests.Session, project_id: str) -> Optional[dict]:
    """
    Fetch full details for a single project.

    Args:
        session: Requests session
        project_id: Project ID (e.g., 'PI20/01361')

    Returns:
        Project dictionary or None if failed
    """
    url = f"{PORTAL_FIS_BASE}/proyecto?idProyecto={project_id}"

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            # Parse project details
            project = parse_project_html(soup, project_id)
            return project

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                return None

    return None


def parse_project_html(soup: BeautifulSoup, project_id: str) -> dict:
    """Parse project details from HTML."""

    # Title - in h2 element
    title = None
    h2 = soup.find("h2")
    if h2:
        title = h2.get_text(strip=True)

    # Abstract - in p.resumenDelProyecto
    abstract = None
    abstract_elem = soup.find("p", class_="resumenDelProyecto")
    if abstract_elem:
        abstract = abstract_elem.get_text(strip=True)

    # Extract field values from the detail page
    # The page uses "bocadillo" divs as headers followed by <p> elements with values

    pi_name = None
    period = None
    keywords = None
    institution = None
    amount = None
    funder_scheme = None

    # Find all bocadillo headers and their following siblings
    for bocadillo in soup.find_all("div", class_="bocadillo"):
        label = bocadillo.get_text(strip=True).lower()
        # Get the next sibling element (usually a <p>)
        next_elem = bocadillo.find_next_sibling()
        if next_elem and next_elem.name == "p":
            value = next_elem.get_text(strip=True)

            if "investigador principal" in label:
                pi_name = value
            elif "periodo" in label or "período" in label:
                period = value
            elif "palabras clave" in label:
                keywords = value
            elif "centro beneficiario" in label:
                institution = value
            elif "financiación" in label or "importe" in label:
                amount = value
            elif "convocatoria" in label or "tipo" in label:
                funder_scheme = value

    # Parse period into start/end dates
    start_date = None
    end_date = None
    if period:
        # Format: "01/01/2020  - 31/12/2022"
        date_match = re.search(r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})", period)
        if date_match:
            try:
                start_date = datetime.strptime(date_match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
            try:
                end_date = datetime.strptime(date_match.group(2), "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Parse PI name into given/family
    pi_given_name = None
    pi_family_name = None
    if pi_name:
        # Spanish names: typically "FAMILY_NAME, GIVEN_NAME" or "GIVEN FAMILY"
        if "," in pi_name:
            parts = pi_name.split(",", 1)
            pi_family_name = parts[0].strip().title()
            pi_given_name = parts[1].strip().title() if len(parts) > 1 else None
        else:
            # Assume last word(s) are family name
            parts = pi_name.split()
            if len(parts) >= 2:
                pi_given_name = parts[0].title()
                pi_family_name = " ".join(parts[1:]).title()
            else:
                pi_family_name = pi_name.title()

    # Parse amount
    amount_value = None
    if amount:
        # Remove currency symbols and parse
        amount_clean = re.sub(r"[^\d.,]", "", amount)
        # Handle Spanish/European number format:
        # - "147,620" = 147620 (comma as thousands separator, no decimal)
        # - "1.234,56" = 1234.56 (period as thousands separator, comma as decimal)
        # - "147620" = 147620 (no separators)
        if "," in amount_clean and "." in amount_clean:
            # Both separators: period is thousands, comma is decimal
            amount_clean = amount_clean.replace(".", "").replace(",", ".")
        elif "," in amount_clean:
            # Only comma: could be thousands or decimal separator
            # If comma is followed by exactly 3 digits at end, it's thousands
            # Otherwise treat as decimal
            parts = amount_clean.split(",")
            if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
                # Comma is thousands separator (e.g., "147,620")
                amount_clean = amount_clean.replace(",", "")
            else:
                # Comma is decimal separator (e.g., "147,62")
                amount_clean = amount_clean.replace(",", ".")
        # Period-only case: just remove periods (thousands separator)
        elif "." in amount_clean and amount_clean.count(".") > 1:
            amount_clean = amount_clean.replace(".", "")
        try:
            amount_value = float(amount_clean)
        except ValueError:
            pass

    # Determine funding type from project ID prefix
    funding_type = "research"  # default
    prefix = project_id.split("/")[0] if "/" in project_id else project_id[:2]
    prefix_upper = prefix.upper()
    if prefix_upper in ["FI", "RD"]:
        funding_type = "fellowship"
    elif prefix_upper in ["CP"]:
        funding_type = "training"
    elif prefix_upper in ["DTS"]:
        funding_type = "infrastructure"
    elif prefix_upper in ["COV"]:
        funding_type = "research"  # COVID research

    return {
        "project_id": project_id,
        "title": title,
        "abstract": abstract,
        "pi_given_name": pi_given_name,
        "pi_family_name": pi_family_name,
        "institution": institution,
        "start_date": start_date,
        "end_date": end_date,
        "amount": amount_value,
        "currency": "EUR" if amount_value else None,
        "keywords": keywords,
        "funder_scheme": funder_scheme,
        "funding_type": funding_type,
        "landing_page_url": f"{PORTAL_FIS_BASE}/proyecto?idProyecto={project_id}",
    }


# =============================================================================
# Discovery Phase
# =============================================================================

def discover_all_project_ids(session: requests.Session, checkpoint: CheckpointManager) -> list[str]:
    """
    Discover all project IDs by searching with different prefixes.
    """
    print(f"\n{'='*60}")
    print("Step 1: Discovering project IDs")
    print(f"{'='*60}")

    # Use existing discovered IDs if resuming
    if checkpoint.data["discovered_ids"]:
        print(f"  [CHECKPOINT] Using {len(checkpoint.data['discovered_ids']):,} previously discovered IDs")
        return checkpoint.data["discovered_ids"]

    all_ids = set()

    for prefix in SEARCH_PREFIXES:
        print(f"\n  Searching for prefix: {prefix}")

        # First page to get total count
        ids, total = search_projects(session, prefix, page=1)
        if total == 0:
            print(f"    No results for {prefix}")
            continue

        all_ids.update(ids)
        print(f"    Found {total:,} results, page 1: {len(ids)} IDs")

        # Calculate total pages (10 results per page)
        total_pages = (total + 9) // 10

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            time.sleep(REQUEST_DELAY)
            ids, _ = search_projects(session, prefix, page=page)
            all_ids.update(ids)

            if page % 50 == 0:
                print(f"    Page {page}/{total_pages}: {len(all_ids):,} unique IDs so far")

        print(f"    Completed {prefix}: {len(all_ids):,} total unique IDs")
        time.sleep(REQUEST_DELAY)

    # Save discovered IDs to checkpoint
    id_list = sorted(list(all_ids))
    checkpoint.add_discovered_ids(id_list)
    checkpoint.save()

    print(f"\n  Total unique project IDs discovered: {len(id_list):,}")
    return id_list


# =============================================================================
# Fetch Phase
# =============================================================================

def fetch_all_projects(
    session: requests.Session,
    project_ids: list[str],
    checkpoint: CheckpointManager,
    max_projects: Optional[int] = None
) -> list[dict]:
    """
    Fetch full details for all projects.
    """
    print(f"\n{'='*60}")
    print("Step 2: Fetching project details")
    print(f"{'='*60}")

    # Get remaining IDs
    remaining_ids = checkpoint.get_remaining_ids()

    if max_projects:
        remaining_ids = remaining_ids[:max_projects]
        print(f"  [LIMIT] Limited to {max_projects} projects")

    if not remaining_ids:
        print("  All projects already fetched!")
        return checkpoint.projects

    print(f"  Projects to fetch: {len(remaining_ids):,}")

    progress = ProgressTracker(len(remaining_ids))
    items_since_checkpoint = 0

    print(f"\n  Starting fetch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for i, project_id in enumerate(remaining_ids):
        project = fetch_project_details(session, project_id)

        if project:
            checkpoint.mark_completed(project_id)
            checkpoint.add_project(project)
            progress.update()
        else:
            checkpoint.mark_failed(project_id)
            progress.update(is_error=True)

        items_since_checkpoint += 1

        # Progress report
        if progress.should_report(15):
            print(f"\r{progress.get_progress_line()}", flush=True)

        # Checkpoint save
        if items_since_checkpoint >= CHECKPOINT_INTERVAL:
            checkpoint.save()
            items_since_checkpoint = 0
            print(f"\n  [CHECKPOINT] Saved: {len(checkpoint.projects):,} projects")

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Final save
    checkpoint.save()

    print(f"\n\n  {'='*50}")
    print(f"  Fetch complete!")
    print(f"  Total projects: {len(checkpoint.projects):,}")
    print(f"  Failed: {progress.errors}")
    print(f"  Time: {progress.get_elapsed()}")

    return checkpoint.projects


# =============================================================================
# Processing Functions
# =============================================================================

def process_projects(projects: list[dict], output_dir: Path) -> Path:
    """Process projects into a parquet file."""
    print(f"\n{'='*60}")
    print("Step 3: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    # Remove duplicates by project_id
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add funder info
    df["funder_id"] = FUNDER_ID
    df["funder_display_name"] = FUNDER_DISPLAY_NAME
    df["funder_ror_id"] = FUNDER_ROR_ID
    df["funder_doi"] = FUNDER_DOI

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["provenance"] = "portal_fis"

    # Save to parquet
    output_path = output_dir / "isciii_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With abstract: {df['abstract'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")

    if "funding_type" in df.columns:
        print(f"\n  Funding types:")
        print(df["funding_type"].value_counts().to_string())

    # Year distribution
    if "start_date" in df.columns:
        df["start_year"] = pd.to_datetime(df["start_date"], errors="coerce").dt.year
        print(f"\n  Year distribution:")
        print(df["start_year"].value_counts().sort_index().tail(15).to_string())

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
    print("Step 4: Uploading to S3")
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
        description="Download ISCIII Portal FIS projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./isciii_data"),
        help="Directory for downloaded/processed files"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available"
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

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ISCIII (Instituto de Salud Carlos III) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Data source: Portal FIS (https://portalfis.isciii.es/)")
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Initialize
    session = get_session()
    checkpoint = CheckpointManager(args.output_dir)

    if args.resume:
        checkpoint.load()

    # Step 1: Discover project IDs
    project_ids = discover_all_project_ids(session, checkpoint)

    if not project_ids:
        print("[ERROR] No project IDs discovered!")
        sys.exit(1)

    # Step 2: Fetch project details
    projects = fetch_all_projects(session, project_ids, checkpoint, args.max_projects)

    if not projects:
        print("[ERROR] No projects fetched!")
        sys.exit(1)

    # Clean up checkpoint on success
    checkpoint.cleanup()

    # Step 3: Process to parquet
    parquet_path = process_projects(projects, args.output_dir)

    # Step 4: Upload to S3
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateISCIIIAwards.ipynb")


if __name__ == "__main__":
    main()
