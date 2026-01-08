#!/usr/bin/env python3
"""
Gateway to Research (GtR) to S3 Data Pipeline
==============================================

This script downloads all UK Research Council grant data from the GtR API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://gtr.ukri.org/gtr/api
Output: s3://openalex-ingest/awards/gtr/gtr_projects.parquet

What this script does:
1. Fetches all projects from the GtR API (171K+ projects)
2. Parses XML responses to extract project metadata
3. Extracts grant reference, funder, amount, dates, PI, and organization info
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved every 100 pages; resume with --resume
- Retry logic: Failed pages are retried up to 3 times with exponential backoff
- ETA reporting: Shows estimated time remaining based on current progress
- Error tracking: Failed pages are logged and can be retried manually

Output Statistics (expected):
- Total projects: ~171K
- Parquet file size: ~100-200 MB

Requirements:
    pip install pandas pyarrow requests lxml

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/gtr/

Usage:
    python gtr_to_s3.py

    # Resume interrupted download:
    python gtr_to_s3.py --resume

    # Or with options:
    python gtr_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# GtR API settings
GTR_API_BASE = "https://gtr.ukri.org/gtr/api"
PAGE_SIZE = 100  # Max allowed by API
REQUEST_DELAY = 0.1  # Seconds between requests (be polite to API)
MAX_WORKERS = 5  # Parallel page fetches
MAX_RETRIES = 3  # Max retries per page
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N pages

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gtr/gtr_projects.parquet"

# XML namespaces (API uses rcuk.ac.uk namespace, not ukri.org)
NS = {
    "ns1": "http://gtr.rcuk.ac.uk/gtr/api",
    "ns2": "http://gtr.rcuk.ac.uk/gtr/api/project",
}


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
        pages_per_second = self.completed_pages / elapsed
        remaining_pages = self.total_pages - self.completed_pages

        if pages_per_second > 0:
            remaining_seconds = remaining_pages / pages_per_second
            eta = timedelta(seconds=int(remaining_seconds))
            return str(eta)
        return "unknown"

    def get_elapsed(self) -> str:
        """Format elapsed time."""
        elapsed = time.time() - self.start_time
        return str(timedelta(seconds=int(elapsed)))

    def get_rate(self) -> float:
        """Get pages per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.completed_pages / elapsed
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
        pct = (self.completed_pages / self.total_pages) * 100 if self.total_pages > 0 else 0
        rate = self.get_rate()
        return (
            f"  [{self.completed_pages:,}/{self.total_pages:,} pages ({pct:.1f}%)] "
            f"[{self.total_projects:,} projects] "
            f"[{rate:.1f} pages/s] "
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
        self.checkpoint_file = output_dir / "gtr_checkpoint.json"
        self.projects_file = output_dir / "gtr_projects_partial.json"
        self.data = {
            "completed_pages": [],
            "failed_pages": [],
            "total_pages": 0,
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
                print(f"  [CHECKPOINT] Loaded checkpoint: {len(self.data['completed_pages']):,} pages completed")

                # Load partial projects
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

        # Save checkpoint metadata
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

        # Save projects separately (can be large)
        with open(self.projects_file, "w") as f:
            json.dump(self.projects, f)

    def mark_completed(self, page: int):
        """Mark a page as completed."""
        if page not in self.data["completed_pages"]:
            self.data["completed_pages"].append(page)

    def mark_failed(self, page: int):
        """Mark a page as failed."""
        if page not in self.data["failed_pages"]:
            self.data["failed_pages"].append(page)

    def add_projects(self, projects: list[dict]):
        """Add projects to the partial save."""
        self.projects.extend(projects)

    def get_remaining_pages(self, total_pages: int) -> list[int]:
        """Get list of pages that still need to be downloaded."""
        completed = set(self.data["completed_pages"])
        return [p for p in range(1, total_pages + 1) if p not in completed]

    def set_total_pages(self, total_pages: int):
        """Set total pages in checkpoint."""
        self.data["total_pages"] = total_pages

    def cleanup(self):
        """Remove checkpoint files after successful completion."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.projects_file.exists():
            self.projects_file.unlink()
        print("  [CHECKPOINT] Cleaned up checkpoint files")


# =============================================================================
# API Functions
# =============================================================================

def fetch_page_with_retry(
    page: int,
    session: requests.Session,
    max_retries: int = MAX_RETRIES
) -> tuple[int, list[dict], Optional[str]]:
    """
    Fetch a single page with retry logic.

    Args:
        page: Page number (1-indexed)
        session: Requests session for connection pooling
        max_retries: Maximum retry attempts

    Returns:
        Tuple of (page_number, list of project dicts, error message or None)
    """
    url = f"{GTR_API_BASE}/projects"
    params = {"p": page, "s": PAGE_SIZE}
    last_error = None

    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()

            projects = parse_projects_xml(response.content)
            return (page, projects, None)

        except requests.exceptions.Timeout as e:
            last_error = f"Timeout (attempt {attempt + 1}/{max_retries})"
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error (attempt {attempt + 1}/{max_retries})"
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Rate limited
                wait_time = RETRY_BACKOFF ** (attempt + 2)
                last_error = f"Rate limited, waiting {wait_time:.1f}s"
                time.sleep(wait_time)
            elif response.status_code >= 500:  # Server error
                last_error = f"Server error {response.status_code} (attempt {attempt + 1}/{max_retries})"
            else:
                # Client error, don't retry
                return (page, [], f"HTTP {response.status_code}: {str(e)}")
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"

        # Wait before retry with exponential backoff
        if attempt < max_retries - 1:
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)

    return (page, [], last_error)


def parse_projects_xml(xml_content: bytes) -> list[dict]:
    """
    Parse XML response containing multiple projects.

    Args:
        xml_content: Raw XML bytes from API

    Returns:
        List of project dictionaries
    """
    projects = []

    try:
        root = ET.fromstring(xml_content)

        # Find all project elements (ns2:project)
        for proj in root.findall(".//ns2:project", NS):
            project = parse_single_project(proj)
            if project:
                projects.append(project)

    except ET.ParseError as e:
        print(f"  [WARN] XML parse error: {e}")

    return projects


def parse_single_project(proj: ET.Element) -> Optional[dict]:
    """
    Parse a single project XML element into a dictionary.

    Args:
        proj: XML Element for a single project

    Returns:
        Dictionary with project fields, or None if parsing fails
    """
    # Namespace URIs for attribute access
    NS1 = "{http://gtr.rcuk.ac.uk/gtr/api}"
    NS2 = "{http://gtr.rcuk.ac.uk/gtr/api/project}"

    try:
        # Helper to get text from element
        def get_text(elem: Optional[ET.Element]) -> Optional[str]:
            return elem.text.strip() if elem is not None and elem.text else None

        # Basic project info (ns1 namespace for attributes)
        project_id = proj.get(f"{NS1}id")

        # Content fields use ns2 namespace
        title = get_text(proj.find("ns2:title", NS))
        abstract = get_text(proj.find("ns2:abstractText", NS))
        tech_abstract = get_text(proj.find("ns2:techAbstractText", NS))
        status = get_text(proj.find("ns2:status", NS))
        grant_category = get_text(proj.find("ns2:grantCategory", NS))
        lead_funder = get_text(proj.find("ns2:leadFunder", NS))

        # Grant reference (RCUK identifier)
        grant_reference = None
        identifiers = proj.find("ns2:identifiers", NS)
        if identifiers is not None:
            for ident in identifiers.findall("ns2:identifier", NS):
                if ident.get(f"{NS2}type") == "RCUK":
                    grant_reference = get_text(ident)
                    break

        # Fund details from links (dates are in link attributes)
        amount = None
        start_date = None
        end_date = None
        lead_org_id = None
        pi_id = None

        links = proj.find("ns1:links", NS)
        if links is not None:
            for link in links.findall("ns1:link", NS):
                rel = link.get(f"{NS1}rel")

                if rel == "FUND":
                    # Get dates from fund link attributes
                    start_date = link.get(f"{NS1}start")
                    end_date = link.get(f"{NS1}end")

                elif rel == "LEAD_ORG":
                    href = link.get(f"{NS1}href", "")
                    lead_org_id = href.split("/")[-1] if href else None

                elif rel == "PI_PER":
                    href = link.get(f"{NS1}href", "")
                    pi_id = href.split("/")[-1] if href else None

        # Get amount from participantValues (grantOffer)
        participant_values = proj.find("ns2:participantValues", NS)
        if participant_values is not None:
            # Look for lead participant's grant offer
            for participant in participant_values.findall("ns2:participant", NS):
                role = get_text(participant.find("ns2:role", NS))
                if role == "LEAD_PARTICIPANT":
                    offer_elem = participant.find("ns2:grantOffer", NS)
                    if offer_elem is not None and offer_elem.text:
                        try:
                            amount = float(offer_elem.text)
                        except ValueError:
                            pass
                    break
            # If no lead participant found, try first participant
            if amount is None:
                for participant in participant_values.findall("ns2:participant", NS):
                    offer_elem = participant.find("ns2:grantOffer", NS)
                    if offer_elem is not None and offer_elem.text:
                        try:
                            amount = float(offer_elem.text)
                        except ValueError:
                            pass
                        break

        # Get lead org name from participantValues
        lead_org_name = None
        if participant_values is not None:
            for participant in participant_values.findall("ns2:participant", NS):
                role = get_text(participant.find("ns2:role", NS))
                if role == "LEAD_PARTICIPANT":
                    lead_org_name = get_text(participant.find("ns2:organisationName", NS))
                    break

        return {
            "project_id": project_id,
            "grant_reference": grant_reference,
            "title": title,
            "abstract": abstract or tech_abstract,
            "status": status,
            "grant_category": grant_category,
            "lead_funder": lead_funder,
            "amount": amount,
            "start_date": start_date,
            "end_date": end_date,
            "lead_org_id": lead_org_id,
            "lead_org_name": lead_org_name,
            "pi_id": pi_id,
            "pi_given_name": None,  # Not available in list response, would need separate API call
            "pi_family_name": None,
        }

    except Exception as e:
        # Don't print warnings for individual project parse failures
        return None


def get_total_pages(session: requests.Session) -> int:
    """
    Get total number of pages from API.

    Args:
        session: Requests session

    Returns:
        Total number of pages
    """
    url = f"{GTR_API_BASE}/projects"
    params = {"p": 1, "s": PAGE_SIZE}

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            # totalPages is in ns1 namespace as attribute
            total_pages = int(root.get("{http://gtr.rcuk.ac.uk/gtr/api}totalPages", 0))
            return total_pages
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  [WARN] Failed to get total pages (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                raise RuntimeError(f"Failed to get total pages after {MAX_RETRIES} attempts: {e}")

    return 0


# =============================================================================
# Download Functions
# =============================================================================

def download_all_projects(
    output_dir: Path,
    resume: bool = False,
    max_pages: Optional[int] = None
) -> list[dict]:
    """
    Download all projects from the GtR API with checkpointing.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_pages: Optional limit on pages to download (for testing)

    Returns:
        List of all project dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading projects from GtR API")
    print(f"{'='*60}")

    # Initialize checkpoint manager
    checkpoint = CheckpointManager(output_dir)

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Accept": "application/xml",
        "User-Agent": "OpenAlex-GtR-Ingest/1.0"
    })

    # Get total pages
    print("  [INFO] Fetching total page count...")
    total_pages = get_total_pages(session)

    if max_pages:
        total_pages = min(total_pages, max_pages)
        print(f"  [INFO] Limited to {total_pages:,} pages (--max-pages)")

    print(f"  [INFO] Total pages: {total_pages:,}")
    print(f"  [INFO] Expected projects: ~{total_pages * PAGE_SIZE:,}")

    # Check for existing checkpoint
    if resume and checkpoint.load():
        checkpoint.set_total_pages(total_pages)
        pages_to_fetch = checkpoint.get_remaining_pages(total_pages)
        print(f"  [RESUME] Resuming download: {len(pages_to_fetch):,} pages remaining")
    else:
        pages_to_fetch = list(range(1, total_pages + 1))
        checkpoint.set_total_pages(total_pages)
        checkpoint.projects = []
        print(f"  [INFO] Starting fresh download")

    if not pages_to_fetch:
        print("  [INFO] All pages already downloaded!")
        return checkpoint.projects

    # Initialize progress tracker
    progress = ProgressTracker(total_pages)
    progress.completed_pages = len(checkpoint.data["completed_pages"])
    progress.total_projects = len(checkpoint.projects)

    # Track pages completed in this session for checkpointing
    pages_since_checkpoint = 0
    failed_pages = []

    print(f"\n  Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {progress.get_progress_line()}")

    # Download pages with controlled concurrency
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit pages in batches to control memory
        batch_size = 500

        for batch_start in range(0, len(pages_to_fetch), batch_size):
            batch_pages = pages_to_fetch[batch_start:batch_start + batch_size]

            # Submit batch
            futures = {
                executor.submit(fetch_page_with_retry, page, session): page
                for page in batch_pages
            }

            # Process results as they complete
            for future in as_completed(futures):
                page = futures[future]
                try:
                    page_num, projects, error = future.result()

                    if error:
                        progress.update(0, is_error=True)
                        failed_pages.append((page_num, error))
                        checkpoint.mark_failed(page_num)
                    else:
                        progress.update(len(projects))
                        checkpoint.mark_completed(page_num)
                        checkpoint.add_projects(projects)

                    pages_since_checkpoint += 1

                    # Print progress every 10 seconds
                    if progress.should_report(10):
                        print(f"\r{progress.get_progress_line()}", flush=True)

                    # Save checkpoint periodically
                    if pages_since_checkpoint >= CHECKPOINT_INTERVAL:
                        checkpoint.save()
                        pages_since_checkpoint = 0
                        print(f"\n  [CHECKPOINT] Saved progress: {progress.completed_pages:,} pages, {progress.total_projects:,} projects")

                except Exception as e:
                    progress.update(0, is_error=True)
                    failed_pages.append((page, str(e)))
                    checkpoint.mark_failed(page)

                # Rate limiting
                time.sleep(REQUEST_DELAY)

            # Save checkpoint after each batch
            checkpoint.save()

    # Final progress report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total pages: {progress.completed_pages:,}/{total_pages:,}")
    print(f"  Total projects: {len(checkpoint.projects):,}")
    print(f"  Total time: {progress.get_elapsed()}")
    print(f"  Average rate: {progress.get_rate():.1f} pages/second")

    if failed_pages:
        print(f"\n  [WARN] {len(failed_pages)} pages failed:")
        for page, error in failed_pages[:10]:  # Show first 10
            print(f"    - Page {page}: {error}")
        if len(failed_pages) > 10:
            print(f"    ... and {len(failed_pages) - 10} more")

        # Save failed pages for manual retry
        failed_file = output_dir / "gtr_failed_pages.json"
        with open(failed_file, "w") as f:
            json.dump(failed_pages, f, indent=2)
        print(f"  [INFO] Failed pages saved to {failed_file}")

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

    # Convert to DataFrame
    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    # Ensure PI name columns are string type (they may be inferred as int if all NULL)
    # This is critical for Spark/Databricks compatibility
    for col in ["pi_given_name", "pi_family_name"]:
        if col in df.columns:
            df[col] = df[col].astype("object")  # object = nullable string in pandas

    # Parse dates - convert to strings for Spark/Databricks compatibility
    # (Spark can't read parquet files with nanosecond timestamp precision)
    print("  [INFO] Parsing dates...")
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            # Convert to date strings (YYYY-MM-DD) for Spark compatibility
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace('NaT', None)

    # Clean up grant references
    if "grant_reference" in df.columns:
        df["grant_reference"] = df["grant_reference"].str.strip()

    # Remove duplicates by grant_reference (keep first)
    print("  [INFO] Deduplicating by grant_reference...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["grant_reference"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata - use string format for Spark compatibility
    df["ingested_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Save to parquet
    output_path = output_dir / "gtr_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")

    print(f"\n  Funders:")
    print(df["lead_funder"].value_counts().head(15).to_string())

    print(f"\n  Grant categories:")
    print(df["grant_category"].value_counts().head(10).to_string())

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def find_aws_cli() -> Optional[str]:
    """Find AWS CLI executable path."""
    import shutil

    # Check if aws is in PATH
    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path

    # Check common installation locations
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

    # Find AWS CLI
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
        description="Download GtR projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./gtr_data"),
        help="Directory for downloaded/processed files (default: ./gtr_data)"
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
        "--max-pages",
        type=int,
        default=None,
        help="Limit to N pages (for testing)"
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Gateway to Research (GtR) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    if args.resume:
        print(f"Mode: RESUME (will continue from checkpoint)")

    # Step 1: Download
    if not args.skip_download:
        projects = download_all_projects(
            args.output_dir,
            resume=args.resume,
            max_pages=args.max_pages
        )

        # Clean up checkpoint on successful completion
        if projects:
            checkpoint = CheckpointManager(args.output_dir)
            checkpoint.cleanup()
    else:
        # Load from existing parquet if skipping download
        existing_path = args.output_dir / "gtr_projects.parquet"
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

    # Step 3: Upload to S3
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
    print(f"  In Databricks, run: notebooks/awards/CreateGTRProjectAwards.ipynb")


if __name__ == "__main__":
    main()
