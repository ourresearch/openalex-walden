#!/usr/bin/env python3
"""
NWO (Dutch Research Council) to S3 Data Pipeline
=================================================

This script downloads all Dutch research grant data from the NWOpen API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://nwopen-api.nwo.nl/NWOpen-API/api/Projects
API Docs: https://data.nwo.nl/en/how-to-use-the-nwopen-api
Output: s3://openalex-ingest/awards/nwo/nwo_projects.parquet

What this script does:
1. Fetches all projects from the NWOpen API (paginated JSON)
2. Parses JSON responses to extract project metadata
3. Extracts grant reference, funder, amount, dates, PI, and organization info
4. Captures Products and summary_updates as JSON strings for later parsing
5. Combines into a single DataFrame
6. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved every 100 pages; resume with --resume
- Retry logic: Failed pages are retried up to 3 times with exponential backoff
- ETA reporting: Shows estimated time remaining based on current progress
- Error tracking: Failed pages are logged and can be retried manually

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nwo/

Usage:
    python nwo_to_s3.py

    # Resume interrupted download:
    python nwo_to_s3.py --resume

    # Or with options:
    python nwo_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# NWOpen API settings
NWO_API_BASE = "https://nwopen-api.nwo.nl/NWOpen-API/api/Projects"
PAGE_SIZE = 100  # Results per page
REQUEST_DELAY = 0.1  # Seconds between requests (be polite to API)
MAX_WORKERS = 5  # Parallel page fetches
MAX_RETRIES = 3  # Max retries per page
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N pages

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nwo/nwo_projects.parquet"


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
        self.checkpoint_file = output_dir / "nwo_checkpoint.json"
        self.projects_file = output_dir / "nwo_projects_partial.json"
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
    params = {"page": page, "per_page": PAGE_SIZE}
    last_error = None

    for attempt in range(max_retries):
        try:
            response = session.get(NWO_API_BASE, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            projects = parse_projects_json(data)
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
        except json.JSONDecodeError as e:
            last_error = f"JSON parse error (attempt {attempt + 1}/{max_retries})"
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"

        # Wait before retry with exponential backoff
        if attempt < max_retries - 1:
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)

    return (page, [], last_error)


def parse_projects_json(data: dict) -> list[dict]:
    """
    Parse JSON response containing multiple projects.

    Args:
        data: Parsed JSON response from API

    Returns:
        List of project dictionaries
    """
    projects = []

    # NWOpen API returns projects under the "projects" key
    project_list = data.get("projects", [])

    if isinstance(project_list, list):
        for proj in project_list:
            project = parse_single_project(proj)
            if project:
                projects.append(project)

    return projects


def parse_single_project(proj: dict) -> Optional[dict]:
    """
    Parse a single project JSON object into a dictionary.

    Args:
        proj: Dictionary for a single project

    Returns:
        Dictionary with project fields, or None if parsing fails
    """
    try:
        # Basic project info
        project_id = proj.get("project_id")
        title = proj.get("title")

        # Abstract: prefer English, fallback to Dutch
        abstract = proj.get("summary_en") or proj.get("summary_nl")

        # Dates
        start_date = proj.get("start_date")
        end_date = proj.get("end_date")
        reporting_year = proj.get("reporting_year")

        # Funder/category info
        # funding_scheme contains the specific program name
        grant_category = proj.get("funding_scheme")
        funding_scheme_id = proj.get("funding_scheme_id")

        # department or sub_department contains the NWO division
        lead_funder = proj.get("department") or proj.get("sub_department")

        # Extract PI and organization from project_members
        pi_id = None
        pi_given_name = None
        pi_family_name = None
        lead_org_name = None

        members = proj.get("project_members", [])
        if members:
            # Find main applicant first, then project leader
            main_applicant = None
            for member in members:
                role = member.get("role", "")
                if role == "Main Applicant":
                    main_applicant = member
                    break

            # Fall back to project leader
            if main_applicant is None:
                for member in members:
                    role = member.get("role", "")
                    if role == "Project leader":
                        main_applicant = member
                        break

            # Fall back to first member if nothing found
            if main_applicant is None and members:
                main_applicant = members[0]

            if main_applicant:
                # ORCID - clean up placeholder values
                orcid = main_applicant.get("orcid", "")
                if orcid and orcid != "https://orcid.org/-":
                    pi_id = orcid

                pi_family_name = main_applicant.get("last_name")
                pi_given_name = main_applicant.get("first_name")

                # Organisation may contain hierarchy separated by ||
                org = main_applicant.get("organisation", "")
                if org:
                    # Take the top-level org (first part before ||)
                    lead_org_name = org.split("||")[0].strip()

        # Capture products and summary_updates as JSON strings for later parsing
        products = proj.get("products", [])
        products_json = json.dumps(products) if products else None

        summary_updates = proj.get("summary_updates", [])
        summary_updates_json = json.dumps(summary_updates) if summary_updates else None

        return {
            "project_id": project_id,
            "title": title,
            "abstract": abstract,
            "grant_category": grant_category,
            "funding_scheme_id": funding_scheme_id,
            "lead_funder": lead_funder,
            "start_date": start_date,
            "end_date": end_date,
            "reporting_year": reporting_year,
            "lead_org_name": lead_org_name,
            "pi_id": pi_id,
            "pi_given_name": pi_given_name,
            "pi_family_name": pi_family_name,
            "products_json": products_json,
            "summary_updates_json": summary_updates_json,
        }

    except Exception as e:
        # Don't print warnings for individual project parse failures
        return None


def get_total_pages(session: requests.Session) -> int:
    """
    Get total number of pages from API.

    The NWOpen API returns pagination info in the meta object:
    - meta.pages: total number of pages
    - meta.count: total number of projects
    - meta.per_page: results per page (should match PAGE_SIZE)

    Args:
        session: Requests session

    Returns:
        Total number of pages
    """
    params = {"page": 1, "per_page": PAGE_SIZE}

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(NWO_API_BASE, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()

            # NWOpen API returns pagination in meta object
            meta = data.get("meta", {})
            total_pages = meta.get("pages")
            total_count = meta.get("count")

            if total_pages:
                print(f"  [INFO] API reports {total_count:,} total projects across {total_pages:,} pages")
                return int(total_pages)

            # Fallback: calculate from count
            if total_count:
                pages = (int(total_count) + PAGE_SIZE - 1) // PAGE_SIZE
                print(f"  [INFO] Calculated {pages:,} pages from {total_count:,} projects")
                return pages

            # Last resort fallback
            print("  [WARN] Could not determine total pages from API response")
            print("  [INFO] Will download until we get an empty response")
            return 10000

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
    Download all projects from the NWOpen API with checkpointing.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_pages: Optional limit on pages to download (for testing)

    Returns:
        List of all project dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading projects from NWOpen API")
    print(f"{'='*60}")

    # Initialize checkpoint manager
    checkpoint = CheckpointManager(output_dir)

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenAlex-NWO-Ingest/1.0"
    })

    # Get total pages (or estimate)
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
    empty_page_streak = 0  # Track consecutive empty pages to detect end

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
                        empty_page_streak = 0
                    elif not projects:
                        # Empty page - might be end of data
                        progress.update(0)
                        checkpoint.mark_completed(page_num)
                        empty_page_streak += 1

                        # If we get 5 consecutive empty pages, we've hit the end
                        if empty_page_streak >= 5:
                            print(f"\n  [INFO] Detected end of data (5 consecutive empty pages)")
                            # Update total pages to current progress
                            progress.total_pages = progress.completed_pages
                            checkpoint.set_total_pages(progress.completed_pages)
                    else:
                        progress.update(len(projects))
                        checkpoint.mark_completed(page_num)
                        checkpoint.add_projects(projects)
                        empty_page_streak = 0

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

            # Stop if we've hit end of data
            if empty_page_streak >= 5:
                break

            # Save checkpoint after each batch
            checkpoint.save()

    # Final progress report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total pages: {progress.completed_pages:,}/{progress.total_pages:,}")
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
        failed_file = output_dir / "nwo_failed_pages.json"
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

    # Convert dates to string format (YYYY-MM-DD) to avoid Spark compatibility issues
    # Spark can't read pandas timestamp columns with nanosecond precision
    print("  [INFO] Converting dates to string format...")
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            # Parse and format as YYYY-MM-DD string
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            # Replace 'NaT' strings with None
            df[col] = df[col].replace("NaT", None)

    # Remove duplicates by project_id (keep first)
    print("  [INFO] Deduplicating by project_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata (as string to avoid Spark timestamp issues)
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "nwo_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With ORCID: {df['pi_id'].notna().sum():,}")
    print(f"    - With products: {df['products_json'].notna().sum():,}")
    print(f"    - With summary updates: {df['summary_updates_json'].notna().sum():,}")

    if "lead_funder" in df.columns:
        print(f"\n  Funders/Departments:")
        print(df["lead_funder"].value_counts().head(15).to_string())

    if "grant_category" in df.columns:
        print(f"\n  Grant categories/Funding schemes:")
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
        description="Download NWO projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./nwo_data"),
        help="Directory for downloaded/processed files (default: ./nwo_data)"
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
    print("NWO (Dutch Research Council) to S3 Data Pipeline")
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
        existing_path = args.output_dir / "nwo_projects.parquet"
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
    print(f"  In Databricks, run: notebooks/awards/CreateNWOAwards.ipynb")


if __name__ == "__main__":
    main()
