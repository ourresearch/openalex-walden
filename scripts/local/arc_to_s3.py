#!/usr/bin/env python3
"""
ARC (Australian Research Council) to S3 Data Pipeline
======================================================

This script downloads all Australian research grant data from the ARC Data Portal API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://dataportal.arc.gov.au/RGS/API/grants
Portal: https://dataportal.arc.gov.au/RGS/Web/Grants
Output: s3://openalex-ingest/awards/arc/arc_projects.parquet

What this script does:
1. Fetches all grants from the ARC RGS API (paginated JSON:API)
2. Parses JSON responses to extract grant metadata
3. Extracts grant code, title, summary, PI, funding amount, dates, institution info
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved every 5 pages; resume with --resume
- Retry logic: Failed pages are retried up to 3 times with exponential backoff
- ETA reporting: Shows estimated time remaining based on current progress
- Error tracking: Failed pages are logged and can be retried manually

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/arc/

Usage:
    python arc_to_s3.py

    # Resume interrupted download:
    python arc_to_s3.py --resume

    # Or with options:
    python arc_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# ARC API settings - uses JSON:API format
ARC_API_BASE = "https://dataportal.arc.gov.au/RGS/API/grants"
PAGE_SIZE = 1000  # Max allowed by API
REQUEST_DELAY = 0.5  # Seconds between requests (be polite to API)
MAX_RETRIES = 3  # Max retries per page
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 5  # Save checkpoint every N pages

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/arc/arc_projects.parquet"


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
            f"[{self.total_projects:,} grants] "
            f"[{rate:.2f} pages/s] "
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
        self.checkpoint_file = output_dir / "arc_checkpoint.json"
        self.projects_file = output_dir / "arc_projects_partial.json"
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
                    print(f"  [CHECKPOINT] Loaded {len(self.projects):,} grants from partial save")

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

    def add_projects(self, projects: list):
        """Add projects to the partial save."""
        self.projects.extend(projects)

    def get_remaining_pages(self, total_pages: int) -> list:
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

def build_url(page: int, page_size: int = PAGE_SIZE) -> str:
    """Build API URL with proper parameter encoding."""
    # JSON:API uses bracket notation for pagination
    params = {
        "page[number]": page,
        "page[size]": page_size
    }
    return f"{ARC_API_BASE}?{urlencode(params)}"


def fetch_page_with_retry(
    page: int,
    session: requests.Session,
    max_retries: int = MAX_RETRIES
) -> tuple:
    """
    Fetch a single page with retry logic.

    Args:
        page: Page number (1-indexed)
        session: Requests session for connection pooling
        max_retries: Maximum retry attempts

    Returns:
        Tuple of (page_number, list of grant dicts, error message or None)
    """
    url = build_url(page)
    last_error = None

    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=120)
            response.raise_for_status()

            data = response.json()
            grants = parse_grants_response(data)
            return (page, grants, None)

        except requests.exceptions.Timeout:
            last_error = f"Timeout (attempt {attempt + 1}/{max_retries})"
        except requests.exceptions.ConnectionError:
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
        except json.JSONDecodeError:
            last_error = f"JSON parse error (attempt {attempt + 1}/{max_retries})"
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"

        # Wait before retry with exponential backoff
        if attempt < max_retries - 1:
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)

    return (page, [], last_error)


def parse_grants_response(data: dict) -> list:
    """
    Parse JSON:API response containing multiple grants.

    Args:
        data: Parsed JSON response from API

    Returns:
        List of grant dictionaries
    """
    grants = []

    # JSON:API returns data under the "data" key
    grant_list = data.get("data", [])

    for item in grant_list:
        grant = parse_single_grant(item)
        if grant:
            grants.append(grant)

    return grants


def parse_single_grant(item: dict) -> Optional[dict]:
    """
    Parse a single grant JSON:API object into a dictionary.

    Args:
        item: Dictionary for a single grant (JSON:API format)

    Returns:
        Dictionary with grant fields, or None if parsing fails
    """
    try:
        # JSON:API format: id at top level, fields in attributes
        grant_id = item.get("id")
        attrs = item.get("attributes", {})

        # Basic grant info
        code = attrs.get("code")
        title = attrs.get("grant-title")
        summary = attrs.get("grant-summary")

        # Dates
        start_year = attrs.get("funding-commencement-year")
        end_date = attrs.get("anticipated-end-date")

        # Funding info
        grant_value = attrs.get("grant-value")
        grant_status = attrs.get("grant-status")

        # Funder and scheme info
        grant_funder = attrs.get("grant-funder")
        program_name = attrs.get("program-name")
        scheme_name = attrs.get("scheme-name")

        # Scheme information (nested dict)
        scheme_info = attrs.get("scheme-information", {})
        scheme_code = scheme_info.get("scheme-code", "").strip() if scheme_info else None
        scheme_round = scheme_info.get("scheme-round") if scheme_info else None
        submission_year = scheme_info.get("submission-year") if scheme_info else None

        # Research field
        primary_for = attrs.get("primary-field-of-research")

        # PI and investigators
        lead_investigator = attrs.get("lead-investigator")
        investigators = attrs.get("investigators")

        # Institution
        grantee = attrs.get("grantee")

        # Grant priorities (usually empty but capture anyway)
        grant_priorities = attrs.get("grant-priorities", [])
        priorities_json = json.dumps(grant_priorities) if grant_priorities else None

        return {
            "grant_id": grant_id,
            "code": code,
            "title": title,
            "summary": summary,
            "start_year": start_year,
            "end_date": end_date,
            "grant_value": grant_value,
            "grant_status": grant_status,
            "grant_funder": grant_funder,
            "program_name": program_name,
            "scheme_name": scheme_name,
            "scheme_code": scheme_code,
            "scheme_round": scheme_round,
            "submission_year": submission_year,
            "primary_field_of_research": primary_for,
            "lead_investigator": lead_investigator,
            "investigators": investigators,
            "grantee": grantee,
            "grant_priorities_json": priorities_json,
        }

    except Exception as e:
        # Don't print warnings for individual parse failures
        return None


def get_total_pages(session: requests.Session) -> tuple:
    """
    Get total number of pages and grants from API.

    The ARC API returns pagination info in the meta object:
    - meta.total-pages: total number of pages
    - meta.total-size: total number of grants

    Args:
        session: Requests session

    Returns:
        Tuple of (total_pages, total_grants)
    """
    url = build_url(1, PAGE_SIZE)

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=120)
            response.raise_for_status()

            data = response.json()

            # ARC API returns pagination in meta object
            meta = data.get("meta", {})
            total_pages = meta.get("total-pages")
            total_size = meta.get("total-size")

            if total_pages and total_size:
                print(f"  [INFO] API reports {total_size:,} total grants across {total_pages:,} pages")
                return (int(total_pages), int(total_size))

            # Fallback: calculate from count
            if total_size:
                pages = (int(total_size) + PAGE_SIZE - 1) // PAGE_SIZE
                print(f"  [INFO] Calculated {pages:,} pages from {total_size:,} grants")
                return (pages, int(total_size))

            # Last resort fallback
            print("  [WARN] Could not determine total pages from API response")
            return (100, 0)  # Conservative estimate

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  [WARN] Failed to get total pages (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                raise RuntimeError(f"Failed to get total pages after {MAX_RETRIES} attempts: {e}")

    return (0, 0)


# =============================================================================
# Download Functions
# =============================================================================

def download_all_grants(
    output_dir: Path,
    resume: bool = False,
    max_pages: Optional[int] = None
) -> list:
    """
    Download all grants from the ARC API with checkpointing.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_pages: Optional limit on pages to download (for testing)

    Returns:
        List of all grant dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading grants from ARC API")
    print(f"{'='*60}")

    # Initialize checkpoint manager
    checkpoint = CheckpointManager(output_dir)

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenAlex-ARC-Ingest/1.0"
    })

    # Get total pages
    print("  [INFO] Fetching total page count...")
    total_pages, total_grants = get_total_pages(session)

    if max_pages:
        total_pages = min(total_pages, max_pages)
        print(f"  [INFO] Limited to {total_pages:,} pages (--max-pages)")

    print(f"  [INFO] Total pages: {total_pages:,}")
    print(f"  [INFO] Expected grants: {total_grants:,}")

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

    # Download pages sequentially (API seems to prefer this)
    for page in pages_to_fetch:
        page_num, grants, error = fetch_page_with_retry(page, session)

        if error:
            progress.update(0, is_error=True)
            failed_pages.append((page_num, error))
            checkpoint.mark_failed(page_num)
            print(f"\n  [ERROR] Page {page_num}: {error}")
        else:
            progress.update(len(grants))
            checkpoint.mark_completed(page_num)
            checkpoint.add_projects(grants)

        pages_since_checkpoint += 1

        # Print progress every 10 seconds
        if progress.should_report(10):
            print(f"\r{progress.get_progress_line()}", flush=True)

        # Save checkpoint periodically
        if pages_since_checkpoint >= CHECKPOINT_INTERVAL:
            checkpoint.save()
            pages_since_checkpoint = 0
            print(f"\n  [CHECKPOINT] Saved progress: {progress.completed_pages:,} pages, {progress.total_projects:,} grants")

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Final checkpoint save
    checkpoint.save()

    # Final progress report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total pages: {progress.completed_pages:,}/{progress.total_pages:,}")
    print(f"  Total grants: {len(checkpoint.projects):,}")
    print(f"  Total time: {progress.get_elapsed()}")
    print(f"  Average rate: {progress.get_rate():.2f} pages/second")

    if failed_pages:
        print(f"\n  [WARN] {len(failed_pages)} pages failed:")
        for page, error in failed_pages[:10]:  # Show first 10
            print(f"    - Page {page}: {error}")
        if len(failed_pages) > 10:
            print(f"    ... and {len(failed_pages) - 10} more")

        # Save failed pages for manual retry
        failed_file = output_dir / "arc_failed_pages.json"
        with open(failed_file, "w") as f:
            json.dump(failed_pages, f, indent=2)
        print(f"  [INFO] Failed pages saved to {failed_file}")

    return checkpoint.projects


# =============================================================================
# Processing Functions
# =============================================================================

def process_grants(grants: list, output_dir: Path) -> Path:
    """
    Process grants into a parquet file.

    Args:
        grants: List of grant dictionaries
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing grants")
    print(f"{'='*60}")

    if not grants:
        raise ValueError("No grants to process!")

    # Convert to DataFrame
    df = pd.DataFrame(grants)
    print(f"  Total rows: {len(df):,}")

    # Convert dates to string format (YYYY-MM-DD) to avoid Spark compatibility issues
    print("  [INFO] Converting dates to string format...")
    if "end_date" in df.columns:
        # end_date is already in YYYY-MM-DD format from API
        df["end_date"] = df["end_date"].astype(str).replace("None", None)

    # Remove duplicates by grant_id (keep first)
    print("  [INFO] Deduplicating by grant_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["grant_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata (as string to avoid Spark timestamp issues)
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "arc_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With summary: {df['summary'].notna().sum():,}")
    print(f"    - With start year: {df['start_year'].notna().sum():,}")
    print(f"    - With grant value: {df['grant_value'].notna().sum():,}")
    print(f"    - With lead investigator: {df['lead_investigator'].notna().sum():,}")

    if "program_name" in df.columns:
        print(f"\n  Programs:")
        print(df["program_name"].value_counts().head(10).to_string())

    if "scheme_name" in df.columns:
        print(f"\n  Schemes (top 10):")
        print(df["scheme_name"].value_counts().head(10).to_string())

    if "start_year" in df.columns:
        print(f"\n  Years (most recent):")
        print(df["start_year"].value_counts().head(10).sort_index(ascending=False).to_string())

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
        description="Download ARC grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./arc_data"),
        help="Directory for downloaded/processed files (default: ./arc_data)"
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
    print("ARC (Australian Research Council) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    if args.resume:
        print(f"Mode: RESUME (will continue from checkpoint)")

    # Step 1: Download
    if not args.skip_download:
        grants = download_all_grants(
            args.output_dir,
            resume=args.resume,
            max_pages=args.max_pages
        )

        # Clean up checkpoint on successful completion
        if grants:
            checkpoint = CheckpointManager(args.output_dir)
            checkpoint.cleanup()
    else:
        # Load from existing parquet if skipping download
        existing_path = args.output_dir / "arc_projects.parquet"
        if existing_path.exists():
            print(f"\n  [SKIP] Loading existing data from {existing_path}")
            df = pd.read_parquet(existing_path)
            grants = df.to_dict("records")
        else:
            print("[ERROR] No existing data found. Run without --skip-download")
            sys.exit(1)

    if not grants:
        print("[ERROR] No grants downloaded!")
        sys.exit(1)

    # Step 2: Process
    parquet_path = process_grants(grants, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateARCAwards.ipynb")


if __name__ == "__main__":
    main()
