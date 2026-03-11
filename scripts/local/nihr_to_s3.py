#!/usr/bin/env python3
"""
NIHR (National Institute for Health and Care Research) to S3 Data Pipeline
===========================================================================

This script downloads all UK NIHR research grant data from the NIHR Open Data API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://nihr.opendatasoft.com/explore/dataset/nihr-summary-view/
API Docs: https://help.opendatasoft.com/apis/ods-explore-v2/
Output: s3://openalex-ingest/awards/nihr/nihr_projects.parquet

What this script does:
1. Fetches all projects from the NIHR OpenDataSoft API (paginated JSON)
2. Parses JSON responses to extract project metadata
3. Extracts award ID, funder, amount, dates, PI, and organization info
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Features:
- Checkpointing: Progress is saved periodically; resume with --resume
- Retry logic: Failed requests are retried up to 3 times with exponential backoff
- ETA reporting: Shows estimated time remaining based on current progress

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nihr/

Usage:
    python nihr_to_s3.py

    # Resume interrupted download:
    python nihr_to_s3.py --resume

    # Or with options:
    python nihr_to_s3.py --output-dir /path/to/output --skip-upload

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

# NIHR OpenDataSoft API settings
# Using nihr-summary-view which has consolidated data including PI info
NIHR_API_BASE = "https://nihr.opendatasoft.com/api/explore/v2.1/catalog/datasets/nihr-summary-view/records"
PAGE_SIZE = 100  # Results per page (max 100 for OpenDataSoft)
REQUEST_DELAY = 0.2  # Seconds between requests (be polite to API)
MAX_RETRIES = 3  # Max retries per request
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 1000  # Save checkpoint every N records

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nihr/nihr_projects.parquet"


# =============================================================================
# Progress Tracker
# =============================================================================

class ProgressTracker:
    """Track download progress with ETA calculation."""

    def __init__(self, total_records: int):
        self.total_records = total_records
        self.completed_records = 0
        self.errors = 0
        self.start_time = time.time()
        self.last_report_time = time.time()

    def update(self, records_count: int, is_error: bool = False):
        """Update progress counters."""
        self.completed_records += records_count
        if is_error:
            self.errors += 1

    def get_eta(self) -> str:
        """Calculate and format ETA."""
        if self.completed_records == 0:
            return "calculating..."

        elapsed = time.time() - self.start_time
        records_per_second = self.completed_records / elapsed
        remaining_records = self.total_records - self.completed_records

        if records_per_second > 0:
            remaining_seconds = remaining_records / records_per_second
            eta = timedelta(seconds=int(remaining_seconds))
            return str(eta)
        return "unknown"

    def get_elapsed(self) -> str:
        """Format elapsed time."""
        elapsed = time.time() - self.start_time
        return str(timedelta(seconds=int(elapsed)))

    def get_rate(self) -> float:
        """Get records per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.completed_records / elapsed
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
        pct = (self.completed_records / self.total_records) * 100 if self.total_records > 0 else 0
        rate = self.get_rate()
        return (
            f"  [{self.completed_records:,}/{self.total_records:,} records ({pct:.1f}%)] "
            f"[{rate:.1f} rec/s] "
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
        self.checkpoint_file = output_dir / "nihr_checkpoint.json"
        self.projects_file = output_dir / "nihr_projects_partial.json"
        self.data = {
            "offset": 0,
            "total_records": 0,
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
                print(f"  [CHECKPOINT] Loaded checkpoint: offset {self.data['offset']:,}")

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

    def set_offset(self, offset: int):
        """Set current offset."""
        self.data["offset"] = offset

    def get_offset(self) -> int:
        """Get current offset."""
        return self.data["offset"]

    def set_total_records(self, total: int):
        """Set total records."""
        self.data["total_records"] = total

    def add_projects(self, projects: list[dict]):
        """Add projects to the partial save."""
        self.projects.extend(projects)

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

def fetch_records_with_retry(
    offset: int,
    session: requests.Session,
    max_retries: int = MAX_RETRIES
) -> tuple[list[dict], int, Optional[str]]:
    """
    Fetch a batch of records with retry logic.

    Args:
        offset: Offset for pagination
        session: Requests session for connection pooling
        max_retries: Maximum retry attempts

    Returns:
        Tuple of (list of record dicts, total_count, error message or None)
    """
    params = {
        "offset": offset,
        "limit": PAGE_SIZE,
        "timezone": "UTC"
    }
    last_error = None

    for attempt in range(max_retries):
        try:
            response = session.get(NIHR_API_BASE, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            total_count = data.get("total_count", 0)
            results = data.get("results", [])

            projects = [parse_single_record(r) for r in results]
            projects = [p for p in projects if p is not None]

            return (projects, total_count, None)

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
                return ([], 0, f"HTTP {response.status_code}: {str(e)}")
        except json.JSONDecodeError as e:
            last_error = f"JSON parse error (attempt {attempt + 1}/{max_retries})"
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"

        # Wait before retry with exponential backoff
        if attempt < max_retries - 1:
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)

    return ([], 0, last_error)


def parse_single_record(record: dict) -> Optional[dict]:
    """
    Parse a single record from the NIHR API into a dictionary.

    Args:
        record: Dictionary for a single award record

    Returns:
        Dictionary with project fields, or None if parsing fails
    """
    try:
        # Basic project info
        project_id = record.get("project_id")
        project_title = record.get("project_title")
        acronym = record.get("acronym")

        # Abstracts
        plain_english_abstract = record.get("plain_english_abstract")
        scientific_abstract = record.get("scientific_abstract")

        # Funding info
        funder = record.get("funder")
        award_amount = record.get("award_amount_from_dh")
        award_amount_m = record.get("award_amount_m")
        funding_stream = record.get("funding_stream")
        programme = record.get("programme")
        programme_type = record.get("programme_type")
        programme_stream = record.get("programme_stream")

        # Dates and status
        start_date = record.get("start_date")
        end_date = record.get("end_date")
        project_status = record.get("project_status")

        # Organization
        contracted_organisation = record.get("contracted_organisation")
        organisation_type = record.get("organisation_type")
        institution_country = record.get("institutioncountry")

        # PI info (from consolidated view)
        award_holder_name = record.get("award_holder_name")
        involvement_type = record.get("involvement_type")
        orcid = record.get("orcid")

        # Clean up ORCID - handle "Not Provided" etc
        if orcid and orcid.lower() in ["not provided", "none", "null", ""]:
            orcid = None

        # Parse award holder name into given/family
        pi_given_name = None
        pi_family_name = None
        if award_holder_name:
            # Try to parse "Dr John Smith" or "Smith, John" formats
            name = award_holder_name.strip()
            # Remove common titles
            for title in ["Dr ", "Prof ", "Professor ", "Mr ", "Mrs ", "Ms ", "Miss "]:
                if name.startswith(title):
                    name = name[len(title):]
                    break

            if ", " in name:
                # "Smith, John" format
                parts = name.split(", ", 1)
                pi_family_name = parts[0].strip()
                pi_given_name = parts[1].strip() if len(parts) > 1 else None
            elif " " in name:
                # "John Smith" format - assume last word is family name
                parts = name.rsplit(" ", 1)
                pi_given_name = parts[0].strip()
                pi_family_name = parts[1].strip() if len(parts) > 1 else name

        # Geographic data
        postcode = record.get("postcode")
        latitude = record.get("latitude")
        longitude = record.get("longitude")

        # Research classification
        hrcs_rac_category = record.get("hrcs_rac_category")
        ukcrc_value = record.get("ukcrc_value")

        # Link
        funding_and_awards_link = record.get("funding_and_awards_link")

        return {
            "project_id": project_id,
            "title": project_title,
            "acronym": acronym,
            "plain_english_abstract": plain_english_abstract,
            "scientific_abstract": scientific_abstract,
            "funder": funder,
            "award_amount": award_amount,
            "award_amount_m": award_amount_m,
            "funding_stream": funding_stream,
            "programme": programme,
            "programme_type": programme_type,
            "programme_stream": programme_stream,
            "start_date": start_date,
            "end_date": end_date,
            "project_status": project_status,
            "contracted_organisation": contracted_organisation,
            "organisation_type": organisation_type,
            "institution_country": institution_country,
            "award_holder_name": award_holder_name,
            "involvement_type": involvement_type,
            "pi_given_name": pi_given_name,
            "pi_family_name": pi_family_name,
            "orcid": orcid,
            "postcode": postcode,
            "latitude": latitude,
            "longitude": longitude,
            "hrcs_rac_category": hrcs_rac_category,
            "ukcrc_value": ukcrc_value,
            "landing_page_url": funding_and_awards_link,
        }

    except Exception as e:
        # Don't print warnings for individual record parse failures
        return None


def get_total_count(session: requests.Session) -> int:
    """
    Get total number of records from API.

    Args:
        session: Requests session

    Returns:
        Total number of records
    """
    params = {"offset": 0, "limit": 0}

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(NIHR_API_BASE, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            total_count = data.get("total_count", 0)

            if total_count:
                print(f"  [INFO] API reports {total_count:,} total records")
                return int(total_count)

            print("  [WARN] Could not determine total count from API response")
            return 0

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  [WARN] Failed to get total count (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                raise RuntimeError(f"Failed to get total count after {MAX_RETRIES} attempts: {e}")

    return 0


# =============================================================================
# Download Functions
# =============================================================================

def download_all_projects(
    output_dir: Path,
    resume: bool = False,
    max_records: Optional[int] = None
) -> list[dict]:
    """
    Download all projects from the NIHR API with checkpointing.

    Args:
        output_dir: Directory for intermediate files
        resume: Whether to resume from checkpoint
        max_records: Optional limit on records to download (for testing)

    Returns:
        List of all project dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading projects from NIHR Open Data API")
    print(f"{'='*60}")

    # Initialize checkpoint manager
    checkpoint = CheckpointManager(output_dir)

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenAlex-NIHR-Ingest/1.0"
    })

    # Get total count
    print("  [INFO] Fetching total record count...")
    total_count = get_total_count(session)

    if max_records:
        total_count = min(total_count, max_records)
        print(f"  [INFO] Limited to {total_count:,} records (--max-records)")

    # Check for existing checkpoint
    starting_offset = 0
    if resume and checkpoint.load():
        starting_offset = checkpoint.get_offset()
        checkpoint.set_total_records(total_count)
        print(f"  [RESUME] Resuming from offset {starting_offset:,}")
    else:
        checkpoint.set_total_records(total_count)
        checkpoint.projects = []
        print(f"  [INFO] Starting fresh download")

    if starting_offset >= total_count:
        print("  [INFO] All records already downloaded!")
        return checkpoint.projects

    # Initialize progress tracker
    progress = ProgressTracker(total_count)
    progress.completed_records = len(checkpoint.projects)

    records_since_checkpoint = 0

    print(f"\n  Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {progress.get_progress_line()}")

    # Download in batches
    offset = starting_offset
    while offset < total_count:
        projects, api_total, error = fetch_records_with_retry(offset, session)

        if error:
            progress.update(0, is_error=True)
            print(f"\n  [ERROR] Offset {offset}: {error}")
            # Continue to next batch
            offset += PAGE_SIZE
            continue

        if not projects:
            # Empty response, might be end of data
            print(f"\n  [INFO] Empty response at offset {offset}, stopping")
            break

        progress.update(len(projects))
        checkpoint.add_projects(projects)
        checkpoint.set_offset(offset + PAGE_SIZE)

        records_since_checkpoint += len(projects)

        # Print progress every 10 seconds
        if progress.should_report(10):
            print(f"\r{progress.get_progress_line()}", flush=True)

        # Save checkpoint periodically
        if records_since_checkpoint >= CHECKPOINT_INTERVAL:
            checkpoint.save()
            records_since_checkpoint = 0
            print(f"\n  [CHECKPOINT] Saved progress: {progress.completed_records:,} records")

        offset += PAGE_SIZE

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Final save
    checkpoint.save()

    # Final progress report
    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total records: {len(checkpoint.projects):,}")
    print(f"  Total time: {progress.get_elapsed()}")
    print(f"  Average rate: {progress.get_rate():.1f} records/second")

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
    output_path = output_dir / "nihr_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With PI name: {df['award_holder_name'].notna().sum():,}")
    print(f"    - With ORCID: {df['orcid'].notna().sum():,}")
    print(f"    - With amount: {df['award_amount'].notna().sum():,}")

    if "programme" in df.columns:
        print(f"\n  Programmes (top 15):")
        print(df["programme"].value_counts().head(15).to_string())

    if "project_status" in df.columns:
        print(f"\n  Project status:")
        print(df["project_status"].value_counts().to_string())

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
        description="Download NIHR projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./nihr_data"),
        help="Directory for downloaded/processed files (default: ./nihr_data)"
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
        "--max-records",
        type=int,
        default=None,
        help="Limit to N records (for testing)"
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("NIHR (National Institute for Health and Care Research) to S3 Pipeline")
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
            max_records=args.max_records
        )

        # Clean up checkpoint on successful completion
        if projects:
            checkpoint = CheckpointManager(args.output_dir)
            checkpoint.cleanup()
    else:
        # Load from existing parquet if skipping download
        existing_path = args.output_dir / "nihr_projects.parquet"
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
    print(f"  In Databricks, run: notebooks/awards/CreateNIHRAwards.ipynb")


if __name__ == "__main__":
    main()