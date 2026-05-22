#!/usr/bin/env python3
"""
USAID Awards to S3 Data Pipeline (Agency for International Development, toptier)
================================

This script downloads USAID grant data from the USAspending.gov API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Scope note:
    This script is for the USAID toptier agency scope. USAspending's canonical
    toptier agency label is "Agency for International Development", while the
    OpenAlex display name is "United States Agency for International
    Development".

CHECK FIRST outcome:
    ForeignAssistance.gov exposes official USAID foreign-assistance activity
    data, but it is organized around implementing agency, country, activity, and
    budget/obligation reporting. It can include non-USAID funding agencies and
    does not provide the same awarding-agency FAIN/recipient scope used by the
    existing federal award ingests. A USAspending FY2024 smoke test on
    2026-05-19 returned real USAID grant/cooperative-agreement awards for
    awarding toptier "Agency for International Development" (for example FHI
    360, GAVI Alliance, and WHO awards). USAspending is therefore the broadest
    working source found for this OpenAlex USAID funder scope.

Data Source: https://api.usaspending.gov/
Output: s3://openalex-ingest/awards/usaid/usaid_awards.parquet

What this script does:
1. Requests bulk downloads from USAspending API for USAID grants (toptier scope) (FY2001-2025)
2. Downloads generated ZIP files containing CSV data
3. Extracts and combines all CSVs
4. Deduplicates by award_id_fain (keeping most recent record)
5. Converts to parquet format with all raw columns stringified per
   plans/awards/how-to-add-a-funder.md
6. Uploads to S3

Award Types:
- 02: Block Grant
- 03: Formula Grant
- 04: Project Grant
- 05: Cooperative Agreement

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/usaid/

Usage:
    python usaid_to_s3.py

    # Resume interrupted download:
    python usaid_to_s3.py --resume

    # Skip upload to S3:
    python usaid_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import os
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

# =============================================================================
# Configuration
# =============================================================================

# USAspending API settings
API_BASE = "https://api.usaspending.gov/api/v2"
BULK_DOWNLOAD_ENDPOINT = f"{API_BASE}/bulk_download/awards/"
STATUS_ENDPOINT = f"{API_BASE}/download/status"

# USAID agency info
USAID_AGENCY_NAME = "Agency for International Development"

# Grant award types
GRANT_TYPES = ["02", "03", "04", "05"]  # Block, Formula, Project, Cooperative

# Fiscal year range
START_YEAR = 2001
END_YEAR = 2025

# API settings
REQUEST_DELAY = 2.0  # Seconds between status checks
MAX_WAIT_TIME = 600  # Max seconds to wait for a download to complete
MAX_RETRIES = 3

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/usaid/usaid_awards.parquet"


# =============================================================================
# API Functions
# =============================================================================

def request_bulk_download(year: int, session: requests.Session) -> dict:
    """
    Request a bulk download for USAID toptier grants in a specific fiscal year.

    Args:
        year: Fiscal year (e.g., 2024)
        session: Requests session

    Returns:
        API response with download info
    """
    # USAspending uses fiscal years (Oct 1 - Sep 30)
    # FY2024 = Oct 1, 2023 - Sep 30, 2024
    start_date = f"{year - 1}-10-01"
    end_date = f"{year}-09-30"

    payload = {
        "filters": {
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": USAID_AGENCY_NAME
                }
            ],
            "prime_award_types": GRANT_TYPES,
            "date_type": "action_date",
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        },
        "file_format": "csv"
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(
                BULK_DOWNLOAD_ENDPOINT,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    [RETRY] Attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
            else:
                raise

    return {}


def wait_for_download(file_name: str, session: requests.Session) -> dict:
    """
    Poll the status endpoint until download is ready.

    Args:
        file_name: Name of the file being generated
        session: Requests session

    Returns:
        Final status response with file URL
    """
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > MAX_WAIT_TIME:
            raise TimeoutError(f"Download timed out after {MAX_WAIT_TIME}s")

        try:
            response = session.get(
                STATUS_ENDPOINT,
                params={"file_name": file_name},
                timeout=30
            )
            response.raise_for_status()
            status = response.json()

            if status.get("status") == "finished":
                return status
            elif status.get("status") == "failed":
                raise RuntimeError(f"Download failed: {status.get('message')}")

            # Still processing
            print(f"    [WAIT] {status.get('status', 'processing')}... ({int(elapsed)}s)", end="\r")
            time.sleep(REQUEST_DELAY)

        except requests.exceptions.RequestException as e:
            print(f"    [WARN] Status check failed: {e}")
            time.sleep(REQUEST_DELAY)


def download_file(url: str, output_path: Path, session: requests.Session) -> bool:
    """
    Download a file from URL.

    Args:
        url: URL to download
        output_path: Local path to save file
        session: Requests session

    Returns:
        True if download succeeded
    """
    try:
        response = session.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    print(f"    [DOWNLOAD] {pct:.1f}%", end="\r")

        print(f"    [DOWNLOAD] Complete ({downloaded / 1024 / 1024:.1f} MB)")
        return True

    except Exception as e:
        print(f"    [ERROR] Download failed: {e}")
        return False


# =============================================================================
# Progress Tracking
# =============================================================================

class ProgressTracker:
    """Track download progress with checkpointing."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.checkpoint_file = output_dir / "usaid_checkpoint.json"
        self.data = {
            "completed_years": [],
            "failed_years": [],
            "total_rows": 0,
            "last_updated": None
        }

    def load(self) -> bool:
        """Load checkpoint from disk."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    self.data = json.load(f)
                print(f"  [CHECKPOINT] Loaded: {len(self.data['completed_years'])} years completed")
                return True
            except Exception as e:
                print(f"  [WARN] Failed to load checkpoint: {e}")
        return False

    def save(self):
        """Save checkpoint to disk."""
        self.data["last_updated"] = datetime.utcnow().isoformat()
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def mark_completed(self, year: int, rows: int):
        """Mark a year as completed."""
        if year not in self.data["completed_years"]:
            self.data["completed_years"].append(year)
        self.data["total_rows"] += rows
        self.save()

    def mark_failed(self, year: int):
        """Mark a year as failed."""
        if year not in self.data["failed_years"]:
            self.data["failed_years"].append(year)
        self.save()

    def get_remaining_years(self) -> list[int]:
        """Get years that still need to be downloaded."""
        completed = set(self.data["completed_years"])
        return [y for y in range(START_YEAR, END_YEAR + 1) if y not in completed]

    def cleanup(self):
        """Remove checkpoint file after successful completion."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            print("  [CHECKPOINT] Cleaned up")


# =============================================================================
# Download Functions
# =============================================================================

def download_year(
    year: int,
    output_dir: Path,
    session: requests.Session
) -> tuple[int, int]:
    """
    Download USAID grants for a single fiscal year.

    Args:
        year: Fiscal year
        output_dir: Directory to save files
        session: Requests session

    Returns:
        Tuple of (year, row_count)
    """
    zip_path = output_dir / f"usaid_fy{year}.zip"

    # Skip if already downloaded
    if zip_path.exists():
        print(f"  [FY{year}] Already downloaded, skipping")
        return (year, 0)

    print(f"  [FY{year}] Requesting bulk download...")

    # Request download
    result = request_bulk_download(year, session)
    file_name = result.get("file_name")

    if not file_name:
        print(f"  [FY{year}] No file_name in response")
        return (year, 0)

    # Wait for download to be ready
    print(f"  [FY{year}] Waiting for download to generate...")
    status = wait_for_download(file_name, session)
    print()  # Clear the wait line

    rows = status.get("total_rows", 0)
    file_url = status.get("file_url")

    if rows == 0:
        print(f"  [FY{year}] No grants found")
        return (year, 0)

    print(f"  [FY{year}] Found {rows:,} transactions, downloading...")

    # Download the file
    if file_url and download_file(file_url, zip_path, session):
        return (year, rows)

    return (year, 0)


def download_all_years(
    output_dir: Path,
    resume: bool = False
) -> list[Path]:
    """
    Download USAID grants for all fiscal years.

    Args:
        output_dir: Directory to save files
        resume: Whether to resume from checkpoint

    Returns:
        List of paths to downloaded zip files
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading USAID grants from USAspending")
    print(f"{'='*60}")
    print(f"  Agency: {USAID_AGENCY_NAME}")
    print(f"  Award types: {GRANT_TYPES}")
    print(f"  Fiscal years: {START_YEAR}-{END_YEAR}")

    # Initialize progress tracker
    tracker = ProgressTracker(output_dir)

    if resume:
        tracker.load()

    years_to_download = tracker.get_remaining_years()

    if not years_to_download:
        print("  [INFO] All years already downloaded!")
    else:
        print(f"  [INFO] Years to download: {len(years_to_download)}")

    # Initialize session
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "User-Agent": "OpenAlex-USAID-Ingest/1.0"
    })

    total_rows = 0
    start_time = time.time()

    for i, year in enumerate(years_to_download):
        print(f"\n  [{i+1}/{len(years_to_download)}] Processing FY{year}...")

        try:
            _, rows = download_year(year, output_dir, session)
            total_rows += rows
            tracker.mark_completed(year, rows)

            # Brief delay between years
            if i < len(years_to_download) - 1:
                time.sleep(1)

        except Exception as e:
            print(f"  [FY{year}] ERROR: {e}")
            tracker.mark_failed(year)

    elapsed = time.time() - start_time
    print(f"\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  Total time: {timedelta(seconds=int(elapsed))}")
    print(f"  Total transactions: {total_rows + tracker.data['total_rows']:,}")

    if tracker.data["failed_years"]:
        print(f"  Failed years: {tracker.data['failed_years']}")

    # Return list of zip files
    return sorted(output_dir.glob("usaid_fy*.zip"))


# =============================================================================
# Processing Functions
# =============================================================================

def extract_and_combine(zip_files: list[Path], output_dir: Path) -> pd.DataFrame:
    """
    Extract zip files and combine CSVs into a single DataFrame.

    Args:
        zip_files: List of zip file paths
        output_dir: Directory for extraction

    Returns:
        Combined DataFrame
    """
    print(f"\n{'='*60}")
    print("Step 2: Extracting and combining data")
    print(f"{'='*60}")

    all_dfs = []

    for zip_path in zip_files:
        print(f"  [EXTRACT] {zip_path.name}...")

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Find CSV files in the zip
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

                for csv_name in csv_files:
                    with zf.open(csv_name) as csv_file:
                        # Read CSV with low_memory=False to avoid dtype warnings
                        df = pd.read_csv(csv_file, low_memory=False, dtype=str)
                        all_dfs.append(df)
                        print(f"    Loaded {len(df):,} rows from {csv_name}")

        except Exception as e:
            print(f"    [ERROR] Failed to extract {zip_path.name}: {e}")

    if not all_dfs:
        raise ValueError("No data extracted from zip files!")

    # Combine all DataFrames
    print(f"\n  [COMBINE] Merging {len(all_dfs)} files...")
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"  Total rows: {len(combined):,}")

    return combined


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process and clean the DataFrame.

    Args:
        df: Raw combined DataFrame

    Returns:
        Processed DataFrame
    """
    print(f"\n{'='*60}")
    print("Step 3: Processing data")
    print(f"{'='*60}")

    # Clean column names (lowercase, replace spaces)
    df.columns = (df.columns
                  .str.lower()
                  .str.replace(' ', '_')
                  .str.replace('-', '_'))

    print(f"  Columns: {len(df.columns)}")

    # Key columns we need:
    # - award_id_fain: Federal Award Identification Number (unique grant ID)
    # - award_description: Title/description
    # - total_obligated_amount: Funding amount
    # - period_of_performance_start_date: Start date
    # - period_of_performance_current_end_date: End date
    # - recipient_name: Awardee organization
    # - primary_place_of_performance_*: Location info
    # - awarding_agency_name, funding_agency_name: Agency info

    # Deduplicate by award_id_fain (keeping most recent action)
    if 'award_id_fain' in df.columns:
        print(f"\n  [DEDUPE] Deduplicating by award_id_fain...")
        original_count = len(df)

        # Sort by action_date descending, then dedupe
        if 'action_date' in df.columns:
            df['action_date'] = pd.to_datetime(df['action_date'], errors='coerce')
            df = df.sort_values('action_date', ascending=False)

        df = df.drop_duplicates(subset=['award_id_fain'], keep='first')
        print(f"  Removed {original_count - len(df):,} duplicates")
        print(f"  Unique awards: {len(df):,}")

    # Convert dates to string format for Spark compatibility
    date_columns = [
        'action_date',
        'period_of_performance_start_date',
        'period_of_performance_current_end_date'
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace('NaT', None)

    # Add ingestion timestamp
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Print summary
    print(f"\n  Summary:")
    print(f"    - Total unique awards: {len(df):,}")

    if 'award_id_fain' in df.columns:
        print(f"    - With FAIN: {df['award_id_fain'].notna().sum():,}")

    if 'award_description' in df.columns:
        print(f"    - With description: {df['award_description'].notna().sum():,}")

    if 'total_obligated_amount' in df.columns:
        amount_for_summary = pd.to_numeric(df['total_obligated_amount'], errors='coerce')
        total_funding = amount_for_summary.sum()
        print(f"    - Total funding: ${total_funding:,.0f}")

    if 'period_of_performance_start_date' in df.columns:
        print(f"    - With start date: {df['period_of_performance_start_date'].notna().sum():,}")

    return df


def save_to_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save DataFrame to parquet file.

    Args:
        df: Processed DataFrame
        output_dir: Output directory

    Returns:
        Path to output file
    """
    output_path = output_dir / "usaid_awards.parquet"

    print(f"\n  [SAVE] Writing to {output_path.name}...")
    # Required by plans/awards/how-to-add-a-funder.md: all source columns string.
    # The Databricks notebook performs award-schema casts with TRY_CAST/TRY_TO_DATE.
    df = df.astype("string")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

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


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """
    Runbook §1.4 — never shrink the corpus on re-ingest. Read the existing
    S3 parquet's row count; if the new dataframe has fewer rows, abort.
    Returns True if it's safe to proceed; False if upload must be aborted.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("  no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        print(f"  [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_usaid_awards.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        import pandas as pd
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"  [ERROR] couldn't read existing parquet ({e}); aborting upload "
              f"to avoid clobbering unknown data. Re-run with --allow-shrink if "
              f"you've verified the previous file is corrupt or empty.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"  previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"  [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(
            f"  [ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Cause is almost always a "
            f"source-side partial outage, schema change, or pagination bug — "
            f"not a genuine retraction. Investigate first; re-run with "
            f"--allow-shrink if confirmed intentional."
        )
        return False
    print("  [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(local_path: Path) -> bool:
    """
    Upload the parquet file to S3.

    Args:
        local_path: Path to local parquet file

    Returns:
        True if upload succeeded
    """
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
        description="Download USAID grants from USAspending and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./usaid_data"),
        help="Directory for downloaded/processed files (default: ./usaid_data)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing files)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional."
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("USAID Awards to S3 Data Pipeline (Agency for International Development, toptier)")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        zip_files = download_all_years(args.output_dir, resume=args.resume)
    else:
        zip_files = sorted(args.output_dir.glob("usaid_fy*.zip"))
        print(f"\n  [SKIP] Using existing files: {len(zip_files)} zip files found")

    if not zip_files:
        print("[ERROR] No zip files found!")
        sys.exit(1)

    # Step 2: Extract and combine
    df = extract_and_combine(zip_files, args.output_dir)

    # Step 3: Process
    df = process_dataframe(df)

    # Save to parquet
    parquet_path = save_to_parquet(df, args.output_dir)

    # Step 4: Upload to S3
    upload_success = True
    if not args.skip_upload:
        if not check_no_shrink(len(df), args.allow_shrink, args.output_dir):
            raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    # Cleanup checkpoint on success
    if upload_success:
        tracker = ProgressTracker(args.output_dir)
        tracker.cleanup()

    print(f"\n{'='*60}")
    if upload_success or args.skip_upload:
        print("Pipeline complete!")
    else:
        print("Pipeline FAILED - S3 upload unsuccessful")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateUSAIDAwards.ipynb")


if __name__ == "__main__":
    main()
