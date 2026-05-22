#!/usr/bin/env python3
"""
CFI (Canada Foundation for Innovation) to S3 Data Pipeline
===========================================================

This script downloads all funded projects from the CFI website,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://www.innovation.ca/projects-results/funded-projects-dashboard
Output: s3://openalex-ingest/awards/cfi/cfi_projects.parquet

What this script does:
1. Downloads the complete funded projects Excel file from CFI
2. Processes and cleans the data
3. Converts to parquet format
4. Uploads to S3

Notes:
- CFI data includes ~14K projects since 1997
- Team Leaders field may contain multiple names separated by "|"
- No abstract/description available in the data
- Currency is CAD

Requirements:
    pip install pandas pyarrow openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/cfi/

Usage:
    python cfi_to_s3.py

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

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

# CFI Excel file URL - this URL pattern may change when they update the file
EXCEL_URL = "https://www.innovation.ca/sites/default/files/2025-10/Funded%20projects%20dataset%20EN%202025-10-10.xlsx"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cfi/cfi_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_excel(url: str, output_path: Path, timeout: int = 60) -> bool:
    """
    Download the Excel file from CFI.

    Args:
        url: URL to download
        output_path: Path to save the file
        timeout: Timeout in seconds

    Returns:
        True if download succeeded
    """
    print(f"  [DOWNLOAD] Fetching Excel file...", flush=True)
    start_time = time.time()

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (OpenAlex Data Ingestion)'}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read()
            elapsed = time.time() - start_time

            with open(output_path, 'wb') as f:
                f.write(content)

            size_mb = len(content) / (1024 * 1024)
            print(f"  [SUCCESS] Downloaded {size_mb:.1f} MB in {elapsed:.1f}s", flush=True)
            return True

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  [ERROR] Download failed after {elapsed:.1f}s: {e}", flush=True)
        return False


# =============================================================================
# Processing Functions
# =============================================================================

def process_data(excel_path: Path, output_dir: Path) -> Path:
    """
    Process the Excel file into a parquet file.

    Args:
        excel_path: Path to Excel file
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 2: Processing data", flush=True)
    print(f"{'='*60}", flush=True)

    # Read Excel
    print("  [INFO] Reading Excel file...", flush=True)
    df = pd.read_excel(excel_path, sheet_name=0, dtype=str)
    print(f"  Total rows: {len(df):,}", flush=True)

    # Standardize column names
    print("  [INFO] Standardizing columns...", flush=True)
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace('-', '_'))

    print(f"  Columns: {list(df.columns)}", flush=True)

    # Parse amounts
    print("  [INFO] Parsing amounts...", flush=True)
    if 'cfi_contribution' in df.columns:
        df['cfi_contribution'] = pd.to_numeric(df['cfi_contribution'], errors='coerce')

    # Parse dates
    print("  [INFO] Parsing dates...", flush=True)
    if 'decision_date' in df.columns:
        df['decision_date'] = pd.to_datetime(df['decision_date'], errors='coerce')
        df['decision_date'] = df['decision_date'].dt.strftime('%Y-%m-%d')

    if 'decision_year' in df.columns:
        df['decision_year'] = pd.to_numeric(df['decision_year'], errors='coerce')

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Save to parquet
    print("\n  [SAVE] Writing to parquet...", flush=True)
    output_path = output_dir / "cfi_projects.parquet"
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB", flush=True)

    # Print summary stats
    print(f"\n  Summary:", flush=True)
    print(f"    - Total projects: {len(df):,}", flush=True)

    if 'project_title' in df.columns:
        print(f"    - With title: {df['project_title'].notna().sum():,}", flush=True)

    if 'team_leaders' in df.columns:
        print(f"    - With team leader: {df['team_leaders'].notna().sum():,}", flush=True)

    if 'cfi_contribution' in df.columns:
        print(f"    - With amount: {df['cfi_contribution'].notna().sum():,}", flush=True)
        total_amount = df['cfi_contribution'].sum()
        if pd.notna(total_amount):
            print(f"    - Total CFI contribution: ${total_amount:,.0f} CAD", flush=True)

    if 'lead_institution' in df.columns:
        print(f"\n  Top institutions:", flush=True)
        print(df['lead_institution'].value_counts().head(10).to_string(), flush=True)

    if 'fund_type' in df.columns:
        print(f"\n  Top fund types:", flush=True)
        print(df['fund_type'].value_counts().head(10).to_string(), flush=True)

    if 'decision_year' in df.columns:
        print(f"\n  Year distribution (recent):", flush=True)
        year_counts = df['decision_year'].value_counts().sort_index(ascending=False).head(10)
        print(year_counts.to_string(), flush=True)

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
    print(f"\n{'='*60}", flush=True)
    print("Step 3: Uploading to S3", flush=True)
    print(f"{'='*60}", flush=True)

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}", flush=True)

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli", flush=True)
        return False

    print(f"  [INFO] Using AWS CLI: {aws_cmd}", flush=True)

    try:
        result = subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print("  [SUCCESS] Upload complete!", flush=True)
        return True

    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}", flush=True)
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download CFI projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./cfi_data"),
        help="Directory for downloaded/processed files (default: ./cfi_data)"
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
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60, flush=True)
    print("CFI (Canada Foundation for Innovation)", flush=True)
    print("to S3 Data Pipeline", flush=True)
    print("=" * 60, flush=True)
    print(f"Output directory: {args.output_dir.absolute()}", flush=True)
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}", flush=True)

    # Step 1: Download
    excel_path = args.output_dir / "cfi_projects.xlsx"

    if not args.skip_download:
        print(f"\n{'='*60}", flush=True)
        print("Step 1: Downloading CFI data", flush=True)
        print(f"{'='*60}", flush=True)

        if not download_excel(EXCEL_URL, excel_path):
            print("\n[ERROR] Download failed!", flush=True)
            sys.exit(1)
    else:
        if not excel_path.exists():
            print(f"[ERROR] Excel file not found at {excel_path}", flush=True)
            sys.exit(1)
        print(f"\n  [SKIP] Using existing file: {excel_path}", flush=True)

    # Step 2: Process
    parquet_path = process_data(excel_path, args.output_dir)

    # Step 3: Upload to S3
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:", flush=True)
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}", flush=True)

    print(f"\n{'='*60}", flush=True)
    if upload_success or args.skip_upload:
        print("Pipeline complete!", flush=True)
    else:
        print("Pipeline FAILED - S3 upload unsuccessful", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"\nNext step:", flush=True)
    print(f"  In Databricks, run: notebooks/awards/CreateCFIAwards.ipynb", flush=True)


if __name__ == "__main__":
    main()
