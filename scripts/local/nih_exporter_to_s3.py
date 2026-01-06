#!/usr/bin/env python3
"""
NIH ExPORTER to S3 Data Pipeline
================================

This script downloads NIH grant data from NIH ExPORTER, processes it into a single
deduplicated parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://reporter.nih.gov/exporter
Output: s3://openalex-ingest/awards/nih/nih_projects_combined.parquet

What this script does:
1. Downloads NIH ExPORTER project files for fiscal years 1985-2024
2. Extracts CSV files from downloaded zip archives
3. Combines all years into a single dataframe
4. Deduplicates by full_project_num (keeping most recent fiscal year's data)
5. Converts to parquet format
6. Uploads to S3

Output Statistics (as of Jan 2025):
- Total records: ~2.9M
- Unique awards after deduplication: ~2.28M
- Parquet file size: ~1.2 GB

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nih/

Usage:
    python nih_exporter_to_s3.py

    # Or with custom output directory:
    python nih_exporter_to_s3.py --output-dir /path/to/output

Author: OpenAlex Team
"""

import argparse
import glob
import os
import subprocess
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# Fiscal years to download (NIH ExPORTER has data from 1985 onwards)
START_YEAR = 1985
END_YEAR = 2024

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nih/nih_projects_combined.parquet"

# NIH ExPORTER download URL template
NIH_EXPORTER_URL = "https://reporter.nih.gov/exporter/projects/download/{year}"


# =============================================================================
# Download Functions
# =============================================================================

def download_year(year: int, output_dir: Path) -> Path:
    """
    Download NIH ExPORTER project file for a single fiscal year.

    Args:
        year: Fiscal year to download (e.g., 2024)
        output_dir: Directory to save the downloaded file

    Returns:
        Path to the downloaded zip file
    """
    url = NIH_EXPORTER_URL.format(year=year)
    output_path = output_dir / f"projects_{year}.zip"

    if output_path.exists():
        print(f"  [SKIP] FY{year} already downloaded")
        return output_path

    print(f"  [DOWNLOAD] FY{year}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return output_path


def download_all_years(output_dir: Path, max_workers: int = 5) -> list[Path]:
    """
    Download NIH ExPORTER project files for all fiscal years in parallel.

    Args:
        output_dir: Directory to save downloaded files
        max_workers: Maximum number of parallel downloads

    Returns:
        List of paths to downloaded zip files
    """
    print(f"\n{'='*60}")
    print(f"Step 1: Downloading NIH ExPORTER data (FY{START_YEAR}-{END_YEAR})")
    print(f"{'='*60}")

    years = range(START_YEAR, END_YEAR + 1)
    downloaded_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_year, year, output_dir): year
                   for year in years}

        for future in as_completed(futures):
            year = futures[future]
            try:
                path = future.result()
                downloaded_files.append(path)
            except Exception as e:
                print(f"  [ERROR] FY{year}: {e}")

    print(f"\nDownloaded {len(downloaded_files)} files")
    return sorted(downloaded_files)


# =============================================================================
# Processing Functions
# =============================================================================

def extract_zip_files(output_dir: Path) -> list[Path]:
    """
    Extract all downloaded zip files to CSV.

    Args:
        output_dir: Directory containing zip files

    Returns:
        List of paths to extracted CSV files
    """
    print(f"\n{'='*60}")
    print("Step 2: Extracting zip files")
    print(f"{'='*60}")

    zip_files = sorted(output_dir.glob("projects_*.zip"))
    csv_files = []

    for zip_path in zip_files:
        print(f"  [EXTRACT] {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(output_dir)

    csv_files = sorted(output_dir.glob("RePORTER_PRJ_C_FY*.csv"))
    print(f"\nExtracted {len(csv_files)} CSV files")
    return csv_files


def combine_and_deduplicate(csv_files: list[Path], output_dir: Path) -> Path:
    """
    Combine all CSV files into a single deduplicated parquet file.

    Deduplication strategy:
    - Group by full_project_num
    - Keep the record from the most recent fiscal year

    Args:
        csv_files: List of paths to CSV files
        output_dir: Directory to save the output parquet file

    Returns:
        Path to the output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 3: Combining and deduplicating")
    print(f"{'='*60}")

    # Read all CSVs
    dfs = []
    for csv_file in csv_files:
        print(f"  [READ] {csv_file.name}")
        # Read all columns as strings to avoid type issues across years
        df = pd.read_csv(csv_file, low_memory=False, dtype=str)
        dfs.append(df)

    # Combine all dataframes
    print(f"\n  [COMBINE] Merging {len(dfs)} dataframes...")
    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Total records: {len(combined):,}")

    # Clean column names (lowercase, replace spaces/hyphens)
    combined.columns = (combined.columns
                        .str.lower()
                        .str.replace(' ', '_')
                        .str.replace('-', '_'))

    # Deduplicate by full_project_num, keeping most recent fiscal year
    print(f"\n  [DEDUPE] Deduplicating by full_project_num...")
    combined['fy'] = pd.to_numeric(combined['fy'], errors='coerce')
    combined = combined.sort_values('fy', ascending=False)
    combined = combined.drop_duplicates(subset=['full_project_num'], keep='first')
    print(f"  Unique awards: {len(combined):,}")

    # Save to parquet
    output_path = output_dir / "nih_projects_combined.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    combined.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def upload_to_s3(local_path: Path) -> bool:
    """
    Upload the parquet file to S3.

    Requires AWS CLI to be installed and configured with appropriate credentials.

    Args:
        local_path: Path to the local parquet file

    Returns:
        True if upload succeeded, False otherwise
    """
    print(f"\n{'='*60}")
    print("Step 4: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    try:
        result = subprocess.run(
            ["aws", "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        print(f"\n  Make sure AWS CLI is installed and configured:")
        print(f"    pip install awscli")
        print(f"    aws configure")
        return False
    except FileNotFoundError:
        print(f"  [ERROR] AWS CLI not found")
        print(f"\n  Install AWS CLI and configure credentials:")
        print(f"    pip install awscli")
        print(f"    aws configure")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download NIH ExPORTER data and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./nih_data"),
        help="Directory for downloaded/processed files (default: ./nih_data)"
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

    print("=" * 60)
    print("NIH ExPORTER to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"Fiscal years: {START_YEAR}-{END_YEAR}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        download_all_years(args.output_dir)

    # Step 2: Extract
    csv_files = extract_zip_files(args.output_dir)
    if not csv_files:
        print("[ERROR] No CSV files found!")
        sys.exit(1)

    # Step 3: Combine and deduplicate
    parquet_path = combine_and_deduplicate(csv_files, args.output_dir)

    # Step 4: Upload to S3
    if not args.skip_upload:
        success = upload_to_s3(parquet_path)
        if not success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateNIHAwards.ipynb")


if __name__ == "__main__":
    main()
