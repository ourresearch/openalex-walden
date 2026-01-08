#!/usr/bin/env python3
"""
NSERC Awards to S3 Data Pipeline
=================================

This script downloads NSERC (Natural Sciences and Engineering Research Council of Canada)
award data from Canada's Open Government Portal, processes it into parquet files,
and uploads to S3 for Databricks ingestion.

Data Source: https://open.canada.ca/data/en/dataset/c1b0f627-8c29-427c-ab73-33968ad9176e
Output: s3://openalex-ingest/awards/nserc/

What this script does:
1. Downloads NSERC award files for fiscal years 1991-2024
2. Downloads Co-Applicants and Partners files for each year
3. Combines all years into single dataframes per file type
4. Deduplicates awards by ApplicationID (keeping most recent fiscal year)
5. Converts to parquet format
6. Uploads to S3

Output Files:
- nserc_awards.parquet (~3M records)
- nserc_coapplicants.parquet
- nserc_partners.parquet

Requirements:
    pip install pandas pyarrow requests openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nserc/

Usage:
    python nserc_to_s3.py

    # Or with custom output directory:
    python nserc_to_s3.py --output-dir /path/to/output

Author: OpenAlex Team
"""

import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# Fiscal years to download (NSERC has data from 1991 onwards)
START_YEAR = 1991
END_YEAR = 2024

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_PREFIX = "awards/nserc"

# Base URL for NSERC open data
NSERC_BASE_URL = "https://www.nserc-crsng.gc.ca/opendata"

# File types and their URL patterns
# Pattern varies by year range
FILE_TYPES = {
    "awards": {
        "url_pattern": "NSERC_FY{year}_Expenditures",
        "output_name": "nserc_awards.parquet",
    },
    "coapplicants": {
        "url_pattern": "NSERC_FY{year}_CO-APP",
        "output_name": "nserc_coapplicants.parquet",
    },
    "partners": {
        "url_pattern": "NSERC_FY{year}_PARTNER",
        "output_name": "nserc_partners.parquet",
    },
}

# SSL verification - NSERC's certificate sometimes has issues
SSL_VERIFY = False
if not SSL_VERIFY:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =============================================================================
# Download Functions
# =============================================================================

def download_file(url: str, output_path: Path, timeout: int = 120) -> bool:
    """
    Download a file from URL to local path.

    Args:
        url: URL to download from
        output_path: Local path to save file
        timeout: Request timeout in seconds

    Returns:
        True if download succeeded, False otherwise
    """
    try:
        response = requests.get(url, stream=True, timeout=timeout, verify=SSL_VERIFY)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except requests.exceptions.RequestException as e:
        return False


def download_year_file(year: int, file_type: str, output_dir: Path) -> Optional[Path]:
    """
    Download a single NSERC file for a given year and type.

    Args:
        year: Fiscal year (e.g., 2024)
        file_type: One of 'awards', 'coapplicants', 'partners'
        output_dir: Directory to save downloaded file

    Returns:
        Path to downloaded file, or None if download failed
    """
    config = FILE_TYPES[file_type]
    base_name = config["url_pattern"].format(year=year)

    # Try different extensions based on year
    extensions = [".xlsx", ".csv"] if year >= 2024 else [".csv", ".xlsx"]

    for ext in extensions:
        url = f"{NSERC_BASE_URL}/{base_name}{ext}"
        output_path = output_dir / f"{base_name}{ext}"

        if output_path.exists():
            return output_path

        if download_file(url, output_path):
            return output_path

    return None


def download_all_files(output_dir: Path, max_workers: int = 5) -> dict[str, list[Path]]:
    """
    Download all NSERC files for all years and types.

    Args:
        output_dir: Directory to save downloaded files
        max_workers: Maximum number of parallel downloads

    Returns:
        Dictionary mapping file type to list of downloaded file paths
    """
    print(f"\n{'='*60}")
    print(f"Step 1: Downloading NSERC data (FY{START_YEAR}-{END_YEAR})")
    print(f"{'='*60}")

    downloaded = {ft: [] for ft in FILE_TYPES}
    years = range(START_YEAR, END_YEAR + 1)

    # Create list of all download tasks
    tasks = [(year, file_type) for year in years for file_type in FILE_TYPES]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_year_file, year, file_type, output_dir): (year, file_type)
            for year, file_type in tasks
        }

        for future in as_completed(futures):
            year, file_type = futures[future]
            try:
                path = future.result()
                if path:
                    downloaded[file_type].append(path)
                    print(f"  [OK] FY{year} {file_type}")
                else:
                    print(f"  [SKIP] FY{year} {file_type} - not available")
            except Exception as e:
                print(f"  [ERROR] FY{year} {file_type}: {e}")

    for file_type, paths in downloaded.items():
        print(f"\nDownloaded {len(paths)} {file_type} files")

    return downloaded


# =============================================================================
# Processing Functions
# =============================================================================

def read_data_file(path: Path) -> pd.DataFrame:
    """
    Read a CSV or XLSX file into a DataFrame.

    Args:
        path: Path to the data file

    Returns:
        DataFrame with the file contents
    """
    if path.suffix.lower() == '.xlsx':
        return pd.read_excel(path, dtype=str)
    else:
        # Try different encodings
        for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
            try:
                return pd.read_csv(path, dtype=str, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Could not read {path} with any supported encoding")


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to lowercase with underscores.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned column names
    """
    df.columns = (df.columns
                  .str.strip()
                  .str.lower()
                  .str.replace(' ', '_')
                  .str.replace('-', '_')
                  .str.replace('é', 'e')
                  .str.replace('è', 'e')
                  .str.replace('à', 'a')
                  .str.replace('ô', 'o'))
    return df


def combine_files(file_paths: list[Path], file_type: str, output_dir: Path) -> Optional[Path]:
    """
    Combine multiple yearly files into a single parquet file.

    Args:
        file_paths: List of paths to yearly files
        file_type: Type of file ('awards', 'coapplicants', 'partners')
        output_dir: Directory to save output parquet

    Returns:
        Path to output parquet file, or None if no files to process
    """
    if not file_paths:
        return None

    print(f"\n{'='*60}")
    print(f"Processing {file_type} files")
    print(f"{'='*60}")

    dfs = []
    for path in sorted(file_paths):
        print(f"  [READ] {path.name}")
        try:
            df = read_data_file(path)
            df = clean_column_names(df)
            dfs.append(df)
        except Exception as e:
            print(f"  [ERROR] Failed to read {path.name}: {e}")

    if not dfs:
        return None

    print(f"\n  [COMBINE] Merging {len(dfs)} files...")
    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Total records: {len(combined):,}")

    # Deduplicate awards by applicationid, keeping most recent fiscal year
    if file_type == "awards" and "applicationid" in combined.columns:
        print(f"\n  [DEDUPE] Deduplicating by applicationid...")

        # Convert fiscal year to numeric for sorting
        fy_col = next((c for c in combined.columns if 'fiscalyear' in c or 'fiscal_year' in c), None)
        if fy_col:
            combined['_fy_numeric'] = pd.to_numeric(
                combined[fy_col].astype(str).str.extract(r'(\d+)')[0],
                errors='coerce'
            )
            combined = combined.sort_values('_fy_numeric', ascending=False)
            combined = combined.drop(columns=['_fy_numeric'])

        combined = combined.drop_duplicates(subset=['applicationid'], keep='first')
        print(f"  Unique awards: {len(combined):,}")

    # Save to parquet
    output_name = FILE_TYPES[file_type]["output_name"]
    output_path = output_dir / output_name

    print(f"\n  [SAVE] Writing to {output_name}...")
    combined.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """
    Upload a file to S3.

    Args:
        local_path: Path to local file
        s3_key: S3 key (path within bucket)

    Returns:
        True if upload succeeded, False otherwise
    """
    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    try:
        result = subprocess.run(
            ["aws", "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"  [ERROR] AWS CLI not found")
        return False


def upload_all_to_s3(parquet_files: dict[str, Path]) -> bool:
    """
    Upload all parquet files to S3.

    Args:
        parquet_files: Dictionary mapping file type to local path

    Returns:
        True if all uploads succeeded, False otherwise
    """
    print(f"\n{'='*60}")
    print("Uploading to S3")
    print(f"{'='*60}")

    success = True
    for file_type, path in parquet_files.items():
        if path and path.exists():
            s3_key = f"{S3_PREFIX}/{path.name}"
            if not upload_to_s3(path, s3_key):
                success = False

    return success


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download NSERC award data and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./nserc_data"),
        help="Directory for downloaded/processed files (default: ./nserc_data)"
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
    print("NSERC Awards to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"Fiscal years: {START_YEAR}-{END_YEAR}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_PREFIX}/")

    # Step 1: Download
    if args.skip_download:
        # Find existing files
        downloaded = {ft: list(args.output_dir.glob(f"NSERC_FY*_{ft.upper()}*"))
                      for ft in FILE_TYPES}
        downloaded["awards"] = list(args.output_dir.glob("NSERC_FY*_Expenditures*"))
        downloaded["coapplicants"] = list(args.output_dir.glob("NSERC_FY*_CO-APP*"))
        downloaded["partners"] = list(args.output_dir.glob("NSERC_FY*_PARTNER*"))
    else:
        downloaded = download_all_files(args.output_dir)

    # Step 2: Process each file type
    parquet_files = {}
    for file_type, paths in downloaded.items():
        result = combine_files(paths, file_type, args.output_dir)
        if result:
            parquet_files[file_type] = result

    if not parquet_files:
        print("\n[ERROR] No files were processed!")
        sys.exit(1)

    # Step 3: Upload to S3
    if not args.skip_upload:
        success = upload_all_to_s3(parquet_files)
        if not success:
            print("\n[WARNING] Some S3 uploads failed. You can upload manually:")
            for file_type, path in parquet_files.items():
                if path:
                    print(f"  aws s3 cp {path} s3://{S3_BUCKET}/{S3_PREFIX}/{path.name}")

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")
    print(f"\nOutput files:")
    for file_type, path in parquet_files.items():
        if path:
            print(f"  {path.name}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateNSERCAwards.ipynb")


if __name__ == "__main__":
    main()