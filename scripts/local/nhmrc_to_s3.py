#!/usr/bin/env python3
"""
NHMRC (National Health and Medical Research Council) to S3 Data Pipeline
=========================================================================

This script downloads all grants from the Australian NHMRC outcomes data,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://www.nhmrc.gov.au/funding/data-research/outcomes
Output: s3://openalex-ingest/awards/nhmrc/nhmrc_projects.parquet

What this script does:
1. Downloads grant outcome XLSX files for years 2013-2025 from NHMRC website
2. Parses each XLSX file and extracts grant data
3. Combines all years into a single dataframe
4. Cleans and standardizes field names
5. Converts to parquet format
6. Uploads to S3

Requirements:
    pip install pandas pyarrow requests openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nhmrc/

Usage:
    python nhmrc_to_s3.py

    # Or with options:
    python nhmrc_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# =============================================================================
# Configuration
# =============================================================================

# NHMRC download URLs by year
# These are the actual URLs from https://www.nhmrc.gov.au/funding/data-research/outcomes
NHMRC_FILES = {
    2025: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/grant%20documents/Summary-of-result-2025-app-round-22122025.xlsx",
    2024: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/grant%20documents/Summary-of-result-2024-app-round-100725.xlsx",
    2023: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/grant%20documents/Summary-of-result-2023-app-round-15122023.xlsx",
    2022: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/1-summary_of_results_2022_app_round_24022023%20%281%29.xlsx",
    2021: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/1.summary_of_results_2021_app_round_020222_1.xlsx",
    2020: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/1.summary_of_results_2020_app_round_110221.xlsx",
    2019: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/summary-of-results-2019-appround-07122019.xlsx",
    2018: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/summary-of-results-2018-app-round-181212.xlsx",
    2017: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/2017-application-round.xlsx",
    2016: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/2016-application-round.xlsx",
    2015: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/2015-application-round.xlsx",
    2014: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/2014-application-round.xlsx",
    2013: "https://www.nhmrc.gov.au/sites/default/files/documents/attachments/Outcomes/2013-application-round.xlsx",
}

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nhmrc/nhmrc_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

# Browser-like headers to avoid being blocked
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}


def download_xlsx(year: int, url: str, output_dir: Path, max_retries: int = 3) -> Optional[Path]:
    """
    Download an NHMRC grants XLSX file with retry logic.

    Args:
        year: The year of the grant round
        url: URL to download from
        output_dir: Directory to save the downloaded file
        max_retries: Number of retry attempts

    Returns:
        Path to the downloaded XLSX file, or None if download failed
    """
    import time

    output_path = output_dir / f"nhmrc_{year}.xlsx"

    print(f"  [DOWNLOAD] {year}: {url}")

    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 5 * attempt  # Exponential backoff
                print(f"    [RETRY] Attempt {attempt + 1}/{max_retries} after {wait_time}s wait...")
                time.sleep(wait_time)

            response = requests.get(url, headers=HEADERS, timeout=180, stream=True)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_kb = output_path.stat().st_size / 1024
            print(f"    [SUCCESS] {size_kb:.1f} KB")
            return output_path

        except requests.RequestException as e:
            print(f"    [ERROR] Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print(f"    [FAILED] All {max_retries} attempts failed for {year}")
                return None

    return None


def download_all_files(output_dir: Path) -> list[Path]:
    """
    Download all NHMRC grant files.

    Args:
        output_dir: Directory to save downloaded files

    Returns:
        List of paths to downloaded files
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading NHMRC grant files")
    print(f"{'='*60}")

    downloaded = []
    for year, url in sorted(NHMRC_FILES.items(), reverse=True):
        path = download_xlsx(year, url, output_dir)
        if path:
            downloaded.append((year, path))

    print(f"\n  Downloaded {len(downloaded)} of {len(NHMRC_FILES)} files")
    return downloaded


def find_existing_files(output_dir: Path) -> list:
    """
    Find existing XLSX files in the output directory and extract years from filenames.

    Handles various filename patterns:
    - nhmrc_2025.xlsx (our standard)
    - Summary-of-result-2025-app-round-22122025.xlsx
    - 2017-application-round.xlsx
    - 1-summary_of_results_2022_app_round_24022023 (1).xlsx
    - summary-of-results-2019-appround-07122019.xlsx

    Returns:
        List of (year, path) tuples
    """
    import re

    files = []
    for xlsx_path in output_dir.glob("*.xlsx"):
        filename = xlsx_path.name

        # Try to extract year from filename using various patterns
        year = None

        # Pattern 1: nhmrc_YYYY.xlsx
        match = re.search(r'nhmrc_(\d{4})\.xlsx', filename)
        if match:
            year = int(match.group(1))

        # Pattern 2: YYYY-application-round.xlsx
        if not year:
            match = re.search(r'^(\d{4})-application-round\.xlsx', filename)
            if match:
                year = int(match.group(1))

        # Pattern 3: Summary-of-result-YYYY- or summary_of_results_YYYY_
        if not year:
            match = re.search(r'[Ss]ummary[-_]of[-_]results?[-_](\d{4})[-_]', filename)
            if match:
                year = int(match.group(1))

        # Pattern 4: Any 4-digit year between 2010-2030 in filename
        if not year:
            match = re.search(r'(20[12]\d)', filename)
            if match:
                year = int(match.group(1))

        if year:
            files.append((year, xlsx_path))
            print(f"  [FOUND] {year}: {filename}")
        else:
            print(f"  [SKIP] Could not determine year: {filename}")

    return sorted(files, key=lambda x: x[0], reverse=True)


# =============================================================================
# Processing Functions
# =============================================================================

def find_header_row(df: pd.DataFrame) -> int:
    """
    Find the header row in an XLSX file by looking for known column names.
    NHMRC files sometimes have metadata rows at the top.
    """
    # Look for rows containing known column headers
    known_headers = ['grant id', 'app id', 'application id', 'grant title', 'cia', 'grant value']

    for idx in range(min(10, len(df))):  # Check first 10 rows
        row_values = df.iloc[idx].astype(str).str.lower().tolist()
        matches = sum(1 for h in known_headers if any(h in str(v) for v in row_values))
        if matches >= 2:  # Found at least 2 known headers
            return idx

    return 0  # Default to first row


def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all column names are unique by adding suffix to duplicates."""
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        dup_indices = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dup_indices[1:], 1):
            cols.iloc[idx] = f"{dup}_{i}"
    df.columns = cols
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names across different year formats.
    """
    # Clean column names
    df.columns = (df.columns
                  .astype(str)
                  .str.lower()
                  .str.strip()
                  .str.replace(r'\s+', '_', regex=True)
                  .str.replace(r'[^\w]', '_', regex=True)
                  .str.replace(r'_+', '_', regex=True)
                  .str.strip('_'))

    # Deduplicate after cleaning
    df = deduplicate_columns(df)

    # Map various column names to standardized names
    column_mapping = {
        # Grant ID variations
        'grant_id': 'grant_id',
        'grantid': 'grant_id',

        # Application ID variations
        'app_id': 'app_id',
        'appid': 'app_id',
        'application_id': 'app_id',

        # Title variations
        'grant_title': 'grant_title',
        'title': 'grant_title',
        'application_title': 'grant_title',
        'project_title': 'grant_title',

        # CIA (Chief Investigator A) variations
        'cia': 'cia_name',
        'cia_name': 'cia_name',
        'chief_investigator_a': 'cia_name',
        'chief_investigator': 'cia_name',
        'ci_a': 'cia_name',

        # Institution variations
        'administering_institution': 'administering_institution',
        'admin_institution': 'administering_institution',
        'institution': 'administering_institution',

        # Amount variations
        'grant_value': 'grant_value',
        'total_budget': 'grant_value',
        'amount': 'grant_value',
        'funded_amount': 'grant_value',

        # Scheme/Type variations
        'grant_type': 'grant_type',
        'scheme': 'grant_type',
        'grant_sub_type': 'grant_sub_type',
        'sub_type': 'grant_sub_type',
        'category': 'grant_sub_type',

        # Date variations
        'start_date': 'start_date',
        'start_year': 'start_year',
        'end_date': 'end_date',
        'end_year': 'end_year',
        'date_announced': 'date_announced',

        # Location
        'state_territory': 'state_territory',
        'state': 'state_territory',

        # Research classification
        'broad_research_area': 'broad_research_area',
        'bra': 'broad_research_area',
        'for': 'fields_of_research',
        'fields_of_research': 'fields_of_research',
    }

    df = df.rename(columns=column_mapping)
    return df


def parse_xlsx(year: int, xlsx_path: Path) -> Optional[pd.DataFrame]:
    """
    Parse an NHMRC XLSX file into a dataframe.

    Args:
        year: The year of the grant round
        xlsx_path: Path to the XLSX file

    Returns:
        DataFrame with grant data, or None if parsing failed
    """
    try:
        # First, read to find header row
        df_preview = pd.read_excel(xlsx_path, header=None, nrows=15, dtype=str)
        header_row = find_header_row(df_preview)

        # Now read the full file with correct header
        df = pd.read_excel(xlsx_path, header=header_row, dtype=str)

        # Handle duplicate column names
        df = deduplicate_columns(df)

        # Remove any empty rows at the top (metadata)
        df = df.dropna(how='all')

        # Standardize columns
        df = standardize_columns(df)

        # Add application round year
        df['application_round_year'] = year

        return df

    except Exception as e:
        print(f"    [ERROR] Failed to parse {xlsx_path}: {e}")
        return None


def process_files(downloaded_files: list, output_dir: Path) -> Path:
    """
    Process all downloaded XLSX files into a single parquet file.

    Args:
        downloaded_files: List of (year, path) tuples
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing XLSX files")
    print(f"{'='*60}")

    all_dfs = []
    for year, xlsx_path in downloaded_files:
        print(f"  [PARSE] {year}: {xlsx_path.name}")
        df = parse_xlsx(year, xlsx_path)
        if df is not None:
            print(f"    Found {len(df):,} grants")
            all_dfs.append(df)

    if not all_dfs:
        raise ValueError("No data parsed from any file!")

    # Combine all dataframes
    print(f"\n  [COMBINE] Merging {len(all_dfs)} files...")
    df = pd.concat(all_dfs, ignore_index=True, sort=False)
    print(f"  Total rows before dedup: {len(df):,}")

    # Ensure we have a grant identifier
    # Prefer grant_id, fall back to app_id
    if 'grant_id' not in df.columns and 'app_id' in df.columns:
        df['grant_id'] = df['app_id']

    # Remove rows without any grant identifier
    if 'grant_id' in df.columns:
        df = df[df['grant_id'].notna() & (df['grant_id'].str.strip() != '')]

    # Deduplicate by grant_id (keep first occurrence - most recent year)
    if 'grant_id' in df.columns:
        original_count = len(df)
        df = df.drop_duplicates(subset=['grant_id'], keep='first')
        print(f"  Removed {original_count - len(df):,} duplicates")

    print(f"  Unique grants: {len(df):,}")

    # Clean and standardize amount field
    if 'grant_value' in df.columns:
        # Remove currency symbols, commas, and convert to numeric
        df['grant_value'] = (df['grant_value']
                            .astype(str)
                            .str.replace(r'[$,\s]', '', regex=True)
                            .str.strip())
        df['grant_value'] = pd.to_numeric(df['grant_value'], errors='coerce')

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Select and order columns for output
    # Include all columns that exist
    output_columns = [
        'grant_id', 'app_id', 'grant_title', 'cia_name',
        'administering_institution', 'grant_value', 'grant_type',
        'grant_sub_type', 'start_date', 'start_year', 'end_date', 'end_year',
        'date_announced', 'state_territory', 'broad_research_area',
        'fields_of_research', 'application_round_year', 'ingested_at'
    ]

    # Only include columns that exist
    existing_columns = [c for c in output_columns if c in df.columns]
    # Add any other columns not in our list
    other_columns = [c for c in df.columns if c not in existing_columns]
    final_columns = existing_columns + other_columns

    df = df[final_columns]

    # Save to parquet
    output_path = output_dir / "nhmrc_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Convert to pyarrow table for better type control
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    if 'grant_title' in df.columns:
        print(f"    - With title: {df['grant_title'].notna().sum():,}")
    if 'cia_name' in df.columns:
        print(f"    - With CIA: {df['cia_name'].notna().sum():,}")
    if 'grant_value' in df.columns:
        print(f"    - With amount: {df['grant_value'].notna().sum():,}")
        total_aud = df['grant_value'].sum()
        print(f"    - Total funding: AUD ${total_aud:,.0f}")

    if 'grant_type' in df.columns:
        print(f"\n  Grant Types:")
        print(df['grant_type'].value_counts().head(10).to_string())

    if 'application_round_year' in df.columns:
        print(f"\n  Years:")
        print(df['application_round_year'].value_counts().sort_index(ascending=False).to_string())

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
        description="Download NHMRC grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./nhmrc_data"),
        help="Directory for downloaded/processed files (default: ./nhmrc_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing XLSX files)"
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
    print("NHMRC to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        downloaded_files = download_all_files(args.output_dir)
    else:
        # Find existing files using pattern matching
        print(f"\n{'='*60}")
        print("Step 1: Finding existing XLSX files")
        print(f"{'='*60}")
        downloaded_files = find_existing_files(args.output_dir)
        print(f"\n  Found {len(downloaded_files)} existing XLSX files")

    if not downloaded_files:
        print("[ERROR] No files to process!")
        sys.exit(1)

    # Step 2: Process
    parquet_path = process_files(downloaded_files, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateNHMRCAwards.ipynb")


if __name__ == "__main__":
    main()
