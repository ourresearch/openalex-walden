#!/usr/bin/env python3
"""
SSHRC (Social Sciences and Humanities Research Council) to S3 Data Pipeline
============================================================================

This script downloads all grants from the SSHRC open data portal,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://open.canada.ca/data/en/dataset/b4e2b302-9bc6-4b33-b880-6496f8cef0f1
Output: s3://openalex-ingest/awards/sshrc/sshrc_projects.parquet

What this script does:
1. Downloads all fiscal year expenditure CSVs from SSHRC open data (1998-2023)
2. Combines all years into a single DataFrame
3. Deduplicates by application number (keeps most recent)
4. Converts to parquet format
5. Uploads to S3

Notes:
- SSHRC data is organized by fiscal year (April 1 to March 31)
- Each row is a payment, not a distinct award - same award may have multiple payments
- We aggregate by application number to get unique awards
- Data includes applicant names but not structured given/family names

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/sshrc/

Usage:
    python sshrc_to_s3.py

    # Or with options:
    python sshrc_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Global verbosity flag
VERBOSE = False
DEFAULT_TIMEOUT = 30  # seconds per download

# =============================================================================
# Configuration
# =============================================================================

# SSHRC CSV URLs - organized by fiscal year
# Note: URL patterns vary between older and newer years
SSHRC_BASE_URL = "https://www.sshrc-crsh.gc.ca/opendata"

# Fiscal years available (1998-2023)
# URL patterns changed over time, so we list them explicitly
# URLs verified from: https://open.canada.ca/data/api/action/package_show?id=b4e2b302-9bc6-4b33-b880-6496f8cef0f1
EXPENDITURE_URLS = {
    # Newer format (2020-2023)
    2023: f"{SSHRC_BASE_URL}/SSHRC_FY2023_Expenditures.csv",
    2022: f"{SSHRC_BASE_URL}/SSHRC_FY2022_Expenditures.csv",
    2021: f"{SSHRC_BASE_URL}/SSHRC_FY2021_Expenditures.csv.xls.csv",
    2020: f"{SSHRC_BASE_URL}/SSHRC_FY2020_Expenditures.csv",
    # Transition years (2016-2019) - different naming patterns
    2019: f"{SSHRC_BASE_URL}/SSHRC_FY_2019_Expenditures.csv.xls.csv",
    2018: f"{SSHRC_BASE_URL}/SSHRC_FYR2018_AWARD.csv",
    2017: f"{SSHRC_BASE_URL}/Open_Data_FY_2017-18_Expenditures_Revised_p.csv",
    2016: f"{SSHRC_BASE_URL}/Open_Data_FY_2016-17_Expenditures_Revised_p.csv",
    # Older format (1998-2015)
    2015: f"{SSHRC_BASE_URL}/SSHRC_FYR2015_AWARD.csv",
    2014: f"{SSHRC_BASE_URL}/SSHRC_FYR2014_AWARD.csv",
    2013: f"{SSHRC_BASE_URL}/SSHRC_FYR2013_AWARD.csv",
    2012: f"{SSHRC_BASE_URL}/SSHRC_FYR2012_AWARD.csv",
    2011: f"{SSHRC_BASE_URL}/SSHRC_FYR2011_AWARD.csv",
    2010: f"{SSHRC_BASE_URL}/SSHRC_FYR2010_AWARD.csv",
    2009: f"{SSHRC_BASE_URL}/SSHRC_FYR2009_AWARD.csv",
    2008: f"{SSHRC_BASE_URL}/SSHRC_FYR2008_AWARD.csv",
    2007: f"{SSHRC_BASE_URL}/SSHRC_FYR2007_AWARD.csv",
    2006: f"{SSHRC_BASE_URL}/SSHRC_FYR2006_AWARD.csv",
    2005: f"{SSHRC_BASE_URL}/SSHRC_FYR2005_AWARD.csv",
    2004: f"{SSHRC_BASE_URL}/SSHRC_FYR2004_AWARD.csv",
    2003: f"{SSHRC_BASE_URL}/SSHRC_FYR2003_AWARD.csv",
    2002: f"{SSHRC_BASE_URL}/SSHRC_FYR2002_AWARD.csv",
    2001: f"{SSHRC_BASE_URL}/SSHRC_FYR2001_AWARD.csv",
    2000: f"{SSHRC_BASE_URL}/SSHRC_FYR2000_AWARD.csv",
    1999: f"{SSHRC_BASE_URL}/SSHRC_FYR1999_AWARD.csv",
    1998: f"{SSHRC_BASE_URL}/SSHRC_FYR1998_AWARD.csv",
}

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sshrc/sshrc_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_csv_with_ssl_bypass(url: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """
    Download CSV content, bypassing SSL verification issues.

    The SSHRC site has SSL certificate issues, so we need to use
    an unverified SSL context.

    Args:
        url: URL to download
        timeout: Timeout in seconds

    Returns:
        CSV content as string, or None if download failed
    """
    # Create an unverified SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    start_time = time.time()

    if VERBOSE:
        print(f"      [DEBUG] Starting download (timeout={timeout}s)...", flush=True)

    try:
        with urllib.request.urlopen(url, context=ssl_context, timeout=timeout) as response:
            elapsed = time.time() - start_time
            if VERBOSE:
                print(f"      [DEBUG] Connection established in {elapsed:.1f}s, reading content...", flush=True)

            # Try different encodings
            content = response.read()
            elapsed = time.time() - start_time

            if VERBOSE:
                print(f"      [DEBUG] Downloaded {len(content):,} bytes in {elapsed:.1f}s", flush=True)

            # Try UTF-8 first, then latin-1 as fallback
            try:
                return content.decode('utf-8-sig')
            except UnicodeDecodeError:
                try:
                    return content.decode('latin-1')
                except UnicodeDecodeError:
                    return content.decode('utf-8', errors='replace')
    except urllib.error.URLError as e:
        elapsed = time.time() - start_time
        if 'timed out' in str(e).lower() or isinstance(e.reason, TimeoutError):
            print(f"    [TIMEOUT] Download timed out after {elapsed:.1f}s (limit: {timeout}s)", flush=True)
            print(f"    [HINT] Try increasing timeout with --timeout flag", flush=True)
        else:
            print(f"    [ERROR] URL error after {elapsed:.1f}s: {e}", flush=True)
        return None
    except TimeoutError:
        elapsed = time.time() - start_time
        print(f"    [TIMEOUT] Download timed out after {elapsed:.1f}s (limit: {timeout}s)", flush=True)
        print(f"    [HINT] Try increasing timeout with --timeout flag", flush=True)
        return None
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"    [ERROR] Failed after {elapsed:.1f}s: {type(e).__name__}: {e}", flush=True)
        return None


def download_all_csvs(output_dir: Path, timeout: int = DEFAULT_TIMEOUT) -> list[tuple[int, pd.DataFrame]]:
    """
    Download all SSHRC expenditure CSVs.

    Args:
        output_dir: Directory to save downloaded files
        timeout: Timeout in seconds for each download

    Returns:
        List of (fiscal_year, DataFrame) tuples
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 1: Downloading SSHRC expenditure data", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Timeout per file: {timeout}s", flush=True)
    print(f"  Total files: {len(EXPENDITURE_URLS)}", flush=True)

    all_data = []
    failed_years = []
    total_urls = len(EXPENDITURE_URLS)
    start_time = time.time()

    for idx, (fiscal_year, url) in enumerate(sorted(EXPENDITURE_URLS.items(), reverse=True), 1):
        elapsed = time.time() - start_time
        if idx > 1:
            avg_time = elapsed / (idx - 1)
            remaining = (total_urls - idx + 1) * avg_time
            eta_str = str(timedelta(seconds=int(remaining)))
        else:
            eta_str = "calculating..."

        filename = url.split('/')[-1]
        print(f"  [{idx}/{total_urls}] FY{fiscal_year}: {filename[:40]}... ", end="", flush=True)

        download_start = time.time()
        content = download_csv_with_ssl_bypass(url, timeout=timeout)
        download_elapsed = time.time() - download_start

        if content is None:
            print(f"FAILED ({download_elapsed:.1f}s)", flush=True)
            failed_years.append((fiscal_year, url))
            continue

        try:
            # Parse CSV
            df = pd.read_csv(StringIO(content), dtype=str, on_bad_lines='skip')

            # Add fiscal year column
            df['fiscal_year'] = fiscal_year

            # Standardize column names
            df.columns = (df.columns
                         .str.lower()
                         .str.strip()
                         .str.replace(' ', '_')
                         .str.replace('/', '_')
                         .str.replace('(', '')
                         .str.replace(')', ''))

            all_data.append((fiscal_year, df))
            print(f"OK ({len(df):,} rows, {download_elapsed:.1f}s) [ETA: {eta_str}]", flush=True)

        except Exception as e:
            print(f"PARSE ERROR: {e}", flush=True)
            failed_years.append((fiscal_year, url))

    total_elapsed = time.time() - start_time
    print(f"\n  Summary:", flush=True)
    print(f"    - Downloaded: {len(all_data)} fiscal years", flush=True)
    print(f"    - Failed: {len(failed_years)} fiscal years", flush=True)
    print(f"    - Total time: {timedelta(seconds=int(total_elapsed))}", flush=True)
    if failed_years:
        print(f"    - Failed years:", flush=True)
        for fy, url in failed_years:
            print(f"        FY{fy}: {url}", flush=True)

    return all_data


# =============================================================================
# Processing Functions
# =============================================================================

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names across different fiscal years.

    SSHRC changed column names over time, so we map them to consistent names.
    Column names are often bilingual (English-French) like "name-nom", "title-titre".
    """
    # Common column mappings (old name -> new name)
    # Note: columns are lowercased in download step, so use lowercase keys
    column_mappings = {
        # Application/Award ID - "file_number" is the unique award ID
        'file_number': 'file_number',
        'cle': 'file_number',  # French "clé" = key
        'application_number': 'file_number',
        'application_no': 'file_number',
        'app_no': 'file_number',
        'appl_id': 'file_number',

        # Title - bilingual "title-titre"
        'title-titre': 'title',
        'title': 'title',
        'application_title': 'title',
        'app_title': 'title',
        'project_title': 'title',

        # Applicant - bilingual "name-nom"
        'name-nom': 'applicant_name',
        'applicant': 'applicant_name',
        'applicant_name': 'applicant_name',
        'appl_name': 'applicant_name',

        # Institution - bilingual columns
        'institution': 'institution',
        'établissement': 'institution',
        'administering_organization': 'institution',
        'admin_org': 'institution',
        'organization': 'institution',

        # Program/Funding - bilingual "program" and "programme"
        'program': 'program',
        'programme': 'program',
        'funding_opportunity': 'program',
        'program_name': 'program',
        'programnameen': 'program',
        'programnaneen': 'program',

        # Amount - bilingual "amount-montant"
        'amount-montant': 'amount',
        'amount': 'amount',
        'awardamount': 'amount',
        'payment': 'amount',
        'payment_amount': 'amount',
        'award_amount': 'amount',

        # Keywords - bilingual "keywords-mots-clés"
        'keywords-mots-clés': 'keywords',
        'keywords': 'keywords',
        'keyword': 'keywords',

        # Discipline/Research area
        'discipline': 'discipline',
        'sshrc_discipline_en': 'discipline',
        'disciplineen': 'discipline',
        'area_of_research': 'area_of_research',
        'sshrc_area_of_research': 'area_of_research',
        'area_of_researchen': 'area_of_research',
        'research_area': 'area_of_research',

        # Competition year - bilingual
        'competition_year-année_du_concours': 'competition_year',
        'competition_year': 'competition_year',
        'comp_year': 'competition_year',

        # Province
        'province_en': 'province_en',
        'provinceen': 'province_en',
    }

    # Apply mappings, handling potential duplicates
    # First pass: determine which columns to keep and their new names
    new_columns = {}
    cols_to_drop = []
    seen_new_names = set()

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in column_mappings:
            new_name = column_mappings[col_lower]
        else:
            new_name = col_lower

        # If we've already seen this target name, mark for dropping
        # (keep the first occurrence, which has English content typically)
        if new_name in seen_new_names:
            cols_to_drop.append(col)
        else:
            new_columns[col] = new_name
            seen_new_names.add(new_name)

    # Drop duplicates all at once (efficient)
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Rename remaining columns
    df = df.rename(columns=new_columns)
    return df


def process_data(all_data: list[tuple[int, pd.DataFrame]], output_dir: Path) -> Path:
    """
    Process all downloaded data into a single parquet file.

    Args:
        all_data: List of (fiscal_year, DataFrame) tuples
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 2: Processing data", flush=True)
    print(f"{'='*60}", flush=True)

    if not all_data:
        raise ValueError("No data to process!")

    # Standardize and combine all DataFrames
    print("  [INFO] Standardizing column names...", flush=True)
    standardized_dfs = []
    for i, (fiscal_year, df) in enumerate(all_data):
        print(f"    Processing FY{fiscal_year} ({i+1}/{len(all_data)})...", end=" ", flush=True)
        df = standardize_columns(df)
        standardized_dfs.append(df)
        print(f"OK ({len(df):,} rows)", flush=True)

    # Combine all years
    print("  [INFO] Combining all fiscal years...", flush=True)
    combined = pd.concat(standardized_dfs, ignore_index=True)
    print(f"  Total rows (all payments): {len(combined):,}", flush=True)

    # Show available columns
    print(f"\n  Available columns ({len(combined.columns)}): {list(combined.columns)[:15]}...", flush=True)

    # Find the file_number column (unique award ID after standardization)
    app_id_col = None
    for col in ['file_number', 'application_number', 'appl_id', 'app_no']:
        if col in combined.columns:
            app_id_col = col
            break

    if app_id_col is None:
        print("  [WARN] Could not find file_number/application ID column!", flush=True)
        print(f"  Columns available: {list(combined.columns)}", flush=True)
        # Try to find any column with 'file' or 'app' in the name
        id_cols = [c for c in combined.columns if 'file' in c.lower() or 'app' in c.lower()]
        if id_cols:
            app_id_col = id_cols[0]
            print(f"  [INFO] Using '{app_id_col}' as file number column", flush=True)
        else:
            app_id_col = combined.columns[0]
            print(f"  [WARN] Falling back to first column: '{app_id_col}'", flush=True)
    else:
        print(f"  [INFO] Using '{app_id_col}' as award ID column", flush=True)

    # Clean amount column
    print("  [INFO] Parsing amounts...", flush=True)
    if 'amount' in combined.columns:
        combined['amount'] = (combined['amount']
                             .astype(str)
                             .str.replace(',', '', regex=False)
                             .str.replace('$', '', regex=False)
                             .str.strip())
        combined['amount'] = pd.to_numeric(combined['amount'], errors='coerce')
    print("  [INFO] Amounts parsed.", flush=True)

    # Aggregate by application number (each award may have multiple payments)
    print("  [INFO] Aggregating by file number (this may take a moment)...", flush=True)

    # First, get the first row for each application (for metadata)
    # Sort by fiscal year descending to keep most recent
    print("    Sorting by fiscal year...", flush=True)
    combined = combined.sort_values('fiscal_year', ascending=False)

    # Only keep columns we need for aggregation (speeds things up significantly)
    keep_cols = [app_id_col, 'amount', 'fiscal_year', 'title', 'applicant_name',
                 'institution', 'program', 'keywords', 'discipline',
                 'area_of_research', 'competition_year', 'province_en']
    keep_cols = [c for c in keep_cols if c in combined.columns]
    print(f"    Keeping {len(keep_cols)} columns for aggregation...", flush=True)
    combined = combined[keep_cols]

    # Group by file number and aggregate
    print("    Building aggregation...", flush=True)
    agg_dict = {}

    # Sum the amounts
    if 'amount' in combined.columns:
        agg_dict['amount'] = 'sum'

    # For other columns, take the first (most recent) value
    for col in combined.columns:
        if col not in ['amount', app_id_col, 'fiscal_year']:
            agg_dict[col] = 'first'

    # Keep track of fiscal years (take min for start, max for most recent activity)
    agg_dict['fiscal_year'] = ['min', 'max']

    print("    Running groupby (may take 30-60 seconds)...", flush=True)
    if app_id_col in combined.columns:
        grouped = combined.groupby(app_id_col, dropna=False).agg(agg_dict)
        print("    Flattening columns...", flush=True)
        grouped.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                          for col in grouped.columns]
        grouped = grouped.reset_index()

        # Rename aggregated columns
        grouped = grouped.rename(columns={
            'fiscal_year_min': 'start_fiscal_year',
            'fiscal_year_max': 'latest_fiscal_year',
            'amount_sum': 'total_amount'
        })
    else:
        grouped = combined

    print(f"  Unique awards: {len(grouped):,}", flush=True)

    # Parse competition year as start date (fiscal year ending)
    # SSHRC fiscal year 2023 means April 2022 - March 2023
    print("  [INFO] Parsing dates...", flush=True)
    if 'competition_year' in grouped.columns:
        grouped['start_year'] = pd.to_numeric(grouped['competition_year'], errors='coerce').astype('Int64')
    elif 'start_fiscal_year' in grouped.columns:
        # Use fiscal year as approximate start (fiscal year N ends March 31 of year N)
        grouped['start_year'] = grouped['start_fiscal_year'].astype('Int64')
    else:
        grouped['start_year'] = pd.NA

    # Add metadata
    grouped['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Select and rename final columns
    print("  [INFO] Selecting final columns...", flush=True)
    final_columns = {
        app_id_col: 'file_number',  # SSHRC's unique award identifier
        'title': 'title',
        'applicant_name': 'applicant_name',
        'institution': 'institution',
        'program': 'program',
        'total_amount': 'amount',
        'keywords': 'keywords',
        'discipline': 'discipline',
        'area_of_research': 'area_of_research',
        'competition_year': 'competition_year',
        'province_en': 'province',
        'start_year': 'start_year',
        'start_fiscal_year': 'start_fiscal_year',
        'latest_fiscal_year': 'latest_fiscal_year',
        'ingested_at': 'ingested_at',
    }

    # Only keep columns that exist
    output_cols = []
    for old_col, new_col in final_columns.items():
        if old_col in grouped.columns:
            grouped = grouped.rename(columns={old_col: new_col})
            output_cols.append(new_col)

    # Keep only the columns we want
    df_final = grouped[[c for c in output_cols if c in grouped.columns]].copy()

    # Define schema for parquet (all strings except amount and years)
    print("\n  [SAVE] Writing to parquet...", flush=True)

    # Convert Int64 to regular int for parquet compatibility
    for col in ['start_year', 'start_fiscal_year', 'latest_fiscal_year']:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype('float64')  # Use float to handle NaN

    output_path = output_dir / "sshrc_projects.parquet"
    df_final.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB", flush=True)

    # Print summary stats
    print(f"\n  Summary:", flush=True)
    print(f"    - Total unique awards: {len(df_final):,}", flush=True)
    if 'title' in df_final.columns:
        print(f"    - With title: {df_final['title'].notna().sum():,}", flush=True)
    if 'applicant_name' in df_final.columns:
        print(f"    - With applicant: {df_final['applicant_name'].notna().sum():,}", flush=True)
    if 'amount' in df_final.columns:
        print(f"    - With amount: {df_final['amount'].notna().sum():,}", flush=True)
        total_amount = df_final['amount'].sum()
        if pd.notna(total_amount):
            print(f"    - Total amount: ${total_amount:,.0f} CAD", flush=True)
    if 'institution' in df_final.columns:
        print(f"\n  Top institutions:", flush=True)
        print(df_final['institution'].value_counts().head(10).to_string(), flush=True)
    if 'program' in df_final.columns:
        print(f"\n  Top programs:", flush=True)
        print(df_final['program'].value_counts().head(10).to_string(), flush=True)

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
    print(f"\n{'='*60}", flush=True)
    print("Step 3: Uploading to S3", flush=True)
    print(f"{'='*60}", flush=True)

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}", flush=True)

    # Find AWS CLI
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
    global VERBOSE

    parser = argparse.ArgumentParser(
        description="Download SSHRC grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./sshrc_data"),
        help="Directory for downloaded/processed files (default: ./sshrc_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing parquet)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output for debugging hangs"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout in seconds for each download (default: {DEFAULT_TIMEOUT})"
    )
    args = parser.parse_args()

    # Set global verbosity
    VERBOSE = args.verbose

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60, flush=True)
    print("SSHRC (Social Sciences and Humanities Research Council)", flush=True)
    print("to S3 Data Pipeline", flush=True)
    print("=" * 60, flush=True)
    print(f"Output directory: {args.output_dir.absolute()}", flush=True)
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}", flush=True)
    print(f"Timeout: {args.timeout}s per download", flush=True)
    if VERBOSE:
        print(f"Verbose mode: ENABLED", flush=True)

    # Step 1 & 2: Download and process
    parquet_path = args.output_dir / "sshrc_projects.parquet"

    if not args.skip_download:
        all_data = download_all_csvs(args.output_dir, timeout=args.timeout)

        if not all_data:
            print("\n[ERROR] No data downloaded!", flush=True)
            sys.exit(1)

        parquet_path = process_data(all_data, args.output_dir)
    else:
        if not parquet_path.exists():
            print(f"[ERROR] Parquet not found at {parquet_path}. Run without --skip-download", flush=True)
            sys.exit(1)
        print(f"\n  [SKIP] Using existing parquet: {parquet_path}", flush=True)

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
    print(f"  In Databricks, run: notebooks/awards/CreateSSHRCAwards.ipynb", flush=True)


if __name__ == "__main__":
    main()
