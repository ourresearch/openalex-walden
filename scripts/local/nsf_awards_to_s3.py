#!/usr/bin/env python3
"""
NSF Awards to S3 Data Pipeline
==============================

This script downloads NSF award data from the NSF Award Search API, processes it into
a single parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://www.nsf.gov/awardsearch/download-awards
API Endpoint: https://api.nsf.gov/services/v2/s3/list-files
Output: s3://openalex-ingest/awards/nsf/nsf_awards_combined.parquet

What this script does:
1. Fetches the list of available award files from NSF API
2. Downloads zip files for each fiscal year (1900-present + Historical)
3. Extracts JSON files from each zip archive
4. Combines all awards into a single dataframe
5. Converts to parquet format
6. Uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/nsf/

Usage:
    python nsf_awards_to_s3.py

    # Or with options:
    python nsf_awards_to_s3.py --output-dir /path/to/output
    python nsf_awards_to_s3.py --skip-download  # Use existing zip files
    python nsf_awards_to_s3.py --skip-upload    # Don't upload to S3

Author: OpenAlex Team
"""

import argparse
import json
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

# NSF API endpoint for listing available files
NSF_API_URL = "https://api.nsf.gov/services/v2/s3/list-files"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nsf/nsf_awards_combined.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def get_file_list() -> list[dict]:
    """
    Fetch the list of available award files from NSF API.

    Returns:
        List of file info dicts with fileName, downloadUrl, size, etc.
    """
    print("  [FETCH] Getting file list from NSF API...")
    response = requests.get(NSF_API_URL)
    response.raise_for_status()
    data = response.json()

    # Filter to only zip files
    files = [f for f in data['files'] if f['fileName'].endswith('.zip')]
    print(f"  Found {len(files)} zip files available")
    return files


def download_file(file_info: dict, output_dir: Path) -> Path:
    """
    Download a single NSF award file.

    Args:
        file_info: Dict with fileName, downloadUrl from NSF API
        output_dir: Directory to save the downloaded file

    Returns:
        Path to the downloaded zip file
    """
    filename = file_info['fileName']
    url = file_info['downloadUrl']
    output_path = output_dir / filename

    if output_path.exists():
        # Check if file size matches
        expected_size = file_info.get('size', 0)
        actual_size = output_path.stat().st_size
        if actual_size == expected_size:
            print(f"  [SKIP] {filename} already downloaded")
            return output_path

    print(f"  [DOWNLOAD] {filename}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return output_path


def download_all_files(output_dir: Path, max_workers: int = 5) -> list[Path]:
    """
    Download all NSF award files in parallel.

    Args:
        output_dir: Directory to save downloaded files
        max_workers: Maximum number of parallel downloads

    Returns:
        List of paths to downloaded zip files
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading NSF Award files")
    print(f"{'='*60}")

    file_list = get_file_list()
    downloaded_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_file, f, output_dir): f['fileName']
                   for f in file_list}

        for future in as_completed(futures):
            filename = futures[future]
            try:
                path = future.result()
                downloaded_files.append(path)
            except Exception as e:
                print(f"  [ERROR] {filename}: {e}")

    print(f"\nDownloaded {len(downloaded_files)} files")
    return sorted(downloaded_files)


# =============================================================================
# Processing Functions
# =============================================================================

def process_zip_file(zip_path: Path) -> list[dict]:
    """
    Extract and parse all JSON files from a single zip archive.

    Args:
        zip_path: Path to the zip file

    Returns:
        List of award dicts
    """
    awards = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            json_files = [n for n in zf.namelist() if n.endswith('.json')]
            for json_file in json_files:
                try:
                    with zf.open(json_file) as f:
                        award = json.load(f)
                        # Add source file info for debugging
                        award['_source_file'] = zip_path.name
                        awards.append(award)
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"    [WARN] Failed to parse {json_file} in {zip_path.name}: {e}")
    except zipfile.BadZipFile as e:
        print(f"  [ERROR] Bad zip file {zip_path.name}: {e}")

    return awards


def flatten_award(award: dict) -> dict:
    """
    Flatten nested structures in an award record for parquet storage.

    Handles nested fields like inst, pi, pgm_ele, pgm_ref, perf_inst, app_fund, por.

    Args:
        award: Raw award dict from JSON

    Returns:
        Flattened award dict
    """
    flat = {}

    # Simple fields - copy directly
    simple_fields = [
        'awd_id', 'agcy_id', 'tran_type', 'awd_istr_txt', 'awd_titl_txt',
        'cfda_num', 'org_code', 'po_phone', 'po_email', 'po_sign_block_name',
        'awd_eff_date', 'awd_exp_date', 'tot_intn_awd_amt', 'awd_amount',
        'awd_min_amd_letter_date', 'awd_max_amd_letter_date',
        'awd_abstract_narration', 'awd_arra_amount', 'awd_agcy_code',
        'fund_agcy_code', 'dir_abbr', 'div_abbr', 'org_dir_long_name',
        'org_div_long_name', 'oblg_fy', '_source_file'
    ]

    for field in simple_fields:
        flat[field] = award.get(field)

    # Performance institution (perf_inst) - use as primary institution
    # perf_inst.perf_inst_name is the main institution field
    # Note: perf_inst can be either a dict (single) or a list (multiple institutions)
    # Always initialize these fields to ensure consistent schema across checkpoints
    flat['inst_name'] = None
    flat['inst_city'] = None
    flat['inst_state_code'] = None
    flat['inst_zip'] = None
    flat['inst_country_code'] = None
    flat['inst_country_name'] = None
    flat['perf_inst_count'] = 0
    flat['perf_inst_all'] = None

    perf_inst = award.get('perf_inst')
    if perf_inst:
        # Handle both dict (single institution) and list (multiple institutions) formats
        if isinstance(perf_inst, dict):
            # Single institution as dict
            flat['inst_name'] = perf_inst.get('perf_inst_name')
            flat['inst_city'] = perf_inst.get('perf_city_name')
            flat['inst_state_code'] = perf_inst.get('perf_st_code')
            flat['inst_zip'] = perf_inst.get('perf_zip_code')
            flat['inst_country_code'] = perf_inst.get('perf_ctry_code')
            flat['inst_country_name'] = perf_inst.get('perf_ctry_name')
            flat['perf_inst_count'] = 1
        elif isinstance(perf_inst, list) and perf_inst:
            # Multiple institutions as list
            first_perf = perf_inst[0]
            flat['inst_name'] = first_perf.get('perf_inst_name')
            flat['inst_city'] = first_perf.get('perf_city_name')
            flat['inst_state_code'] = first_perf.get('perf_st_code')
            flat['inst_zip'] = first_perf.get('perf_zip_code')
            flat['inst_country_code'] = first_perf.get('perf_ctry_code')
            flat['inst_country_name'] = first_perf.get('perf_ctry_name')
            flat['perf_inst_count'] = len(perf_inst)
            if len(perf_inst) > 1:
                flat['perf_inst_all'] = json.dumps(perf_inst)

    # Principal Investigators (pi) - use pi[0].pi_full_name
    # Always initialize these fields to ensure consistent schema across checkpoints
    flat['pi_full_name'] = None
    flat['pi_first_name'] = None
    flat['pi_last_name'] = None
    flat['pi_email'] = None
    flat['pi_count'] = 0
    flat['pi_all'] = None

    pi_list = award.get('pi', [])
    if isinstance(pi_list, list) and pi_list:
        first_pi = pi_list[0]
        flat['pi_full_name'] = first_pi.get('pi_full_name')
        flat['pi_first_name'] = first_pi.get('pi_first_name')
        flat['pi_last_name'] = first_pi.get('pi_last_name')
        flat['pi_email'] = first_pi.get('pi_email')

        flat['pi_count'] = len(pi_list)
        if len(pi_list) > 1:
            flat['pi_all'] = json.dumps(pi_list)

    # Program elements (pgm_ele) - store as JSON array
    pgm_ele = award.get('pgm_ele', [])
    if isinstance(pgm_ele, list) and pgm_ele:
        flat['pgm_ele_codes'] = ','.join(str(p.get('pgm_ele_code', '')) for p in pgm_ele)
        flat['pgm_ele_text'] = '; '.join(str(p.get('pgm_ele_txt', '')) for p in pgm_ele)
        flat['pgm_ele_all'] = json.dumps(pgm_ele)

    # Program references (pgm_ref) - store as JSON array
    pgm_ref = award.get('pgm_ref', [])
    if isinstance(pgm_ref, list) and pgm_ref:
        flat['pgm_ref_codes'] = ','.join(str(p.get('pgm_ref_code', '')) for p in pgm_ref)
        flat['pgm_ref_text'] = '; '.join(str(p.get('pgm_ref_txt', '')) for p in pgm_ref)
        flat['pgm_ref_all'] = json.dumps(pgm_ref)

    # Appropriation fund (app_fund) - store as JSON array
    app_fund = award.get('app_fund', [])
    if isinstance(app_fund, list) and app_fund:
        flat['app_fund_all'] = json.dumps(app_fund)

    # Program officer (por)
    por = award.get('por', {})
    if isinstance(por, dict):
        flat['por_email'] = por.get('por_email')
        flat['por_name'] = por.get('por_name')

    return flat


def process_all_zips(output_dir: Path) -> pd.DataFrame:
    """
    Process all zip files and combine into a single DataFrame.

    Uses incremental parquet checkpoints - each zip file's results are saved
    immediately after processing, allowing resumption from any point if the
    script crashes.

    Args:
        output_dir: Directory containing zip files

    Returns:
        Combined DataFrame of all awards
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing zip files and extracting JSON")
    print(f"{'='*60}")

    # Create checkpoint directory for incremental saves
    checkpoint_dir = output_dir / "_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)

    zip_files = sorted(output_dir.glob("*.zip"))
    print(f"  Found {len(zip_files)} zip files to process")

    # Check for existing checkpoints
    existing_checkpoints = set(p.stem for p in checkpoint_dir.glob("*.parquet"))
    if existing_checkpoints:
        print(f"  [RESUME] Found {len(existing_checkpoints)} existing checkpoints")

    # Process each zip file, saving checkpoint after each
    processed_count = 0
    skipped_count = 0

    for zip_path in zip_files:
        checkpoint_path = checkpoint_dir / f"{zip_path.stem}.parquet"

        if zip_path.stem in existing_checkpoints:
            print(f"  [SKIP] {zip_path.name} (checkpoint exists)")
            skipped_count += 1
            continue

        print(f"  [PROCESS] {zip_path.name}...")
        awards = process_zip_file(zip_path)
        print(f"    Extracted {len(awards)} awards")

        if awards:
            # Flatten and save checkpoint immediately
            flattened = [flatten_award(a) for a in awards]
            chunk_df = pd.DataFrame(flattened)

            # Convert object columns to string before saving (consistency with final output)
            for col in chunk_df.columns:
                if chunk_df[col].dtype == 'object':
                    chunk_df[col] = chunk_df[col].apply(
                        lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x)
                    )

            chunk_df.to_parquet(checkpoint_path, index=False)
            print(f"    [CHECKPOINT] Saved {len(chunk_df):,} awards to {checkpoint_path.name}")

        processed_count += 1

    print(f"\n  Processed: {processed_count}, Skipped (cached): {skipped_count}")

    # Combine all checkpoints into single DataFrame
    print("\n  [COMBINE] Loading and combining all checkpoints...")
    checkpoint_files = sorted(checkpoint_dir.glob("*.parquet"))

    if not checkpoint_files:
        print("  [ERROR] No checkpoint files found!")
        return pd.DataFrame()

    all_dfs = []
    total_records = 0
    for cp_path in checkpoint_files:
        chunk_df = pd.read_parquet(cp_path)
        total_records += len(chunk_df)
        all_dfs.append(chunk_df)
        print(f"    Loaded {cp_path.name}: {len(chunk_df):,} records")

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n  Total awards combined: {len(df):,}")
    print(f"  DataFrame shape: {df.shape}")

    return df


def save_to_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save DataFrame to parquet format.

    Args:
        df: DataFrame to save
        output_dir: Directory to save the output file

    Returns:
        Path to the output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 3: Saving to parquet")
    print(f"{'='*60}")

    # Convert object columns to string to avoid mixed-type errors in parquet
    # (some JSON files have integers where others have strings)
    print("  [CONVERT] Converting object columns to string...")
    for col in df.columns:
        if df[col].dtype == 'object':
            # Convert to string, preserving None/NaN as actual nulls
            # Note: pd.notna(x) returns an array when x is a list, so check for scalar NaN explicitly
            df[col] = df[col].apply(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))

    output_path = output_dir / "nsf_awards_combined.parquet"
    print(f"  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")
    print(f"  Total records: {len(df):,}")

    # Clean up intermediate cache file after successful parquet creation
    flattened_cache_path = output_dir / "_flattened_awards.json"
    if flattened_cache_path.exists():
        print(f"  [CLEANUP] Removing intermediate cache {flattened_cache_path.name}...")
        flattened_cache_path.unlink()

    return output_path


# =============================================================================
# Verification
# =============================================================================

def verify_data(df: pd.DataFrame) -> bool:
    """
    Verify the processed data by showing sample awards from different years.

    Args:
        df: DataFrame containing all awards

    Returns:
        True if verification passes, False otherwise
    """
    print(f"\n{'='*60}")
    print("Step 4: Verifying data")
    print(f"{'='*60}")

    # Extract year from source file (e.g., "2024.zip" -> 2024)
    df['_year'] = df['_source_file'].str.extract(r'(\d{4})').astype(float)

    # Get available years and pick 5 samples spread across the range
    available_years = sorted(df['_year'].dropna().unique())
    if len(available_years) == 0:
        print("  [ERROR] No valid years found in data!")
        return False

    print(f"  Years in dataset: {int(min(available_years))} - {int(max(available_years))}")
    print(f"  Total unique years: {len(available_years)}")

    # Select 5 sample years spread across the range
    n_samples = min(5, len(available_years))
    sample_indices = [int(i * (len(available_years) - 1) / (n_samples - 1)) for i in range(n_samples)]
    sample_years = [available_years[i] for i in sample_indices]

    print(f"\n  Sample years for verification: {[int(y) for y in sample_years]}")

    # Display columns to show
    display_cols = ['awd_id', 'awd_titl_txt', 'inst_name', 'pi_full_name',
                    'awd_amount', 'awd_eff_date', '_source_file']
    display_cols = [c for c in display_cols if c in df.columns]

    for year in sample_years:
        year_df = df[df['_year'] == year]
        print(f"\n  {'─'*56}")
        print(f"  Year {int(year)}: {len(year_df):,} awards")
        print(f"  {'─'*56}")

        # Show 2 sample awards from this year
        samples = year_df.head(2)
        for idx, (_, row) in enumerate(samples.iterrows(), 1):
            print(f"\n  Sample {idx}:")
            print(f"    Award ID:    {row.get('awd_id', 'N/A')}")
            title = str(row.get('awd_titl_txt', 'N/A'))[:70]
            if len(str(row.get('awd_titl_txt', ''))) > 70:
                title += "..."
            print(f"    Title:       {title}")
            print(f"    Institution: {row.get('inst_name', 'N/A')}")
            print(f"    PI:          {row.get('pi_full_name', 'N/A')}")
            print(f"    Amount:      ${row.get('awd_amount', 0):,.0f}" if pd.notna(row.get('awd_amount')) else "    Amount:      N/A")
            print(f"    Start Date:  {row.get('awd_eff_date', 'N/A')}")

    # Summary statistics
    print(f"\n  {'─'*56}")
    print(f"  Summary Statistics")
    print(f"  {'─'*56}")
    print(f"  Total awards:        {len(df):,}")
    print(f"  Unique award IDs:    {df['awd_id'].nunique():,}")
    print(f"  Unique institutions: {df['inst_name'].nunique():,}")

    # Amount statistics (filter valid amounts)
    valid_amounts = df['awd_amount'].dropna()
    if len(valid_amounts) > 0:
        print(f"  Total funding:       ${valid_amounts.sum():,.0f}")
        print(f"  Average award:       ${valid_amounts.mean():,.0f}")
        print(f"  Median award:        ${valid_amounts.median():,.0f}")

    # Clean up temp column
    df.drop('_year', axis=1, inplace=True)

    print(f"\n  [OK] Verification complete!")
    return True


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
    print("Step 5: Uploading to S3")
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
        description="Download NSF Award data and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./"),
        help="Directory for downloaded/processed files (default: current directory)"
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
    print("NSF Awards to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        download_all_files(args.output_dir)

    # Step 2: Process all zips
    df = process_all_zips(args.output_dir)
    if df.empty:
        print("[ERROR] No awards found!")
        sys.exit(1)

    # Step 3: Save to parquet
    parquet_path = save_to_parquet(df, args.output_dir)

    # Step 4: Verify data
    verify_data(df)

    # Step 5: Upload to S3
    if not args.skip_upload:
        success = upload_to_s3(parquet_path)
        if not success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
