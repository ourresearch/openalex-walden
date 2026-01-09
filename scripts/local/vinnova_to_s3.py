#!/usr/bin/env python3
"""
Vinnova (Sweden's Innovation Agency) to S3 Data Pipeline
=========================================================

This script processes Vinnova funded projects data from SWECRIS
and uploads to S3 for Databricks ingestion.

Data Source: https://www.vr.se/english/swecris.html (filtered by Vinnova)
Output: s3://openalex-ingest/awards/vinnova/vinnova_projects.parquet

What this script does:
1. Reads the CSV file downloaded from SWECRIS (must be pre-downloaded)
2. Processes and cleans the data
3. Converts to parquet format
4. Uploads to S3

Notes:
- Vinnova data includes ~24K projects from 2008 onwards
- Data comes from SWECRIS (Swedish Research Information System)
- CSV uses semicolon (;) as delimiter
- Has Swedish and English titles/abstracts
- Currency is SEK (Swedish Krona)
- InvolvedPeople field contains PI name, ORCID, and role

Prerequisites:
    Download the CSV from SWECRIS:
    1. Go to https://www.vr.se/english/swecris.html
    2. Filter by Funding body: Vinnova
    3. Click "Export data" -> "csv"
    4. Save to scripts/local/vinnova_data/vinnova_projects.csv

Requirements:
    pip install pandas pyarrow

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/vinnova/

Usage:
    python vinnova_to_s3.py

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# =============================================================================
# Configuration
# =============================================================================

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/vinnova/vinnova_projects.parquet"


# =============================================================================
# Processing Functions
# =============================================================================

def extract_pi_info(involved_people: str) -> dict:
    """
    Extract principal investigator info from the InvolvedPeople field.

    Format: ¤¤¤ID¤Name¤ORCID¤Role¤RoleSv¤Gender
    Example: ¤¤¤41110¤Craig Wheelock¤0000-0002-8113-0653¤Principal Investigator¤Projektledare¤Male

    Returns dict with: name, orcid, role, gender
    """
    if not involved_people or pd.isna(involved_people):
        return {'pi_name': None, 'pi_orcid': None, 'pi_role': None, 'pi_gender': None}

    # Find Principal Investigator entry
    parts = involved_people.split('¤¤¤')
    for part in parts:
        if 'Principal Investigator' in part or 'Projektledare' in part:
            fields = part.split('¤')
            if len(fields) >= 5:
                return {
                    'pi_name': fields[1] if len(fields) > 1 and fields[1] else None,
                    'pi_orcid': fields[2] if len(fields) > 2 and fields[2] else None,
                    'pi_role': fields[3] if len(fields) > 3 and fields[3] else None,
                    'pi_gender': fields[5] if len(fields) > 5 and fields[5] else None
                }

    return {'pi_name': None, 'pi_orcid': None, 'pi_role': None, 'pi_gender': None}


def process_data(csv_path: Path, output_dir: Path) -> Path:
    """
    Process the CSV file into a parquet file.

    Args:
        csv_path: Path to CSV file
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 1: Processing data", flush=True)
    print(f"{'='*60}", flush=True)

    # Read CSV (semicolon-delimited, UTF-8 with BOM)
    print("  [INFO] Reading CSV file...", flush=True)
    df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig', dtype=str)
    print(f"  Total rows: {len(df):,}", flush=True)
    print(f"  Unique projects: {df['ProjectId'].nunique():,}", flush=True)

    # Print columns
    print(f"  Columns: {list(df.columns)}", flush=True)

    # Extract PI information
    print("  [INFO] Extracting PI information...", flush=True)
    pi_info = df['InvolvedPeople'].apply(extract_pi_info)
    df['pi_name'] = pi_info.apply(lambda x: x['pi_name'])
    df['pi_orcid'] = pi_info.apply(lambda x: x['pi_orcid'])
    df['pi_role'] = pi_info.apply(lambda x: x['pi_role'])
    df['pi_gender'] = pi_info.apply(lambda x: x['pi_gender'])

    # Parse amounts
    print("  [INFO] Parsing amounts...", flush=True)
    df['FundingsSek'] = pd.to_numeric(df['FundingsSek'], errors='coerce')

    # Parse dates
    print("  [INFO] Parsing dates...", flush=True)
    for date_col in ['ProjectStartDate', 'ProjectEndDate', 'FundingStartDate', 'FundingEndDate']:
        if date_col in df.columns:
            # Parse datetime and convert to date string
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')

    # Parse funding year
    df['FundingYear'] = pd.to_numeric(df['FundingYear'], errors='coerce')

    # Standardize column names (lowercase with underscores)
    print("  [INFO] Standardizing column names...", flush=True)
    df.columns = (df.columns
                  .str.lower()
                  .str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True)  # CamelCase to snake_case
                  .str.lower())

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Save to parquet
    print("\n  [SAVE] Writing to parquet...", flush=True)
    output_path = output_dir / "vinnova_projects.parquet"
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB", flush=True)

    # Print column names for debugging
    print(f"  Final columns: {list(df.columns)}", flush=True)

    # Print summary stats - find the right column names
    print(f"\n  Summary:", flush=True)

    # Find project ID column
    project_id_col = [c for c in df.columns if 'project' in c and 'id' in c][0] if any('project' in c and 'id' in c for c in df.columns) else None
    if project_id_col:
        print(f"    - Total projects: {df[project_id_col].nunique():,}", flush=True)

    # Find title column
    title_col = [c for c in df.columns if 'title' in c and 'en' in c][0] if any('title' in c and 'en' in c for c in df.columns) else None
    if title_col:
        print(f"    - With English title: {df[title_col].notna().sum():,}", flush=True)

    # Find abstract column
    abstract_col = [c for c in df.columns if 'abstract' in c and 'en' in c][0] if any('abstract' in c and 'en' in c for c in df.columns) else None
    if abstract_col:
        print(f"    - With English abstract: {df[abstract_col].notna().sum():,}", flush=True)

    if 'pi_name' in df.columns:
        print(f"    - With PI name: {df['pi_name'].notna().sum():,}", flush=True)

    if 'pi_orcid' in df.columns:
        print(f"    - With PI ORCID: {df['pi_orcid'].notna().sum():,}", flush=True)

    # Find funding amount column
    funding_col = [c for c in df.columns if 'fundings' in c or 'sek' in c][0] if any('fundings' in c or ('funding' in c and 'sek' in c) for c in df.columns) else None
    if funding_col:
        print(f"    - With amount: {df[funding_col].notna().sum():,}", flush=True)
        total_amount = df[funding_col].sum()
        if pd.notna(total_amount):
            print(f"    - Total funding: {total_amount:,.0f} SEK ({total_amount/1e9:.1f} billion SEK)", flush=True)

    # Find institution column
    inst_col = [c for c in df.columns if 'organisation' in c and 'name' in c and 'en' in c and 'coordinating' in c][0] if any('organisation' in c and 'name' in c and 'en' in c and 'coordinating' in c for c in df.columns) else None
    if inst_col:
        print(f"\n  Top institutions:", flush=True)
        print(df[inst_col].value_counts().head(10).to_string(), flush=True)

    # Find award type column
    award_col = [c for c in df.columns if 'award' in c and 'descr' in c and 'en' in c][0] if any('award' in c and 'descr' in c and 'en' in c for c in df.columns) else None
    if award_col:
        print(f"\n  Award types:", flush=True)
        print(df[award_col].value_counts().head(10).to_string(), flush=True)

    # Find year column
    year_col = [c for c in df.columns if 'funding' in c and 'year' in c][0] if any('funding' in c and 'year' in c for c in df.columns) else None
    if year_col:
        print(f"\n  Year distribution (recent):", flush=True)
        year_counts = df[year_col].value_counts().sort_index(ascending=False).head(10)
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
    print("Step 2: Uploading to S3", flush=True)
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
        description="Process Vinnova projects and upload to S3"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).parent / "vinnova_data",
        help="Directory containing vinnova_projects.csv (default: ./vinnova_data)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    args = parser.parse_args()

    csv_path = args.input_dir / "vinnova_projects.csv"

    print("=" * 60, flush=True)
    print("Vinnova (Sweden's Innovation Agency)", flush=True)
    print("to S3 Data Pipeline", flush=True)
    print("=" * 60, flush=True)
    print(f"Input file: {csv_path.absolute()}", flush=True)
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}", flush=True)

    # Check input file exists
    if not csv_path.exists():
        print(f"\n[ERROR] CSV file not found at {csv_path}", flush=True)
        print("\nTo download the data:", flush=True)
        print("  1. Go to https://www.vr.se/english/swecris.html", flush=True)
        print("  2. Filter by Funding body: Vinnova", flush=True)
        print("  3. Click 'Export data' -> 'csv'", flush=True)
        print(f"  4. Save to {csv_path}", flush=True)
        sys.exit(1)

    # Step 1: Process
    parquet_path = process_data(csv_path, args.input_dir)

    # Step 2: Upload to S3
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
    print(f"  In Databricks, run: notebooks/awards/CreateVinnovaAwards.ipynb", flush=True)


if __name__ == "__main__":
    main()
