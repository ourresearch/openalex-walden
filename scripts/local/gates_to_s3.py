#!/usr/bin/env python3
"""
Gates Foundation to S3 Data Pipeline
=====================================

This script downloads all grants from the Bill & Melinda Gates Foundation
committed grants database, processes it into a parquet file, and uploads
it to S3 for Databricks ingestion.

Data Source: https://www.gatesfoundation.org/about/committed-grants
Output: s3://openalex-ingest/awards/gates/gates_projects.parquet

What this script does:
1. Downloads the committed grants CSV from the Gates Foundation website
2. Parses the CSV (skipping metadata header row)
3. Cleans and standardizes field names
4. Converts to parquet format
5. Uploads to S3

Note: Gates Foundation grants are to organizations, not individual researchers.
The grantee is typically an institution, not a PI.

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/gates/

Usage:
    python gates_to_s3.py

    # Or with options:
    python gates_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# =============================================================================
# Configuration
# =============================================================================

# Gates Foundation CSV download URL
GATES_CSV_URL = "https://www.gatesfoundation.org/-/media/files/bmgf-grants.csv"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gates/gates_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_csv(output_dir: Path) -> Path:
    """
    Download the Gates Foundation committed grants CSV.

    Args:
        output_dir: Directory to save the downloaded file

    Returns:
        Path to the downloaded CSV file
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading Gates Foundation grants CSV")
    print(f"{'='*60}")

    output_path = output_dir / "bmgf-grants.csv"

    print(f"  [DOWNLOAD] {GATES_CSV_URL}")
    response = requests.get(GATES_CSV_URL, timeout=120)
    response.raise_for_status()

    # Save to file
    with open(output_path, 'wb') as f:
        f.write(response.content)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  [SUCCESS] Downloaded {size_mb:.1f} MB")

    return output_path


# =============================================================================
# Processing Functions
# =============================================================================

def process_csv(csv_path: Path, output_dir: Path) -> Path:
    """
    Process the Gates Foundation CSV into a parquet file.

    The CSV has a metadata row first ("Updated X, YYYY"), then headers.

    Args:
        csv_path: Path to the downloaded CSV
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing CSV")
    print(f"{'='*60}")

    # Read CSV, skipping the first metadata row
    # The actual headers are on row 2 (index 1 after skip)
    print(f"  [READ] {csv_path.name}")
    df = pd.read_csv(csv_path, skiprows=1, dtype=str, encoding='utf-8-sig')

    print(f"  Total rows: {len(df):,}")

    # Clean column names (lowercase, replace spaces)
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace('(', '')
                  .str.replace(')', ''))

    # Rename columns to match our schema
    column_mapping = {
        'grant_id': 'grant_id',
        'grantee': 'grantee_name',
        'purpose': 'purpose',
        'division': 'division',
        'date_committed': 'date_committed',
        'duration_months': 'duration_months',
        'amount_committed': 'amount',
        'grantee_website': 'grantee_website',
        'grantee_city': 'grantee_city',
        'grantee_state': 'grantee_state',
        'grantee_country': 'grantee_country',
        'region_served': 'region_served',
        'topic': 'topic'
    }
    df = df.rename(columns=column_mapping)

    # Parse and format date_committed (YYYY-MM format to YYYY-MM-DD)
    print("  [INFO] Parsing dates...")
    df['start_date'] = pd.to_datetime(df['date_committed'] + '-01', format='%Y-%m-%d', errors='coerce')
    df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
    df['start_date'] = df['start_date'].replace('NaT', None)

    # Calculate end_date from duration_months
    df['duration_months'] = pd.to_numeric(df['duration_months'], errors='coerce')

    # Create end_date by adding months to start_date
    start_dates = pd.to_datetime(df['date_committed'] + '-01', format='%Y-%m-%d', errors='coerce')
    df['end_date'] = start_dates + pd.to_timedelta(df['duration_months'] * 30.44, unit='D')
    df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')
    df['end_date'] = df['end_date'].replace('NaT', None)

    # Clean amount (remove commas, convert to numeric)
    print("  [INFO] Parsing amounts...")
    df['amount'] = df['amount'].str.replace(',', '').str.strip()
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Remove duplicates by grant_id (keep first)
    print("  [INFO] Deduplicating by grant_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['grant_id'], keep='first')
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Define explicit schema for Spark compatibility
    # All string columns except amount (double) and duration_months (int)
    schema = pa.schema([
        ('grant_id', pa.string()),
        ('grantee_name', pa.string()),
        ('purpose', pa.string()),
        ('division', pa.string()),
        ('date_committed', pa.string()),
        ('duration_months', pa.float64()),
        ('amount', pa.float64()),
        ('grantee_website', pa.string()),
        ('grantee_city', pa.string()),
        ('grantee_state', pa.string()),
        ('grantee_country', pa.string()),
        ('region_served', pa.string()),
        ('topic', pa.string()),
        ('start_date', pa.string()),
        ('end_date', pa.string()),
        ('ingested_at', pa.string()),
    ])

    # Save to parquet with explicit schema
    output_path = output_dir / "gates_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Convert to pyarrow table with schema
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With purpose: {df['purpose'].notna().sum():,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - Total amount: ${df['amount'].sum():,.0f}")

    if 'division' in df.columns:
        print(f"\n  Divisions:")
        print(df['division'].value_counts().head(10).to_string())

    if 'topic' in df.columns:
        print(f"\n  Topics (top 10):")
        print(df['topic'].value_counts().head(10).to_string())

    if 'grantee_country' in df.columns:
        print(f"\n  Countries (top 10):")
        print(df['grantee_country'].value_counts().head(10).to_string())

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
        description="Download Gates Foundation grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./gates_data"),
        help="Directory for downloaded/processed files (default: ./gates_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing CSV)"
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
    print("Gates Foundation to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        csv_path = download_csv(args.output_dir)
    else:
        csv_path = args.output_dir / "bmgf-grants.csv"
        if not csv_path.exists():
            print(f"[ERROR] CSV not found at {csv_path}. Run without --skip-download")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing CSV: {csv_path}")

    # Step 2: Process
    parquet_path = process_csv(csv_path, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateGatesAwards.ipynb")


if __name__ == "__main__":
    main()
