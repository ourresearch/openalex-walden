#!/usr/bin/env python3
"""
DFG (Deutsche Forschungsgemeinschaft) to S3 Data Pipeline
==========================================================

Downloads DFG GEPRIS funding data from Kaggle and uploads to S3.

Data Source: https://www.kaggle.com/datasets/vicdoroshenko/dfg-gepris-funding-database-snapshot
Output: s3://openalex-ingest/awards/dfg/dfg_projects.parquet

Prerequisites:
    1. Install dependencies:
       pip install pandas pyarrow kaggle

    2. Set up Kaggle API credentials:
       - Create account at kaggle.com
       - Go to Settings > API > Create New Token
       - Save kaggle.json to ~/.kaggle/kaggle.json
       - chmod 600 ~/.kaggle/kaggle.json

Usage:
    python dfg_to_s3.py                    # Full pipeline
    python dfg_to_s3.py --skip-upload      # Download only (for testing)

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# =============================================================================
# Configuration
# =============================================================================

KAGGLE_DATASET = "vicdoroshenko/dfg-gepris-funding-database-snapshot"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dfg/dfg_projects.parquet"


# =============================================================================
# Download from Kaggle
# =============================================================================

def download_from_kaggle(output_dir: Path) -> Path:
    """Download the DFG GEPRIS dataset from Kaggle."""
    print(f"\n{'='*60}")
    print("Step 1: Downloading from Kaggle")
    print(f"{'='*60}")

    # Check for kaggle package
    try:
        import kaggle
    except ImportError:
        print("  [ERROR] kaggle package not installed. Run: pip install kaggle")
        sys.exit(1)

    # Check for credentials
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if not kaggle_json.exists():
        print("  [ERROR] Kaggle credentials not found at ~/.kaggle/kaggle.json")
        print("\n  Setup instructions:")
        print("    1. Create account at kaggle.com")
        print("    2. Go to Settings > API > Create New Token")
        print("    3. Save kaggle.json to ~/.kaggle/kaggle.json")
        print("    4. chmod 600 ~/.kaggle/kaggle.json")
        sys.exit(1)

    print(f"  [INFO] Dataset: {KAGGLE_DATASET}")
    print(f"  [INFO] Output: {output_dir}")

    # Download
    try:
        kaggle.api.dataset_download_files(
            KAGGLE_DATASET,
            path=str(output_dir),
            unzip=True
        )
        print("  [SUCCESS] Download complete!")
    except Exception as e:
        print(f"  [ERROR] Download failed: {e}")
        sys.exit(1)

    # Find the projects CSV
    csv_files = list(output_dir.glob("*.csv"))
    if not csv_files:
        print("  [ERROR] No CSV files found in download")
        sys.exit(1)

    # Use the largest CSV (likely the main projects file)
    csv_path = max(csv_files, key=lambda p: p.stat().st_size)
    print(f"  [INFO] Using: {csv_path.name}")

    return csv_path


# =============================================================================
# Process Data
# =============================================================================

def process_projects(csv_path: Path, output_dir: Path) -> Path:
    """Process the projects CSV into parquet format."""
    print(f"\n{'='*60}")
    print("Step 2: Processing projects")
    print(f"{'='*60}")

    print(f"  [INFO] Reading {csv_path.name}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  [INFO] Loaded {len(df):,} rows")
    print(f"  [INFO] Columns: {list(df.columns)}")

    # Standardize column names
    df.columns = [c.lower().strip().replace(' ', '_').replace('-', '_') for c in df.columns]
    print(f"  [INFO] Standardized columns: {list(df.columns)}")

    # Map to standard schema
    column_mapping = {
        'project_id': 'project_id',
        'projectid': 'project_id',
        'id': 'project_id',
        'gepris_id': 'project_id',
        'title': 'title',
        'project_title': 'title',
        'abstract': 'abstract',
        'description': 'abstract',
        'start_date': 'start_date',
        'funding_start': 'start_date',
        'end_date': 'end_date',
        'funding_end': 'end_date',
        'funding_amount': 'amount',
        'amount': 'amount',
        'programme': 'funding_programme',
        'funding_programme': 'funding_programme',
        'dfg_programme': 'funding_programme',
        'subject_area': 'subject_area',
        'institution': 'institution',
        'research_institution': 'institution',
        'applicant': 'pi_name',
        'principal_investigator': 'pi_name',
    }

    rename_dict = {}
    for old, new in column_mapping.items():
        if old in df.columns and new not in df.columns:
            rename_dict[old] = new
    if rename_dict:
        df = df.rename(columns=rename_dict)
        print(f"  [INFO] Renamed: {rename_dict}")

    # Ensure project_id exists
    if 'project_id' not in df.columns:
        id_cols = [c for c in df.columns if 'id' in c.lower()]
        if id_cols:
            df['project_id'] = df[id_cols[0]]
        else:
            df['project_id'] = df.index.astype(str)

    # Process dates
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # Process amount
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Deduplicate
    original = len(df)
    df = df.drop_duplicates(subset=['project_id'], keep='first')
    print(f"  [INFO] Deduplicated: {original:,} -> {len(df):,}")

    # Add metadata
    df['ingested_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    df['source'] = 'kaggle_gepris_snapshot'

    # Save parquet
    output_path = output_dir / "dfg_projects.parquet"
    df.to_parquet(output_path, index=False)
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  [SAVE] {output_path.name} ({size_mb:.1f} MB)")

    # Summary
    print(f"\n  Summary:")
    print(f"    Total projects: {len(df):,}")
    for col in ['title', 'abstract', 'amount', 'pi_name', 'institution']:
        if col in df.columns:
            print(f"    With {col}: {df[col].notna().sum():,}")

    return output_path


# =============================================================================
# Upload to S3
# =============================================================================

def find_aws_cli() -> Optional[str]:
    """Find AWS CLI executable."""
    import shutil
    aws = shutil.which("aws")
    if aws:
        return aws
    for p in ["/usr/local/bin/aws", "/opt/homebrew/bin/aws"]:
        if Path(p).exists():
            return p
    return None


def upload_to_s3(local_path: Path) -> bool:
    """Upload parquet to S3."""
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws = find_aws_cli()
    if not aws:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    try:
        subprocess.run([aws, "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print("  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Download DFG GEPRIS from Kaggle and upload to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./dfg_data"))
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-download", action="store_true", help="Use existing CSV")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("DFG (Deutsche Forschungsgemeinschaft) to S3 Pipeline")
    print("=" * 60)
    print(f"Output: {args.output_dir.absolute()}")
    print(f"S3: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if args.skip_download:
        csv_files = list(args.output_dir.glob("*.csv"))
        if not csv_files:
            print("[ERROR] No CSV found. Run without --skip-download")
            sys.exit(1)
        csv_path = max(csv_files, key=lambda p: p.stat().st_size)
        print(f"\n  [SKIP] Using existing: {csv_path}")
    else:
        csv_path = download_from_kaggle(args.output_dir)

    # Step 2: Process
    parquet_path = process_projects(csv_path, args.output_dir)

    # Step 3: Upload
    if not args.skip_upload:
        upload_to_s3(parquet_path)

    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}")
    print(f"\nNext: Run notebooks/awards/CreateDFGAwards.ipynb in Databricks")


if __name__ == "__main__":
    main()