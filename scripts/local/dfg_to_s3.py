#!/usr/bin/env python3
"""
DFG (Deutsche Forschungsgemeinschaft) to S3 Data Pipeline
==========================================================

Downloads DFG GEPRIS funding data from Kaggle and uploads to S3 for Databricks.

Data Source: https://www.kaggle.com/datasets/vicdoroshenko/dfg-gepris-funding-database-snapshot
Output: s3://openalex-ingest/awards/dfg/dfg_projects.parquet

Output columns (mapped for CreateDFGAwards.ipynb):
    project_id          -> funder_award_id
    title               -> display_name
    description         -> description
    amount              -> amount (DOUBLE, in EUR; converted from source millions)
    program_type        -> funder_scheme
    area                -> subject_area
    start_date          -> start_date (STRING, YYYY-MM-DD)
    end_date            -> end_date (STRING, YYYY-MM-DD)
    start_year          -> start_year (INT)
    end_year            -> end_year (INT)
    lead_inst           -> institution name
    pis_json            -> PI IDs as JSON array
    pi_countries_json   -> PI countries as JSON array
    pi_cities_json      -> PI cities as JSON array
    pi_gender_json      -> PI genders as JSON array

Prerequisites:
    pip install pandas pyarrow kaggle

    Kaggle credentials: ~/.kaggle/kaggle.json
    (kaggle.com > Settings > API > Create New Token)

Usage:
    python dfg_to_s3.py                 # Full pipeline
    python dfg_to_s3.py --skip-download # Use existing data
    python dfg_to_s3.py --skip-upload   # Local only
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Configuration
KAGGLE_DATASET = "vicdoroshenko/dfg-gepris-funding-database-snapshot"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dfg/dfg_projects.parquet"


def download_from_kaggle(output_dir: Path) -> Path:
    """Download GEPRIS dataset from Kaggle."""
    print(f"\n{'='*60}")
    print("Step 1: Downloading from Kaggle")
    print(f"{'='*60}")

    try:
        import kaggle
    except ImportError:
        print("  [ERROR] Run: pip install kaggle")
        sys.exit(1)

    if not (Path.home() / ".kaggle" / "kaggle.json").exists():
        print("  [ERROR] Kaggle credentials not found")
        print("  Setup: kaggle.com > Settings > API > Create New Token")
        print("  Save to ~/.kaggle/kaggle.json")
        sys.exit(1)

    print(f"  Dataset: {KAGGLE_DATASET}")
    print(f"  Downloading...")

    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=str(output_dir), unzip=True)
    print("  [OK] Download complete")

    return find_data_file(output_dir)


def find_data_file(output_dir: Path) -> Path:
    """Find the projects JSON file in downloaded data."""
    # Check Archive folder (Kaggle structure)
    archive = output_dir / "Archive"
    if archive.exists():
        for f in archive.glob("*projects*.json"):
            print(f"  Found: Archive/{f.name}")
            return f

    # Check main dir
    for f in output_dir.glob("**/*projects*.json"):
        print(f"  Found: {f.name}")
        return f

    # Any JSON
    json_files = list(output_dir.glob("**/*.json"))
    if json_files:
        f = max(json_files, key=lambda p: p.stat().st_size)
        print(f"  Found: {f.name}")
        return f

    print("  [ERROR] No data file found")
    sys.exit(1)


def process_projects(data_path: Path, output_dir: Path) -> Path:
    """Process JSON to parquet with Spark-compatible schema."""
    print(f"\n{'='*60}")
    print("Step 2: Processing")
    print(f"{'='*60}")

    print(f"  Reading {data_path.name} ({data_path.stat().st_size / 1e6:.0f} MB)...")
    df = pd.read_json(data_path)
    print(f"  Loaded {len(df):,} projects")

    # Standardize column names
    df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]

    # -------------------------------------------------------------------------
    # Map source columns to output schema
    # -------------------------------------------------------------------------

    # project_id -> string
    df['project_id'] = df['project_id'].astype(str)

    # title stays as title
    # description stays as description (source has 'description' not 'abstract')

    # estimated_budget -> amount (convert from millions of EUR to EUR)
    # e.g., 0.22 million EUR -> 220,000 EUR
    if 'estimated_budget' in df.columns:
        df['amount'] = pd.to_numeric(df['estimated_budget'], errors='coerce') * 1_000_000
        df = df.drop(columns=['estimated_budget'])
        print("  estimated_budget * 1M -> amount (EUR)")

    # start/stop (years as floats) -> dates
    # Use Int64 (nullable integer) to avoid float conversion when nulls present
    if 'start' in df.columns:
        df['start_year'] = df['start'].apply(
            lambda x: int(x) if pd.notna(x) and 1900 <= x <= 2100 else pd.NA
        ).astype('Int64')
        df['start_date'] = df['start_year'].apply(
            lambda y: f"{int(y)}-01-01" if pd.notna(y) else None
        )
        print("  start -> start_year, start_date")

    if 'stop' in df.columns:
        df['end_year'] = df['stop'].apply(
            lambda x: int(x) if pd.notna(x) and 1900 <= x <= 2100 else pd.NA
        ).astype('Int64')
        df['end_date'] = df['end_year'].apply(
            lambda y: f"{int(y)}-12-31" if pd.notna(y) else None
        )
        print("  stop -> end_year, end_date")

    # -------------------------------------------------------------------------
    # Convert array columns to JSON strings (Spark compatibility)
    # -------------------------------------------------------------------------
    array_cols = ['pis', 'pi_countries', 'pi_bundeslaender', 'pi_cities',
                  'pi_gender', 'pi_locations', 'embedding', 'sub_projects']

    for col in array_cols:
        if col in df.columns:
            df[f'{col}_json'] = df[col].apply(
                lambda x: json.dumps(x.tolist() if hasattr(x, 'tolist') else x)
                if x is not None and not (isinstance(x, float) and pd.isna(x)) else None
            )
            df = df.drop(columns=[col])
            print(f"  {col} -> {col}_json")

    # -------------------------------------------------------------------------
    # Add metadata
    # -------------------------------------------------------------------------
    df['ingested_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    df['source'] = 'kaggle_gepris_snapshot'

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=['project_id'], keep='first')
    if len(df) < before:
        print(f"  Removed {before - len(df):,} duplicates")

    # -------------------------------------------------------------------------
    # Save parquet
    # -------------------------------------------------------------------------
    output_path = output_dir / "dfg_projects.parquet"
    df.to_parquet(output_path, index=False)
    print(f"\n  Saved: {output_path.name} ({output_path.stat().st_size / 1e6:.0f} MB)")

    # Summary
    print(f"\n  Summary ({len(df):,} projects):")
    for col, label in [
        ('title', 'title'),
        ('description', 'description'),
        ('amount', 'amount'),
        ('start_date', 'start_date'),
        ('program_type', 'program_type'),
        ('lead_inst', 'lead_inst'),
    ]:
        if col in df.columns:
            n = df[col].notna().sum()
            print(f"    {label}: {n:,} ({100*n/len(df):.0f}%)")

    print(f"\n  Columns: {sorted(df.columns)}")

    return output_path


def upload_to_s3(local_path: Path) -> bool:
    """Upload parquet to S3."""
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    import shutil
    aws = shutil.which("aws")
    if not aws:
        print("  [ERROR] AWS CLI not found")
        print(f"  Manual: aws s3 cp {local_path} s3://{S3_BUCKET}/{S3_KEY}")
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  {local_path.name} -> {s3_uri}")

    try:
        subprocess.run([aws, "s3", "cp", str(local_path), s3_uri],
                       check=True, capture_output=True, text=True)
        print("  [OK] Upload complete")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] {e.stderr}")
        print(f"  Try: export AWS_PROFILE=openalex && aws s3 cp {local_path} {s3_uri}")
        return False


def main():
    parser = argparse.ArgumentParser(description="DFG GEPRIS to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./dfg_data"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("DFG (Deutsche Forschungsgemeinschaft) to S3")
    print("=" * 60)
    print(f"Output: {args.output_dir.absolute()}")
    print(f"S3: s3://{S3_BUCKET}/{S3_KEY}")

    # Download
    if args.skip_download:
        data_path = find_data_file(args.output_dir)
    else:
        data_path = download_from_kaggle(args.output_dir)

    # Process
    parquet_path = process_projects(data_path, args.output_dir)

    # Upload
    if not args.skip_upload:
        upload_to_s3(parquet_path)

    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}")
    print(f"\nNext: Run notebooks/awards/CreateDFGAwards.ipynb in Databricks")


if __name__ == "__main__":
    main()
