#!/usr/bin/env python3
"""
NCN (Narodowe Centrum Nauki) to S3 Data Pipeline
=================================================

This script downloads Polish research grant data from the dane.gov.pl portal,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://dane.gov.pl/pl/dataset/2797
XLSX: https://api.dane.gov.pl/media/resources/20221223/NCN_listy_rankingowe_20221220.xlsx
Output: s3://openalex-ingest/awards/ncn/ncn_projects.parquet

What this script does:
1. Downloads the XLSX file from dane.gov.pl (or uses cached copy)
2. Parses ~23,765 grants with metadata
3. Saves as parquet and uploads to S3

Requirements:
    pip install pandas pyarrow openpyxl requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/ncn/

Usage:
    python ncn_to_s3.py

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# Data source - dane.gov.pl dataset 2797
XLSX_URL = "https://api.dane.gov.pl/media/resources/20221223/NCN_listy_rankingowe_20221220.xlsx"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ncn/ncn_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_xlsx(output_dir: Path) -> Path:
    """Download the XLSX file from dane.gov.pl."""
    print(f"\n{'='*60}")
    print("Step 1: Downloading XLSX from dane.gov.pl")
    print(f"{'='*60}")

    xlsx_path = output_dir / "NCN_listy_rankingowe.xlsx"

    # Check if we already have it
    if xlsx_path.exists():
        size_mb = xlsx_path.stat().st_size / (1024 * 1024)
        print(f"  [CACHE] Using existing file: {xlsx_path.name} ({size_mb:.1f} MB)")
        return xlsx_path

    print(f"  [DOWNLOAD] {XLSX_URL}")
    start_time = time.time()

    response = requests.get(XLSX_URL, stream=True, timeout=120)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(xlsx_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                print(f"\r  [{downloaded:,}/{total_size:,}] ({pct:.1f}%)", end="", flush=True)

    elapsed = time.time() - start_time
    size_mb = xlsx_path.stat().st_size / (1024 * 1024)
    print(f"\n  [DONE] Downloaded {size_mb:.1f} MB in {elapsed:.1f}s")

    return xlsx_path


# =============================================================================
# Processing Functions
# =============================================================================

def process_xlsx(xlsx_path: Path, output_dir: Path) -> Path:
    """Process XLSX into a parquet file."""
    print(f"\n{'='*60}")
    print("Step 2: Processing XLSX")
    print(f"{'='*60}")

    print(f"  [READ] Loading {xlsx_path.name}...")
    df = pd.read_excel(xlsx_path, engine="openpyxl")

    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    # Rename columns to English
    column_map = {
        "Lp": "row_number",
        "edycja": "edition",
        "typkonkursu": "competition_type",
        "panel": "panel",
        "tytul.pl": "title_pl",
        "tytul.en": "title_en",
        "koszt": "amount_pln",
        "jednostka.poziom1": "institution",
        "jednostka.poziom2": "department",
        "kierownik.tytul": "pi_title",
        "kierownik.imie": "pi_given_name",
        "kierownik.imie2": "pi_middle_name",
        "kierownik.nazwisko": "pi_family_name",
    }

    # Only rename columns that exist
    rename_map = {k: v for k, v in column_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Create a project ID from edition + competition type + row number
    # This creates a stable ID like "1_HARMONIA_1"
    df["project_id"] = (
        df["edition"].astype(str) + "_" +
        df["competition_type"].astype(str) + "_" +
        df["row_number"].astype(str)
    )

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["source_file"] = "NCN_listy_rankingowe_20221220.xlsx"
    df["currency"] = "PLN"

    # Ensure numeric columns are proper types
    if "amount_pln" in df.columns:
        df["amount_pln"] = pd.to_numeric(df["amount_pln"], errors="coerce")

    if "edition" in df.columns:
        df["edition"] = pd.to_numeric(df["edition"], errors="coerce").astype("Int64")

    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    if original_count != len(df):
        print(f"  Removed {original_count - len(df):,} duplicates")

    print(f"  Unique grants: {len(df):,}")

    # Save to parquet
    output_path = output_dir / "ncn_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With title (EN): {df['title_en'].notna().sum():,}")
    print(f"    - With title (PL): {df['title_pl'].notna().sum():,}")
    print(f"    - With amount: {df['amount_pln'].notna().sum():,}")
    print(f"    - With PI name: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With institution: {df['institution'].notna().sum():,}")

    if "amount_pln" in df.columns:
        total_pln = df["amount_pln"].sum()
        print(f"    - Total funding: {total_pln/1e9:.2f}B PLN")

    if "competition_type" in df.columns:
        print(f"\n  Competition types:")
        print(df["competition_type"].value_counts().head(15).to_string())

    if "edition" in df.columns:
        print(f"\n  Edition distribution:")
        edition_counts = df["edition"].dropna().value_counts().sort_index()
        print(edition_counts.tail(10).to_string())

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
    """Upload the parquet file to S3."""
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

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
        description="Download NCN grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./ncn_data"),
        help="Directory for downloaded/processed files"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--xlsx-path",
        type=Path,
        help="Use existing XLSX file instead of downloading"
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("NCN (Narodowe Centrum Nauki) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download or use existing
    if args.xlsx_path and args.xlsx_path.exists():
        xlsx_path = args.xlsx_path
        print(f"\n  [INFO] Using provided XLSX: {xlsx_path}")
    else:
        xlsx_path = download_xlsx(args.output_dir)

    # Step 2: Process
    parquet_path = process_xlsx(xlsx_path, args.output_dir)

    # Step 3: Upload
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print(f"\n[WARNING] S3 upload failed. Upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    print("Pipeline complete!" if upload_success else "Pipeline FAILED")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateNCNAwards.ipynb")


if __name__ == "__main__":
    main()
