#!/usr/bin/env python3
"""
SNSF (Swiss National Science Foundation) to S3 Data Pipeline
=============================================================

This script downloads all Swiss research grant data from the SNSF Data Portal,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://data.snf.ch/exportcsv/GrantWithAbstracts.csv
Output: s3://openalex-ingest/awards/snsf/snsf_projects.parquet

What this script does:
1. Downloads the SNSF grants CSV (direct download, ~90K grants)
2. Parses CSV to extract grant metadata
3. Extracts grant number, title, amount, dates, PI, and institution info
4. Converts to parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/snsf/

Usage:
    python snsf_to_s3.py

    # Or with options:
    python snsf_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# SNSF Data Portal CSV URL
SNSF_CSV_URL = "https://data.snf.ch/exportcsv/GrantWithAbstracts.csv"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/snsf/snsf_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_grants_csv(output_dir: Path) -> Path:
    """
    Download the SNSF grants CSV file.

    Args:
        output_dir: Directory to save the CSV

    Returns:
        Path to downloaded CSV file
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading SNSF grants CSV")
    print(f"{'='*60}")
    print(f"  URL: {SNSF_CSV_URL}")

    output_path = output_dir / "snsf_grants_raw.csv"

    # Download with streaming to handle large file
    print("  [INFO] Downloading...")
    response = requests.get(SNSF_CSV_URL, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                print(f"\r  [DOWNLOAD] {downloaded / 1e6:.1f} / {total_size / 1e6:.1f} MB ({pct:.1f}%)", end="", flush=True)

    print(f"\n  [SUCCESS] Downloaded to {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1e6:.1f} MB")

    return output_path


def parse_grants_csv(csv_path: Path) -> list[dict]:
    """
    Parse the SNSF grants CSV into a list of dictionaries.

    Args:
        csv_path: Path to the CSV file

    Returns:
        List of grant dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 2: Parsing CSV data")
    print(f"{'='*60}")

    # SNSF CSV uses semicolon separator
    df = pd.read_csv(csv_path, sep=';', encoding='utf-8', low_memory=False)
    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    projects = []
    for _, row in df.iterrows():
        project = {
            "grant_number": str(row.get("GrantNumber", "")) if pd.notna(row.get("GrantNumber")) else None,
            "grant_number_string": row.get("GrantNumberString") if pd.notna(row.get("GrantNumberString")) else None,
            "title": row.get("Title") if pd.notna(row.get("Title")) else None,
            "title_english": row.get("TitleEnglish") if pd.notna(row.get("TitleEnglish")) else None,
            "responsible_applicant": row.get("ResponsibleApplicantName") if pd.notna(row.get("ResponsibleApplicantName")) else None,
            "funding_instrument": row.get("FundingInstrumentPublished") if pd.notna(row.get("FundingInstrumentPublished")) else None,
            "funding_instrument_reporting": row.get("FundingInstrumentReporting") if pd.notna(row.get("FundingInstrumentReporting")) else None,
            "funding_instrument_level1": row.get("FundingInstrumentLevel1") if pd.notna(row.get("FundingInstrumentLevel1")) else None,
            "institute": row.get("Institute") if pd.notna(row.get("Institute")) else None,
            "institute_city": row.get("InstituteCity") if pd.notna(row.get("InstituteCity")) else None,
            "institute_country": row.get("InstituteCountry") if pd.notna(row.get("InstituteCountry")) else None,
            "research_institution": row.get("ResearchInstitution") if pd.notna(row.get("ResearchInstitution")) else None,
            "research_institution_type": row.get("ResearchInstitutionType") if pd.notna(row.get("ResearchInstitutionType")) else None,
            "main_discipline": row.get("MainDiscipline") if pd.notna(row.get("MainDiscipline")) else None,
            "main_discipline_level1": row.get("MainDiscipline_Level1") if pd.notna(row.get("MainDiscipline_Level1")) else None,
            "start_date": row.get("EffectiveGrantStartDate") if pd.notna(row.get("EffectiveGrantStartDate")) else None,
            "end_date": row.get("EffectiveGrantEndDate") if pd.notna(row.get("EffectiveGrantEndDate")) else None,
            "amount": row.get("AmountGrantedAllSets") if pd.notna(row.get("AmountGrantedAllSets")) else None,
            "keywords": row.get("Keywords") if pd.notna(row.get("Keywords")) else None,
            "abstract": row.get("Abstract") if pd.notna(row.get("Abstract")) else None,
            "lay_summary_en": row.get("LaySummary_En") if pd.notna(row.get("LaySummary_En")) else None,
            "state": row.get("State") if pd.notna(row.get("State")) else None,
            "call_title": row.get("CallFullTitle") if pd.notna(row.get("CallFullTitle")) else None,
            "call_decision_year": row.get("CallDecisionYear") if pd.notna(row.get("CallDecisionYear")) else None,
        }
        projects.append(project)

    print(f"  Parsed {len(projects):,} grants")
    return projects


# =============================================================================
# Processing Functions
# =============================================================================

def process_projects(projects: list[dict], output_dir: Path) -> Path:
    """
    Process projects into a parquet file.

    Args:
        projects: List of project dictionaries
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 3: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    # Convert to DataFrame
    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    # Convert dates to string format (YYYY-MM-DD) to avoid Spark compatibility issues
    print("  [INFO] Converting dates to string format...")
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            # Parse ISO format and convert to YYYY-MM-DD
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    # Convert amount to float
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Remove duplicates by grant_number (keep first)
    print("  [INFO] Deduplicating by grant_number...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["grant_number"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "snsf_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With English title: {df['title_english'].notna().sum():,}")
    print(f"    - With PI: {df['responsible_applicant'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With abstract: {df['abstract'].notna().sum():,}")

    if "funding_instrument" in df.columns:
        print(f"\n  Funding instruments (top 15):")
        print(df["funding_instrument"].value_counts().head(15).to_string())

    if "state" in df.columns:
        print(f"\n  Grant states:")
        print(df["state"].value_counts().to_string())

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
    print(f"\n{'='*60}")
    print("Step 4: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

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
        description="Download SNSF grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./snsf_data"),
        help="Directory for downloaded/processed files (default: ./snsf_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing data)"
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
    print("SNSF (Swiss National Science Foundation) to S3 Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        csv_path = download_grants_csv(args.output_dir)
    else:
        csv_path = args.output_dir / "snsf_grants_raw.csv"
        if not csv_path.exists():
            print("[ERROR] No existing CSV found. Run without --skip-download")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing CSV: {csv_path}")

    # Step 2: Parse CSV
    projects = parse_grants_csv(csv_path)

    if not projects:
        print("[ERROR] No projects found!")
        sys.exit(1)

    # Step 3: Process
    parquet_path = process_projects(projects, args.output_dir)

    # Step 4: Upload to S3
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
    print(f"  In Databricks, run: notebooks/awards/CreateSNSFAwards.ipynb")


if __name__ == "__main__":
    main()