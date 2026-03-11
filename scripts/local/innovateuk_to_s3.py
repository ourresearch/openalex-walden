#!/usr/bin/env python3
"""
Innovate UK to S3 Data Pipeline
================================

This script processes Innovate UK funded projects Excel files and uploads
to S3 for Databricks ingestion.

Data Source: https://www.ukri.org/publications/innovate-uk-funded-projects-since-2004/
Output: s3://openalex-ingest/awards/innovateuk/innovateuk_projects.parquet

What this script does:
1. Loads the pre-downloaded Excel files (2004-2015 and 2015-present)
2. Combines and deduplicates to project level (lead participants only)
3. Cleans and standardizes field names
4. Converts to parquet format
5. Uploads to S3

Note: Raw data is at participant level (multiple rows per project for
multi-partner projects). We filter to lead participants to get project-level.

Requirements:
    pip install pandas pyarrow openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/innovateuk/

Usage:
    # First download the Excel files to innovateuk_data/ directory:
    # https://www.ukri.org/wp-content/uploads/2026/01/IUK-130126-InnovateUKFundedProjects2004ToFY2014To2015.xlsx
    # https://www.ukri.org/wp-content/uploads/2026/01/IUK-130126-InnovateUKFundedProjectsFY2015To2016ToPresent.xlsx

    python innovateuk_to_s3.py

    # Or with options:
    python innovateuk_to_s3.py --output-dir /path/to/output --skip-upload

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

# =============================================================================
# Configuration
# =============================================================================

# Input file paths (relative to script directory)
EXCEL_FILE_2004_2015 = "innovateuk_data/innovateuk_2004_2015.xlsx"
EXCEL_FILE_2015_PRESENT = "innovateuk_data/innovateuk_2015_present.xlsx"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/innovateuk/innovateuk_projects.parquet"


# =============================================================================
# Processing Functions
# =============================================================================

def load_and_combine_excel(script_dir: Path) -> pd.DataFrame:
    """
    Load and combine the two Excel files.

    Args:
        script_dir: Directory containing the Excel files

    Returns:
        Combined DataFrame
    """
    print(f"\n{'='*60}")
    print("Step 1: Loading Excel files")
    print(f"{'='*60}")

    file1 = script_dir / EXCEL_FILE_2004_2015
    file2 = script_dir / EXCEL_FILE_2015_PRESENT

    print(f"  [LOAD] {file1.name}")
    df1 = pd.read_excel(file1)
    print(f"    Rows: {len(df1):,}")

    print(f"  [LOAD] {file2.name}")
    df2 = pd.read_excel(file2)
    print(f"    Rows: {len(df2):,}")

    # Combine
    df = pd.concat([df1, df2], ignore_index=True)
    print(f"  [COMBINED] Total rows: {len(df):,}")

    return df


def process_to_project_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to lead participants only to get project-level records.

    Args:
        df: Combined participant-level DataFrame

    Returns:
        Project-level DataFrame
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing to project level")
    print(f"{'='*60}")

    # Filter to lead participants only
    print(f"  [FILTER] Keeping lead participants only")
    df_lead = df[df['Is Lead Participant'] == 'Yes'].copy()
    print(f"    Lead participant rows: {len(df_lead):,}")

    # Check for duplicate project numbers
    dupes = df_lead['Project Number'].duplicated().sum()
    if dupes > 0:
        print(f"  [DEDUPE] Removing {dupes} duplicate project numbers (keeping first)")
        df_lead = df_lead.drop_duplicates(subset=['Project Number'], keep='first')

    print(f"  [RESULT] Unique projects: {len(df_lead):,}")

    # Calculate summary stats
    total_award = df_lead['Award Offered (£)'].sum()
    print(f"  [STATS] Total funding: £{total_award:,.0f}")

    return df_lead


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to snake_case.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with standardized column names
    """
    print(f"\n{'='*60}")
    print("Step 3: Standardizing columns")
    print(f"{'='*60}")

    # Map original names to snake_case
    column_mapping = {
        'Competition Reference': 'competition_reference',
        'Competition Title': 'competition_title',
        'Programme Title': 'programme_title',
        'Sector': 'sector',
        'Application Number': 'application_number',
        'Project Number': 'project_number',
        'Project Title': 'project_title',
        'Public Description': 'public_description',
        'Competition Year': 'competition_year',
        'Innovate UK Product Type': 'product_type',
        'Participant Name': 'participant_name',
        'Is Lead Participant': 'is_lead_participant',
        'CRN': 'company_registration_number',
        'Project Start Date': 'start_date',
        'Project End Date': 'end_date',
        'Award Offered (£)': 'award_amount',
        'Total Costs (£)': 'total_costs',
        'Actual Spend to Date (£)': 'actual_spend',
        'Participant Withdrawn From Project': 'participant_status',
        'Project Status': 'project_status',
        'Enterprise Size': 'enterprise_size',
        'Postcode': 'postcode',
        'Address Region': 'region',
        'Address LEP': 'lep',
        'Address Local Authority': 'local_authority',
        'In Multiple LEPs': 'in_multiple_leps',
        'Industrial Strategy Challenge Fund (ISCF)': 'is_iscf',
    }

    df = df.rename(columns=column_mapping)
    print(f"  [RENAMED] {len(column_mapping)} columns")

    # Convert dates to strings for Spark compatibility
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace('NaT', None)

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    return df


def save_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save DataFrame to parquet with explicit schema.

    Args:
        df: Processed DataFrame
        output_dir: Output directory

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 4: Saving to parquet")
    print(f"{'='*60}")

    output_path = output_dir / "innovateuk_projects.parquet"

    # Define schema for Spark compatibility
    schema = pa.schema([
        ('competition_reference', pa.string()),
        ('competition_title', pa.string()),
        ('programme_title', pa.string()),
        ('sector', pa.string()),
        ('application_number', pa.string()),
        ('project_number', pa.int64()),
        ('project_title', pa.string()),
        ('public_description', pa.string()),
        ('competition_year', pa.string()),
        ('product_type', pa.string()),
        ('participant_name', pa.string()),
        ('is_lead_participant', pa.string()),
        ('company_registration_number', pa.string()),
        ('start_date', pa.string()),
        ('end_date', pa.string()),
        ('award_amount', pa.float64()),
        ('total_costs', pa.float64()),
        ('actual_spend', pa.float64()),
        ('participant_status', pa.string()),
        ('project_status', pa.string()),
        ('enterprise_size', pa.string()),
        ('postcode', pa.string()),
        ('region', pa.string()),
        ('lep', pa.string()),
        ('local_authority', pa.string()),
        ('in_multiple_leps', pa.string()),
        ('is_iscf', pa.string()),
        ('ingested_at', pa.string()),
    ])

    # Ensure correct column order
    df = df[[col.name for col in schema]]

    # Convert to pyarrow table and write
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  [SAVED] {output_path.name} ({size_mb:.1f} MB)")
    print(f"  [ROWS] {len(df):,} projects")

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
    print("Step 5: Uploading to S3")
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
        description="Process Innovate UK Excel files and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for output files (default: same as input)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    args = parser.parse_args()

    # Determine directories
    script_dir = Path(__file__).parent
    output_dir = args.output_dir or script_dir / "innovateuk_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Innovate UK to S3 Data Pipeline")
    print("=" * 60)
    print(f"Script directory: {script_dir}")
    print(f"Output directory: {output_dir}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Load Excel files
    df = load_and_combine_excel(script_dir)

    # Step 2: Process to project level
    df = process_to_project_level(df)

    # Step 3: Standardize columns
    df = standardize_columns(df)

    # Step 4: Save to parquet
    parquet_path = save_parquet(df, output_dir)

    # Step 5: Upload to S3
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print(f"\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    if upload_success or args.skip_upload:
        print("Pipeline complete!")
    else:
        print("Pipeline FAILED - S3 upload unsuccessful")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, create: notebooks/awards/CreateInnovateUKAwards.ipynb")


if __name__ == "__main__":
    main()
