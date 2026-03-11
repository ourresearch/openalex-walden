#!/usr/bin/env python3
"""
European Commission (CORDIS) to S3 Data Pipeline
=================================================

This script downloads EU research grant data from CORDIS (Community Research
and Development Information Service), processes it into a parquet file, and
uploads it to S3 for Databricks ingestion.

Data Sources:
- Horizon Europe (2021-2027): https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip
- Horizon 2020 (2014-2020): https://cordis.europa.eu/data/cordis-h2020projects-json.zip
- FP7 (2007-2013): https://cordis.europa.eu/data/cordis-fp7projects-json.zip

Output: s3://openalex-ingest/awards/european_commission/european_commission_projects.parquet

What this script does:
1. Downloads bulk data files from CORDIS (ZIP archives)
2. Extracts project and organization JSON files
3. Joins projects with coordinator organizations
4. Combines all framework programmes into a single DataFrame
5. Saves as parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/european_commission/

Usage:
    python european_commission_to_s3.py

    # Skip download (use cached data):
    python european_commission_to_s3.py --skip-download

    # Skip S3 upload:
    python european_commission_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import io
import json
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# CORDIS data URLs
CORDIS_DATASETS = {
    "horizon_europe": {
        "name": "Horizon Europe (2021-2027)",
        "projects_url": "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip",
        "framework": "HORIZON",
    },
    "h2020": {
        "name": "Horizon 2020 (2014-2020)",
        "projects_url": "https://cordis.europa.eu/data/cordis-h2020projects-json.zip",
        "framework": "H2020",
    },
    "fp7": {
        "name": "FP7 (2007-2013)",
        "projects_url": "https://cordis.europa.eu/data/cordis-fp7projects-json.zip",
        "framework": "FP7",
    },
}

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/european_commission/european_commission_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_and_extract_json(url: str, output_dir: Path, dataset_name: str) -> tuple[list, list]:
    """
    Download ZIP file from CORDIS and extract project and organization data.

    Args:
        url: URL to download
        output_dir: Directory to cache downloads
        dataset_name: Name for logging

    Returns:
        Tuple of (projects list, organizations list)
    """
    print(f"\n  Downloading {dataset_name}...")
    print(f"    URL: {url}")

    # Check for cached file
    cache_file = output_dir / f"{dataset_name}.zip"

    if cache_file.exists():
        print(f"    [CACHE] Using cached file: {cache_file}")
        with open(cache_file, "rb") as f:
            content = f.read()
    else:
        # Download
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        print(f"    Size: {total_size / (1024*1024):.1f} MB")

        content = response.content

        # Cache the download
        with open(cache_file, "wb") as f:
            f.write(content)
        print(f"    [CACHE] Saved to: {cache_file}")

    # Extract JSON files from ZIP
    print(f"    Extracting...")
    projects = []
    organizations = []

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        file_list = zf.namelist()
        print(f"    Files in archive: {file_list}")

        # Find and extract project.json
        for filename in file_list:
            if filename.lower() == "project.json":
                with zf.open(filename) as f:
                    projects = json.load(f)
                print(f"    Projects: {len(projects):,}")

            elif filename.lower() == "organization.json":
                with zf.open(filename) as f:
                    organizations = json.load(f)
                print(f"    Organizations: {len(organizations):,}")

    return projects, organizations


def get_coordinator_map(organizations: list) -> dict:
    """
    Create a mapping of project_id -> coordinator organization.

    Args:
        organizations: List of organization dictionaries

    Returns:
        Dict mapping project_id to coordinator organization dict
    """
    coord_map = {}
    for org in organizations:
        if org.get("role") == "coordinator":
            project_id = org.get("projectID")
            if project_id:
                coord_map[project_id] = org
    return coord_map


def process_projects(
    projects: list,
    organizations: list,
    framework: str
) -> list[dict]:
    """
    Process raw CORDIS data into normalized records.

    Args:
        projects: List of project dictionaries from CORDIS
        organizations: List of organization dictionaries
        framework: Framework programme name (H2020, HORIZON, FP7)

    Returns:
        List of processed project dictionaries
    """
    # Build coordinator lookup
    coord_map = get_coordinator_map(organizations)

    processed = []
    for proj in projects:
        project_id = proj.get("id")

        # Get coordinator info
        coord = coord_map.get(project_id, {})

        record = {
            # Project identifiers
            "cordis_project_id": project_id,
            "acronym": proj.get("acronym"),
            "rcn": proj.get("rcn"),
            "grant_doi": proj.get("grantDoi"),

            # Project details
            "title": proj.get("title"),
            "objective": proj.get("objective"),
            "status": proj.get("status"),
            "framework_programme": framework,
            "funding_scheme": proj.get("fundingScheme"),
            "topics": proj.get("topics"),
            "legal_basis": proj.get("legalBasis"),
            "master_call": proj.get("masterCall"),
            "sub_call": proj.get("subCall"),
            "keywords": proj.get("keywords"),

            # Dates
            "start_date": proj.get("startDate"),
            "end_date": proj.get("endDate"),
            "ec_signature_date": proj.get("ecSignatureDate"),

            # Funding
            "total_cost": proj.get("totalCost"),
            "ec_max_contribution": proj.get("ecMaxContribution"),

            # Coordinator organization
            "coordinator_name": coord.get("name"),
            "coordinator_short_name": coord.get("shortName"),
            "coordinator_country": coord.get("country"),
            "coordinator_city": coord.get("city"),
            "coordinator_org_id": coord.get("organisationID"),
            "coordinator_activity_type": coord.get("activityType"),
            "coordinator_ec_contribution": coord.get("ecContribution"),

            # Metadata
            "content_update_date": proj.get("contentUpdateDate"),
        }
        processed.append(record)

    return processed


# =============================================================================
# Main Pipeline
# =============================================================================

def download_all_datasets(output_dir: Path, skip_download: bool = False) -> pd.DataFrame:
    """
    Download and process all CORDIS datasets.

    Args:
        output_dir: Directory for cached files
        skip_download: If True, use cached parquet if available

    Returns:
        Combined DataFrame of all projects
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading CORDIS datasets")
    print(f"{'='*60}")

    # Check for cached parquet
    cache_parquet = output_dir / "european_commission_projects.parquet"
    if skip_download and cache_parquet.exists():
        print(f"  [CACHE] Loading from {cache_parquet}")
        return pd.read_parquet(cache_parquet)

    all_projects = []

    for dataset_key, dataset_info in CORDIS_DATASETS.items():
        print(f"\n  Processing: {dataset_info['name']}")

        try:
            projects, organizations = download_and_extract_json(
                dataset_info["projects_url"],
                output_dir,
                dataset_key
            )

            if projects:
                processed = process_projects(
                    projects,
                    organizations,
                    dataset_info["framework"]
                )
                all_projects.extend(processed)
                print(f"    Added {len(processed):,} projects")
            else:
                print(f"    [WARN] No projects found in {dataset_key}")

        except Exception as e:
            print(f"    [ERROR] Failed to process {dataset_key}: {e}")
            continue

    print(f"\n  Total projects from all sources: {len(all_projects):,}")

    # Convert to DataFrame
    df = pd.DataFrame(all_projects)

    return df


def process_dataframe(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Process DataFrame and save as parquet.

    Args:
        df: Raw DataFrame
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing data")
    print(f"{'='*60}")

    print(f"  Total rows: {len(df):,}")

    # Convert dates to string format (YYYY-MM-DD)
    print("  Converting dates to string format...")
    date_columns = ["start_date", "end_date", "ec_signature_date"]
    for col in date_columns:
        if col in df.columns:
            # Handle various date formats
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    # Convert numeric fields
    print("  Converting numeric fields...")
    numeric_columns = ["total_cost", "ec_max_contribution", "coordinator_ec_contribution"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure string columns are properly typed (avoid mixed types)
    print("  Cleaning string columns...")
    string_columns = [
        "acronym", "title", "objective", "status", "framework_programme",
        "funding_scheme", "topics", "legal_basis", "master_call", "sub_call",
        "keywords", "coordinator_name", "coordinator_short_name",
        "coordinator_country", "coordinator_city", "coordinator_activity_type",
        "grant_doi", "content_update_date", "coordinator_org_id", "cordis_project_id", "rcn"
    ]
    for col in string_columns:
        if col in df.columns:
            # Convert all values to string, replacing None/NaN with None
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)

    # Remove duplicates by cordis_project_id
    print("  Deduplicating by project ID...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["cordis_project_id"], keep="first")
    if original_count > len(df):
        print(f"    Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "european_commission_projects.parquet"
    print(f"\n  Saving to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With objective: {df['objective'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With coordinator: {df['coordinator_name'].notna().sum():,}")
    print(f"    - With DOI: {df['grant_doi'].notna().sum():,}")

    if "framework_programme" in df.columns:
        print(f"\n  Framework Programmes:")
        print(df["framework_programme"].value_counts().to_string())

    if "funding_scheme" in df.columns:
        print(f"\n  Funding Schemes (top 15):")
        print(df["funding_scheme"].value_counts().head(15).to_string())

    # Calculate total funding
    total_funding = df["ec_max_contribution"].sum()
    print(f"\n  Total EC Contribution: EUR {total_funding/1e9:.2f} billion")

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
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    print(f"  Using AWS CLI: {aws_cmd}")

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
        description="Download European Commission (CORDIS) projects and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./european_commission_data"),
        help="Directory for downloaded/processed files"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use cached data)"
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
    print("European Commission (CORDIS) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    df = download_all_datasets(args.output_dir, args.skip_download)

    if df.empty:
        print("[ERROR] No data downloaded!")
        sys.exit(1)

    # Step 2: Process
    parquet_path = process_dataframe(df, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateEuropeanCommissionAwards.ipynb")


if __name__ == "__main__":
    main()
