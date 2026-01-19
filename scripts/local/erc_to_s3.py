#!/usr/bin/env python3
"""
ERC (European Research Council) to S3 Data Pipeline
====================================================

This script downloads ERC grant data from CORDIS (EU's research database),
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Sources:
- Horizon Europe (2021-2027): https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip
- Horizon 2020 (2014-2020): https://cordis.europa.eu/data/cordis-h2020projects-csv.zip
- FP7 (2007-2013): https://cordis.europa.eu/data/cordis-fp7projects-csv.zip

Output: s3://openalex-ingest/awards/erc/erc_projects.parquet

What this script does:
1. Downloads CORDIS ZIP files for each framework programme
2. Extracts project.csv and organization.csv from each
3. Filters for ERC projects only (funding schemes starting with "ERC")
4. Joins with organization data to get host institution
5. Combines all programmes into a single DataFrame
6. Saves as parquet and uploads to S3

ERC Grant Types:
- ERC-STG: Starting Grants (early-career researchers)
- ERC-COG: Consolidator Grants (mid-career researchers)
- ERC-ADG: Advanced Grants (established researchers)
- ERC-SyG: Synergy Grants (collaborative teams)
- ERC-POC: Proof of Concept (commercialization)

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/erc/

Usage:
    python erc_to_s3.py

    # Skip upload for testing:
    python erc_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import io
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

# CORDIS data sources
CORDIS_SOURCES = {
    "HORIZON": {
        "url": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
        "name": "Horizon Europe (2021-2027)",
    },
    "H2020": {
        "url": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
        "name": "Horizon 2020 (2014-2020)",
    },
    "FP7": {
        "url": "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip",
        "name": "FP7 (2007-2013)",
    },
}

# ERC funding scheme prefixes
ERC_SCHEME_PREFIXES = ["ERC-", "HORIZON-ERC"]

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/erc/erc_projects.parquet"

# Request settings
REQUEST_TIMEOUT = 300  # 5 minutes for large downloads


# =============================================================================
# Download Functions
# =============================================================================

def download_cordis_zip(url: str, name: str) -> Optional[bytes]:
    """
    Download a CORDIS ZIP file.

    Args:
        url: URL to download
        name: Human-readable name for logging

    Returns:
        ZIP file contents as bytes, or None if download failed
    """
    print(f"  [DOWNLOAD] {name}...")
    print(f"    URL: {url}")

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()

        # Show download progress
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        chunks = []

        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    print(f"\r    Downloaded: {downloaded / 1024 / 1024:.1f} MB ({pct:.1f}%)", end="", flush=True)

        print()  # Newline after progress

        content = b"".join(chunks)
        print(f"    Size: {len(content) / 1024 / 1024:.1f} MB")
        return content

    except requests.exceptions.RequestException as e:
        print(f"    [ERROR] Download failed: {e}")
        return None


def extract_csv_from_zip(zip_content: bytes, filename: str) -> Optional[pd.DataFrame]:
    """
    Extract a CSV file from ZIP content and load as DataFrame.

    Args:
        zip_content: ZIP file contents as bytes
        filename: Name of CSV file to extract

    Returns:
        DataFrame, or None if extraction failed
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            if filename not in zf.namelist():
                print(f"    [WARN] {filename} not found in ZIP")
                return None

            with zf.open(filename) as f:
                # CORDIS uses semicolon as delimiter
                # on_bad_lines='skip' handles malformed rows
                df = pd.read_csv(
                    f,
                    sep=";",
                    dtype=str,
                    low_memory=False,
                    on_bad_lines="skip",
                    quoting=1  # QUOTE_ALL - handle quoted fields better
                )
                print(f"    Extracted {filename}: {len(df):,} rows")
                return df

    except Exception as e:
        print(f"    [ERROR] Failed to extract {filename}: {e}")
        return None


# =============================================================================
# Data Processing Functions
# =============================================================================

def filter_erc_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to only include ERC projects.

    Args:
        df: DataFrame with fundingScheme column

    Returns:
        Filtered DataFrame
    """
    if "fundingScheme" not in df.columns:
        print("    [WARN] No fundingScheme column found")
        return df

    # Filter for ERC funding schemes
    mask = df["fundingScheme"].str.upper().str.startswith(tuple(ERC_SCHEME_PREFIXES), na=False)
    erc_df = df[mask].copy()

    print(f"    Filtered to {len(erc_df):,} ERC projects")

    # Show scheme distribution
    if len(erc_df) > 0:
        schemes = erc_df["fundingScheme"].value_counts().head(10)
        print("    Top schemes:")
        for scheme, count in schemes.items():
            print(f"      - {scheme}: {count:,}")

    return erc_df


def get_coordinator_org(org_df: pd.DataFrame, project_id: str) -> Optional[dict]:
    """
    Get the coordinator organization for a project.

    Args:
        org_df: Organizations DataFrame
        project_id: Project ID to look up

    Returns:
        Dict with organization info, or None
    """
    # Find coordinator (role = "coordinator" or order = 1)
    proj_orgs = org_df[org_df["projectID"] == project_id]

    if len(proj_orgs) == 0:
        return None

    # Try to find coordinator by role
    coordinator = proj_orgs[proj_orgs["role"].str.lower() == "coordinator"]

    # Fall back to first organization (order = 1)
    if len(coordinator) == 0:
        coordinator = proj_orgs[proj_orgs["order"] == "1"]

    # Fall back to first row
    if len(coordinator) == 0:
        coordinator = proj_orgs.head(1)

    if len(coordinator) == 0:
        return None

    row = coordinator.iloc[0]
    return {
        "org_name": row.get("name"),
        "org_short_name": row.get("shortName"),
        "org_country": row.get("country"),
        "org_city": row.get("city"),
    }


def process_framework_programme(
    source_key: str,
    source_info: dict,
    output_dir: Path
) -> Optional[pd.DataFrame]:
    """
    Process a single framework programme (download, extract, filter).

    Args:
        source_key: Key like "H2020", "HORIZON", "FP7"
        source_info: Dict with url and name
        output_dir: Directory for intermediate files

    Returns:
        DataFrame of ERC projects, or None if failed
    """
    print(f"\n  Processing {source_info['name']}...")

    # Download ZIP
    zip_content = download_cordis_zip(source_info["url"], source_info["name"])
    if zip_content is None:
        return None

    # Extract project.csv
    projects_df = extract_csv_from_zip(zip_content, "project.csv")
    if projects_df is None:
        return None

    # Extract organization.csv for coordinator info
    org_df = extract_csv_from_zip(zip_content, "organization.csv")

    # Filter for ERC projects
    erc_df = filter_erc_projects(projects_df)
    if len(erc_df) == 0:
        return None

    # Add framework programme identifier
    erc_df["framework_programme"] = source_key

    # Add coordinator organization info
    if org_df is not None:
        print("    Adding coordinator organization info...")
        org_info = []
        for _, row in erc_df.iterrows():
            info = get_coordinator_org(org_df, str(row["id"]))
            org_info.append(info if info else {})

        # Add org columns
        for col in ["org_name", "org_short_name", "org_country", "org_city"]:
            erc_df[col] = [d.get(col) for d in org_info]

    return erc_df


def combine_programmes(dfs: list) -> pd.DataFrame:
    """
    Combine DataFrames from multiple framework programmes.

    Args:
        dfs: List of DataFrames

    Returns:
        Combined DataFrame
    """
    print("\n  Combining all framework programmes...")

    # Filter out None values
    valid_dfs = [df for df in dfs if df is not None and len(df) > 0]

    if not valid_dfs:
        raise ValueError("No valid DataFrames to combine!")

    # Concatenate
    combined = pd.concat(valid_dfs, ignore_index=True)
    print(f"    Total projects: {len(combined):,}")

    # Show distribution by framework programme
    print("    By framework programme:")
    for fp, count in combined["framework_programme"].value_counts().items():
        print(f"      - {fp}: {count:,}")

    return combined


# =============================================================================
# Output Functions
# =============================================================================

def save_to_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save DataFrame to parquet file.

    Args:
        df: DataFrame to save
        output_dir: Output directory

    Returns:
        Path to output file
    """
    print(f"\n{'='*60}")
    print("Saving to parquet")
    print(f"{'='*60}")

    # Rename columns to snake_case for consistency
    column_mapping = {
        "id": "project_id",
        "acronym": "acronym",
        "status": "status",
        "title": "title",
        "startDate": "start_date",
        "endDate": "end_date",
        "totalCost": "total_cost",
        "ecMaxContribution": "ec_contribution",
        "legalBasis": "legal_basis",
        "topics": "topics",
        "ecSignatureDate": "signature_date",
        "frameworkProgramme": "framework_programme_code",
        "masterCall": "master_call",
        "subCall": "sub_call",
        "fundingScheme": "funding_scheme",
        "objective": "abstract",
        "contentUpdateDate": "content_update_date",
        "rcn": "rcn",
        "grantDoi": "grant_doi",
        "keywords": "keywords",
        "framework_programme": "framework_programme",
        "org_name": "host_institution",
        "org_short_name": "host_institution_short",
        "org_country": "host_country",
        "org_city": "host_city",
    }

    # Only rename columns that exist
    existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_cols)

    # Keep only relevant columns
    keep_cols = list(existing_cols.values())
    df = df[[c for c in keep_cols if c in df.columns]]

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Remove duplicates by project_id
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    if original_count != len(df):
        print(f"  Removed {original_count - len(df):,} duplicates")

    print(f"  Total rows: {len(df):,}")

    # Save to parquet
    output_path = output_dir / "erc_projects.parquet"
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file: {output_path}")
    print(f"  File size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With abstract: {df['abstract'].notna().sum():,}")
    print(f"    - With start date: {df['start_date'].notna().sum():,}")
    print(f"    - With EC contribution: {df['ec_contribution'].notna().sum():,}")
    print(f"    - With host institution: {df['host_institution'].notna().sum():,}")

    # Funding scheme distribution
    print(f"\n  Funding schemes:")
    for scheme, count in df["funding_scheme"].value_counts().head(15).items():
        print(f"    - {scheme}: {count:,}")

    return output_path


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
    print("Uploading to S3")
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
        description="Download ERC grants from CORDIS and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./erc_data"),
        help="Directory for downloaded/processed files (default: ./erc_data)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--frameworks",
        nargs="+",
        choices=list(CORDIS_SOURCES.keys()),
        default=list(CORDIS_SOURCES.keys()),
        help="Framework programmes to include (default: all)"
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ERC (European Research Council) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    print(f"Framework programmes: {', '.join(args.frameworks)}")

    # Step 1: Download and process each framework programme
    print(f"\n{'='*60}")
    print("Step 1: Downloading and processing CORDIS data")
    print(f"{'='*60}")

    dfs = []
    for source_key in args.frameworks:
        source_info = CORDIS_SOURCES[source_key]
        df = process_framework_programme(source_key, source_info, args.output_dir)
        if df is not None:
            dfs.append(df)

    if not dfs:
        print("[ERROR] No data downloaded!")
        sys.exit(1)

    # Step 2: Combine all programmes
    combined_df = combine_programmes(dfs)

    # Step 3: Save to parquet
    parquet_path = save_to_parquet(combined_df, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateERCAwards.ipynb")


if __name__ == "__main__":
    main()