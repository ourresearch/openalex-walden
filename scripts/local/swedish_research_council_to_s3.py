#!/usr/bin/env python3
"""
Swedish Research Council (Vetenskapsrådet) to S3 Data Pipeline
===============================================================

This script downloads Swedish Research Council grant data from SweCRIS,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: SweCRIS API (https://swecris-api.vr.se)
Output: s3://openalex-ingest/awards/swedish_research_council/swedish_research_council_projects.parquet

What this script does:
1. Downloads Swedish Research Council grants from SweCRIS API (~23K grants)
2. Parses JSON to extract grant metadata
3. Extracts project ID, title, amount, dates, PI, and institution info
4. Converts to parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/swedish_research_council/

Usage:
    python swedish_research_council_to_s3.py

    # Or with options:
    python swedish_research_council_to_s3.py --output-dir /path/to/output --skip-upload

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

# SweCRIS API endpoint for Swedish Research Council
SWECRIS_API_BASE = "https://swecris-api.vr.se/v1"
SWECRIS_FUNDER_ID = "202100-5208"  # Swedish Research Council (Vetenskapsrådet)
SWECRIS_API_TOKEN = "VRSwecrisAPI2025-1"  # Public API token

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/swedish_research_council/swedish_research_council_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_grants_from_api(output_dir: Path) -> Path:
    """
    Download Swedish Research Council grants from SweCRIS API.

    Args:
        output_dir: Directory to save the JSON

    Returns:
        Path to downloaded JSON file
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading Swedish Research Council grants from SweCRIS API")
    print(f"{'='*60}")

    url = f"{SWECRIS_API_BASE}/projects/funders/{SWECRIS_FUNDER_ID}"
    print(f"  URL: {url}")
    print(f"  API Token: {SWECRIS_API_TOKEN[:10]}...")

    output_path = output_dir / "swecris_grants_raw.json"

    # Download with streaming for large response
    print("  [INFO] Downloading (this may take 2-3 minutes)...")
    headers = {"Authorization": f"Bearer {SWECRIS_API_TOKEN}"}
    response = requests.get(url, headers=headers, timeout=300)
    response.raise_for_status()

    # Save raw JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"\n  [SUCCESS] Downloaded to {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1e6:.1f} MB")

    return output_path


def parse_grants_json(json_path: Path) -> list[dict]:
    """
    Parse the SweCRIS grants JSON into a list of dictionaries.

    Args:
        json_path: Path to the JSON file

    Returns:
        List of grant dictionaries
    """
    print(f"\n{'='*60}")
    print("Step 2: Parsing JSON data")
    print(f"{'='*60}")

    import json
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"  Total projects: {len(data):,}")

    projects = []
    for row in data:
        # Extract PI info from peopleList
        pi_given_name = None
        pi_family_name = None
        pi_orcid = None
        people_list = row.get("peopleList", []) or []

        for person in people_list:
            if person.get("roleEn") == "Principal Investigator":
                full_name = person.get("fullName", "")
                # Try to split into given/family name
                if " " in full_name:
                    parts = full_name.rsplit(" ", 1)
                    pi_given_name = parts[0]
                    pi_family_name = parts[1]
                else:
                    pi_family_name = full_name
                pi_orcid = person.get("orcId")
                break

        # Extract subject classification
        scbs = row.get("scbs", []) or []
        main_discipline = None
        main_discipline_level1 = None
        if scbs:
            main_discipline = scbs[0].get("scb5NameEn")
            main_discipline_level1 = scbs[0].get("scb1NameEn")

        project = {
            "project_id": row.get("projectId"),
            "title": row.get("projectTitleSv"),
            "title_english": row.get("projectTitleEn"),
            "abstract": row.get("projectAbstractSv"),
            "abstract_english": row.get("projectAbstractEn"),
            "start_date": row.get("projectStartDate"),
            "end_date": row.get("projectEndDate"),
            "coordinating_organisation_id": row.get("coordinatingOrganisationId"),
            "coordinating_organisation": row.get("coordinatingOrganisationNameEn") or row.get("coordinatingOrganisationNameSv"),
            "coordinating_organisation_type": row.get("coordinatingOrganisationTypeOfOrganisationEn"),
            "funding_organisation_id": row.get("fundingOrganisationId"),
            "funding_organisation": row.get("fundingOrganisationNameEn") or row.get("fundingOrganisationNameSv"),
            "amount": row.get("fundingsSek"),
            "funding_year": row.get("fundingYear"),
            "funding_start_date": row.get("fundingStartDate"),
            "funding_end_date": row.get("fundingEndDate"),
            "type_of_award_id": row.get("typeOfAwardId"),
            "type_of_award": row.get("typeOfAwardDescrEn") or row.get("typeOfAwardDescrSv"),
            "pi_given_name": pi_given_name,
            "pi_family_name": pi_family_name,
            "pi_orcid": pi_orcid,
            "main_discipline": main_discipline,
            "main_discipline_level1": main_discipline_level1,
            "updated_date": row.get("updatedDate"),
            "loaded_date": row.get("loadedDate"),
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

    # Parse and convert dates to string format (YYYY-MM-DD)
    print("  [INFO] Converting dates to string format...")
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            # Parse datetime format "YYYY-MM-DD HH:MM:SS" and convert to YYYY-MM-DD
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    # Convert amount to float
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Remove duplicates by project_id (keep first)
    print("  [INFO] Deduplicating by project_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "swedish_research_council_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With English title: {df['title_english'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With ORCID: {df['pi_orcid'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With abstract: {df['abstract'].notna().sum():,}")

    if "type_of_award" in df.columns:
        print(f"\n  Award types:")
        print(df["type_of_award"].value_counts().head(10).to_string())

    if "coordinating_organisation" in df.columns:
        print(f"\n  Top institutions (top 10):")
        print(df["coordinating_organisation"].value_counts().head(10).to_string())

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
        description="Download Swedish Research Council grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./swedish_research_council_data"),
        help="Directory for downloaded/processed files (default: ./swedish_research_council_data)"
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
    print("Swedish Research Council (Vetenskapsrådet) to S3 Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        json_path = download_grants_from_api(args.output_dir)
    else:
        json_path = args.output_dir / "swecris_grants_raw.json"
        if not json_path.exists():
            print("[ERROR] No existing JSON found. Run without --skip-download")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing JSON: {json_path}")

    # Step 2: Parse JSON
    projects = parse_grants_json(json_path)

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
    print(f"  In Databricks, run: notebooks/awards/CreateSwedishResearchCouncilAwards.ipynb")


if __name__ == "__main__":
    main()