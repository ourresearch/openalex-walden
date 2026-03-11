#!/usr/bin/env python3
"""
Academy of Finland (Research Council of Finland) to S3 Data Pipeline
=====================================================================

This script downloads all Finnish research grant data from the Research.fi API,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://research.fi/en/results/fundings?funder=0245893-9
API: https://researchfi-api-production.2.rahtiapp.fi/portalapi/funding/_search
Output: s3://openalex-ingest/awards/academy_of_finland/academy_of_finland_projects.parquet

What this script does:
1. Queries the Research.fi Elasticsearch API for all Academy of Finland fundings
2. Uses pagination to fetch all ~5,400 grants
3. Extracts project metadata including PI, amounts, dates, fields of science
4. Saves as parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/academy_of_finland/

Usage:
    python academy_of_finland_to_s3.py

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# Research.fi API settings
API_URL = "https://researchfi-api-production.2.rahtiapp.fi/portalapi/funding/_search"
FUNDER_ORG_ID = "02458939"  # Academy of Finland / Research Council of Finland
PAGE_SIZE = 500  # Results per request (ES max is usually 10000)
REQUEST_DELAY = 0.2  # Seconds between requests

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/academy_of_finland/academy_of_finland_projects.parquet"


# =============================================================================
# API Functions
# =============================================================================

def get_total_count() -> int:
    """Get total number of grants from API."""
    query = {
        "query": {
            "match_phrase": {
                "funderOrganizationId": FUNDER_ORG_ID
            }
        },
        "size": 0
    }

    response = requests.post(API_URL, json=query, timeout=60)
    response.raise_for_status()
    data = response.json()

    return data["hits"]["total"]["value"]


def fetch_page(from_offset: int, size: int = PAGE_SIZE) -> list[dict]:
    """Fetch a page of grants from the API."""
    query = {
        "query": {
            "match_phrase": {
                "funderOrganizationId": FUNDER_ORG_ID
            }
        },
        "from": from_offset,
        "size": size
    }

    response = requests.post(API_URL, json=query, timeout=60)
    response.raise_for_status()
    data = response.json()

    return [hit["_source"] for hit in data["hits"]["hits"]]


def parse_grant(grant: dict) -> dict:
    """Parse a single grant into our schema."""

    # Extract PI info from fundingGroupPerson if available
    pi_given_name = grant.get("fundingContactPersonFirstNames")
    pi_family_name = grant.get("fundingContactPersonLastName")
    pi_orcid = None
    pi_affiliation_name = None
    pi_affiliation_country = None

    # Try to get more PI details from fundingGroupPerson
    funding_group = grant.get("fundingGroupPerson", [])
    if funding_group and isinstance(funding_group, list):
        for person in funding_group:
            if isinstance(person, dict):
                # Get ORCID if available
                if not pi_orcid and person.get("orcid"):
                    pi_orcid = person.get("orcid")
                # Get affiliation
                if not pi_affiliation_name and person.get("organizationNameEn"):
                    pi_affiliation_name = person.get("organizationNameEn") or person.get("organizationNameFi")
                    pi_affiliation_country = "Finland"  # Research.fi is Finland-focused
                break  # Just get first person's details

    # Extract fields of science
    fields_of_science = grant.get("fieldsOfScience", [])
    fos_names = []
    if fields_of_science and isinstance(fields_of_science, list):
        for fos in fields_of_science:
            if isinstance(fos, dict):
                name = fos.get("nameEn") or fos.get("nameFi")
                if name:
                    fos_names.append(name)

    # Parse dates
    start_year = grant.get("fundingStartYear")
    end_year = grant.get("fundingEndYear")

    # Build start_date from year
    start_date = None
    if start_year:
        try:
            start_date = f"{int(start_year)}-01-01"
        except (ValueError, TypeError):
            pass

    # Build end_date from fundingEndDate or year
    end_date_raw = grant.get("fundingEndDate")
    end_date = None
    if end_date_raw:
        # Try to parse various date formats
        for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%Y"]:
            try:
                parsed = datetime.strptime(str(end_date_raw)[:10], fmt)
                end_date = parsed.strftime("%Y-%m-%d")
                break
            except (ValueError, TypeError):
                pass
    if not end_date and end_year:
        try:
            end_date = f"{int(end_year)}-12-31"
        except (ValueError, TypeError):
            pass

    return {
        "project_id": grant.get("projectId"),
        "funder_project_number": grant.get("funderProjectNumber"),
        "title_en": grant.get("projectNameEn"),
        "title_fi": grant.get("projectNameFi"),
        "description_en": grant.get("projectDescriptionEn"),
        "description_fi": grant.get("projectDescriptionFi"),
        "amount_eur": grant.get("amount_in_EUR"),
        "start_date": start_date,
        "end_date": end_date,
        "start_year": start_year,
        "end_year": end_year,
        "funding_type": grant.get("typeOfFundingNameEn"),
        "call_programme": grant.get("callProgrammeNameEn"),
        "pi_given_name": pi_given_name,
        "pi_family_name": pi_family_name,
        "pi_orcid": pi_orcid,
        "pi_affiliation_name": pi_affiliation_name,
        "pi_affiliation_country": pi_affiliation_country,
        "fields_of_science": json.dumps(fos_names) if fos_names else None,
        "homepage": grant.get("projectHomepage"),
        "funder_name": grant.get("funderNameEn"),
        "funder_org_id": grant.get("funderOrganizationId"),
    }


def download_all_grants() -> list[dict]:
    """Download all grants from the API."""
    print(f"\n{'='*60}")
    print("Step 1: Downloading grants from Research.fi API")
    print(f"{'='*60}")

    # Get total count
    print("  [INFO] Fetching total count...")
    total = get_total_count()
    print(f"  [INFO] Total grants: {total:,}")

    all_grants = []
    offset = 0
    start_time = time.time()

    while offset < total:
        try:
            grants = fetch_page(offset)
            if not grants:
                break

            parsed = [parse_grant(g) for g in grants]
            all_grants.extend(parsed)

            # Progress report
            elapsed = time.time() - start_time
            pct = (len(all_grants) / total) * 100
            rate = len(all_grants) / elapsed if elapsed > 0 else 0
            eta = (total - len(all_grants)) / rate if rate > 0 else 0

            print(f"  [{len(all_grants):,}/{total:,}] ({pct:.1f}%) "
                  f"Rate: {rate:.1f}/s, ETA: {timedelta(seconds=int(eta))}")

            offset += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"  [ERROR] Failed at offset {offset}: {e}")
            # Try to continue from next page
            offset += PAGE_SIZE
            time.sleep(1)

    print(f"\n  [DONE] Downloaded {len(all_grants):,} grants")
    return all_grants


# =============================================================================
# Processing Functions
# =============================================================================

def process_grants(grants: list[dict], output_dir: Path) -> Path:
    """Process grants into a parquet file."""
    print(f"\n{'='*60}")
    print("Step 2: Processing grants")
    print(f"{'='*60}")

    if not grants:
        raise ValueError("No grants to process!")

    df = pd.DataFrame(grants)
    print(f"  Total rows: {len(df):,}")

    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "academy_of_finland_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With title (EN): {df['title_en'].notna().sum():,}")
    print(f"    - With description: {df['description_en'].notna().sum():,}")
    print(f"    - With amount: {df['amount_eur'].notna().sum():,}")
    print(f"    - With PI name: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With ORCID: {df['pi_orcid'].notna().sum():,}")
    print(f"    - With start year: {df['start_year'].notna().sum():,}")

    if "funding_type" in df.columns:
        print(f"\n  Funding types:")
        print(df["funding_type"].value_counts().head(10).to_string())

    if "start_year" in df.columns:
        print(f"\n  Year distribution:")
        year_counts = df["start_year"].dropna().astype(int).value_counts().sort_index()
        print(year_counts.tail(10).to_string())

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
        description="Download Academy of Finland grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./academy_of_finland_data"),
        help="Directory for downloaded/processed files"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Academy of Finland to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    grants = download_all_grants()

    if not grants:
        print("[ERROR] No grants downloaded!")
        sys.exit(1)

    # Step 2: Process
    parquet_path = process_grants(grants, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateAcademyOfFinlandAwards.ipynb")


if __name__ == "__main__":
    main()
