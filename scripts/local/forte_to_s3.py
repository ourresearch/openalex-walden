#!/usr/bin/env python3
"""
Forte (Swedish Research Council for Health, Working Life and Welfare) to S3 Pipeline
=====================================================================================

This script downloads Forte grant data from SweCRIS,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: SweCRIS API (https://swecris-api.vr.se)
Output: s3://openalex-ingest/awards/forte/forte_projects.parquet

Requirements:
    pip install pandas pyarrow requests

Usage:
    python forte_to_s3.py

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

SWECRIS_API_BASE = "https://swecris-api.vr.se/v1"
SWECRIS_FUNDER_ID = "202100-5240"  # Forte
SWECRIS_API_TOKEN = "VRSwecrisAPI2025-1"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/forte/forte_projects.parquet"


def download_grants_from_api(output_dir: Path) -> Path:
    print(f"\n{'='*60}")
    print("Step 1: Downloading Forte grants from SweCRIS API")
    print(f"{'='*60}")

    url = f"{SWECRIS_API_BASE}/projects/funders/{SWECRIS_FUNDER_ID}"
    print(f"  URL: {url}")

    output_path = output_dir / "forte_grants_raw.json"

    print("  [INFO] Downloading...")
    headers = {"Authorization": f"Bearer {SWECRIS_API_TOKEN}"}
    response = requests.get(url, headers=headers, timeout=600)
    response.raise_for_status()

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"  [SUCCESS] Downloaded to {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1e6:.1f} MB")

    return output_path


def parse_grants_json(json_path: Path) -> list[dict]:
    print(f"\n{'='*60}")
    print("Step 2: Parsing JSON data")
    print(f"{'='*60}")

    import json
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"  Total projects: {len(data):,}")

    projects = []
    for row in data:
        pi_given_name = None
        pi_family_name = None
        pi_orcid = None
        people_list = row.get("peopleList", []) or []

        for person in people_list:
            if person.get("roleEn") == "Principal Investigator":
                full_name = person.get("fullName", "")
                if " " in full_name:
                    parts = full_name.rsplit(" ", 1)
                    pi_given_name = parts[0]
                    pi_family_name = parts[1]
                else:
                    pi_family_name = full_name
                pi_orcid = person.get("orcId")
                break

        scbs = row.get("scbs", []) or []
        main_discipline = scbs[0].get("scb5NameEn") if scbs else None
        main_discipline_level1 = scbs[0].get("scb1NameEn") if scbs else None

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


def process_projects(projects: list[dict], output_dir: Path) -> Path:
    print(f"\n{'='*60}")
    print("Step 3: Processing projects")
    print(f"{'='*60}")

    if not projects:
        raise ValueError("No projects to process!")

    df = pd.DataFrame(projects)
    print(f"  Total rows: {len(df):,}")

    for col in ["start_date", "end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    original_count = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    output_path = output_dir / "forte_projects.parquet"
    df.to_parquet(output_path, index=False)

    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With English title: {df['title_english'].notna().sum():,}")
    print(f"    - With PI: {df['pi_family_name'].notna().sum():,}")
    print(f"    - With ORCID: {df['pi_orcid'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")

    if "type_of_award" in df.columns:
        print(f"\n  Award types:")
        print(df["type_of_award"].value_counts().head(10).to_string())

    return output_path


def upload_to_s3(local_path: Path) -> bool:
    print(f"\n{'='*60}")
    print("Step 4: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    try:
        subprocess.run(
            ["aws", "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True,
            env={**subprocess.os.environ, "AWS_PROFILE": "openalex"}
        )
        print("  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Download Forte grants and upload to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./forte_data"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Forte (Swedish Research Council for Health, Working Life)")
    print("=" * 60)

    if not args.skip_download:
        json_path = download_grants_from_api(args.output_dir)
    else:
        json_path = args.output_dir / "forte_grants_raw.json"
        if not json_path.exists():
            print("[ERROR] No existing JSON found.")
            sys.exit(1)

    projects = parse_grants_json(json_path)
    if not projects:
        print("[ERROR] No projects found!")
        sys.exit(1)

    parquet_path = process_projects(projects, args.output_dir)

    if not args.skip_upload:
        upload_to_s3(parquet_path)

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print(f"{'='*60}")
    print(f"\nNext: Run notebooks/awards/CreateForteAwards.ipynb")


if __name__ == "__main__":
    main()