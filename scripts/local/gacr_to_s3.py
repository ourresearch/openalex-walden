#!/usr/bin/env python3
"""
GACR (Czech Science Foundation) to S3 Data Pipeline
====================================================

This script downloads Czech Science Foundation (GACR) grants data from the IS VaVaI
(Czech Research Information System) open data portal and uploads it to S3 for
Databricks ingestion.

Data Source: IS VaVaI Open Data (https://www.isvavai.cz/opendata)
- CEP-projekty.csv: All Czech research projects (~160MB)
- CEP-ucastnici.csv: Project participants with ROR IDs (~15MB)
- We filter for provider code "GA0" (GACR)

Output: s3://openalex-ingest/awards/gacr/gacr_projects.parquet

What this script does:
1. Downloads CEP-projekty.csv and CEP-ucastnici.csv from IS VaVaI
2. Filters projects where poskytovatel = "GA0" (GACR)
3. Joins participant data to get ROR IDs for institutions
4. Converts to parquet format
5. Uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/gacr/

Usage:
    python gacr_to_s3.py

    # Skip S3 upload (for testing):
    python gacr_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# IS VaVaI Open Data URLs
PROJECTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-projekty.csv"
PARTICIPANTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-ucastnici.csv"

# GACR provider code in IS VaVaI
GACR_PROVIDER_CODE = "GA0"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gacr/gacr_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_csv(url: str, description: str) -> pd.DataFrame:
    """Download CSV file from URL with progress reporting."""
    print(f"\nDownloading {description}...")
    print(f"  URL: {url}")

    start_time = time.time()

    # Stream the download to report progress
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    # Get file size if available
    total_size = int(response.headers.get('content-length', 0))
    if total_size:
        print(f"  Size: {total_size / 1024 / 1024:.1f} MB")

    # Download content
    chunks = []
    downloaded = 0
    last_report = time.time()

    for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
        chunks.append(chunk)
        downloaded += len(chunk)

        # Report progress every 5 seconds
        if time.time() - last_report > 5:
            if total_size:
                pct = downloaded / total_size * 100
                print(f"  Progress: {downloaded / 1024 / 1024:.1f} MB ({pct:.1f}%)")
            else:
                print(f"  Progress: {downloaded / 1024 / 1024:.1f} MB")
            last_report = time.time()

    content = b''.join(chunks)
    elapsed = time.time() - start_time
    print(f"  Downloaded {len(content) / 1024 / 1024:.1f} MB in {elapsed:.1f}s")

    # Parse CSV - handle Czech encoding
    text = content.decode('utf-8')
    df = pd.read_csv(StringIO(text), dtype=str)
    print(f"  Parsed {len(df):,} rows, {len(df.columns)} columns")

    return df


# =============================================================================
# Data Processing
# =============================================================================

def process_gacr_data(projects_df: pd.DataFrame, participants_df: pd.DataFrame) -> pd.DataFrame:
    """Filter for GACR projects and enrich with participant data."""
    print("\nProcessing GACR data...")

    # Filter for GACR projects
    gacr_projects = projects_df[projects_df['poskytovatel'] == GACR_PROVIDER_CODE].copy()
    print(f"  GACR projects: {len(gacr_projects):,} (of {len(projects_df):,} total)")

    # Filter participants for GACR projects
    gacr_participants = participants_df[participants_df['poskytovatel'] == GACR_PROVIDER_CODE].copy()
    print(f"  GACR participants: {len(gacr_participants):,}")

    # Get lead participant (hlavni_prijemce) ROR for each project
    # The hlavni_prijemce column in projects contains something like "ico:12345678"
    # We need to match this with the ucastnik column in participants

    # Create a lookup for ROR by project code + hlavni_prijemce
    participant_ror = gacr_participants[['kod_projektu', 'ucastnik', 'ROR', 'nazev_ucastnika']].drop_duplicates()
    participant_ror = participant_ror.rename(columns={
        'ROR': 'lead_ror',
        'nazev_ucastnika': 'lead_org_name_participant'
    })

    # Merge to get ROR for lead institution
    # Match on project code and hlavni_prijemce = ucastnik
    gacr_projects = gacr_projects.merge(
        participant_ror,
        left_on=['kod_projektu', 'hlavni_prijemce'],
        right_on=['kod_projektu', 'ucastnik'],
        how='left'
    )

    # Keep relevant columns and rename for clarity
    columns_to_keep = [
        'kod_projektu',
        'odkaz_na_isvavai.cz',
        'nazev_projektu_originalni',
        'nazev_projektu_anglicky',
        'rok_zahajeni',
        'rok_ukonceni',
        'datum_zahajeni',
        'datum_ukonceni',
        'cile_reseni_originalni',
        'cile_reseni_anglicky',
        'klicova_slova_anglicky',
        'hlavni_obor',
        'hlavni_vedni_obor_oecd',
        'kod_programu',
        'nazev_organizace',
        'ico',
        'mesto_sidla',
        'zeme_sidla',
        'lead_ror',
        'celkove_naklady_na_dobu_reseni',
        'statni_podpora_na_dobu_reseni',
        'podpora_z_verejnych_zahranicnich_zdroju',
        'posledni_stav_projektu',
        'kod_hodnoceni',
    ]

    # Only keep columns that exist
    columns_to_keep = [c for c in columns_to_keep if c in gacr_projects.columns]
    gacr_projects = gacr_projects[columns_to_keep]

    # Rename columns to English
    gacr_projects = gacr_projects.rename(columns={
        'kod_projektu': 'project_code',
        'odkaz_na_isvavai.cz': 'landing_page_url',
        'nazev_projektu_originalni': 'title_cs',
        'nazev_projektu_anglicky': 'title_en',
        'rok_zahajeni': 'start_year',
        'rok_ukonceni': 'end_year',
        'datum_zahajeni': 'start_date',
        'datum_ukonceni': 'end_date',
        'cile_reseni_originalni': 'description_cs',
        'cile_reseni_anglicky': 'description_en',
        'klicova_slova_anglicky': 'keywords',
        'hlavni_obor': 'research_field',
        'hlavni_vedni_obor_oecd': 'oecd_field',
        'kod_programu': 'program_code',
        'nazev_organizace': 'institution_name',
        'ico': 'institution_id',
        'mesto_sidla': 'institution_city',
        'zeme_sidla': 'institution_country',
        'lead_ror': 'ror_id',
        'celkove_naklady_na_dobu_reseni': 'total_cost',
        'statni_podpora_na_dobu_reseni': 'funding_amount',
        'podpora_z_verejnych_zahranicnich_zdroju': 'foreign_funding',
        'posledni_stav_projektu': 'status',
        'kod_hodnoceni': 'evaluation_code',
    })

    # Convert numeric columns
    for col in ['total_cost', 'funding_amount', 'foreign_funding']:
        if col in gacr_projects.columns:
            gacr_projects[col] = pd.to_numeric(gacr_projects[col], errors='coerce')

    # Use English title if available, fall back to Czech
    gacr_projects['title'] = gacr_projects['title_en'].fillna(gacr_projects['title_cs'])
    gacr_projects['description'] = gacr_projects['description_en'].fillna(gacr_projects['description_cs'])

    print(f"\nFinal dataset: {len(gacr_projects):,} projects")

    return gacr_projects


# =============================================================================
# Upload to S3
# =============================================================================

def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    """Upload file to S3 using AWS CLI."""
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")

    try:
        result = subprocess.run(
            ["aws", "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Download GACR projects from IS VaVaI to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/gacr_download"),
                        help="Directory for output files")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (for testing)")
    args = parser.parse_args()

    print("=" * 60)
    print("GACR (Czech Science Foundation) Data Download")
    print("=" * 60)
    print(f"Source: IS VaVaI Open Data")
    print(f"Output: s3://{S3_BUCKET}/{S3_KEY}")
    print(f"Local dir: {args.output_dir}")
    print("=" * 60)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Download data files
    projects_df = download_csv(PROJECTS_URL, "CEP-projekty.csv (all Czech research projects)")
    participants_df = download_csv(PARTICIPANTS_URL, "CEP-ucastnici.csv (project participants)")

    # Process GACR data
    gacr_df = process_gacr_data(projects_df, participants_df)

    if gacr_df.empty:
        print("No GACR projects found!")
        return 1

    # Save to parquet
    output_path = args.output_dir / "gacr_projects.parquet"
    gacr_df.to_parquet(output_path, index=False)
    print(f"\nSaved to {output_path}")
    print(f"  Rows: {len(gacr_df):,}")
    print(f"  Columns: {list(gacr_df.columns)}")

    # Show sample
    print("\nSample data:")
    sample_cols = ['project_code', 'title', 'funding_amount', 'start_year']
    sample_cols = [c for c in sample_cols if c in gacr_df.columns]
    print(gacr_df[sample_cols].head(3).to_string())

    # Summary stats
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    print(f"Total projects: {len(gacr_df):,}")

    if 'funding_amount' in gacr_df.columns:
        has_amount = gacr_df['funding_amount'].notna().sum()
        print(f"Projects with funding amount: {has_amount:,}")
        if gacr_df['funding_amount'].notna().any():
            total_funding = gacr_df['funding_amount'].sum()
            print(f"Total funding: CZK {total_funding:,.0f} (~EUR {total_funding/25:,.0f})")

    if 'start_year' in gacr_df.columns:
        print(f"Year range: {gacr_df['start_year'].min()} to {gacr_df['start_year'].max()}")

    if 'ror_id' in gacr_df.columns:
        has_ror = gacr_df['ror_id'].notna().sum()
        pct_ror = has_ror / len(gacr_df) * 100
        print(f"Projects with ROR ID: {has_ror:,} ({pct_ror:.1f}%)")

    # Upload to S3
    if not args.skip_upload:
        if upload_to_s3(output_path, S3_BUCKET, S3_KEY):
            print("\nSuccess! Data is ready for Databricks ingestion.")
            return 0
        else:
            print("\nUpload failed. Data saved locally.")
            return 1
    else:
        print("\nSkipped S3 upload (--skip-upload)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
