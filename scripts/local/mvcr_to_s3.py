#!/usr/bin/env python3
"""
Czech Ministry of the Interior (MV0) to S3 Data Pipeline
====================================================

This script downloads Czech Ministry of the Interior (MV0) grants data from the IS VaVaI
(Czech Research Information System) open data portal and uploads it to S3 for
Databricks ingestion.

Data Source: IS VaVaI Open Data (https://www.isvavai.cz/opendata)
- CEP-projekty.csv: All Czech research projects (~160MB)
- CEP-ucastnici.csv: Project participants with ROR IDs (~15MB)
- We filter for provider code "MV0" (Czech Ministry of the Interior)

Output: s3://openalex-ingest/awards/mvcr/mvcr_projects.parquet

What this script does:
1. Downloads CEP-projekty.csv and CEP-ucastnici.csv from IS VaVaI
2. Filters projects where poskytovatel = "MV0" (Czech Ministry of the Interior)
3. Joins participant data to get ROR IDs for institutions
4. Converts to parquet format
5. Uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/mvcr/

Usage:
    python mvcr_to_s3.py

    # Skip S3 upload (for testing):
    python mvcr_to_s3.py --skip-upload

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

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

# =============================================================================
# Configuration
# =============================================================================

# IS VaVaI Open Data URLs
PROJECTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-projekty.csv"
PARTICIPANTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-ucastnici.csv"

# MV0 provider code in IS VaVaI
MV0_PROVIDER_CODE = "MV0"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mvcr/mvcr_projects.parquet"


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

def process_mvcr_data(projects_df: pd.DataFrame, participants_df: pd.DataFrame) -> pd.DataFrame:
    """Filter for MV0 projects and enrich with participant data."""
    print("\nProcessing MV0 data...")

    # Filter for MV0 projects
    mvcr_projects = projects_df[projects_df['poskytovatel'] == MV0_PROVIDER_CODE].copy()
    print(f"  MV0 projects: {len(mvcr_projects):,} (of {len(projects_df):,} total)")

    # Filter participants for MV0 projects
    mvcr_participants = participants_df[participants_df['poskytovatel'] == MV0_PROVIDER_CODE].copy()
    print(f"  MV0 participants: {len(mvcr_participants):,}")

    # Get lead participant (hlavni_prijemce) ROR for each project
    # The hlavni_prijemce column in projects contains something like "ico:12345678"
    # We need to match this with the ucastnik column in participants

    # Create a lookup for ROR by project code + hlavni_prijemce
    participant_ror = mvcr_participants[['kod_projektu', 'ucastnik', 'ROR', 'nazev_ucastnika']].drop_duplicates()
    participant_ror = participant_ror.rename(columns={
        'ROR': 'lead_ror',
        'nazev_ucastnika': 'lead_org_name_participant'
    })

    # Merge to get ROR for lead institution
    # Match on project code and hlavni_prijemce = ucastnik
    mvcr_projects = mvcr_projects.merge(
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
    columns_to_keep = [c for c in columns_to_keep if c in mvcr_projects.columns]
    mvcr_projects = mvcr_projects[columns_to_keep]

    # Rename columns to English
    mvcr_projects = mvcr_projects.rename(columns={
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
        if col in mvcr_projects.columns:
            mvcr_projects[col] = pd.to_numeric(mvcr_projects[col], errors='coerce')

    # Use English title if available, fall back to Czech
    mvcr_projects['title'] = mvcr_projects['title_en'].fillna(mvcr_projects['title_cs'])
    mvcr_projects['description'] = mvcr_projects['description_en'].fillna(mvcr_projects['description_cs'])

    print(f"\nFinal dataset: {len(mvcr_projects):,} projects")

    return mvcr_projects


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
    parser = argparse.ArgumentParser(description="Download MV0 projects from IS VaVaI to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/mvcr_download"),
                        help="Directory for output files")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (for testing)")
    args = parser.parse_args()

    print("=" * 60)
    print("Czech Ministry of the Interior (MV0) Data Download")
    print("=" * 60)
    print(f"Source: IS VaVaI Open Data")
    print(f"Output: s3://{S3_BUCKET}/{S3_KEY}")
    print(f"Local dir: {args.output_dir}")
    print("=" * 60)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Download data files
    projects_df = download_csv(PROJECTS_URL, "CEP-projekty.csv (all Czech research projects)")
    participants_df = download_csv(PARTICIPANTS_URL, "CEP-ucastnici.csv (project participants)")

    # Process MV0 data
    mvcr_df = process_mvcr_data(projects_df, participants_df)

    if mvcr_df.empty:
        print("No MV0 projects found!")
        return 1

    # Save to parquet
    output_path = args.output_dir / "mvcr_projects.parquet"
    mvcr_df.to_parquet(output_path, index=False)
    print(f"\nSaved to {output_path}")
    print(f"  Rows: {len(mvcr_df):,}")
    print(f"  Columns: {list(mvcr_df.columns)}")

    # Show sample
    print("\nSample data:")
    sample_cols = ['project_code', 'title', 'funding_amount', 'start_year']
    sample_cols = [c for c in sample_cols if c in mvcr_df.columns]
    print(mvcr_df[sample_cols].head(3).to_string())

    # Summary stats
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    print(f"Total projects: {len(mvcr_df):,}")

    if 'funding_amount' in mvcr_df.columns:
        has_amount = mvcr_df['funding_amount'].notna().sum()
        print(f"Projects with funding amount: {has_amount:,}")
        if mvcr_df['funding_amount'].notna().any():
            total_funding = mvcr_df['funding_amount'].sum()
            print(f"Total funding: CZK {total_funding:,.0f} (~EUR {total_funding/25:,.0f})")

    if 'start_year' in mvcr_df.columns:
        print(f"Year range: {mvcr_df['start_year'].min()} to {mvcr_df['start_year'].max()}")

    if 'ror_id' in mvcr_df.columns:
        has_ror = mvcr_df['ror_id'].notna().sum()
        pct_ror = has_ror / len(mvcr_df) * 100
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
