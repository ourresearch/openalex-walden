#!/usr/bin/env python3
"""
CIHR (Canadian Institutes of Health Research) to S3 Data Pipeline
==================================================================

This script downloads all grants from the CIHR open data portal,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://open.canada.ca/data/en/dataset/49edb1d7-5cb4-4fa7-897c-515d1aad5da3
Output: s3://openalex-ingest/awards/cihr/cihr_projects.parquet

What this script does:
1. Downloads all fiscal year XLSX files from CIHR open data (2000-01 to 2024-25)
2. Extracts the "Grants & Awards" sheet from each file
3. Combines all years and deduplicates by funding reference number
4. Converts to parquet format
5. Uploads to S3

Notes:
- CIHR data uses bilingual column names (English_French)
- Each row is a funding record, awards may appear in multiple fiscal years
- We deduplicate by FundingReferenceNumber, keeping most recent data
- Data includes investigator names with given/family name separation

Requirements:
    pip install pandas pyarrow requests openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/cihr/

Usage:
    python cihr_to_s3.py

    # Or with options:
    python cihr_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Global verbosity flag
VERBOSE = False
DEFAULT_TIMEOUT = 60  # seconds per download (XLSX files are larger)

# =============================================================================
# Configuration
# =============================================================================

# CIHR XLSX URLs - organized by fiscal year
# URLs from: https://open.canada.ca/data/en/dataset/49edb1d7-5cb4-4fa7-897c-515d1aad5da3
BASE_URL = "https://open.canada.ca/data/dataset/49edb1d7-5cb4-4fa7-897c-515d1aad5da3/resource"

INVESTMENT_URLS = {
    "2024-25": f"{BASE_URL}/b9d0adcd-a898-4e70-9dca-5b96db76ffe3/download/cihr_investments_investissements_irsc_202425.xlsx",
    "2023-24": f"{BASE_URL}/e712481d-2e86-4c3a-88ef-e047974665f0/download/cihr_investments_investissements_irsc_202324.xlsx",
    "2022-23": f"{BASE_URL}/66aff71a-a637-4810-ad0d-23b956323bc3/download/cihr_investments_investissements_irsc_202223.xlsx",
    "2021-22": f"{BASE_URL}/5fd71a96-66e0-463a-a45d-ac768f5fa7c5/download/cihr_investments_investissements_irsc_202122.xlsx",
    "2020-21": f"{BASE_URL}/c0f671dd-f339-42ac-bfd9-0e804bbaf4bc/download/cihr_investments_investissements_irsc_202021.xlsx",
    "2019-20": f"{BASE_URL}/9ee78927-fb7c-4240-86ed-1e1e35c55929/download/cihr_investments_investissements_irsc_201920.xlsx",
    "2018-19": f"{BASE_URL}/1f858bcc-cfab-4936-8393-39e4f50a3422/download/cihr_investments_investissements_irsc_201819.xlsx",
    "2017-18": f"{BASE_URL}/680131d3-9141-4a68-a3c4-990c1fd020b1/download/cihr_investments_investissements_irsc_201718.xlsx",
    "2016-17": f"{BASE_URL}/c84aaa9b-046e-4ba3-a79e-7fc97e451d04/download/cihr_investments_investissements_irsc_201617.xlsx",
    "2015-16": f"{BASE_URL}/2842059b-4ec4-4fc0-af29-20d9e3a64ce8/download/cihr_investments_investissements_irsc_201516.xlsx",
    "2014-15": f"{BASE_URL}/cc76bb70-b7cf-41f1-9721-752cb6a42dd7/download/cihr_investments_investissements_irsc_201415.xlsx",
    "2013-14": f"{BASE_URL}/4eeada9b-b3f9-4824-b6ea-54d4afd06bce/download/cihr_investments_investissements_irsc_201314.xlsx",
    "2012-13": f"{BASE_URL}/657a3dba-879b-4556-a121-220358e6016b/download/cihr_investments_investissements_irsc_201213.xlsx",
    "2011-12": f"{BASE_URL}/874444a0-5e2a-4f55-9281-ddd1f9582eba/download/cihr_investments_investissements_irsc_201112.xlsx",
    "2010-11": f"{BASE_URL}/5f0eed06-d8cf-42bb-827e-99accf74eda3/download/cihr_investments_investissements_irsc_201011.xlsx",
    "2009-10": f"{BASE_URL}/63f3a448-c641-4f13-805b-20e9c781febe/download/cihr_investments_investissements_irsc_200910.xlsx",
    "2008-09": f"{BASE_URL}/477454f7-4fe8-44f5-9229-b041347cdca8/download/cihr_investments_investissements_irsc_200809.xlsx",
    "2007-08": f"{BASE_URL}/2f43627a-fc9e-4e78-9053-fab0713c796d/download/cihr_investments_investissements_irsc_200708.xlsx",
    "2006-07": f"{BASE_URL}/423f08f6-9743-4439-9850-251dc30462fa/download/cihr_investments_investissements_irsc_200607.xlsx",
    "2005-06": f"{BASE_URL}/9a202a37-75d0-4364-a49d-41aa07f33a09/download/cihr_investments_investissements_irsc_200506.xlsx",
    "2004-05": f"{BASE_URL}/a624794e-6f39-443f-a21f-4c59b1b36fba/download/cihr_investments_investissements_irsc_200405.xlsx",
    "2003-04": f"{BASE_URL}/e55dfff9-c483-4bc8-9808-2b9d17e44e3f/download/cihr_investments_investissements_irsc_200304.xlsx",
    "2002-03": f"{BASE_URL}/0b30cd6e-0478-497b-9d4b-5d82ca6033e5/download/cihr_investments_investissements_irsc_200203.xlsx",
    "2001-02": f"{BASE_URL}/a0fb7d4a-816a-4e68-9f3a-e9378efbcb3f/download/cihr_investments_investissements_irsc_200102.xlsx",
    "2000-01": f"{BASE_URL}/8fea8468-0e10-4ee3-a33a-f49273c85fda/download/cihr_investments_investissements_irsc_200001.xlsx",
}

# Sheet name in XLSX files (bilingual: Grants&Awards_Subventions&Bourses)
SHEET_NAME = "G&A_S&B"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cihr/cihr_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_xlsx(url: str, output_path: Path, timeout: int = DEFAULT_TIMEOUT) -> bool:
    """
    Download an XLSX file from a URL.

    Args:
        url: URL to download
        output_path: Path to save the file
        timeout: Timeout in seconds

    Returns:
        True if download succeeded
    """
    start_time = time.time()

    if VERBOSE:
        print(f"      [DEBUG] Starting download (timeout={timeout}s)...", flush=True)

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (OpenAlex Data Ingestion)'}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            elapsed = time.time() - start_time
            if VERBOSE:
                print(f"      [DEBUG] Connection established in {elapsed:.1f}s, reading content...", flush=True)

            content = response.read()
            elapsed = time.time() - start_time

            if VERBOSE:
                print(f"      [DEBUG] Downloaded {len(content):,} bytes in {elapsed:.1f}s", flush=True)

            with open(output_path, 'wb') as f:
                f.write(content)

            return True

    except urllib.error.URLError as e:
        elapsed = time.time() - start_time
        if 'timed out' in str(e).lower():
            print(f"    [TIMEOUT] Download timed out after {elapsed:.1f}s", flush=True)
        else:
            print(f"    [ERROR] URL error after {elapsed:.1f}s: {e}", flush=True)
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"    [ERROR] Failed after {elapsed:.1f}s: {type(e).__name__}: {e}", flush=True)
        return False


def download_all_xlsx(output_dir: Path, timeout: int = DEFAULT_TIMEOUT) -> list[tuple[str, pd.DataFrame]]:
    """
    Download all CIHR investment XLSX files.

    Args:
        output_dir: Directory to save downloaded files
        timeout: Timeout in seconds for each download

    Returns:
        List of (fiscal_year, DataFrame) tuples
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 1: Downloading CIHR investment data", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Timeout per file: {timeout}s", flush=True)
    print(f"  Total files: {len(INVESTMENT_URLS)}", flush=True)

    all_data = []
    failed_years = []
    total_urls = len(INVESTMENT_URLS)
    start_time = time.time()

    for idx, (fiscal_year, url) in enumerate(sorted(INVESTMENT_URLS.items(), reverse=True), 1):
        elapsed = time.time() - start_time
        if idx > 1:
            avg_time = elapsed / (idx - 1)
            remaining = (total_urls - idx + 1) * avg_time
            eta_str = str(timedelta(seconds=int(remaining)))
        else:
            eta_str = "calculating..."

        filename = f"cihr_{fiscal_year.replace('-', '')}.xlsx"
        filepath = output_dir / filename
        print(f"  [{idx}/{total_urls}] FY{fiscal_year}: {filename}... ", end="", flush=True)

        # Check if already downloaded
        if filepath.exists():
            print(f"CACHED ", end="", flush=True)
        else:
            download_start = time.time()
            success = download_xlsx(url, filepath, timeout=timeout)
            download_elapsed = time.time() - download_start

            if not success:
                print(f"FAILED ({download_elapsed:.1f}s)", flush=True)
                failed_years.append((fiscal_year, url))
                continue

        # Parse XLSX
        try:
            df = pd.read_excel(filepath, sheet_name=SHEET_NAME, dtype=str)
            df['fiscal_year'] = fiscal_year
            all_data.append((fiscal_year, df))
            print(f"OK ({len(df):,} rows) [ETA: {eta_str}]", flush=True)

        except Exception as e:
            print(f"PARSE ERROR: {e}", flush=True)
            failed_years.append((fiscal_year, url))

    total_elapsed = time.time() - start_time
    print(f"\n  Summary:", flush=True)
    print(f"    - Downloaded: {len(all_data)} fiscal years", flush=True)
    print(f"    - Failed: {len(failed_years)} fiscal years", flush=True)
    print(f"    - Total time: {timedelta(seconds=int(total_elapsed))}", flush=True)
    if failed_years:
        print(f"    - Failed years:", flush=True)
        for fy, url in failed_years:
            print(f"        FY{fy}: {url}", flush=True)

    return all_data


# =============================================================================
# Processing Functions
# =============================================================================

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names by extracting English names from bilingual format.

    CIHR uses bilingual column names like "FamilyName_NomFamille"
    We extract the English part (before underscore with French).
    """
    # Map bilingual columns to clean English names
    column_mappings = {
        'fundingcode_codefinancement': 'funding_code',
        'fiscalyear_anneefinanciere': 'source_fiscal_year',
        'fundingreferencenumber_numeroreferencefinancement': 'funding_reference_number',
        'familyname_nomfamille': 'family_name',
        'firstname_prenom': 'first_name',
        'middlename_deuxiemeprenom': 'middle_name',
        'institutionpaidnameen_nometablissementpayean': 'institution_paid_name',
        'institutionpaidnamefr_nometablissementpayefr': 'institution_paid_name_fr',
        'institutionpaidcode_codeetablissementpaye': 'institution_paid_code',
        'researchinstitutionnameen_nometablissementrecherchean': 'research_institution_name',
        'researchinstitutionnamefr_nometablissementrecherchefr': 'research_institution_name_fr',
        'researchinstitutioncode_codeetablissementrecherche': 'research_institution_code',
        'researchinstitutiondepartment_departementetablissementrecherche': 'research_institution_department',
        'competitioncode_codeconcours': 'competition_code',
        'programnameen_nomprogrammean': 'program_name',
        'programnamefr_nomprogrammefr': 'program_name_fr',
        'programcode_codeprogramme': 'program_code',
        'programtypeen_typeprogrammean': 'program_type',
        'programtypefr_typeprogrammefr': 'program_type_fr',
        'committeenameen_nomcomitean': 'committee_name',
        'committeenamefr_nomcomitefr': 'committee_name_fr',
        'committeecode_codecomite': 'committee_code',
        'competitiondate_dateconcours': 'competition_date',
        'competitionfy_afconcours': 'competition_fiscal_year',
        'fundingstartdate_datepremierversement': 'funding_start_date',
        'fundingenddate_datedernierversement': 'funding_end_date',
        'amountpaidfy_montantpayeaf': 'amount_paid_fy',
        'totalamountpaid_montanttotalpaye': 'total_amount_paid',
        'totalamountawarded_montanttotalaccorde': 'total_amount_awarded',
        'primarythemeen_themeprincipalanl': 'primary_theme',
        'primarythemefr_themeprincipalfr': 'primary_theme_fr',
        'primaryinstituteen_institutprincipalanl': 'primary_institute',
        'primaryinstitutefr_institutprincipalfr': 'primary_institute_fr',
        'allresearchcategoriesen_toutescategoriesrecherchean': 'research_categories',
        'allresearchcategoriesfr_toutescategoriesrecherchefr': 'research_categories_fr',
        'applicationtitle_titredemande': 'application_title',
        'applicationabstract_resumedemande': 'application_abstract',
        'applicationkeywords_motsclesdemande': 'application_keywords',
        # Handle variations
        'primarythemeen_themeprincipalanl': 'primary_theme',
        'primarythemeen_themeprincipalanl': 'primary_theme',
        'primarythemeen_themeprincipalanl': 'primary_theme',
        'primaryinstituteten_institutprincipalanl': 'primary_institute',
        'primaryinstituteten_institutprincipalanl': 'primary_institute',
        'allresearchcategoriesen_toutescategoriesrechercheanl': 'research_categories',
    }

    # Lowercase all column names first
    df.columns = df.columns.str.lower().str.strip()

    # Apply mappings
    rename_dict = {}
    for col in df.columns:
        col_clean = col.replace(' ', '').replace('-', '').replace('é', 'e').replace('è', 'e')
        if col_clean in column_mappings:
            rename_dict[col] = column_mappings[col_clean]
        elif col not in ['fiscal_year']:
            # Try to extract English part (before French part with underscore)
            # Pattern: EnglishName_FrenchName
            rename_dict[col] = col

    df = df.rename(columns=rename_dict)
    return df


def process_data(all_data: list[tuple[str, pd.DataFrame]], output_dir: Path) -> Path:
    """
    Process all downloaded data into a single parquet file.

    Args:
        all_data: List of (fiscal_year, DataFrame) tuples
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}", flush=True)
    print("Step 2: Processing data", flush=True)
    print(f"{'='*60}", flush=True)

    if not all_data:
        raise ValueError("No data to process!")

    # Standardize and combine all DataFrames
    print("  [INFO] Standardizing column names...", flush=True)
    standardized_dfs = []
    for i, (fiscal_year, df) in enumerate(all_data):
        print(f"    Processing FY{fiscal_year} ({i+1}/{len(all_data)})...", end=" ", flush=True)
        df = standardize_columns(df)
        standardized_dfs.append(df)
        print(f"OK ({len(df):,} rows)", flush=True)

    # Combine all years
    print("  [INFO] Combining all fiscal years...", flush=True)
    combined = pd.concat(standardized_dfs, ignore_index=True)
    print(f"  Total rows (all records): {len(combined):,}", flush=True)

    # Show available columns
    print(f"\n  Available columns ({len(combined.columns)}):", flush=True)
    for col in combined.columns:
        print(f"    - {col}", flush=True)

    # Find the funding reference number column
    ref_col = None
    for col in combined.columns:
        if 'fundingreferencenumber' in col.lower().replace('_', ''):
            ref_col = col
            break
    if ref_col is None:
        ref_col = 'funding_reference_number'

    print(f"\n  [INFO] Using '{ref_col}' as unique award ID", flush=True)

    # Clean amount columns
    print("  [INFO] Parsing amounts...", flush=True)
    amount_cols = ['total_amount_awarded', 'total_amount_paid', 'amount_paid_fy',
                   'totalamountawarded_montanttotalaccorde', 'totalamountpaid_montanttotalpaye']
    for col in amount_cols:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors='coerce')
            print(f"    Parsed {col}", flush=True)

    # Deduplicate by funding reference number (keep most recent by fiscal year)
    print("  [INFO] Deduplicating by funding reference number...", flush=True)
    print("    Sorting by fiscal year (descending)...", flush=True)
    combined = combined.sort_values('fiscal_year', ascending=False)

    # For each funding reference, keep the first (most recent) row
    print("    Dropping duplicates...", flush=True)
    if ref_col in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[ref_col], keep='first')
        after = len(combined)
        print(f"    Deduplicated: {before:,} -> {after:,} unique awards", flush=True)
    else:
        print(f"    [WARN] Could not find reference column, skipping deduplication", flush=True)

    # Parse dates
    print("  [INFO] Parsing dates...", flush=True)
    date_cols = ['funding_start_date', 'funding_end_date', 'competition_date',
                 'fundingstartdate_datepremierversement', 'fundingenddate_datedernierversement']
    for col in date_cols:
        if col in combined.columns:
            combined[col] = pd.to_datetime(combined[col], errors='coerce')
            combined[col] = combined[col].dt.strftime('%Y-%m-%d')
            print(f"    Parsed {col}", flush=True)

    # Extract start year
    start_date_col = next((c for c in ['funding_start_date', 'fundingstartdate_datepremierversement'] if c in combined.columns), None)
    if start_date_col:
        combined['start_year'] = pd.to_datetime(combined[start_date_col], errors='coerce').dt.year

    # Add metadata
    combined['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Save to parquet
    print("\n  [SAVE] Writing to parquet...", flush=True)

    # Convert numeric columns properly for parquet
    if 'start_year' in combined.columns:
        combined['start_year'] = combined['start_year'].astype('float64')

    output_path = output_dir / "cihr_projects.parquet"
    combined.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB", flush=True)

    # Print summary stats
    print(f"\n  Summary:", flush=True)
    print(f"    - Total unique awards: {len(combined):,}", flush=True)

    title_col = next((c for c in ['application_title', 'applicationtitle_titredemande'] if c in combined.columns), None)
    if title_col:
        print(f"    - With title: {combined[title_col].notna().sum():,}", flush=True)

    name_col = next((c for c in ['family_name', 'familyname_nomfamille'] if c in combined.columns), None)
    if name_col:
        print(f"    - With PI name: {combined[name_col].notna().sum():,}", flush=True)

    amount_col = next((c for c in ['total_amount_awarded', 'totalamountawarded_montanttotalaccorde'] if c in combined.columns), None)
    if amount_col:
        print(f"    - With amount: {combined[amount_col].notna().sum():,}", flush=True)
        total_amount = combined[amount_col].sum()
        if pd.notna(total_amount):
            print(f"    - Total awarded: ${total_amount:,.0f} CAD", flush=True)

    inst_col = next((c for c in ['research_institution_name', 'researchinstitutionnameen_nometablissementrecherchean'] if c in combined.columns), None)
    if inst_col:
        print(f"\n  Top institutions:", flush=True)
        print(combined[inst_col].value_counts().head(10).to_string(), flush=True)

    program_col = next((c for c in ['program_name', 'programnameen_nomprogrammean'] if c in combined.columns), None)
    if program_col:
        print(f"\n  Top programs:", flush=True)
        print(combined[program_col].value_counts().head(10).to_string(), flush=True)

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def find_aws_cli() -> Optional[str]:
    """Find AWS CLI executable path."""
    import shutil

    # Check if aws is in PATH
    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path

    # Check common installation locations
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
    print(f"\n{'='*60}", flush=True)
    print("Step 3: Uploading to S3", flush=True)
    print(f"{'='*60}", flush=True)

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}", flush=True)

    # Find AWS CLI
    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli", flush=True)
        return False

    print(f"  [INFO] Using AWS CLI: {aws_cmd}", flush=True)

    try:
        result = subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print("  [SUCCESS] Upload complete!", flush=True)
        return True

    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}", flush=True)
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    global VERBOSE

    parser = argparse.ArgumentParser(
        description="Download CIHR grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./cihr_data"),
        help="Directory for downloaded/processed files (default: ./cihr_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing parquet)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output for debugging hangs"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout in seconds for each download (default: {DEFAULT_TIMEOUT})"
    )
    args = parser.parse_args()

    # Set global verbosity
    VERBOSE = args.verbose

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60, flush=True)
    print("CIHR (Canadian Institutes of Health Research)", flush=True)
    print("to S3 Data Pipeline", flush=True)
    print("=" * 60, flush=True)
    print(f"Output directory: {args.output_dir.absolute()}", flush=True)
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}", flush=True)
    print(f"Timeout: {args.timeout}s per download", flush=True)
    if VERBOSE:
        print(f"Verbose mode: ENABLED", flush=True)

    # Step 1 & 2: Download and process
    parquet_path = args.output_dir / "cihr_projects.parquet"

    if not args.skip_download:
        all_data = download_all_xlsx(args.output_dir, timeout=args.timeout)

        if not all_data:
            print("\n[ERROR] No data downloaded!", flush=True)
            sys.exit(1)

        parquet_path = process_data(all_data, args.output_dir)
    else:
        if not parquet_path.exists():
            print(f"[ERROR] Parquet not found at {parquet_path}. Run without --skip-download", flush=True)
            sys.exit(1)
        print(f"\n  [SKIP] Using existing parquet: {parquet_path}", flush=True)

    # Step 3: Upload to S3
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:", flush=True)
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}", flush=True)

    print(f"\n{'='*60}", flush=True)
    if upload_success or args.skip_upload:
        print("Pipeline complete!", flush=True)
    else:
        print("Pipeline FAILED - S3 upload unsuccessful", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"\nNext step:", flush=True)
    print(f"  In Databricks, run: notebooks/awards/CreateCIHRAwards.ipynb", flush=True)


if __name__ == "__main__":
    main()
