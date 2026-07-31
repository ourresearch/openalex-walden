#!/usr/bin/env python3
"""
Wellcome Trust to S3 Data Pipeline
===================================

This script downloads all grants from the Wellcome Trust grants database,
processes it into a parquet file, and uploads it to S3 for Databricks ingestion.

Data Source: https://wellcome.org/grant-funding/funded-people-and-projects
(uses 360Giving data standard)
Output: s3://openalex-ingest/awards/wellcome/wellcome_projects.parquet

What this script does:
1. Downloads the Wellcome grants XLSX from their website
2. Parses the main grants sheet and additional locations sheet
3. Cleans and standardizes field names
4. Builds `grant_ref` = the CITABLE Wellcome grant reference
   ("323416/Z/24/Z", from the `Internal ID` column) - NOT the 360Giving
   `Identifier` ("360G-Wellcome-323416_Z_24_Z"), which is kept only as
   `identifier_360g`. The citable form is what appears in acknowledgements
   and Crossref funding metadata; shipping the 360G form orphaned ~19.6K
   award records from their funded works (fixed 2026-07-31).
5. Converts to parquet format
6. Uploads to S3 (refusing to shrink the existing corpus, runbook §1.4)

The Wellcome XLSX contains two sheets:
- Main Grant List: core grant data
- Additional Location Data: supplementary location info

Note: We only ingest the Main Grant List. Additional locations can be
joined via Internal ID if needed in the future.

Requirements:
    pip install pandas pyarrow requests openpyxl

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/wellcome/

Usage:
    python wellcome_to_s3.py

    # Or with options:
    python wellcome_to_s3.py --output-dir /path/to/output --skip-upload

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

# Wellcome Trust XLSX download URL (360Giving format)
# Note: This URL may be updated periodically with new date ranges
WELLCOME_XLSX_URL = "https://cms.wellcome.org/sites/default/files/2025-09/Wellcome-grants-awarded-1-October-2005-to-1-September-2025.xlsx"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wellcome/wellcome_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_xlsx(output_dir: Path) -> Path:
    """
    Download the Wellcome Trust grants XLSX.

    Args:
        output_dir: Directory to save the downloaded file

    Returns:
        Path to the downloaded XLSX file
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading Wellcome Trust grants XLSX")
    print(f"{'='*60}")

    output_path = output_dir / "wellcome-grants.xlsx"

    print(f"  [DOWNLOAD] {WELLCOME_XLSX_URL}")
    response = requests.get(WELLCOME_XLSX_URL, timeout=300)
    response.raise_for_status()

    # Save to file
    with open(output_path, 'wb') as f:
        f.write(response.content)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  [SUCCESS] Downloaded {size_mb:.1f} MB")

    return output_path


# =============================================================================
# Processing Functions
# =============================================================================

def process_xlsx(xlsx_path: Path, output_dir: Path) -> Path:
    """
    Process the Wellcome Trust XLSX into a parquet file.

    The XLSX has two sheets:
    - Main Grant List (we use this)
    - Additional Location Data (for multi-site grants)

    Args:
        xlsx_path: Path to the downloaded XLSX
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing XLSX")
    print(f"{'='*60}")

    # Read the main grants sheet
    print(f"  [READ] {xlsx_path.name} - Main Grant List")
    df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str, engine='openpyxl')

    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)[:10]}...")

    # Clean column names (lowercase, replace spaces/special chars)
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace(':', '')
                  .str.replace('(', '')
                  .str.replace(')', '')
                  .str.replace('/', '_')
                  .str.replace('-', '_'))

    print(f"  Cleaned columns: {list(df.columns)}")

    # The 360Giving standard has specific field names - map to our schema
    # Common 360Giving fields:
    # - Identifier, Title, Description, Currency, Amount Awarded
    # - Award Date, Planned Dates:Start Date, Planned Dates:End Date
    # - Recipient Org:Identifier, Recipient Org:Name, Recipient Org:Country Code
    # - Funding Org:Identifier, Funding Org:Name
    # - Grant Programme:Title
    # - Beneficiary Location:Name, Beneficiary Location:Country Code

    # Rename 360Giving columns to our internal names
    #
    # ⚠️ AWARD-ID COLUMN CHOICE (fixed 2026-07-31):
    # The 360Giving `Identifier` column is the registry-namespaced form
    # ("360G-Wellcome-323416_Z_24_Z") — it is NOT the grant reference
    # researchers cite. The citable reference lives in Wellcome's
    # `Internal ID` column ("323416/Z/24/Z"), which is what appears in
    # acknowledgements, Crossref funding metadata, and Wellcome's own
    # reporting. The 360G form is a pure derivation of it (prefix +
    # "/"→"_"). We ship `internal_id` as the award id (grant_ref) and
    # keep the 360G identifier only as a secondary column.
    column_mapping = {
        'identifier': 'identifier_360g',
        'title': 'title',
        'description': 'description',
        'currency': 'currency',
        'amount_awarded': 'amount',
        'award_date': 'award_date',
        'planned_datesstart_date': 'start_date',
        'planned_datesend_date': 'end_date',
        'recipient_orgidentifier': 'recipient_org_id',
        'recipient_orgname': 'recipient_org_name',
        'recipient_orgcountry_code': 'recipient_country',
        'recipient_orgpostal_code': 'recipient_postal_code',
        'funding_orgidentifier': 'funding_org_id',
        'funding_orgname': 'funding_org_name',
        'grant_programmetitle': 'grant_programme',
        'beneficiary_locationname': 'beneficiary_location',
        'beneficiary_locationcountry_code': 'beneficiary_country',
        'internal_id': 'internal_id',
        # Lead applicant fields (360Giving person extension)
        'lead_applicant': 'lead_applicant_name',
        'lead_applicantname': 'lead_applicant_name',
        'lead_applicantorcid': 'lead_applicant_orcid',
        # Department / school
        'department': 'department',
        'school': 'school',
        # Multi-location indicator
        'multi_location': 'multi_location',
    }

    # Apply mappings where columns exist
    rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=rename_dict)

    print(f"  Final columns: {list(df.columns)}")

    # Build grant_ref = the citable Wellcome grant reference (e.g. "323416/Z/24/Z").
    # Primary source: the `Internal ID` column. Fallback: strip the 360Giving
    # prefix and restore "/" separators from the Identifier column.
    print("  [INFO] Building grant_ref (citable award id)...")
    if 'internal_id' not in df.columns:
        raise SystemExit(
            "[FATAL] 'Internal ID' column missing from Wellcome XLSX - "
            "cannot build citable grant_ref. Inspect the source file."
        )
    df['grant_ref'] = df['internal_id'].str.strip()
    fallback_mask = df['grant_ref'].isna() | (df['grant_ref'] == '')
    if fallback_mask.any() and 'identifier_360g' in df.columns:
        derived = (df.loc[fallback_mask, 'identifier_360g']
                   .str.replace('360G-Wellcome-', '', regex=False)
                   .str.replace('_', '/', regex=False))
        df.loc[fallback_mask, 'grant_ref'] = derived
        print(f"  [WARN] {fallback_mask.sum():,} rows had no Internal ID; derived from 360G identifier")

    # Sanity gate: the canonical Wellcome shape is NNNNNN/L/NN/L (with /A/, /B/
    # amendment variants). If most rows don't look like that, the source layout
    # changed - stop rather than ship garbage.
    shape_ok = df['grant_ref'].str.match(r'^\d{5,6}/[A-Z]/\d{2}/[A-Z]$', na=False)
    pct_ok = shape_ok.mean() * 100
    print(f"  grant_ref matching NNNNNN/L/NN/L shape: {shape_ok.sum():,}/{len(df):,} ({pct_ok:.1f}%)")
    if pct_ok < 95:
        print(df.loc[~shape_ok, ['grant_ref', 'identifier_360g']].head(10).to_string())
        raise SystemExit("[FATAL] <95% of grant_ref values match the Wellcome reference shape - inspect source")

    # No 360G prefix may survive in the citable column
    assert not df['grant_ref'].str.startswith('360G-', na=False).any(), \
        "360G-prefixed values leaked into grant_ref"

    # Parse dates
    print("  [INFO] Parsing dates...")

    # Award date
    if 'award_date' in df.columns:
        df['award_date'] = pd.to_datetime(df['award_date'], errors='coerce')
        df['award_date'] = df['award_date'].dt.strftime('%Y-%m-%d')
        df['award_date'] = df['award_date'].replace('NaT', None)

    # Start date
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
        df['start_date'] = df['start_date'].replace('NaT', None)

    # End date
    if 'end_date' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')
        df['end_date'] = df['end_date'].replace('NaT', None)

    # Clean amount (remove commas, convert to numeric)
    print("  [INFO] Parsing amounts...")
    if 'amount' in df.columns:
        df['amount'] = df['amount'].str.replace(',', '', regex=False).str.strip()
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Deduplicate by the citable grant reference
    print("  [INFO] Deduplicating by grant_ref...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['grant_ref'], keep='first')
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Build schema dynamically based on columns present
    schema_fields = []
    for col in df.columns:
        if col == 'amount':
            schema_fields.append((col, pa.float64()))
        else:
            schema_fields.append((col, pa.string()))

    schema = pa.schema(schema_fields)

    # Save to parquet with explicit schema
    output_path = output_dir / "wellcome_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Convert to pyarrow table with schema
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    if 'title' in df.columns:
        print(f"    - With title: {df['title'].notna().sum():,}")
    if 'description' in df.columns:
        print(f"    - With description: {df['description'].notna().sum():,}")
    if 'start_date' in df.columns:
        print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    if 'amount' in df.columns:
        print(f"    - With amount: {df['amount'].notna().sum():,}")
        total = df['amount'].sum()
        if pd.notna(total):
            # Assume GBP
            print(f"    - Total amount: £{total:,.0f}")
    if 'lead_applicant_name' in df.columns:
        print(f"    - With lead applicant: {df['lead_applicant_name'].notna().sum():,}")
    if 'lead_applicant_orcid' in df.columns:
        print(f"    - With ORCID: {df['lead_applicant_orcid'].notna().sum():,}")

    if 'grant_programme' in df.columns:
        print(f"\n  Grant Programmes (top 10):")
        print(df['grant_programme'].value_counts().head(10).to_string())

    if 'recipient_country' in df.columns:
        print(f"\n  Recipient Countries (top 10):")
        print(df['recipient_country'].value_counts().head(10).to_string())

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


def check_no_shrink(local_path: Path, allow_shrink: bool) -> None:
    """Re-ingestion safety (runbook §1.4): never overwrite the S3 parquet with
    fewer rows than it already has. A shrinking corpus almost always means a
    partial outage or a broken parse, not retracted grants."""
    import pyarrow.parquet as _pq

    new_count = _pq.ParquetFile(local_path).metadata.num_rows
    try:
        from pyarrow.fs import S3FileSystem

        fs = S3FileSystem(region="us-east-1")
        prev = _pq.ParquetFile(f"{S3_BUCKET}/{S3_KEY}", filesystem=fs)
        prev_count = prev.metadata.num_rows
    except Exception as e:
        print(f"  [WARN] Could not read previous S3 row count ({e}); "
              "treating as first upload")
        return

    print(f"  [CHECK] previous S3 rows: {prev_count:,} | new rows: {new_count:,}")
    if new_count < prev_count and not allow_shrink:
        raise SystemExit(
            f"[FATAL] New corpus ({new_count:,}) is SMALLER than previous "
            f"({prev_count:,}). Refusing to overwrite (runbook §1.4). "
            "Re-run with --allow-shrink only if the shrink is genuinely expected."
        )


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

    # Find AWS CLI
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
        description="Download Wellcome Trust grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./wellcome_data"),
        help="Directory for downloaded/processed files (default: ./wellcome_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing XLSX)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Allow overwriting S3 with fewer rows than the previous corpus (runbook §1.4)"
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Wellcome Trust to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        xlsx_path = download_xlsx(args.output_dir)
    else:
        xlsx_path = args.output_dir / "wellcome-grants.xlsx"
        if not xlsx_path.exists():
            print(f"[ERROR] XLSX not found at {xlsx_path}. Run without --skip-download")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing XLSX: {xlsx_path}")

    # Step 2: Process
    parquet_path = process_xlsx(xlsx_path, args.output_dir)

    # Step 3: Upload to S3
    upload_success = True
    if not args.skip_upload:
        check_no_shrink(parquet_path, args.allow_shrink)
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
    print(f"  In Databricks, run: notebooks/awards/CreateWellcomeAwards.ipynb")


if __name__ == "__main__":
    main()