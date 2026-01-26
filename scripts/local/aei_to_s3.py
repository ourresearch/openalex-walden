#!/usr/bin/env python3
"""
AEI (Agencia Estatal de Investigación) to S3 Data Pipeline
===========================================================

This script downloads all Spanish research grants from the BDNS API
for the Agencia Estatal de Investigación (AEI), processes them into
a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://www.infosubvenciones.es/bdnstrans/api/concesiones/busqueda
             (BDNS - Base de Datos Nacional de Subvenciones)
Output: s3://openalex-ingest/awards/aei/aei_grants.parquet

What this script does:
1. Fetches all grants (concesiones) for AEI from BDNS API (organos=3319)
2. Parses JSON responses to extract grant metadata
3. Extracts grant code, beneficiary, amount, dates, and call information
4. Combines into a single DataFrame
5. Saves as parquet and uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/aei/

Usage:
    python aei_to_s3.py

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# BDNS API settings
BDNS_API_BASE = "https://www.infosubvenciones.es/bdnstrans/api/concesiones/busqueda"
AEI_ORGANO_ID = 3319  # Agencia Estatal de Investigación
PAGE_SIZE = 10000  # Maximum allowed by API
REQUEST_DELAY = 0.5  # Seconds between requests
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aei/aei_grants.parquet"


# =============================================================================
# Progress Tracker
# =============================================================================

class ProgressTracker:
    """Track download progress with ETA calculation."""

    def __init__(self, total_items: int):
        self.total_items = total_items
        self.completed_items = 0
        self.start_time = time.time()
        self.last_report_time = time.time()

    def update(self, items_count: int):
        """Update progress counters."""
        self.completed_items += items_count

    def get_eta(self) -> str:
        """Calculate and format ETA."""
        if self.completed_items == 0:
            return "calculating..."

        elapsed = time.time() - self.start_time
        items_per_second = self.completed_items / elapsed
        remaining_items = self.total_items - self.completed_items

        if items_per_second > 0:
            remaining_seconds = remaining_items / items_per_second
            eta = timedelta(seconds=int(remaining_seconds))
            return str(eta)
        return "unknown"

    def get_elapsed(self) -> str:
        """Format elapsed time."""
        elapsed = time.time() - self.start_time
        return str(timedelta(seconds=int(elapsed)))

    def should_report(self, interval: int = 10) -> bool:
        """Check if we should print a progress report."""
        now = time.time()
        if now - self.last_report_time >= interval:
            self.last_report_time = now
            return True
        return False

    def get_progress_line(self) -> str:
        """Get formatted progress line."""
        pct = (self.completed_items / self.total_items) * 100 if self.total_items > 0 else 0
        return (
            f"  [{self.completed_items:,}/{self.total_items:,} grants ({pct:.1f}%)] "
            f"[Elapsed: {self.get_elapsed()}] "
            f"[ETA: {self.get_eta()}]"
        )


# =============================================================================
# API Functions
# =============================================================================

def fetch_page(
    page: int,
    session: requests.Session,
    max_retries: int = MAX_RETRIES
) -> tuple[int, list[dict], Optional[str]]:
    """
    Fetch a single page with retry logic.

    Args:
        page: Page number (0-indexed)
        session: Requests session
        max_retries: Maximum retry attempts

    Returns:
        Tuple of (page_number, list of grant dicts, error message or None)
    """
    params = {
        "page": page,
        "pageSize": PAGE_SIZE,
        "vpd": "GE",
        "organos": AEI_ORGANO_ID
    }
    last_error = None

    for attempt in range(max_retries):
        try:
            response = session.get(BDNS_API_BASE, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            grants = data.get("content", [])
            return (page, grants, None)

        except requests.exceptions.Timeout as e:
            last_error = f"Timeout (attempt {attempt + 1}/{max_retries})"
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error (attempt {attempt + 1}/{max_retries})"
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait_time = RETRY_BACKOFF ** (attempt + 2)
                last_error = f"Rate limited, waiting {wait_time:.1f}s"
                time.sleep(wait_time)
            elif response.status_code >= 500:
                last_error = f"Server error {response.status_code}"
            else:
                return (page, [], f"HTTP {response.status_code}: {str(e)}")
        except json.JSONDecodeError as e:
            last_error = f"JSON parse error"
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"

        if attempt < max_retries - 1:
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)

    return (page, [], last_error)


def get_total_count(session: requests.Session) -> int:
    """Get total number of grants from API."""
    params = {
        "page": 0,
        "pageSize": 1,
        "vpd": "GE",
        "organos": AEI_ORGANO_ID
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(BDNS_API_BASE, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            total = data.get("totalElements", 0)
            print(f"  [INFO] API reports {total:,} total grants")
            return int(total)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  [WARN] Failed to get total count (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                raise RuntimeError(f"Failed to get total count: {e}")

    return 0


# =============================================================================
# Download Functions
# =============================================================================

def download_all_grants(output_dir: Path) -> list[dict]:
    """Download all grants from BDNS API."""
    print(f"\n{'='*60}")
    print("Step 1: Downloading grants from BDNS API (AEI)")
    print(f"{'='*60}")

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenAlex-AEI-Ingest/1.0"
    })

    # Get total count
    print("  [INFO] Fetching total grant count...")
    total_count = get_total_count(session)
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE

    print(f"  [INFO] Total grants: {total_count:,}")
    print(f"  [INFO] Pages to fetch: {total_pages:,} (page size: {PAGE_SIZE:,})")

    # Initialize progress tracker
    progress = ProgressTracker(total_count)
    all_grants = []
    failed_pages = []

    print(f"\n  Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for page in range(total_pages):
        page_num, grants, error = fetch_page(page, session)

        if error:
            print(f"\n  [ERROR] Page {page}: {error}")
            failed_pages.append((page, error))
        else:
            all_grants.extend(grants)
            progress.update(len(grants))

        # Print progress
        if progress.should_report(5) or page == total_pages - 1:
            print(f"\r{progress.get_progress_line()}", flush=True)

        time.sleep(REQUEST_DELAY)

    print(f"\n\n  {'='*50}")
    print(f"  Download complete!")
    print(f"  {'='*50}")
    print(f"  Total grants: {len(all_grants):,}")
    print(f"  Total time: {progress.get_elapsed()}")

    if failed_pages:
        print(f"\n  [WARN] {len(failed_pages)} pages failed")
        failed_file = output_dir / "aei_failed_pages.json"
        with open(failed_file, "w") as f:
            json.dump(failed_pages, f, indent=2)
        print(f"  [INFO] Failed pages saved to {failed_file}")

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

    # Convert to DataFrame
    df = pd.DataFrame(grants)
    print(f"  Total rows: {len(df):,}")

    # Rename columns for clarity
    column_mapping = {
        "id": "bdns_id",
        "codConcesion": "grant_code",
        "fechaConcesion": "grant_date",
        "beneficiario": "beneficiary",
        "instrumento": "instrument_type",
        "importe": "amount",
        "ayudaEquivalente": "equivalent_aid",
        "urlBR": "legal_basis_url",
        "tieneProyecto": "has_project",
        "numeroConvocatoria": "call_number",
        "idConvocatoria": "call_id",
        "convocatoria": "call_title",
        "descripcionCooficial": "title_coofficial",
        "nivel1": "admin_level1",
        "nivel2": "admin_level2",
        "nivel3": "admin_level3",
        "codigoInvente": "invente_code",
        "idPersona": "person_id"
    }

    # Only rename columns that exist
    existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_columns)

    # Parse dates
    if "grant_date" in df.columns:
        df["grant_date"] = pd.to_datetime(df["grant_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["grant_date"] = df["grant_date"].replace("NaT", None)

    # Extract year
    df["grant_year"] = pd.to_datetime(df["grant_date"], errors="coerce").dt.year

    # Remove duplicates
    print("  [INFO] Deduplicating by grant_code...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["grant_code"], keep="first")
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "aei_grants.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With amounts: {df['amount'].notna().sum():,}")
    print(f"    - Total funding: €{df['amount'].sum():,.0f}")

    if "grant_year" in df.columns:
        print(f"\n  Year distribution:")
        print(df["grant_year"].value_counts().sort_index().tail(10).to_string())

    if "instrument_type" in df.columns:
        print(f"\n  Instrument types:")
        print(df["instrument_type"].value_counts().head(5).to_string())

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
        description="Download AEI grants from BDNS and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./aei_data"),
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
    print("AEI (Agencia Estatal de Investigación) to S3 Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    grants = download_all_grants(args.output_dir)

    if not grants:
        print("[ERROR] No grants downloaded!")
        sys.exit(1)

    # Step 2: Process
    parquet_path = process_grants(grants, args.output_dir)

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
    print(f"  In Databricks, run: notebooks/awards/CreateAEIAwards.ipynb")


if __name__ == "__main__":
    main()
