#!/usr/bin/env python3
"""
IES Awards to S3 Data Pipeline (Institute of Education Sciences)
================================================================

This script downloads IES-funded award records from the official IES awards
search API, processes them into a parquet file, and uploads the parquet to S3
for Databricks ingestion.

Scope note:
    IES is a U.S. Department of Education subtier funder, but this pipeline
    intentionally uses the official IES public awards API rather than the
    USAspending fallback. The tracker explicitly says to CHECK FIRST for the
    IES grant search portal; the current portal at https://ies.ed.gov/use-work/awards
    exposes https://api.ies.ed.gov/web/search/v1/execute and returns Grant,
    Contract, and Cooperative agreement records.

CHECK FIRST outcome:
    On 2026-05-16, the official IES API returned 3,183 award records for the
    public awards page with Result Type in Grant, Contract, or Cooperative
    agreement. The response includes native award ID (mid), title, description,
    awardee, amount, program, award status, office/center, topic tags, award
    date, content type, and IES landing-page path. This official source is
    narrower and richer than a USAspending fallback for the OpenAlex IES funder.

Data Source: https://ies.ed.gov/use-work/awards
API Endpoint: https://api.ies.ed.gov/web/search/v1/execute
Output: s3://openalex-ingest/awards/ies/ies_awards.parquet

What this script does:
1. Calls the official IES search API for Grant, Contract, and Cooperative
   agreement records
2. Paginates through the complete result set
3. Flattens the OpenSearch/Drupal response into one row per award
4. Deduplicates by mid (keeping the latest dated record if duplicates appear)
5. Converts all raw/source columns to string per plans/awards/how-to-add-a-funder.md
6. Writes parquet and uploads it to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/ies/

Usage:
    python ies_to_s3.py

    # Resume interrupted download:
    python ies_to_s3.py --resume

    # Skip upload to S3:
    python ies_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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

IES_AWARDS_PAGE = "https://ies.ed.gov/use-work/awards"
IES_API_ENDPOINT = "https://api.ies.ed.gov/web/search/v1/execute"
IES_URL_BASE = "https://ies.ed.gov"

CONTENT_TYPES = ["Grant", "Contract", "Cooperative agreement"]
PAGE_SIZE = 100
REQUEST_DELAY = 0.25
MAX_RETRIES = 3

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ies/ies_awards.parquet"
CHECKPOINT_FILE = "ies_download_checkpoint.json"


# =============================================================================
# API Functions
# =============================================================================

def build_request(page_num: int, page_size: int) -> dict[str, Any]:
    """Build the request object used by the IES awards search page."""
    return {
        "requestObj": {
            "apiVersion": "2.0",
            "searchTerms": "",
            "singleSelectFilters": [],
            "multiSelectFilters": [
                {
                    "mcontenttype": CONTENT_TYPES,
                    "recommended_display_name": "Result Type",
                }
            ],
            "andedMultiSelectFilters": [],
            "rangeFilters": [],
            "geoDistanceFilter": {},
            "bestBetSize": 5,
            "context": IES_URL_BASE,
            "userEID": "123",
            "typeAheadSelected": False,
            "raw": False,
            "pageNum": page_num,
            "applyAutoFilter": True,
            "enableAutocorrect": True,
            "pageSize": page_size,
            "maxFacetCount": 10,
            "sort": [{"mdateprimary": "desc"}],
            "urlsForDeductiveFilters": [],
            "preFilters": {},
            "relatedResultsClues": [],
        }
    }


def fetch_page(page_num: int, session: requests.Session) -> dict[str, Any]:
    """Fetch one page of IES awards search results."""
    payload = build_request(page_num, PAGE_SIZE)

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(IES_API_ENDPOINT, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()

            if "hits" not in data:
                raise RuntimeError(f"Unexpected response shape: {data!r}")

            return data
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                print(f"  [RETRY] Page {page_num} attempt {attempt + 1} failed: {exc}")
                time.sleep(2 ** attempt)
            else:
                raise

    return {}


def json_string(value: Any) -> Optional[str]:
    """Serialize nested/list values while preserving nulls for scalar fields."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def flatten_hit(hit: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    """Flatten one OpenSearch hit into a single raw award row."""
    source = hit.get("_source") or {}
    row: dict[str, Any] = {
        "source_index": hit.get("_index"),
        "source_id": hit.get("_id"),
        "source_score": hit.get("_score"),
        "source_sort": hit.get("sort"),
        "source_fetched_at": fetched_at,
        "raw_source_json": source,
    }

    for key, value in source.items():
        row[key] = value

    murl = source.get("murl")
    if isinstance(murl, str) and murl:
        row["landing_page_url"] = murl if murl.startswith("http") else f"{IES_URL_BASE}{murl}"
    else:
        row["landing_page_url"] = None

    return {key: json_string(value) for key, value in row.items()}


def save_checkpoint(
    checkpoint_path: Path,
    rows: list[dict[str, Any]],
    next_page: int,
    total: int,
    last_page: int,
    fetched_at: str,
) -> None:
    """Persist progress so an interrupted IES API crawl can resume."""
    payload = {
        "rows": rows,
        "next_page": next_page,
        "total": total,
        "last_page": last_page,
        "fetched_at": fetched_at,
        "last_updated": datetime.utcnow().isoformat(),
    }
    checkpoint_path.write_text(json.dumps(payload), encoding="utf-8")


def load_checkpoint(checkpoint_path: Path) -> dict[str, Any]:
    """Load a previously saved IES API checkpoint."""
    return json.loads(checkpoint_path.read_text(encoding="utf-8"))


def fetch_all_awards(output_dir: Path, resume: bool = False) -> pd.DataFrame:
    """Download all IES award rows from the official search API."""
    print(f"\n{'=' * 60}")
    print("Step 1: Downloading IES awards from official IES API")
    print(f"{'=' * 60}")
    print(f"  Awards page: {IES_AWARDS_PAGE}")
    print(f"  API endpoint: {IES_API_ENDPOINT}")
    print(f"  Content types: {', '.join(CONTENT_TYPES)}")

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=utf-8",
            "User-Agent": "OpenAlex-IES-Ingest/1.0",
        }
    )

    checkpoint_path = output_dir / CHECKPOINT_FILE
    if resume and checkpoint_path.exists():
        checkpoint = load_checkpoint(checkpoint_path)
        rows = checkpoint["rows"]
        next_page = int(checkpoint["next_page"])
        total = int(checkpoint["total"])
        last_page = int(checkpoint["last_page"])
        fetched_at = checkpoint["fetched_at"]
        print(
            f"  [CHECKPOINT] Resuming at page {next_page + 1:,}/{last_page + 1:,} "
            f"with {len(rows):,}/{total:,} rows"
        )
    else:
        first_page = fetch_page(0, session)
        total = int(first_page["hits"]["total"]["value"])
        last_page = int(first_page["hits"].get("last_page_num", 0))
        print(f"  [INFO] API reports {total:,} records across {last_page + 1:,} pages")

        fetched_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = [flatten_hit(hit, fetched_at) for hit in first_page["hits"]["hits"]]
        next_page = 1
        save_checkpoint(checkpoint_path, rows, next_page, total, last_page, fetched_at)

    for page_num in range(next_page, last_page + 1):
        print(f"  [PAGE] {page_num + 1}/{last_page + 1}", end="\r")
        data = fetch_page(page_num, session)
        rows.extend(flatten_hit(hit, fetched_at) for hit in data["hits"]["hits"])
        save_checkpoint(checkpoint_path, rows, page_num + 1, total, last_page, fetched_at)
        time.sleep(REQUEST_DELAY)

    print()
    df = pd.DataFrame(rows)
    print(f"  Downloaded rows: {len(df):,}")

    if len(df) != total:
        print(f"  [WARN] Downloaded row count ({len(df):,}) differs from API total ({total:,})")
    elif checkpoint_path.exists():
        checkpoint_path.unlink()
        print("  [CHECKPOINT] Completed; checkpoint removed")

    return df


# =============================================================================
# Processing Functions
# =============================================================================

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and deduplicate raw IES award rows."""
    print(f"\n{'=' * 60}")
    print("Step 2: Processing data")
    print(f"{'=' * 60}")

    if df.empty:
        raise ValueError("IES API returned no rows")

    # Deduplicate by native IES award ID if duplicates appear.
    if "mid" in df.columns:
        original_count = len(df)
        if "mdateprimary" in df.columns:
            df["_sort_date"] = pd.to_datetime(df["mdateprimary"], errors="coerce")
            df = df.sort_values("_sort_date", ascending=False)
            df = df.drop(columns=["_sort_date"])
        df = df.drop_duplicates(subset=["mid"], keep="first")
        print(f"  Removed duplicates by mid: {original_count - len(df):,}")

    print(f"  Unique awards: {len(df):,}")

    if "mid" in df.columns:
        print(f"  With native award ID: {df['mid'].notna().sum():,}")
    if "mtitle" in df.columns:
        print(f"  With title: {df['mtitle'].notna().sum():,}")
    if "lawardamount" in df.columns:
        amounts = pd.to_numeric(df["lawardamount"], errors="coerce")
        print(f"  With amount: {amounts.notna().sum():,}")
        print(f"  Total amount: ${amounts.sum():,.0f}")
    if "mcontenttype" in df.columns:
        print("  Content types:")
        for content_type, count in df["mcontenttype"].value_counts(dropna=False).items():
            print(f"    - {content_type}: {count:,}")

    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return df


def save_to_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """Save DataFrame to parquet with every raw column stored as string."""
    output_path = output_dir / "ies_awards.parquet"

    print(f"\n{'=' * 60}")
    print("Step 3: Saving parquet")
    print(f"{'=' * 60}")
    print(f"  [SAVE] {output_path}")

    # Required by plans/awards/how-to-add-a-funder.md: all source columns string.
    # The Databricks notebook performs award-schema casts with TRY_CAST/TRY_TO_DATE.
    df = df.astype("string")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

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
    print(f"\n{'=' * 60}")
    print("Step 4: Uploading to S3")
    print(f"{'=' * 60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    try:
        subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True,
        )
        print("  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"  [ERROR] Upload failed: {exc.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download IES awards from the official IES API and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./ies_data"),
        help="Directory for downloaded/processed files (default: ./ies_data)",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if an earlier API crawl was interrupted",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("IES Awards to S3 Data Pipeline (official IES API)")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    df = fetch_all_awards(args.output_dir, resume=args.resume)
    df = process_dataframe(df)
    parquet_path = save_to_parquet(df, args.output_dir)

    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'=' * 60}")
    if upload_success or args.skip_upload:
        print("Pipeline complete!")
    else:
        print("Pipeline FAILED - S3 upload unsuccessful")
    print(f"{'=' * 60}")
    print("\nNext step:")
    print("  In Databricks, run: notebooks/awards/CreateIESAwards.ipynb")


if __name__ == "__main__":
    main()
