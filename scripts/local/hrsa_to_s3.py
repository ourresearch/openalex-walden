#!/usr/bin/env python3
"""
Download HRSA awarded-grants data to parquet for OpenAlex awards ingestion.

Why this source:
    The awards runbook says to prefer official source-owned data over fallback
    aggregators. Its method ladder places bulk file downloads (CSV/JSON/XML)
    above static scraping, Playwright/browser scraping, and regulatory or
    USAspending-style fallbacks. This script therefore uses HRSA's own Data
    Warehouse CSV rather than the USAspending fallback used by the original
    draft PR.

Runbook fit:
    - Method 4: official bulk CSV download from HRSA Data Warehouse.
    - Raw/source columns are preserved closely and written as strings.
    - The notebook should do all award-schema casting with TRY_CAST/TRY_TO_DATE.
    - The final notebook output must contain only the OpenAlex awards schema.
      Helper fields added here, such as source_url, downloaded_at, and
      source_row_hash, are raw-stage aids only.

Important modeling caveat:
    Local validation showed 71,889 rows but only 27,872 unique grant_number
    values. The source appears to contain annual financial-assistance rows, not
    one row per unique grant number. A downstream notebook should inspect
    whether the final OpenAlex award key should use grant_number alone or a
    more specific native key, such as grant_number plus award_year or a
    source-row component. Do not blindly deduplicate by grant_number.

Validated locally on 2026-05-15:
    - URL reachable with curl.
    - Full CSV read passed required-column checks.
    - 71,889 rows, 44 columns.
    - award_year range: 2016-2026.
    - financial_assistance present: 71,889/71,889.
    - financial_assistance total: 118,354,684,716.99.
    - Parquet writing was not fully tested in the Codex runtime because that
      runtime lacked pyarrow/fastparquet. The repo requirements.txt includes
      pyarrow>=14.0.0, which is sufficient for the normal project environment.

Source:
    https://data.hrsa.gov/DataDownload/DD_Files/FS_EHB_AWARD_GRANT_FA_AGR_MVX.csv

Output:
    s3://openalex-ingest/awards/hrsa/hrsa_projects.parquet

Notebook implications:
    Existing HRSA notebook drafts may still be USAspending-shaped, with columns
    like award_id_fain and provenance usaspending_hrsa. If using this source,
    update the notebook to read hrsa_projects.parquet, map HRSA Data Warehouse
    columns, and use a direct-source provenance such as hrsa_data_warehouse.

Usage:
    python scripts/local/hrsa_to_s3.py --validate-only
    python scripts/local/hrsa_to_s3.py --sample-rows 1000 --validate-only
    python scripts/local/hrsa_to_s3.py --skip-upload

Safe local smoke test:
    python scripts/local/hrsa_to_s3.py \
        --output-dir /tmp/hrsa_smoke \
        --sample-rows 1000 \
        --skip-upload
"""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


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

SOURCE_URL = "https://data.hrsa.gov/DataDownload/DD_Files/FS_EHB_AWARD_GRANT_FA_AGR_MVX.csv"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hrsa/hrsa_projects.parquet"
OUTPUT_FILE = "hrsa_projects.parquet"
RAW_CSV_FILE = "hrsa_awarded_grants.csv"
CHECKPOINT_FILE = "hrsa_download_checkpoint.json"
CHUNK_SIZE = 1024 * 1024

EXPECTED_COLUMNS = {
    "award_year",
    "financial_assistance",
    "grant_number",
    "project_period_start_date",
    "grant_project_period_end_date",
    "hrsa_program_area_name",
    "grant_program_name",
    "abstract",
    "grantee_name",
}

HASH_COLUMNS = [
    "grant_number",
    "award_year",
    "financial_assistance",
    "grantee_name",
    "grant_program_name",
    "project_period_start_date",
    "grant_project_period_end_date",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_column(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def write_checkpoint(path: Path, metadata: dict) -> None:
    path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def download_csv(output_dir: Path, force: bool = False) -> Path:
    """Download the HRSA CSV with an atomic local checkpoint."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / RAW_CSV_FILE
    checkpoint_path = output_dir / CHECKPOINT_FILE

    if csv_path.exists() and not force:
        log(f"[SKIP] Using cached CSV: {csv_path}")
        return csv_path

    part_path = csv_path.with_suffix(csv_path.suffix + ".part")
    if part_path.exists():
        part_path.unlink()

    log(f"[DOWNLOAD] GET {SOURCE_URL}")
    start = time.time()
    first_chunk_at: Optional[float] = None
    downloaded = 0
    last_log = start

    request = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": "OpenAlex-HRSA-Ingest/1.0"},
    )

    with urllib.request.urlopen(request, timeout=120) as response:
        log(
            "[DOWNLOAD] "
            f"status={response.status} "
            f"content_length={response.headers.get('content-length', 'unknown')} "
            f"last_modified={response.headers.get('last-modified', 'unknown')}"
        )

        with part_path.open("wb") as handle:
            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    if first_chunk_at is None and time.time() - start > 30:
                        raise TimeoutError("No HRSA CSV bytes received within 30 seconds")
                    break

                if first_chunk_at is None:
                    first_chunk_at = time.time()

                handle.write(chunk)
                downloaded += len(chunk)

                now = time.time()
                if now - last_log >= 5 or downloaded == len(chunk):
                    log(f"[DOWNLOAD] {downloaded / 1024 / 1024:.1f} MB received")
                    last_log = now

    if downloaded == 0:
        raise RuntimeError("Downloaded zero bytes from HRSA source")

    part_path.replace(csv_path)
    elapsed = max(time.time() - start, 0.001)
    log(f"[DOWNLOAD] Complete: {downloaded / 1024 / 1024:.1f} MB in {elapsed:.1f}s")

    write_checkpoint(
        checkpoint_path,
        {
            "source_url": SOURCE_URL,
            "downloaded_at": utc_now(),
            "bytes": downloaded,
            "raw_csv": str(csv_path),
        },
    )
    return csv_path


def read_source_csv(csv_path: Path, sample_rows: Optional[int]) -> pd.DataFrame:
    log(f"[READ] {csv_path}")
    df = pd.read_csv(
        csv_path,
        dtype=str,
        low_memory=False,
        encoding="utf-8-sig",
        nrows=sample_rows,
    )
    df.columns = [clean_column(column) for column in df.columns]
    log(f"[READ] rows={len(df):,} columns={len(df.columns):,}")

    missing = sorted(EXPECTED_COLUMNS - set(df.columns))
    if missing:
        raise RuntimeError(f"HRSA source schema changed; missing columns: {missing}")

    return df


def add_source_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for column in df.columns:
        df[column] = df[column].astype("string").str.strip()

    available_hash_columns = [column for column in HASH_COLUMNS if column in df.columns]
    hash_values = (
        df[available_hash_columns]
        .fillna("")
        .astype("string")
        .agg("|".join, axis=1)
    )

    df["source_url"] = SOURCE_URL
    df["downloaded_at"] = utc_now()
    df["source_row_hash"] = hash_values.map(
        lambda value: hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]
    )

    duplicate_hashes = int(df["source_row_hash"].duplicated().sum())
    if duplicate_hashes:
        duplicate_examples = df.loc[
            df["source_row_hash"].duplicated(keep=False),
            [column for column in HASH_COLUMNS if column in df.columns] + ["source_row_hash"],
        ].head(20)
        raise RuntimeError(
            f"{duplicate_hashes:,} duplicate source_row_hash values found. "
            "The HRSA notebook uses source_row_hash in the row-level award key; "
            "update HASH_COLUMNS or inspect source duplicates before proceeding.\n"
            + duplicate_examples.to_string(index=False)
        )

    return df


def log_summary(df: pd.DataFrame) -> None:
    amount = pd.to_numeric(
        df["financial_assistance"].astype("string").str.replace(r"[$,]", "", regex=True),
        errors="coerce",
    )
    years = pd.to_numeric(df["award_year"], errors="coerce")

    log("[SUMMARY] HRSA source rows")
    log(f"[SUMMARY] rows={len(df):,}")
    log(f"[SUMMARY] unique grant_number={df['grant_number'].nunique(dropna=True):,}")
    log(f"[SUMMARY] award_year range={int(years.min()) if years.notna().any() else 'NA'}-{int(years.max()) if years.notna().any() else 'NA'}")
    log(f"[SUMMARY] financial_assistance present={amount.notna().sum():,}/{len(df):,}")
    if amount.notna().any():
        log(f"[SUMMARY] financial_assistance total={amount.sum():,.2f}")


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    output_path = output_dir / OUTPUT_FILE

    # Required by plans/awards/how-to-add-a-funder.md: all source columns string.
    df = df.astype("string")
    try:
        df.to_parquet(output_path, index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Parquet support is missing. Install repo requirements first: "
            "python -m pip install -r requirements.txt"
        ) from exc

    size_mb = output_path.stat().st_size / 1024 / 1024
    log(f"[SAVE] {output_path} ({size_mb:.1f} MB)")
    return output_path


def find_aws_cli() -> Optional[str]:
    return shutil.which("aws")


def upload_to_s3(local_path: Path) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"[UPLOAD] {local_path} -> {s3_uri}")

    aws_cmd = find_aws_cli()
    if not aws_cmd:
        log("[UPLOAD] AWS CLI not found; skipping upload.")
        return False

    try:
        subprocess.run([aws_cmd, "s3", "cp", str(local_path), s3_uri], check=True)
        log("[UPLOAD] Complete")
        return True
    except subprocess.CalledProcessError as exc:
        log(f"[UPLOAD] Failed: {exc}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download official HRSA awards CSV to parquet")
    parser.add_argument("--output-dir", type=Path, default=Path("./hrsa_data"))
    parser.add_argument("--sample-rows", type=int, default=None, help="Read only the first N rows for a smoke test")
    parser.add_argument("--force-download", action="store_true", help="Download even if the raw CSV exists locally")
    parser.add_argument("--skip-download", action="store_true", help="Use existing raw CSV in output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--validate-only", action="store_true", help="Read and validate the source, then stop before parquet/S3")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    log("=" * 72)
    log("HRSA awarded-grants CSV pipeline")
    log(f"Source: {SOURCE_URL}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    csv_path = args.output_dir / RAW_CSV_FILE
    if args.skip_download:
        if not csv_path.exists():
            log(f"[ERROR] Missing cached CSV: {csv_path}")
            sys.exit(1)
        log(f"[SKIP] Using existing CSV: {csv_path}")
    else:
        csv_path = download_csv(args.output_dir, force=args.force_download)

    df = read_source_csv(csv_path, sample_rows=args.sample_rows)
    df = add_source_metadata(df)
    log_summary(df)

    if args.validate_only:
        log("[DONE] --validate-only set; source read and required-column checks passed.")
        return

    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        log("[DONE] --skip-upload set; local parquet only.")
        return

    if not upload_to_s3(parquet_path):
        log("[WARN] Upload did not complete. Manual command:")
        log(f"aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
