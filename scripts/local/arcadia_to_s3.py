#!/usr/bin/env python3
"""
Arcadia Fund grants -> S3 Data Pipeline
=======================================

Downloads Arcadia Fund's official 360Giving grants CSV and writes a parquet
file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://arcadiafund.org.uk/grants-awarded

Arcadia links directly from that page to its grants data CSV and states that
it began publishing these grant records with 360Giving for transparency. The
page renders amounts with a dollar sign, so this script maps positive
`Amount awarded` values to USD.

Output
------
s3://openalex-ingest/awards/arcadia/arcadia_grants.parquet

Usage
-----
    python arcadia_to_s3.py --skip-upload
    python arcadia_to_s3.py --limit 10 --skip-upload
    python arcadia_to_s3.py --skip-download --skip-upload
    python arcadia_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
from io import BytesIO
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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


GRANTS_PAGE_URL = "https://arcadiafund.org.uk/grants-awarded"

FUNDER_ID = 4320313262
FUNDER_DISPLAY_NAME = "Arcadia Fund"
PROVENANCE = "arcadia_360giving"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/arcadia/arcadia_grants.parquet"

USER_AGENT = "openalex-walden-arcadia-ingest/1.0 (+https://openalex.org)"


def clean_text(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_amount(value: Any) -> Optional[float]:
    text = clean_text(value)
    if not text:
        return None
    text = text.replace(",", "").replace("$", "")
    try:
        amount = float(text)
    except ValueError:
        return None
    return amount if amount > 0 else None


def parse_int(value: Any) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return int(float(text.replace(",", "")))
    except ValueError:
        return None


def source_row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(row, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def discover_csv_url() -> str:
    print(f"  Fetching grants page: {GRANTS_PAGE_URL}")
    resp = get_session().get(GRANTS_PAGE_URL, timeout=45)
    resp.raise_for_status()
    html = resp.text
    matches = re.findall(r"href=[\"']([^\"']+\.csv(?:\?[^\"']*)?)[\"']", html, flags=re.I)
    if not matches:
        raise RuntimeError("Could not find a CSV link on Arcadia grants page")
    csv_url = urljoin(GRANTS_PAGE_URL, matches[0])
    print(f"  [OK] discovered CSV: {csv_url}")
    return csv_url


def download_csv(output_dir: Path) -> tuple[bytes, str]:
    print("\n" + "=" * 60)
    print("Step 1: Download official Arcadia CSV")
    print("=" * 60)
    csv_url = discover_csv_url()
    resp = get_session().get(csv_url, timeout=90)
    resp.raise_for_status()
    content = resp.content
    raw_path = output_dir / "arcadia_grants_raw.csv"
    raw_path.write_bytes(content)
    manifest_path = output_dir / "arcadia_manifest.json"
    manifest_path.write_text(
        json.dumps({
            "grants_page_url": GRANTS_PAGE_URL,
            "csv_url": csv_url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "bytes": len(content),
        }, indent=2),
        encoding="utf-8",
    )
    print(f"  [OK] wrote raw CSV ({len(content):,} bytes) to {raw_path}")
    return content, csv_url


def load_cached_csv(output_dir: Path) -> tuple[bytes, str]:
    raw_path = output_dir / "arcadia_grants_raw.csv"
    manifest_path = output_dir / "arcadia_manifest.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    csv_url = GRANTS_PAGE_URL
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        csv_url = manifest.get("csv_url") or csv_url
    content = raw_path.read_bytes()
    print(f"  [OK] loaded cached CSV ({len(content):,} bytes) from {raw_path}")
    return content, csv_url


def read_source_csv(content: bytes) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            df = pd.read_csv(BytesIO(content), encoding=encoding, dtype="string")
            print(f"  [OK] parsed CSV with {encoding}: {len(df):,} rows, {len(df.columns)} columns")
            return df
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Could not parse Arcadia CSV as utf-8-sig or cp1252")


def normalize(df: pd.DataFrame, csv_url: str, limit: Optional[int]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize and validate rows")
    print("=" * 60)
    if limit:
        df = df.head(limit).copy()
        print(f"  [LIMIT] keeping first {len(df):,} rows")

    required_columns = ["Title", "Identifier", "Description"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise RuntimeError(f"Missing expected columns: {missing_columns}")

    def value(row: pd.Series, *names: str) -> Any:
        for name in names:
            if name in row.index and not pd.isna(row[name]):
                return row[name]
        return None

    downloaded_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        raw = {col: (None if pd.isna(row[col]) else row[col]) for col in df.columns}
        amount = parse_amount(value(row, "Amount Awarded", "Amount awarded"))
        year = parse_int(value(row, "Award Year", "Year Awarded"))
        duration = parse_int(value(row, "Term (Years)", "Grant Duration"))
        end_year = year + duration - 1 if year is not None and duration and duration > 0 else year
        currency = clean_text(value(row, "Currency")) or CURRENCY
        records.append({
            "funder_award_id": clean_text(value(row, "Identifier")),
            "display_name": clean_text(value(row, "Title")),
            "description": clean_text(value(row, "Description")),
            "beneficiary": clean_text(value(row, "Recipient Org:Name", "Grant Recipient")),
            "beneficiary_url": clean_text(value(row, "Recipient Org: Web Address", "Recipient URL")),
            "recipient_org_identifier": clean_text(value(row, "Recipient Org: Identifier")),
            "recipient_org_charity_number": clean_text(value(row, "Recipient Org: Charity Number")),
            "grant_type": clean_text(value(row, "Grant Type")) or "Grant",
            "source_status": clean_text(value(row, "Status")),
            "funding_area": clean_text(value(row, "Grant Programme: Title", "Funding area")),
            "grant_priority": clean_text(value(row, "Grant Programme: Subtitle", "Grant priority")),
            "award_date": clean_text(value(row, "Award date")),
            "term_months": clean_text(value(row, "Term (Months)")),
            "funding_org_name": clean_text(value(row, "Funding Org: Name")),
            "funding_org_identifier": clean_text(value(row, "Funding Org:Identifier")),
            "amount": f"{amount:.2f}" if amount is not None and not amount.is_integer() else (f"{amount:.0f}" if amount is not None else None),
            "currency": currency if amount is not None else None,
            "amount_awarded_raw": clean_text(value(row, "Amount Awarded", "Amount awarded")),
            "source_year": str(year) if year is not None else None,
            "duration_years": str(duration) if duration is not None else None,
            "start_date": f"{year:04d}-01-01" if year is not None else None,
            "end_date": f"{end_year:04d}-12-31" if end_year is not None else None,
            "landing_page_url": GRANTS_PAGE_URL,
            "source_csv_url": csv_url,
            "source_row_hash": source_row_hash(raw),
            "source_row_json": json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str),
            "downloaded_at": downloaded_at,
        })

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No Arcadia rows were normalized")

    required = ["funder_award_id", "display_name", "beneficiary", "amount", "currency", "source_year"]
    for col in required:
        missing = int(out[col].isna().sum())
        if missing:
            raise RuntimeError(f"Required column {col} has {missing:,} missing values")

    dupes = out["funder_award_id"].duplicated().sum()
    if dupes:
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes:,}")

    amount_numeric = pd.to_numeric(out["amount"], errors="coerce")
    year_numeric = pd.to_numeric(out["source_year"], errors="coerce")
    print(f"  Rows: {len(out):,}")
    print(f"  Unique funder_award_id: {out['funder_award_id'].nunique():,}")
    print(f"  Year range: {int(year_numeric.min())} - {int(year_numeric.max())}")
    print(f"  Amount coverage: {amount_numeric.notna().mean() * 100:.1f}%")
    print(f"  Total USD amount: {amount_numeric.sum():,.2f}")
    print(f"  Funding areas: {out['funding_area'].value_counts().to_dict()}")
    print(f"  Grant types: {out['grant_type'].value_counts().to_dict()}")
    print(f"  Description coverage: {out['description'].notna().mean() * 100:.1f}%")

    return out.astype("string")


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "arcadia_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df):,} rows ({parquet_path.stat().st_size / 1024:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink-check; "
            "rerun with --skip-upload for local validation."
        ) from exc

    client = boto3.client("s3")
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check needed.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_arcadia_grants.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        print(f"\n[ERROR] Refusing to shrink Arcadia corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Arcadia grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/arcadia"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse arcadia_grants_raw.csv from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("Arcadia Fund grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {GRANTS_PAGE_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw CSV")
        content, csv_url = load_cached_csv(args.output_dir)
    else:
        content, csv_url = download_csv(args.output_dir)

    source_df = read_source_csv(content)
    df = normalize(source_df, csv_url, args.limit)
    parquet_path = write_outputs(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
