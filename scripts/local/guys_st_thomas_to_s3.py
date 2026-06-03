#!/usr/bin/env python3
"""
Guy's and St Thomas' Charity Grants -> S3 Pipeline (360Giving)
=============================================================

Downloads Guy's and St Thomas' Foundation's published 360Giving grant
workbook from the foundation's own website. The source and OpenAlex funder
row use the historical Guy's and St Thomas' Charity name.

Source authority
----------------
The 360Giving Data Registry (https://registry.threesixtygiving.org/data.json)
lists publisher "Guy's and St Thomas' Foundation (previously Guy's and
St Thomas' Charity)" and resolves to one direct Excel workbook download from
gsttfoundation.org.uk:

  Final-data-set-360-Giving-correct-29-Nov-2024.xlsx

Awarding body in OpenAlex:
  Guy's and St Thomas' Charity (F4320320083, GB,
  ROR https://ror.org/02p7svq74, DOI 10.13039/501100000380).

Output
------
  s3://openalex-ingest/awards/guys_st_thomas/guys_st_thomas_grants.parquet

Usage
-----
    python guys_st_thomas_to_s3.py                                  # full run
    python guys_st_thomas_to_s3.py --skip-upload                    # local dev
    python guys_st_thomas_to_s3.py --limit 50                       # smoke
    python guys_st_thomas_to_s3.py --skip-download --skip-upload    # reuse cache
    python guys_st_thomas_to_s3.py --allow-shrink                   # override section 1.4

Requirements
------------
    pip install pandas pyarrow openpyxl requests boto3
"""

import argparse
import re
import time
from collections import Counter
from pathlib import Path
from typing import Optional

import pandas as pd

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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


SOURCE_URL = "https://gsttfoundation.org.uk/wp-content/uploads/2024/11/Final-data-set-360-Giving-correct-29-Nov-2024.xlsx"
SOURCE_REGISTRY_IDENTIFIER = "a00P400000Lw5cqIAB"
SOURCE_WORKBOOK_NAME = "guys_st_thomas_360giving_2024.xlsx"

FUNDER_ID = 4320320083
FUNDER_DISPLAY_NAME = "Guy's and St Thomas' Charity"
PROVENANCE = "guys_st_thomas_360giving"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/guys_st_thomas/guys_st_thomas_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-guys-st-thomas-ingest/1.0; +https://openalex.org)"
DEFAULT_CACHE_DIR = Path(".cache/guys_st_thomas")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s.strip().lower() in ("", "nan", "none", "<na>"):
        return None
    s = s.replace("_x000D_", "\n")
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    s = re.sub(r"\n{2,}", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip() or None


def get_col(rec: dict, *names):
    for name in names:
        if name in rec:
            return rec.get(name)
    return None


def download_file(url: str, cache_path: Path, skip_download: bool) -> bytes:
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download set but cache not found: {cache_path}")
        log(f"  reusing cached workbook {cache_path}")
        return cache_path.read_bytes()
    log(f"  downloading {url}")
    try:
        import requests
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
        resp.raise_for_status()
        content = resp.content
    except ImportError:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            content = resp.read()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(content)
    log(f"  wrote cache {cache_path} ({len(content) / 1e3:.0f} KB)")
    return content


def parse_amount(v) -> Optional[float]:
    s = clean_text(v)
    if not s:
        return None
    try:
        amount = float(s.replace(",", ""))
    except ValueError:
        return None
    return amount if amount > 0 else None


def iso_date(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def year_of(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        return int(iso[:4])
    except ValueError:
        return None


def build_row(rec: dict) -> Optional[dict]:
    source_identifier = clean_text(rec.get("Identifier"))
    if not source_identifier:
        return None
    amount = parse_amount(rec.get("Amount Awarded"))
    currency = clean_text(rec.get("Currency"))
    award_date = iso_date(get_col(rec, "Award Date", "Award date"))

    return {
        "source_identifier": source_identifier,
        "funder_award_id": source_identifier,
        "line_no": clean_text(rec.get("LineNo")),
        "financial_reference": clean_text(rec.get("Financial reference")),
        "title": clean_text(rec.get("Title")),
        "description": clean_text(rec.get("Description")),
        "amount": amount,
        "amount_raw": clean_text(rec.get("Amount Awarded")),
        "currency": currency.upper() if amount is not None and currency else None,
        "award_date": award_date,
        "start_date": award_date,
        "start_year": year_of(award_date),
        "recipient_org": clean_text(rec.get("Recipient Org:Name")),
        "recipient_org_identifier": clean_text(rec.get("Recipient Org:Identifier")),
        "funding_org": clean_text(rec.get("Funding Org:Name")) or FUNDER_DISPLAY_NAME,
        "funding_org_identifier": clean_text(rec.get("Funding Org:Identifier")),
        "grant_programme": clean_text(get_col(rec, "Grant Programme:Title", "Grant Programme: Title")),
        "registry_identifier": SOURCE_REGISTRY_IDENTIFIER,
        "source_workbook_url": SOURCE_URL,
    }


def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for field in (
        "funder_award_id", "financial_reference", "title", "description",
        "amount", "currency", "award_date", "start_date", "start_year",
        "recipient_org", "recipient_org_identifier", "grant_programme",
    ):
        non_null = sum(1 for row in rows if row.get(field) not in (None, "", []))
        log(f"  {field:<26} coverage {non_null}/{n} ({non_null * 100 / n:.1f}%)")

    ids = [row["funder_award_id"] for row in rows if row.get("funder_award_id")]
    dupes = [k for k, v in Counter(ids).items() if v > 1]
    if dupes:
        raise RuntimeError(f"funder_award_id collisions: {dupes[:5]}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")

    years = [row["start_year"] for row in rows if row.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}-{max(years)}")
    amounts = [row["amount"] for row in rows if row.get("amount") is not None]
    if amounts:
        amounts_sorted = sorted(amounts)
        log(
            f"  amount stats: n={len(amounts)} ({len(amounts) * 100 / n:.1f}%) "
            f"min={min(amounts):,.0f} median={amounts_sorted[len(amounts_sorted) // 2]:,.0f} "
            f"max={max(amounts):,.0f} total={sum(amounts):,.0f}"
        )

    programs = Counter(row.get("grant_programme") or "(none)" for row in rows)
    recipients = Counter(row.get("recipient_org") or "(none)" for row in rows)
    log("  top programmes: " + ", ".join(f"{k}={v}" for k, v in programs.most_common(10)))
    log("  top recipients: " + ", ".join(f"{k}={v}" for k, v in recipients.most_common(10)))


def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping section 1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        previous_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        previous_df = pd.read_parquet(io.BytesIO(previous_bytes))
        previous_count = len(previous_df)
        log(f"  section 1.4 shrink-check: previous S3 parquet had {previous_count:,} rows")
        if new_count < previous_count:
            log(f"  section 1.4 FAIL: new ({new_count:,}) < previous ({previous_count:,}). Aborting.")
            return False
        log(f"  section 1.4 OK: new {new_count:,} >= previous {previous_count:,}")
        return True
    except Exception as exc:
        log(f"  section 1.4 shrink-check skipped: {type(exc).__name__}: {str(exc)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Guy's and St Thomas' Charity 360Giving grants -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "guys_st_thomas_grants.parquet"

    log("=== Guy's and St Thomas' Charity grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache_dir={args.cache_dir}")

    import io
    content = download_file(SOURCE_URL, args.cache_dir / SOURCE_WORKBOOK_NAME, args.skip_download)
    workbook = pd.ExcelFile(io.BytesIO(content))
    if "Sheet1" not in workbook.sheet_names:
        raise RuntimeError(f"Expected sheet 'Sheet1', found {workbook.sheet_names}")
    raw_df = pd.read_excel(workbook, sheet_name="Sheet1", dtype=str)
    log(f"  loaded {len(raw_df):,} rows x {len(raw_df.columns)} cols from Sheet1")

    rows = []
    for rec in raw_df.to_dict("records"):
        row = build_row(rec)
        if row is not None:
            rows.append(row)

    if args.limit is not None:
        rows = rows[:args.limit]
        log(f"--limit {args.limit}: validating/writing {len(rows)} rows")

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = pd.DataFrame(rows)
    df = df.astype("string")
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size / 1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Guy's and St Thomas' Charity ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("section 1.4 shrink-check failed.")
    upload_to_s3(output_path)
    log("=== Guy's and St Thomas' Charity ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
