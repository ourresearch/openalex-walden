#!/usr/bin/env python3
"""
Ahmanson Foundation grants -> S3 Data Pipeline
==============================================

Downloads The Ahmanson Foundation's official grant archive and writes a
parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://theahmansonfoundation.org/grants/

The public WordPress archive renders one `fc_grants` article per grant with
the foundation's native WordPress post ID, grantee, amount, program area,
fiscal year, location, and purpose text.

Output
------
s3://openalex-ingest/awards/ahmanson/ahmanson_grants.parquet

Usage
-----
    python ahmanson_to_s3.py --skip-upload
    python ahmanson_to_s3.py --limit 10 --skip-upload
    python ahmanson_to_s3.py --skip-download --skip-upload
    python ahmanson_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

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

    def _open_utf8(
        file,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


FUNDER_ID = 4320314405
FUNDER_DISPLAY_NAME = "Ahmanson Foundation"
PROVENANCE = "ahmanson_grants_archive"
CURRENCY = "USD"

GRANTS_URL = "https://theahmansonfoundation.org/grants/"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ahmanson/ahmanson_grants.parquet"

USER_AGENT = "openalex-walden-ahmanson-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25
MIN_EXPECTED_FULL_ROWS = 7000

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 90, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            print(f"  GET {url}")
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            print(f"    -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_amount_usd(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    if not match:
        return None
    return match.group(1).replace(",", "")


def parse_location(value: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    text = clean_text(value)
    if not text or "," not in text:
        return text, None, None
    parts = [part.strip() for part in text.split(",")]
    if len(parts) < 2:
        return text, None, None
    region = parts[-1]
    city = ", ".join(parts[:-1]).strip() or None
    country = "US" if region in US_STATE_CODES else None
    return city, region, country


def make_display_name(grantee_name: str, description: Optional[str]) -> str:
    if not description:
        return f"Grant to {grantee_name}"
    short_description = description
    if len(short_description) > 180:
        short_description = short_description[:177].rsplit(" ", 1)[0] + "..."
    return f"{grantee_name}: {short_description}"


def extract_info_table(article: Any) -> dict[str, str]:
    divs = article.select(".fc-grant-info-tbl > div")
    data: dict[str, str] = {}
    for idx in range(0, len(divs) - 1, 2):
        key = clean_text(divs[idx].get_text(" ", strip=True))
        value = clean_text(divs[idx + 1].get_text(" ", strip=True))
        if key and value:
            data[key.lower()] = value
    return data


def fetch_source_html() -> str:
    print("\n" + "=" * 60)
    print("Step 1: Download official Ahmanson grants archive")
    print("=" * 60)
    resp = polite_get(GRANTS_URL)
    if "fc_grants" not in resp.text or "fc-grant-info-tbl" not in resp.text:
        raise RuntimeError("Official grants page did not contain expected grant archive markup")
    return resp.text


def parse_grants(html: str, limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 2: Parse grant archive")
    print("=" * 60)
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.select("article.fc-grant")
    if not articles:
        raise RuntimeError("No grant article rows found on official grants page")
    print(f"  Found {len(articles):,} grant article rows")

    records: list[dict[str, Any]] = []
    downloaded_at = datetime.now(timezone.utc).isoformat()
    for idx, article in enumerate(articles, start=1):
        if idx == 1 or idx == len(articles) or idx % 1000 == 0:
            print(f"  Parsing {idx:,}/{len(articles):,}")

        raw_article_id = clean_text(article.get("id"))
        post_id = raw_article_id.replace("post-", "", 1) if raw_article_id else None
        if not post_id:
            raise RuntimeError(f"Grant article missing native post ID at row {idx}")

        heading = article.select_one("header h1")
        grantee_name = clean_text(heading.get_text(" ", strip=True)) if heading else None
        grantee_link = heading.find("a") if heading else None
        grantee_url = clean_text(grantee_link.get("href")) if grantee_link else None

        info = extract_info_table(article)
        amount = parse_amount_usd(info.get("amount"))
        currency = CURRENCY if amount else None
        program_area = clean_text(info.get("program area"))
        fiscal_year = clean_text(info.get("fiscal year"))
        location = clean_text(info.get("location"))

        parent_section = article.find_parent("section")
        section_year = None
        section_total_amount = None
        if parent_section:
            section_classes = parent_section.get("class") or []
            section_year = clean_text(section_classes[0]) if section_classes else None
            section_h1 = parent_section.find("h1")
            if section_h1:
                section_total_amount = parse_amount_usd(section_h1.get_text(" ", strip=True))
        if not fiscal_year:
            fiscal_year = section_year

        description_tag = article.find("p")
        description = clean_text(description_tag.get_text(" ", strip=True)) if description_tag else None
        city, region, country = parse_location(location)

        if not grantee_name:
            raise RuntimeError(f"Grant article {post_id} missing grantee name")
        if not fiscal_year:
            raise RuntimeError(f"Grant article {post_id} missing fiscal year and section-year fallback")

        records.append({
            "funder_award_id": f"ahmanson-{post_id}",
            "post_id": post_id,
            "display_name": make_display_name(grantee_name, description),
            "description": description,
            "grantee_name": grantee_name,
            "grantee_url": grantee_url,
            "amount": amount,
            "currency": currency,
            "program_area": program_area,
            "fiscal_year": fiscal_year,
            "recipient_location": location,
            "recipient_city": city,
            "recipient_region": region,
            "recipient_country": country,
            "source_section_year": section_year,
            "source_section_total_amount": section_total_amount,
            "source_row_number": str(idx),
            "landing_page_url": f"{GRANTS_URL}#post-{post_id}",
            "source_url": GRANTS_URL,
            "provenance": PROVENANCE,
            "downloaded_at": downloaded_at,
        })

        if limit is not None and len(records) >= limit:
            print(f"  [LIMIT] parsed first {len(records):,} grant rows")
            return records

    print(f"  [OK] parsed {len(records):,} grant rows")
    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "ahmanson_grants_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
    return records


def normalize_records(records: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 3: Normalize and validate")
    print("=" * 60)
    df = pd.DataFrame(records)

    expected_cols = [
        "funder_award_id",
        "post_id",
        "display_name",
        "description",
        "grantee_name",
        "grantee_url",
        "amount",
        "currency",
        "program_area",
        "fiscal_year",
        "recipient_location",
        "recipient_city",
        "recipient_region",
        "recipient_country",
        "source_section_year",
        "source_section_total_amount",
        "source_row_number",
        "landing_page_url",
        "source_url",
        "provenance",
        "downloaded_at",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols]

    validate_dataframe(df, full_run=full_run)

    # Prevent pyarrow type drift on sparse nullable fields.
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("No Ahmanson grant rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print("  funder_award_id duplicates: 0")

    required_thresholds = {
        "funder_award_id": 1.00,
        "display_name": 0.99,
        "grantee_name": 0.99,
        "amount": 0.95,
        "currency": 0.95,
        "program_area": 0.95,
        "fiscal_year": 0.99,
        "recipient_location": 0.95,
        "recipient_country": 0.95,
        "description": 0.95,
        "landing_page_url": 0.99,
    }
    for col, min_coverage in required_thresholds.items():
        non_null = df[col].notna().sum()
        coverage = non_null / total if total else 0
        print(f"  {col:24s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < min_coverage:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    if full_run and total < MIN_EXPECTED_FULL_ROWS:
        raise RuntimeError(
            f"Full Ahmanson run returned only {total:,} rows; expected at least "
            f"{MIN_EXPECTED_FULL_ROWS:,} from the official grants archive."
        )

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    fiscal_years = pd.to_numeric(df["fiscal_year"], errors="coerce")
    print(f"  Fiscal year range: {int(fiscal_years.min())} - {int(fiscal_years.max())}")
    print(f"  Fiscal year distribution: {df['fiscal_year'].value_counts(dropna=False).sort_index().to_dict()}")
    print(f"  Program area distribution: {df['program_area'].value_counts(dropna=False).to_dict()}")
    print(f"  Currency distribution: {df['currency'].value_counts(dropna=False).to_dict()}")
    print(f"  Recipient country distribution: {df['recipient_country'].value_counts(dropna=False).to_dict()}")
    print(f"  Total USD amount: {amount_numeric.sum():,.0f}")
    print(f"  Amount range USD: {amount_numeric.min():,.0f} - {amount_numeric.max():,.0f}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "ahmanson_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "ahmanson_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
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

    prev_path = output_dir / "_prev_ahmanson_grants.parquet"
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
        print(
            f"\n[ERROR] Refusing to shrink Ahmanson corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 5: Upload to S3")
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
        description="Download Ahmanson Foundation grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/ahmanson"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit grant rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse ahmanson_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("Ahmanson Foundation grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {GRANTS_URL}")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        records = load_cached_records(args.output_dir)
        if args.limit:
            records = records[:args.limit]
            print(f"  [LIMIT] keeping first {len(records):,} cached records")
    else:
        html = fetch_source_html()
        records = parse_grants(html, limit=args.limit)

    df = normalize_records(records, full_run=args.limit is None)
    parquet_path = write_outputs(records, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
