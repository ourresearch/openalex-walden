#!/usr/bin/env python3
"""
Lumina Foundation grants -> S3 Data Pipeline
============================================

Downloads grant rows from Lumina Foundation's official grants database and
writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.luminafoundation.org/resources/grants/grant-database/
    https://www.luminafoundation.org/sb_grants-sitemap.xml

The grant database page and sitemap expose server-rendered grant detail pages
under /grant/{native_id}/. Each detail page contains a labeled grant table with
the native grant number, amount, location, date range, and description.

Output
------
s3://openalex-ingest/awards/lumina/lumina_grants.parquet

Usage
-----
    python lumina_to_s3.py --skip-upload
    python lumina_to_s3.py --limit 10 --skip-upload
    python lumina_to_s3.py --skip-download --skip-upload
    python lumina_to_s3.py --allow-shrink

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
from urllib.parse import urlparse

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


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://www.luminafoundation.org"
GRANT_DATABASE_URL = f"{BASE_URL}/resources/grants/grant-database/"
GRANT_SITEMAP_URL = f"{BASE_URL}/sb_grants-sitemap.xml"

FUNDER_ID = 4320306409
FUNDER_DISPLAY_NAME = "Lumina Foundation"
PROVENANCE = "lumina_grant_database"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lumina/lumina_grants.parquet"

USER_AGENT = "openalex-walden-lumina-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

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


def polite_get(url: str, timeout: int = 60, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s: {url}")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s: {url}")
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
    match = re.search(r"\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    if not match:
        return None
    amount = match.group(1).replace(",", "")
    return amount or None


def parse_date(value: str) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
    if not match:
        return None
    month, day, year = (int(part) for part in match.groups())
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_date_range(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(value)
    if not text:
        return None, None
    parts = re.split(r"\s+through\s+", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return parse_date(text), None
    return parse_date(parts[0]), parse_date(parts[1])


def extract_detail_table(soup: BeautifulSoup) -> dict[str, str]:
    data: dict[str, str] = {}
    table = soup.select_one("table.grant-data")
    if not table:
        return data
    for row in table.select("tr"):
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.select("td, th")]
        cells = [cell for cell in cells if cell]
        if len(cells) >= 2:
            key = cells[0].strip().rstrip(":").lower()
            data[key] = cells[1]
    return data


def discover_grant_urls() -> list[str]:
    print("\n" + "=" * 60)
    print("Step 1: Discover Lumina grant detail pages")
    print("=" * 60)

    database_resp = polite_get(GRANT_DATABASE_URL)
    database_soup = BeautifulSoup(database_resp.text, "html.parser")
    found_posts = {
        clean_text(article.get("data-found_posts"))
        for article in database_soup.select("article.grant[data-found_posts]")
    }
    found_posts.discard(None)
    if found_posts:
        print(f"  Grants database reports {', '.join(sorted(found_posts))} visible grant records")

    sitemap_resp = polite_get(GRANT_SITEMAP_URL)
    sitemap_soup = BeautifulSoup(sitemap_resp.text, "xml")
    urls: list[str] = []
    for loc in sitemap_soup.find_all("loc"):
        url = clean_text(loc.get_text())
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.netloc == "www.luminafoundation.org" and parsed.path.startswith("/grant/"):
            if re.search(r"/grant/[0-9]{4}-[0-9]+/$", parsed.path):
                urls.append(url)

    seen: set[str] = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            unique_urls.append(url)
            seen.add(url)

    if not unique_urls:
        raise RuntimeError("No Lumina grant detail URLs discovered from official sitemap")
    print(f"  Discovered {len(unique_urls):,} grant detail URLs from {GRANT_SITEMAP_URL}")
    return unique_urls


def parse_grant_detail(url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    recipient_name = clean_text(soup.select_one(".block-grant h1").get_text(" ", strip=True)) if soup.select_one(".block-grant h1") else None
    table = extract_detail_table(soup)

    funder_award_id = clean_text(table.get("grant #"))
    amount = parse_amount_usd(table.get("grant amount"))
    location = clean_text(table.get("location"))
    start_date, end_date = parse_date_range(table.get("date range"))

    description = None
    block = soup.select_one(".block-grant")
    if block:
        paragraphs = [clean_text(p.get_text(" ", strip=True)) for p in block.find_all("p", recursive=False)]
        paragraphs = [p for p in paragraphs if p]
        description = paragraphs[0] if paragraphs else None

    if not funder_award_id:
        match = re.search(r"/grant/([^/]+)/", url)
        funder_award_id = match.group(1) if match else None

    display_name = description or (f"Grant to {recipient_name}" if recipient_name else None)
    source_year = start_date[:4] if start_date else None

    return {
        "funder_award_id": funder_award_id,
        "display_name": display_name,
        "description": description,
        "recipient_name": recipient_name,
        "location": location,
        "amount": amount,
        "currency": CURRENCY if amount else None,
        "start_date": start_date,
        "end_date": end_date,
        "source_year": source_year,
        "funding_type": "grant",
        "funder_scheme": None,
        "provenance": PROVENANCE,
        "landing_page_url": url,
        "source_url": url,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_grant_records(limit: Optional[int]) -> list[dict[str, Any]]:
    urls = discover_grant_urls()
    if limit is not None:
        urls = urls[:limit]
        print(f"  [LIMIT] fetching first {len(urls):,} grant detail pages")

    print("\n" + "=" * 60)
    print("Step 2: Fetch and parse grant details")
    print("=" * 60)
    records = []
    total = len(urls)
    for idx, url in enumerate(urls, start=1):
        if idx == 1 or idx == total or idx % 10 == 0:
            print(f"  Fetching {idx:,}/{total:,}: {url}")
        resp = polite_get(url)
        records.append(parse_grant_detail(url, resp.text))
    print(f"  [OK] parsed {len(records):,} Lumina grant detail pages")
    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "lumina_grants_raw.json"
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
        "display_name",
        "description",
        "recipient_name",
        "location",
        "amount",
        "currency",
        "start_date",
        "end_date",
        "source_year",
        "funding_type",
        "funder_scheme",
        "provenance",
        "landing_page_url",
        "source_url",
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
        raise RuntimeError("No Lumina grant rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print("  funder_award_id duplicates: 0")

    required_cols = [
        "funder_award_id",
        "display_name",
        "description",
        "recipient_name",
        "location",
        "amount",
        "currency",
        "start_date",
        "end_date",
        "source_year",
        "landing_page_url",
    ]
    for col in required_cols:
        non_null = df[col].notna().sum()
        coverage = non_null / total if total else 0
        print(f"  {col:18s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        if col in {"funder_award_id", "display_name", "description", "recipient_name", "amount", "currency"} and coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    if full_run and total < 25:
        raise RuntimeError(f"Full Lumina run returned only {total:,} rows; expected 25 from the official sitemap.")
    if full_run and total > 25:
        print(f"  [INFO] Lumina sitemap has grown beyond the 2026-05-27 baseline: {total:,} rows")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  Total USD amount: {amount_numeric.sum():,.0f}")
    print(f"  Year range: {df['source_year'].min()} - {df['source_year'].max()}")
    print(f"  Currency distribution: {df['currency'].value_counts(dropna=False).to_dict()}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "lumina_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "lumina_grants.parquet"
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

    prev_path = output_dir / "_prev_lumina_grants.parquet"
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
            f"\n[ERROR] Refusing to shrink Lumina corpus "
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
        description="Download Lumina Foundation grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/lumina"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit grant records for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse lumina_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("Lumina Foundation grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Database:   {GRANT_DATABASE_URL}")
    print(f"  Sitemap:    {GRANT_SITEMAP_URL}")
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
        records = fetch_grant_records(limit=args.limit)

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
