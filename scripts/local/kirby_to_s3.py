#!/usr/bin/env python3
"""
James N. Kirby Foundation recent grants -> S3 Data Pipeline
===========================================================

Downloads the James N. Kirby Foundation's official recent-grants table and
writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.kirbyfoundation.com.au/grants/recent-grants/
    https://www.kirbyfoundation.com.au/wp-json/wp/v2/pages?slug=recent-grants

The public WordPress REST page exposes the same server-rendered table shown on
the official Recent Grants page. The table has recipient/project descriptions
plus separate amount columns for the current recent years.

Output
------
s3://openalex-ingest/awards/kirby/kirby_recent_grants.parquet

Usage
-----
    python kirby_to_s3.py --skip-upload
    python kirby_to_s3.py --limit 10 --skip-upload
    python kirby_to_s3.py --skip-download --skip-upload
    python kirby_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
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


FUNDER_ID = 4320314616
FUNDER_DISPLAY_NAME = "James N. Kirby Foundation"
PROVENANCE = "kirby_recent_grants"
CURRENCY = "AUD"

RECENT_GRANTS_URL = "https://www.kirbyfoundation.com.au/grants/recent-grants/"
RECENT_GRANTS_API_URL = "https://www.kirbyfoundation.com.au/wp-json/wp/v2/pages?slug=recent-grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kirby/kirby_recent_grants.parquet"

USER_AGENT = "openalex-walden-kirby-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25
MIN_EXPECTED_FULL_ROWS = 150

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get_json(url: str, timeout: int = 60, max_attempts: int = 4) -> Any:
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
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET JSON failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
    text = text.replace("\xa0", " ")
    text = text.replace("\u2019", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_cell_text(cell: Any) -> Optional[str]:
    text = cell.get_text(" ", strip=True) if cell is not None else None
    if text is None:
        return None
    text = text.replace("\xa0", " ")
    text = text.replace("\u2019", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug[:max_len].strip("-") or "unknown")


def normalize_amount(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(value)
    if not text or text in {"-", "–", "—"}:
        return None, text
    match = re.search(r"([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    if not match:
        return None, text
    raw_amount = match.group(1)
    if "," not in raw_amount and re.fullmatch(r"\d{1,3}(?:\.\d{3})+", raw_amount):
        return raw_amount.replace(".", ""), text
    return raw_amount.replace(",", ""), text


def parse_recipient_and_description(cell: Any) -> tuple[Optional[str], Optional[str]]:
    strong = cell.find("strong") if cell is not None else None
    recipient = clean_cell_text(strong) if strong else None

    cell_copy = BeautifulSoup(str(cell), "html.parser")
    first_strong = cell_copy.find("strong")
    if first_strong:
        first_strong.decompose()
    description = clean_cell_text(cell_copy)
    return recipient, description


def make_display_name(recipient_name: str, description: Optional[str]) -> str:
    if not description:
        return f"Grant to {recipient_name}"
    short_description = description
    if len(short_description) > 180:
        short_description = short_description[:177].rsplit(" ", 1)[0] + "..."
    return f"{recipient_name}: {short_description}"


def make_funder_award_id(year: str, category: str, recipient_name: str,
                         description: Optional[str]) -> str:
    digest_src = "|".join([
        year,
        category.lower(),
        recipient_name.lower(),
        (description or "").lower(),
    ])
    digest = hashlib.sha1(digest_src.encode("utf-8")).hexdigest()[:12]
    return f"kirby-{year}-{slugify(category, 32)}-{slugify(recipient_name)}-{digest}"


def fetch_recent_grants_page() -> dict[str, Any]:
    print("\n" + "=" * 60)
    print("Step 1: Download official recent-grants page from WordPress REST")
    print("=" * 60)
    payload = polite_get_json(RECENT_GRANTS_API_URL)
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"Unexpected WordPress REST response from {RECENT_GRANTS_API_URL}")
    page = payload[0]
    if page.get("link") != RECENT_GRANTS_URL:
        print(f"  [WARN] REST page link is {page.get('link')}; expected {RECENT_GRANTS_URL}")
    content = page.get("content", {}).get("rendered")
    if not content:
        raise RuntimeError("WordPress REST page has no rendered content")
    print(f"  [OK] page id {page.get('id')} modified_gmt={page.get('modified_gmt')}")
    return page


def parse_recent_grants(page: dict[str, Any], limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 2: Parse recent-grants table")
    print("=" * 60)
    html = page["content"]["rendered"]
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Could not find the Recent Grants table in official page content")

    rows = table.find_all("tr")
    if not rows:
        raise RuntimeError("Recent Grants table has no rows")

    header_cells = rows[0].find_all(["td", "th"])
    years = [clean_cell_text(cell) for cell in header_cells[1:]]
    years = [year for year in years if year and re.fullmatch(r"\d{4}", year)]
    if len(years) < 2:
        raise RuntimeError(f"Expected at least two year columns; got {years}")
    print(f"  Year columns: {', '.join(years)}")

    records: list[dict[str, Any]] = []
    current_category: Optional[str] = None
    source_row_number = 0
    downloaded_at = datetime.now(timezone.utc).isoformat()

    for tr in rows[1:]:
        source_row_number += 1
        cells = tr.find_all(["td", "th"])
        texts = [clean_cell_text(cell) for cell in cells]
        texts = [text for text in texts if text is not None]
        if not texts:
            continue

        if len(cells) >= 3:
            first = clean_cell_text(cells[0])
            amount_markers = [clean_cell_text(cell) for cell in cells[1:1 + len(years)]]
            if first and all(marker == "$" for marker in amount_markers):
                current_category = first.upper()
                print(f"  Category: {current_category}")
                continue

        if current_category is None or len(cells) < 1 + len(years):
            continue

        recipient_name, description = parse_recipient_and_description(cells[0])
        if not recipient_name:
            continue

        for idx, year in enumerate(years):
            amount, amount_display = normalize_amount(clean_cell_text(cells[idx + 1]))
            if amount is None:
                continue
            funder_award_id = make_funder_award_id(year, current_category, recipient_name, description)
            record = {
                "funder_award_id": funder_award_id,
                "display_name": make_display_name(recipient_name, description),
                "description": description,
                "recipient_name": recipient_name,
                "source_category": current_category,
                "source_year": year,
                "amount": amount,
                "currency": CURRENCY,
                "funding_type": "grant",
                "funder_scheme": current_category,
                "source_amount_display": amount_display,
                "source_row_number": str(source_row_number),
                "source_column_year": str(idx + 1),
                "landing_page_url": RECENT_GRANTS_URL,
                "source_url": RECENT_GRANTS_API_URL,
                "wp_page_id": str(page.get("id") or ""),
                "wp_modified_gmt": clean_text(page.get("modified_gmt")),
                "provenance": PROVENANCE,
                "downloaded_at": downloaded_at,
            }
            records.append(record)
            if limit is not None and len(records) >= limit:
                print(f"  [LIMIT] parsed first {len(records):,} award rows")
                return records

    print(f"  [OK] parsed {len(records):,} award rows")
    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "kirby_recent_grants_raw.json"
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
        "source_category",
        "source_year",
        "amount",
        "currency",
        "funding_type",
        "funder_scheme",
        "source_amount_display",
        "source_row_number",
        "source_column_year",
        "landing_page_url",
        "source_url",
        "wp_page_id",
        "wp_modified_gmt",
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
        raise RuntimeError("No James N. Kirby Foundation grant rows parsed")

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
        "source_category",
        "source_year",
        "amount",
        "currency",
        "landing_page_url",
    ]
    for col in required_cols:
        non_null = df[col].notna().sum()
        coverage = non_null / total if total else 0
        print(f"  {col:20s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        min_coverage = 0.90 if col == "description" else 0.95
        if coverage < min_coverage:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    if full_run and total < MIN_EXPECTED_FULL_ROWS:
        raise RuntimeError(
            f"Full Kirby run returned only {total:,} rows; expected at least "
            f"{MIN_EXPECTED_FULL_ROWS:,} from the official recent-grants table."
        )

    years = sorted(df["source_year"].dropna().unique().tolist())
    categories = df["source_category"].value_counts(dropna=False).to_dict()
    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")

    print(f"  Year range: {min(years)} - {max(years)}")
    print(f"  Year distribution: {df['source_year'].value_counts(dropna=False).to_dict()}")
    print(f"  Category distribution: {categories}")
    print(f"  Currency distribution: {df['currency'].value_counts(dropna=False).to_dict()}")
    print(f"  Total AUD amount: {amount_numeric.sum():,.0f}")
    print(f"  Amount range AUD: {amount_numeric.min():,.0f} - {amount_numeric.max():,.0f}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "kirby_recent_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "kirby_recent_grants.parquet"
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

    prev_path = output_dir / "_prev_kirby_recent_grants.parquet"
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
            f"\n[ERROR] Refusing to shrink Kirby corpus "
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
        description="Download James N. Kirby Foundation recent grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/kirby"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit award rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse kirby_recent_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("James N. Kirby Foundation recent grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {RECENT_GRANTS_URL}")
    print(f"  API:        {RECENT_GRANTS_API_URL}")
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
        page = fetch_recent_grants_page()
        records = parse_recent_grants(page, limit=args.limit)

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
