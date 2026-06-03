#!/usr/bin/env python3
"""
David and Elaine Potter Foundation grants -> S3 Data Pipeline
=============================================================

Downloads The David and Elaine Potter Foundation's official grant-recipient
sources and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The sources are first-party:

    https://www.potterfoundation.com/grant_recipients.html
    https://www.potterfoundation.com/downloads/360giving_Potter_Foundation_data_2013-2024.xlsx

The grant-recipient page publishes year-by-year grant tables for 2000-2024.
The linked official 360Giving workbook publishes richer structured records for
2013-2024, including native identifiers, descriptions, recipient identifiers,
award dates, programme labels, and GBP amounts. This script uses the workbook
for 2013-2024 and the HTML tables for 2000-2012.

Output
------
s3://openalex-ingest/awards/potter_foundation/potter_foundation_grants.parquet

Usage
-----
    python potter_foundation_to_s3.py --skip-upload
    python potter_foundation_to_s3.py --limit 10 --skip-upload
    python potter_foundation_to_s3.py --skip-download --skip-upload
    python potter_foundation_to_s3.py --allow-shrink

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
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
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


FUNDER_ID = 4320314720
FUNDER_DISPLAY_NAME = "David and Elaine Potter Foundation"
PROVENANCE = "potter_foundation_grants"
DEFAULT_CURRENCY = "GBP"

GRANTS_PAGE_URL = "https://www.potterfoundation.com/grant_recipients.html"
WORKBOOK_URL = (
    "https://www.potterfoundation.com/downloads/"
    "360giving_Potter_Foundation_data_2013-2024.xlsx"
)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/potter_foundation/potter_foundation_grants.parquet"

USER_AGENT = "openalex-walden-potter-foundation-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25
MIN_EXPECTED_FULL_ROWS = 550

COUNTRY_NORMALIZATION = {
    "austria": "AT",
    "burkina faso": "BF",
    "canada": "CA",
    "gb": "GB",
    "ghana": "GH",
    "israel": "IL",
    "norway": "NO",
    "pakistan": "PK",
    "south africa": "ZA",
    "swaziland": "SZ",
    "switzerland": "CH",
    "uk": "GB",
    "united kingdom": "GB",
    "us": "US",
    "usa": "US",
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


def polite_get(url: str, timeout: int = 60, max_attempts: int = 4) -> requests.Response:
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
    if pd.isna(value):
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def slugify(value: Optional[str], max_len: int = 60) -> str:
    text = clean_text(value) or "unknown"
    text = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def short_hash(*values: Any) -> str:
    joined = "|".join("" if value is None else str(value) for value in values)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:10]


def normalize_country(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    mapped = COUNTRY_NORMALIZATION.get(text.lower())
    if mapped:
        return mapped
    if re.fullmatch(r"[A-Z]{2}", text):
        return text
    return None


def parse_amount(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", text)
    if not match:
        return None
    amount = match.group(0).replace(",", "")
    if amount.endswith(".0"):
        amount = amount[:-2]
    return amount


def excel_serial_to_date(value: float) -> Optional[str]:
    if value < 20000 or value > 60000:
        return None
    return (date(1899, 12, 30) + timedelta(days=int(value))).isoformat()


def parse_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        parsed = excel_serial_to_date(float(text))
        if parsed:
            return parsed
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date().isoformat()


def column_index(cell_ref: str) -> int:
    letters = re.match(r"[A-Z]+", cell_ref)
    if not letters:
        raise RuntimeError(f"Could not parse XLSX cell reference: {cell_ref}")
    idx = 0
    for char in letters.group(0):
        idx = idx * 26 + (ord(char) - ord("A") + 1)
    return idx - 1


def read_xlsx_rows(workbook_bytes: bytes) -> list[dict[str, Optional[str]]]:
    """Read the simple one-sheet 360Giving XLSX with only stdlib modules."""
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(BytesIO(workbook_bytes)) as zf:
        shared_strings: list[str] = []
        shared_xml = ET.fromstring(zf.read("xl/sharedStrings.xml"))
        for si in shared_xml.findall("a:si", ns):
            parts = [node.text or "" for node in si.findall(".//a:t", ns)]
            shared_strings.append("".join(parts))

        sheet_xml = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        rows: list[list[Optional[str]]] = []
        for row in sheet_xml.findall(".//a:sheetData/a:row", ns):
            values: list[Optional[str]] = []
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                idx = column_index(ref)
                while len(values) <= idx:
                    values.append(None)
                cell_type = cell.attrib.get("t")
                value_node = cell.find("a:v", ns)
                if cell_type == "s":
                    value = shared_strings[int(value_node.text)] if value_node is not None and value_node.text is not None else None
                elif cell_type == "inlineStr":
                    parts = [node.text or "" for node in cell.findall(".//a:t", ns)]
                    value = "".join(parts) or None
                else:
                    value = value_node.text if value_node is not None else None
                values[idx] = clean_text(value)
            rows.append(values)

    if not rows:
        raise RuntimeError("Official 360Giving workbook had no rows")
    headers = [clean_text(value) for value in rows[0]]
    parsed_rows: list[dict[str, Optional[str]]] = []
    for row in rows[1:]:
        item = {}
        for idx, header in enumerate(headers):
            if header:
                item[header] = row[idx] if idx < len(row) else None
        if any(item.values()):
            parsed_rows.append(item)
    return parsed_rows


def make_display_name(recipient_name: str, title: Optional[str], year: Optional[str]) -> str:
    title = clean_text(title)
    if title:
        return f"{recipient_name}: {title}"
    if year:
        return f"Grant to {recipient_name} ({year})"
    return f"Grant to {recipient_name}"


def fetch_source_records(limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download official Potter Foundation sources")
    print("=" * 60)
    page_resp = polite_get(GRANTS_PAGE_URL)
    workbook_resp = polite_get(WORKBOOK_URL)

    if "Grant commitments by year" not in page_resp.text:
        raise RuntimeError("Grant recipient page did not contain expected yearly tables")

    print("\n" + "=" * 60)
    print("Step 2: Parse official sources")
    print("=" * 60)
    records = []
    downloaded_at = datetime.now(timezone.utc).isoformat()

    records.extend(parse_workbook_records(workbook_resp.content, downloaded_at))
    records.extend(parse_legacy_html_records(page_resp.text, downloaded_at))

    records.sort(key=lambda r: (r.get("award_year") or "", r.get("source_sort_key") or ""))
    if limit is not None:
        records = records[:limit]
        print(f"  [LIMIT] keeping first {len(records):,} rows")
    print(f"  [OK] parsed {len(records):,} combined grant rows")
    return records


def parse_workbook_records(workbook_bytes: bytes, downloaded_at: str) -> list[dict[str, Any]]:
    rows = read_xlsx_rows(workbook_bytes)
    expected_cols = {
        "Identifier",
        "Title",
        "Description",
        "Currency",
        "Amount Awarded",
        "Award Date",
        "Recipient Org:Name",
        "Grant Programme:Title",
    }
    missing = sorted(expected_cols - set(rows[0].keys() if rows else []))
    if missing:
        raise RuntimeError(f"Official 360Giving workbook missing expected columns: {missing}")

    records: list[dict[str, Any]] = []
    print(f"  Workbook grants sheet rows: {len(rows):,}")
    for idx, row in enumerate(rows):
        identifier = clean_text(row.get("Identifier"))
        recipient_name = clean_text(row.get("Recipient Org:Name"))
        if not identifier or not recipient_name:
            raise RuntimeError(f"Workbook row {idx + 2} missing Identifier or Recipient Org:Name")

        award_date = parse_date(row.get("Award Date"))
        award_year = award_date[:4] if award_date else None
        amount = parse_amount(row.get("Amount Awarded"))
        currency = clean_text(row.get("Currency")) or DEFAULT_CURRENCY
        title = clean_text(row.get("Title"))
        description = clean_text(row.get("Description"))
        recipient_country = normalize_country(row.get("Recipient Org:Country"))

        records.append({
            "funder_award_id": f"potter-{identifier.lower()}",
            "source_record_id": identifier,
            "source_record_type": "360giving_workbook",
            "display_name": make_display_name(recipient_name, title, award_year),
            "description": description,
            "title": title,
            "recipient_name": recipient_name,
            "recipient_org_identifier": clean_text(row.get("Recipient Org:Identifier")),
            "recipient_charity_number": clean_text(row.get("Recipient Org:Charity Number")),
            "recipient_city": clean_text(row.get("Recipient Org:City")),
            "recipient_postal_code": clean_text(row.get("Recipient Org:Postal Code")),
            "recipient_country": recipient_country,
            "recipient_country_raw": clean_text(row.get("Recipient Org:Country")),
            "beneficiary_location_name": clean_text(row.get("Beneficiary Location:Name")),
            "beneficiary_country": normalize_country(row.get("Beneficiary Location:Country Code")),
            "beneficiary_country_raw": clean_text(row.get("Beneficiary Location:Country Code")),
            "amount": amount,
            "currency": currency if amount else None,
            "award_date": award_date,
            "award_year": award_year,
            "duration_months": parse_amount(row.get("Planned Dates:Duration (months)")),
            "program_code": clean_text(row.get("Grant Programme:Code")),
            "program_title": clean_text(row.get("Grant Programme:Title")),
            "from_open_call": clean_text(row.get("From an open call?")),
            "funding_org_identifier": clean_text(row.get("Funding Org:Identifier")),
            "funding_org_name": clean_text(row.get("Funding Org:Name")),
            "last_modified": parse_date(row.get("Last modified")),
            "source_row_number": str(idx + 2),
            "source_sort_key": f"workbook-{idx + 2:04d}",
            "landing_page_url": GRANTS_PAGE_URL,
            "source_url": WORKBOOK_URL,
            "provenance": PROVENANCE,
            "downloaded_at": downloaded_at,
        })

    return records


def parse_legacy_html_records(html: str, downloaded_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []
    for table_idx, table in enumerate(soup.find_all("table"), start=1):
        rows = table.find_all("tr")
        if not rows:
            continue
        header_cells = [clean_text(cell.get_text(" ", strip=True)) for cell in rows[0].find_all(["th", "td"])]
        year_match = re.search(r"(20\d{2})", " ".join(cell or "" for cell in header_cells))
        if not year_match:
            continue
        year = int(year_match.group(1))
        if year >= 2013:
            continue

        parsed_for_year = 0
        for row_idx, tr in enumerate(rows[1:], start=1):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"])]
            if len(cells) < 6:
                continue
            row_number, _blank, recipient_name, amount_raw, region, category = cells[:6]
            if not row_number or not re.search(r"\d", row_number):
                continue
            if not recipient_name or recipient_name.lower() == "total":
                continue
            amount = parse_amount(amount_raw)
            source_hash = short_hash(year, row_number, recipient_name, amount, region, category)
            award_id = f"potter-html-{year}-{int(float(row_number)):03d}-{slugify(recipient_name, 36)}-{source_hash}"
            region_country = normalize_country(region)
            records.append({
                "funder_award_id": award_id,
                "source_record_id": f"html-{year}-{int(float(row_number)):03d}",
                "source_record_type": "html_table",
                "display_name": make_display_name(recipient_name, None, str(year)),
                "description": None,
                "title": None,
                "recipient_name": recipient_name,
                "recipient_org_identifier": None,
                "recipient_charity_number": None,
                "recipient_city": None,
                "recipient_postal_code": None,
                "recipient_country": region_country,
                "recipient_country_raw": region,
                "beneficiary_location_name": region,
                "beneficiary_country": region_country,
                "beneficiary_country_raw": region,
                "amount": amount,
                "currency": DEFAULT_CURRENCY if amount else None,
                "award_date": None,
                "award_year": str(year),
                "duration_months": None,
                "program_code": None,
                "program_title": category,
                "from_open_call": None,
                "funding_org_identifier": None,
                "funding_org_name": FUNDER_DISPLAY_NAME,
                "last_modified": None,
                "source_row_number": str(row_idx + 1),
                "source_sort_key": f"html-{table_idx:02d}-{row_idx:04d}",
                "landing_page_url": GRANTS_PAGE_URL,
                "source_url": GRANTS_PAGE_URL,
                "provenance": PROVENANCE,
                "downloaded_at": downloaded_at,
            })
            parsed_for_year += 1
        print(f"  HTML table {year}: {parsed_for_year:,} legacy grant rows")

    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "potter_foundation_grants_raw.json"
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
        "source_record_id",
        "source_record_type",
        "display_name",
        "description",
        "title",
        "recipient_name",
        "recipient_org_identifier",
        "recipient_charity_number",
        "recipient_city",
        "recipient_postal_code",
        "recipient_country",
        "recipient_country_raw",
        "beneficiary_location_name",
        "beneficiary_country",
        "beneficiary_country_raw",
        "amount",
        "currency",
        "award_date",
        "award_year",
        "duration_months",
        "program_code",
        "program_title",
        "from_open_call",
        "funding_org_identifier",
        "funding_org_name",
        "last_modified",
        "source_row_number",
        "source_sort_key",
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
        raise RuntimeError("No Potter Foundation grant rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print("  funder_award_id duplicates: 0")

    required_thresholds = {
        "funder_award_id": 1.00,
        "display_name": 1.00,
        "recipient_name": 1.00,
        "amount": 1.00,
        "currency": 1.00,
        "award_year": 1.00,
        "program_title": 0.99,
        "landing_page_url": 1.00,
    }
    for col, min_coverage in required_thresholds.items():
        non_null = df[col].notna().sum()
        coverage = non_null / total if total else 0
        print(f"  {col:24s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < min_coverage:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    if full_run and total < MIN_EXPECTED_FULL_ROWS:
        raise RuntimeError(
            f"Full Potter Foundation run returned only {total:,} rows; expected at least "
            f"{MIN_EXPECTED_FULL_ROWS:,} from the official grant sources."
        )

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    years = pd.to_numeric(df["award_year"], errors="coerce")
    print(f"  Award year range: {int(years.min())} - {int(years.max())}")
    print(f"  Source type distribution: {df['source_record_type'].value_counts(dropna=False).to_dict()}")
    print(f"  Award year distribution: {df['award_year'].value_counts(dropna=False).sort_index().to_dict()}")
    print(f"  Program distribution: {df['program_title'].value_counts(dropna=False).to_dict()}")
    print(f"  Currency distribution: {df['currency'].value_counts(dropna=False).to_dict()}")
    print(f"  Recipient country distribution: {df['recipient_country'].value_counts(dropna=False).to_dict()}")
    print(f"  Description coverage: {df['description'].notna().sum():,}/{total:,}")
    print(f"  Total GBP amount: {amount_numeric.sum():,.0f}")
    print(f"  Amount range GBP: {amount_numeric.min():,.0f} - {amount_numeric.max():,.0f}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "potter_foundation_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "potter_foundation_grants.parquet"
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

    prev_path = output_dir / "_prev_potter_foundation_grants.parquet"
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
            f"\n[ERROR] Refusing to shrink Potter Foundation corpus "
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
        description="Download Potter Foundation grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/potter_foundation"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit grant rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse potter_foundation_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("David and Elaine Potter Foundation grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Sources:    {GRANTS_PAGE_URL}")
    print(f"              {WORKBOOK_URL}")
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
        records = fetch_source_records(limit=args.limit)

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
