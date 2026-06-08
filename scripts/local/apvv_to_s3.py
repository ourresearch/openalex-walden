#!/usr/bin/env python3
"""
APVV Funded Projects -> S3 Pipeline
===================================

Downloads funded-project records from the official Slovak Research and
Development Agency (APVV) public project database:

    https://www.apvv.sk/databaza-financovanych-projektov.html

Source authority
----------------
APVV publishes a searchable "Databaza financovanych projektov" table on its
own website. The table includes one row per funded project with a real APVV
project number, project title, recipient organization, approved support in EUR,
call label, scientific field, and final-report PDF link when available.

OpenAlex funder
---------------
F4320323251 - Agentura na Podporu Vyskumu a Vyvoja (SK)
ROR: NULL
DOI: NULL

Output
------
s3://openalex-ingest/awards/apvv/apvv_projects.parquet

Usage
-----
    python scripts/local/apvv_to_s3.py --limit 10 --skip-upload
    python scripts/local/apvv_to_s3.py --skip-upload
    python scripts/local/apvv_to_s3.py --skip-download --skip-upload
    python scripts/local/apvv_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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


SOURCE_URL = "https://www.apvv.sk/databaza-financovanych-projektov.html"
FUNDER_ID = 4320323251
FUNDER_DISPLAY_NAME = "Agentúra na Podporu Výskumu a Vývoja"
PROVENANCE = "apvv_funded_projects"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/apvv/apvv_projects.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp")
JSON_CACHE_FILENAME = "apvv_projects.json"
PARQUET_FILENAME = "apvv_projects.parquet"

REQUEST_TIMEOUT = (8, 25)
MAX_RETRIES = 5
PAGE_SLEEP_SECONDS = 0.05
EXPECTED_MIN_FULL_ROWS = 5000

HEADERS = {
    "User-Agent": "openalex-walden-apvv/1.0 (+https://openalex.org)",
    "Accept": "text/html,application/xhtml+xml",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"[\u00a0\u2028\u2029 \t\r\n]+", " ", text).strip()
    return text or None


def fetch_html(session: requests.Session, url: str) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                response.encoding = "utf-8"
                return response.text
            log(f"  HTTP {response.status_code} for {url} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            last_exc = exc
            log(f"  {url}: {exc} (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def parse_last_page_index(html_text: str) -> int:
    indexes = [int(value) for value in re.findall(r"select_index=(\d+)", html_text)]
    if not indexes:
        raise RuntimeError("Could not find APVV pagination links")
    return max(indexes)


def page_url(index: int) -> str:
    if index == 0:
        return SOURCE_URL
    return f"{SOURCE_URL}?select_index={index}&order_by=project_number-ASC"


def parse_amount_eur(amount_text: str | None) -> tuple[str | None, str | None]:
    text = clean_text(amount_text)
    if not text or text == "-":
        return None, None
    first_part = text.split("(", 1)[0]
    match = re.search(r"([0-9][0-9\s.,]*)", first_part)
    if not match:
        return None, None
    normalized = match.group(1).replace(" ", "").replace(",", "")
    try:
        value = float(normalized)
    except ValueError:
        return None, None
    if value <= 0:
        return None, None
    return f"{value:.2f}", "EUR"


def parse_call_year(call_text: str | None) -> str | None:
    text = clean_text(call_text)
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return None
    year = int(match.group(0))
    return str(year) if 1900 <= year <= 2100 else None


def classify_funding_type(call_text: str | None, field_text: str | None) -> str:
    haystack = " ".join(part for part in [call_text, field_text] if part).lower()
    if any(token in haystack for token in ["msca", "ľudský potenciál", "ludsky potencial"]):
        return "fellowship"
    if "populariz" in haystack:
        return "training"
    return "research"


def parse_project_rows(html_text: str, source_page_url: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.select_one("table.projectsearchresults")
    if table is None:
        return []

    records: list[dict[str, str | None]] = []
    for tr in table.select("tr")[1:]:
        cells = tr.select("td")
        if len(cells) < 7:
            continue
        project_number = clean_text(cells[0].get_text(" ", strip=True))
        title = clean_text(cells[1].get_text(" ", strip=True))
        recipient = clean_text(cells[2].get_text(" ", strip=True))
        amount_raw = clean_text(cells[3].get_text(" ", strip=True))
        call = clean_text(cells[4].get_text(" ", strip=True))
        scientific_field = clean_text(cells[5].get_text(" ", strip=True))
        final_report_text = clean_text(cells[6].get_text(" ", strip=True))
        links = [urljoin(SOURCE_URL, a.get("href")) for a in cells[6].select("a[href]") if a.get("href")]
        final_report_url = links[0] if links else None

        if not project_number or not title:
            continue

        amount, currency = parse_amount_eur(amount_raw)
        start_year = parse_call_year(call)
        records.append(
            {
                "funder_award_id": project_number,
                "project_number": project_number,
                "display_name": title,
                "description": None,
                "recipient_organization": recipient,
                "amount": amount,
                "currency": currency,
                "amount_raw": amount_raw,
                "funder_scheme": call,
                "scientific_field": None if scientific_field == "-" else scientific_field,
                "funding_type": classify_funding_type(call, scientific_field),
                "start_year": start_year,
                "end_year": None,
                "start_date": None,
                "end_date": None,
                "final_report_text": None if final_report_text == "-" else final_report_text,
                "final_report_url": final_report_url,
                "landing_page_url": final_report_url or source_page_url,
                "source_page_url": source_page_url,
                "provenance": PROVENANCE,
                "funder_id": str(FUNDER_ID),
                "funder_display_name": FUNDER_DISPLAY_NAME,
            }
        )
    return records


def download_records(limit: int | None = None) -> list[dict[str, str | None]]:
    session = requests.Session()
    session.headers.update(HEADERS)

    first_html = fetch_html(session, SOURCE_URL)
    last_index = parse_last_page_index(first_html)
    log(f"APVV database reports page indexes 0..{last_index}")

    records: list[dict[str, str | None]] = []
    empty_pages: list[int] = []
    for index in range(last_index + 1):
        url = page_url(index)
        html_text = first_html if index == 0 else fetch_html(session, url)
        page_records = parse_project_rows(html_text, url)
        if not page_records:
            empty_pages.append(index)
        records.extend(page_records)
        if index in {0, 1, last_index} or index % 25 == 0:
            log(f"  page {index}/{last_index}: {len(page_records)} rows (total {len(records)})")
        if limit is not None and len(records) >= limit:
            records = records[:limit]
            log(f"  --limit {limit} reached; stopping after page {index}")
            break
        time.sleep(PAGE_SLEEP_SECONDS)

    if empty_pages:
        raise RuntimeError(f"APVV returned empty result pages where rows were expected: {empty_pages[:20]}")
    return records


def validate_records(records: list[dict[str, str | None]], full_run: bool) -> None:
    if not records:
        raise RuntimeError("No APVV project records parsed")
    if full_run and len(records) < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(f"Full APVV run parsed only {len(records)} rows; expected at least {EXPECTED_MIN_FULL_ROWS}")

    ids = [record["funder_award_id"] for record in records]
    duplicate_count = len(ids) - len(set(ids))
    if duplicate_count:
        raise RuntimeError(f"Duplicate APVV project numbers found: {duplicate_count}")

    df = pd.DataFrame(records)
    amount_count = df["amount"].notna().sum()
    title_count = df["display_name"].notna().sum()
    recipient_count = df["recipient_organization"].notna().sum()
    scheme_count = df["funder_scheme"].notna().sum()
    start_year_count = df["start_year"].notna().sum()
    final_report_count = df["final_report_url"].notna().sum()
    amount_total = pd.to_numeric(df["amount"], errors="coerce").sum()
    start_years = pd.to_numeric(df["start_year"], errors="coerce")

    log("")
    log("Validation summary")
    log(f"  rows: {len(df):,}")
    log(f"  unique funder_award_id: {df['funder_award_id'].nunique():,}")
    log(f"  title coverage: {title_count:,}/{len(df):,} ({title_count / len(df):.1%})")
    log(f"  recipient coverage: {recipient_count:,}/{len(df):,} ({recipient_count / len(df):.1%})")
    log(f"  amount coverage: {amount_count:,}/{len(df):,} ({amount_count / len(df):.1%}), total EUR {amount_total:,.0f}")
    log(f"  scheme coverage: {scheme_count:,}/{len(df):,} ({scheme_count / len(df):.1%})")
    log(f"  start_year coverage: {start_year_count:,}/{len(df):,} ({start_year_count / len(df):.1%})")
    log(f"  start_year range: {int(start_years.min()) if start_year_count else 'NULL'}-{int(start_years.max()) if start_year_count else 'NULL'}")
    log(f"  final report URL coverage: {final_report_count:,}/{len(df):,} ({final_report_count / len(df):.1%})")
    log("")
    log("Top calls")
    for scheme, count in df["funder_scheme"].value_counts(dropna=False).head(12).items():
        log(f"  {scheme}: {count:,}")
    log("")
    log("Top scientific fields")
    for field, count in df["scientific_field"].fillna("NULL").value_counts(dropna=False).head(12).items():
        log(f"  {field}: {count:,}")


def load_or_download(output_dir: Path, skip_download: bool, limit: int | None) -> list[dict[str, str | None]]:
    cache_path = output_dir / JSON_CACHE_FILENAME
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download requested but cache does not exist: {cache_path}")
        log(f"Loading cached APVV records from {cache_path}")
        records = json.loads(cache_path.read_text(encoding="utf-8"))
        if limit is not None:
            records = records[:limit]
        return records

    records = download_records(limit=limit)
    cache_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Wrote JSON cache: {cache_path}")
    return records


def write_parquet(records: list[dict[str, str | None]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    enriched = [{**record, "downloaded_at": downloaded_at} for record in records]

    df = pd.DataFrame(enriched)
    ordered_columns = [
        "funder_award_id",
        "project_number",
        "display_name",
        "description",
        "recipient_organization",
        "amount",
        "currency",
        "amount_raw",
        "funder_scheme",
        "scientific_field",
        "funding_type",
        "start_year",
        "end_year",
        "start_date",
        "end_date",
        "final_report_text",
        "final_report_url",
        "landing_page_url",
        "source_page_url",
        "provenance",
        "funder_id",
        "funder_display_name",
        "downloaded_at",
    ]
    df = df[ordered_columns]
    df = df.astype("string")

    parquet_path = output_dir / PARQUET_FILENAME
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log(f"Wrote parquet: {parquet_path} ({len(df):,} rows)")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping section 1.4 shrink-check")
        return True

    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 is required for the §1.4 shrink-check; rerun with --skip-upload for local validation.")

    previous_path = output_dir / "_previous_apvv_projects.parquet"
    s3 = boto3.client("s3")
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous_path))
    except Exception as exc:
        log(f"  No previous S3 parquet found or not accessible ({exc}); allowing first upload.")
        return True

    previous_count = len(pd.read_parquet(previous_path))
    log(f"  §1.4 no-shrink check: previous={previous_count:,}, new={new_count:,}")
    if new_count < previous_count:
        log("  [BLOCKED] New APVV corpus is smaller than previous S3 object.")
        log("  Re-run with --allow-shrink only if this shrink is intentional and documented.")
        return False
    return True


def upload_to_s3(parquet_path: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 is required for S3 upload; use --skip-upload for local validation.")
    log(f"Uploading {parquet_path} to s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(parquet_path), S3_BUCKET, S3_KEY)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download APVV funded projects and write/upload parquet.")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for cache/parquet outputs")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSON from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    records = load_or_download(args.output_dir, args.skip_download, args.limit)
    full_run = args.limit is None
    validate_records(records, full_run=full_run)
    parquet_path = write_parquet(records, args.output_dir)

    if args.skip_upload:
        log("")
        log("[SKIP] --skip-upload set; not uploading to S3.")
        return

    if not check_no_shrink(len(records), args.allow_shrink, args.output_dir):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
