#!/usr/bin/env python3
"""
Hong Kong Research Grants Council projects -> S3 Data Pipeline
==============================================================

Downloads project records from the official Research Grants Council (RGC)
"Project Enquiry by General Public" portal and writes a parquet file for the
OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.ugc.edu.hk/eng/rgc/funding_opport/grf/funded_research.html
    https://cerg1.ugc.edu.hk/cergprod/scrrm00541.jsp
    https://cerg1.ugc.edu.hk/cergprod/scrrm00542.jsp

RGC links "Project Enquiry" from its official funded-research pages. The
enquiry portal exposes one detail page per funded project with project number,
scheme, titles, PI/co-investigators, institution, panel, subject area, approved
fund, status, completion date, and abstract.

Output
------
s3://openalex-ingest/awards/rgc_hk/rgc_hk_projects.parquet

Usage
-----
    python rgc_hk_to_s3.py --skip-upload
    python rgc_hk_to_s3.py --limit 10 --skip-upload
    python rgc_hk_to_s3.py --skip-download --skip-upload
    python rgc_hk_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
import json
import math
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from bs4 import BeautifulSoup
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


FUNDER_ID = 4320321592
FUNDER_DISPLAY_NAME = "Research Grants Council, University Grants Committee"
PROVENANCE = "rgc_hk_project_enquiry"
CURRENCY = "HKD"

SOURCE_PAGE_URL = "https://www.ugc.edu.hk/eng/rgc/funding_opport/grf/funded_research.html"
SEARCH_URL = "https://cerg1.ugc.edu.hk/cergprod/scrrm00541.jsp"
DETAIL_URL = "https://cerg1.ugc.edu.hk/cergprod/scrrm00542.jsp"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rgc_hk/rgc_hk_projects.parquet"

USER_AGENT = "openalex-walden-rgc-hk-ingest/1.0 (+https://openalex.org)"
DEFAULT_CACHE_DIR = Path(".cache/rgc_hk")
PAGE_SIZE = 10

THREAD_LOCAL = threading.local()


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })
        THREAD_LOCAL.session = session
    return session


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\ufeff", "").replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text or None


def compact_text(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def amount_to_string(value: Any) -> Optional[str]:
    text = compact_text(value)
    if text is None:
        return None
    text = text.replace(",", "").replace("HK$", "").replace("$", "").strip()
    try:
        amount = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    if amount <= 0:
        return None
    normalized = amount.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def iso_date_from_dmy(value: Any) -> Optional[str]:
    text = compact_text(value)
    if text is None:
        return None
    match = re.search(r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b", text)
    if not match:
        return None
    day, month, year = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def year_from_exercise(value: Any) -> Optional[str]:
    text = compact_text(value)
    if text is None:
        return None
    match = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    return match.group(1) if match else None


def year_from_date(value: Optional[str]) -> Optional[str]:
    if value and re.match(r"^\d{4}-", value):
        return value[:4]
    return None


HONORIFIC_RE = re.compile(
    r"^(Prof(?:essor)?|Dr|Mr|Mrs|Ms|Miss|Ir)\.?\s+",
    flags=re.IGNORECASE,
)


def split_person_name(raw_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = compact_text(raw_name)
    if text is None:
        return None, None
    text = HONORIFIC_RE.sub("", text).strip()
    suffixes = {"Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}
    if "," in text:
        family, given = [part.strip() for part in text.split(",", 1)]
        return given or None, family or None
    tokens = [token.strip(",") for token in text.split() if token.strip(",")]
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_people(value: Optional[str]) -> list[str]:
    text = clean_text(value)
    if text is None:
        return []
    parts = []
    for line in re.split(r"\n+|;\s*", text):
        item = compact_text(line)
        if item and item not in parts:
            parts.append(item)
    return parts


def person_structs(names: list[str]) -> list[dict[str, Optional[str]]]:
    people = []
    for name in names:
        given, family = split_person_name(name)
        people.append({
            "raw_name": name,
            "given_name": given,
            "family_name": family,
        })
    return people


def co_investigator_struct_json(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    try:
        names = json.loads(str(value))
    except (TypeError, ValueError):
        names = split_people(str(value))
    if not names:
        return None
    return json_dumps(person_structs([str(name) for name in names if compact_text(name)]))


def request_with_retries(
    method: str,
    url: str,
    *,
    data: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
    timeout: int = 45,
    max_attempts: int = 4,
) -> str:
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            if method == "POST":
                resp = session.post(url, data=data, timeout=timeout)
            else:
                resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code} for {resp.url}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"{method} failed after {max_attempts} attempts: {url}") from last_exc


def search_payload(page: int) -> dict[str, str]:
    return {
        "mode": "search",
        "subject": "",
        "panel": "",
        "scheme": "",
        "sScheme": "",
        "sStatus": "",
        "proj_id": "",
        "Old_proj_id": "",
        "proj_title": "",
        "isname": "",
        "ioname": "",
        "institution": "",
        "Year": "",
        "fromAwardYear": "",
        "toAwardYear": "",
        "pages": str(page),
    }


def listing_cache_path(cache_dir: Path, page: int) -> Path:
    return cache_dir / "listings" / f"page_{page:04d}.html"


def detail_cache_path(cache_dir: Path, project_id: str) -> Path:
    return cache_dir / "details" / f"{project_id}.html"


def fetch_listing_page(page: int, cache_dir: Path, *, skip_download: bool = False) -> str:
    path = listing_cache_path(cache_dir, page)
    if path.exists():
        return path.read_text(encoding="utf-8", errors="replace")
    if skip_download:
        raise FileNotFoundError(f"Missing cached listing page {page}: {path}")
    html = request_with_retries("POST", SEARCH_URL, data=search_payload(page))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return html


def detail_params(project_id: str) -> dict[str, str]:
    return {
        "proj_id": project_id,
        "old_proj_id": "null",
        "proj_title": "",
        "isname": "",
        "ioname": "",
        "institution": "",
        "subject": "",
        "pages": "1",
        "year": "",
        "theSubmit": project_id,
    }


def detail_url(project_id: str) -> str:
    return f"{DETAIL_URL}?{urlencode(detail_params(project_id))}"


def fetch_detail_page(project_id: str, cache_dir: Path, *, skip_download: bool = False) -> str:
    path = detail_cache_path(cache_dir, project_id)
    if path.exists():
        return path.read_text(encoding="utf-8", errors="replace")
    if skip_download:
        raise FileNotFoundError(f"Missing cached detail page for {project_id}: {path}")
    html = request_with_retries("GET", DETAIL_URL, params=detail_params(project_id))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return html


def parse_total(html: str) -> tuple[int, int]:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    records_match = re.search(r"Number of records found\s*:\s*([0-9,]+)", text)
    if not records_match:
        raise RuntimeError("Could not find RGC result count in listing page.")
    total_records = int(records_match.group(1).replace(",", ""))
    total_pages = math.ceil(total_records / PAGE_SIZE)
    return total_records, total_pages


def parse_listing_page(html: str, page: int) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Optional[str]]] = []
    for form in soup.find_all("form", {"name": "theForm"}):
        project_id = None
        old_project_id = None
        hidden_year = None
        for field in form.find_all("input"):
            name = field.get("name")
            if name == "proj_id":
                project_id = compact_text(field.get("value"))
            elif name == "old_proj_id":
                old_project_id = compact_text(field.get("value"))
            elif name == "year":
                hidden_year = compact_text(field.get("value"))
        if not project_id:
            continue
        text_lines = [compact_text(line) for line in form.get_text("\n", strip=True).split("\n")]
        text_lines = [line for line in text_lines if line]
        rows.append({
            "project_id": project_id,
            "old_project_id": None if old_project_id == "null" else old_project_id,
            "listing_year": hidden_year,
            "listing_page": str(page),
            "listing_title": text_lines[0] if len(text_lines) >= 1 else None,
            "listing_pi": text_lines[1] if len(text_lines) >= 2 else None,
            "listing_status": text_lines[2] if len(text_lines) >= 3 else None,
        })
    return rows


def parse_detail_fields(html: str) -> dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    fields: dict[str, Optional[str]] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 2:
            continue
        key = compact_text(cells[0].get_text(" ", strip=True))
        if not key or ":" not in key:
            continue
        key = key.replace(" :", ":").rstrip(":").strip()
        value = clean_text(cells[1].get_text("\n", strip=True))
        fields[key] = value
    return fields


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def parse_detail_record(listing: dict[str, Optional[str]], html: str) -> dict[str, Optional[str]]:
    fields = parse_detail_fields(html)
    project_id = fields.get("Project Number") or listing["project_id"]
    if not project_id:
        raise RuntimeError(f"Detail page missing project number: {listing}")

    principal_investigator = fields.get("Principal Investigator(English)")
    pi_given, pi_family = split_person_name(principal_investigator)
    co_investigators = split_people(fields.get("Co - Investigator(s)"))
    co_investigator_people = person_structs(co_investigators)
    completion_date = iso_date_from_dmy(fields.get("Completion Date"))
    start_year = year_from_exercise(fields.get("Exercise Year"))
    amount = amount_to_string(fields.get("Fund Approved"))
    funding_scheme = compact_text(fields.get("Funding Scheme"))

    row = {
        "funder_id": str(FUNDER_ID),
        "funder_display_name": FUNDER_DISPLAY_NAME,
        "funder_award_id": compact_text(project_id),
        "old_project_id": listing.get("old_project_id"),
        "funding_scheme": funding_scheme,
        "funding_type_hint": "fellowship" if funding_scheme and "fellowship" in funding_scheme.lower() else "research",
        "title": clean_text(fields.get("Project Title(English)")),
        "title_chinese": clean_text(fields.get("Project Title(Chinese)")),
        "description": clean_text(fields.get("Abstract as per original application (English/Chinese)")),
        "principal_investigator": principal_investigator,
        "pi_given_name": pi_given,
        "pi_family_name": pi_family,
        "department": compact_text(fields.get("Department")),
        "institution": compact_text(fields.get("Institution")),
        "co_investigators_json": json_dumps(co_investigators) if co_investigators else None,
        "co_investigators_struct_json": json_dumps(co_investigator_people) if co_investigator_people else None,
        "panel": compact_text(fields.get("Panel")),
        "subject_area": compact_text(fields.get("Subject Area")),
        "exercise_year": compact_text(fields.get("Exercise Year")),
        "amount": amount,
        "currency": CURRENCY if amount else None,
        "project_status": compact_text(fields.get("Project Status")),
        "completion_date": completion_date,
        "start_year": start_year,
        "end_year": year_from_date(completion_date),
        "layman_summary_status": clean_text(fields.get("Layman's Summary of Completion Report")),
        "listing_title": listing.get("listing_title"),
        "listing_pi": listing.get("listing_pi"),
        "listing_status": listing.get("listing_status"),
        "listing_page": listing.get("listing_page"),
        "source_url": detail_url(str(project_id)),
        "source_search_url": SEARCH_URL,
        "source_row_json": json_dumps(fields),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return row


def fetch_listing_records(cache_dir: Path, limit: Optional[int], workers: int, skip_download: bool) -> list[dict[str, Optional[str]]]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch RGC listing pages")
    print("=" * 60)

    first_html = fetch_listing_page(1, cache_dir, skip_download=skip_download)
    total_records, total_pages = parse_total(first_html)
    target_pages = total_pages if limit is None else min(total_pages, math.ceil(limit / PAGE_SIZE))
    print(f"  Source reports {total_records:,} project records across {total_pages:,} pages.")
    print(f"  Fetching {target_pages:,} listing pages with {workers} workers.")

    pages = list(range(1, target_pages + 1))
    html_by_page: dict[int, str] = {1: first_html}
    remaining = [page for page in pages if page != 1]
    if remaining:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_listing_page, page, cache_dir, skip_download=skip_download): page for page in remaining}
            for done, future in enumerate(as_completed(futures), 1):
                page = futures[future]
                html_by_page[page] = future.result()
                if done % 100 == 0 or done == len(futures):
                    print(f"  Fetched listing pages: {done:,}/{len(futures):,}")

    listings: list[dict[str, Optional[str]]] = []
    for page in pages:
        listings.extend(parse_listing_page(html_by_page[page], page))
    if limit is not None:
        listings = listings[:limit]
    if not listings:
        raise RuntimeError("No RGC listing records parsed.")

    print(f"  Parsed {len(listings):,} listing records.")
    return listings


def fetch_detail_records(
    listings: list[dict[str, Optional[str]]],
    cache_dir: Path,
    workers: int,
    skip_download: bool,
) -> list[dict[str, Optional[str]]]:
    print("\n" + "=" * 60)
    print("Step 2: Fetch RGC project detail pages")
    print("=" * 60)
    print(f"  Fetching {len(listings):,} detail pages with {workers} workers.")

    def load_one(listing: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
        project_id = listing.get("project_id")
        if not project_id:
            raise RuntimeError(f"Listing missing project_id: {listing}")
        html = fetch_detail_page(project_id, cache_dir, skip_download=skip_download)
        return parse_detail_record(listing, html)

    rows: list[dict[str, Optional[str]]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load_one, listing): listing for listing in listings}
        for done, future in enumerate(as_completed(futures), 1):
            rows.append(future.result())
            if done % 250 == 0 or done == len(futures):
                print(f"  Parsed detail pages: {done:,}/{len(futures):,}")

    rows.sort(key=lambda row: (row.get("start_year") or "", row.get("funder_award_id") or ""))
    return rows


def write_raw_cache(cache_dir: Path, rows: list[dict[str, Optional[str]]]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    raw_path = cache_dir / "rgc_hk_projects_raw.jsonl"
    with raw_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json_dumps(row) + "\n")
    manifest = {
        "source": SOURCE_PAGE_URL,
        "search_url": SEARCH_URL,
        "detail_url": DETAIL_URL,
        "funder_id": FUNDER_ID,
        "provenance": PROVENANCE,
        "rows": len(rows),
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    (cache_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"  Cached parsed rows to {raw_path}")


def load_raw_cache(cache_dir: Path, limit: Optional[int]) -> list[dict[str, Optional[str]]]:
    raw_path = cache_dir / "rgc_hk_projects_raw.jsonl"
    if raw_path.exists():
        rows = [json.loads(line) for line in raw_path.read_text(encoding="utf-8").split("\n") if line.strip()]
        if limit is not None:
            rows = rows[:limit]
        print(f"  Loaded {len(rows):,} cached parsed rows from {raw_path}")
        return rows
    print("  No parsed JSONL cache found; rebuilding from cached HTML.")
    listings = fetch_listing_records(cache_dir, limit, workers=1, skip_download=True)
    rows = fetch_detail_records(listings, cache_dir, workers=1, skip_download=True)
    write_raw_cache(cache_dir, rows)
    return rows


def normalize(rows: list[dict[str, Optional[str]]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 3: Normalize and validate")
    print("=" * 60)
    df = pd.DataFrame(rows)
    expected_columns = [
        "funder_id", "funder_display_name", "funder_award_id", "old_project_id",
        "funding_scheme", "funding_type_hint", "title", "title_chinese", "description",
        "principal_investigator", "pi_given_name", "pi_family_name", "department",
        "institution", "co_investigators_json", "panel", "subject_area", "exercise_year",
        "co_investigators_struct_json", "amount", "currency", "project_status",
        "completion_date", "start_year", "end_year", "layman_summary_status",
        "listing_title", "listing_pi", "listing_status", "listing_page",
        "source_url", "source_search_url", "source_row_json", "downloaded_at",
    ]
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None
    df["co_investigators_struct_json"] = df["co_investigators_struct_json"].combine_first(
        df["co_investigators_json"].apply(co_investigator_struct_json)
    )
    df = df[expected_columns]
    df = df.where(pd.notna(df), None)

    if df["funder_award_id"].isna().any():
        raise RuntimeError("Some rows are missing funder_award_id / project number.")
    dupes = int(df["funder_award_id"].duplicated().sum())
    if dupes:
        duped = df.loc[df["funder_award_id"].duplicated(), "funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate RGC project numbers: {dupes:,}; examples={duped}")
    if full_run and len(df) < 20_000:
        raise RuntimeError(f"Expected at least 20,000 RGC project rows in a full run; got {len(df):,}")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    start_year_numeric = pd.to_numeric(df["start_year"], errors="coerce")
    print(f"  Rows: {len(df):,}")
    print(f"  Unique funder_award_id: {df['funder_award_id'].nunique():,}")
    print(f"  Year range: {int(start_year_numeric.min())} - {int(start_year_numeric.max())}")
    print(f"  Title coverage: {df['title'].notna().mean() * 100:.1f}%")
    print(f"  Description coverage: {df['description'].notna().mean() * 100:.1f}%")
    print(f"  PI coverage: {df['principal_investigator'].notna().mean() * 100:.1f}%")
    print(f"  Institution coverage: {df['institution'].notna().mean() * 100:.1f}%")
    print(f"  Amount coverage: {amount_numeric.notna().mean() * 100:.1f}%")
    print(f"  Total HKD approved amount: {amount_numeric.sum():,.2f}")
    print(f"  Completion-date coverage: {df['completion_date'].notna().mean() * 100:.1f}%")
    print(f"  Funding schemes: {df['funding_scheme'].value_counts().head(10).to_dict()}")
    print(f"  Status distribution: {df['project_status'].value_counts().to_dict()}")

    return df.astype("string")


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "rgc_hk_projects.parquet"
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

    prev_path = output_dir / "_prev_rgc_hk_projects.parquet"
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
        print(f"\n[ERROR] Refusing to shrink RGC HK corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
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
        description="Download Hong Kong RGC project records and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/rgc_hk"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent workers for listing/detail requests")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached RGC HTML/JSONL")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    if args.workers < 1:
        raise ValueError("--workers must be >= 1")

    print("=" * 60)
    print("Hong Kong RGC projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {SOURCE_PAGE_URL}")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  Cache dir:  {args.cache_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached RGC rows/pages")
        rows = load_raw_cache(args.cache_dir, args.limit)
    else:
        listings = fetch_listing_records(args.cache_dir, args.limit, args.workers, skip_download=False)
        rows = fetch_detail_records(listings, args.cache_dir, args.workers, skip_download=False)
        write_raw_cache(args.cache_dir, rows)

    df = normalize(rows, full_run=args.limit is None)
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
