#!/usr/bin/env python3
"""
Swedish Collegium for Advanced Study former fellows -> S3 Data Pipeline
======================================================================

Downloads the official SCAS "All Former Fellows" page and writes a parquet
file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: the Swedish Collegium for Advanced Study publishes
an all-former-fellows page with names, affiliations, and fellowship terms:

    https://www.swedishcollegium.se/fellows/former-fellows/all-former-fellows

This parser includes the source sections "Former Fellows-in-Residence" and
"Former Short-Term Researchers". It intentionally excludes "Former Associated
Researchers" because the page does not identify those rows as fellowships or
award-style appointments.

Validation on 2026-05-28:
  - 784 award rows after splitting repeat fellowship/researcher terms
  - 100% coverage: funder_award_id, name, fellowship term, display_name,
    landing_page_url
  - 780 rows (99.9%) with affiliation coverage; one row is explicit NULL
    because the official listing has only ", (2003-04)" for Angela Steidele
  - 782 rows (99.7%) with parsed year coverage; two source terms have no
    parseable year ("Short-term visitor" and "first half of May")
  - Amount/currency are NULL by runbook section 6.7 waiver because SCAS does
    not publish per-fellowship monetary amounts on the official listing

Output
------
s3://openalex-ingest/awards/scas/scas_former_fellows.parquet

Usage
-----
    python scas_former_fellows_to_s3.py --skip-upload
    python scas_former_fellows_to_s3.py --limit 10 --skip-upload
    python scas_former_fellows_to_s3.py --skip-download --skip-upload
    python scas_former_fellows_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

# --- Windows UTF-8 compatibility shim (runbook §1.2 #7; no-op on Linux/Databricks) ---
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

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


# =============================================================================
# Configuration
# =============================================================================

SOURCE_URL = "https://www.swedishcollegium.se/fellows/former-fellows/all-former-fellows"
SITE_BASE_URL = "https://www.swedishcollegium.se"

# Awarding body: Swedish Collegium for Advanced Study.
# Verified in OpenAlex as F4320319588 (SE).
FUNDER_ID = 4320319588
FUNDER_DISPLAY_NAME = "Swedish Collegium for Advanced Study"
PROVENANCE = "scas_former_fellows"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/scas/scas_former_fellows.parquet"

USER_AGENT = "openalex-walden-scas-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

RAW_HTML_FILENAME = "scas_all_former_fellows.html"
RAW_JSON_FILENAME = "scas_former_fellows_raw.json"
PARQUET_FILENAME = "scas_former_fellows.parquet"

MONTH_WORDS = (
    "jan",
    "january",
    "feb",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "aug",
    "august",
    "sept",
    "september",
    "oct",
    "october",
    "nov",
    "november",
    "dec",
    "december",
)
TERM_WORDS = ("spring", "autumn", "fall", "winter", "summer", *MONTH_WORDS)
YEAR_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
YEAR_RANGE_RE = re.compile(r"(?<!\d)(\d{4})\s*[-\u2010-\u2015]\s*(\d{4}|\d{2})(?!\d)")

_last_request_t = 0.0


@dataclass
class SourceRecord:
    source_record_id: str
    source_section: str
    section_label: str
    scheme_label: str
    funding_type: str
    name: str
    affiliation: Optional[str]
    term: str
    term_sequence: int
    profile_url: Optional[str]
    source_url: str
    source_page_title: Optional[str]
    source_page_year_range: Optional[str]
    retrieved_at: str
    source_html_sha256: str


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def normalize_dash(text: str) -> str:
    return re.sub(r"[\u2010-\u2015]", "-", text)


def slugify(value: str, *, max_len: int = 48) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return (slug[:max_len].strip("-") or "unknown")


def split_name(name: str) -> tuple[Optional[str], Optional[str]]:
    parts = [p for p in clean_text(name).split(" ") if p] if clean_text(name) else []
    _SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while parts and parts[-1].lower().strip(",.") in _SUFFIXES:
        parts.pop()
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return " ".join(parts[:-1]), parts[-1]


def request_get(url: str, *, timeout: int = 90, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": SITE_BASE_URL,
    }
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                log(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            log(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def download_source_html() -> str:
    log("\n" + "=" * 60)
    log("Step 1: Download official SCAS all-former-fellows page")
    log("=" * 60)
    resp = request_get(SOURCE_URL)
    log(f"  [OK] downloaded {len(resp.text):,} characters from {resp.url}")
    return resp.text


def get_main_soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup.body
    if main is None:
        raise RuntimeError("Could not find page body/main in SCAS source HTML")
    return main


def source_lines(main: BeautifulSoup) -> list[str]:
    return [line.strip() for line in main.get_text("\n", strip=True).splitlines() if line.strip()]


def link_map_by_name(main: BeautifulSoup) -> dict[str, str]:
    links: dict[str, str] = {}
    for a in main.find_all("a", href=True):
        label = clean_text(a.get_text(" ", strip=True))
        if not label:
            continue
        label = label.rstrip(" ,")
        if not label:
            continue
        href = urljoin(SOURCE_URL, a["href"])
        links.setdefault(label, href)
    return links


def page_title_and_year_range(lines: list[str]) -> tuple[Optional[str], Optional[str]]:
    title = lines[0] if lines else None
    year_range = None
    for line in lines[:10]:
        if re.search(r"\b\d{4}\s*-\s*\d{4}\b", line):
            year_range = clean_text(line)
            break
    return title, year_range


def section_from_heading(line: str) -> Optional[str]:
    lower = line.lower()
    if lower.startswith("former fellows-in-residence"):
        return "fellow_in_residence"
    if lower.startswith("former short-term researchers"):
        return "short_term_researcher"
    if lower.startswith("former associated researchers"):
        return "stop"
    return None


def should_skip_line(line: str, *, in_record: bool) -> bool:
    lower = line.lower()
    if lower.startswith("see the bottom of the page"):
        return True
    if lower.startswith("former short-term researchers/"):
        return True
    if "visiting fellowship programme" in lower and not re.search(r"\d{4}", lower):
        return True
    if not in_record and len(line) == 1 and line.isalpha():
        return True
    return False


def final_parenthetical(text: str) -> Optional[str]:
    match = re.search(r"\(([^()]*)\)\s*$", text)
    if not match:
        return None
    term = clean_text(match.group(1))
    if not term:
        return None
    lower = term.lower()
    if YEAR_RE.search(term) or any(word in lower for word in TERM_WORDS):
        return term
    return None


def strip_final_term(text: str) -> str:
    return re.sub(r"\s*\([^()]*\)\s*$", "", text).strip()


def split_terms(term_text: str) -> list[str]:
    def add_term(output: list[str], value: str) -> None:
        value = value.strip()
        if not value:
            return
        and_parts = [p.strip() for p in re.split(r"\s+and\s+", value, flags=re.IGNORECASE) if p.strip()]
        if (
            len(and_parts) == 2
            and all(YEAR_RE.search(p) or any(word in p.lower() for word in TERM_WORDS) for p in and_parts)
        ):
            output.extend(and_parts)
        else:
            output.append(value)

    terms: list[str] = []
    for semi_part in re.split(r"\s*;\s*", term_text):
        semi_part = semi_part.strip()
        if not semi_part:
            continue
        comma_parts = [p.strip() for p in semi_part.split(",") if p.strip()]
        if len(comma_parts) <= 1:
            add_term(terms, semi_part)
            continue

        i = 0
        while i < len(comma_parts):
            part = comma_parts[i]
            if (
                i + 1 < len(comma_parts)
                and re.fullmatch(r"\d{4}", comma_parts[i + 1])
                and not YEAR_RE.search(part)
            ):
                add_term(terms, f"{part}, {comma_parts[i + 1]}")
                i += 2
                continue
            if (
                i + 1 < len(comma_parts)
                and not YEAR_RE.search(part)
                and any(word in part.lower() for word in TERM_WORDS)
                and YEAR_RE.search(comma_parts[i + 1])
            ):
                add_term(terms, f"{part} {YEAR_RE.search(comma_parts[i + 1]).group(1)}")
                i += 1
                continue
            add_term(terms, part)
            i += 1
    return terms


def parsed_years(term: str) -> tuple[Optional[int], Optional[int]]:
    normalized = normalize_dash(term)
    range_match = YEAR_RANGE_RE.search(normalized)
    if range_match:
        start_year = int(range_match.group(1))
        end_raw = range_match.group(2)
        if len(end_raw) == 2:
            end_year = int(str(start_year)[:2] + end_raw)
            if end_year < start_year:
                end_year += 100
        else:
            end_year = int(end_raw)
        other_years = [int(y) for y in YEAR_RE.findall(normalized)]
        return min([start_year, *other_years]), max([end_year, *other_years])

    years = [int(y) for y in YEAR_RE.findall(normalized)]
    if years:
        return min(years), max(years)
    return None, None


def date_bounds(term: str) -> tuple[Optional[str], Optional[str]]:
    start_year, end_year = parsed_years(term)
    if start_year is None or end_year is None:
        return None, None
    return f"{start_year:04d}-01-01", f"{end_year:04d}-12-31"


def parse_source_records(html: str) -> list[SourceRecord]:
    main = get_main_soup(html)
    lines = source_lines(main)
    title, year_range = page_title_and_year_range(lines)
    profile_links = link_map_by_name(main)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    source_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

    records: list[SourceRecord] = []
    section: Optional[str] = None
    buf: list[str] = []

    def flush_buffer() -> None:
        nonlocal buf
        if not buf or section not in {"fellow_in_residence", "short_term_researcher"}:
            buf = []
            return

        joined = clean_text(" ".join(buf))
        if not joined:
            buf = []
            return
        term_text = final_parenthetical(joined)
        if not term_text:
            raise RuntimeError(f"Could not find final fellowship term in record buffer: {buf!r}")

        name = clean_text(buf[0].rstrip(" ,"))
        if not name:
            raise RuntimeError(f"Could not parse fellow name from record buffer: {buf!r}")

        affiliation_text = clean_text(" ".join(buf[1:]))
        affiliation = clean_text(strip_final_term(affiliation_text or "").lstrip(" ,"))
        terms = split_terms(term_text)
        if not terms:
            raise RuntimeError(f"No terms split from source term {term_text!r} for {name!r}")

        for idx, term in enumerate(terms, start=1):
            term = clean_text(term)
            if not term:
                continue
            section_label = (
                "Former Fellows-in-Residence"
                if section == "fellow_in_residence"
                else "Former Short-Term Researchers"
            )
            scheme_label = (
                "SCAS Fellow-in-Residence"
                if section == "fellow_in_residence"
                else "SCAS Short-Term Researcher"
            )
            seed = "|".join([section, name, affiliation or "", term, str(idx)])
            source_record_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
            records.append(SourceRecord(
                source_record_id=source_record_id,
                source_section=section,
                section_label=section_label,
                scheme_label=scheme_label,
                funding_type="fellowship" if section == "fellow_in_residence" else "research",
                name=name,
                affiliation=affiliation,
                term=term,
                term_sequence=idx,
                profile_url=profile_links.get(name),
                source_url=SOURCE_URL,
                source_page_title=title,
                source_page_year_range=year_range,
                retrieved_at=retrieved_at,
                source_html_sha256=source_hash,
            ))

        buf = []

    for line in lines:
        heading = section_from_heading(line)
        if heading:
            if buf:
                flush_buffer()
            if heading == "stop":
                section = None
                break
            section = heading
            continue
        if section is None:
            continue
        if should_skip_line(line, in_record=bool(buf)):
            continue

        buf.append(line)
        joined = clean_text(" ".join(buf)) or ""
        if final_parenthetical(joined):
            flush_buffer()

    if buf:
        flush_buffer()

    if not records:
        raise RuntimeError("No SCAS former fellow records parsed from official page")
    return records


def funder_award_id(record: SourceRecord) -> str:
    start_year, end_year = parsed_years(record.term)
    year_part = f"{start_year or 'na'}-{end_year or 'na'}"
    name_part = slugify(record.name)
    term_part = slugify(record.term, max_len=36)
    return f"scas-{record.source_section}-{year_part}-{name_part}-{term_part}-{record.source_record_id}"


def display_name(record: SourceRecord) -> str:
    return f"{record.scheme_label} {record.term} - {record.name}"


def description(record: SourceRecord) -> str:
    if record.affiliation:
        return (
            f"{record.name} is listed by the Swedish Collegium for Advanced Study "
            f"as {record.scheme_label} for {record.term}; source affiliation: "
            f"{record.affiliation}."
        )
    return (
        f"{record.name} is listed by the Swedish Collegium for Advanced Study "
        f"as {record.scheme_label} for {record.term}."
    )


def normalize_records(records: list[SourceRecord], *, full_run: bool) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: Normalize records")
    log("=" * 60)

    rows: list[dict[str, Any]] = []
    for record in records:
        given_name, family_name = split_name(record.name)
        start_year, end_year = parsed_years(record.term)
        start_date, end_date = date_bounds(record.term)
        landing_page_url = record.profile_url or record.source_url
        rows.append({
            "funder_award_id": funder_award_id(record),
            "source_record_id": record.source_record_id,
            "source_section": record.source_section,
            "section_label": record.section_label,
            "scheme_label": record.scheme_label,
            "funding_type": record.funding_type,
            "display_name": display_name(record),
            "description": description(record),
            "name": record.name,
            "given_name": given_name,
            "family_name": family_name,
            "affiliation": record.affiliation,
            "term": record.term,
            "term_sequence": str(record.term_sequence),
            "start_year": str(start_year) if start_year is not None else None,
            "end_year": str(end_year) if end_year is not None else None,
            "start_date": start_date,
            "end_date": end_date,
            "amount": None,
            "currency": None,
            "profile_url": record.profile_url,
            "landing_page_url": landing_page_url,
            "source_url": record.source_url,
            "source_page_title": record.source_page_title,
            "source_page_year_range": record.source_page_year_range,
            "source_html_sha256": record.source_html_sha256,
            "retrieved_at": record.retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    coverage_cols = [
        "funder_award_id",
        "display_name",
        "name",
        "term",
        "scheme_label",
        "landing_page_url",
    ]
    for col in coverage_cols:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:18s}: {covered:,}/{total:,} ({pct:.1f}%)")
        if covered != total:
            raise RuntimeError(f"Unexpected NULL values in required source column {col}")

    affiliation_covered = int(df["affiliation"].notna().sum())
    log(f"  {'affiliation':18s}: {affiliation_covered:,}/{total:,} ({affiliation_covered / total * 100 if total else 0:.1f}%)")
    if affiliation_covered / total < 0.99:
        raise RuntimeError("Unexpectedly low affiliation coverage in SCAS listing")

    year_covered = int(df["start_year"].notna().sum())
    log(f"  {'start_year':18s}: {year_covered:,}/{total:,} ({year_covered / total * 100 if total else 0:.1f}%)")
    log(f"  {'profile_url':18s}: {int(df['profile_url'].notna().sum()):,}/{total:,} ({df['profile_url'].notna().mean() * 100 if total else 0:.1f}%)")
    log(f"  {'amount':18s}: {int(df['amount'].notna().sum()):,}/{total:,} (0.0%; waived because source publishes no per-fellowship amounts)")

    if year_covered / total < 0.99:
        raise RuntimeError("Unexpectedly low year coverage in SCAS terms")
    if full_run and total < 750:
        raise RuntimeError(f"Full SCAS run returned only {total:,} rows; expected at least 750")

    start_years = pd.to_numeric(df["start_year"], errors="coerce")
    end_years = pd.to_numeric(df["end_year"], errors="coerce")
    log(f"  [OK] unique funder_award_id values: {df['funder_award_id'].nunique():,}")
    log(f"  Year range: {int(start_years.min())} - {int(end_years.max())}")
    log(f"  Scheme distribution: {df['scheme_label'].value_counts(dropna=False).to_dict()}")
    log(f"  Section distribution: {df['source_section'].value_counts(dropna=False).to_dict()}")
    no_year_terms = df[df["start_year"].isna()][["name", "term"]].to_dict("records")
    if no_year_terms:
        log(f"  [INFO] terms without parseable year: {no_year_terms}")


def records_to_json(records: list[SourceRecord]) -> list[dict[str, Any]]:
    return [asdict(record) for record in records]


def write_outputs(records: list[SourceRecord], html: Optional[str], df: pd.DataFrame, output_dir: Path) -> Path:
    log("\n" + "=" * 60)
    log("Step 3: Write raw files and parquet")
    log("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    if html is not None:
        html_path = output_dir / RAW_HTML_FILENAME
        html_path.write_text(html, encoding="utf-8")
        log(f"  [OK] wrote source HTML cache to {html_path}")

    raw_json_path = output_dir / RAW_JSON_FILENAME
    raw_json_path.write_text(json.dumps(records_to_json(records), ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"  [OK] wrote normalized source records to {raw_json_path}")

    parquet_path = output_dir / PARQUET_FILENAME
    parquet_df = df.astype("string")
    parquet_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    log(f"  [OK] wrote {len(parquet_df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_records(output_dir: Path) -> list[SourceRecord]:
    raw_json_path = output_dir / RAW_JSON_FILENAME
    if not raw_json_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_json_path}")
    payload = json.loads(raw_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_json_path}")
    records = [SourceRecord(**item) for item in payload]
    log(f"  [OK] loaded {len(records):,} cached source records from {raw_json_path}")
    return records


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
    log(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            log("    no existing parquet: first ingest, no shrink check needed.")
            return True
        log(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_scas_former_fellows.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        log(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            log("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        log(
            f"\n[ERROR] Refusing to shrink SCAS former fellows corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
        return False
    log("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    log("\n" + "=" * 60)
    log("Step 4: Upload to S3")
    log("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        log(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        log("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        log(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download SCAS former fellows and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/scas_former_fellows"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached source records from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    log("=" * 60)
    log("SCAS former fellows -> S3 pipeline")
    log("=" * 60)
    log(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  Source:     {SOURCE_URL}")
    log(f"  Output dir: {args.output_dir.absolute()}")
    log(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    log(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    html: Optional[str] = None
    if args.skip_download:
        log("\nStep 1: Reuse cached source records")
        records = load_cached_records(args.output_dir)
    else:
        html = download_source_html()
        records = parse_source_records(html)
        log(f"  [OK] parsed {len(records):,} source award rows")

    if args.limit:
        records = records[:args.limit]
        log(f"  [LIMIT] keeping first {len(records):,} source award rows")

    df = normalize_records(records, full_run=args.limit is None)
    parquet_path = write_outputs(records, html, df, args.output_dir)

    if args.skip_upload:
        log("\n[SKIP] --skip-upload set; not uploading to S3.")
        log(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
