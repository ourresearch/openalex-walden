#!/usr/bin/env python3
"""
Marsden Fund Awarded Grants -> S3 Pipeline
==========================================

Downloads awarded-grant records from Royal Society Te Apārangi's official
Marsden Fund awarded grants pages and workbooks.

Source authority
----------------
Royal Society Te Apārangi publishes the Marsden Fund awarded-grants index:

    https://www.royalsociety.org.nz/what-we-do/funds-and-opportunities/marsden/awarded-grants

The index links an official 2008-2017 workbook and annual official award
workbooks for later years. The workbooks carry real Marsden project IDs,
project titles/abstracts, investigators and roles, organizations, panels,
categories, and funding amounts.

OpenAlex funder
---------------
F4320335369 - Marsden Fund (NZ)
ROR: NULL
DOI: NULL

Output
------
s3://openalex-ingest/awards/marsden/marsden_awards.parquet

Usage
-----
    python scripts/local/marsden_to_s3.py --limit 10 --skip-upload
    python scripts/local/marsden_to_s3.py --skip-upload
    python scripts/local/marsden_to_s3.py --skip-download --skip-upload
    python scripts/local/marsden_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
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


INDEX_URL = "https://www.royalsociety.org.nz/what-we-do/funds-and-opportunities/marsden/awarded-grants"
FUNDER_ID = 4320335369
FUNDER_DISPLAY_NAME = "Marsden Fund"
PROVENANCE = "marsden_awarded_grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/marsden/marsden_awards.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp")
JSON_CACHE_FILENAME = "marsden_awards.json"
PARQUET_FILENAME = "marsden_awards.parquet"

REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5
EXPECTED_MIN_FULL_ROWS = 1900

HEADERS = {
    "User-Agent": "openalex-walden-marsden/1.0 (+https://openalex.org)",
    "Accept": "text/html,application/xhtml+xml,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

PREFIX_TITLES = {
    "dr", "dr.", "prof", "prof.", "professor", "associate professor",
    "assoc professor", "assoc. professor", "assistant professor", "mr", "mr.",
    "mrs", "mrs.", "ms", "ms.", "miss", "sir", "dame",
}
SUFFIXES = {"phd", "dphil", "msc", "ma", "ba", "bsc", "mnzm", "frsnz", "fres", "frs"}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = html.unescape(str(value))
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"[\u00a0\u2028\u2029 \t\r\n]+", " ", text).strip()
    if not text or text.lower() in {"nan", "none", "null", "-"}:
        return None
    return text


def normalize_key(value: Any) -> str:
    text = clean_text(value) or ""
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def column_lookup(columns: list[Any]) -> dict[str, str]:
    return {normalize_key(col): col for col in columns}


def find_column(columns: list[Any], candidates: list[str]) -> str | None:
    lookup = column_lookup(columns)
    for candidate in candidates:
        key = normalize_key(candidate)
        if key in lookup:
            return lookup[key]
    for col in columns:
        col_key = normalize_key(col)
        if any(normalize_key(candidate) in col_key for candidate in candidates):
            return col
    return None


def parse_amount(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)) and float(value) > 0:
        return f"{float(value):.2f}"
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"([0-9][0-9,\s]*(?:\.[0-9]+)?)", text)
    if not match:
        return None
    try:
        parsed = float(match.group(1).replace(",", "").replace(" ", ""))
    except ValueError:
        return None
    return f"{parsed:.2f}" if parsed > 0 else None


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    name = clean_text(full_name)
    if not name:
        return None, None
    name = re.sub(r"\([^)]*\)", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    lowered = name.lower()
    for title in sorted(PREFIX_TITLES, key=len, reverse=True):
        if lowered.startswith(title + " "):
            name = name[len(title):].strip()
            break
    tokens = name.split()
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def fetch_text(session: requests.Session, url: str) -> str:
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


def fetch_binary(session: requests.Session, url: str) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200 and len(response.content) > 1000:
                return response.content
            log(f"  workbook {url}: HTTP {response.status_code}, {len(response.content)} bytes (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            last_exc = exc
            log(f"  workbook {url}: {exc} (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"Failed to fetch workbook {url}: {last_exc}")


def discover_workbooks(session: requests.Session) -> list[dict[str, str]]:
    index_html = fetch_text(session, INDEX_URL)
    index_soup = BeautifulSoup(index_html, "html.parser")

    year_pages: dict[int, str] = {}
    for link in index_soup.select("a[href]"):
        href = urljoin(INDEX_URL, link.get("href"))
        text = clean_text(link.get_text(" ", strip=True)) or ""
        haystack = f"{href} {text}"
        if "search-marsden-awards" in href:
            year_pages[2008] = href
            continue
        if "marsden" not in href.lower():
            continue
        match = re.search(r"(20\d{2})", haystack)
        if match:
            year_pages.setdefault(int(match.group(1)), href)

    workbooks: list[dict[str, str]] = []
    for year in sorted(year_pages):
        page_url = year_pages[year]
        if year not in {2008} and year < 2018:
            continue
        page_html = fetch_text(session, page_url)
        page_soup = BeautifulSoup(page_html, "html.parser")
        xlsx_urls: list[str] = []
        for link in page_soup.select("a[href]"):
            href = urljoin(page_url, link.get("href"))
            if re.search(r"\.xlsx?$", href, flags=re.I) and href not in xlsx_urls:
                xlsx_urls.append(href)
        if not xlsx_urls:
            log(f"  no workbook found for year marker {year}: {page_url}")
            continue
        if year == 2008:
            workbooks.append({"kind": "combined_2008_2017", "year": "2008", "page_url": page_url, "workbook_url": xlsx_urls[0]})
        else:
            workbooks.append({"kind": "annual", "year": str(year), "page_url": page_url, "workbook_url": xlsx_urls[0]})
    return workbooks


def choose_project_sheet(xl: pd.ExcelFile) -> str:
    best: tuple[int, str] | None = None
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet, dtype=str, nrows=5)
        cols = list(df.columns)
        if not find_column(cols, ["Project ID"]):
            continue
        score = 0
        if find_column(cols, ["Project Abstract", "Abstract"]):
            score += 20
        if find_column(cols, ["Funding (ex GST)", "Funding (exc GST)", "Funding (GST excl)"]):
            score += 10
        if find_column(cols, ["Project Title", "Project", "Media Title"]):
            score += 8
        if "team" in sheet.lower():
            score -= 15
        if best is None or score > best[0]:
            best = (score, sheet)
    if best is None:
        raise RuntimeError(f"Could not choose project sheet from {xl.sheet_names}")
    return best[1]


def find_team_sheets(xl: pd.ExcelFile, project_sheet: str) -> list[str]:
    sheets: list[str] = []
    for sheet in xl.sheet_names:
        if sheet == project_sheet:
            continue
        df = pd.read_excel(xl, sheet_name=sheet, dtype=str, nrows=5)
        cols = list(df.columns)
        if find_column(cols, ["Project ID"]) and find_column(cols, ["Investigator"]) and find_column(cols, ["Role"]):
            sheets.append(sheet)
    return sheets


def parse_project_sheet(df: pd.DataFrame, source_year: int, source_page_url: str, workbook_url: str) -> dict[str, dict[str, Any]]:
    cols = list(df.columns)
    project_id_col = find_column(cols, ["Project ID"])
    title_col = find_column(cols, ["Project Title", "Project", "Media Title"])
    media_title_col = find_column(cols, ["Media Title"])
    investigator_col = find_column(cols, ["Contact Investigator", "Investigator"])
    institution_col = find_column(cols, ["Institution", "Organisation", "Organization"])
    panel_col = find_column(cols, ["Panel"])
    category_col = find_column(cols, ["Category", "Catergory"])
    funding_col = find_column(cols, ["Funding (ex GST)", "Funding (exc GST)", "Funding (GST excl)"])
    abstract_col = find_column(cols, ["Project Abstract", "Abstract", "Media Summary"])
    year_col = find_column(cols, ["Year"])

    if not project_id_col or not title_col or not funding_col:
        raise RuntimeError(f"Project sheet missing required columns: {cols}")

    projects: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        project_id = clean_text(row.get(project_id_col))
        if not project_id or not re.match(r"\d{2}-[A-Z0-9]{2,5}-\d{3}", project_id):
            continue
        year = clean_text(row.get(year_col)) if year_col else str(source_year)
        title = clean_text(row.get(title_col))
        media_title = clean_text(row.get(media_title_col)) if media_title_col else None
        abstract = clean_text(row.get(abstract_col)) if abstract_col else None
        amount = parse_amount(row.get(funding_col))
        projects[project_id] = {
            "project_id": project_id,
            "source_year": str(int(float(year))) if year and re.fullmatch(r"\d+(?:\.0)?", year) else str(source_year),
            "display_name": title or media_title,
            "media_title": media_title,
            "description": abstract,
            "contact_investigator_raw": clean_text(row.get(investigator_col)) if investigator_col else None,
            "lead_organization": clean_text(row.get(institution_col)) if institution_col else None,
            "panel": clean_text(row.get(panel_col)) if panel_col else None,
            "category": clean_text(row.get(category_col)) if category_col else None,
            "amount": amount,
            "currency": "NZD" if amount else None,
            "source_page_url": source_page_url,
            "source_workbook_url": workbook_url,
        }
    return projects


def person_struct(name: str | None, affiliation_name: str | None) -> dict[str, Any] | None:
    cleaned = clean_text(name)
    if not cleaned and not clean_text(affiliation_name):
        return None
    given, family = split_name(cleaned)
    return {
        "given_name": given,
        "family_name": family,
        "orcid": None,
        "role_start": None,
        "affiliation": {
            "name": clean_text(affiliation_name),
            "country": None,
            "ids": None,
        },
    }


def parse_team_sheets(xl: pd.ExcelFile, sheets: list[str]) -> dict[str, list[dict[str, str | None]]]:
    teams: dict[str, list[dict[str, str | None]]] = defaultdict(list)
    for sheet in sheets:
        df = pd.read_excel(xl, sheet_name=sheet, dtype=str)
        cols = list(df.columns)
        project_id_col = find_column(cols, ["Project ID"])
        investigator_col = find_column(cols, ["Investigator"])
        role_col = find_column(cols, ["Role"])
        institution_col = find_column(cols, ["Institution", "Organisation", "Organization"])
        if not project_id_col or not investigator_col or not role_col:
            continue
        df[project_id_col] = df[project_id_col].ffill()
        for _, row in df.iterrows():
            project_id = clean_text(row.get(project_id_col))
            investigator = clean_text(row.get(investigator_col))
            if not project_id or not investigator or not re.match(r"\d{2}-[A-Z0-9]{2,5}-\d{3}", project_id):
                continue
            teams[project_id].append(
                {
                    "investigator": investigator,
                    "role": clean_text(row.get(role_col)),
                    "organization": clean_text(row.get(institution_col)) if institution_col else None,
                }
            )
    return teams


def merge_projects_and_teams(projects: dict[str, dict[str, Any]], teams: dict[str, list[dict[str, str | None]]]) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    for project_id, project in sorted(projects.items()):
        team = teams.get(project_id, [])
        pi_team = [member for member in team if (member.get("role") or "").upper() == "PI"]
        ordered = pi_team + [member for member in team if member not in pi_team]
        if not ordered and project.get("contact_investigator_raw"):
            ordered = [{"investigator": project.get("contact_investigator_raw"), "role": "PI", "organization": project.get("lead_organization")}]

        lead = person_struct(ordered[0].get("investigator"), ordered[0].get("organization") or project.get("lead_organization")) if ordered else None
        co_lead = person_struct(ordered[1].get("investigator"), ordered[1].get("organization") or project.get("lead_organization")) if len(ordered) > 1 and (ordered[1].get("role") or "").upper() == "PI" else None
        rest_start = 2 if co_lead else 1
        investigators = [
            person_struct(member.get("investigator"), member.get("organization") or project.get("lead_organization"))
            for member in ordered[rest_start:]
        ]
        investigators = [person for person in investigators if person is not None]

        panel = project.get("panel")
        category = project.get("category")
        scheme_parts = [part for part in [panel, category] if part]
        record = {
            "funder_award_id": project_id,
            "project_id": project_id,
            "display_name": clean_text(project.get("display_name")),
            "description": clean_text(project.get("description")),
            "media_title": clean_text(project.get("media_title")),
            "amount": project.get("amount"),
            "currency": project.get("currency"),
            "funder_scheme": " - ".join(scheme_parts) if scheme_parts else panel,
            "panel": panel,
            "category": category,
            "funding_type": "research",
            "start_year": project.get("source_year"),
            "end_year": None,
            "start_date": None,
            "end_date": None,
            "lead_given_name": lead.get("given_name") if lead else None,
            "lead_family_name": lead.get("family_name") if lead else None,
            "lead_affiliation_name": lead.get("affiliation", {}).get("name") if lead else project.get("lead_organization"),
            "co_lead_given_name": co_lead.get("given_name") if co_lead else None,
            "co_lead_family_name": co_lead.get("family_name") if co_lead else None,
            "co_lead_affiliation_name": co_lead.get("affiliation", {}).get("name") if co_lead else None,
            "investigators_json": json.dumps(investigators, ensure_ascii=False) if investigators else None,
            "team_json": json.dumps(team, ensure_ascii=False) if team else None,
            "landing_page_url": project.get("source_page_url"),
            "source_page_url": project.get("source_page_url"),
            "source_workbook_url": project.get("source_workbook_url"),
            "provenance": PROVENANCE,
            "funder_id": str(FUNDER_ID),
            "funder_display_name": FUNDER_DISPLAY_NAME,
        }
        records.append(record)
    return records


def parse_workbook(content: bytes, meta: dict[str, str]) -> list[dict[str, str | None]]:
    xl = pd.ExcelFile(BytesIO(content))
    project_sheet = "Marsden announcements 2008-2017" if meta["kind"] == "combined_2008_2017" else choose_project_sheet(xl)
    project_df = pd.read_excel(xl, sheet_name=project_sheet, dtype=str)
    projects = parse_project_sheet(project_df, int(meta["year"]), meta["page_url"], meta["workbook_url"])
    team_sheets = find_team_sheets(xl, project_sheet)
    teams = parse_team_sheets(xl, team_sheets)
    log(f"  {Path(meta['workbook_url']).name}: project sheet '{project_sheet}' -> {len(projects):,} projects; team sheets {team_sheets}")
    return merge_projects_and_teams(projects, teams)


def download_records(limit: int | None = None) -> list[dict[str, str | None]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    workbooks = discover_workbooks(session)
    if not workbooks:
        raise RuntimeError("No Marsden workbooks discovered")
    log(f"Discovered {len(workbooks)} Marsden workbooks")

    by_id: dict[str, dict[str, str | None]] = {}
    for meta in workbooks:
        log(f"Downloading {meta['workbook_url']}")
        content = fetch_binary(session, meta["workbook_url"])
        for record in parse_workbook(content, meta):
            existing = by_id.get(record["funder_award_id"])
            if existing and existing != record:
                log(f"  duplicate project ID {record['funder_award_id']} across workbooks; keeping first occurrence")
                continue
            by_id[record["funder_award_id"]] = record
        if limit is not None and len(by_id) >= limit:
            break

    records = sorted(by_id.values(), key=lambda record: (record.get("start_year") or "", record["funder_award_id"]))
    if limit is not None:
        records = records[:limit]
        log(f"--limit {limit} applied after workbook parsing")
    return records


def validate_records(records: list[dict[str, str | None]], full_run: bool) -> None:
    if not records:
        raise RuntimeError("No Marsden records parsed")
    if full_run and len(records) < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(f"Full Marsden run parsed only {len(records)} rows; expected at least {EXPECTED_MIN_FULL_ROWS}")

    ids = [record["funder_award_id"] for record in records]
    duplicate_count = len(ids) - len(set(ids))
    if duplicate_count:
        raise RuntimeError(f"Duplicate Marsden project IDs found: {duplicate_count}")

    df = pd.DataFrame(records)
    amount_count = df["amount"].notna().sum()
    title_count = df["display_name"].notna().sum()
    lead_count = df["lead_family_name"].notna().sum()
    affiliation_count = df["lead_affiliation_name"].notna().sum()
    abstract_count = df["description"].notna().sum()
    scheme_count = df["funder_scheme"].notna().sum()
    start_year_count = df["start_year"].notna().sum()
    amount_total = pd.to_numeric(df["amount"], errors="coerce").sum()
    start_years = pd.to_numeric(df["start_year"], errors="coerce")

    log("")
    log("Validation summary")
    log(f"  rows: {len(df):,}")
    log(f"  unique funder_award_id: {df['funder_award_id'].nunique():,}")
    log(f"  title coverage: {title_count:,}/{len(df):,} ({title_count / len(df):.1%})")
    log(f"  abstract coverage: {abstract_count:,}/{len(df):,} ({abstract_count / len(df):.1%})")
    log(f"  lead family coverage: {lead_count:,}/{len(df):,} ({lead_count / len(df):.1%})")
    log(f"  lead affiliation coverage: {affiliation_count:,}/{len(df):,} ({affiliation_count / len(df):.1%})")
    log(f"  amount coverage: {amount_count:,}/{len(df):,} ({amount_count / len(df):.1%}), total NZD {amount_total:,.0f}")
    log(f"  scheme coverage: {scheme_count:,}/{len(df):,} ({scheme_count / len(df):.1%})")
    log(f"  start_year coverage: {start_year_count:,}/{len(df):,} ({start_year_count / len(df):.1%})")
    log(f"  start_year range: {int(start_years.min()) if start_year_count else 'NULL'}-{int(start_years.max()) if start_year_count else 'NULL'}")
    log("")
    log("Top panels")
    for panel, count in df["panel"].fillna("NULL").value_counts(dropna=False).head(12).items():
        log(f"  {panel}: {count:,}")
    log("")
    log("Top categories")
    for category, count in df["category"].fillna("NULL").value_counts(dropna=False).head(12).items():
        log(f"  {category}: {count:,}")


def load_or_download(output_dir: Path, skip_download: bool, limit: int | None) -> list[dict[str, str | None]]:
    cache_path = output_dir / JSON_CACHE_FILENAME
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download requested but cache does not exist: {cache_path}")
        log(f"Loading cached Marsden records from {cache_path}")
        records = json.loads(cache_path.read_text(encoding="utf-8"))
        return records[:limit] if limit is not None else records

    records = download_records(limit=limit)
    cache_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Wrote JSON cache: {cache_path}")
    return records


def write_parquet(records: list[dict[str, str | None]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    enriched = [{**record, "downloaded_at": downloaded_at} for record in records]

    ordered_columns = [
        "funder_award_id",
        "project_id",
        "display_name",
        "description",
        "media_title",
        "amount",
        "currency",
        "funder_scheme",
        "panel",
        "category",
        "funding_type",
        "start_year",
        "end_year",
        "start_date",
        "end_date",
        "lead_given_name",
        "lead_family_name",
        "lead_affiliation_name",
        "co_lead_given_name",
        "co_lead_family_name",
        "co_lead_affiliation_name",
        "investigators_json",
        "team_json",
        "landing_page_url",
        "source_page_url",
        "source_workbook_url",
        "provenance",
        "funder_id",
        "funder_display_name",
        "downloaded_at",
    ]
    df = pd.DataFrame(enriched)
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

    previous_path = output_dir / "_previous_marsden_awards.parquet"
    s3 = boto3.client("s3")
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous_path))
    except Exception as exc:
        log(f"  No previous S3 parquet found or not accessible ({exc}); allowing first upload.")
        return True

    previous_count = len(pd.read_parquet(previous_path))
    log(f"  §1.4 no-shrink check: previous={previous_count:,}, new={new_count:,}")
    if new_count < previous_count:
        log("  [BLOCKED] New Marsden corpus is smaller than previous S3 object.")
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
    parser = argparse.ArgumentParser(description="Download Marsden Fund awards and write/upload parquet.")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for cache/parquet outputs")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSON from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    records = load_or_download(args.output_dir, args.skip_download, args.limit)
    validate_records(records, full_run=args.limit is None)
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
