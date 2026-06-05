#!/usr/bin/env python3
"""
Taiwan MOST GRB projects -> S3 pipeline
=======================================

Downloads legacy Ministry of Science and Technology, Taiwan project records
from the public Government Research Bulletin (GRB) search/export API
and writes a parquet staging file for the OpenAlex awards pipeline.

Source authority
----------------
The source is official. The GRB search page at https://www.grb.gov.tw/search
loads its public backend from https://grbdef.stpi.niar.org.tw. The frontend
uses the same endpoints this script calls:

    POST /searcher                  search project records
    POST /home/planExport           export selected records as XLSX

This ingest is scoped to the legacy MOST plan-organ code only:

    BT100 = 科技部

GRB also contains current National Science and Technology Council (BT200) and
legacy National Science Council (BT00) records. Those map to different
OpenAlex funder entities and are ingested separately if pursued.

Validation on 2026-06-04 source probe:
  - BT100 search returned 177,137 records.
  - Export XLSX includes native grant number, title, PI, co-PI, institution,
    project year, year-month project period, amount in thousand TWD, research
    field/type, abstracts, and official detail URL.
  - The backend supports 200 selected IDs per XLSX export, matching the
    frontend's selection limit.

Output
------
s3://openalex-ingest/awards/taiwan_most_grb/taiwan_most_grb_projects.parquet

Usage
-----
    python taiwan_most_grb_to_s3.py --limit 10 --skip-upload
    python taiwan_most_grb_to_s3.py --skip-upload
    python taiwan_most_grb_to_s3.py --skip-download --skip-upload
    python taiwan_most_grb_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests openpyxl boto3
"""

from __future__ import annotations

import argparse
import builtins
import io
import json
import math
import re
import subprocess
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

warnings.filterwarnings("ignore", message="Workbook contains no default style")

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = builtins.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    builtins.open = _open_utf8
# --- end shim ---


FUNDER_ID = 4320322795
FUNDER_DISPLAY_NAME = "Ministry of Science and Technology, Taiwan"
PROVENANCE = "grb_most_projects"

SEARCH_PAGE_URL = "https://www.grb.gov.tw/search"
API_ROOT = "https://grbdef.stpi.niar.org.tw"
SEARCH_API_URL = f"{API_ROOT}/searcher"
EXPORT_API_URL = f"{API_ROOT}/home/planExport"
PLAN_ORGAN_CODE = "BT100"
PLAN_ORGAN_NAME = "科技部"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/taiwan_most_grb/taiwan_most_grb_projects.parquet"

USER_AGENT = "openalex-walden-most-grb-ingest/1.0 (+https://openalex.org)"
ALLOWED_PAGE_SIZES = (10, 50, 100, 200)
DEFAULT_PAGE_SIZE = 200
EXPECTED_MIN_FULL_ROWS = 120000

SPACE_RE = re.compile(r"\s+")
DELIMITER_RE = re.compile(r"\s*[;；、,，/]\s*")
LATIN_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z .,'-]+$")
SUFFIX_RE = re.compile(
    r"\b(?:Ph\.?D\.?|MD|M\.?D\.?|Dr\.?|Prof\.?|Jr\.?|Sr\.?|II|III|IV)\b\.?",
    re.IGNORECASE,
)

COLUMN_MAP = {
    "系統識別號": "grb_id",
    "系統編號": "system_number",
    "原計畫編號": "original_plan_number",
    "計畫中文名稱": "title_zh",
    "計畫英文名稱": "title_en",
    "執行單位名稱": "executing_institution",
    "計畫年度": "plan_year_roc",
    "計畫主管機關": "plan_organ_name",
    "研究方式": "research_method",
    "研究性質": "research_nature",
    "研究領域": "research_field",
    "本期期間(起)": "period_start_roc_ym",
    "本期期間(訖)": "period_end_roc_ym",
    "本期經費(千元)": "amount_thousand_twd",
    "計畫主持人": "lead_name",
    "共同/協同主持人": "co_lead_names",
    "中文關鍵詞": "keywords_zh",
    "英文關鍵詞": "keywords_en",
    "中文摘要": "abstract_zh",
    "英文摘要": "abstract_en",
    "研究計畫詳目連結": "landing_page_url",
}


def log(message: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = SPACE_RE.sub(" ", text).strip()
    if not text or text.lower() in {"none", "null", "nan", "-"}:
        return None
    return text


def normalize_award_id(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return SPACE_RE.sub("", text).upper()


def current_roc_year() -> int:
    return datetime.now(timezone.utc).year - 1911


def choose_page_size(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_PAGE_SIZE
    for size in ALLOWED_PAGE_SIZES:
        if limit <= size:
            return size
    return DEFAULT_PAGE_SIZE


def roc_year(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 3:
        return None
    year_roc = int(digits[:3])
    year = year_roc + 1911
    if year < 1900 or year > datetime.now(timezone.utc).year + 1:
        return None
    return str(year)


def roc_month(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 5:
        return None
    month = int(digits[3:5])
    if month < 1 or month > 12:
        return None
    return f"{month:02d}"


def amount_twd(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    number = re.sub(r"[^0-9.\-]", "", text)
    if not number:
        return None
    try:
        value_float = float(number) * 1000.0
    except ValueError:
        return None
    if value_float <= 0:
        return None
    if value_float.is_integer():
        return str(int(value_float))
    return f"{value_float:.2f}".rstrip("0").rstrip(".")


def split_latin_name(name: str) -> tuple[Optional[str], Optional[str]]:
    text = SUFFIX_RE.sub("", name)
    text = SPACE_RE.sub(" ", text).strip(" ,")
    if not text:
        return None, None
    parts = text.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def person_from_name(name: Any, institution: Optional[str], role_start_year: Optional[str]) -> Optional[dict[str, Optional[str]]]:
    text = clean_text(name)
    if not text:
        return None
    if LATIN_NAME_RE.match(text) and " " in text:
        given, family = split_latin_name(text)
    else:
        # Most GRB names are Chinese personal names without separators. Do not
        # infer given/family order; preserve the official name as family_name.
        given, family = None, text
    return {
        "given_name": given,
        "family_name": family,
        "orcid": None,
        "role_start_year": role_start_year,
        "affiliation_name": institution,
        "affiliation_country": None,
    }


def split_people(value: Any) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    return [part for part in (clean_text(p) for p in DELIMITER_RE.split(text)) if part]


def json_string(value: Any) -> Optional[str]:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.grb.gov.tw",
        "Referer": SEARCH_PAGE_URL,
    })
    return session


def search_payload(page: int, page_size: int) -> dict[str, str]:
    return {
        "keyword": "",
        "queryType": "GRB05",
        "projNums": "",
        "planYearSt": "82",
        "planYearEn": str(current_roc_year()),
        "excuOrganPrefix": "",
        "planOrgans": PLAN_ORGAN_CODE,
        "nowPage": str(page),
        "rowsPerPage": str(page_size),
        "orderType": "SIMILARITY",
    }


def post_form(session: requests.Session, url: str, data: dict[str, str], retries: int = 4) -> requests.Response:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, data=data, headers=headers, timeout=120)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            return resp
        except Exception as exc:
            last_error = exc
            log(f"  [WARN] POST {url} attempt {attempt}/{retries} failed: {exc}")
            time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed POST {url}") from last_error


def fetch_search_page(session: requests.Session, page: int, page_size: int) -> dict[str, Any]:
    payload = search_payload(page, page_size)
    resp = post_form(session, SEARCH_API_URL, payload)
    log(f"POST search page={page} size={page_size} -> HTTP {resp.status_code}; {len(resp.content):,} bytes")
    return resp.json()


def export_ids(session: requests.Session, ids: list[str], export_path: Path) -> None:
    if not ids:
        raise ValueError("No GRB ids supplied for export.")
    resp = post_form(session, EXPORT_API_URL, {"data": ",".join(ids)})
    if not resp.content.startswith(b"PK"):
        raise RuntimeError(f"GRB export did not return XLSX bytes: {resp.text[:300]}")
    export_path.write_bytes(resp.content)
    log(f"  exported {len(ids):,} ids -> {export_path.name} ({len(resp.content):,} bytes)")


def read_export(export_path: Path, page: int) -> pd.DataFrame:
    df = pd.read_excel(export_path, dtype=str)
    df = df.rename(columns=COLUMN_MAP)
    missing = [col for col in COLUMN_MAP.values() if col not in df.columns]
    if missing:
        raise RuntimeError(f"GRB export {export_path} is missing expected columns: {missing}")
    df["_source_export_file"] = str(export_path)
    df["_search_page"] = str(page)
    return df


def fetch_exports(output_dir: Path, limit: Optional[int]) -> tuple[list[Path], int]:
    print("\n" + "=" * 60)
    print("Step 1: Download GRB search/export pages")
    print("=" * 60)
    export_dir = output_dir / "raw_exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    page_size = choose_page_size(limit)
    session = make_session()
    first = fetch_search_page(session, 1, page_size)
    total = int(first.get("totalRows") or 0)
    if total <= 0:
        raise RuntimeError("GRB search returned zero MOST rows.")
    expected_rows = min(limit, total) if limit else total
    expected_pages = math.ceil(expected_rows / page_size)
    log(f"GRB reports {total:,} BT100/MOST records; fetching {expected_rows:,} rows across {expected_pages:,} pages.")

    exports: list[Path] = []

    def handle_page(page: int, payload: dict[str, Any], remaining: Optional[int]) -> None:
        objects = payload.get("obj") or []
        if not objects and page <= expected_pages:
            log(f"  [WARN] empty search page {page} before expected end; continuing per runbook no-first-empty rule.")
            return
        ids = [str(obj.get("id")) for obj in objects if obj.get("id") is not None]
        if remaining is not None:
            ids = ids[:remaining]
        if not ids:
            return
        export_path = export_dir / f"taiwan_most_grb_page_{page:05d}.xlsx"
        export_ids(session, ids, export_path)
        exports.append(export_path)

    remaining = expected_rows
    handle_page(1, first, remaining)
    remaining -= page_size
    for page in range(2, expected_pages + 1):
        payload = fetch_search_page(session, page, page_size)
        page_remaining = max(0, min(page_size, remaining)) if limit else None
        handle_page(page, payload, page_remaining)
        remaining -= page_size
        if page % 10 == 0 or page == expected_pages:
            log(f"Progress: page {page:,}/{expected_pages:,}, exports {len(exports):,}")
        time.sleep(0.1)

    if not exports:
        raise RuntimeError("No GRB XLSX exports were downloaded.")
    manifest = {
        "source": SEARCH_PAGE_URL,
        "api_root": API_ROOT,
        "plan_organ_code": PLAN_ORGAN_CODE,
        "plan_organ_name": PLAN_ORGAN_NAME,
        "reported_total": total,
        "limit": limit,
        "page_size": page_size,
        "export_count": len(exports),
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }
    (output_dir / "taiwan_most_grb_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    return exports, total


def load_cached_exports(output_dir: Path, limit: Optional[int]) -> tuple[list[Path], int]:
    print("\n" + "=" * 60)
    print("Step 1: Reuse cached GRB XLSX exports")
    print("=" * 60)
    export_dir = output_dir / "raw_exports"
    exports = sorted(export_dir.glob("taiwan_most_grb_page_*.xlsx"))
    if not exports:
        raise FileNotFoundError(f"--skip-download requested but no cached exports found in {export_dir}")
    if limit is not None:
        rows_needed = limit
        page_size = DEFAULT_PAGE_SIZE
        exports = exports[: max(1, math.ceil(rows_needed / page_size))]
    manifest_path = output_dir / "taiwan_most_grb_manifest.json"
    reported_total = 0
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        reported_total = int(manifest.get("reported_total") or 0)
    log(f"Loaded {len(exports):,} cached XLSX exports from {export_dir}")
    return exports, reported_total


def load_exports(exports: list[Path], limit: Optional[int]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for index, export_path in enumerate(exports, start=1):
        frames.append(read_export(export_path, index))
    df = pd.concat(frames, ignore_index=True)
    if limit is not None:
        df = df.head(limit).copy()
    log(f"Parsed {len(df):,} rows from {len(exports):,} GRB XLSX exports.")
    return df


def normalize_dataframe(raw: pd.DataFrame, reported_total: int, limit: Optional[int]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize GRB export rows")
    print("=" * 60)

    rows: list[dict[str, Any]] = []
    retrieved_at = datetime.now(timezone.utc).isoformat()
    for _, row in raw.iterrows():
        original_plan_number = normalize_award_id(row.get("original_plan_number"))
        system_number = normalize_award_id(row.get("system_number"))
        grb_id = normalize_award_id(row.get("grb_id"))
        funder_award_id = original_plan_number or system_number or (f"GRB-{grb_id}" if grb_id else None)
        if not funder_award_id:
            continue

        start_year = roc_year(row.get("period_start_roc_ym")) or roc_year(row.get("plan_year_roc"))
        end_year = roc_year(row.get("period_end_roc_ym"))
        institution = clean_text(row.get("executing_institution"))
        lead = person_from_name(row.get("lead_name"), institution, start_year)
        co_names = split_people(row.get("co_lead_names"))
        co_people = [person_from_name(name, institution, start_year) for name in co_names]
        co_people = [person for person in co_people if person]
        co_lead = co_people[0] if co_people else None
        investigators = co_people[1:] if len(co_people) > 1 else []

        title_zh = clean_text(row.get("title_zh"))
        title_en = clean_text(row.get("title_en"))
        abstract_zh = clean_text(row.get("abstract_zh"))
        abstract_en = clean_text(row.get("abstract_en"))

        rows.append({
            "funder_award_id": funder_award_id,
            "original_plan_number": original_plan_number,
            "system_number": system_number,
            "grb_id": grb_id,
            "display_name": title_en or title_zh or f"MOST project {funder_award_id}",
            "title_zh": title_zh,
            "title_en": title_en,
            "description": abstract_en or abstract_zh,
            "abstract_zh": abstract_zh,
            "abstract_en": abstract_en,
            "executing_institution": institution,
            "plan_year_roc": clean_text(row.get("plan_year_roc")),
            "plan_organ_name": clean_text(row.get("plan_organ_name")),
            "research_method": clean_text(row.get("research_method")),
            "research_nature": clean_text(row.get("research_nature")),
            "research_field": clean_text(row.get("research_field")),
            "period_start_roc_ym": clean_text(row.get("period_start_roc_ym")),
            "period_end_roc_ym": clean_text(row.get("period_end_roc_ym")),
            "start_year": start_year,
            "end_year": end_year,
            "start_month": roc_month(row.get("period_start_roc_ym")),
            "end_month": roc_month(row.get("period_end_roc_ym")),
            "amount": amount_twd(row.get("amount_thousand_twd")),
            "amount_thousand_twd": clean_text(row.get("amount_thousand_twd")),
            "currency": "TWD" if amount_twd(row.get("amount_thousand_twd")) else None,
            "lead_name": clean_text(row.get("lead_name")),
            "lead_given_name": lead.get("given_name") if lead else None,
            "lead_family_name": lead.get("family_name") if lead else None,
            "co_lead_names": clean_text(row.get("co_lead_names")),
            "co_lead_json": json_string(co_lead),
            "investigators_json": json_string(investigators),
            "keywords_zh": clean_text(row.get("keywords_zh")),
            "keywords_en": clean_text(row.get("keywords_en")),
            "landing_page_url": clean_text(row.get("landing_page_url")),
            "source_search_page": SEARCH_PAGE_URL,
            "source_api_root": API_ROOT,
            "source_plan_organ_code": PLAN_ORGAN_CODE,
            "source_plan_organ_name": PLAN_ORGAN_NAME,
            "source_export_file": clean_text(row.get("_source_export_file")),
            "source_search_page_number": clean_text(row.get("_search_page")),
            "reported_total": str(reported_total) if reported_total else None,
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    df = aggregate_awards(df)
    validate_dataframe(df, full_run=limit is None)
    return df


def first_not_null(group: pd.DataFrame, column: str) -> Optional[str]:
    if column not in group:
        return None
    for value in group[column]:
        text = clean_text(value)
        if text:
            return text
    return None


def min_not_null(group: pd.DataFrame, column: str) -> Optional[str]:
    values = [clean_text(value) for value in group[column]] if column in group else []
    values = [value for value in values if value]
    return min(values) if values else None


def max_not_null(group: pd.DataFrame, column: str) -> Optional[str]:
    values = [clean_text(value) for value in group[column]] if column in group else []
    values = [value for value in values if value]
    return max(values) if values else None


def unique_json(group: pd.DataFrame, column: str) -> Optional[str]:
    if column not in group:
        return None
    values = []
    seen = set()
    for value in group[column]:
        text = clean_text(value)
        if text and text not in seen:
            values.append(text)
            seen.add(text)
    return json_string(values)


def amount_components_json(group: pd.DataFrame) -> Optional[str]:
    components = []
    for _, row in group.iterrows():
        amount = amount_twd(row.get("amount_thousand_twd"))
        if not amount:
            continue
        components.append({
            "system_number": clean_text(row.get("system_number")),
            "grb_id": clean_text(row.get("grb_id")),
            "period_start_roc_ym": clean_text(row.get("period_start_roc_ym")),
            "period_end_roc_ym": clean_text(row.get("period_end_roc_ym")),
            "amount": amount,
            "currency": "TWD",
        })
    return json_string(components)


def aggregate_awards(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate GRB annual/period rows to one award row per cited MOST grant number."""
    if df.empty:
        return df
    before = len(df)
    df = df.sort_values(["funder_award_id", "start_year", "period_start_roc_ym", "system_number"], na_position="last")
    rows: list[dict[str, Any]] = []
    for funder_award_id, group in df.groupby("funder_award_id", dropna=False, sort=True):
        numeric_amounts = pd.to_numeric(group["amount"], errors="coerce")
        amount_sum = numeric_amounts.sum(min_count=1)
        amount = None if pd.isna(amount_sum) or amount_sum <= 0 else str(int(amount_sum))
        start_year = min_not_null(group, "start_year")
        end_year = max_not_null(group, "end_year")
        lead_name = first_not_null(group, "lead_name")
        institution = first_not_null(group, "executing_institution")
        lead = person_from_name(lead_name, institution, start_year)
        co_lead_json = first_not_null(group, "co_lead_json")
        investigators_json = first_not_null(group, "investigators_json")
        rows.append({
            "funder_award_id": funder_award_id,
            "original_plan_number": first_not_null(group, "original_plan_number"),
            "system_number": first_not_null(group, "system_number"),
            "grb_id": first_not_null(group, "grb_id"),
            "display_name": first_not_null(group, "display_name"),
            "title_zh": first_not_null(group, "title_zh"),
            "title_en": first_not_null(group, "title_en"),
            "description": first_not_null(group, "description"),
            "abstract_zh": first_not_null(group, "abstract_zh"),
            "abstract_en": first_not_null(group, "abstract_en"),
            "executing_institution": institution,
            "plan_year_roc": min_not_null(group, "plan_year_roc"),
            "plan_organ_name": first_not_null(group, "plan_organ_name"),
            "research_method": first_not_null(group, "research_method"),
            "research_nature": first_not_null(group, "research_nature"),
            "research_field": first_not_null(group, "research_field"),
            "period_start_roc_ym": min_not_null(group, "period_start_roc_ym"),
            "period_end_roc_ym": max_not_null(group, "period_end_roc_ym"),
            "start_year": start_year,
            "end_year": end_year,
            "start_month": first_not_null(group, "start_month"),
            "end_month": max_not_null(group, "end_month"),
            "amount": amount,
            "amount_thousand_twd": None if amount is None else str(int(float(amount) / 1000)),
            "currency": "TWD" if amount else None,
            "lead_name": lead_name,
            "lead_given_name": lead.get("given_name") if lead else None,
            "lead_family_name": lead.get("family_name") if lead else None,
            "co_lead_names": first_not_null(group, "co_lead_names"),
            "co_lead_json": co_lead_json,
            "investigators_json": investigators_json,
            "keywords_zh": first_not_null(group, "keywords_zh"),
            "keywords_en": first_not_null(group, "keywords_en"),
            "landing_page_url": first_not_null(group, "landing_page_url"),
            "source_search_page": SEARCH_PAGE_URL,
            "source_api_root": API_ROOT,
            "source_plan_organ_code": PLAN_ORGAN_CODE,
            "source_plan_organ_name": PLAN_ORGAN_NAME,
            "source_export_file": first_not_null(group, "source_export_file"),
            "source_search_page_number": first_not_null(group, "source_search_page_number"),
            "reported_total": first_not_null(group, "reported_total"),
            "retrieved_at": first_not_null(group, "retrieved_at"),
            "source_row_count": str(len(group)),
            "system_numbers_json": unique_json(group, "system_number"),
            "grb_ids_json": unique_json(group, "grb_id"),
            "landing_page_urls_json": unique_json(group, "landing_page_url"),
            "amount_components_json": amount_components_json(group),
        })
    aggregated = pd.DataFrame(rows)
    collapsed = before - len(aggregated)
    if collapsed:
        log(
            f"Aggregated {collapsed:,} annual/period rows into cited native grant IDs; "
            f"{before:,} source rows -> {len(aggregated):,} award rows."
        )
    return aggregated.reset_index(drop=True)


def coverage(df: pd.DataFrame, column: str) -> tuple[int, float]:
    total = len(df)
    count = int(df[column].notna().sum()) if column in df else 0
    pct = count / total * 100 if total else 0.0
    return count, pct


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("Normalized dataframe is empty.")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(20).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values after dedupe: {dupes}")
    print(f"  Distinct funder_award_id values: {df['funder_award_id'].nunique():,}")

    for column in [
        "original_plan_number", "system_number", "display_name", "description",
        "executing_institution", "amount", "currency", "start_year", "end_year",
        "lead_name", "co_lead_names", "research_method", "research_nature",
        "research_field", "landing_page_url",
    ]:
        count, pct = coverage(df, column)
        print(f"  {column:24s}: {count:,}/{total:,} ({pct:.1f}%)")

    required_thresholds = {
        "display_name": 0.99,
        "original_plan_number": 0.90,
        "executing_institution": 0.90,
        "amount": 0.80,
        "currency": 0.80,
        "start_year": 0.80,
        "end_year": 0.80,
        "lead_name": 0.90,
        "landing_page_url": 0.95,
    }
    for column, threshold in required_thresholds.items():
        pct = df[column].notna().mean()
        if pct < threshold:
            raise RuntimeError(f"Unexpectedly low {column} coverage: {pct * 100:.1f}%")

    years = pd.to_numeric(df["start_year"], errors="coerce")
    amounts = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  Start year range: {int(years.min())}-{int(years.max())}")
    print(f"  Amount range TWD: {amounts.min():,.0f}-{amounts.max():,.0f}; avg {amounts.mean():,.0f}")
    print(f"  Top research methods: {df['research_method'].value_counts(dropna=False).head(10).to_dict()}")
    print(f"  Top research natures: {df['research_nature'].value_counts(dropna=False).head(10).to_dict()}")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full MOST GRB run returned only {total:,} rows; expected at least {EXPECTED_MIN_FULL_ROWS:,}."
        )


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "taiwan_most_grb_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  [OK] wrote {len(df):,} rows ({size_mb:.2f} MB) to {parquet_path}")
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

    prev_path = output_dir / "_prev_taiwan_most_grb_projects.parquet"
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
        print(f"\n[ERROR] Refusing to shrink MOST GRB corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
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
        description="Download Taiwan MOST GRB project records and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/taiwan_most_grb"))
    parser.add_argument("--limit", type=int, default=None, help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached XLSX exports from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("Taiwan MOST GRB projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Source:     {SEARCH_PAGE_URL}")
    print(f"  Plan organ: {PLAN_ORGAN_CODE} ({PLAN_ORGAN_NAME})")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.skip_download:
        exports, reported_total = load_cached_exports(args.output_dir, args.limit)
    else:
        exports, reported_total = fetch_exports(args.output_dir, args.limit)

    raw = load_exports(exports, args.limit)
    df = normalize_dataframe(raw, reported_total, args.limit)
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
