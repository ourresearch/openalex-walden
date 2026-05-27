#!/usr/bin/env python3
"""
Israel Science Foundation grants -> S3 Data Pipeline
====================================================

Downloads grant rows from the official Israel Science Foundation (ISF) public
site API and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.isf.org.il/
    https://www.isf.org.il/api/grant/GetGrantSearchData

The Angular application at isf.org.il calls GetGrantSearchData for its public
Research Grants, Scientific Equipment, Workshop, Publications, and Postdoctoral
Fellowships pages. The endpoint returns native application IDs, program names,
PIs, institutions, award years, durations, and the source-published approved
annual budget in Israeli new shekels.

Output
------
s3://openalex-ingest/awards/isf/isf_grants.parquet

Usage
-----
    python isf_to_s3.py --skip-upload
    python isf_to_s3.py --limit 10 --skip-upload
    python isf_to_s3.py --skip-download --skip-upload
    python isf_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---


BASE_URL = "https://www.isf.org.il"
GRANT_SEARCH_URL = f"{BASE_URL}/api/grant/GetGrantSearchData"
ACTIVE_GRANT_TYPES_URL = f"{BASE_URL}/api/grant/GetActiveGrantTypes"
GLOBAL_PARAMETER_URL = f"{BASE_URL}/api/innerPages/GetGlobalParameter?id=2"

FUNDER_ID = 4320322252
FUNDER_DISPLAY_NAME = "Israel Science Foundation"
PROVENANCE = "isf_grant_search"
CURRENCY = "ILS"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/isf/isf_grants.parquet"

USER_AGENT = "openalex-walden-isf-ingest/1.0 (+https://openalex.org)"
START_YEAR = 1991
DEFAULT_MAX_WORKERS = 4
MAX_ATTEMPTS = 4

ALLOCATION_TYPES: dict[int, str] = {
    1: "Research Grants",
    2: "Scientific Equipment",
    3: "Workshops",
    4: "Publications",
    5: "Postdoctoral Fellowships",
}


_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json; charset=UTF-8",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/",
        })
    return _session


def request_json(method: str, url: str, *, payload: Optional[dict[str, Any]] = None,
                 timeout: int = 75) -> Any:
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            if method == "GET":
                resp = session.get(url, timeout=timeout)
            else:
                resp = session.post(url, json=payload, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code} for {url}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt >= MAX_ATTEMPTS:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"{method} failed after {MAX_ATTEMPTS} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def int_value(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def amount_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        amount = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    return amount


def string_or_none(value: Any) -> Optional[str]:
    text = clean_text(value)
    return text if text not in {"", "None", "null"} else None


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    text = re.sub(
        r"^(Professor|Prof\\.?|Associate Professor|Assistant Professor|Dr\\.?|Doctor)\\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    tokens = [token.strip(",") for token in text.split() if token.strip(",")]
    suffixes = {"PhD", "MD", "DPhil", "Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def iso_date_from_year(year: Optional[int]) -> Optional[str]:
    if year is None:
        return None
    return f"{year:04d}-01-01"


def iso_end_date(start_year: Optional[int], years: Optional[int]) -> Optional[str]:
    if start_year is None or years is None or years <= 0:
        return None
    return f"{start_year + years - 1:04d}-12-31"


def landing_page_for(allocation_type: int, grant_type_id: Optional[int]) -> str:
    if allocation_type == 1:
        suffix = grant_type_id if grant_type_id is not None else ""
        return f"{BASE_URL}/#/studies/undefined/undefined/{suffix}"
    if allocation_type == 2:
        suffix = grant_type_id if grant_type_id is not None else ""
        return f"{BASE_URL}/#/scientific-equipment/{suffix}"
    if allocation_type == 3:
        return f"{BASE_URL}/#/workshop"
    if allocation_type == 4:
        return f"{BASE_URL}/#/books"
    if allocation_type == 5:
        return f"{BASE_URL}/#/post-doc"
    return f"{BASE_URL}/"


def funding_type_for(allocation_type: int) -> str:
    if allocation_type == 1:
        return "research"
    if allocation_type == 5:
        return "fellowship"
    return "grant"


def display_name_for(row: dict[str, Any]) -> Optional[str]:
    title = string_or_none(row.get("title")) or string_or_none(row.get("publicationName"))
    if title:
        return title
    parts = [
        string_or_none(row.get("grantTypeName")) or "ISF grant",
        string_or_none(row.get("firstPi")),
    ]
    year = string_or_none(row.get("applicationYear"))
    name = " - ".join(part for part in parts if part)
    if year:
        name = f"{name} ({year})"
    return clean_text(name)


def get_current_cycle_year() -> int:
    data = request_json("GET", GLOBAL_PARAMETER_URL)
    year = int_value(data.get("value") if isinstance(data, dict) else None)
    if not year:
        raise RuntimeError(f"Could not parse current ISF cycle year from {data!r}")
    print(f"  Current ISF cycle year: {year}")
    return year


def get_grant_type_names() -> dict[int, dict[int, str]]:
    print("  Fetching active grant type metadata")
    by_allocation: dict[int, dict[int, str]] = {}
    for allocation_type in ALLOCATION_TYPES:
        url = f"{ACTIVE_GRANT_TYPES_URL}?allocationType={allocation_type}&lang=en-US"
        data = request_json("GET", url)
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected grant type response for allocation {allocation_type}: {data!r}")
        by_allocation[allocation_type] = {
            int(row["id"]): clean_text(row.get("name")) or f"Grant type {row['id']}"
            for row in data
            if row.get("id") is not None
        }
        print(f"    allocation {allocation_type}: {len(by_allocation[allocation_type]):,} active grant types")
    return by_allocation


def fetch_slice(allocation_type: int, year: int) -> list[dict[str, Any]]:
    payload = {
        "AllocationType": allocation_type,
        "allocationType": allocation_type,
        "lang": "en-US",
        "applicationYear": year,
    }
    rows = request_json("POST", GRANT_SEARCH_URL, payload=payload)
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected grant search response for allocation {allocation_type}, year {year}: {rows!r}")
    for row in rows:
        row["_source_allocation_type_id"] = allocation_type
        row["_source_allocation_type_name"] = ALLOCATION_TYPES[allocation_type]
        row["_source_application_year_request"] = year
        row["_source_api_url"] = GRANT_SEARCH_URL
        row["_source_api_payload"] = payload
    return rows


def fetch_rows(limit: Optional[int], start_year: int, end_year: int,
               max_workers: int) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download official ISF API rows")
    print("=" * 60)
    get_grant_type_names()

    tasks = [
        (allocation_type, year)
        for year in range(end_year, start_year - 1, -1)
        for allocation_type in ALLOCATION_TYPES
    ]
    print(f"  Request plan: {len(tasks):,} allocation/year slices ({start_year}-{end_year})")

    if limit:
        print(f"  [LIMIT] sequential smoke mode until at least {limit:,} rows are fetched")
        rows: list[dict[str, Any]] = []
        for allocation_type, year in tasks:
            part = fetch_slice(allocation_type, year)
            print(f"    {year} allocation {allocation_type}: {len(part):,} rows")
            rows.extend(part)
            if len(rows) >= limit:
                return rows[:limit]
        return rows

    rows = []
    started = time.time()
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_slice, allocation_type, year): (allocation_type, year)
            for allocation_type, year in tasks
        }
        for future in as_completed(future_map):
            allocation_type, year = future_map[future]
            part = future.result()
            rows.extend(part)
            completed += 1
            elapsed = time.time() - started
            rate = completed / elapsed if elapsed else 0.0
            remaining = len(tasks) - completed
            eta_s = remaining / rate if rate else 0.0
            print(
                f"    [{completed:3d}/{len(tasks)}] {year} allocation {allocation_type}: "
                f"{len(part):,} rows; total {len(rows):,}; ETA {eta_s/60:.1f}m"
            )
    return rows


def make_funder_award_id(row: dict[str, Any]) -> str:
    application_id = clean_text(row.get("applicationId"))
    if application_id:
        return f"isf-{application_id}"
    digest = hashlib.sha1(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return f"isf-hash-{digest}"


def source_row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def same_award_core(left: dict[str, Any], right: dict[str, Any]) -> bool:
    core_fields = [
        "applicationId",
        "title",
        "grantTypeId",
        "applicationYear",
        "firstPi",
        "instituteName",
    ]
    return all(clean_text(left.get(field)) == clean_text(right.get(field)) for field in core_fields)


def row_quality(row: dict[str, Any]) -> tuple[float, int, int]:
    """Choose the most complete row when ISF emits a zero-amount duplicate."""
    amount = amount_value(row.get("approvedSum")) or 0.0
    has_currency = 1 if clean_text(row.get("currency")) else 0
    special_len = len(clean_text(row.get("specialFund")) or "")
    return amount, has_currency, special_len


def dedupe_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    selected_hashes: dict[str, str] = {}
    exact_duplicates = 0
    replaced_with_richer = 0
    dropped_lower_quality = 0

    for row in rows:
        key = make_funder_award_id(row)
        digest = source_row_hash(row)
        if key not in selected:
            selected[key] = row
            selected_hashes[key] = digest
            continue
        if selected_hashes[key] == digest:
            exact_duplicates += 1
            continue
        if not same_award_core(selected[key], row):
            raise RuntimeError(f"Conflicting duplicate funder_award_id: {key}")
        if row_quality(row) > row_quality(selected[key]):
            selected[key] = row
            selected_hashes[key] = digest
            replaced_with_richer += 1
        else:
            dropped_lower_quality += 1

    if exact_duplicates or replaced_with_richer or dropped_lower_quality:
        print(
            "  [INFO] duplicate native IDs resolved: "
            f"{exact_duplicates:,} exact skipped, "
            f"{replaced_with_richer:,} richer rows kept, "
            f"{dropped_lower_quality:,} lower-quality rows dropped"
        )
    return list(selected.values())


def normalize_rows(rows: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize and validate rows")
    print("=" * 60)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    deduped_rows = dedupe_source_rows(rows)

    for row in deduped_rows:
        allocation_type = int_value(row.get("_source_allocation_type_id")) or 0
        grant_type_id = int_value(row.get("grantTypeId"))
        application_year = int_value(row.get("applicationYear"))
        years = int_value(row.get("years"))
        amount = amount_value(row.get("approvedSum"))
        first_pi = string_or_none(row.get("firstPi"))
        given_name, family_name = split_person_name(first_pi)
        funder_award_id = make_funder_award_id(row)
        source_record_json = json.dumps(row, ensure_ascii=False, sort_keys=True)
        source_record_hash = hashlib.sha1(source_record_json.encode("utf-8")).hexdigest()

        out.append({
            "funder_award_id": funder_award_id,
            "application_id": string_or_none(row.get("applicationId")),
            "display_name": display_name_for(row),
            "source_title": string_or_none(row.get("title")),
            "description": string_or_none(row.get("keywords")),
            "keywords": string_or_none(row.get("keywords")),
            "source_allocation_type_id": str(allocation_type) if allocation_type else None,
            "source_allocation_type_name": string_or_none(row.get("_source_allocation_type_name")),
            "grant_type_id": str(grant_type_id) if grant_type_id is not None else None,
            "grant_type_name": string_or_none(row.get("grantTypeName")),
            "broad_field_id": string_or_none(row.get("broadFieldId")),
            "broad_field": string_or_none(row.get("broadField")),
            "general_field_id": string_or_none(row.get("generalFieldId")),
            "general_field": string_or_none(row.get("generalField")),
            "source_year": str(application_year) if application_year is not None else None,
            "years": str(years) if years is not None else None,
            "start_date": iso_date_from_year(application_year),
            "end_date": iso_end_date(application_year, years),
            "amount": f"{amount:.0f}" if amount is not None and amount.is_integer() else (str(amount) if amount is not None else None),
            "currency": CURRENCY if amount is not None else None,
            "approved_sum_raw": string_or_none(row.get("approvedSum")),
            "source_currency_symbol": string_or_none(row.get("currency")),
            "special_fund": string_or_none(row.get("specialFund")),
            "special_fund_sum": string_or_none(row.get("specialFundSum")),
            "funding_type": funding_type_for(allocation_type),
            "lead_investigator_name": first_pi,
            "lead_investigator_given_name": given_name,
            "lead_investigator_family_name": family_name,
            "foreign_first_pi": string_or_none(row.get("foreignFirstPi")),
            "additional_investigators_raw": string_or_none(row.get("personsInGrant")),
            "pis_in_grant": string_or_none(row.get("pisInGrant")),
            "institution": string_or_none(row.get("instituteName")),
            "institute_id": string_or_none(row.get("instituteId")),
            "cooperation_with_advanced": string_or_none(row.get("cooperationWithAdvanced")),
            "publication_name": string_or_none(row.get("publicationName")),
            "publication_location": string_or_none(row.get("publicationLocation")),
            "publication_year": string_or_none(row.get("publicationYear")),
            "book_status": string_or_none(row.get("bookStatus")),
            "link_name": string_or_none(row.get("linkName")),
            "years_range": string_or_none(row.get("yearsRange")),
            "research_time_not_pass": string_or_none(row.get("researchTimeNotPass")),
            "landing_page_url": landing_page_for(allocation_type, grant_type_id),
            "source_api_url": string_or_none(row.get("_source_api_url")),
            "source_api_payload": json.dumps(row.get("_source_api_payload"), ensure_ascii=False, sort_keys=True),
            "source_record_hash": source_record_hash,
            "source_record_json": source_record_json,
            "downloaded_at": downloaded_at,
        })

    df = pd.DataFrame(out)
    if df.empty:
        raise RuntimeError("No ISF rows were normalized")

    required = ["funder_award_id", "display_name", "source_year", "grant_type_name", "landing_page_url"]
    for col in required:
        missing = int(df[col].isna().sum())
        if missing:
            raise RuntimeError(f"Required column {col} has {missing:,} missing values")

    dupes = df["funder_award_id"].duplicated().sum()
    if dupes:
        raise RuntimeError(f"Duplicate funder_award_id values after normalization: {dupes:,}")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    year_numeric = pd.to_numeric(df["source_year"], errors="coerce")

    print(f"  Rows: {len(df):,}")
    print(f"  Unique funder_award_id: {df['funder_award_id'].nunique():,}")
    print(f"  Year range: {int(year_numeric.min())} - {int(year_numeric.max())}")
    print(f"  Rows with amount: {int(amount_numeric.notna().sum()):,} ({amount_numeric.notna().mean() * 100:.1f}%)")
    print(f"  Total ILS amount (annual approved budgets): {amount_numeric.sum():,.0f}")
    print(f"  Allocation distribution: {df['source_allocation_type_name'].value_counts().to_dict()}")
    print(f"  Top grant types: {df['grant_type_name'].value_counts().head(12).to_dict()}")
    print(f"  Institution coverage: {df['institution'].notna().mean() * 100:.1f}%")
    print(f"  Lead PI coverage: {df['lead_investigator_name'].notna().mean() * 100:.1f}%")

    if full_run and len(df) < 1_000:
        raise RuntimeError(f"Full ISF run unexpectedly small: {len(df):,} rows")

    # Runbook section 1.2: force all source columns to Spark-compatible strings
    # immediately before parquet write.
    return df.astype("string")


def write_outputs(rows: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "isf_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")

    parquet_path = output_dir / "isf_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_rows(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "isf_grants_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached rows from {raw_path}")
    return rows


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

    prev_path = output_dir / "_prev_isf_grants.parquet"
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
        print(f"\n[ERROR] Refusing to shrink ISF corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
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
        description="Download ISF grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/isf"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse isf_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help="First application year to request")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Last application year to request; defaults to ISF current cycle")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS,
                        help="Parallel allocation/year API requests for full runs")
    args = parser.parse_args()

    print("=" * 60)
    print("ISF grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source API: {GRANT_SEARCH_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        rows = load_cached_rows(args.output_dir)
        if args.limit:
            rows = rows[:args.limit]
            print(f"  [LIMIT] keeping first {len(rows):,} cached rows")
    else:
        end_year = args.end_year or get_current_cycle_year()
        if args.start_year > end_year:
            raise ValueError(f"--start-year {args.start_year} is after end year {end_year}")
        rows = fetch_rows(args.limit, args.start_year, end_year, max(1, args.max_workers))

    df = normalize_rows(rows, full_run=args.limit is None)
    parquet_path = write_outputs(rows, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
