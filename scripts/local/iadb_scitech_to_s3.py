#!/usr/bin/env python3
"""
IADB science and technology projects -> S3 Data Pipeline
========================================================

Downloads project rows from the official Inter-American Development Bank
Open Data CKAN API and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://data.iadb.org/dataset/idb-projects-dataset
    https://data.iadb.org/api/3/action/package_show?id=9cd9a75d-b964-4029-8983-19fa0651c216
    https://data.iadb.org/api/3/action/datastore_search

The IDB describes the dataset as sovereign-guaranteed and non-sovereign
guaranteed programs and projects supporting borrowing member countries,
including credit lines, lending and grant financing facilities, loans,
guarantees, investment grants, technical cooperation, and equity investments
from 1960 to date. This ingest filters the official English project resource
to sector code ST / SCIENCE AND TECHNOLOGY.

Output
------
s3://openalex-ingest/awards/iadb_scitech/iadb_scitech_projects.parquet

Usage
-----
    python iadb_scitech_to_s3.py --skip-upload
    python iadb_scitech_to_s3.py --limit 10 --skip-upload
    python iadb_scitech_to_s3.py --skip-download --skip-upload
    python iadb_scitech_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
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


PACKAGE_ID = "9cd9a75d-b964-4029-8983-19fa0651c216"
EN_RESOURCE_ID = "814b7b54-477a-4c25-b3bf-6be05412069d"
DATASET_URL = "https://data.iadb.org/dataset/idb-projects-dataset"
PACKAGE_SHOW_URL = f"https://data.iadb.org/api/3/action/package_show?id={PACKAGE_ID}"
DATASTORE_SEARCH_URL = "https://data.iadb.org/api/3/action/datastore_search"
PROJECT_URL_TEMPLATE = "https://www.iadb.org/en/project/{oper_num}"

FUNDER_ID = 4320307862
FUNDER_DISPLAY_NAME = "Inter-American Development Bank"
PROVENANCE = "iadb_project_search_scitech"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/iadb_scitech/iadb_scitech_projects.parquet"

USER_AGENT = "openalex-walden-iadb-scitech-ingest/1.0 (+https://openalex.org)"
PAGE_SIZE = 500
MIN_REQUEST_INTERVAL_S = 0.25

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
        })
    return _session


def polite_json(url: str, *, params: Optional[dict[str, Any]] = None, timeout: int = 90,
                max_attempts: int = 4) -> dict[str, Any]:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, params=params, timeout=timeout)
            _last_request_t = time.monotonic()
            print(f"  GET {resp.url} -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success", False):
                raise RuntimeError(f"CKAN API returned success=false: {data}")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
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
    text = str(value).replace("\ufeff", "").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def amount_to_string(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    try:
        amount = Decimal(text.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    if amount <= 0:
        return None
    normalized = amount.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def iso_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    match = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", text)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


def year_from_date(value: Optional[str]) -> Optional[str]:
    if value and re.match(r"^\d{4}-", value):
        return value[:4]
    return None


def source_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)


def discover_resource() -> dict[str, Any]:
    print("\n" + "=" * 60)
    print("Step 1: Discover official IDB Open Data resource")
    print("=" * 60)
    package = polite_json(PACKAGE_SHOW_URL)["result"]
    resources = package.get("resources") or []
    resource = None
    for candidate in resources:
        if candidate.get("id") == EN_RESOURCE_ID:
            resource = candidate
            break
    if resource is None:
        for candidate in resources:
            if clean_text(candidate.get("name")) == "EN - IDB Projects List":
                resource = candidate
                break
    if resource is None:
        raise RuntimeError("Could not find the official English IDB project resource in package_show")
    if not resource.get("datastore_active"):
        raise RuntimeError(f"IDB resource {resource.get('id')} is not datastore_active")
    print(f"  [OK] package: {package.get('title', {}).get('en') or package.get('name')}")
    print(f"  [OK] resource: {resource.get('name')} ({resource.get('id')})")
    print(f"  [OK] resource modified: {resource.get('last_modified')}")
    return {
        "package": package,
        "resource": resource,
    }


def fetch_scitech_rows(resource_id: str, limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 2: Fetch science and technology project rows")
    print("=" * 60)
    rows: list[dict[str, Any]] = []
    total: Optional[int] = None
    offset = 0
    filters = json.dumps({"sector_cd": "ST"})

    while True:
        page_limit = PAGE_SIZE
        if limit is not None:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            page_limit = min(page_limit, remaining)

        data = polite_json(
            DATASTORE_SEARCH_URL,
            params={
                "resource_id": resource_id,
                "filters": filters,
                "limit": page_limit,
                "offset": offset,
            },
        )["result"]
        if total is None:
            total = int(data["total"])
            print(f"  Source reports {total:,} science and technology rows")
        batch = data.get("records") or []
        print(f"  Fetched offset {offset:,}: {len(batch):,} rows")
        if not batch:
            raise RuntimeError(f"Empty CKAN page before reaching reported total at offset {offset:,}")
        rows.extend(batch)
        offset += len(batch)
        if limit is not None and len(rows) >= limit:
            print(f"  [LIMIT] keeping first {len(rows):,} rows")
            break
        if total is not None and len(rows) >= total:
            break

    if not rows:
        raise RuntimeError("No IDB science and technology rows were fetched")
    return rows


def write_raw_cache(output_dir: Path, manifest: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    cache = {
        "manifest": manifest,
        "rows": rows,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    (output_dir / "iadb_scitech_projects.json").write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  [OK] cached raw CKAN response to {output_dir / 'iadb_scitech_projects.json'}")


def load_raw_cache(output_dir: Path, limit: Optional[int]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cache_path = output_dir / "iadb_scitech_projects.json"
    if not cache_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {cache_path}")
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    rows = cache.get("rows") or []
    if limit is not None:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} cached rows")
    print(f"  [OK] loaded {len(rows):,} cached raw rows from {cache_path}")
    return cache.get("manifest") or {}, rows


def funding_type_for(operation_type: Optional[str]) -> str:
    text = (operation_type or "").lower()
    if "grant" in text or "technical cooperation" in text:
        return "grant"
    if "loan" in text:
        return "loan"
    if "equity" in text:
        return "equity"
    if "guarantee" in text:
        return "guarantee"
    return "research"


def normalize(manifest: dict[str, Any], rows: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 3: Normalize and validate")
    print("=" * 60)
    resource = manifest.get("resource") or {}
    downloaded_at = manifest.get("downloaded_at") or datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for raw in rows:
        award_id = clean_text(raw.get("oper_num"))
        approved_date = iso_date(raw.get("apprvl_dt"))
        signed_date = iso_date(raw.get("sign_dt"))
        amount = amount_to_string(raw.get("orig_apprvd_useq_amnt"))
        total_cost = amount_to_string(raw.get("totl_cost_orig"))
        local_counterpart = amount_to_string(raw.get("loc_cntrprt"))
        operation_type = clean_text(raw.get("opertyp_nm"))
        records.append({
            "funder_award_id": award_id,
            "display_name": clean_text(raw.get("oper_nm")),
            "description": clean_text(raw.get("objtv")),
            "country_code": clean_text(raw.get("cntry_cd")),
            "country_name": clean_text(raw.get("cntry_nm")),
            "status": clean_text(raw.get("publc_sts_nm")),
            "sector_code": clean_text(raw.get("sector_cd")),
            "sector_name": clean_text(raw.get("sector_nm")),
            "subsector_code": clean_text(raw.get("subsector_cd")),
            "subsector_name": clean_text(raw.get("subsector_nm")),
            "operation_type_code": clean_text(raw.get("oper_typ_cd")),
            "operation_type": operation_type,
            "funding_type": funding_type_for(operation_type),
            "amount": amount,
            "currency": CURRENCY if amount is not None else None,
            "total_cost_usd_equiv": total_cost,
            "local_counterpart_usd_equiv": local_counterpart,
            "lending_type": clean_text(raw.get("lending_typ_nm")),
            "modality_code": clean_text(raw.get("modality_cd")),
            "modality": clean_text(raw.get("modality_nm")),
            "facility_type_code": clean_text(raw.get("facility_typ_cd")),
            "facility_type": clean_text(raw.get("facility_typ_nm")),
            "environmental_classification_code": clean_text(raw.get("envmntl_clssfctn_cd")),
            "environmental_classification": clean_text(raw.get("envmntl_clssfctn_nm")),
            "lending_instrument_code": clean_text(raw.get("lending_instrmnt_cd")),
            "lending_instrument": clean_text(raw.get("lending_instrmnt_nm")),
            "lending_type_code": clean_text(raw.get("lending_typ_cd")),
            "status_code": clean_text(raw.get("sts_cd")),
            "approved_date": approved_date,
            "signed_date": signed_date,
            "source_year": year_from_date(approved_date) or year_from_date(signed_date),
            "landing_page_url": PROJECT_URL_TEMPLATE.format(oper_num=award_id) if award_id else DATASET_URL,
            "source_dataset_url": DATASET_URL,
            "source_package_id": PACKAGE_ID,
            "source_resource_id": resource.get("id") or EN_RESOURCE_ID,
            "source_resource_name": clean_text(resource.get("name")),
            "source_resource_last_modified": clean_text(resource.get("last_modified")),
            "source_filter": "sector_cd=ST",
            "source_row_json": source_row_json(raw),
            "downloaded_at": downloaded_at,
        })

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No IDB rows were normalized")

    required = ["funder_award_id", "display_name", "sector_code", "amount", "currency"]
    for col in required:
        missing = int(out[col].isna().sum())
        if missing:
            raise RuntimeError(f"Required column {col} has {missing:,} missing values")

    non_st = out.loc[out["sector_code"] != "ST", "sector_code"].dropna().unique().tolist()
    if non_st:
        raise RuntimeError(f"Unexpected non-ST sector codes: {non_st[:10]}")

    dupes = int(out["funder_award_id"].duplicated().sum())
    if dupes:
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes:,}")

    if full_run and len(out) < 700:
        raise RuntimeError(f"Expected at least 700 IADB science/technology rows in a full run; got {len(out):,}")

    amount_numeric = pd.to_numeric(out["amount"], errors="coerce")
    year_numeric = pd.to_numeric(out["source_year"], errors="coerce")
    print(f"  Rows: {len(out):,}")
    print(f"  Unique funder_award_id: {out['funder_award_id'].nunique():,}")
    print(f"  Year range: {int(year_numeric.min())} - {int(year_numeric.max())}")
    print(f"  Amount coverage: {amount_numeric.notna().mean() * 100:.1f}%")
    print(f"  Total USD-equivalent approved amount: {amount_numeric.sum():,.2f}")
    print(f"  Description coverage: {out['description'].notna().mean() * 100:.1f}%")
    print(f"  Approved-date coverage: {out['approved_date'].notna().mean() * 100:.1f}%")
    print(f"  Operation type distribution: {out['operation_type'].value_counts().to_dict()}")
    print(f"  Status distribution: {out['status'].value_counts().to_dict()}")
    print(f"  Top countries: {out['country_name'].value_counts().head(10).to_dict()}")

    return out.astype("string")


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "iadb_scitech_projects.parquet"
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

    prev_path = output_dir / "_prev_iadb_scitech_projects.parquet"
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
        print(f"\n[ERROR] Refusing to shrink IADB science/technology corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download IADB science/technology projects and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/iadb_scitech"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached CKAN JSON from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("IADB science and technology projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {DATASET_URL}")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw CKAN rows")
        manifest, rows = load_raw_cache(args.output_dir, args.limit)
    else:
        manifest = discover_resource()
        manifest["downloaded_at"] = datetime.now(timezone.utc).isoformat()
        resource_id = manifest["resource"]["id"]
        rows = fetch_scitech_rows(resource_id, args.limit)
        write_raw_cache(args.output_dir, manifest, rows)

    df = normalize(manifest, rows, full_run=args.limit is None)
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
