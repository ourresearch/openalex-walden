#!/usr/bin/env python3
"""
COSTECH NFAST projects -> S3 Data Pipeline
==========================================

Downloads project rows from the official Tanzania Commission for Science and
Technology (COSTECH) NFAST funded-projects API and writes a parquet file for
the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.costech.or.tz/projects/costech-funded
    https://rclearance.costech.or.tz/api/v1/nfast/project/list

The current COSTECH website is a React SPA. Its "COSTECH Funded Projects"
view calls the public NFAST API above, which returns project-level records
with project titles, principal researchers, programs, sectors, statuses,
dates, external funding-source labels, total budgets, objectives, and results.

Output
------
s3://openalex-ingest/awards/costech/costech_nfast_projects.parquet

Usage
-----
    python costech_to_s3.py --skip-upload
    python costech_to_s3.py --limit 10 --skip-upload
    python costech_to_s3.py --skip-download --skip-upload
    python costech_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
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


# =============================================================================
# Configuration
# =============================================================================

API_URL = "https://rclearance.costech.or.tz/api/v1/nfast/project/list"
SITE_PROJECT_BASE = "https://www.costech.or.tz/projects/costech-funded"

FUNDER_ID = 4320323478
FUNDER_DISPLAY_NAME = "Tanzania Commission for Science and Technology"
PROVENANCE = "costech_nfast_projects"
CURRENCY = "TZS"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/costech/costech_nfast_projects.parquet"

USER_AGENT = "openalex-walden-costech-ingest/1.0 (+https://openalex.org)"
PAGE_SIZE = 100
MIN_REQUEST_INTERVAL_S = 0.25

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.costech.or.tz",
            "Referer": f"{SITE_PROJECT_BASE}/",
        })
    return _session


def polite_post_json(url: str, payload: dict[str, Any],
                     timeout: int = 60, max_attempts: int = 4) -> dict[str, Any]:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.post(url, json=payload, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Expected JSON object from {url}")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"POST failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def nested_name(row: dict[str, Any], key: str) -> Optional[str]:
    value = row.get(key)
    if isinstance(value, dict):
        return clean_text(value.get("name") or value.get("description"))
    return None


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    text = re.sub(
        r"^(Prof\\.?|Professor|Assoc\\.?\\s+Prof\\.?|Dr\\.?|Doctor|Mr\\.?|Ms\\.?|Mrs\\.?)\\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    tokens = [token.strip(" ,") for token in text.split() if token.strip(" ,")]
    suffixes = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1].rstrip(",") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_amount(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    try:
        amount = float(text.replace(",", ""))
    except ValueError:
        return None
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def fetch_project_records(limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch COSTECH NFAST project list")
    print("=" * 60)

    rows: list[dict[str, Any]] = []
    page = 1
    total_pages: Optional[int] = None
    expected_total: Optional[int] = None

    while total_pages is None or page <= total_pages:
        payload = {"page_no": page, "page_size": PAGE_SIZE}
        data = polite_post_json(API_URL, payload)
        page_rows = data.get("data") or []
        meta = data.get("meta") or {}
        if not isinstance(page_rows, list):
            raise RuntimeError(f"Unexpected data shape on page {page}: {type(page_rows)}")

        total_pages = int(meta.get("pages") or total_pages or 1)
        expected_total = int(meta.get("total") or expected_total or len(page_rows))
        rows.extend(page_rows)
        print(f"  page {page:,}/{total_pages:,}: {len(page_rows):,} rows; total {len(rows):,}/{expected_total:,}")

        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            print(f"  [LIMIT] stopping after {len(rows):,} rows")
            break
        page += 1

    if limit is None and expected_total is not None and len(rows) != expected_total:
        raise RuntimeError(f"Expected {expected_total:,} rows from API metadata, got {len(rows):,}")
    if not rows:
        raise RuntimeError("No COSTECH NFAST project rows returned")
    return rows


def normalize_records(records: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize and validate")
    print("=" * 60)
    rows: list[dict[str, Any]] = []
    downloaded_at = datetime.now(timezone.utc).isoformat()
    for row in records:
        project_id = row.get("id")
        researcher = row.get("researcher") if isinstance(row.get("researcher"), dict) else {}
        researcher_name = clean_text(researcher.get("name"))
        given_name, family_name = split_person_name(researcher_name)
        amount = parse_amount(row.get("total_budget"))
        program_name = nested_name(row, "program")
        subprogram_name = nested_name(row, "subprogram")
        source_funder_name = nested_name(row, "funder")

        rows.append({
            "funder_award_id": f"nfast-{project_id}" if project_id is not None else None,
            "source_project_id": clean_text(project_id),
            "display_name": clean_text(row.get("research_title")),
            "description": clean_text(row.get("background") or row.get("objective")),
            "objective": clean_text(row.get("objective")),
            "result": clean_text(row.get("result")),
            "lead_investigator_name": researcher_name,
            "lead_investigator_given_name": given_name,
            "lead_investigator_family_name": family_name,
            "program_name": program_name,
            "subprogram_name": subprogram_name,
            "sector_name": nested_name(row, "sector"),
            "status_name": nested_name(row, "status"),
            "source_funder_name": source_funder_name,
            "source_funder_description": clean_text((row.get("funder") or {}).get("description")) if isinstance(row.get("funder"), dict) else None,
            "amount": amount,
            "currency": CURRENCY if amount is not None else None,
            "start_date": clean_text(row.get("start_date")),
            "end_date": clean_text(row.get("end_date")),
            "source_year": clean_text(row.get("start_date"))[:4] if clean_text(row.get("start_date")) else None,
            "funding_type": "research" if (program_name or "").lower() == "research program" else "grant",
            "funder_scheme": " - ".join(part for part in [program_name, subprogram_name] if part),
            "number_publication": clean_text(row.get("number_publication")),
            "number_knowledge_asset": clean_text(row.get("number_knowledge_asset")),
            "is_startup_registered": clean_text(row.get("is_startup_registered")),
            "provenance": PROVENANCE,
            "landing_page_url": f"{SITE_PROJECT_BASE}/{project_id}" if project_id is not None else None,
            "source_url": API_URL,
            "created_at": clean_text(row.get("created_at")),
            "updated_at": clean_text(row.get("updated_at")),
            "downloaded_at": downloaded_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("No COSTECH rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print("  funder_award_id duplicates: 0")

    required_cols = [
        "funder_award_id",
        "display_name",
        "lead_investigator_name",
        "program_name",
        "subprogram_name",
        "sector_name",
        "status_name",
        "source_funder_name",
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
        print(f"  {col:24s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    desc_coverage = df["description"].notna().mean() if total else 0
    print(f"  {'description':24s}: {df['description'].notna().sum():,}/{total:,} ({desc_coverage * 100:.1f}%)")
    if desc_coverage < 0.95:
        raise RuntimeError(f"Unexpectedly low description coverage: {desc_coverage * 100:.1f}%")

    if full_run and total < 400:
        raise RuntimeError(f"Full COSTECH run returned only {total:,} rows; expected at least 400.")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    positive = (amount_numeric > 0).sum()
    print(f"  Positive TZS budgets: {positive:,}/{total:,} ({positive / total * 100:.1f}%)")
    print(f"  Total TZS budget: {amount_numeric.sum():,.0f}")
    print(f"  Year range: {df['source_year'].min()} - {df['source_year'].max()}")
    print(f"  Program distribution: {df['program_name'].value_counts(dropna=False).to_dict()}")
    print(f"  Source funder distribution: {df['source_funder_name'].value_counts(dropna=False).to_dict()}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "costech_nfast_projects_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw API rows to {raw_path}")

    parquet_path = output_dir / "costech_nfast_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "costech_nfast_projects_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
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

    prev_path = output_dir / "_prev_costech_nfast_projects.parquet"
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
            f"\n[ERROR] Refusing to shrink COSTECH corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
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
        description="Download COSTECH NFAST projects and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/costech"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit project records for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse costech_nfast_projects_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("COSTECH NFAST projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  API:        {API_URL}")
    print(f"  Site:       {SITE_PROJECT_BASE}")
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
        records = fetch_project_records(limit=args.limit)

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
