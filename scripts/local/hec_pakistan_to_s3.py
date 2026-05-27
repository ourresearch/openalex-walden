#!/usr/bin/env python3
"""
Higher Education Commission Pakistan NRPU grants -> S3 pipeline
===============================================================

Downloads completed National Research Program for Universities (NRPU)
project records from the Higher Education Commission, Pakistan public
outcomes portal and writes a parquet staging file for the OpenAlex awards
pipeline.

Source authority
----------------
The source is first-party. HEC's official Research Grant Statistics page links
to the NRPU outcomes portal:

    https://www.hec.gov.pk/english/services/RnD/Pages/Research-Grant-Statistics.aspx
    http://nrpuonline.hec.gov.pk/NRPUApplication/Public_Completed

The public portal uses a same-origin DataTables endpoint:

    http://nrpuonline.hec.gov.pk/NRPUApplication/Public_Completed/PublicCompletedTblData

Validation on 2026-05-27:
  - DataTables endpoint reports 1,892 NRPU project rows
  - 1,892 distinct project numbers
  - 100% title/project number/university/status coverage
  - 99.9% PI name coverage and 98.2% PI email coverage
  - Per-project amount, currency, and award year are not published by this
    endpoint; those fields are intentionally NULL per runbook section 6.7.

Output
------
s3://openalex-ingest/awards/hec_pakistan/hec_pakistan_nrpu.parquet

Usage
-----
    python hec_pakistan_to_s3.py --skip-upload
    python hec_pakistan_to_s3.py --limit 10 --skip-upload
    python hec_pakistan_to_s3.py --skip-download --skip-upload
    python hec_pakistan_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import html
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


API_URL = "http://nrpuonline.hec.gov.pk/NRPUApplication/Public_Completed/PublicCompletedTblData"
PUBLIC_PAGE_URL = "http://nrpuonline.hec.gov.pk/NRPUApplication/Public_Completed"
HEC_SOURCE_PAGE_URL = "https://www.hec.gov.pk/english/services/RnD/Pages/Research-Grant-Statistics.aspx"

FUNDER_ID = 4320322799
FUNDER_DISPLAY_NAME = "Higher Education Commission, Pakistan"
PROVENANCE = "hec_pakistan_nrpu"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hec_pakistan/hec_pakistan_nrpu.parquet"

USER_AGENT = "openalex-walden-hec-pakistan-ingest/1.0 (+https://openalex.org)"
EXPECTED_MIN_FULL_ROWS = 1800

TITLE_RE = re.compile(r'title="([^"]*)"')
HREF_RE = re.compile(r'href="([^"]*)"')
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
PI_TITLE_RE = re.compile(
    r"^(?:Dr|Dr\.|Prof|Prof\.|Professor|Mr|Mr\.|Ms|Ms\.|Mrs|Mrs\.)\s+",
    re.IGNORECASE,
)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = TAG_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text).strip()
    return text or None


def extract_title(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = str(value)
    match = TITLE_RE.search(raw)
    if match:
        return clean_text(match.group(1))
    return clean_text(raw)


def extract_href(value: Any) -> Optional[str]:
    if value is None:
        return None
    match = HREF_RE.search(str(value))
    if not match:
        return None
    return html.unescape(match.group(1)).strip() or None


_PI_SUFFIX_TOKENS = {
    "phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
}


def split_pi_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split a Pakistani HEC PI name into (given, family).

    Strips leading honorifics (Dr/Prof/Mr/Ms/Mrs via PI_TITLE_RE) AND
    trailing degree/suffix tokens (PhD, MD, Jr., II, etc.) before splitting,
    so "Dr. Ahmed Ali PhD" -> ("Ahmed", "Ali") not ("Ahmed Ali", "PhD").
    Matches the canonical split_name helper from scripts/local/wolf_to_s3.py
    per how-to-add-a-funder-v2.md §2.4.1.
    """
    if not name:
        return None, None
    stripped = PI_TITLE_RE.sub("", name).strip()
    parts = stripped.split()
    while parts and parts[-1].lower().strip(",.") in _PI_SUFFIX_TOKENS:
        parts.pop()
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def slugify_project_no(project_no: Optional[str]) -> str:
    if not project_no:
        raise RuntimeError("Missing project number; cannot build stable award ID.")
    slug = re.sub(r"[^a-z0-9]+", "-", project_no.lower()).strip("-")
    if not slug:
        raise RuntimeError(f"Project number did not yield an ID slug: {project_no!r}")
    return slug


def datatables_payload(length: int) -> dict[str, str]:
    payload = {
        "draw": "1",
        "start": "0",
        "length": str(length),
        "order[0][column]": "0",
        "order[0][dir]": "asc",
        "search[value]": "",
        "search[regex]": "false",
    }
    for col in range(9):
        payload[f"columns[{col}][data]"] = str(col)
        payload[f"columns[{col}][name]"] = ""
        payload[f"columns[{col}][searchable]"] = "true"
        payload[f"columns[{col}][orderable]"] = "true"
        payload[f"columns[{col}][search][value]"] = ""
        payload[f"columns[{col}][search][regex]"] = "false"
    return payload


def fetch_records(limit: Optional[int] = None) -> tuple[list[list[Any]], int]:
    print("\n" + "=" * 60)
    print("Step 1: Download HEC Pakistan NRPU project rows")
    print("=" * 60)

    length = limit if limit else -1
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PUBLIC_PAGE_URL,
    })

    print(f"  POST {API_URL} length={length}")
    resp = session.post(API_URL, data=datatables_payload(length), timeout=120)
    print(f"  HTTP {resp.status_code}; {len(resp.content):,} bytes")
    resp.raise_for_status()
    data = resp.json()

    total = int(data.get("iTotalRecords") or 0)
    rows = data.get("data") or []
    if not isinstance(rows, list):
        raise RuntimeError("DataTables response `data` was not a list.")
    print(f"  Endpoint reports {total:,} total rows; received {len(rows):,}")

    if not rows:
        raise RuntimeError("No NRPU rows returned from HEC DataTables endpoint.")
    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} records")
    elif len(rows) != total:
        raise RuntimeError(
            f"Full run returned {len(rows):,} rows, but endpoint reported {total:,}. "
            "Do not upload a partial corpus."
        )
    return rows, total


def normalize_records(raw_rows: list[list[Any]], *, total_reported: Optional[int], full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize source rows")
    print("=" * 60)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows, start=1):
        if len(raw) < 9:
            raise RuntimeError(f"Unexpected DataTables row shape at row {idx}: {raw!r}")

        display_name = extract_title(raw[0])
        pi_name = clean_text(raw[1])
        pi_given_name, pi_family_name = split_pi_name(pi_name)
        project_no = clean_text(raw[7])
        outcomes_url = extract_href(raw[8])
        funder_award_id = f"hec-pakistan-nrpu-{slugify_project_no(project_no)}"

        status = clean_text(raw[4])
        university = clean_text(raw[3])
        major_field = clean_text(raw[5])
        minor_field = clean_text(raw[6])
        description_parts = []
        if pi_name:
            description_parts.append(f"Principal investigator: {pi_name}")
        if university:
            description_parts.append(f"University: {university}")
        if status:
            description_parts.append(f"Status: {status}")
        if major_field:
            description_parts.append(f"Major field: {major_field}")
        if minor_field:
            description_parts.append(f"Minor field: {minor_field}")

        rows.append({
            "funder_award_id": funder_award_id,
            "project_number": project_no,
            "display_name": display_name,
            "description": "; ".join(description_parts) if description_parts else None,
            "pi_name": pi_name,
            "pi_given_name": pi_given_name,
            "pi_family_name": pi_family_name,
            "pi_email": clean_text(raw[2]),
            "university": university,
            "status": status,
            "major_field": major_field,
            "minor_field": minor_field,
            "funder_scheme": "National Research Program for Universities (NRPU)",
            "funding_type": "grant",
            "amount": None,
            "currency": None,
            "source_year": None,
            "start_date": None,
            "end_date": None,
            "landing_page_url": outcomes_url or PUBLIC_PAGE_URL,
            "outcomes_pdf_url": outcomes_url,
            "source_table_url": PUBLIC_PAGE_URL,
            "source_authority_url": HEC_SOURCE_PAGE_URL,
            "source_total_reported": str(total_reported) if total_reported is not None else None,
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("Normalized dataframe is empty.")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print(f"  Distinct funder_award_id values: {df['funder_award_id'].nunique():,}")

    for col in [
        "project_number",
        "display_name",
        "pi_name",
        "pi_email",
        "university",
        "status",
        "landing_page_url",
        "outcomes_pdf_url",
        "major_field",
        "minor_field",
        "amount",
        "currency",
        "source_year",
    ]:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100
        print(f"  {col:18s}: {covered:,}/{total:,} ({pct:.1f}%)")

    required_thresholds = {
        "project_number": 1.00,
        "display_name": 1.00,
        "university": 0.95,
        "status": 0.95,
        "pi_name": 0.95,
    }
    for col, threshold in required_thresholds.items():
        coverage = df[col].notna().mean()
        if coverage < threshold:
            raise RuntimeError(f"Unexpectedly low {col} coverage: {coverage * 100:.1f}%")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full HEC Pakistan run returned only {total:,} rows; expected at least "
            f"{EXPECTED_MIN_FULL_ROWS:,}."
        )

    years = pd.to_numeric(df["source_year"], errors="coerce").dropna()
    if years.empty:
        print("  Year range: NULL (source endpoint does not publish per-project award years)")
    else:
        print(f"  Year range: {int(years.min())}-{int(years.max())}")
    print(f"  Status distribution: {df['status'].value_counts(dropna=False).to_dict()}")
    print(f"  Major-field top 10: {df['major_field'].value_counts(dropna=False).head(10).to_dict()}")
    print("  Amount/currency: NULL for all rows; source does not publish per-project money.")


def write_outputs(raw_rows: list[list[Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "hec_pakistan_nrpu_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(raw_rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw DataTables rows to {raw_path}")

    parquet_path = output_dir / "hec_pakistan_nrpu.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_records(output_dir: Path) -> tuple[list[list[Any]], Optional[int]]:
    raw_path = output_dir / "hec_pakistan_nrpu_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of DataTables rows: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached DataTables rows from {raw_path}")
    return rows, None


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

    prev_path = output_dir / "_prev_hec_pakistan_nrpu.parquet"
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
            f"\n[ERROR] Refusing to shrink HEC Pakistan corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
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
        description="Download HEC Pakistan NRPU project rows and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/hec_pakistan"))
    parser.add_argument("--limit", type=int, default=None, help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse hec_pakistan_nrpu_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("HEC Pakistan NRPU grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  API:        {API_URL}")
    print(f"  Source:     {HEC_SOURCE_PAGE_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        raw_rows, total_reported = load_cached_records(args.output_dir)
        if args.limit:
            raw_rows = raw_rows[:args.limit]
            print(f"  [LIMIT] keeping first {len(raw_rows):,} cached records")
    else:
        raw_rows, total_reported = fetch_records(limit=args.limit)

    df = normalize_records(raw_rows, total_reported=total_reported, full_run=args.limit is None)
    parquet_path = write_outputs(raw_rows, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
