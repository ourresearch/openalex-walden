#!/usr/bin/env python3
"""
TrygFonden donations -> S3 Data Pipeline
========================================

Downloads donation records from TrygFonden's official public project
database on tryghed.dk and writes a parquet file for the OpenAlex awards
pipeline.

Source authority
----------------
The source is first-party: TrygFonden/Tryghed's public Projects and Donations
site exposes a same-origin JSON endpoint used by the page component:

    https://tryghed.dk/api/donations?templateName=Donation

The official donation sitemap corroborates the corpus size:

    https://tryghed.dk/sitemap-donations.xml

Validation on 2026-05-26:
  - API TotalSearchResults: 8,824 donation records
  - Sitemap donation URLs: 8,824
  - Unique SharepointId values: 8,824
  - 100% coverage: Title, Institution, Council, PublishedYear, Type,
    TargetArea, Url
  - 99.8% amount coverage; all amounts are in Danish kroner (DKK)

Important parser note
---------------------
Do not scrape the detail page H1 as the project title. The static page chrome
currently exposes an unrelated outer H1 on donation detail pages. The official
API fields and the detail overlay carry the correct Title, Institution, Amount,
PublishedYear, Council, Type, TargetArea, FocusArea, Description, and Url.

Output
------
s3://openalex-ingest/awards/trygfonden/trygfonden_donations.parquet

Usage
-----
    python trygfonden_to_s3.py --skip-upload
    python trygfonden_to_s3.py --limit 10 --skip-upload
    python trygfonden_to_s3.py --skip-download --skip-upload
    python trygfonden_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import json
import math
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. Production runs on Linux/Databricks where UTF-8 is the default,
# but this keeps local validation portable for names and descriptions with
# Danish characters.
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


# =============================================================================
# Configuration
# =============================================================================

API_URL = "https://tryghed.dk/api/donations"
SITE_BASE_URL = "https://tryghed.dk"
DONATION_SITEMAP_URL = "https://tryghed.dk/sitemap-donations.xml"
PAGE_SIZE = 1000
MIN_REQUEST_INTERVAL_S = 0.25

# Awarding body: TrygFonden.
# Verified in OpenAlex as F4320324424 (DK).
FUNDER_ID = 4320324424
FUNDER_DISPLAY_NAME = "TrygFonden"
PROVENANCE = "trygfonden_donations_api"
CURRENCY = "DKK"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/trygfonden/trygfonden_donations.parquet"

USER_AGENT = "openalex-walden-trygfonden-ingest/1.0 (+https://openalex.org)"


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://tryghed.dk/saadan-stoetter-vi/projekter-og-donationer",
        })
    return _session


def polite_get(url: str, *, params: Optional[dict[str, Any]] = None,
               timeout: int = 90, max_attempts: int = 4) -> requests.Response:
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


def fetch_api_page(page: int, page_size: int) -> dict[str, Any]:
    params = {
        "templateName": "Donation",
        "page": page,
        "pageSize": page_size,
    }
    resp = polite_get(API_URL, params=params)
    return resp.json()


def fetch_donation_records(limit: Optional[int] = None) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download TrygFonden donation records")
    print("=" * 60)

    page_size = min(PAGE_SIZE, limit) if limit else PAGE_SIZE
    first = fetch_api_page(page=1, page_size=page_size)
    total = int(first.get("TotalSearchResults") or 0)
    page_count = int(first.get("PageCount") or math.ceil(total / page_size))
    hits = list(first.get("Hits") or [])
    print(f"  API reports {total:,} donation records across {page_count:,} pages")
    print(f"  Page 1: {len(hits):,} records")

    if limit:
        target_pages = min(page_count, math.ceil(limit / page_size))
    else:
        target_pages = page_count

    for page in range(2, target_pages + 1):
        data = fetch_api_page(page=page, page_size=page_size)
        page_hits = list(data.get("Hits") or [])
        hits.extend(page_hits)
        print(f"  Page {page}: {len(page_hits):,} records (running {len(hits):,})")

    if limit:
        hits = hits[:limit]
        print(f"  [LIMIT] keeping first {len(hits):,} records")

    if not hits:
        raise RuntimeError("No donation records returned from TrygFonden API")
    if not limit and len(hits) != total:
        raise RuntimeError(
            f"Full API run returned {len(hits):,} records, but API reported {total:,}. "
            "Do not upload a partial corpus."
        )
    return hits


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def as_int_string(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return clean_text(value)


def json_string(value: Any) -> Optional[str]:
    if value in (None, "", [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def normalize_records(records: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize records")
    print("=" * 60)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for rec in records:
        sharepoint_id = rec.get("SharepointId")
        if sharepoint_id in (None, ""):
            raise ValueError(f"Record missing SharepointId: {rec!r}")
        published_year = rec.get("PublishedYear")
        year_text = as_int_string(published_year)
        amount = rec.get("Amount")
        amount_text = None
        if amount not in (None, ""):
            amount_text = str(float(amount))
        source_path = clean_text(rec.get("Url"))
        rows.append({
            "funder_award_id": f"trygfonden-{sharepoint_id}",
            "sharepoint_id": as_int_string(sharepoint_id),
            "source_id": clean_text(rec.get("Id")),
            "display_name": clean_text(rec.get("Title")),
            "recipient_name": clean_text(rec.get("Institution")),
            "council": clean_text(rec.get("Council")),
            "amount": amount_text,
            "currency": CURRENCY if amount_text is not None else None,
            "published_year": year_text,
            "start_date": f"{year_text}-01-01" if year_text else None,
            "end_date": f"{year_text}-12-31" if year_text else None,
            "type": clean_text(rec.get("Type")),
            "target_area": clean_text(rec.get("TargetArea")),
            "focus_area": clean_text(rec.get("FocusArea")),
            "description": clean_text(rec.get("Description")),
            "target_group": clean_text(rec.get("TargetGroup")),
            "result": clean_text(rec.get("Result")),
            "effect": clean_text(rec.get("Effect")),
            "anchoring": clean_text(rec.get("Anchoring")),
            "key_learning": clean_text(rec.get("KeyLearning")),
            "knowledge_impact": clean_text(rec.get("KnowledgeImpact")),
            "special_pool": clean_text(rec.get("SpecialPool")),
            "children_and_youth": as_int_string(rec.get("ChildrenAndYouth")),
            "adults": as_int_string(rec.get("Adults")),
            "seniors": as_int_string(rec.get("Seniors")),
            "latitude": clean_text(rec.get("Latitude")),
            "longitude": clean_text(rec.get("Longitude")),
            "taxonomy_css": clean_text(rec.get("TaxonomyCss")),
            "taxonomy_image_url": clean_text(rec.get("TaxonomyImageUrl")),
            "is_evaluated": clean_text(rec.get("IsEvaluated")),
            "evaluation_page_url": (
                urljoin(SITE_BASE_URL, clean_text(rec.get("EvaluationPageUrl")))
                if clean_text(rec.get("EvaluationPageUrl")) else None
            ),
            "feedback_json": json_string(rec.get("Feedback")),
            "landing_page_url": urljoin(SITE_BASE_URL, source_path) if source_path else None,
            "source_url_path": source_path,
            "template_name": clean_text(rec.get("TemplateName")),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)

    # Keep the write-site safety property even though all source strings are
    # normalized above. This prevents pyarrow type drift on sparse columns.
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    required_cols = ["display_name", "recipient_name", "published_year", "type", "target_area", "landing_page_url"]
    for col in required_cols:
        coverage = df[col].notna().mean() if total else 0
        print(f"  {col:18s}: {df[col].notna().sum():,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    amount_coverage = df["amount"].notna().mean() if total else 0
    print(f"  {'amount':18s}: {df['amount'].notna().sum():,}/{total:,} ({amount_coverage * 100:.1f}%)")
    if amount_coverage < 0.90:
        raise RuntimeError(f"Unexpectedly low amount coverage: {amount_coverage * 100:.1f}%")

    if full_run and total < 8500:
        raise RuntimeError(
            f"Full TrygFonden run returned only {total:,} rows; expected at least 8,500."
        )
    if not full_run and total >= 100 and total < 8500:
        print(f"  [WARN] limited run has {total:,} rows; do not treat this as a full corpus.")
    elif total >= 8500:
        print(f"  [OK] full corpus size looks plausible: {total:,} rows")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  Total DKK amount: {amount_numeric.sum():,.0f}")
    print(f"  Year range: {df['published_year'].min()} - {df['published_year'].max()}")
    print(f"  Type distribution: {df['type'].value_counts(dropna=False).to_dict()}")
    print(f"  Council distribution: {df['council'].value_counts(dropna=False).to_dict()}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "trygfonden_donations_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw API response hits to {raw_path}")

    parquet_path = output_dir / "trygfonden_donations.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "trygfonden_donations_raw.json"
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

    prev_path = output_dir / "_prev_trygfonden_donations.parquet"
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
            f"\n[ERROR] Refusing to shrink TrygFonden corpus "
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
        description="Download TrygFonden donations and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/trygfonden"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse trygfonden_donations_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("TrygFonden donations -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  API:        {API_URL}?templateName=Donation")
    print(f"  Sitemap:    {DONATION_SITEMAP_URL}")
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
        records = fetch_donation_records(limit=args.limit)

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
