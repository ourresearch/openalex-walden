#!/usr/bin/env python3
"""
PCHRD ongoing funded projects -> S3 Data Pipeline
=================================================

Downloads the official Philippine Council for Health Research and Development
(DOST-PCHRD) funded-project listing from the council's WordPress REST API and
writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: PCHRD publishes "Funded Projects" / "Ongoing
Projects" on its own website, backed by public WordPress REST endpoints:

    https://www.pchrd.dost.gov.ph/wp-json/wp/v2/ongoing_projects
    https://www.pchrd.dost.gov.ph/wp-json/wp/v2/projects_category
    https://www.pchrd.dost.gov.ph/wp-json/wp/v2/implementing_agency

As of the 2026-05-28 probe, the `ongoing_projects` endpoint advertises 185
project records. Project detail pages do not expose additional fields beyond
the title, but REST taxonomy IDs resolve to official project categories and
implementing agencies.

Schema choices
--------------
Each row is one official ongoing PCHRD project. PCHRD does not expose project
budgets, project-start dates, PIs, or descriptions for nearly all rows. The
script preserves the official WordPress record publication date separately and
uses it as the source-observed date in the notebook; amount/currency are left
NULL under the runbook section 6.7 waiver.

Output
------
s3://openalex-ingest/awards/pchrd/pchrd_ongoing_projects.parquet

Usage
-----
    python pchrd_to_s3.py --skip-upload
    python pchrd_to_s3.py --limit 10 --skip-upload
    python pchrd_to_s3.py --skip-download --skip-upload
    python pchrd_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
import urllib3

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
        file, mode="r", buffering=-1, encoding=None, errors=None,
        newline=None, closefd=True, opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


SITE_BASE = "https://www.pchrd.dost.gov.ph"
API_BASE = f"{SITE_BASE}/wp-json/wp/v2"
PROJECT_ENDPOINT = f"{API_BASE}/ongoing_projects"
CATEGORY_ENDPOINT = f"{API_BASE}/projects_category"
AGENCY_ENDPOINT = f"{API_BASE}/implementing_agency"

# Awarding body: Philippine Council for Health Research and Development.
# Verified in OpenAlex as F4320335609 (PH; no DOI/ROR in public API).
FUNDER_ID = 4320335609
FUNDER_DISPLAY_NAME = "Philippine Council for Health Research and Development"
PROVENANCE = "pchrd_ongoing_projects"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/pchrd/pchrd_ongoing_projects.parquet"

USER_AGENT = "openalex-walden-pchrd-ingest/1.0 (+https://openalex.org)"
WP_PAGE_SIZE = 50
MIN_REQUEST_INTERVAL_S = 0.6
EXPECTED_MIN_FULL_ROWS = 150

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.match(r"^(\d{4}-\d{2}-\d{2})", str(value))
    return match.group(1) if match else None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
        })
        # The PCHRD server presents a certificate chain that local curl/Python
        # may not verify. We still require HTTPS and restrict requests to the
        # official PCHRD host; disabling verification avoids false local CA
        # failures while preserving source authority.
        _session.verify = False
    return _session


def http_get_json(url: str, *, params: Optional[dict[str, Any]] = None, max_attempts: int = 4) -> tuple[Any, requests.Response]:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        started = time.monotonic()
        try:
            resp = session.get(url, params=params, timeout=(8, 45), allow_redirects=True)
            _last_request_t = time.monotonic()
            elapsed_s = _last_request_t - started
            print(f"  GET {resp.url} -> {resp.status_code} ({len(resp.content):,} bytes, {elapsed_s:.1f}s)")
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"    [WARN] transient HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json(), resp
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_exc = exc
            print(f"    [WARN] request attempt {attempt}/{max_attempts} failed: {exc}")
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            time.sleep(sleep_s)
    raise RuntimeError(f"GET JSON failed after {max_attempts} attempts: {url}") from last_exc


def fetch_taxonomy(endpoint: str, label: str) -> dict[int, str]:
    print(f"  Fetch taxonomy: {label}")
    data, _ = http_get_json(endpoint, params={"per_page": 100})
    if not isinstance(data, list):
        raise RuntimeError(f"Expected list from taxonomy endpoint {endpoint}")
    out = {int(term["id"]): clean_text(term.get("name")) or "" for term in data}
    print(f"    {label}: {len(out):,} terms")
    return out


def fetch_projects(*, limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch official PCHRD WP REST data")
    print("=" * 60)
    categories = fetch_taxonomy(CATEGORY_ENDPOINT, "projects_category")
    agencies = fetch_taxonomy(AGENCY_ENDPOINT, "implementing_agency")

    projects: list[dict[str, Any]] = []
    total_pages: Optional[int] = None
    total_rows: Optional[int] = None
    page = 1
    while True:
        data, resp = http_get_json(PROJECT_ENDPOINT, params={"per_page": WP_PAGE_SIZE, "page": page})
        if not isinstance(data, list):
            raise RuntimeError("Expected list from ongoing_projects endpoint")
        if total_pages is None:
            total_pages = int(resp.headers.get("X-WP-TotalPages") or 0) or None
            total_rows = int(resp.headers.get("X-WP-Total") or 0) or None
            print(f"  advertised rows={total_rows}; pages={total_pages}")
        print(f"  page {page}: {len(data):,} rows")
        for item in data:
            wp_id = str(item.get("id") or "").strip()
            if not wp_id:
                continue
            cat_names = [categories.get(int(i)) for i in item.get("projects_category", []) if int(i) in categories]
            agency_names = [agencies.get(int(i)) for i in item.get("implementing_agency", []) if int(i) in agencies]
            source_date = normalize_date(item.get("date"))
            modified_date = normalize_date(item.get("modified"))
            projects.append({
                "funder_award_id": f"pchrd-{wp_id}",
                "wp_id": wp_id,
                "slug": clean_text(item.get("slug")),
                "display_name": clean_text((item.get("title") or {}).get("rendered")),
                "description": clean_text((item.get("content") or {}).get("rendered")),
                "project_category": cat_names[0] if cat_names else None,
                "project_categories": json.dumps(cat_names, ensure_ascii=False, sort_keys=True),
                "source_implementing_agency": agency_names[0] if agency_names else None,
                "implementing_agencies": json.dumps(agency_names, ensure_ascii=False, sort_keys=True),
                "source_posted_date": source_date,
                "source_modified_date": modified_date,
                "source_year": source_date[:4] if source_date else None,
                "landing_page_url": clean_text(item.get("link")),
                "source_api_url": resp.url,
                "amount": None,
                "currency": None,
                "funder_id": str(FUNDER_ID),
                "funder_display_name": FUNDER_DISPLAY_NAME,
                "provenance": PROVENANCE,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            })
            if limit is not None and len(projects) >= limit:
                print(f"  [LIMIT] keeping first {len(projects):,} rows")
                return projects
        if total_pages is not None and page >= total_pages:
            break
        if not data and total_pages is None:
            break
        page += 1

    if total_rows is not None and len(projects) != total_rows:
        print(f"  [WARN] fetched {len(projects):,} rows but endpoint advertised {total_rows:,}")
    return projects


def load_cached(output_dir: Path, *, limit: Optional[int]) -> pd.DataFrame:
    cache_path = output_dir / "pchrd_ongoing_projects_raw.json"
    if not cache_path.exists():
        raise FileNotFoundError(f"Missing cache file: {cache_path}")
    rows = json.loads(cache_path.read_text(encoding="utf-8"))
    if limit:
        rows = rows[:limit]
    return pd.DataFrame(rows)


def validate(df: pd.DataFrame, *, full_run: bool) -> None:
    print("\n" + "=" * 60)
    print("Validation")
    print("=" * 60)
    rows = len(df)
    print(f"  rows: {rows:,}")
    if rows == 0:
        raise RuntimeError("No rows to write")
    if full_run and rows < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(f"Full run fetched only {rows:,} rows; expected at least {EXPECTED_MIN_FULL_ROWS:,}")

    duplicate_ids = int(df["funder_award_id"].duplicated().sum())
    print(f"  duplicate funder_award_id values: {duplicate_ids:,}")
    if duplicate_ids:
        dupes = df.loc[df["funder_award_id"].duplicated(keep=False), "funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    for col in ["display_name", "project_category", "source_implementing_agency", "source_posted_date", "landing_page_url"]:
        coverage = float(df[col].notna().mean() * 100.0)
        print(f"  {col} coverage: {coverage:.1f}%")
    if df["display_name"].isna().any():
        raise RuntimeError("display_name coverage must be 100%")
    if df["project_category"].isna().any():
        raise RuntimeError("project_category coverage must be 100%")
    if full_run and float(df["source_implementing_agency"].isna().mean()) > 0.02:
        raise RuntimeError("More than 2% of rows are missing implementing agency")

    print("  amount/currency coverage: 0.0% (section 6.7 waiver; source does not publish per-project budgets)")
    print("  source posted years:")
    print(df["source_year"].value_counts(dropna=False).sort_index().to_string())
    print("  top categories:")
    print(df["project_category"].value_counts(dropna=False).head(12).to_string())
    print("  top implementing agencies:")
    print(df["source_implementing_agency"].value_counts(dropna=False).head(12).to_string())


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "pchrd_ongoing_projects_raw.json"
    cache_path.write_text(json.dumps(df.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")

    parquet_path = output_dir / "pchrd_ongoing_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    print(f"\nWrote {len(df):,} rows to {parquet_path}")
    return parquet_path


def check_no_shrink(parquet_path: Path, *, allow_shrink: bool) -> None:
    if allow_shrink:
        print("  [ALLOW-SHRINK] Skipping S3 shrink check by explicit request")
        return
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    tmp_path = parquet_path.with_name("existing_pchrd_ongoing_projects.parquet")
    print(f"  Checking existing S3 object for shrink guard: {s3_uri}")
    result = subprocess.run(["aws", "s3", "cp", s3_uri, str(tmp_path)], capture_output=True, text=True)
    if result.returncode != 0:
        stderr = result.stderr or ""
        if "404" in stderr or "NoSuchKey" in stderr or "Not Found" in stderr:
            print("  No existing S3 parquet found; shrink check passes for first upload")
            return
        raise RuntimeError(f"Could not fetch existing S3 parquet for shrink check:\n{stderr}")
    old_rows = len(pd.read_parquet(tmp_path))
    new_rows = len(pd.read_parquet(parquet_path))
    print(f"  existing rows: {old_rows:,}; new rows: {new_rows:,}")
    if new_rows < old_rows:
        raise RuntimeError(
            f"Refusing to upload shrinking corpus ({new_rows:,} < {old_rows:,}). "
            "Use --allow-shrink only after reviewing the source change."
        )


def upload_to_s3(parquet_path: Path, *, allow_shrink: bool) -> None:
    print("\n" + "=" * 60)
    print("S3 Upload")
    print("=" * 60)
    check_no_shrink(parquet_path, allow_shrink=allow_shrink)
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    print(f"Uploaded to {s3_uri}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download official PCHRD ongoing projects to parquet")
    parser.add_argument("--limit", type=int, help="Limit rows for smoke tests")
    parser.add_argument("--output-dir", default="data/pchrd", help="Local output/cache directory")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSON instead of downloading")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow a shrinking corpus during upload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    if args.skip_download:
        df = load_cached(output_dir, limit=args.limit)
    else:
        rows = fetch_projects(limit=args.limit)
        df = pd.DataFrame(rows)

    full_run = not args.limit
    validate(df, full_run=full_run)
    parquet_path = write_outputs(df, output_dir)

    if args.skip_upload:
        print("\nSkipping S3 upload (--skip-upload)")
    else:
        upload_to_s3(parquet_path, allow_shrink=args.allow_shrink)


if __name__ == "__main__":
    main()
