#!/usr/bin/env python3
"""
John Templeton Foundation to S3 Data Pipeline
==============================================

Templeton publishes its full grants database via the WordPress REST API at
https://www.templeton.org/wp-json/wp/v2/grants — including a custom ACF block
with grant_amount (USD), grant_start_date, grant_end_date, grant_grantee, and
grant_project_leader. ~5,956 grants total.

Output: s3://openalex-ingest/awards/templeton/templeton_projects.parquet
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

API = "https://www.templeton.org/wp-json/wp/v2/grants"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/templeton/templeton_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (+https://openalex.org)",
}
PER_PAGE = 100  # WP REST max
REQUEST_DELAY = 0.3
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_page(page: int) -> list[dict]:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(
                API,
                params={"per_page": PER_PAGE, "page": page, "_fields": "id,slug,date,modified,link,title,acf"},
                headers=HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Page {page} failed after {RETRIES} retries: {last_err}")


def get_total_pages() -> tuple[int, int]:
    r = requests.get(API, params={"per_page": PER_PAGE, "page": 1}, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return int(r.headers.get("X-WP-Total", 0)), int(r.headers.get("X-WP-TotalPages", 0))


def normalise_record(g: dict) -> dict:
    acf = g.get("acf") or {}
    title = (g.get("title") or {}).get("rendered") or ""
    return {
        # WP record identifiers
        "wp_post_id": g.get("id"),
        "slug": g.get("slug"),
        "url": g.get("link"),
        "wp_date": g.get("date"),
        "wp_modified": g.get("modified"),
        # Title — fall back to ACF field if WP title is empty
        "title": title or (acf.get("grant_web_title") or ""),
        # Templeton-specific ACF block (preserve source field names)
        "grant_id": acf.get("grant_id"),
        "grant_web_title": acf.get("grant_web_title"),
        "grant_max_content": acf.get("grant_max_content"),
        "grant_content": acf.get("grant_content"),
        "grant_project_leader": acf.get("grant_project_leader"),
        "grant_grantee": acf.get("grant_grantee"),
        # Amount stored as a string (e.g. "245005") — keep raw, parse in notebook
        "grant_amount_raw": acf.get("grant_amount"),
        "grant_start_date": acf.get("grant_start_date"),
        "grant_end_date": acf.get("grant_end_date"),
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Templeton WP REST -> parquet -> S3")
    p.add_argument("--limit-pages", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Templeton -> S3 pipeline starting")

    total_grants, total_pages = get_total_pages()
    log(f"Templeton reports {total_grants} grants across {total_pages} pages (per_page={PER_PAGE})")

    if args.limit_pages:
        total_pages = min(total_pages, args.limit_pages)
        log(f"Smoke-test mode: capped at {total_pages} pages")

    rows: list[dict] = []
    for page in range(1, total_pages + 1):
        grants = fetch_page(page)
        for g in grants:
            rows.append(normalise_record(g))
        if page == 1 or page % 10 == 0 or page == total_pages:
            log(f"[page {page}/{total_pages}] running total: {len(rows):,}")
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage — title: {df.title.astype(bool).sum()}, "
        f"grant_amount_raw: {df.grant_amount_raw.notna().sum()}, "
        f"grant_start_date: {df.grant_start_date.notna().sum()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "templeton_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
