#!/usr/bin/env python3
"""
Rockefeller Foundation to S3 Data Pipeline
===========================================

Two-phase ingest:
  Phase 1: paginate the WP REST endpoint
           https://www.rockefellerfoundation.org/wp-json/wp/v2/grant
           to enumerate all grant URLs, slugs, titles, and taxonomy
           term IDs (awardyears, grant_regions). The WP API exposes
           ~1,511 records but the ACF block is empty in the JSON.
  Phase 2: fetch each landing page and parse the labelled
           div.grant_amount / div.grant_term / div.grant_initiative /
           div.grant_description blocks for the actual amount, term,
           focus area, and description.

Currency is hardcoded to USD (Rockefeller is US-based; landing pages
display amounts as "$N").

Output: s3://openalex-ingest/awards/rockefeller/rockefeller_projects.parquet
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

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

API_BASE = "https://www.rockefellerfoundation.org/wp-json/wp/v2"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rockefeller/rockefeller_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (+https://openalex.org)",
}
PER_PAGE = 100
REQUEST_DELAY = 0.3
RETRIES = 3
TIMEOUT = 30


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_with_retries(url: str, params: dict | None = None) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed: {url} ({last_err})")


def fetch_taxonomy(term: str) -> dict[int, str]:
    """Fetch all terms in a custom taxonomy and return id -> name mapping."""
    out: dict[int, str] = {}
    page = 1
    while True:
        r = fetch_with_retries(f"{API_BASE}/{term}", params={"per_page": PER_PAGE, "page": page})
        for t in r.json():
            out[t["id"]] = t["name"]
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
    return out


def list_grants() -> list[dict]:
    """Paginate WP REST grants endpoint to get URL list and taxonomy refs."""
    out: list[dict] = []
    page = 1
    while True:
        r = fetch_with_retries(
            f"{API_BASE}/grant",
            params={"per_page": PER_PAGE, "page": page,
                    "_fields": "id,slug,date,modified,link,title,grant_commitments,grant_regions,awardyears"},
        )
        out.extend(r.json())
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page == 1:
            log(f"WP REST reports {r.headers.get('X-WP-Total')} grants across {total_pages} pages")
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


# Landing-page parsing -------------------------------------------------------

# Each labelled section is `div.grant_<name>.data_box` containing an <h2>Label</h2>
# followed by either <span>value</span> or trailing text. We collect everything
# inside the div EXCEPT the h2.

PARSE_LABELS = {
    "grant_amount": "amount_raw",
    "grant_term": "term_raw",
    "grant_initiative": "focus_area_raw",
    "grant_description": "description_raw",
    "grant_grantee": "grantee_raw",
    "grant_partner": "partner_raw",
}


def parse_landing_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    out: dict = {}
    for cls, field in PARSE_LABELS.items():
        el = soup.find("div", class_=cls)
        if not el:
            out[field] = None
            continue
        # remove the label h2
        for h2 in el.find_all("h2"):
            h2.decompose()
        text = el.get_text(" ", strip=True)
        out[field] = re.sub(r"\s+", " ", text).strip() or None
    return out


def parse_amount(amount_raw: str | None) -> float | None:
    """'$750,000' or '$1.5M' or '$1,500,000' -> float USD."""
    if not amount_raw:
        return None
    m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*([KMB])?", amount_raw)
    if not m:
        return None
    num = float(m.group(1).replace(",", ""))
    suffix = (m.group(2) or "").upper()
    if suffix == "K":
        num *= 1_000
    elif suffix == "M":
        num *= 1_000_000
    elif suffix == "B":
        num *= 1_000_000_000
    return num


def parse_term(term_raw: str | None) -> tuple[str | None, str | None]:
    """'03.31.2026 - 09.30.2027' -> ('2026-03-31', '2027-09-30')."""
    if not term_raw:
        return None, None
    # MM.DD.YYYY style
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s*[-–—to]+\s*(\d{1,2})\.(\d{1,2})\.(\d{4})", term_raw)
    if m:
        sm, sd, sy, em, ed, ey = m.groups()
        return f"{sy}-{int(sm):02d}-{int(sd):02d}", f"{ey}-{int(em):02d}-{int(ed):02d}"
    # Year-only fallback: '2024 - 2026'
    m = re.search(r"(\d{4})\s*[-–—to]+\s*(\d{4})", term_raw)
    if m:
        return f"{m.group(1)}-01-01", f"{m.group(2)}-12-31"
    return None, None


def main() -> None:
    p = argparse.ArgumentParser(description="Rockefeller -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None,
                   help="For smoke-test: process only first N grants from list")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Rockefeller -> S3 pipeline starting")

    log("Phase 0: fetch taxonomy mappings")
    awardyears_map = fetch_taxonomy("awardyears")
    grant_regions_map = fetch_taxonomy("grant_regions")
    log(f"  awardyears: {len(awardyears_map)} terms ({sorted(awardyears_map.values())[:5]}...)")
    log(f"  grant_regions: {len(grant_regions_map)} terms ({sorted(grant_regions_map.values())})")

    log("Phase 1: list all grants via WP REST")
    grants = list_grants()
    log(f"  fetched {len(grants):,} grants")
    if args.limit:
        grants = grants[: args.limit]
        log(f"  smoke-test mode: limited to first {len(grants)} grants")

    log("Phase 2: fetch landing pages and extract details")
    rows: list[dict] = []
    for i, g in enumerate(grants, 1):
        try:
            r = fetch_with_retries(g["link"])
            parsed = parse_landing_page(r.text)
            amount = parse_amount(parsed["amount_raw"])
            start_iso, end_iso = parse_term(parsed["term_raw"])
            year_terms = [awardyears_map.get(tid) for tid in (g.get("awardyears") or []) if tid in awardyears_map]
            region_terms = [grant_regions_map.get(tid) for tid in (g.get("grant_regions") or []) if tid in grant_regions_map]
            rows.append({
                "wp_post_id": g.get("id"),
                "slug": g.get("slug"),
                "url": g.get("link"),
                "wp_date": g.get("date"),
                "wp_modified": g.get("modified"),
                "title": (g.get("title") or {}).get("rendered") or "",
                # Source-fidelity raw fields
                "amount_raw": parsed["amount_raw"],
                "term_raw": parsed["term_raw"],
                "focus_area_raw": parsed["focus_area_raw"],
                "description_raw": parsed["description_raw"],
                "grantee_raw": parsed["grantee_raw"],
                "partner_raw": parsed["partner_raw"],
                # Parsed
                "amount_usd": amount,
                "currency": "USD",
                "start_date": start_iso,
                "end_date": end_iso,
                "award_years": ",".join(sorted(set(y for y in year_terms if y))) or None,
                "regions": ",".join(sorted(set(r for r in region_terms if r))) or None,
                "downloaded_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            log(f"  [{i}/{len(grants)}] FAILED {g.get('link')}: {e}")
            continue
        if i == 1 or i % 50 == 0 or i == len(grants):
            log(f"  [{i}/{len(grants)}] cumulative ok")
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage — title: {df.title.astype(bool).sum()}, "
        f"amount_usd: {df.amount_usd.notna().sum()}, "
        f"start_date: {df.start_date.notna().sum()}")

    # Force string dtype on all columns before to_parquet. pyarrow otherwise
    # infers all-null columns (e.g. description_raw / grantee_raw / partner_raw
    # when no landing-page DOM block matches) as int, which breaks the notebook's
    # NULLIF(...) string ops. amount_usd stays a parseable number string and is
    # recovered via TRY_CAST(... AS DOUBLE) in CreateRockefellerAwards.
    df = df.astype("string")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "rockefeller_projects.parquet"
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
    # Suppress urllib3 SSL warnings — Rockefeller's site sometimes has cert issues
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
