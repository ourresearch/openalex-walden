#!/usr/bin/env python3
"""
NOMIS Foundation Projects → S3 Pipeline (GRANT PATTERN, method-2 WordPress REST API)
====================================================================================

Downloads NOMIS Foundation research projects from the foundation's
own WordPress REST API at nomisfoundation.ch. Discovery method-2
(REST API) — the foundation runs a clean WordPress + custom post
types setup with these public endpoints:

  /wp-json/wp/v2/projects       101 funded research projects
  /wp-json/wp/v2/organization   99 partner institutions (taxonomy)
  /wp-json/wp/v2/person         207 PI / researcher persons
  /wp-json/wp/v2/award          12 NOMIS Distinguished Scientists award groups (annual cohorts 2016-2025)

This script ingests **projects** (the substantive grants). The award
records are program-level group announcements rather than per-laureate
rows and are intentionally out of scope here; a follow-up could ingest
them under a separate provenance if desired.

Awarding body in OpenAlex:
  NOMIS Stiftung (F4320325162, CH, no ROR, DOI 10.13039/501100008483).
  Swiss basic-research funder, supports projects in human behavior,
  cognitive science, biology, archaeology, philosophy of science.

Per-project schema (from wp/v2/projects + linked organization terms):

  - id, slug              stable WP IDs
  - title.rendered        project name
  - link                  canonical landing-page URL
  - content.rendered      multi-section HTML body containing
                          "The Question" / "The Approach" / etc.
  - organization          array of org-taxonomy term IDs (resolved to
                          institution names via /wp/v2/organization)
  - date                  project page creation date (NOT the award
                          start date — Nomis doesn't publish a structured
                          award date in the API)

Amount/currency handling:
  amount/currency are NULL with §6.7 waiver. NOMIS publishes
  program-level information about its funding philosophy on
  /how-we-give/ but does NOT publish per-project amount on either the
  REST API or the rendered project pages. Per the runbook's source-
  authority rule we don't backfill from external press releases or
  third-party databases — NULL is correct. Fellowship/research-grant
  precedent: HHMI #44, CIFAR #79, Damon Runyon #73, Packard #95,
  Rita Allen #107, Schmidt Sciences #108 (all NULL by design).

Output
------
  s3://openalex-ingest/awards/nomis/nomis_projects.parquet

Usage
-----
    python nomis_foundation_to_s3.py                                  # full run
    python nomis_foundation_to_s3.py --skip-upload                    # local dev
    python nomis_foundation_to_s3.py --limit 5                        # smoke
    python nomis_foundation_to_s3.py --skip-download --skip-upload    # reuse cache
    python nomis_foundation_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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

BASE_URL = "https://nomisfoundation.ch/wp-json/wp/v2"
PROJECTS_URL = f"{BASE_URL}/projects"
ORGS_URL = f"{BASE_URL}/organization"

FUNDER_ID = 4320325162
FUNDER_DISPLAY_NAME = "NOMIS Stiftung"

PROVENANCE = "nomis_projects"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nomis/nomis_projects.parquet"

USER_AGENT = "openalex-walden-nomis-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/nomis_projects.json")

PER_PAGE = 100  # WordPress REST max


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get_json(url: str, timeout: int = 30):
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.json()


def fetch_paginated(endpoint: str, label: str, use_cache: bool, cache_key: str, cache: dict) -> list[dict]:
    """Fetch all items from a paginated WP REST endpoint."""
    if use_cache and cache_key in cache:
        log(f"  cache hit: {label} ({len(cache[cache_key])} items)")
        return cache[cache_key]

    out = []
    page = 1
    while True:
        url = f"{endpoint}?per_page={PER_PAGE}&page={page}"
        log(f"  GET {label} page {page}")
        try:
            batch = _http_get_json(url)
        except requests.HTTPError as e:
            # WP REST returns 400 when paging past the last page; treat as terminator
            if e.response.status_code == 400 and page > 1:
                log(f"    end of pages ({label}) at page {page}")
                break
            raise
        if not batch:
            break
        out.extend(batch)
        log(f"    +{len(batch)} (running total {len(out)})")
        if len(batch) < PER_PAGE:
            break
        page += 1
    cache[cache_key] = out
    return out


def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


# =============================================================================
# Parse + content cleanup
# =============================================================================

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
YEAR_RE = re.compile(r"\b(19[5-9]\d|20[0-3]\d)\b")


def strip_html(html: str) -> str:
    """Strip tags; collapse whitespace."""
    if not html:
        return ""
    import html as _html
    text = HTML_TAG_RE.sub(" ", html)
    text = _html.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def extract_year_from_text(text: str, project_year: Optional[int]) -> Optional[int]:
    """Find the earliest plausible award year mentioned in the project content.
    Falls back to the page-creation year if no in-text year is found."""
    if not text:
        return project_year
    years = [int(m.group(1)) for m in YEAR_RE.finditer(text[:3000])]
    # Project content often mentions "starting in 2024" / "the 2023 award" /
    # "began in 2022". Take the earliest year >= 2000 that is plausibly the
    # award year. If everything is too old (pre-2000), fall back.
    candidates = [y for y in years if 2000 <= y <= 2030]
    if candidates:
        return min(candidates)
    return project_year


def parse_project(p: dict, org_lookup: dict[int, dict]) -> dict:
    title = strip_html(p.get("title", {}).get("rendered", ""))
    content_html = p.get("content", {}).get("rendered", "")
    content_text = strip_html(content_html)
    description = content_text[:5000] if content_text else None
    page_date = p.get("date") or ""
    page_year = int(page_date[:4]) if page_date[:4].isdigit() else None
    award_year = extract_year_from_text(content_text, page_year)

    org_ids = p.get("organization") or []
    org_names = [org_lookup.get(oid, {}).get("name") for oid in org_ids if oid in org_lookup]
    org_names = [n for n in org_names if n]
    org_slugs = [org_lookup.get(oid, {}).get("slug") for oid in org_ids if oid in org_lookup]
    org_slugs = [s for s in org_slugs if s]

    return {
        "project_id":     str(p.get("id")),
        "slug":           p.get("slug"),
        "title":          title,
        "link":           p.get("link"),
        "page_date":      page_date,
        "page_year":      page_year,
        "award_year":     award_year,
        "description":    description,
        "organization_names": json.dumps(org_names, ensure_ascii=False) if org_names else None,
        "organization_slugs": json.dumps(org_slugs, ensure_ascii=False) if org_slugs else None,
        "primary_organization": org_names[0] if org_names else None,
        "status":         p.get("status"),
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No project rows parsed")
    n = len(rows)
    for f in ("title", "slug", "description", "primary_organization", "award_year"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # funder_award_id uniqueness = slug (Nomis slugs are stable per WordPress)
    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {len(slugs) - len(set(slugs))}: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "nomis-" + df["slug"]
    # Runbook §1.2.5: string dtype before to_parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NOMIS Foundation projects → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                        help="Output directory; parquet written to {output-dir}/nomis_projects.parquet")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Path to cache the fetched JSON")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse cached JSON")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but skip S3 upload")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook §1.4 shrink-check")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N projects (smoke)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "nomis_projects.parquet"

    log(f"=== NOMIS projects ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    log("Fetching organization taxonomy...")
    orgs = fetch_paginated(ORGS_URL, "organization", args.skip_download, "organizations", cache)
    org_lookup = {o["id"]: o for o in orgs}
    log(f"  {len(org_lookup)} organizations indexed")
    save_cache(args.cache, cache)

    log("Fetching projects...")
    projects = fetch_paginated(PROJECTS_URL, "projects", args.skip_download, "projects", cache)
    log(f"  {len(projects)} projects fetched")
    save_cache(args.cache, cache)

    if args.limit is not None:
        projects = projects[:args.limit]
        log(f"--limit {args.limit}: truncating to {len(projects)} projects")

    log("Parsing rows...")
    rows = [parse_project(p, org_lookup) for p in projects]
    rows = [r for r in rows if r.get("title")]
    log(f"  {len(rows)} parsed rows")

    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")

    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== NOMIS projects ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== NOMIS projects ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
