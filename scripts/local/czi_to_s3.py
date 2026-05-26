#!/usr/bin/env python3
"""
Chan Zuckerberg Initiative (CZI) Grants → S3 Pipeline (GRANT PATTERN, method-2 WordPress REST + method-5 sitemap)
=================================================================================================================

Downloads CZI grant records from the foundation's own WordPress at
chanzuckerberg.com. CZI funds major scientific initiatives in
biomedicine, education, and open science, including the Human Cell
Atlas, Essential Open Source Software for Science (EOSS), Imaging,
Neurodegenerative Disease Networks, Pediatric Disease, etc.

Discovery (method-5 static HTML, WordPress YOAST sub-sitemaps):

  /czi_eoss-sitemap.xml            195 EOSS grants (Open Source Software)
  /czi_hca-sitemap.xml             123 Human Cell Atlas grants
  /czi_ncn-sitemap.xml             133 Neurodegenerative Networks grants
  /czi_imaging-sitemap.xml          74 Imaging grants
  /czi_frontiers-sitemap.xml        76 Frontiers of Imaging grants
  /czi_data_insights-sitemap.xml    55 Data Insights grants
  /czi_los-sitemap.xml              57 Listening to Our Stakeholders
  /czi_seed_networks-sitemap.xml    38 Seed Networks
  /czi_rfa-sitemap.xml              32 RFA-specific grants
  /czi_single_cell-sitemap.xml      29 Single Cell Biology
  /czi_pediatric-sitemap.xml        18 Pediatric Disease
  /czi_rao-sitemap.xml              17 Rare As One
  /czi_ecb-sitemap.xml               6 Education Capacity Building
  /czi_sharing-sitemap.xml           6 Sharing/Open Science
  /czi_cop-sitemap.xml               6 Communities of Practice
  /czi_other_grants-sitemap.xml      5 Other Grants
  TOTAL                            870 grant URLs

Program slug ↔ user-facing program name mapping is encoded in
PROGRAM_LABEL below. The URL prefix (`/eoss/proposals/`, `/hca/`,
`/imaging/`, etc.) identifies which program the grant belongs to.

Each grant page (~52KB) renders:

  <h1>{Project Title}</h1>
  "Project | {Project Name} | Funding Cycle | {N} | Proposal Summary | ..."
  "Key Personnel | {PI Name} | Affiliation | {Institution(s)} | GitHub Handle | {handle}"

Plus JSON-LD `WebPage` with name + datePublished.

Awarding body in OpenAlex:
  Chan Zuckerberg Initiative (F4320315474, US,
  DOI 10.13039/100014989).

Amount handling:
  amount/currency are NULL with §6.7 waiver. CZI publishes program-
  level budget envelopes on its /science/programs-resources/ pages
  (e.g., EOSS cycles allocate ~$1M total per cycle split across
  proposals) but does NOT publish per-proposal amount on the
  individual grant pages. Grant/research-funder precedent for NULL
  amount: HHMI #44, CIFAR #79, Damon Runyon #73, Packard #95,
  Rita Allen #107, Schmidt Sciences #108, NOMIS #109, Wenner-Gren
  #110, Mercator #116, Fritz Thyssen #117.

Output
------
  s3://openalex-ingest/awards/czi/czi_grants.parquet

Usage
-----
    python czi_to_s3.py                                  # full run (~5 min @ 0.3s)
    python czi_to_s3.py --skip-upload                    # local dev
    python czi_to_s3.py --limit 10                       # smoke
    python czi_to_s3.py --skip-download --skip-upload    # reuse cache
    python czi_to_s3.py --allow-shrink                   # override §1.4

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
from bs4 import BeautifulSoup

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

SUB_SITEMAPS = [
    "https://chanzuckerberg.com/czi_eoss-sitemap.xml",
    "https://chanzuckerberg.com/czi_ecb-sitemap.xml",
    "https://chanzuckerberg.com/czi_sharing-sitemap.xml",
    "https://chanzuckerberg.com/czi_cop-sitemap.xml",
    "https://chanzuckerberg.com/czi_other_grants-sitemap.xml",
    "https://chanzuckerberg.com/czi_hca-sitemap.xml",
    "https://chanzuckerberg.com/czi_los-sitemap.xml",
    "https://chanzuckerberg.com/czi_ncn-sitemap.xml",
    "https://chanzuckerberg.com/czi_rao-sitemap.xml",
    "https://chanzuckerberg.com/czi_imaging-sitemap.xml",
    "https://chanzuckerberg.com/czi_frontiers-sitemap.xml",
    "https://chanzuckerberg.com/czi_data_insights-sitemap.xml",
    "https://chanzuckerberg.com/czi_pediatric-sitemap.xml",
    "https://chanzuckerberg.com/czi_rfa-sitemap.xml",
    "https://chanzuckerberg.com/czi_seed_networks-sitemap.xml",
    "https://chanzuckerberg.com/czi_single_cell-sitemap.xml",
]

# Program label by sub-sitemap basename
PROGRAM_LABEL = {
    "czi_eoss": "Essential Open Source Software for Science (EOSS)",
    "czi_ecb": "Education Capacity Building",
    "czi_sharing": "Open Science Sharing",
    "czi_cop": "Communities of Practice",
    "czi_other_grants": "Other Grants",
    "czi_hca": "Human Cell Atlas",
    "czi_los": "Listening to Our Stakeholders",
    "czi_ncn": "Neurodegenerative Disease Networks",
    "czi_rao": "Rare As One",
    "czi_imaging": "Imaging",
    "czi_frontiers": "Frontiers of Imaging",
    "czi_data_insights": "Data Insights",
    "czi_pediatric": "Pediatric Disease",
    "czi_rfa": "RFA",
    "czi_seed_networks": "Seed Networks",
    "czi_single_cell": "Single Cell Biology",
}

FUNDER_ID = 4320315474
FUNDER_DISPLAY_NAME = "Chan Zuckerberg Initiative"

PROVENANCE = "czi_grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/czi/czi_grants.parquet"

USER_AGENT = "openalex-walden-czi-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/czi_pages.json")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    tokens = re.split(r"\s+", name.strip())
    while tokens and tokens[-1].lower().strip(",.") in _SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> str:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.text


def load_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def get_page(url: str, cache: dict[str, str], use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _http_get(url)
    cache[url] = html
    return html


# =============================================================================
# Discovery + parse
# =============================================================================

def discover_grant_urls(use_cache: bool, cache: dict[str, str]) -> list[tuple[str, str]]:
    """Return [(program_key, url), ...] across all CZI sub-sitemaps.
    The program_key is the sub-sitemap basename (e.g., 'czi_eoss')."""
    out = []
    for sm_url in SUB_SITEMAPS:
        prog_key = sm_url.rsplit("/", 1)[-1].replace("-sitemap.xml", "")
        try:
            xml = get_page(sm_url, cache, use_cache)
        except Exception as e:
            log(f"  sub-sitemap {prog_key} fetch fail: {type(e).__name__}: {e}")
            continue
        urls = [m.group(1) for m in re.finditer(r"<loc>([^<]+)</loc>", xml)]
        for u in urls:
            out.append((prog_key, u))
    log(f"  enumerated {len(out)} grant URLs across {len(SUB_SITEMAPS)} sub-sitemaps")
    return out


# Patterns for parsing the structured prose
KEY_PERSONNEL_RE = re.compile(r"Key Personnel\s*\|?(.+?)(?=Sign Up|Sorry,|We use cookies|$)", re.IGNORECASE | re.DOTALL)
PROPOSAL_SUMMARY_RE = re.compile(r"Proposal Summary\s*\|?\s*([^|]+?)\s*(?:\||$)", re.IGNORECASE)
PROJECT_FIELD_RE = re.compile(r"Project\s*\|?\s*([^|]+?)\s*(?:\||$)")
FUNDING_CYCLE_RE = re.compile(r"Funding Cycle\s*\|?\s*([^|]+?)\s*(?:\||$)", re.IGNORECASE)


def parse_grant_page(html: str, url: str, program_key: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        return None

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # JSON-LD for datePublished + canonical description
    description = None
    published = None
    for m in re.finditer(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            d = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        for item in (d.get("@graph") if isinstance(d, dict) and "@graph" in d else [d]):
            if isinstance(item, dict) and item.get("@type") == "WebPage":
                description = item.get("description") or description
                published = item.get("datePublished") or published

    # Strip chrome before scraping prose
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'svg']):
        tag.decompose()
    main = soup.find('main') or soup
    plain = main.get_text(" | ", strip=True)

    # Find structured fields
    project_name = None
    m = PROJECT_FIELD_RE.search(plain)
    if m:
        project_name = m.group(1).strip()
        if project_name == title or len(project_name) > 200:
            project_name = None

    funding_cycle = None
    m = FUNDING_CYCLE_RE.search(plain)
    if m:
        funding_cycle = m.group(1).strip()

    proposal_summary = None
    m = PROPOSAL_SUMMARY_RE.search(plain)
    if m:
        proposal_summary = m.group(1).strip()
        if len(proposal_summary) < 30:
            proposal_summary = None

    # Key Personnel block
    key_personnel_raw = None
    m = KEY_PERSONNEL_RE.search(plain)
    if m:
        kp = m.group(1).strip().rstrip("|").strip()
        if len(kp) > 5:
            key_personnel_raw = kp[:2000]  # cap length

    # Parse first PI: pattern "Name | Affiliation | {Institution} | ..."
    pi_name = None
    pi_affiliation = None
    if key_personnel_raw:
        # First segment before "Affiliation"
        parts = re.split(r"\bAffiliation\b", key_personnel_raw, maxsplit=1)
        candidate_name = parts[0].rstrip("|").strip()
        # Strip trailing pipe-segments
        candidate_name = candidate_name.split("|")[0].strip()
        if 2 < len(candidate_name) < 100:
            pi_name = candidate_name
        if len(parts) > 1:
            # Affiliation is the next pipe-segment after the "Affiliation" label
            aff_text = parts[1].strip().lstrip("|").strip()
            aff_first = aff_text.split("|")[0].strip()
            if aff_first and 2 < len(aff_first) < 300:
                pi_affiliation = aff_first

    pi_given, pi_family = split_name(pi_name)

    # Year from datePublished
    year = None
    if published and len(published) >= 4 and published[:4].isdigit():
        year = int(published[:4])

    return {
        "slug":               slug,
        "program_key":        program_key,
        "program_label":      PROGRAM_LABEL.get(program_key, program_key),
        "title":              title,
        "project_name":       project_name,
        "funding_cycle":      funding_cycle,
        "proposal_summary":   proposal_summary,
        "description":        description or proposal_summary,
        "pi_raw":             pi_name,
        "pi_given_name":      pi_given,
        "pi_family_name":     pi_family,
        "pi_affiliation":     pi_affiliation,
        "key_personnel_raw":  key_personnel_raw,
        "datePublished":      published,
        "start_year":         year,
        "landing_page_url":   url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "slug", "program_label", "pi_raw", "pi_affiliation", "start_year",
              "proposal_summary", "description"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # slug is unique across programs in CZI's URL space
    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        log(f"  WARNING: slug collisions across programs: {dups}")
    else:
        log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "czi-" + df["program_key"] + "-" + df["slug"]
    df = df.astype("string")
    return df


# =============================================================================
# Shrink + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3, io
        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_count = len(pd.read_parquet(io.BytesIO(prev_bytes)))
        log(f"  §1.4 shrink-check: previous parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch CZI grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "czi_grants.parquet"

    log("=== CZI grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")

    cache = load_cache(args.cache)
    urls = discover_grant_urls(args.skip_download, cache)
    save_cache(args.cache, cache)

    if args.limit is not None:
        urls = urls[:args.limit]
        log(f"--limit {args.limit}: processing {len(urls)} URLs")

    rows = []
    for i, (program_key, url) in enumerate(urls, 1):
        try:
            html = get_page(url, cache, args.skip_download)
        except Exception as e:
            log(f"  [{i}/{len(urls)}] FETCH FAIL {url}: {type(e).__name__}: {e}")
            continue
        row = parse_grant_page(html, url, program_key)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 100 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== CZI ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== CZI ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
