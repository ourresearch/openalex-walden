#!/usr/bin/env python3
"""
World Food Prize → S3 Data Pipeline (PRIZE PATTERN, decade-bucket scrape)
==========================================================================

Downloads the World Food Prize laureate roster from the foundation's
official site at `worldfoodprize.org/en/laureates/`. The prize is
awarded annually since 1987 for outstanding contributions to improving
global food security. Laureates include Norman Borlaug himself
(implicitly — he founded the prize and was the inaugural awardee in
1987 to M.S. Swaminathan after his Nobel), Muhammad Yunus (1994),
Pedro Sanchez (2002), the World Food Programme (2008 jointly to Dole
& McGovern), and Cary Fowler + Geoffrey Hawtin (2024 joint).

Source authority
----------------
`worldfoodprize.org` is the foundation's own site (Webflow / sitevizcms
CMS — first ingest on the project to scrape this CMS family). The
laureate corpus is organized into **4 decade-bucketed listing pages**
(method-5 static-HTML scrape, decade-bucket variant):

    /en/laureates/19871999_laureates/   → 14 laureates
    /en/laureates/20002009_laureates/   → 10 laureates
    /en/laureates/20102019_laureates/   → 10 laureates
    /en/laureates/20202026_laureates/   →  7 laureates
                                       → 41 total

Each decade page links to per-laureate detail pages at
`/en/laureates/{decade}_laureates/{year}_{slug}/` where the slug is
the surname(s) — joint awards use `surname_and_surname` (1996
Beachell+Khush) or `surname_surname_surname` (2006 Lobato+McClung+
Paolinelli; 2013 van Montagu+Chilton+Fraley).

Each detail page has the structure:

    <h1>YYYY: SURNAME</h1>                  ← year + last name(s)
    <h2>Full Name</h2>                       ← canonical display name
    <h2>Full Biography</h2>
    {bio paragraphs}

Country isn't a separate field on the foundation's site; we leave it
NULL and capture the biographical text as `description`.

Amount handling
---------------
The foundation's own laureates index page states verbatim: **"The
Laureate receives a cash prize of $500,000, recognizing their
outstanding contributions to improving global food security."**
(verified 2026-05-22 at worldfoodprize.org/en/laureates/). We ship
USD 500,000 uniformly across all years. §6.7 amount-coverage NOT
waived — 100% expected.

Output
------
s3://openalex-ingest/awards/world_food_prize/world_food_prize_laureates.parquet

Usage
-----
    python world_food_prize_to_s3.py                # full run (~41 laureates × ~0.4s = ~25 sec)
    python world_food_prize_to_s3.py --skip-upload  # local dev
    python world_food_prize_to_s3.py --skip-download
    python world_food_prize_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
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
from typing import Optional
from urllib.parse import urljoin

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

# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://www.worldfoodprize.org"
INDEX_URL = f"{BASE_URL}/en/laureates/"
# The 4 decade-bucket listing pages — discovered from /en/laureates/.
DECADE_URLS = [
    f"{BASE_URL}/en/laureates/19871999_laureates/",
    f"{BASE_URL}/en/laureates/20002009_laureates/",
    f"{BASE_URL}/en/laureates/20102019_laureates/",
    f"{BASE_URL}/en/laureates/20202026_laureates/",
]

# Awarding body — World Food Prize Foundation.
# Verified F4320308859, country US, DOI 10.13039/100005989, no ROR.
FUNDER_ID = 4320308859
FUNDER_DISPLAY_NAME = "World Food Prize Foundation"

PROVENANCE = "world_food_prize"
CURRENCY = "USD"
WORLD_FOOD_PRIZE_AMOUNT_USD = 500_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/world_food_prize/world_food_prize_laureates.parquet"

USER_AGENT = "openalex-walden-world-food-prize-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.4


# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html, application/xhtml+xml, */*; q=0.01",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: index + decade pages + sample detail reachable")
    print("=" * 60)
    # Check first decade page
    resp = _http_get(DECADE_URLS[0])
    resp.raise_for_status()
    urls = parse_decade_page(resp.text, DECADE_URLS[0])
    print(f"  {DECADE_URLS[0].rsplit('/',2)[-2]}: {len(urls)} laureate URLs")
    if len(urls) < 10:
        print(f"[ERROR] expected ~14 laureates on first decade, got {len(urls)}")
        sys.exit(3)
    # Sample one detail page
    first = urls[0]
    detail_resp = _http_get(first)
    detail_resp.raise_for_status()
    rec = parse_detail_page(detail_resp.text, first)
    print(f"  spot-check ({first.rsplit('/',2)[-2]}): year={rec['year']} "
          f"name={rec.get('display_name')!r}")


# =============================================================================
# Listing-page parsing (4 decade pages)
# =============================================================================

_LAUREATE_URL_RE = re.compile(
    r'href="(/en/laureates/\d+_laureates/\d{4}_[^"/]+/)"'
)


def parse_decade_page(html: str, page_url: str) -> list[str]:
    """Extract all laureate detail-page URLs from a decade listing."""
    seen: set[str] = set()
    out: list[str] = []
    for m in _LAUREATE_URL_RE.finditer(html):
        url = urljoin(page_url, m.group(1))
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


# =============================================================================
# Detail-page parsing
# =============================================================================

# <h1>YYYY: SURNAME</h1>
_H1_YEAR_SURNAME_RE = re.compile(
    r'<h1[^>]*>\s*(\d{4})\s*:\s*([^<]+)\s*</h1>',
    re.DOTALL,
)
# The h2 immediately following the h1 carries the full display name.
_FIRST_H2_RE = re.compile(
    r'</h1>\s*(?:<[^>]+>\s*)*<h2[^>]*>\s*([^<]+)\s*</h2>',
    re.DOTALL,
)
# Bio section: "Full Biography" h2 followed by paragraphs
_BIO_SECTION_RE = re.compile(
    r'<h2[^>]*>\s*Full Biography\s*</h2>(.+?)<h2',
    re.DOTALL,
)


def _strip_html(s: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip()


def parse_detail_page(html: str, url: str) -> dict:
    """Parse a per-laureate detail page into a record."""
    # Year + surname from URL slug (most reliable — h1 sometimes has odd formatting)
    slug_m = re.search(r'/(\d{4})_([^/]+)/$', url)
    year = int(slug_m.group(1)) if slug_m else None
    slug = slug_m.group(2) if slug_m else None
    # H1 confirms year + provides the surname-only display
    h1_m = _H1_YEAR_SURNAME_RE.search(html)
    h1_year = int(h1_m.group(1)) if h1_m else None
    h1_surname_part = _strip_html(h1_m.group(2)) if h1_m else None
    # Use URL year as the source of truth (h1 is often abbreviated; in 2008
    # joint-prize pages the h1 may say "2008: Dole and McGovern" — fine for
    # display)
    final_year = year or h1_year
    # First H2 after H1 = full display name
    h2_m = _FIRST_H2_RE.search(html)
    full_name = _strip_html(h2_m.group(1)) if h2_m else h1_surname_part
    # Bio
    bio_m = _BIO_SECTION_RE.search(html)
    bio = None
    if bio_m:
        bio = _strip_html(bio_m.group(1))
        # Trim to the first ~600 chars for description field
        if len(bio) > 800:
            bio = bio[:800].rsplit(" ", 1)[0] + "…"

    # Joint detection — h1 / display name contains " and " or multiple " " in surname.
    is_joint = bool(
        re.search(r'\s+and\s+', full_name or '', re.I)
        or re.search(r'\s+and\s+', h1_surname_part or '', re.I)
        or (slug and ("_and_" in slug or slug.count("_") > 1))
    )

    return {
        "year":              final_year,
        "slug":              slug,
        "h1_surname_block":  h1_surname_part,
        "display_name":      full_name,
        "is_joint":          is_joint,
        "bio":               bio,
        "landing_page_url":  url,
    }


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r'^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame|Sen\.?|Rep\.?|Hon\.?)\s+',
    re.I,
)
_JOINT_RE = re.compile(r'\s+(?:and|&)\s+', re.I)


def split_name(full_name: str) -> tuple[Optional[str], Optional[str]]:
    """Return (given_name, family_name) for the FIRST recipient of a joint
    award (co-laureate captured separately in bio/description)."""
    if not full_name:
        return None, None
    name = _JOINT_RE.split(full_name, maxsplit=1)[0]
    name = _HONORIFIC_PREFIXES_RE.sub("", name).strip()
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()
    toks = name.split()
    while toks and toks[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


# =============================================================================
# Download loop
# =============================================================================

def download_all(output_dir: Path) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download decade-bucket pages + per-laureate detail pages")
    print("=" * 60)
    # First, gather all laureate URLs from the 4 decade pages.
    all_urls: list[str] = []
    for d in DECADE_URLS:
        resp = _http_get(d)
        resp.raise_for_status()
        urls = parse_decade_page(resp.text, d)
        print(f"  {d.rsplit('/',2)[-2]}: {len(urls)} laureate URLs")
        all_urls.extend(urls)
    # De-duplicate
    all_urls = sorted(set(all_urls))
    print(f"\n  total unique laureate URLs: {len(all_urls)}")

    # Fetch each detail page
    rows: list[dict] = []
    for i, url in enumerate(all_urls):
        resp = _http_get(url)
        if resp.status_code != 200:
            print(f"  [WARN] {url} → HTTP {resp.status_code}; skipping")
            continue
        rec = parse_detail_page(resp.text, url)
        rows.append(rec)
        if (i + 1) % 10 == 0:
            print(f"  ... fetched {i+1}/{len(all_urls)}")
    print(f"\n  ✓ collected {len(rows)} laureate records")
    return rows


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw records")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for r in all_rows:
        year = r.get("year")
        slug = r.get("slug")
        if year is None or slug is None:
            continue
        funder_award_id = f"world-food-prize-{year}-{slug}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "World Food Prize (year, slug) should be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        full_name = r.get("display_name") or r.get("h1_surname_block") or ""
        given, family = split_name(full_name)

        display_name = f"World Food Prize {year} — {full_name}"
        description = r.get("bio")

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              year,
            "slug":              slug,
            "name":              full_name,
            "given_name":        given,
            "family_name":       family,
            "is_joint":          r.get("is_joint", False),
            "h1_surname_block":  r.get("h1_surname_block"),
            "display_name":      display_name,
            "description":       description,
            "amount":            WORLD_FOOD_PRIZE_AMOUNT_USD,
            "currency":          CURRENCY,
            "start_date":        f"{year}-01-01",
            "end_date":          f"{year}-12-31",
            "landing_page_url":  r.get("landing_page_url"),
            "declined":          False,
        })

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  joint laureates: {df['is_joint'].sum()}")
    print(f"  given_name coverage: {df['given_name'].notna().sum()}/{len(df)} "
          f"({df['given_name'].notna().sum()*100/len(df):.0f}%)")
    print(f"  description coverage: {df['description'].notna().sum()}/{len(df)} "
          f"({df['description'].notna().sum()*100/len(df):.0f}%)")

    # Runbook §1.2.5 — string before parquet
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "world_food_prize_laureates.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_world_food_prize.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"    [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(
            f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Investigate first."
        )
        return False
    print(f"    [OK] new corpus not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
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
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/wfp"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse world_food_prize_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("World Food Prize → S3 Pipeline (PRIZE PATTERN, decade-bucket scrape)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "world_food_prize_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        all_rows = download_all(args.output_dir)
        raw_path = args.output_dir / "world_food_prize_raw.json"
        raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
        print(f"\n  Cached {len(all_rows)} raw rows to {raw_path}")

    df = build_dataframe(all_rows)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateWorldFoodPrizeAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
