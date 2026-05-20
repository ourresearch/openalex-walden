#!/usr/bin/env python3
"""
Damon Runyon Cancer Research Foundation to S3 Data Pipeline
============================================================

Downloads Damon Runyon fellow profiles from damonrunyon.org (Drupal CMS)
via the public sitemap + per-page field-label extraction.

The site is Drupal with consistent `class="field field-name-field-X"`
wrappers around each metadata field. The sitemap at
`https://www.damonrunyon.org/sitemap/sitemap.xml` exposes 859 scientist
profile URLs as of 2026-05-20. Each profile carries:
  - field-name-field-institution     → host institution
  - field-name-field-sponsor-mentor  → mentor / sponsor name (PI's senior advisor)
  - field-name-field-award-program   → e.g. "Damon Runyon Fellowship Award"
  - field-name-field-award-type      → e.g. "Postdoctoral Fellowship"
  - field-name-field-cancer-type     → cancer specialty (e.g. "Breast Cancer")
  - field-name-field-research-area   → research area (e.g. "Genomics")

Source authority
----------------
damonrunyon.org is the awarding body's own site (Drupal, public, no
auth, no rate limits documented). No Wikipedia/Wikidata.

Amount and currency
-------------------
Damon Runyon does NOT publicly disclose per-fellow funding amounts on
their site (only the headline program-level numbers, e.g. "$300K over
three years for postdoctoral fellows"). `amount` and `currency` ship
as NULL by design — same waiver as HHMI (priority 44) and CIFAR.

Output
------
s3://openalex-ingest/awards/damon_runyon/damon_runyon_fellows.parquet

Usage
-----
    python damon_runyon_to_s3.py                    # full run
    python damon_runyon_to_s3.py --skip-upload      # local dev
    python damon_runyon_to_s3.py --skip-download    # reuse cached JSON
    python damon_runyon_to_s3.py --limit 25         # smoke test
    python damon_runyon_to_s3.py --allow-shrink     # override §1.4

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

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

SITE_BASE = "https://www.damonrunyon.org"
SITEMAP_URL = f"{SITE_BASE}/sitemap/sitemap.xml"

# Awarding body — Damon Runyon Cancer Research Foundation.
# Verified F4320306271, country US.
FUNDER_ID = 4320306271
FUNDER_DISPLAY_NAME = "Damon Runyon Cancer Research Foundation"

PROVENANCE = "damon_runyon_drupal"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/damon_runyon/damon_runyon_fellows.parquet"

USER_AGENT = "openalex-walden-damonrunyon-ingest/1.0 (+https://openalex.org)"

# Polite throttle — 2 req/sec for 859 pages = ~7 min wall-clock.
MIN_REQUEST_INTERVAL_S = 0.5


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Smoke test (auth-free; just confirms sitemap + one page parse)
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: sitemap reachable + one profile parses")
    print("=" * 60)
    r = _http_get(SITEMAP_URL)
    r.raise_for_status()
    n = r.text.count("<loc>")
    print(f"  sitemap loc count: {n}")
    if n < 500 or n > 5000:
        print(f"  [WARN] sitemap entry count {n} outside 500-5000 expected range")
    # Sample one profile
    urls = extract_scientist_urls(r.text)
    if not urls:
        print("[ERROR] sitemap returned 0 scientist URLs — selector changed?")
        sys.exit(3)
    print(f"  {len(urls)} scientist profile URLs")
    r2 = _http_get(urls[0])
    fields = parse_scientist_page(r2.text)
    print(f"  sample {urls[0]}: extracted {len(fields)} fields")
    if "name" not in fields:
        print(f"  [WARN] sample page didn't yield a 'name' field — check parser")


# =============================================================================
# Sitemap + page parsing
# =============================================================================

_LOC_RE = re.compile(r'<loc>([^<]+)</loc>')
_SCIENTIST_URL_RE = re.compile(r'^https://www\.damonrunyon\.org/scientists/[a-z0-9-]+/?$')


def extract_scientist_urls(sitemap_xml: str) -> list[str]:
    """Pull only /scientists/{slug} URLs from the sitemap."""
    out = []
    for u in _LOC_RE.findall(sitemap_xml):
        if _SCIENTIST_URL_RE.match(u.strip()):
            out.append(u.strip())
    # Dedup, preserve order
    seen = set()
    uniq = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


# Drupal field markup:
#   <div class="field field-name-field-X field-type-Y ...">
#       <div class="field_X_label">Label</div>
#       <div class="field_X">VALUE_TEXT</div>   (or <a>VALUE</a>)
#   </div>
# We capture the inner field value div by name and strip tags.
def _field_value(html: str, field_name: str) -> Optional[str]:
    """Extract the visible text of a Drupal field by short field name."""
    # Match the inner div (not the *_label one)
    pat = re.compile(
        rf'<div class="field_{re.escape(field_name)}"[^>]*>(.+?)</div>',
        re.DOTALL,
    )
    m = pat.search(html)
    if not m:
        return None
    inner = m.group(1)
    text = re.sub(r'<[^>]+>', ' ', inner)
    text = unescape(re.sub(r'\s+', ' ', text)).strip()
    return text or None


# Name lives in separate first-name / last-name input-like spans on the page header.
_NAME_FIRST_RE = re.compile(r'class="field text first-name[^"]*"[^>]*>([^<]+)<', re.IGNORECASE)
_NAME_LAST_RE = re.compile(r'class="field text last-name[^"]*"[^>]*>([^<]+)<', re.IGNORECASE)
# Also try Drupal node title (h1 inside content area).
_TITLE_H1_RE = re.compile(r'<h1[^>]*>\s*([^<]+?)\s*</h1>', re.IGNORECASE)


def parse_scientist_page(html: str) -> dict:
    """Pull Damon Runyon field values from one rendered Drupal page."""
    rec: dict = {}
    # Name
    first = _NAME_FIRST_RE.search(html)
    last = _NAME_LAST_RE.search(html)
    if first and last:
        rec["given_name"] = unescape(first.group(1).strip())
        rec["family_name"] = unescape(last.group(1).strip())
        rec["name"] = f"{rec['given_name']} {rec['family_name']}".strip()
    else:
        # Fallback: first <h1> (typically the scientist name on Drupal node pages)
        h1 = _TITLE_H1_RE.search(html)
        if h1:
            full = unescape(h1.group(1).strip())
            rec["name"] = full
            parts = full.split()
            if parts:
                rec["family_name"] = parts[-1]
                rec["given_name"] = " ".join(parts[:-1]) if len(parts) > 1 else ""
    # Drupal fields
    rec["institution"]    = _field_value(html, "institution")
    rec["sponsor_mentor"] = _field_value(html, "sponsor_mentor")
    rec["award_program"]  = _field_value(html, "award_program")
    rec["award_type"]     = _field_value(html, "award_type")
    rec["cancer_type"]    = _field_value(html, "cancer_type")
    rec["research_area"]  = _field_value(html, "research_area")
    return rec


# =============================================================================
# Download
# =============================================================================

def download_scientists(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Download sitemap + scrape scientist profiles")
    print("=" * 60)
    r = _http_get(SITEMAP_URL)
    r.raise_for_status()
    urls = extract_scientist_urls(r.text)
    print(f"  {len(urls)} scientist URLs")
    if limit:
        urls = urls[:limit]
        print(f"  [LIMIT] only scraping first {limit}")
    out: list[dict] = []
    for i, url in enumerate(urls, 1):
        try:
            page = _http_get(url)
            if page.status_code != 200:
                print(f"  [{i}/{len(urls)}] {url} -> HTTP {page.status_code}; "
                      f"emitting URL-only row")
                rec = {}
            else:
                rec = parse_scientist_page(page.text)
        except Exception as e:
            print(f"  [{i}/{len(urls)}] {url} -> error {e}")
            rec = {}
        # Slug from URL
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        rec["url"] = url
        rec["slug"] = slug
        out.append(rec)
        if i % 50 == 0 or i == len(urls):
            n_inst = sum(1 for r in out if r.get("institution"))
            n_prog = sum(1 for r in out if r.get("award_program"))
            n_nm = sum(1 for r in out if r.get("name"))
            print(f"  [{i}/{len(urls)}] coverage so far: "
                  f"name={n_nm} institution={n_inst} program={n_prog}")
    raw_path = output_dir / "damon_runyon_raw.json"
    raw_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(out)} records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

# Conservative name-split, same idiom as Holberg/CIFAR/Nuffield.
_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Sr.", "II", "III", "IV", "MPH", "MSc"}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    # Trim trailing degree suffixes
    while tokens and tokens[-1].rstrip(".,") in {s.rstrip(".,") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        slug = r.get("slug")
        if not slug:
            continue
        funder_award_id = f"damon-runyon-{slug}"
        if funder_award_id in seen_ids:
            raise RuntimeError(
                f"Duplicate funder_award_id {funder_award_id!r}. Slugs "
                f"should be unique per scientist — investigate raw payload."
            )
        seen_ids.add(funder_award_id)
        # Prefer scraped given/family; fall back to split_name on `name`.
        given = r.get("given_name") or ""
        family = r.get("family_name") or ""
        if (not given or not family) and r.get("name"):
            g, fam = split_name(r["name"])
            given = given or g
            family = family or fam
        rows.append({
            "funder_award_id":     funder_award_id,
            "slug":                slug,
            "scientist_full_name": r.get("name"),
            "given_name":          given or None,
            "family_name":         family or None,
            "institution":         r.get("institution"),
            "sponsor_mentor":      r.get("sponsor_mentor"),
            "award_program":       r.get("award_program"),
            "award_type":          r.get("award_type"),
            "cancer_type":         r.get("cancer_type"),
            "research_area":       r.get("research_area"),
            "landing_page_url":    r["url"],
            "declined":            False,  # schema parity; no declined Damon Runyon fellows on record
        })
    df = pd.DataFrame.from_records(rows)
    n_nm = df["scientist_full_name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    n_prog = df["award_program"].astype(bool).sum()
    n_mentor = df["sponsor_mentor"].astype(bool).sum()
    print(f"  rows: {len(df)}  name={n_nm} ({n_nm*100/len(df):.0f}%) "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"program={n_prog} ({n_prog*100/len(df):.0f}%) "
          f"mentor={n_mentor} ({n_mentor*100/len(df):.0f}%)")
    print(f"\n  By award_program (top 10):")
    print(df.groupby("award_program").size().sort_values(ascending=False).head(10).to_string())
    # Runbook §1.2.5 — string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "damon_runyon_fellows.parquet"
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
    prev_path = output_dir / "_prev_damon_runyon_fellows.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/damon"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse damon_runyon_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape first N scientist profiles (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Damon Runyon Cancer Research Foundation → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "damon_runyon_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_scientists(args.output_dir, args.limit)

    df = build_dataframe(raw_path)
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
    print(f"Next: notebooks/awards/CreateDamonRunyonAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
