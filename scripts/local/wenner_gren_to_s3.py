#!/usr/bin/env python3
"""
Wenner-Gren Foundation Grantees → S3 Pipeline (GRANT/FELLOWSHIP PATTERN, method-5 static HTML)
=============================================================================================

Downloads Wenner-Gren Foundation grantees from the foundation's own
WordPress-published grantee archive at wennergren.org. The
Wenner-Gren Foundation is a major US-based anthropology funder
(Hunt Postdoctoral Fellowships, Wadsworth International Fellowships,
Engaged Anthropology Grants, Post-PhD Research Grants, etc.).

Discovery (method-5 static HTML): three sitemap files enumerate the
full corpus.

  /grantee-sitemap.xml      1,000 grantee URLs
  /grantee-sitemap2.xml       995 grantee URLs
  /grantee-sitemap3.xml       166 grantee URLs
  TOTAL                     2,161 grantee URLs

Each grantee page (~23KB) is server-rendered with a tidy
`<div class="c-grantee-block">` repeating block structure. Per
grantee the page exposes:

  Grant Type               e.g., "Wadsworth International Fellowship"
  Institutional Affiliation e.g., "Philippines, U. of the"
  Grant number             e.g., "Gr. WIF-294"
  Approve Date             e.g., "May 1, 2023"
  Project Title            full project description prose

The grantee's display_name comes from the page's H1 (uppercased on
the site; we preserve the original case from the title-tag or H1
text and let SQL render later).

Awarding body in OpenAlex:
  Wenner-Gren Foundation (F4320306550, US, no ROR, DOI 10.13039/100001388).

Amount handling:
  amount/currency are NULL with §6.7 waiver. Wenner-Gren publishes
  program-level fixed amounts on its /funding-opportunities/ pages
  (Hunt Postdoc: $42k, Wadsworth: $25k/yr × 2y, Engaged Anthropology:
  up to $5k) but does NOT publish per-grantee amount on the grantee
  archive. Per the runbook's source-authority rule we don't backfill
  from program-level narrative. Grant/fellowship-pattern precedent
  for NULL amount: HHMI #44, CIFAR #79, Damon Runyon #73, Packard #95,
  Rita Allen #107, Schmidt Sciences #108, NOMIS #109.

Output
------
  s3://openalex-ingest/awards/wenner_gren/wenner_gren_grantees.parquet

Usage
-----
    python wenner_gren_to_s3.py                                  # full run (~10 min @ 0.3s throttle)
    python wenner_gren_to_s3.py --skip-upload                    # local dev
    python wenner_gren_to_s3.py --limit 10                       # smoke
    python wenner_gren_to_s3.py --skip-download --skip-upload    # reuse cache
    python wenner_gren_to_s3.py --allow-shrink                   # override §1.4

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

SITEMAP_URLS = [
    "https://wennergren.org/grantee-sitemap.xml",
    "https://wennergren.org/grantee-sitemap2.xml",
    "https://wennergren.org/grantee-sitemap3.xml",
]

FUNDER_ID = 4320306550
FUNDER_DISPLAY_NAME = "Wenner-Gren Foundation"

PROVENANCE = "wenner_gren_grantees"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wenner_gren/wenner_gren_grantees.parquet"

USER_AGENT = "openalex-walden-wenner-gren-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/wenner_gren_pages.json")

# Map "Approve Date" text format to a date parser.
APPROVE_DATE_FORMATS = [
    "%B %d, %Y",         # "May 1, 2023"
    "%b %d, %Y",         # "May 1, 2023" alt
    "%Y",                # bare year
]


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "unknown"


# =============================================================================
# Name split (canonical helper per runbook §2.4.1)
# =============================================================================

_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    tokens = name.split()
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

def discover_grantee_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    urls = []
    for sm_url in SITEMAP_URLS:
        xml = get_page(sm_url, cache, use_cache)
        for m in re.finditer(r"<loc>(https?://[^<]+/grantee/[^<]+)</loc>", xml):
            urls.append(m.group(1))
    log(f"  enumerated {len(urls)} grantee URLs across {len(SITEMAP_URLS)} sitemaps")
    return urls


# Title-case-ify "MINH THI TRAN" → "Minh Thi Tran" only if all-caps; otherwise keep
def normalize_name(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if raw == raw.upper() and len(raw) > 2:
        return " ".join(part.capitalize() for part in raw.split())
    return raw


def parse_approve_date(s: Optional[str]) -> tuple[Optional[str], Optional[int]]:
    """Return (ISO-date-string, year). Parses "May 1, 2023" or bare year."""
    if not s:
        return None, None
    s = s.strip()
    from datetime import datetime
    for fmt in APPROVE_DATE_FORMATS:
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime("%Y-%m-%d"), d.year
        except ValueError:
            continue
    m = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    if m:
        return f"{m.group(1)}-01-01", int(m.group(1))
    return None, None


def parse_grantee_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Name from H1 (or fallback to og:title)
    h1 = soup.find("h1")
    raw_name = h1.get_text(strip=True) if h1 else ""
    if not raw_name:
        og = soup.find("meta", property="og:title")
        raw_name = og.get("content", "").strip() if og else ""
    name = normalize_name(raw_name)
    if not name:
        return None

    # Slug from URL
    m = re.search(r"/grantee/([^/]+)/?", url)
    slug = m.group(1) if m else slugify(name)

    # Iterate c-grantee-block divs: heading + value
    fields = {}
    for block in soup.find_all("div", class_="c-grantee-block"):
        heading_el = block.find(class_="c-grantee-block__heading")
        if not heading_el:
            continue
        heading = heading_el.get_text(strip=True)
        # Value is the remainder of the block text after removing the heading
        value = block.get_text(" ", strip=True)
        if value.startswith(heading):
            value = value[len(heading):].strip()
        fields[heading] = value

    grant_type     = fields.get("Grant Type")
    affiliation    = fields.get("Institutional Affiliation")
    grant_number   = fields.get("Grant number") or fields.get("Grant Number")
    approve_date_s = fields.get("Approve Date") or fields.get("Approve date")
    project_title  = fields.get("Project Title") or fields.get("Project title")

    parsed_date, approve_year = parse_approve_date(approve_date_s)

    given_name, family_name = split_name(name)

    return {
        "grantee_name":     name,
        "given_name":       given_name,
        "family_name":      family_name,
        "slug":             slug,
        "grant_type":       grant_type,
        "affiliation":      affiliation,
        "grant_number":     grant_number,
        "approve_date_raw": approve_date_s,
        "approve_date_iso": parsed_date,
        "approve_year":     approve_year,
        "project_title":    project_title,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grantee rows parsed")
    n = len(rows)
    for f in ("grantee_name", "slug", "grant_type", "affiliation", "grant_number", "approve_year", "project_title"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # Funder-award-id uniqueness — prefer grant_number, fallback to slug.
    # The grant_number IS the native award ID ("Gr. WIF-294") and is canonical.
    keys = []
    for r in rows:
        gn = (r.get("grant_number") or "").strip()
        if gn:
            keys.append(slugify(gn))
        else:
            keys.append(r["slug"])
    if len(keys) != len(set(keys)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(keys).items() if v > 1][:5]
        log(f"  WARNING: grant_number uniqueness imperfect: {len(keys) - len(set(keys))} duplicates "
            f"(example: {dups}). Falling back to slug-disambiguation in build_dataframe.")
    else:
        log(f"  grant_number uniqueness: {len(keys)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # funder_award_id = slugified grant_number when present, else slug. If a
    # grant_number repeats across rows (rare but possible for multi-year grants),
    # disambiguate by appending the page slug.
    def make_id(row):
        gn = (row.get("grant_number") or "").strip()
        if gn:
            return f"wg-{slugify(gn)}-{row['slug']}"
        return f"wg-{row['slug']}"
    df["funder_award_id"] = df.apply(make_id, axis=1)
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
    parser = argparse.ArgumentParser(description="Fetch Wenner-Gren Foundation grantees → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                        help="Output directory; parquet written to {output-dir}/wenner_gren_grantees.parquet")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Path to cache the fetched page HTML")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse cached pages where available")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but skip S3 upload")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook §1.4 shrink-check")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N grantees (smoke test)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "wenner_gren_grantees.parquet"

    log(f"=== Wenner-Gren grantees ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)
    log(f"  loaded {len(cache)} cached pages")

    urls = discover_grantee_urls(args.skip_download, cache)
    save_cache(args.cache, cache)

    if args.limit is not None:
        urls = urls[:args.limit]
        log(f"--limit {args.limit}: processing {len(urls)} URLs")

    rows = []
    save_every = 50  # persist cache periodically during long runs
    for i, url in enumerate(urls, 1):
        try:
            html = get_page(url, cache, args.skip_download)
        except Exception as e:
            log(f"  [{i}/{len(urls)}] FETCH FAIL {url}: {type(e).__name__}: {e}")
            continue
        row = parse_grantee_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}: no name")
            continue
        rows.append(row)
        if i % 100 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total so far: {len(rows)})")
        if i % save_every == 0:
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} grantee rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Wenner-Gren grantees ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Wenner-Gren grantees ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
