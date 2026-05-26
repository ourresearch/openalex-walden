#!/usr/bin/env python3
"""
Riksbankens Jubileumsfond (RJ) Grants → S3 Pipeline (GRANT PATTERN, method-5 static HTML)
==========================================================================================

Downloads grant records from RJ's own Drupal-published archive at
rj.se. Riksbankens Jubileumsfond (the Jubilee Fund of the Bank of
Sweden) is Sweden's leading humanities and social-sciences research
funder, awarding ~SEK 600-700M/year since 1965.

Discovery (method-5 static HTML, Drupal Simple XML Sitemap):

  /sitemap.xml                          4,083 total URLs
  /en/grants/{YEAR}/{slug}/             1,676 English grant pages
  /bidrag/{YEAR}/{slug}/                1,918 Swedish grant pages

We scrape the **English** versions for canonical English-language
metadata. The Swedish-only grants (older than 2018 in some cases)
are NOT covered by this MVP; a follow-up could merge `/bidrag/`
URLs that lack an EN counterpart.

Each EN grant page has clean labeled fields in
`<div class="contentBox grantInfoBox">`:

  Grant administrator    institution name (e.g., "Språkrådet")
  Reference number       native award ID (e.g., "RMP20-0015")
  Amount                 SEK with comma separators (e.g., "SEK 976,000")
  Funding                program name (e.g., "RJ Flexit")
  Subject                topic taxonomy
  Year                   decision year

The h1 is the project title. The PI's name appears as the next
paragraph after the h1.

Awarding body in OpenAlex:
  Riksbankens Jubileumsfond (F4320322659, SE, DOI 10.13039/501100004472).

Amount handling:
  amount/currency populated from the "Amount" labeled field.
  Currency = SEK (RJ funds Swedish-currency only). NOT a global
  §6.7 waiver — RJ publishes per-grant amounts structurally.
  Expected coverage ~95%+ based on the structured-field pattern.

Output
------
  s3://openalex-ingest/awards/rj/rj_grants.parquet

Usage
-----
    python rj_jubileumsfond_to_s3.py                                  # full run (~8 min @ 0.3s)
    python rj_jubileumsfond_to_s3.py --skip-upload                    # local dev
    python rj_jubileumsfond_to_s3.py --limit 10                       # smoke
    python rj_jubileumsfond_to_s3.py --skip-download --skip-upload    # reuse cache
    python rj_jubileumsfond_to_s3.py --allow-shrink                   # override §1.4

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

SITEMAP_URL = "https://www.rj.se/sitemap.xml"

FUNDER_ID = 4320322659
FUNDER_DISPLAY_NAME = "Riksbankens Jubileumsfond"

PROVENANCE = "rj_jubileumsfond_grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rj/rj_grants.parquet"

USER_AGENT = "openalex-walden-rj-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/rj_pages.json")

# Match /en/grants/YEAR/slug — restrict to year-pathed URLs (skip /en/grants
# index pages)
EN_GRANT_RE = re.compile(r"^https://www\.rj\.se/en/grants/\d{4}/[^/]+/?$")

# Amount parser: "SEK 976,000" → 976000.0
AMOUNT_RE = re.compile(r"([0-9][0-9,\s]*)\s*(SEK|kr)?", re.IGNORECASE)


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                  "prof.", "dr.", "prof", "dr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    tokens = re.split(r"\s+", name.strip())
    while tokens and tokens[0].lower().strip(",.") in _SUFFIX_TOKENS:
        tokens.pop(0)
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

def discover_grant_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    xml = get_page(SITEMAP_URL, cache, use_cache)
    urls = [m.group(1) for m in re.finditer(r"<loc>([^<]+)</loc>", xml)
            if EN_GRANT_RE.match(m.group(1))]
    # dedup
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} EN grant URLs from sitemap")
    return out


def parse_amount(text: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Parse "SEK 976,000" -> (976000.0, 'SEK'). Returns (amount, currency)."""
    if not text:
        return None, None
    m = AMOUNT_RE.search(text)
    if not m:
        return None, None
    raw = m.group(1).replace(",", "").replace(" ", "")
    try:
        val = float(raw)
    except ValueError:
        return None, None
    # Currency: RJ always SEK
    return val, "SEK"


def parse_grant_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        return None

    # URL slug: /en/grants/YYYY/{slug}/
    parts = url.rstrip("/").split("/")
    year_in_url = parts[-2] if len(parts) >= 2 else None
    slug = parts[-1]
    try:
        year_from_url = int(year_in_url) if year_in_url and year_in_url.isdigit() else None
    except ValueError:
        year_from_url = None

    # PI name: it's the first paragraph or h2/h3 right after the h1.
    # Pattern: "Project Title" then "PI Name" then description body.
    pi_raw = None
    if h1:
        nxt = h1.find_next_sibling()
        # The next sibling is sometimes a wrapper div; walk into it for the name
        # Look for the first short text element (< 80 chars, no commas at the start) after h1
        for el in h1.find_all_next(["p", "h2", "h3", "span", "div"], limit=10):
            txt = el.get_text(strip=True)
            if txt and 2 < len(txt) < 80 and "," not in txt[:20]:
                # Skip if it looks like a navigation/grant info label
                if any(s in txt.lower() for s in ["grant administrator", "reference number", "amount",
                                                   "funding", "subject", "year", "grants", "main menu",
                                                   "english", "svenska"]):
                    continue
                # Assume this is the PI name if it has at least one space (first + last)
                if " " in txt and not any(c in txt for c in (":", "|", "/")):
                    pi_raw = txt
                    break

    # grantInfoBox: <span class="contentBoxHeader">LABEL</span><div class="content">VALUE</div>
    fields: dict[str, str] = {}
    box = soup.find("div", class_="grantInfoBox")
    if box:
        headers = box.find_all("span", class_="contentBoxHeader")
        values = box.find_all("div", class_="content")
        # Walk pair-wise; the box renders header-value-header-value...
        for h_el in headers:
            label = h_el.get_text(strip=True)
            # The matching value is the next-sibling div.content
            v_el = h_el.find_next_sibling("div", class_="content")
            if v_el:
                fields[label] = v_el.get_text(strip=True)

    grant_admin = fields.get("Grant administrator")
    reference_number = fields.get("Reference number")
    amount_raw = fields.get("Amount")
    funding_program = fields.get("Funding")
    subject = fields.get("Subject")
    year_field = fields.get("Year")

    amount, currency = parse_amount(amount_raw)

    try:
        award_year = int(year_field) if year_field and year_field.isdigit() else year_from_url
    except (ValueError, TypeError):
        award_year = year_from_url

    # Description: pull the main project description block
    description = None
    # Find a long paragraph inside the main content area
    main = soup.find("main") or soup
    body_paragraphs = []
    for p in main.find_all("p"):
        t = p.get_text(strip=True)
        if t and len(t) > 100 and not any(s in t.lower() for s in ["request funds", "rj research initiation"]):
            body_paragraphs.append(t)
    if body_paragraphs:
        description = " ".join(body_paragraphs)[:5000]

    pi_given, pi_family = split_name(pi_raw)

    return {
        "slug":               slug,
        "title":              title,
        "reference_number":   reference_number,
        "grant_administrator": grant_admin,
        "funding_program":    funding_program,
        "subject":            subject,
        "amount_raw":         amount_raw,
        "amount":             amount,
        "currency":           currency,
        "award_year":         award_year,
        "pi_raw":             pi_raw,
        "pi_given_name":      pi_given,
        "pi_family_name":     pi_family,
        "description":        description,
        "landing_page_url":   url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "slug", "reference_number", "grant_administrator",
              "amount", "currency", "award_year", "funding_program", "subject", "description"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # Prefer reference_number for uniqueness; fall back to slug
    keys = []
    for r in rows:
        rn = (r.get("reference_number") or "").strip()
        keys.append(rn or r["slug"])
    if len(keys) != len(set(keys)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(keys).items() if v > 1][:5]
        log(f"  WARNING: reference_number+slug uniqueness imperfect: {dups}")
    else:
        log(f"  ref+slug uniqueness: {len(keys)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)

    # funder_award_id = rj-{slugified-reference-number}-{slug} for collision safety
    def make_id(row):
        rn = (row.get("reference_number") or "").strip()
        if rn:
            rn_slug = re.sub(r"[^a-z0-9]+", "-", rn.lower()).strip("-")
            return f"rj-{rn_slug}-{row['slug']}"
        return f"rj-{row['slug']}"
    df["funder_award_id"] = df.apply(make_id, axis=1)

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
    parser = argparse.ArgumentParser(description="Fetch Riksbankens Jubileumsfond grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "rj_grants.parquet"

    log("=== RJ Jubileumsfond ingest start ===")
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
    for i, url in enumerate(urls, 1):
        try:
            html = get_page(url, cache, args.skip_download)
        except Exception as e:
            log(f"  [{i}/{len(urls)}] FETCH FAIL {url}: {type(e).__name__}: {e}")
            continue
        row = parse_grant_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 200 == 0:
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
        log("=== RJ ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== RJ ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
