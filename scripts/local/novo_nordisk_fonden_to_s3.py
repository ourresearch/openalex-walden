#!/usr/bin/env python3
"""
Novo Nordisk Fonden Prize Recipients → S3 Pipeline (PRIZE PATTERN, method-5 static HTML)
==========================================================================================

Downloads prize recipients from the foundation's own WordPress
archive at novonordiskfonden.dk. Novo Nordisk Fonden (Danish:
"Novo Nordisk Foundation") is one of the world's largest
biomedical-research funders by endowment (DKK 100+ billion). The
foundation awards multiple named prizes (Novo Nordisk Prisen,
Marie og August Krogh Prisen, Hagedorn Prisen, Nordic Diabetes
Prize, etc.) — this ingest covers the prize-recipient archive.

Discovery (method-5 static HTML, WordPress YOAST sub-sitemap):

  /prize_recipients-sitemap.xml    839 recipient URLs

Each recipient page (~64KB) renders the recipient header as a
sequence:

  <h1>{Recipient Name}</h1>
  <h4>{Credentials}</h4>   e.g., "Professor, dr.med."
  "{Prize Name} - {Year}"  in header strip text

The URL slug ends in `-{YEAR}/` which is also a reliable year source.

Awarding body in OpenAlex:
  Novo Nordisk Fonden (F4320325957, DK,
  DOI 10.13039/501100009708).

Amount handling:
  amount/currency are NULL with §6.7 waiver. Novo Nordisk Fonden
  publishes per-prize amount info on the prize-definition pages
  under `/priser/{prize-slug}/` (e.g., Novo Nordisk Prisen carries
  DKK 5M) but does NOT publish per-recipient amount on the
  recipient pages themselves. Prize-pattern precedent for NULL
  amount: Fields Medal #50, Royal Society Medals #71, Wolf Prize
  #47, Lasker #48.

  (A follow-up could enrich amount by joining recipient.prize_slug
  → priser/{slug} cell, but the prize_slug field is currently
  derived heuristically and would need manual verification per
  prize family.)

Output
------
  s3://openalex-ingest/awards/novo_nordisk_fonden/novo_nordisk_fonden_recipients.parquet

Usage
-----
    python novo_nordisk_fonden_to_s3.py                                  # full run (~5 min @ 0.3s)
    python novo_nordisk_fonden_to_s3.py --skip-upload                    # local dev
    python novo_nordisk_fonden_to_s3.py --limit 10                       # smoke
    python novo_nordisk_fonden_to_s3.py --skip-download --skip-upload    # reuse cache
    python novo_nordisk_fonden_to_s3.py --allow-shrink                   # override §1.4

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

SITEMAP_URL = "https://novonordiskfonden.dk/prize_recipients-sitemap.xml"

FUNDER_ID = 4320325957
FUNDER_DISPLAY_NAME = "Novo Nordisk Fonden"

PROVENANCE = "novo_nordisk_fonden_prizes"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/novo_nordisk_fonden/novo_nordisk_fonden_recipients.parquet"

USER_AGENT = "openalex-walden-novo-nordisk-fonden-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/novo_nordisk_fonden_pages.json")

# Match /prismodtagere/{slug-with-trailing-year}/
RECIPIENT_URL_RE = re.compile(
    r"^https://novonordiskfonden\.dk/prismodtagere/[^/]+-(\d{4})/?$"
)
# Year from slug: `erik-a-richter-2012` → 2012
SLUG_YEAR_RE = re.compile(r"-(\d{4})$")
# Prize line: "Novo Nordisk Prisen - 2012" or "Marie og August Krogh Prisen 2013"
PRIZE_LINE_RE = re.compile(
    r"^\s*([^\d\n]+?)\s*[-–]?\s*(\d{4})\s*$"
)


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

def discover_recipient_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    xml = get_page(SITEMAP_URL, cache, use_cache)
    urls = [m.group(0) for m in re.finditer(r"https://novonordiskfonden\.dk/prismodtagere/[^<\s]+", xml)]
    # Filter to year-suffixed recipient URLs (skip the index page)
    out = []
    seen = set()
    for u in urls:
        u = u.rstrip("/")
        # Skip if it doesn't end in -YYYY
        if not SLUG_YEAR_RE.search(u):
            continue
        u_with_slash = u + "/"
        if u_with_slash not in seen:
            seen.add(u_with_slash)
            out.append(u_with_slash)
    log(f"  enumerated {len(out)} prize-recipient URLs from sitemap")
    return out


def parse_recipient_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else None
    if not name:
        return None

    # Credentials: h4 right after h1
    credentials = None
    if h1:
        h4 = h1.find_next("h4")
        if h4:
            credentials = h4.get_text(strip=True)
            # Only accept if it's a reasonable-length credentials string
            if len(credentials) > 200 or any(s in credentials.lower() for s in ["fonden", "footer", "info"]):
                credentials = None

    # Prize name + year: look in the page's header strip text
    # The sequence is: name | credentials | "Prize Name - YEAR"
    # We find the next sibling block after h1+h4 that contains a year token.
    prize_name = None
    prize_year_from_header = None
    # Strip footer/nav before searching
    for tag in soup(['nav', 'footer', 'aside']):
        tag.decompose()
    # Look for the header strip text
    header = soup.find("div", class_=re.compile("nnf-node-header", re.I))
    if header:
        header_text = header.get_text(" | ", strip=True)
        # Try the prize line regex against each candidate segment
        for seg in header_text.split("|"):
            seg = seg.strip()
            m = PRIZE_LINE_RE.match(seg)
            if m and len(m.group(1).strip()) > 3:
                candidate_prize = m.group(1).strip()
                candidate_year = int(m.group(2))
                # Skip if the "name" portion is just the recipient's name
                if name.lower() in candidate_prize.lower():
                    continue
                prize_name = candidate_prize
                prize_year_from_header = candidate_year
                break

    # Year from URL slug
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    year_from_slug = None
    m = SLUG_YEAR_RE.search(slug)
    if m:
        year_from_slug = int(m.group(1))

    award_year = prize_year_from_header or year_from_slug

    pi_given, pi_family = split_name(name)

    # Derive a "name-only" slug (strip the trailing -YEAR)
    name_slug = SLUG_YEAR_RE.sub("", slug)

    return {
        "slug":             slug,
        "name_slug":        name_slug,
        "recipient_name":   name,
        "given_name":       pi_given,
        "family_name":      pi_family,
        "credentials":      credentials,
        "prize_name":       prize_name,
        "award_year":       award_year,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No recipient rows parsed")
    n = len(rows)
    for f in ("recipient_name", "slug", "credentials", "prize_name", "award_year"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "nnf-" + df["slug"]
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
    parser = argparse.ArgumentParser(description="Fetch Novo Nordisk Fonden prize recipients → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "novo_nordisk_fonden_recipients.parquet"

    log("=== Novo Nordisk Fonden recipients ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")

    cache = load_cache(args.cache)
    urls = discover_recipient_urls(args.skip_download, cache)
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
        row = parse_recipient_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 200 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} recipient rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Novo Nordisk Fonden ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Novo Nordisk Fonden ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
