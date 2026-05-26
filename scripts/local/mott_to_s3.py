#!/usr/bin/env python3
"""
Charles Stewart Mott Foundation Grants → S3 Pipeline (GRANT PATTERN, method-5 static HTML)
=========================================================================================

Downloads grants from the Charles Stewart Mott Foundation's own
WordPress-published grants archive at mott.org. Mott is a major US
private foundation funding civil society, environment, education,
and Flint-area community development.

Discovery (method-5 static HTML): 3 sitemap files enumerate the full
corpus.

  /grant-sitemap.xml     1,000 grant URLs
  /grant-sitemap2.xml    1,000 grant URLs
  /grant-sitemap3.xml    1,000 grant URLs
  TOTAL                  3,000 grant URLs

Each grant detail page (~50KB) has a `<ul class="list1-entries">`
block with `<li><strong>Label</strong><span>Value</span></li>`
pairs exposing **every structured field we need**:

  Program             e.g., Environment
  Program Initiative  e.g., One Water
  Grant Amount        e.g., $200,000
  Grant Period        e.g., June 1, 2026–May 31, 2028
  Location            e.g., Detroit, MI, United States
  Geographic Focus    e.g., Michigan

The page H1 is the project name; the OpenGraph title is the recipient
organization followed by the project name and an internal grant number
(e.g., "Center for Michigan, Bridge Michigan Environment Watch, 2026-14226").

Native award ID is the trailing numeric part of the URL slug (e.g.,
`/grants/2026-14226/` → `2026-14226`).

Awarding body in OpenAlex:
  Charles Stewart Mott Foundation (F4320307861, US, no ROR,
  DOI 10.13039/100004428).

Currency: USD hardcoded (single-country US foundation).

Output
------
  s3://openalex-ingest/awards/mott/mott_grants.parquet

Usage
-----
    python mott_to_s3.py                                  # full run (~15 min @ 0.3s throttle)
    python mott_to_s3.py --skip-upload                    # local dev
    python mott_to_s3.py --limit 10                       # smoke
    python mott_to_s3.py --skip-download --skip-upload    # reuse cache
    python mott_to_s3.py --allow-shrink                   # override §1.4

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
    "https://www.mott.org/grant-sitemap.xml",
    "https://www.mott.org/grant-sitemap2.xml",
    "https://www.mott.org/grant-sitemap3.xml",
]

FUNDER_ID = 4320307861
FUNDER_DISPLAY_NAME = "Charles Stewart Mott Foundation"

PROVENANCE = "mott_grants"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mott/mott_grants.parquet"

USER_AGENT = "openalex-walden-mott-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/mott_pages.json")

GRANT_URL_RE = re.compile(r"^https://www\.mott\.org/grants/[^/]+/?$")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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
    urls = []
    for sm_url in SITEMAP_URLS:
        xml = get_page(sm_url, cache, use_cache)
        for m in re.finditer(r"<loc>([^<]+)</loc>", xml):
            u = m.group(1)
            if GRANT_URL_RE.match(u):
                urls.append(u)
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} grant URLs across {len(SITEMAP_URLS)} sitemaps")
    return out


AMOUNT_NUMBER_RE = re.compile(r"\$\s*([0-9,]+(?:\.[0-9]+)?)")
GRANT_PERIOD_RE = re.compile(
    r"^([A-Za-z]+ \d{1,2},?\s*\d{4})\s*[–—\-]\s*([A-Za-z]+ \d{1,2},?\s*\d{4})$"
)


def parse_grant_period(s: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Parse "June 1, 2026–May 31, 2028" -> (start ISO, end ISO, start year)."""
    if not s:
        return None, None, None
    from datetime import datetime
    m = GRANT_PERIOD_RE.match(s.strip())
    if not m:
        # Single-date fallback
        try:
            d = datetime.strptime(s.strip(), "%B %d, %Y")
            return d.strftime("%Y-%m-%d"), None, d.year
        except ValueError:
            pass
        y_m = re.search(r"(19\d{2}|20\d{2})", s)
        if y_m:
            y = int(y_m.group(1))
            return f"{y}-01-01", None, y
        return None, None, None
    start_s, end_s = m.group(1).strip(), m.group(2).strip()
    start_d, end_d, year = None, None, None
    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            d = datetime.strptime(start_s, fmt)
            start_d = d.strftime("%Y-%m-%d")
            year = d.year
            break
        except ValueError:
            continue
    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            d = datetime.strptime(end_s, fmt)
            end_d = d.strftime("%Y-%m-%d")
            break
        except ValueError:
            continue
    return start_d, end_d, year


def parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    m = AMOUNT_NUMBER_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_grant_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        return None

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # Recipient comes from the OG title: "Recipient Name, Project Name, grant-id | Mott Foundation"
    recipient = None
    og_title = soup.find("meta", property="og:title")
    if og_title:
        og_text = og_title.get("content", "")
        # Strip " | Mott Foundation" suffix
        og_text = re.sub(r"\s*\|\s*Mott Foundation\s*$", "", og_text)
        # The leading segment up to the first comma is the recipient
        parts = [p.strip() for p in og_text.split(",")]
        if parts and parts[0] and parts[0] != title:
            recipient = parts[0]

    # Walk the labeled list (<ul class="list1-entries">) for structured fields
    fields: dict[str, str] = {}
    for ul in soup.find_all("ul", class_="list1-entries"):
        for li in ul.find_all("li"):
            strong = li.find("strong")
            span = li.find("span")
            if strong and span:
                label = strong.get_text(strip=True)
                value = span.get_text(strip=True)
                if label:
                    fields[label] = value

    program           = fields.get("Program")
    initiative        = fields.get("Program Initiative")
    amount_raw        = fields.get("Grant Amount")
    period_raw        = fields.get("Grant Period")
    location          = fields.get("Location")
    geographic_focus  = fields.get("Geographic Focus")

    amount = parse_amount(amount_raw or "")
    start_iso, end_iso, start_year = parse_grant_period(period_raw or "")

    # Description from JSON-LD if available
    description = None
    for m in re.finditer(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            d = json.loads(m.group(1))
            for item in (d.get("@graph", [d]) if isinstance(d, dict) else []):
                if isinstance(item, dict) and item.get("@type") in ("Article", "WebPage"):
                    description = item.get("description") or description
        except json.JSONDecodeError:
            continue
    # Description fallback: og:description
    if not description:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            description = og_desc.get("content", "").strip() or None

    return {
        "slug":             slug,
        "title":            title,
        "recipient":        recipient,
        "program":          program,
        "initiative":       initiative,
        "amount":           amount,
        "amount_raw":       amount_raw,
        "period_raw":       period_raw,
        "start_date":       start_iso,
        "end_date":         end_iso,
        "start_year":       start_year,
        "location":         location,
        "geographic_focus": geographic_focus,
        "description":      description,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "slug", "recipient", "program", "amount", "start_year", "location"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        log(f"  amount stats: n={len(amts)} min=${min(amts):,.0f} median=${sorted(amts)[len(amts)//2]:,.0f} max=${max(amts):,.0f}")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "mott-" + df["slug"]
    df["currency"] = CURRENCY
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
    parser = argparse.ArgumentParser(description="Fetch Mott Foundation grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "mott_grants.parquet"

    log(f"=== Mott Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

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
        if i % 100 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total so far: {len(rows)})")
        if i % 50 == 0:
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
        log("=== Mott ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Mott ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
