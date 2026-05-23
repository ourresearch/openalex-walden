#!/usr/bin/env python3
"""
Helmsley Charitable Trust Grants → S3 Pipeline (GRANT PATTERN, method-5 static HTML)
====================================================================================

Downloads the Leona M. and Harry B. Helmsley Charitable Trust grants
from the foundation's own WordPress-published grants archive at
helmsleytrust.org. Helmsley is one of the largest US private
foundations (~$700M/year in grants) with programs across Type 1
Diabetes, Crohn's Disease, Health Sciences, Conservation, Rural
Healthcare, Vulnerable Children in Sub-Saharan Africa, and Israel
non-sectarian programs.

Discovery (method-5 static HTML): 4 WordPress sitemap files enumerate
the full corpus.

  /grants-sitemap.xml      1,000 grant URLs
  /grants-sitemap2.xml     1,000 grant URLs
  /grants-sitemap3.xml     1,000 grant URLs
  /grants-sitemap4.xml        93 grant URLs
  TOTAL                    3,093 grant URLs

Each grant page (~42KB) is server-rendered with a `<div class="grant-info">`
labeled-key-value block exposing every structured field we need:

  Date of Award      e.g., 03.31.2009
  Program            e.g., Health Sciences
  Amount             e.g., $9,761,000.00
  Term of Grant      e.g., 12 months
  Project Title      e.g., to support the capital construction of...

The page H1 is the recipient organization name.

Awarding body in OpenAlex:
  Leona M. and Harry B. Helmsley Charitable Trust (F4320309446, US,
  no ROR, DOI 10.13039/100007028).

Output
------
  s3://openalex-ingest/awards/helmsley/helmsley_grants.parquet

Usage
-----
    python helmsley_to_s3.py                                  # full run (~16 min @ 0.3s throttle)
    python helmsley_to_s3.py --skip-upload                    # local dev
    python helmsley_to_s3.py --limit 10                       # smoke
    python helmsley_to_s3.py --skip-download --skip-upload    # reuse cache
    python helmsley_to_s3.py --allow-shrink                   # override §1.4

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
    "https://www.helmsleytrust.org/grants-sitemap.xml",
    "https://www.helmsleytrust.org/grants-sitemap2.xml",
    "https://www.helmsleytrust.org/grants-sitemap3.xml",
    "https://www.helmsleytrust.org/grants-sitemap4.xml",
]

FUNDER_ID = 4320309446
FUNDER_DISPLAY_NAME = "Leona M. and Harry B. Helmsley Charitable Trust"

PROVENANCE = "helmsley_grants"
CURRENCY = "USD"  # Helmsley is a US foundation; the source publishes USD only

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/helmsley/helmsley_grants.parquet"

USER_AGENT = "openalex-walden-helmsley-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/helmsley_pages.json")

GRANT_URL_RE = re.compile(r"^https://www\.helmsleytrust\.org/grants/[^/]+/?$")


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
    # dedup while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} grant URLs across {len(SITEMAP_URLS)} sitemaps")
    return out


AMOUNT_NUMBER_RE = re.compile(r"\$\s*([0-9,]+(?:\.[0-9]+)?)")
DATE_RE_DOT = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")  # MM.DD.YYYY
DATE_RE_SLASH = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def parse_date(s: str) -> tuple[Optional[str], Optional[int]]:
    """Parse Helmsley's MM.DD.YYYY date format. Returns (ISO date, year)."""
    if not s:
        return None, None
    s = s.strip()
    m = DATE_RE_DOT.match(s)
    if m:
        mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}", int(yyyy)
    m = DATE_RE_SLASH.match(s)
    if m:
        mm, dd, yyyy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}", int(yyyy)
    # Bare year fallback
    m = re.search(r"(19\d{2}|20\d{2})", s)
    if m:
        y = int(m.group(1))
        return f"{y}-01-01", y
    return None, None


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
    recipient = h1.get_text(strip=True) if h1 else None
    if not recipient:
        return None

    # Slug from URL (typically {orgname}-{numeric-id}/)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    # Numeric grant ID from the slug suffix (e.g., "greenwich-hospital-2512" → 2512)
    m_id = re.search(r"-(\d+)$", slug)
    native_grant_id = m_id.group(1) if m_id else None

    info = soup.find("div", class_="grant-info")
    fields: dict[str, str] = {}
    if info:
        # Helmsley detail pages use <h6>Label</h6><p>Value</p> sibling pairs
        # inside the grant-info block. Walk the direct children sequentially
        # and pair each h6 with the next <p>.
        children = list(info.find_all(["h6", "p"], recursive=False))
        i = 0
        while i < len(children):
            el = children[i]
            if el.name == "h6":
                label = el.get_text(strip=True).rstrip(":").strip()
                # next sibling that's a <p>
                value = ""
                if i + 1 < len(children) and children[i + 1].name == "p":
                    value = children[i + 1].get_text(strip=True)
                    i += 1
                if label:
                    fields[label] = value
            i += 1

    date_raw = fields.get("Date of Award") or fields.get("Date")
    iso_date, year = parse_date(date_raw or "")

    program = fields.get("Program")
    amount_raw = fields.get("Amount")
    amount = parse_amount(amount_raw or "")
    term = fields.get("Term of Grant") or fields.get("Term")
    project_title = fields.get("Project Title") or fields.get("Title")

    # If no display title found, use the recipient name as a fallback
    display_name = project_title or recipient

    return {
        "recipient":        recipient,
        "slug":             slug,
        "native_grant_id":  native_grant_id,
        "program":          program,
        "amount":           amount,
        "amount_raw":       amount_raw,
        "term":             term,
        "date_raw":         date_raw,
        "award_date_iso":   iso_date,
        "award_year":       year,
        "project_title":    project_title,
        "display_name":     display_name,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("recipient", "slug", "program", "amount", "award_year", "project_title", "term"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # native_grant_id should be unique (numeric suffix of slug)
    ids = [r.get("native_grant_id") for r in rows if r.get("native_grant_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        log(f"  WARNING: native_grant_id collisions: {len(ids) - len(set(ids))} duplicates "
            f"(example: {dups}). funder_award_id will use slug to disambiguate.")
    else:
        log(f"  native_grant_id uniqueness: {len(ids)}/{n} distinct ✓")

    if rows[0].get("amount") is not None:
        amts = [r["amount"] for r in rows if r.get("amount") is not None]
        log(f"  amount stats: n={len(amts)} min=${min(amts):,.0f} median=${sorted(amts)[len(amts)//2]:,.0f} max=${max(amts):,.0f}")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # funder_award_id = slug (slug already contains the numeric id at the end)
    df["funder_award_id"] = "helmsley-" + df["slug"]
    df["currency"] = CURRENCY
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
    parser = argparse.ArgumentParser(description="Fetch Helmsley Charitable Trust grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "helmsley_grants.parquet"

    log(f"=== Helmsley grants ingest start ===")
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
        log("=== Helmsley grants ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Helmsley grants ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
