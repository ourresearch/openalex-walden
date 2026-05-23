#!/usr/bin/env python3
"""
Fritz Thyssen Stiftung Fundings → S3 Pipeline (GRANT PATTERN, method-5 static HTML)
====================================================================================

Downloads Fritz Thyssen Stiftung "funding" records from the
foundation's own WordPress archive at fritz-thyssen-stiftung.de.
Major German humanities/social-sciences funder (Geisteswissenschaften
und Sozialwissenschaften), funding research projects since 1959.

Discovery (method-5 static HTML, WordPress YOAST sub-sitemaps):

  /wp-sitemap-posts-funding-1.xml      2,000 URLs
  /wp-sitemap-posts-funding-2.xml      2,000 URLs
  /wp-sitemap-posts-funding-3.xml      2,000 URLs
  /wp-sitemap-posts-funding-4.xml        790 URLs
  TOTAL                                6,790 unique funding URLs

Each funding page (~99KB) is server-rendered with clean labeled
fields in `<div class="info-box fundingbox">` and
`<div class="funding-detail">`:

  Institution     "Prof. W. Vossenkuhl | Lehrstuhl Philosophie I,
                   Ludwig-Maximilians-Universität München"
  Bewilligung     YEAR (German for "Approval/Grant")
  Förderbereich   funding area / topic taxonomy

`<h1>` is the project title (in German).

Awarding body in OpenAlex:
  Fritz Thyssen Stiftung (F4320321876, DE, DOI 10.13039/501100003390).

Amount handling:
  amount/currency are NULL with §6.7 waiver. Fritz Thyssen publishes
  program-level fixed amounts on /foerderung/ pages (e.g., research
  grants in the range of EUR 100k-500k) but does NOT publish per-
  funding amount on the funding-archive pages. Per the runbook
  source-authority rule we don't backfill from program narrative.
  Grant/research-funder precedent for NULL amount: HHMI #44, CIFAR
  #79, Damon Runyon #73, Packard #95, Rita Allen #107, Schmidt
  Sciences #108, NOMIS #109, Wenner-Gren #110, Mercator #116.

Output
------
  s3://openalex-ingest/awards/fritz_thyssen/fritz_thyssen_fundings.parquet

Usage
-----
    python fritz_thyssen_to_s3.py                                  # full run (~34 min @ 0.3s)
    python fritz_thyssen_to_s3.py --skip-upload                    # local dev
    python fritz_thyssen_to_s3.py --limit 10                       # smoke
    python fritz_thyssen_to_s3.py --skip-download --skip-upload    # reuse cache
    python fritz_thyssen_to_s3.py --allow-shrink                   # override §1.4

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
    f"https://www.fritz-thyssen-stiftung.de/wp-sitemap-posts-funding-{n}.xml"
    for n in (1, 2, 3, 4)
]

FUNDER_ID = 4320321876
FUNDER_DISPLAY_NAME = "Fritz Thyssen Stiftung"

PROVENANCE = "fritz_thyssen_fundings"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fritz_thyssen/fritz_thyssen_fundings.parquet"

USER_AGENT = "openalex-walden-fritz-thyssen-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/fritz_thyssen_pages.json")

URL_PATTERN_RE = re.compile(r"^https://www\.fritz-thyssen-stiftung\.de/fundings/[^/]+/?$")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                  "prof.", "dr.", "prof", "dr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split a German PI name. Handles "Prof. Dr. First Last" -> ('First', 'Last').
    Title prefixes (Prof., Dr.) are stripped before splitting."""
    if not name:
        return None, None
    # Strip leading title tokens (Prof., Dr., PD, Priv.-Doz.)
    tokens = re.split(r"\s+", name.strip())
    while tokens and tokens[0].lower().strip(".,") in _SUFFIX_TOKENS:
        tokens.pop(0)
    while tokens and tokens[-1].lower().strip(".,") in _SUFFIX_TOKENS:
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

def discover_funding_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    """Enumerate funding URLs. The sitemap index lists 4 sub-sitemaps but in
    practice only `wp-sitemap-posts-funding-1.xml` returns 200 — the others
    are 404 (likely a stale W3 Total Cache reference). We try each and skip
    any 404, so the script is resilient to upstream pagination changes."""
    urls = []
    found_sitemaps = 0
    for sm_url in SITEMAP_URLS:
        try:
            xml = get_page(sm_url, cache, use_cache)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log(f"  sitemap {sm_url.rsplit('/', 1)[-1]} not found (404); skipping")
                continue
            raise
        found_sitemaps += 1
        for m in re.finditer(r"<loc>([^<]+)</loc>", xml):
            u = m.group(1)
            if URL_PATTERN_RE.match(u):
                urls.append(u)
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} funding URLs across {found_sitemaps} reachable sub-sitemap(s)")
    return out


def parse_funding_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        og = soup.find("meta", property="og:title")
        title = (og.get("content") or "").strip() if og else None
    if not title:
        return None

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # Institution box: <div class="info-box fundingbox"> contains
    # "Institution | PI Name | Affiliation"
    institution = None
    pi_raw = None
    info_box = soup.find("div", class_="info-box")
    if info_box and "fundingbox" in (info_box.get("class") or []):
        # Children paragraphs
        parts = [p.get_text(strip=True) for p in info_box.find_all(["p", "a", "span"]) if p.get_text(strip=True)]
        # Drop "Institution" label if present
        parts = [p for p in parts if p.lower() != "institution"]
        if parts:
            # First non-label item = PI name, second = affiliation
            pi_raw = parts[0]
            if len(parts) > 1:
                institution = parts[1]

    # Bewilligung (Approval year) + Förderbereich (Funding area).
    # The funding-detail elements are <p class="funding-detail"> on Fritz
    # Thyssen pages — we use a tag-agnostic find_all so the parser keeps
    # working if the CMS swaps tag types in a redesign.
    bewilligung = None
    foerderbereich = None
    for det in soup.find_all(class_="funding-detail"):
        text = det.get_text(" | ", strip=True)
        if "Bewilligung" in text:
            # "Bewilligung | 2011"
            m = re.search(r"(\d{4})", text)
            if m:
                bewilligung = int(m.group(1))
        elif "Förderbereich" in text or "Foerderbereich" in text:
            # "Förderbereich | Geschichte, Sprache & Kultur"
            parts = text.split("|", 1)
            if len(parts) > 1:
                foerderbereich = parts[1].strip()

    pi_given, pi_family = split_name(pi_raw)

    return {
        "slug":             slug,
        "title":            title,
        "pi_raw":           pi_raw,
        "pi_given_name":    pi_given,
        "pi_family_name":   pi_family,
        "institution":      institution,
        "bewilligung_year": bewilligung,
        "foerderbereich":   foerderbereich,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No funding rows parsed")
    n = len(rows)
    for f in ("title", "slug", "pi_raw", "institution", "bewilligung_year", "foerderbereich"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "ft-" + df["slug"]
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
    parser = argparse.ArgumentParser(description="Fetch Fritz Thyssen Stiftung fundings → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "fritz_thyssen_fundings.parquet"

    log("=== Fritz Thyssen Stiftung ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")

    cache = load_cache(args.cache)
    urls = discover_funding_urls(args.skip_download, cache)
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
        row = parse_funding_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 200 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} funding rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Fritz Thyssen ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Fritz Thyssen ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
