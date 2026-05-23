#!/usr/bin/env python3
"""
Stiftung Mercator Fellows → S3 Pipeline (FELLOWSHIP PATTERN, method-5 static HTML)
==================================================================================

Downloads Stiftung Mercator fellow profiles from the foundation's own
WordPress-published archive at stiftung-mercator.de. Major German
philanthropic foundation funding fellowships in international affairs,
climate, education, science, and other policy areas.

Discovery (method-5 static HTML, WordPress YOAST sitemap):

  /fellow-sitemap.xml      1,000 URLs (482 EN + 518 DE — duplicates by language)
  /fellow-sitemap2.xml       950 URLs (429 EN + 521 DE — duplicates by language)

We scrape only the EN versions to avoid double-counting; the EN
pages provide identical metadata in English, which is preferred for
the OpenAlex `description` and `display_name` fields. Total EN
fellows: ~911.

Each EN fellow page (~75KB) is server-rendered with labeled `<h4
class="single-fellow__info-title">{Label}</h4>` followed by a value
sibling. Labels include:

  - Fellowship          (program name, e.g., "Mercator Fellowship on
                         International Affairs")
  - Period of the Fellowship   ("October 2020 - September 2021")
  - Project title        full project description

Plus `<h3 class="single-fellow__biography-title">Short biography</h3>`
+ following paragraph for the bio. `<h1>` is the fellow's full name.

Awarding body in OpenAlex:
  Stiftung Mercator (F4320327917, DE, DOI 10.13039/501100013326).
  NOT to be confused with Mercator Research Center Ruhr (F4320327916,
  DE) or Stiftung Mercator Schweiz (F4320321101, CH).

Amount handling:
  amount/currency are NULL with §6.7 waiver. Stiftung Mercator does
  NOT publish per-fellow amount on the fellow archive. The foundation
  publishes its overall annual budget (~EUR 80M/year) but program-
  specific amounts (per-fellow) are not in the API or the rendered
  page. Fellowship-pattern precedent for NULL amount: HHMI #44,
  CIFAR #79, Damon Runyon #73, Packard #95, Rita Allen #107,
  Schmidt Sciences #108, NOMIS #109, Wenner-Gren #110.

Output
------
  s3://openalex-ingest/awards/mercator/mercator_fellows.parquet

Usage
-----
    python mercator_fellows_to_s3.py                                  # full run (~5 min)
    python mercator_fellows_to_s3.py --skip-upload                    # local dev
    python mercator_fellows_to_s3.py --limit 5                        # smoke
    python mercator_fellows_to_s3.py --skip-download --skip-upload    # reuse cache
    python mercator_fellows_to_s3.py --allow-shrink                   # override §1.4

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
    "https://www.stiftung-mercator.de/fellow-sitemap.xml",
    "https://www.stiftung-mercator.de/fellow-sitemap2.xml",
]

FUNDER_ID = 4320327917
FUNDER_DISPLAY_NAME = "Stiftung Mercator"

PROVENANCE = "mercator_fellows"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mercator/mercator_fellows.parquet"

USER_AGENT = "openalex-walden-mercator-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/mercator_pages.json")

EN_PATH_RE = re.compile(r"^https://www\.stiftung-mercator\.de/en/fellows/[^/]+/?$")

# Common month names for period parsing
MONTH_LOOKUP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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

def discover_en_fellow_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    urls = []
    for sm_url in SITEMAP_URLS:
        xml = get_page(sm_url, cache, use_cache)
        for m in re.finditer(r"<loc>([^<]+)</loc>", xml):
            u = m.group(1)
            if EN_PATH_RE.match(u):
                urls.append(u)
    # dedup
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} EN fellow URLs across {len(SITEMAP_URLS)} sitemaps")
    return out


PERIOD_RE = re.compile(
    r"(?P<m1>[A-Za-z]+)\s+(?P<y1>\d{4})\s*[-–to]+\s*(?P<m2>[A-Za-z]+)?\s*(?P<y2>\d{4})?",
    re.IGNORECASE,
)


def parse_period(text: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[int], Optional[int]]:
    """Parse "October 2020 - September 2021" -> (start_iso, end_iso, start_year, end_year)."""
    if not text:
        return None, None, None, None
    m = PERIOD_RE.search(text)
    if not m:
        # Try bare year
        ym = re.search(r"\b(19\d{2}|20\d{2})\b", text)
        if ym:
            y = int(ym.group(1))
            return f"{y}-01-01", None, y, None
        return None, None, None, None
    m1 = MONTH_LOOKUP.get((m.group("m1") or "").lower())
    y1 = int(m.group("y1")) if m.group("y1") else None
    m2 = MONTH_LOOKUP.get((m.group("m2") or "").lower()) if m.group("m2") else None
    y2 = int(m.group("y2")) if m.group("y2") else None

    start_iso = None
    end_iso = None
    if m1 and y1:
        start_iso = f"{y1:04d}-{m1:02d}-01"
    elif y1:
        start_iso = f"{y1:04d}-01-01"

    if m2 and y2:
        end_iso = f"{y2:04d}-{m2:02d}-01"
    elif y2:
        end_iso = f"{y2:04d}-12-31"

    return start_iso, end_iso, y1, y2


def parse_fellow_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Name from h1 (or fallback to og:title which Mercator strips lang suffix from)
    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else None
    if not name:
        og = soup.find("meta", property="og:title")
        if og:
            name = (og.get("content") or "").split(" – ")[0].strip()
    if not name:
        return None

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # Walk h4.single-fellow__info-title labels + sibling
    fields = {}
    for h4 in soup.find_all("h4", class_="single-fellow__info-title"):
        label = h4.get_text(strip=True)
        nxt = h4.find_next_sibling()
        if nxt:
            fields[label] = nxt.get_text(" ", strip=True)

    # Biography
    bio = None
    bio_h3 = soup.find("h3", class_="single-fellow__biography-title")
    if bio_h3:
        nxt = bio_h3.find_next_sibling()
        if nxt:
            bio = nxt.get_text(" ", strip=True)

    fellowship_program = fields.get("Fellowship")
    period_text = fields.get("Period of the Fellowship")
    project_title = fields.get("Project title") or fields.get("Project Title")

    start_iso, end_iso, start_year, end_year = parse_period(period_text)

    given, family = split_name(name)

    description = bio if bio else project_title

    return {
        "slug":               slug,
        "fellow_name":        name,
        "given_name":         given,
        "family_name":        family,
        "fellowship_program": fellowship_program,
        "period_text":        period_text,
        "period_start":       start_iso,
        "period_end":         end_iso,
        "start_year":         start_year,
        "end_year":           end_year,
        "project_title":      project_title,
        "biography":          bio,
        "description":        description,
        "landing_page_url":   url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No fellow rows parsed")
    n = len(rows)
    for f in ("fellow_name", "slug", "fellowship_program", "period_text", "start_year", "project_title", "biography"):
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
    df["funder_award_id"] = "mercator-fellow-" + df["slug"]
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
    parser = argparse.ArgumentParser(description="Fetch Stiftung Mercator fellows → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "mercator_fellows.parquet"

    log("=== Stiftung Mercator fellows ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)
    urls = discover_en_fellow_urls(args.skip_download, cache)
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
        row = parse_fellow_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 100 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} fellow rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Stiftung Mercator ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Stiftung Mercator ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
