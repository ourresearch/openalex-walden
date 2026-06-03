#!/usr/bin/env python3
"""
Guggenheim Fellowships → S3 Pipeline (FELLOWSHIP PATTERN, method-5 static-HTML listing scrape)
==============================================================================================

Scrapes the John Simon Guggenheim Memorial Foundation's public Fellows
directory at gf.org. The Guggenheim Fellowship (awarded annually since 1925
to ~175 mid-career scholars, scientists and artists "across every field of
knowledge and every form of art") is one of the most prestigious fellowships
in the world — its ~19,800 named fellows are notable individuals, most of
whom are already authors in OpenAlex. This ingest is therefore valuable as an
**award↔person linkage**: each row ties a named fellow to the Guggenheim
Fellowship in a given year.

Discovery (method-5, server-rendered static HTML): the Fellows directory is a
paginated, server-rendered listing — `gf.org/fellows/?page=N` (~100 fellows
per page, ~198 pages). Each fellow card exposes a stable slug, the fellow's
name, and the fellowship year:

    <a data-modalFellow-fellow="kyle-abraham" ...><h3>Kyle Abraham</h3></a>
    <p> 2026 </p>

Plain `requests`, no browser automation.

This is a FELLOWSHIP-pattern funder: each award is held by a **named
individual**. `lead_investigator` carries given/family parsed from the
fellow's name; `co_lead_investigator`/`investigators` are NULL. The fellow
slug is the stable, unique, source-authoritative award id.

Awarding body in OpenAlex:
  John Simon Guggenheim Memorial Foundation (F4320308774, US,
  ROR 0407tnq23, DOI 10.13039/100005851).

Amount: NULL with the §6.7 fellowship-pattern waiver — the foundation does
not publish per-fellow stipend amounts on the public directory (same waiver
as HHMI #44 / CIFAR #79 / Pew #97). funder_scheme is NULL: the fellow's
**field** (discipline) is rendered client-side only (an empty `<p>` in the
static listing), so it is left NULL rather than guessed — it can be enriched
later via a Playwright pass if wanted.

start_year = the fellowship year (real, source-authoritative). start_date /
end_date / end_year are NULL (the award is point-in-time; no day precision
and no published end).

Output
------
  s3://openalex-ingest/awards/guggenheim/guggenheim_fellows.parquet

Usage
-----
    python guggenheim_to_s3.py                                  # full run
    python guggenheim_to_s3.py --skip-upload                    # local dev
    python guggenheim_to_s3.py --limit 5                        # smoke (5 pages)
    python guggenheim_to_s3.py --skip-download --skip-upload    # reuse cache
    python guggenheim_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import html as ihtml
import json
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

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

BASE = "https://www.gf.org"
LIST_URL = BASE + "/fellows/?page={page}"

FUNDER_ID = 4320308774
FUNDER_DISPLAY_NAME = "John Simon Guggenheim Memorial Foundation"

PROVENANCE = "guggenheim_fellowship"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/guggenheim/guggenheim_fellows.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-guggenheim-ingest/1.0; +https://openalex.org)"

REQUEST_DELAY_S = 0.25
MAX_PAGES = 260               # ~198 pages observed; generous ceiling
DEFAULT_CACHE = Path(".cache/guggenheim_fellows.json")

NAME_PARTICLES = {
    "von", "van", "der", "den", "de", "del", "della", "di", "da", "dos", "das",
    "le", "la", "du", "do", "ten", "ter", "zu", "af", "al", "bin", "ibn", "st", "st.",
}

CARD_RE = re.compile(r'<li class="relative group flex.*?</li>', re.S)
SLUG_RE = re.compile(r'data-modalFellow-fellow="([^"]+)"')
NAME_RE = re.compile(r'<h3[^>]*>(.*?)</h3>', re.S)
YEAR_RE = re.compile(r'<p[^>]*>\s*((?:18|19|20)\d{2})\s*</p>')


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# HTTP
# =============================================================================

_session: Optional[requests.Session] = None
_last_t = 0.0


def _http_get(url: str, timeout: int = 30) -> str:
    global _session, _last_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    dt = time.monotonic() - _last_t
    if dt < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - dt)
    r = _session.get(url, timeout=timeout)
    _last_t = time.monotonic()
    r.raise_for_status()
    return r.text


# =============================================================================
# Scrape
# =============================================================================

def parse_cards(htmltext: str) -> list:
    rows = []
    for card in CARD_RE.findall(htmltext):
        ms = SLUG_RE.search(card)
        mn = NAME_RE.search(card)
        my = YEAR_RE.search(card)
        if not ms or not mn:
            continue
        slug = ms.group(1).strip()
        name = ihtml.unescape(re.sub(r"<[^>]+>", "", mn.group(1))).strip()
        year = int(my.group(1)) if my else None
        if slug and name:
            rows.append({"slug": slug, "name": name, "year": year})
    return rows


def scrape_all(use_cache: bool, cache_path: Path) -> list:
    if use_cache and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            if isinstance(cached, list) and cached:
                log(f"  loaded {len(cached):,} fellows from cache")
                return cached
        except Exception:
            pass
    all_rows = []
    empty_streak = 0
    for page in range(1, MAX_PAGES + 1):
        try:
            htmltext = _http_get(LIST_URL.format(page=page))
        except Exception as e:
            log(f"  page {page} fetch error: {type(e).__name__}: {str(e)[:60]}")
            break
        cards = parse_cards(htmltext)
        if not cards:
            empty_streak += 1
            if empty_streak >= 2:
                log(f"  page {page}: empty (end of directory)")
                break
            continue
        empty_streak = 0
        all_rows.extend(cards)
        if page % 25 == 0:
            log(f"  page {page}: +{len(cards)} (total {len(all_rows):,})")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(all_rows, ensure_ascii=False))
    log(f"  cached {len(all_rows):,} fellows -> {cache_path}")
    return all_rows


# =============================================================================
# Field parsing
# =============================================================================

def split_name(full: Optional[str]):
    if not full:
        return None, None
    parts = str(full).strip().split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    fam_start = len(parts) - 1
    while fam_start - 1 >= 1 and parts[fam_start - 1].lower() in NAME_PARTICLES:
        fam_start -= 1
    given = " ".join(parts[:fam_start]).strip() or None
    family = " ".join(parts[fam_start:]).strip() or None
    return given, family


def build_row(rec: dict) -> Optional[dict]:
    slug = (rec.get("slug") or "").strip()
    name = (rec.get("name") or "").strip()
    if not slug or not name:
        return None
    given, family = split_name(name)
    year = rec.get("year")
    try:
        year = int(year) if year is not None else None
    except (TypeError, ValueError):
        year = None
    return {
        "slug":             slug,
        "funder_award_id":  slug,
        "fellow_name":      name,
        "given_name":       given,
        "family_name":      family,
        "start_year":       year,
        "landing_page_url": f"{BASE}/fellows/{slug}/",
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No fellow rows parsed")
    n = len(rows)
    for f in ("fellow_name", "funder_award_id", "family_name", "start_year"):
        nn = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<16} coverage {nn}/{n} ({nn*100/n:.1f}%)")
    ids = [r["funder_award_id"] for r in rows if r.get("funder_award_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(f"funder_award_id collisions: {dups}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")
    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  fellowship year range: {min(years)}–{max(years)}")
        from collections import Counter
        per = Counter(years)
        log(f"  sample recent cohorts: " + ", ".join(f"{y}={per[y]}" for y in sorted(per)[-4:]))


def build_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.astype("string")  # §1.2.5
    return df


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3, io
        prev = pd.read_parquet(io.BytesIO(boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()))
        log(f"  §1.4 shrink-check: previous S3 parquet had {len(prev):,} rows")
        if new_count < len(prev):
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({len(prev):,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {len(prev):,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:90]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    import boto3
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Guggenheim Fellows → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="limit to N pages (smoke)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "guggenheim_fellows.parquet"

    log("=== Guggenheim Fellowships ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")

    global MAX_PAGES
    if args.limit is not None:
        MAX_PAGES = args.limit
        log(f"--limit {args.limit}: capping at {args.limit} pages")

    fellows = scrape_all(args.skip_download, args.cache)
    log(f"  scraped {len(fellows):,} fellows")

    rows = [r for r in (build_row(f) for f in fellows) if r is not None]
    log(f"Built {len(rows):,} fellow rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Guggenheim ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")
    upload_to_s3(output_path)
    log("=== Guggenheim ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
