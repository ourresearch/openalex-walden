#!/usr/bin/env python3
"""
Radcliffe Institute Fellowships → S3 Pipeline (FELLOWSHIP PATTERN, method-5 static-HTML listing scrape)
======================================================================================================

Scrapes the Harvard Radcliffe Institute for Advanced Study's public Fellows
directory at radcliffe.harvard.edu. The Radcliffe Fellowship is a prestigious
year-long residential fellowship (~50/year) for scholars, scientists, artists
and writers across disciplines — its named fellows are notable individuals,
most already authors in OpenAlex. This ingest is therefore valuable as an
**award↔person linkage**: each row ties a named fellow to the fellowship in a
given academic year, and adds a humanities/interdisciplinary coverage angle.

Discovery (method-5, server-rendered static HTML): the directory is filtered
by academic-year cohort and paginated —
`radcliffe.harvard.edu/radcliffe-fellowship/fellows?year[]=YYYY–YYYY&page=N`
(~25 fellows/page). Each card exposes the fellow's slug and explicit name:

    <a href="/people/david-alekhuogie?...">
      <h3 class="m-card__title" data-text="David Alekhuogie">

The fellowship year comes from the cohort filter (so it is exact). Plain
`requests`, no browser automation.

FELLOWSHIP pattern: each award is held by a **named individual**.
`lead_investigator` carries given/family parsed from the fellow's name;
`co_lead_investigator`/`investigators` are NULL. `funder_award_id` is the
slug + cohort year (`{slug}-{year}`) so a returning fellow's distinct cohorts
are distinct awards.

Awarding body in OpenAlex:
  Radcliffe Institute for Advanced Study, Harvard University
  (F4320332388, US, DOI 10.13039/100006274; no ROR in OpenAlex).

Amount: NULL with the §6.7 fellowship-pattern waiver — the Institute does not
publish per-fellow stipend amounts in the public directory (same waiver as
HHMI #44 / CIFAR #79 / Pew #97). funder_scheme NULL. start_year = the cohort's
first year (real); start_date/end_date/end_year NULL (academic-year award).

Output
------
  s3://openalex-ingest/awards/radcliffe/radcliffe_fellows.parquet

Usage
-----
    python radcliffe_to_s3.py                                  # full run
    python radcliffe_to_s3.py --skip-upload                    # local dev
    python radcliffe_to_s3.py --limit 3                        # smoke (3 cohorts)
    python radcliffe_to_s3.py --skip-download --skip-upload    # reuse cache
    python radcliffe_to_s3.py --allow-shrink                   # override §1.4

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

BASE = "https://www.radcliffe.harvard.edu"
DASH = "%E2%80%93"  # en-dash used in the cohort filter value
LIST_URL = BASE + "/radcliffe-fellowship/fellows?year%5B%5D={y0}" + DASH + "{y1}&page={page}"

FIRST_COHORT = 2000
LAST_COHORT = 2026   # 2026 = the 2026–2027 cohort; ceiling refreshed by re-run

FUNDER_ID = 4320332388
FUNDER_DISPLAY_NAME = "Radcliffe Institute for Advanced Study, Harvard University"

PROVENANCE = "radcliffe_fellowship"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/radcliffe/radcliffe_fellows.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-radcliffe-ingest/1.0; +https://openalex.org)"

REQUEST_DELAY_S = 0.25
MAX_PAGES_PER_COHORT = 20
DEFAULT_CACHE = Path(".cache/radcliffe_fellows.json")

NAME_PARTICLES = {
    "von", "van", "der", "den", "de", "del", "della", "di", "da", "dos", "das",
    "le", "la", "du", "do", "ten", "ter", "zu", "af", "al", "bin", "ibn", "st", "st.",
}

CARD_RE = re.compile(r'href="(/people/[a-z0-9\-]+)\?[^"]*"[^>]*>\s*<h3 class="m-card__title" data-text="([^"]+)"', re.S)
# fallback: any /people/ link followed by an m-card__title data-text
CARD_RE2 = re.compile(r'/people/([a-z0-9\-]{3,}).{0,400}?m-card__title" data-text="([^"]+)"', re.S)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# HTTP
# =============================================================================

import requests
_session: Optional[requests.Session] = None
_last_t = 0.0


def _http_get(url: str, timeout: int = 30, max_attempts: int = 4) -> str:
    global _session, _last_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(1, max_attempts + 1):
        dt = time.monotonic() - _last_t
        if dt < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - dt)
        try:
            r = _session.get(url, timeout=timeout)
            _last_t = time.monotonic()
            if r.status_code < 500:
                r.raise_for_status()  # 2xx returns; 4xx is permanent -> raise
                return r.text
            last_err = requests.HTTPError(f"HTTP {r.status_code} (server)")
        except (requests.Timeout, requests.ConnectionError) as e:
            _last_t = time.monotonic()
            last_err = e
        # transient (5xx / timeout / connection reset) -> back off and retry,
        # so a single blip never drops a whole cohort.
        if attempt < max_attempts:
            back = min(20, 2 ** attempt)
            log(f"  transient fetch issue ({type(last_err).__name__}) on …{url[-40:]}; retry {attempt}/{max_attempts} in {back}s")
            time.sleep(back)
    raise RuntimeError(f"Radcliffe: fetch failed after {max_attempts} attempts: {url} ({last_err})")


# =============================================================================
# Scrape
# =============================================================================

def parse_cards(htmltext: str) -> list:
    out = []
    seen = set()
    for m in CARD_RE.finditer(htmltext):
        slug = m.group(1).split("/")[-1].strip()
        name = ihtml.unescape(m.group(2)).strip()
        if slug and name and slug not in seen:
            seen.add(slug)
            out.append((slug, name))
    if not out:  # fallback parser
        for m in CARD_RE2.finditer(htmltext):
            slug = m.group(1).strip()
            name = ihtml.unescape(m.group(2)).strip()
            if slug and name and slug not in seen:
                seen.add(slug)
                out.append((slug, name))
    return out


def scrape_all(use_cache: bool, cache_path: Path, limit_cohorts: Optional[int]) -> list:
    if use_cache and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            if isinstance(cached, list) and cached:
                log(f"  loaded {len(cached):,} fellow-cohort rows from cache")
                return cached
        except Exception:
            pass
    rows = []
    seen = set()  # (slug, year)
    cohorts = list(range(FIRST_COHORT, LAST_COHORT + 1))
    if limit_cohorts:
        cohorts = cohorts[-limit_cohorts:]
    for y0 in cohorts:
        y1 = y0 + 1
        cohort_n = 0
        for page in range(1, MAX_PAGES_PER_COHORT + 1):
            url = LIST_URL.format(y0=y0, y1=y1, page=page)
            try:
                htmltext = _http_get(url)  # retries transient flakes internally
            except requests.HTTPError as e:
                # a 404 just means we paged past this cohort's last page
                if getattr(e, "response", None) is not None and e.response.status_code == 404:
                    break
                raise  # other client errors are real -> abort, never silently drop a cohort
            cards = parse_cards(htmltext)
            if not cards:
                break  # 200 with no cards = end of this cohort's pagination
            new_this_page = 0
            for slug, name in cards:
                key = (slug, y0)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"slug": slug, "name": name, "year": y0})
                cohort_n += 1
                new_this_page += 1
            if new_this_page == 0:
                break
        log(f"  cohort {y0}-{y1}: {cohort_n} fellows (total {len(rows):,})")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(rows, ensure_ascii=False))
    log(f"  cached {len(rows):,} fellow-cohort rows")
    return rows


# =============================================================================
# Field parsing
# =============================================================================

def split_name(full: Optional[str]):
    if not full:
        return None, None
    parts = str(full).strip().split()
    _SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while parts and parts[-1].lower().strip(",.") in _SUFFIXES:
        parts.pop()
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
    year = rec.get("year")
    if not slug or not name:
        return None
    given, family = split_name(name)
    return {
        "slug":             slug,
        "funder_award_id":  f"{slug}-{year}" if year else slug,
        "fellow_name":      name,
        "given_name":       given,
        "family_name":      family,
        "start_year":       int(year) if year else None,
        "landing_page_url": f"{BASE}/people/{slug}",
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
        log(f"  cohort year range: {min(years)}–{max(years)}")
        distinct_people = len({r['slug'] for r in rows})
        log(f"  distinct fellows: {distinct_people} | fellow-cohort rows: {n}")


def build_dataframe(rows: list) -> pd.DataFrame:
    return pd.DataFrame(rows).astype("string")  # §1.2.5


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
    parser = argparse.ArgumentParser(description="Fetch Radcliffe Institute Fellows → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="limit to N most-recent cohorts (smoke)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "radcliffe_fellows.parquet"

    log("=== Radcliffe Institute Fellowships ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")

    fellows = scrape_all(args.skip_download, args.cache, args.limit)
    log(f"  scraped {len(fellows):,} fellow-cohort rows")

    rows = [r for r in (build_row(f) for f in fellows) if r is not None]
    log(f"Built {len(rows):,} rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Radcliffe ingest done (local-only) ===")
        return
    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")
    upload_to_s3(output_path)
    log("=== Radcliffe ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
