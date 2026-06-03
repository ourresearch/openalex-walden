#!/usr/bin/env python3
"""
Conrad N. Hilton Foundation Grants → S3 Pipeline (GRANT PATTERN, method-5 WP REST + static HTML)
===============================================================================================

Downloads grants from the Conrad N. Hilton Foundation's own
WordPress-published grants archive at hiltonfoundation.org. The
foundation is a large US private foundation funding humanitarian work
across program areas such as Safe Water, Homelessness, Foster Youth,
Catholic Sisters, Early Childhood Development, Avoidable Blindness,
Multiple Sclerosis, Opportunity Youth, and the Hilton Humanitarian Prize.

Discovery (method-5 WP REST): the full corpus is enumerated from the
WordPress `grant` custom-post-type REST endpoint, which exposes one
object per grant carrying the recipient organization (post title), the
slug, the canonical landing URL, and taxonomy terms (program-area, area,
grant_year) embedded in `class_list`.

  /wp-json/wp/v2/grant?per_page=100&page=N      ~2,552 grants (26 pages)
  /wp-json/wp/v2/program-area?per_page=100      program-area term -> label
  /wp-json/wp/v2/grant_year?per_page=100        grant_year term   -> year label

The REST object does NOT expose the dollar amount or the project dates
(`content.rendered` is empty; ACF fields are not surfaced). Those live in
each grant's server-rendered detail page, in a `<ul class="grant-info-list">`
sidebar of `<li><strong>Label:</strong> value</li>` rows:

  Grantee Name            recipient organization (== post title)
  Project Description      abstract
  Grant Amount             e.g., $3000000        (where published)
  Awarded Date             e.g., November, 2021
  Project Start Date       e.g., December, 2021
  Project End Date         e.g., August, 2025
  Term (Months)            e.g., 45
  Area Served              e.g., Africa          (a region, NOT a country)
  Related Priority         Yes/No
  Website URL              the grantee's site

This is an ORG-LEVEL grant funder: each grant is led by the grantee
organization (no named principal investigator). lead_investigator
therefore carries given/family NULL and affiliation.name = grantee org.

Native award ID is the URL slug (e.g.
`/grant/world-resources-institute-2/` -> `world-resources-institute-2`),
which is stable, unique, and source-authoritative.

Awarding body in OpenAlex:
  Conrad N. Hilton Foundation (F4320306180, US, ROR 05g9snv96,
  DOI 10.13039/100000910).

Amount: published as a dollar figure on a subset of grant pages;
populated where present, NULL otherwise (NOT a blanket §6.7 waiver;
never imputed). Currency USD hardcoded (single-country US foundation),
set only where amount present. funder_scheme = the program-area label.

Output
------
  s3://openalex-ingest/awards/hilton/hilton_grants.parquet

Usage
-----
    python hilton_to_s3.py                                  # full run
    python hilton_to_s3.py --skip-upload                    # local dev
    python hilton_to_s3.py --limit 10                       # smoke
    python hilton_to_s3.py --skip-download --skip-upload    # reuse cache
    python hilton_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import html as ihtml
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

BASE = "https://www.hiltonfoundation.org"
GRANT_REST = BASE + "/wp-json/wp/v2/grant"
PROGRAM_AREA_REST = BASE + "/wp-json/wp/v2/program-area"
GRANT_YEAR_REST = BASE + "/wp-json/wp/v2/grant_year"

FUNDER_ID = 4320306180
FUNDER_DISPLAY_NAME = "Conrad N. Hilton Foundation"

PROVENANCE = "hilton_foundation"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hilton/hilton_grants.parquet"

USER_AGENT = "openalex-walden-hilton-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/hilton_pages.json")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30):
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
    return resp


def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def get_page(url: str, cache: dict, use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _http_get(url).text
    cache[url] = html
    return html


# =============================================================================
# Taxonomy maps + discovery
# =============================================================================

def fetch_taxonomy_map(endpoint: str) -> dict:
    """slug -> human label for a WP taxonomy (program-area, grant_year)."""
    out = {}
    page = 1
    while True:
        url = f"{endpoint}?per_page=100&page={page}"
        try:
            terms = _http_get(url).json()
        except Exception:
            break
        if not isinstance(terms, list) or not terms:
            break
        for t in terms:
            slug = t.get("slug")
            name = ihtml.unescape(t.get("name", "") or "").strip()
            if slug:
                out[slug] = name or slug
        if len(terms) < 100:
            break
        page += 1
    return out


def discover_grants(use_cache: bool, cache: dict) -> list:
    """Enumerate grant CPT objects via WP REST pagination.

    Returns list of dicts: id, slug, link, title (grantee org),
    program_slug, area_slug, grant_year_slug.
    """
    grants = []
    page = 1
    while True:
        url = (f"{GRANT_REST}?per_page=100&page={page}"
               "&_fields=id,slug,link,title,class_list")
        cache_key = f"REST::{url}"
        if use_cache and cache_key in cache:
            objs = json.loads(cache[cache_key])
        else:
            try:
                resp = _http_get(url)
            except requests.HTTPError as e:
                # WP returns 400 once page > totalpages
                if e.response is not None and e.response.status_code == 400:
                    break
                raise
            objs = resp.json()
            cache[cache_key] = json.dumps(objs, ensure_ascii=False)
        if not isinstance(objs, list) or not objs:
            break
        for g in objs:
            classes = g.get("class_list", []) or []
            prog = next((c[len("program-area-"):] for c in classes
                         if c.startswith("program-area-")), None)
            area = next((c[len("area-"):] for c in classes
                         if c.startswith("area-")), None)
            gyear = next((c[len("grant_year-"):] for c in classes
                          if c.startswith("grant_year-")), None)
            title = ihtml.unescape(((g.get("title") or {}).get("rendered") or "")).strip()
            grants.append({
                "id":         g.get("id"),
                "slug":       g.get("slug"),
                "link":       g.get("link"),
                "title":      title or None,
                "prog_slug":  prog,
                "area_slug":  area,
                "gyear_slug": gyear,  # term id as string, not a year
            })
        log(f"  enumerated REST page {page}: {len(objs)} grants (total {len(grants)})")
        if len(objs) < 100:
            break
        page += 1
    return grants


# =============================================================================
# Detail-page parse
# =============================================================================

AMOUNT_NUMBER_RE = re.compile(r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)")
# "December, 2021" or "December 2021"
MONTH_YEAR_RE = re.compile(r"([A-Za-z]+)\.?,?\s+(\d{4})")
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")


def parse_amount(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = AMOUNT_NUMBER_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_month_year(s: Optional[str]):
    """'December, 2021' -> (12, 2021); fallback to bare year."""
    if not s:
        return None, None
    m = MONTH_YEAR_RE.search(s)
    if m and m.group(1).lower() in MONTHS:
        return MONTHS[m.group(1).lower()], int(m.group(2))
    y = YEAR_RE.search(s)
    if y:
        return None, int(y.group(1))
    return None, None


def parse_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group(0)) if m else None


def parse_detail(html: str) -> dict:
    """Parse the <ul class='grant-info-list'> sidebar into {label: (text, href)}."""
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.find("ul", class_="grant-info-list")
    fields = {}
    if not ul:
        return fields
    for li in ul.find_all("li", recursive=False):
        strong = li.find("strong")
        if not strong:
            continue
        label = strong.get_text(" ", strip=True).rstrip(":").strip().lower()
        full = li.get_text(" ", strip=True)
        lab_txt = strong.get_text(" ", strip=True)
        val = full
        if full.startswith(lab_txt):
            val = full[len(lab_txt):].strip()
        a = li.find("a")
        href = a.get("href") if a else None
        fields[label] = (ihtml.unescape(val).strip() or None, href)
    return fields


def build_row(meta: dict, html: str, prog_map: dict) -> Optional[dict]:
    f = parse_detail(html)

    def fv(label):
        return (f.get(label) or (None, None))[0]

    slug = meta["slug"]
    if not slug:
        return None

    grantee_org = fv("grantee name") or meta.get("title")
    description = fv("project description")
    amount = parse_amount(fv("grant amount"))
    _, awarded_year = parse_month_year(fv("awarded date"))
    _, start_year = parse_month_year(fv("project start date"))
    _, end_year = parse_month_year(fv("project end date"))
    term_months = parse_int(fv("term (months)"))
    area_served = fv("area served")
    related_priority = fv("related priority")
    website_url = (f.get("website url") or (None, None))[1]

    # start_year: prefer Project Start Date, then Awarded Date.
    if start_year is None:
        start_year = awarded_year

    program = None
    if meta.get("prog_slug"):
        program = prog_map.get(meta["prog_slug"]) or meta["prog_slug"].replace("-", " ").title()

    return {
        "slug":             slug,
        "funder_award_id":  slug,
        "title":            grantee_org,
        "grantee_org":      grantee_org,
        "description":      description,
        "program":          program,
        "program_slug":     meta.get("prog_slug"),
        "area_served":      area_served or (meta.get("area_slug").replace("-", " ").title()
                                            if meta.get("area_slug") else None),
        "amount":           amount,
        "amount_raw":       fv("grant amount"),
        "currency":         CURRENCY if amount is not None else None,
        "awarded_year":     awarded_year,
        "start_year":       start_year,
        "end_year":         end_year,
        "term_months":      term_months,
        "related_priority": related_priority,
        "website_url":      website_url,
        "landing_page_url": meta.get("link"),
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "slug", "program", "amount", "start_year", "end_year",
              "grantee_org", "description", "area_served"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<16} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    no_lead = [r["slug"] for r in rows if not r.get("grantee_org")]
    if no_lead:
        log(f"  WARN: {len(no_lead)} rows with no grantee org (e.g. {no_lead[:3]})")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ok")

    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}–{max(years)}")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        srt = sorted(amts)
        log(f"  amount stats: n={len(amts)} min=${min(amts):,.0f} "
            f"median=${srt[len(srt)//2]:,.0f} max=${max(amts):,.0f} "
            f"total=${sum(amts):,.0f}")

    progs = {}
    for r in rows:
        p = r.get("program") or "(none)"
        progs[p] = progs.get(p, 0) + 1
    log("  programs: " + ", ".join(f"{k}={v}" for k, v in sorted(progs.items(), key=lambda x: -x[1])))


def build_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # §1.2.5: force string dtype to stop pandas int-inferring null-heavy cols.
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
    parser = argparse.ArgumentParser(description="Fetch Conrad N. Hilton Foundation grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "hilton_grants.parquet"

    log("=== Conrad N. Hilton Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    log("Fetching program-area taxonomy map...")
    prog_map = fetch_taxonomy_map(PROGRAM_AREA_REST)
    log(f"  {len(prog_map)} program-area terms")

    grants = discover_grants(args.skip_download, cache)
    save_cache(args.cache, cache)
    log(f"  enumerated {len(grants)} grants total")

    if args.limit is not None:
        grants = grants[:args.limit]
        log(f"--limit {args.limit}: processing {len(grants)} grants")

    rows = []
    for i, meta in enumerate(grants, 1):
        url = meta.get("link")
        if not url:
            continue
        try:
            html = get_page(url, cache, args.skip_download)
        except Exception as e:
            log(f"  [{i}/{len(grants)}] FETCH FAIL {url}: {type(e).__name__}: {e}")
            continue
        row = build_row(meta, html, prog_map)
        if row is None:
            log(f"  [{i}/{len(grants)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 100 == 0:
            log(f"  [{i}/{len(grants)}] parsed (total so far: {len(rows)})")
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
        log("=== Hilton ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Hilton ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
