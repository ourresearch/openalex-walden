#!/usr/bin/env python3
"""
Schmidt Sciences Awardees → S3 Pipeline (FELLOWSHIP PATTERN, method-5 big-page-inline)
========================================================================================

Downloads all Schmidt Sciences (formerly Schmidt Futures, rebranded
2024) awardees from the foundation's own /awardees/ listing page at
schmidtsciences.org. Discovery method-5 static HTML, big-page-inline
variant — the entire 451-row corpus renders in one ~580KB HTML
response with no pagination, no JS, no auth, no admin-ajax. This is
the same shape as Stockholm Water Prize (priority 100).

Each awardee renders as one `<li class="js-filter-item">` element
with these data attributes:

  - data-program          program name (e.g., "AI in Sci", "AI2050",
                          "Schmidt Science Polymaths")
  - data-focus-area       category slug (e.g., "ai-advanced-computing")
  - data-year             year or comma-list of years for multi-year awards
                          (e.g., "2023" or "2020, 2021, 2022, 2023, 2024, 2025")
  - data-term             lowercased searchable name

And visible text:

  - focus-area display name | grantee full name | program display name

Plus an `<a href="https://www.schmidtsciences.org/grantee/{slug}/">`
linking to the grantee's detail page. We don't fetch the detail pages
in this MVP — the listing has everything needed for the awards schema
except per-grantee institution and bio, which the listing doesn't
expose either, so the detail page wouldn't add structured data
matching the schema.

Awarding body in OpenAlex:
  Schmidt Futures (F4026159580, US, no ROR, DOI 10.13039/100027426).
  The organization legally rebranded to "Schmidt Sciences" in 2024 but
  remains the same Crossref Funder ID and OpenAlex funder_id, same as
  the CONACYT/CONAHCYT/SECIHTI precedent (priority 83) where the funder
  was renamed but the OpenAlex record was kept under the original name.

Amount handling:
  amount/currency are NULL with §6.7 waiver. Schmidt Sciences publishes
  program-level amount narrative on each program's overview page (e.g.,
  AI2050 awards range $150k-$2M, Schmidt Science Polymaths get
  $2.5M total over five years) but does NOT publish per-grantee amounts
  on the awardee listing. Per the runbook's prize/fellowship pattern
  source-authority rule, we leave NULL rather than backfill from
  program-level narrative. Fellowship precedent: HHMI #44, CIFAR #79,
  Damon Runyon #73, Packard #95, Rita Allen #107.

Output
------
  s3://openalex-ingest/awards/schmidt_sciences/schmidt_sciences_awardees.parquet

Usage
-----
    python schmidt_sciences_to_s3.py                                  # full run
    python schmidt_sciences_to_s3.py --skip-upload                    # local dev
    python schmidt_sciences_to_s3.py --limit 20                       # smoke
    python schmidt_sciences_to_s3.py --skip-download --skip-upload    # reuse cache
    python schmidt_sciences_to_s3.py --allow-shrink                   # override §1.4

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

LISTING_URL = "https://www.schmidtsciences.org/awardees/"

FUNDER_ID = 4026159580  # Schmidt Futures (legal entity, now operating as Schmidt Sciences)
FUNDER_DISPLAY_NAME = "Schmidt Futures"  # canonical OpenAlex display

PROVENANCE = "schmidt_sciences_awardees"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/schmidt_sciences/schmidt_sciences_awardees.parquet"

USER_AGENT = "openalex-walden-schmidt-sciences-ingest/1.0 (+https://openalex.org)"

DEFAULT_CACHE = Path(".cache/schmidt_sciences_awardees.html")

# Focus-area slug → display name (extracted from the listing's visible text;
# the data-focus-area attribute uses the slug form).
FOCUS_AREA_DISPLAY = {
    "ai-advanced-computing":  "AI & Advanced Computing",
    "astrophysics-space":     "Astrophysics & Space",
    "biosciences":            "Biosciences",
    "climate":                "Climate",
    "science":                "Science Systems",
}


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
    """Split 'Chad A. Mirkin' -> ('Chad A.', 'Mirkin'). Strips degree/suffix tokens."""
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
# Fetch
# =============================================================================

def fetch_listing(use_cache: bool, cache_file: Path) -> str:
    if use_cache and cache_file.exists():
        log(f"Cache hit at {cache_file} ({cache_file.stat().st_size/1e6:.2f} MB)")
        return cache_file.read_text()

    log(f"GET {LISTING_URL}")
    t0 = time.monotonic()
    resp = requests.get(LISTING_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    elapsed = time.monotonic() - t0
    log(f"  status={resp.status_code} size={len(resp.content)/1e6:.2f} MB elapsed={elapsed:.1f}s")

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(resp.text)
    log(f"  cached to {cache_file}")
    return resp.text


# =============================================================================
# Parse
# =============================================================================

def parse_year_range(raw: Optional[str]) -> tuple[Optional[int], Optional[int]]:
    """Parse data-year. May be '2023' or '2020, 2021, 2022, 2023, 2024, 2025' or '' or None.
    Returns (start_year, end_year). Empty -> (None, None). Single -> (Y, Y).
    Multi-year list -> (min, max)."""
    if not raw or not raw.strip():
        return None, None
    parts = [p.strip() for p in raw.split(",")]
    years = []
    for p in parts:
        try:
            y = int(p)
            if 1900 <= y <= 2100:
                years.append(y)
        except ValueError:
            pass
    if not years:
        return None, None
    if len(years) == 1:
        return years[0], years[0]
    return min(years), max(years)


def parse_listing(html: str) -> list[dict]:
    """Extract one row per <li class='js-filter-item'> from the awardees page."""
    soup = BeautifulSoup(html, "html.parser")
    items = soup.find_all("li", class_="js-filter-item")
    log(f"  found {len(items)} <li class='js-filter-item'> elements")

    rows = []
    for li in items:
        # Data attributes
        program     = (li.get("data-program") or "").strip() or None
        focus_area_slug = (li.get("data-focus-area") or "").strip() or None
        year_raw    = (li.get("data-year") or "").strip()

        # Link to /grantee/{slug}/
        a = li.find("a", href=True)
        grantee_url = a.get("href") if a else None
        slug = None
        if grantee_url:
            m = re.search(r"/grantee/([^/]+)/?", grantee_url)
            if m:
                slug = m.group(1)

        # Grantee name — from the link text, or from data-term as fallback (lowercased)
        # Prefer the link text since data-term is lowercased.
        name = None
        if a:
            # The link content may include the program too; look for the first
            # text node that's not in the data-term form
            txt = a.get_text(" ", strip=True)
            # Heuristic: name is the cleanest piece of text. Look at all <p>/<span>
            # inside the link, but if not present, take the link text minus any
            # program/focus-area strings.
            for el in a.find_all(["h3", "h4", "p", "span", "div"]):
                t = el.get_text(strip=True)
                if not t:
                    continue
                if program and t == program:
                    continue
                if focus_area_slug and t == FOCUS_AREA_DISPLAY.get(focus_area_slug):
                    continue
                # Skip data-term lowercased match
                if li.get("data-term") and t.lower() == li.get("data-term"):
                    name = t
                    break
                # Otherwise the first remaining text is likely the name
                if not name and len(t) < 80 and any(c.isalpha() for c in t):
                    name = t
                    break

        # Fallback: derive name from slug (title-case the slug)
        if not name and slug:
            name = " ".join(part.capitalize() for part in slug.split("-"))

        if not name:
            continue

        start_year, end_year = parse_year_range(year_raw)

        focus_area_display = FOCUS_AREA_DISPLAY.get(focus_area_slug) if focus_area_slug else None

        given_name, family_name = split_name(name)

        rows.append({
            "grantee_name":            name,
            "given_name":              given_name,
            "family_name":             family_name,
            "slug":                    slug or slugify(name),
            "program":                 program,
            "focus_area_slug":         focus_area_slug,
            "focus_area_display":      focus_area_display,
            "year_raw":                year_raw or None,
            "start_year":              start_year,
            "end_year":                end_year,
            "grantee_url":             grantee_url,
        })

    return rows


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No grantee rows parsed")
    n = len(rows)
    for f in ("grantee_name", "slug", "program", "focus_area_slug"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")
    year_some = sum(1 for r in rows if r.get("start_year") is not None)
    log(f"  start_year (any year)   coverage {year_some}/{n} ({year_some*100/n:.1f}%)")

    # funder_award_id uniqueness: (slug, start_year, program) — sometimes the
    # same person appears under multiple programs/years.
    keys = [
        f"schmidt-{r.get('start_year', 'na')}-{slugify(r.get('program') or 'none')}-{r['slug']}"
        for r in rows
    ]
    if len(keys) != len(set(keys)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(keys).items() if v > 1][:5]
        raise RuntimeError(
            f"funder_award_id collisions: {len(keys) - len(set(keys))} duplicates. "
            f"Example: {dups}. (slug, year, program) was meant to be a unique tuple; "
            f"if Schmidt genuinely has same-person-same-program-same-year duplicates "
            f"add a row-index disambiguator."
        )
    log(f"  funder_award_id uniqueness: {len(keys)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = (
        "schmidt-" + df["start_year"].fillna(0).astype(int).astype(str)
        + "-" + df["program"].fillna("none").apply(slugify)
        + "-" + df["slug"]
    )
    # Runbook §1.2.5: force string dtype before to_parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check (runbook §1.4) + upload
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
    parser = argparse.ArgumentParser(description="Fetch Schmidt Sciences awardees → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                        help="Output directory; parquet written to {output-dir}/schmidt_sciences_awardees.parquet")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Path to cache the downloaded listing HTML")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse cached listing HTML")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but skip S3 upload")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook §1.4 shrink-check. Use only after confirming intent.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N parsed rows (smoke test)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "schmidt_sciences_awardees.parquet"

    log(f"=== Schmidt Sciences awardees ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    html = fetch_listing(args.skip_download, args.cache)
    rows = parse_listing(html)

    if args.limit is not None:
        rows = rows[:args.limit]
        log(f"--limit {args.limit}: truncating to {len(rows)} rows")

    log(f"Parsed {len(rows)} awardee rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")

    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Schmidt Sciences awardees ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")

    upload_to_s3(output_path)
    log("=== Schmidt Sciences awardees ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
