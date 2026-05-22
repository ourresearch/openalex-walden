#!/usr/bin/env python3
"""
Nuffield Foundation to S3 Data Pipeline (REST + landing-page hybrid)
=====================================================================

Downloads Nuffield Foundation grant projects from the
nuffieldfoundation.org WordPress REST API + per-project landing pages.

Why a hybrid scrape? The REST API at `/wp-json/wp/v2/project` exposes
title, content, categories, and link for all 663 projects — but the
fields that matter for the awards table (`amount` in GBP, date range,
theme, lead researchers) live ONLY on each project's rendered landing
page, inside `<div class="article-meta"><svg class="icon awarded">...
£27,692</div>`-style blocks. This script pulls the cheap metadata from
the REST API in one pass, then fetches each landing page and parses
the visible meta via regex anchored on the `icon awarded` / `icon date`
/ `Researchers:` markers.

This is a new pattern in the awards pipeline — most existing scripts
are either REST-only (Holberg, CIFAR) or pure HTML scrape (Wolf,
Lasker). Documented here as a precedent for future foundations whose
REST API exposes only navigation metadata.

Source authority
----------------
nuffieldfoundation.org is the awarding body's own site (Wordpress
REST API + public landing pages, no auth, CC BY-style content). No
Wikipedia/Wikidata.

Output
------
s3://openalex-ingest/awards/nuffield/nuffield_projects.parquet

Usage
-----
    python nuffield_to_s3.py                    # full run (default)
    python nuffield_to_s3.py --skip-upload      # local dev / smoke test
    python nuffield_to_s3.py --skip-download    # reuse cached raw JSON
    python nuffield_to_s3.py --limit 20         # smoke-test on 20 projects
    python nuffield_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
    AWS CLI configured for s3://openalex-ingest/awards/nuffield/
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

WP_BASE = "https://www.nuffieldfoundation.org/wp-json/wp/v2"
SITE_BASE = "https://www.nuffieldfoundation.org"

# Awarding body — Nuffield Foundation. Verified F4320319997, country GB.
FUNDER_ID = 4320319997
FUNDER_DISPLAY_NAME = "Nuffield Foundation"

PROVENANCE = "nuffield_wp_rest"
CURRENCY = "GBP"  # Hardcoded — Nuffield is UK-only

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/nuffield/nuffield_projects.parquet"

# Polite throttle (~2 req/sec). 663 landing-page fetches at this rate = ~6 min.
MIN_REQUEST_INTERVAL_S = 0.5
USER_AGENT = "openalex-walden-nuffield-ingest/1.0 (+https://openalex.org)"


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, params: Optional[dict] = None, timeout: int = 60,
              accept_html: bool = False) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    headers = {"Accept": "text/html"} if accept_html else {"Accept": "application/json"}
    resp = _session.get(url, params=params, timeout=timeout, headers=headers)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Smoke-test gate
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: REST API reachable + landing pages parsable")
    print("=" * 60)
    resp = _http_get(f"{WP_BASE}/project", params={"per_page": 1, "page": 1})
    resp.raise_for_status()
    total = resp.headers.get("X-WP-Total")
    print(f"  /wp/v2/project total: {total}")
    if not total:
        print("[ERROR] no X-WP-Total header — API shape changed?")
        sys.exit(3)
    try:
        n = int(total)
        if n < 100 or n > 5000:
            print(f"  [WARN] project count {n} outside expected 100-5000 range")
    except ValueError:
        pass
    # Sanity: a single landing page parses
    sample = resp.json()
    if sample:
        link = sample[0]["link"]
        page = _http_get(link, accept_html=True)
        page.raise_for_status()
        amount = parse_landing_amount(page.text)
        print(f"  smoke landing-page amount: {amount}")
        if amount is None:
            print("  [WARN] sample page parsed but no amount — markup may have changed")


# =============================================================================
# Landing-page parsing
# =============================================================================

# Anchored on the unique <svg class="icon awarded ..."> + amount text after.
# Example markup:
#   <svg class="icon awarded " role="img"><use xlink:href="#awarded"></use></svg>£27,692
_AMOUNT_RE = re.compile(
    r'icon\s+awarded\b[^>]*>(?:\s*<use[^>]*>\s*</use>)?\s*</svg>\s*([£$€]?[\d,\.]+[KMm]?)',
    re.IGNORECASE,
)
# Date range — same icon pattern with class "icon date".
# Example: <svg class="icon date">...</svg>May 2026  -  April 2027
_DATE_RE = re.compile(
    r'icon\s+date\b[^>]*>(?:\s*<use[^>]*>\s*</use>)?\s*</svg>\s*([^<]+?)\s*(?:</div>|<)',
    re.IGNORECASE | re.DOTALL,
)
# Theme — same pattern with class "icon education" (or other discipline icons).
# Use a generic capture of any icon-classed svg-meta block.
_THEME_RE = re.compile(
    r'icon\s+([a-z][a-z\-]+)\b[^>]*>(?:\s*<use[^>]*>\s*</use>)?\s*</svg>\s*([^<]+?)\s*</div>',
    re.IGNORECASE,
)
# Researchers list — `<p>Researchers: <strong>...</strong>|<strong>...</strong></p>`
_RESEARCHERS_RE = re.compile(
    r'(?:Researchers?|Project lead|Lead\s+researcher)s?:\s*((?:\s*<strong[^>]*>[^<]+</strong>\s*(?:<span[^>]*>\s*\|\s*</span>)?)+)',
    re.IGNORECASE,
)
_STRONG_RE = re.compile(r'<strong[^>]*>([^<]+)</strong>')

# Excluded icon classes (these aren't grant themes — they're metadata icons).
_NON_THEME_ICONS = {"awarded", "date", "calendar", "clock", "share", "twitter",
                    "facebook", "linkedin", "bsky", "email", "print", "download",
                    "bluesky", "instagram", "youtube", "rss", "search", "menu"}


def parse_landing_amount(html: str) -> Optional[str]:
    m = _AMOUNT_RE.search(html)
    return m.group(1).strip() if m else None


def parse_landing_dates(html: str) -> Optional[str]:
    m = _DATE_RE.search(html)
    if not m:
        return None
    return re.sub(r'\s+', ' ', m.group(1)).strip()


def parse_landing_theme(html: str) -> Optional[str]:
    # First matching icon-XYZ pair that isn't a metadata icon (awarded/date/social).
    for m in _THEME_RE.finditer(html):
        icon_class = m.group(1).lower()
        text = re.sub(r'\s+', ' ', m.group(2)).strip()
        if icon_class in _NON_THEME_ICONS:
            continue
        # Discard if the text is a £/$/€ amount (caught by amount regex too)
        if text.startswith(('£', '$', '€')):
            continue
        # First plausible theme wins
        return text
    return None


def parse_landing_researchers(html: str) -> list[str]:
    m = _RESEARCHERS_RE.search(html)
    if not m:
        return []
    return [unescape(s.strip()) for s in _STRONG_RE.findall(m.group(1))]


# Normalize an amount string like "£27,692" or "$2.5M" → float pounds.
def parse_amount_to_float(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    s = raw.strip()
    # strip currency prefix
    s = re.sub(r'^[£$€]', '', s).strip()
    multiplier = 1.0
    if s.endswith(('K', 'k')):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith(('M', 'm')):
        multiplier = 1_000_000.0
        s = s[:-1]
    try:
        return float(s.replace(',', '')) * multiplier
    except ValueError:
        return None


# Parse "May 2026 - April 2027" or "May 2026" → (start_date_str, end_date_str)
_MONTH_YEAR_RE = re.compile(
    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
    re.IGNORECASE,
)
_MONTH_TO_NUM = {m.lower(): i+1 for i,m in enumerate(
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"])}


def parse_date_range(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not raw:
        return (None, None)
    matches = _MONTH_YEAR_RE.findall(raw)
    if not matches:
        return (None, None)
    def fmt(m):
        month, year = m
        return f"{year}-{_MONTH_TO_NUM[month.lower()]:02d}-01"
    start = fmt(matches[0]) if matches else None
    end = fmt(matches[1]) if len(matches) >= 2 else None
    return (start, end)


# Conservative name-split (matches Holberg/CIFAR idiom).
_DEGREE_SUFFIXES = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor", "Mr", "Mr.",
                  "Mrs", "Mrs.", "Ms", "Ms.", "Sir", "Dame", "Lord", "Lady"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    while tokens and tokens[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


# =============================================================================
# Download
# =============================================================================

def fetch_all_projects() -> list[dict]:
    """Pull every project record from the REST API."""
    print("\n" + "=" * 60)
    print("Step 1a: Download project list from /wp/v2/project")
    print("=" * 60)
    projects: list[dict] = []
    page = 1
    while True:
        resp = _http_get(f"{WP_BASE}/project", params={"per_page": 100, "page": page,
                                                       "_embed": "false"})
        if resp.status_code != 200:
            print(f"  [ERROR] page {page}: HTTP {resp.status_code}")
            break
        batch = resp.json()
        if not batch:
            break
        projects.extend(batch)
        print(f"  page {page}: +{len(batch)} (total {len(projects)})")
        if len(batch) < 100:
            break
        page += 1
    print(f"\n  Fetched {len(projects)} project records")
    return projects


def enrich_with_landings(projects: list[dict], limit: Optional[int],
                         output_dir: Path) -> Path:
    """For each project, fetch its landing page and parse meta blocks."""
    print("\n" + "=" * 60)
    print("Step 1b: Scrape per-project landing pages for amount/date/theme/researchers")
    print("=" * 60)
    if limit:
        projects = projects[:limit]
        print(f"  [LIMIT] only enriching first {limit} of original list")
    out: list[dict] = []
    for i, p in enumerate(projects, 1):
        link = p["link"]
        try:
            resp = _http_get(link, accept_html=True)
            if resp.status_code != 200:
                print(f"  [{i}/{len(projects)}] {link} -> HTTP {resp.status_code}; "
                      f"emitting REST-only row")
                html = ""
            else:
                html = resp.text
        except Exception as e:
            print(f"  [{i}/{len(projects)}] {link} -> error {e}; emitting REST-only row")
            html = ""
        amount_raw = parse_landing_amount(html)
        date_raw = parse_landing_dates(html)
        theme = parse_landing_theme(html)
        researchers = parse_landing_researchers(html)
        # REST content excerpt (strip HTML)
        content = re.sub(r'<[^>]+>', ' ', p["content"]["rendered"])
        content = unescape(re.sub(r'\s+', ' ', content)).strip()
        out.append({
            "wp_id":              p["id"],
            "slug":               p["slug"],
            "title":              unescape(p["title"]["rendered"]),
            "landing_page_url":   link,
            "date_posted":        p.get("date"),
            "modified":           p.get("modified"),
            "content_text":       content,
            "amount_raw":         amount_raw,
            "date_raw":           date_raw,
            "theme":              theme,
            "researchers":        "|".join(researchers) if researchers else None,
            "categories":         "|".join(str(c) for c in (p.get("categories") or [])) or None,
        })
        if i % 25 == 0 or i == len(projects):
            n_amt = sum(1 for r in out if r["amount_raw"])
            n_dt = sum(1 for r in out if r["date_raw"])
            n_th = sum(1 for r in out if r["theme"])
            n_rs = sum(1 for r in out if r["researchers"])
            print(f"  [{i}/{len(projects)}] running coverage: "
                  f"amount={n_amt} dates={n_dt} theme={n_th} researchers={n_rs}")
    raw_path = output_dir / "nuffield_projects_raw.json"
    raw_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n  Cached {len(out)} enriched records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        funder_award_id = f"nuffield-{r['slug']}"
        if funder_award_id in seen_ids:
            raise RuntimeError(
                f"Duplicate funder_award_id {funder_award_id!r}. WP slugs "
                f"should be unique per project — investigate raw payload."
            )
        seen_ids.add(funder_award_id)
        amount_f = parse_amount_to_float(r.get("amount_raw"))
        start, end = parse_date_range(r.get("date_raw"))
        # First researcher → lead, rest → co-leads / investigators array
        researchers = (r.get("researchers") or "").split("|") if r.get("researchers") else []
        lead = researchers[0] if researchers else ""
        lead_given, lead_family = split_name(lead) if lead else ("", "")
        co_names = researchers[1:] if len(researchers) > 1 else []
        rows.append({
            "funder_award_id":    funder_award_id,
            "wp_id":              r["wp_id"],
            "slug":               r["slug"],
            "title":              r["title"],
            "description":        r["content_text"][:5000] if r["content_text"] else None,
            "theme":              r.get("theme"),
            "amount":             amount_f,
            "currency":           CURRENCY if amount_f is not None else None,
            "start_date":         start,
            "end_date":           end,
            "lead_full_name":     lead,
            "lead_given_name":    lead_given,
            "lead_family_name":   lead_family,
            "co_investigators":   "|".join(co_names) if co_names else None,
            "landing_page_url":   r["landing_page_url"],
            "first_seen_date":    r["date_posted"],
            "declined":           False,  # schema parity; no declined Nuffield projects on record
        })
    df = pd.DataFrame.from_records(rows)
    n_amt = df["amount"].notna().sum()
    n_dt = df["start_date"].notna().sum()
    n_th = df["theme"].notna().sum()
    n_rs = df["lead_full_name"].astype(bool).sum()
    print(f"  rows: {len(df)}  amount={n_amt} ({n_amt*100/len(df):.0f}%) "
          f"dates={n_dt} ({n_dt*100/len(df):.0f}%) "
          f"theme={n_th} ({n_th*100/len(df):.0f}%) "
          f"researchers={n_rs} ({n_rs*100/len(df):.0f}%)")
    print(f"\n  By theme (top 10):")
    print(df.groupby("theme").size().sort_values(ascending=False).head(10).to_string())
    # Runbook §1.2.5 — cast all columns to string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "nuffield_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4 — never shrink the corpus on re-ingest."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_nuffield_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"    [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(
            f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Investigate first."
        )
        return False
    print(f"    [OK] new corpus not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/nuffield"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse nuffield_projects_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only enrich the first N projects (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Nuffield Foundation → S3 Pipeline (REST + landing-page hybrid)")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "nuffield_projects_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        projects = fetch_all_projects()
        raw_path = enrich_with_landings(projects, args.limit, args.output_dir)

    df = build_dataframe(raw_path)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateNuffieldAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
