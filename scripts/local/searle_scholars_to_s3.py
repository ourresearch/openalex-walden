#!/usr/bin/env python3
"""
Searle Scholars Program to S3 Data Pipeline
============================================

Downloads Searle Scholars from searlescholars.org's public scholars
listing page plus latest-year announcement posts not yet folded into
that listing.

Source authority
----------------
searlescholars.org is the awarding body's own site (WordPress, public,
no auth). Its `/current-scholars/` page renders ALL recent-cohort
scholars as a category-tagged grid of `<article class="obb-content
category-{year}-scholar">` cards with name, profile URL, institution,
and research title in well-structured HTML. The site's WP REST
`/wp-json/wp/v2/posts` endpoint is also public and surfaces the latest
class-announcement blog posts (e.g.
`/{year}/MM/DD/searle-scholars-program-names-15-scientists-as-searle-scholars-for-{year}/`)
which use the same `<a><strong>Name</strong></a><br>Institution<em><br>Title</em>`
markup pattern. We use both sources and dedup by (year, name slug).

Amount and currency
-------------------
Verified from the 2025 class-announcement post on searlescholars.org:
"Each receives an award of $300,000 in flexible funding to support
their own work over the next three years." All 4 most recent
announcement posts confirm the same USD 300,000 / 3-year structure.
We ship `amount=300000.0`, `currency='USD'`.

Coverage scope
--------------
`/current-scholars/` lists the 6 most recent cohorts (typically 2020-
2025 at the time of ingest). The 2026 announcement post added 15 more.
Total expected: ~105 scholars. The pre-2020 archive (~615 scholars
back to 1981) is NOT exposed on the current site — the foundation
quotes "707 total since 1981" in the 2025 announcement but does not
publish the historical list. Document as a Step 0 follow-up; do not
fabricate from third-party sources.

Output
------
s3://openalex-ingest/awards/searle_scholars/searle_scholars.parquet

Usage
-----
    python searle_scholars_to_s3.py                    # full run
    python searle_scholars_to_s3.py --skip-upload      # local dev
    python searle_scholars_to_s3.py --skip-download    # reuse cached JSON
    python searle_scholars_to_s3.py --limit 5          # smoke test (first N cohorts)
    python searle_scholars_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
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

SITE_BASE = "https://searlescholars.org"
CURRENT_SCHOLARS_URL = f"{SITE_BASE}/current-scholars/"
WP_POSTS_URL = f"{SITE_BASE}/wp-json/wp/v2/posts"

# Awarding body — Searle Scholars Program.
# Verified F4320314849, country US, DOI 10.13039/100012316.
FUNDER_ID = 4320314849
FUNDER_DISPLAY_NAME = "Searle Scholars Program"

PROVENANCE = "searle_scholars"

# Official program parameters, verified from 2025 class-announcement post.
AMOUNT_USD       = 300000.0
DURATION_YEARS   = 3

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/searle_scholars/searle_scholars.parquet"

USER_AGENT = "openalex-walden-searle-ingest/1.0 (+https://openalex.org)"

# Polite throttle for the announcement-post follow-up fetches.
MIN_REQUEST_INTERVAL_S = 0.5

# Regex matching class-announcement post URLs:
#   https://searlescholars.org/{year}/MM/DD/searle-scholars-program-names-{N}-scientists-as-searle-scholars-for-{year}/
_ANNOUNCEMENT_URL_RE = re.compile(
    r"^https://searlescholars\.org/(20[0-9]{2})/[0-9]{2}/[0-9]{2}/searle-scholars-program-names-[0-9]+-scientists-as-searle-scholars-for-(20[0-9]{2})/?$"
)


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    return unescape(re.sub(r"\s+", " ", s)).strip()


# =============================================================================
# Parser: /current-scholars/ grid
# =============================================================================

# Each scholar is one <article class="obb-content ... category-{year}-scholar">...</article>.
# Inside: <h4 class="obb-title"><a href="profile-url">Name</a></h4>
#         <div class="obb-excerpt"><p>Institution<br />Research Title</p></div>
_CURRENT_ARTICLE_RE = re.compile(
    r'<article class="(obb-content[^"]*)"[^>]*>(.*?)</article>', re.DOTALL
)
_OBB_TITLE_RE = re.compile(
    r'<h4 class="obb-title">\s*<a href="([^"]+)">([^<]+)</a>\s*</h4>', re.DOTALL
)
_OBB_EXCERPT_RE = re.compile(
    r'<div class="obb-excerpt">\s*<p>(.*?)</p>', re.DOTALL
)
_YEAR_FROM_CLASS_RE = re.compile(r"category-(20[0-9]{2})-scholar")


def parse_current_scholars(html: str) -> list[dict]:
    """Return one dict per scholar on the /current-scholars/ grid."""
    rows: list[dict] = []
    for m in _CURRENT_ARTICLE_RE.finditer(html):
        classes = m.group(1)
        body = m.group(2)
        yr_m = _YEAR_FROM_CLASS_RE.search(classes)
        if not yr_m:
            continue
        name_m = _OBB_TITLE_RE.search(body)
        if not name_m:
            continue
        exc_m = _OBB_EXCERPT_RE.search(body)
        institution = title = None
        if exc_m:
            parts = re.split(r"<br\s*/?>", exc_m.group(1))
            if parts:
                institution = _strip_tags(parts[0]) or None
            if len(parts) > 1:
                title = _strip_tags(parts[1]) or None
        rows.append({
            "year":           int(yr_m.group(1)),
            "name":           unescape(name_m.group(2)).strip(),
            "profile_url":    name_m.group(1),
            "institution":    institution,
            "research_title": title,
            "source_page":    CURRENT_SCHOLARS_URL,
        })
    return rows


# =============================================================================
# Parser: class-announcement blog post
# =============================================================================

# In the announcement post body, each scholar is one
# <p class="wp-block-paragraph">
#   <a href="lab-url" ...><strong>Name</strong></a><br>
#   Institution<em><br>
#   Research Title</em>
# </p>
_ANNOUNCEMENT_PARA_RE = re.compile(
    r'<p class="wp-block-paragraph">(.*?)</p>', re.DOTALL
)


def parse_announcement_post(html: str, year: int, source_url: str) -> list[dict]:
    """Return scholars listed in an announcement post body."""
    rows: list[dict] = []
    seen: set[str] = set()
    for m in _ANNOUNCEMENT_PARA_RE.finditer(html):
        block = m.group(1)
        # Two markup variants observed across years:
        #   2025-style:  <a href="..."><strong>Name</strong></a>
        #   2026-style:  <strong><a href="...">Name</a></strong>
        # Try both (in either order) before falling back to a bare <strong>.
        profile_url: Optional[str] = None
        name: Optional[str] = None
        a_strong = re.search(
            r'<a\s+href="([^"]*)"[^>]*>\s*<strong>([^<]+)</strong>\s*</a>',
            block,
            re.DOTALL,
        )
        strong_a = re.search(
            r'<strong>\s*<a\s+href="([^"]*)"[^>]*>([^<]+)</a>\s*</strong>',
            block,
            re.DOTALL,
        )
        if a_strong:
            profile_url = a_strong.group(1) or None
            name = unescape(a_strong.group(2)).strip()
        elif strong_a:
            profile_url = strong_a.group(1) or None
            name = unescape(strong_a.group(2)).strip()
        else:
            strong_m = re.search(r"<strong>([^<]+)</strong>", block)
            if not strong_m:
                continue
            name = unescape(strong_m.group(1)).strip()

        # Title in <em>
        em_m = re.search(r"<em>(.*?)</em>", block, re.DOTALL)
        title = _strip_tags(em_m.group(1)) if em_m else None
        # Institution = text immediately after the </a> (or </strong>) and before <em>
        # Strip all tags from the block, then split on the name and the title.
        flat = _strip_tags(block)
        if name and title and name in flat and title in flat:
            after_name = flat.split(name, 1)[1]
            before_title = after_name.split(title, 1)[0]
            institution = before_title.strip(" ,; ") or None
        else:
            institution = None

        if not name or name.lower() in {"about the searle scholars program", "name"}:
            continue
        slug_key = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        if slug_key in seen:
            continue
        seen.add(slug_key)
        rows.append({
            "year":           year,
            "name":           name,
            "profile_url":    profile_url,
            "institution":    institution,
            "research_title": title,
            "source_page":    source_url,
        })
    return rows


# =============================================================================
# Discover class-announcement posts via WP REST
# =============================================================================

def discover_announcement_posts(known_years: set[int]) -> list[tuple[int, str]]:
    """List (year, post_url) for class-announcement blog posts whose year
    is NOT already covered by /current-scholars/.

    Pagination terminator is `X-WP-TotalPages` from the FIRST page response,
    per how-to-add-a-funder-v2.md §1 ("empty page != end of corpus"). A
    mid-walk empty page or transient 4xx no longer truncates the corpus.
    """
    out: list[tuple[int, str]] = []
    page = 1
    total_pages: Optional[int] = None
    consecutive_failures = 0
    MAX_PAGES_FALLBACK = 100  # safety net if X-WP-TotalPages is missing
    while True:
        r = _http_get(f"{WP_POSTS_URL}?per_page=100&page={page}")
        if total_pages is None:
            try:
                total_pages = int(r.headers.get("X-WP-TotalPages") or 0) or None
            except (TypeError, ValueError):
                total_pages = None
        if r.status_code == 400 and total_pages is not None and page > total_pages:
            # WP returns 400 for page > X-WP-TotalPages; we already hit the end.
            break
        if r.status_code != 200:
            consecutive_failures += 1
            print(f"  [searle WP] non-200 (status {r.status_code}) on page {page}; "
                  f"consecutive_failures={consecutive_failures}")
            if consecutive_failures >= 3:
                raise RuntimeError(
                    f"discover_announcement_posts: {consecutive_failures} consecutive "
                    f"non-200 responses ending at page {page}; refusing to short-circuit "
                    f"corpus walk (total_pages={total_pages})."
                )
            page += 1
            if total_pages is not None and page > total_pages:
                break
            if page > MAX_PAGES_FALLBACK:
                break
            continue
        consecutive_failures = 0
        posts = r.json() if r.text.strip() else []
        if not posts:
            # Empty page mid-walk is logged but does NOT terminate (v2 §1).
            print(f"  [searle WP] empty page {page} (total_pages={total_pages}); continuing")
        for p in posts:
            link = p.get("link", "")
            m = _ANNOUNCEMENT_URL_RE.match(link)
            if not m:
                continue
            year = int(m.group(2))
            if year in known_years:
                continue
            out.append((year, link))
        if total_pages is not None and page >= total_pages:
            break
        if total_pages is None and page >= MAX_PAGES_FALLBACK:
            print(f"  [searle WP] no X-WP-TotalPages; stopping at MAX_PAGES_FALLBACK={MAX_PAGES_FALLBACK}")
            break
        page += 1
    # Dedup by year (keep first)
    seen = set()
    uniq = []
    for y, url in out:
        if y in seen:
            continue
        seen.add(y)
        uniq.append((y, url))
    return uniq


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: /current-scholars/ + WP REST posts")
    print("=" * 60)
    r = _http_get(CURRENT_SCHOLARS_URL)
    r.raise_for_status()
    rows = parse_current_scholars(r.text)
    print(f"  /current-scholars/ articles parsed: {len(rows)}")
    if len(rows) < 30:
        print(f"  [WARN] only {len(rows)} scholars parsed; expected 60+")
    by_year = {}
    for x in rows:
        by_year[x["year"]] = by_year.get(x["year"], 0) + 1
    print(f"  by year: {dict(sorted(by_year.items()))}")
    # Show one row
    if rows:
        s = rows[0]
        print(f"  sample: {s['year']} | {s['name']} | {s['institution']} | {(s['research_title'] or '')[:60]}")
    # WP REST availability check (not required for ingest if /current-scholars/ has the cohort)
    rp = _http_get(f"{WP_POSTS_URL}?per_page=1")
    print(f"  WP REST /posts probe: HTTP {rp.status_code}")


# =============================================================================
# Download
# =============================================================================

def download_scholars(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Fetch /current-scholars/ + class-announcement posts")
    print("=" * 60)
    r = _http_get(CURRENT_SCHOLARS_URL)
    r.raise_for_status()
    current_rows = parse_current_scholars(r.text)
    print(f"  /current-scholars/: {len(current_rows)} rows")
    known_years = {x["year"] for x in current_rows}
    print(f"  cohorts in current-scholars/: {sorted(known_years)}")

    announcement_posts = discover_announcement_posts(known_years)
    if announcement_posts:
        print(f"  WP REST returned {len(announcement_posts)} class-announcement posts NOT in /current-scholars/: "
              f"{[y for y, _ in announcement_posts]}")

    announcement_rows: list[dict] = []
    for year, url in announcement_posts:
        try:
            resp = _http_get(url)
            if resp.status_code != 200:
                print(f"    {year} {url}: HTTP {resp.status_code}; skipping")
                continue
            yr_rows = parse_announcement_post(resp.text, year, url)
            announcement_rows.extend(yr_rows)
            print(f"    {year} {url}: +{len(yr_rows)} rows")
        except Exception as e:
            print(f"    {year} {url}: error {e}")

    all_rows = current_rows + announcement_rows
    if limit:
        # Keep the N most recent cohorts (sort by year desc)
        cohorts = sorted({r["year"] for r in all_rows}, reverse=True)[:limit]
        all_rows = [r for r in all_rows if r["year"] in cohorts]
        print(f"  [LIMIT] keeping {limit} most recent cohorts: {cohorts}")

    raw_path = output_dir / "searle_raw.json"
    raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_rows)} records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Jr", "Sr.", "Sr",
                    "II", "III", "IV", "MPH", "MSc"}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    while tokens and tokens[-1].rstrip(".,") in {s.rstrip(".,") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def _slugify(s: str) -> str:
    s = unescape(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        name = r.get("name") or ""
        year = r.get("year")
        if not name or not year:
            continue
        slug = _slugify(name)
        funder_award_id = f"searle-{year}-{slug}"
        if funder_award_id in seen_ids:
            # Dedup: /current-scholars/ might overlap with an announcement post
            # for a transition year; first record wins.
            continue
        seen_ids.add(funder_award_id)
        given, family = split_name(name)
        rows.append({
            "funder_award_id":   funder_award_id,
            "slug":              slug,
            "year":              year,
            "name":              name,
            "given_name":        given or None,
            "family_name":       family or None,
            "institution":       r.get("institution"),
            "research_title":    r.get("research_title"),
            "profile_url":       r.get("profile_url"),
            "amount":            AMOUNT_USD,
            "currency":          "USD",
            "duration_years":    DURATION_YEARS,
            "landing_page_url":  r.get("source_page"),
        })
    df = pd.DataFrame.from_records(rows)
    n_nm = df["name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    n_title = df["research_title"].astype(bool).sum()
    n_url = df["profile_url"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: name={n_nm} ({n_nm*100/len(df):.0f}%) "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"title={n_title} ({n_title*100/len(df):.0f}%) "
          f"profile_url={n_url} ({n_url*100/len(df):.0f}%)")
    print(f"  year range: {df['year'].min()} – {df['year'].max()}")
    print(f"\n  By year:")
    print(df.groupby("year").size().to_string())
    print(f"\n  Top 10 institutions:")
    print(df["institution"].value_counts().head(10).to_string())
    # Runbook §1.2.5 — astype("string") immediately before parquet write.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "searle_scholars.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4."""
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
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_searle_scholars.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/searle"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse searle_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape the N most recent cohorts (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Searle Scholars Program → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "searle_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_scholars(args.output_dir, args.limit)

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
    print(f"Next: notebooks/awards/CreateSearleScholarsAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
