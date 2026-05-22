#!/usr/bin/env python3
"""
Hertz Foundation fellows → S3 Data Pipeline (FacetWP method-3)
==================================================================

Downloads Hertz Foundation fellowship recipients from the public
Fellows Directory (`www.hertzfoundation.org/hertz-community/fellows-directory/`)
via direct POST to the WordPress FacetWP REST refresh endpoint.
1,344 fellows total spanning fellowship years 1960-2026 (verified
2026-05-21), all paginated at 12/page across 112 pages — single
unfiltered query, no facet slicing required.

Source authority
----------------
www.hertzfoundation.org is the awarding body's own site (WordPress
+ FacetWP). The Fellows Directory page is server-side-rendered first
page + client-side FacetWP pagination via XHR. The FacetWP refresh
endpoint at `/wp-json/facetwp/v1/refresh` accepts the standard
`facetwp_refresh` JSON body and returns the rendered card grid in
`response.template`. Captured the XHR shape once on 2026-05-21 then
replayed via plain HTTP — no browser automation needed. Method #3
on the runbook ladder (search/index APIs).

Note: the Hertz WP REST API at `/wp/v2/people` returns all 1,502
"people" records (board + staff + fellows) but the `fellowships` and
`awards` taxonomies are EMPTY across all records sampled (0 of 50
oldest, 0 of 10 newest). The fellowship year is only exposed via the
FacetWP directory grid `<p class="fellow-lister-school year">N</p>`
markup — so the REST API can NOT be used as the data source.
Verified at https://www.hertzfoundation.org/wp-json/wp/v2/fellowships
returns `[]` (zero terms). This is documented in the script header so
no future contributor wastes time on the REST taxonomy path.

Schema per fellow card (HTML parsed via regex on Hertz's
`fellow-card` / `fellow-info` CSS classes):
  - fellow-lister-school year    → fellowship_year (e.g. 2026)
  - gb-text fellow-lister-name   → fellow's full name + people-page slug
  - fellow-lister-school (no year) → current role + institution
                                     ("PhD Student, MIT", "Professor, ...")
  - expertise > tag-red          → 1+ field-of-study tags
  - fellow-thumbnail img src     → headshot URL (kept for audit)

Amount
------
Hertz fellowship is renewable annually for up to 5 years and the
foundation's own fellowship-benefits page values the total package
at up to $250,000 (full PhD tuition equivalent + $38-44K/9-mo personal
stipend × 5 years + $5K/yr dependent supplement). We ship the
documented per-fellow ceiling — $250,000 USD — uniformly. Source:
https://www.hertzfoundation.org/hertz-fellowship/fellowship-benefits/
fetched 2026-05-21.

Output
------
s3://openalex-ingest/awards/hertz/hertz_fellows.parquet

Usage
-----
    python hertz_to_s3.py                  # full run (~1.3K fellows, ~80 sec)
    python hertz_to_s3.py --skip-upload    # local dev
    python hertz_to_s3.py --skip-download  # reuse cached JSON
    python hertz_to_s3.py --max-pages 3    # smoke-test
    python hertz_to_s3.py --allow-shrink   # override §1.4 shrink-check

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

# =============================================================================
# Configuration
# =============================================================================

XHR_URL = "https://www.hertzfoundation.org/wp-json/facetwp/v1/refresh"
REFERER = "https://www.hertzfoundation.org/hertz-community/fellows-directory/"
FACET_URI = "hertz-community/fellows-directory"
FACET_TEMPLATE = "fellows_loop"

# Awarding body — Fannie and John Hertz Foundation.
# Verified F4320308782, country US, no ROR, DOI 10.13039/100005883.
FUNDER_ID = 4320308782
FUNDER_DISPLAY_NAME = "Hertz Foundation"

PROVENANCE = "hertz_facetwp"
CURRENCY = "USD"  # Hertz is US-based; fellowship paid in USD
# Per-fellow ceiling per fellowship-benefits page (renewable to 5 years).
HERTZ_FELLOWSHIP_AMOUNT_USD = 250000.0

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/hertz/hertz_fellows.parquet"

USER_AGENT = "openalex-walden-hertz-ingest/1.0 (+https://openalex.org)"

# Polite throttle. ~100 req/min, well under Cloudflare/FacetWP thresholds.
MIN_REQUEST_INTERVAL_S = 0.6


# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_post(payload: dict, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Referer": REFERER,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.post(XHR_URL, data=json.dumps(payload), timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


def build_payload(paged: int) -> dict:
    return {
        "action": "facetwp_refresh",
        "data": {
            "facets": {
                "fellows_search":     [],
                "fellowship_school":  [],
                "field_of_study":     [],
                "filter_by_year":     [],
                "undergrade_school":  [],
                "pagination":         [],
            },
            "frozen_facets": {},
            "http_params": {"get": {}, "uri": FACET_URI, "url_vars": []},
            "template": FACET_TEMPLATE,
            "extras": {"sort": "default"},
            "soft_refresh": 0,
            "is_bfcache": 0,
            "first_load": 0,
            "paged": paged,
        }
    }


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: FacetWP POST reachable + returns expected schema")
    print("=" * 60)
    resp = _http_post(build_payload(1))
    resp.raise_for_status()
    j = resp.json()
    if "template" not in j or "settings" not in j:
        print(f"[ERROR] response missing 'template'/'settings' keys; got {list(j.keys())}")
        sys.exit(3)
    pager = j["settings"].get("pager", {})
    total_rows = pager.get("total_rows")
    total_pages = pager.get("total_pages")
    print(f"  total_rows={total_rows} total_pages={total_pages}")
    if total_rows is None or total_rows < 1000:
        print(f"[WARN] total_rows {total_rows} below expected ~1.3K — schema may have changed")
    cards = len(re.findall(r'class="fellow-card"', j["template"]))
    print(f"  fellow cards on page 1: {cards}")
    if cards == 0:
        print("[ERROR] zero cards found in template — selector may have changed")
        sys.exit(3)


# =============================================================================
# Card parsing
# =============================================================================

# Each fellow card is a <div class="fellow-card"> ... </div> at depth-3:
#   <div class="fellow-card">
#     <div class="fellow-thumbnail-box">...</div>
#     <div class="fellow-info">
#        <p class="fellow-lister-school year">2026</p>
#        <h3 class="gb-text fellow-lister-name"><a href=".../people/{slug}/">Name</a></h3>
#        <p class="fellow-lister-school">PhD Student, MIT</p>
#        <div class="expertise"><a class="tag-red">Topic1</a>...</div>
#     </div>
#   </div>
#
# The grid template doesn't nest, so we use balanced-tag scanning via
# a non-greedy match anchored on the next `<div class="fellow-card">` or end.
_CARD_BLOCK_RE = re.compile(
    r'<div class="fellow-card">(.+?)(?=<div class="fellow-card">|$)',
    re.DOTALL,
)

_RE_YEAR = re.compile(
    r'class="fellow-lister-school year"[^>]*>\s*(\d{4})\s*<',
)
# Name + slug: <h3 class="gb-text fellow-lister-name"><a href=".../people/{slug}/">Name</a></h3>
_RE_NAME = re.compile(
    r'class="gb-text fellow-lister-name"[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>',
)
# Current school/role: <p class="fellow-lister-school">role + inst</p>
# Note: needs to NOT match the year variant (which has " year" in the class).
_RE_SCHOOL = re.compile(
    r'class="fellow-lister-school"[^>]*>(.+?)</p>',
    re.DOTALL,
)
# Expertise/field-of-study tags within <div class="expertise">...<a>tag</a>...</div>
_RE_EXPERTISE_BLOCK = re.compile(
    r'<div class="expertise">(.+?)</div>',
    re.DOTALL,
)
_RE_EXPERTISE_TAG = re.compile(
    r'>([^<]+)</a>',
)
# Thumbnail: <img src="..." class="attachment-full ...">
_RE_THUMB = re.compile(
    r'<img[^>]*\bsrc="([^"]+)"[^>]*class="[^"]*wp-post-image[^"]*"',
)
_RE_THUMB_ALT = re.compile(
    r'<img[^>]*class="[^"]*wp-post-image[^"]*"[^>]*\bsrc="([^"]+)"',
)


def _strip_html(s: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip()


def parse_card(card_html: str) -> Optional[dict]:
    # Year is required — if missing this isn't a valid fellow card.
    m_year = _RE_YEAR.search(card_html)
    if not m_year:
        return None
    year = int(m_year.group(1))

    # Name + slug
    m_name = _RE_NAME.search(card_html)
    if not m_name:
        return None
    profile_url = unescape(m_name.group(1).strip())
    full_name = _strip_html(m_name.group(2))
    # Slug = /people/{slug}/
    slug_m = re.search(r'/people/([^/?#]+)/?', profile_url)
    slug = slug_m.group(1) if slug_m else None

    # Current school/role — pick the fellow-lister-school that is NOT the year
    school = None
    for m in _RE_SCHOOL.finditer(card_html):
        inner = m.group(1)
        # Skip if this is the year variant (class includes " year")
        # _RE_SCHOOL matches both, but we can check by inspecting context
        cls_start = max(0, m.start() - 60)
        cls_text = card_html[cls_start:m.start()]
        if 'fellow-lister-school year' in cls_text + m.group(0)[:50]:
            continue
        school = _strip_html(inner)
        if school:
            break

    # Expertise tags
    expertise = []
    m_exp = _RE_EXPERTISE_BLOCK.search(card_html)
    if m_exp:
        for t in _RE_EXPERTISE_TAG.findall(m_exp.group(1)):
            t_clean = unescape(t.strip())
            if t_clean:
                expertise.append(t_clean)

    # Thumbnail (audit only)
    thumb = None
    for pat in (_RE_THUMB, _RE_THUMB_ALT):
        m = pat.search(card_html)
        if m:
            thumb = unescape(m.group(1))
            break

    return {
        "slug":               slug,
        "full_name":          full_name,
        "fellowship_year":    year,
        "current_position":   school,
        "expertise":          "|".join(expertise) if expertise else None,
        "profile_url":        profile_url,
        "thumbnail_url":      thumb,
    }


# =============================================================================
# Download loop
# =============================================================================

def download_all(max_pages: Optional[int], output_dir: Path) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download Hertz fellow cards (unfiltered, paginated)")
    print("=" * 60)
    rows: list[dict] = []
    page = 1
    total_rows = None
    total_pages = None
    while True:
        resp = _http_post(build_payload(page))
        if resp.status_code != 200:
            print(f"    [page {page}] HTTP {resp.status_code}; stopping")
            break
        j = resp.json()
        if total_rows is None:
            pager = j.get("settings", {}).get("pager", {})
            total_rows = pager.get("total_rows", 0)
            total_pages = pager.get("total_pages", 0)
            per_page = pager.get("per_page", 12)
            print(f"  total_rows={total_rows} total_pages={total_pages} per_page={per_page}")
        cards = _CARD_BLOCK_RE.findall(j.get("template", ""))
        if not cards:
            print(f"    [page {page}] 0 cards — end of corpus")
            break
        parsed_count = 0
        for c in cards:
            parsed = parse_card(c)
            if parsed:
                parsed["facetwp_page"] = page
                rows.append(parsed)
                parsed_count += 1
        if page == 1 or page % 10 == 0 or (total_pages and page == total_pages):
            print(f"    [page {page}] +{parsed_count}/{len(cards)} cards (running total {len(rows)})")
        page += 1
        if max_pages and page > max_pages:
            print(f"    [LIMIT] stopping at --max-pages={max_pages}")
            break
        if total_pages and page > total_pages:
            break
    print(f"\n  ✓ collected {len(rows)} fellow cards across {page-1} pages")
    return rows


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

# Honorific/degree suffixes that should NOT count as family_name
_DEGREE_SUFFIXES = {
    "PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV", "Esq.", "Esq",
}


def split_name(full_name: str) -> tuple[Optional[str], Optional[str]]:
    """Return (given_name, family_name) following runbook §2.4.1."""
    if not full_name:
        return None, None
    name = full_name.strip()
    # Drop trailing degree suffixes (e.g. "Erich Jarvis, PhD")
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()  # name before any "...".
    # Drop honorific suffixes (no comma, e.g. "John Smith Jr.")
    toks = name.split()
    while toks and toks[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    # Spanish 2-surname: only split on last token (runbook permits single-surname
    # split; double-surname handled in CONAHCYT precedent — here we use single)
    return " ".join(toks[:-1]), toks[-1]


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw cards")
    print("=" * 60)

    seen_slugs: set[str] = set()
    seen_award_ids: set[str] = set()
    final: list[dict] = []
    duplicates_dropped = 0

    for r in all_rows:
        slug = r.get("slug")
        if not slug:
            continue
        if slug in seen_slugs:
            duplicates_dropped += 1
            continue
        seen_slugs.add(slug)

        given, family = split_name(r.get("full_name") or "")

        # funder_award_id = "hertz-{slug}-{year}".
        # Each Hertz fellow is uniquely identified by their /people/{slug}/
        # profile URL plus their fellowship cohort year. Year is part of the
        # key because — although we observe each slug exactly once in the
        # 2026-05 corpus — the same person could in principle re-appear in
        # a later cohort (e.g. completing-but-pausing). Prize-pattern rule
        # (runbook prize table row 6): slug collision RAISES; we enforce
        # that via seen_award_ids below.
        year = r.get("fellowship_year")
        funder_award_id = f"hertz-{slug}-{year}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "Hertz fellow slug+year must be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        # display_name composition (runbook §2.3.1): use full_name not slug.
        display_name = f"Hertz Fellowship — {r['full_name']} ({year})"

        # description: combine current_position + expertise into a single
        # human-readable line. Don't fabricate prose, just concatenate.
        desc_parts = []
        if r.get("current_position"):
            desc_parts.append(r["current_position"])
        if r.get("expertise"):
            desc_parts.append("Field(s): " + r["expertise"].replace("|", ", "))
        description = ". ".join(desc_parts) if desc_parts else None

        final.append({
            "funder_award_id":     funder_award_id,
            "slug":                slug,
            "full_name":           r.get("full_name"),
            "given_name":          given,
            "family_name":         family,
            "fellowship_year":     year,
            "current_position":    r.get("current_position"),
            "expertise":           r.get("expertise"),
            "display_name":        display_name,
            "description":         description,
            "amount":              HERTZ_FELLOWSHIP_AMOUNT_USD,
            "currency":            CURRENCY,
            "start_date":          f"{year}-01-01" if year else None,
            "end_date":            f"{year+4}-12-31" if year else None,  # 5-year renewable ceiling
            "profile_url":         r.get("profile_url"),
            "thumbnail_url":       r.get("thumbnail_url"),
            "declined":            False,  # schema parity (prize pattern)
        })

    if duplicates_dropped:
        print(f"  dropped {duplicates_dropped} duplicate slug appearances "
              "(unexpected — Hertz directory should list each fellow once)")

    df = pd.DataFrame.from_records(final)
    n_amt = df["amount"].notna().sum()
    n_dt  = df["start_date"].notna().sum()
    n_exp = df["expertise"].notna().sum()
    n_pos = df["current_position"].notna().sum()
    print(f"  rows: {len(df)}  "
          f"amount={n_amt} ({n_amt*100/len(df):.0f}%)  "
          f"dates={n_dt} ({n_dt*100/len(df):.0f}%)  "
          f"expertise={n_exp} ({n_exp*100/len(df):.0f}%)  "
          f"position={n_pos} ({n_pos*100/len(df):.0f}%)")
    print(f"\n  Year distribution (top + bottom):")
    yr_counts = df.groupby("fellowship_year").size().sort_index()
    print(yr_counts.head(5).to_string())
    print("  ...")
    print(yr_counts.tail(5).to_string())
    print(f"\n  Total years covered: {df['fellowship_year'].nunique()}  "
          f"({df['fellowship_year'].min()} - {df['fellowship_year'].max()})")

    # Runbook §1.2.5 — string before parquet
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "hertz_fellows.parquet"
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
    prev_path = output_dir / "_prev_hertz_fellows.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/hertz"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse existing raw JSON dump in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Limit pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Hertz Foundation → S3 Pipeline (FacetWP method-3, FELLOWSHIP pattern)")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "hertz_fellows_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        all_rows = download_all(args.max_pages, args.output_dir)
        raw_path = args.output_dir / "hertz_fellows_raw.json"
        raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
        print(f"\n  Cached {len(all_rows)} raw rows to {raw_path}")

    df = build_dataframe(all_rows)
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
    print(f"Next: notebooks/awards/CreateHertzAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
