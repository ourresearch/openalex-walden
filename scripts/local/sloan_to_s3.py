#!/usr/bin/env python3
"""
Alfred P. Sloan Foundation Grants Database to S3 Data Pipeline
==============================================================

Downloads the Alfred P. Sloan Foundation's public grants database from
sloan.org and emits one row per grant for the awards registry.

Source authority
----------------
sloan.org/grants-database is the foundation's own grants directory. The
origin sits behind a Cloudflare JS challenge that 403s plain `requests`,
so this is a Method-6 (Playwright + stealth) ingest via the shared helper
`scripts/local/_playwright_helper.py`. The Cloudflare challenge is heavier
than Klingenstein's — it only clears under `wait_until="networkidle"`
(confirmed 2026-05-29).

Pagination is `?page=N` (10 grants/page). Each grant renders as an `<li>`
with a `<header>` (grantee / amount / city / year) and a `.details` block
(brief description; Program / Sub-program / Initiative / Investigator
label-value pairs; and a permalink of the form
`/grant-detail/g-{YEAR}-{ID}`). The DOM carries one CSS class per header
field and a `.label` per detail, so we extract structurally rather than
parsing rendered text.

Coverage note
-------------
The online grants database only goes back to **2008**. Pre-2008 Sloan
grants are not in this source (they require IRS 990-PF parsing — tracked
separately). This ingest therefore covers 2008-present.

Schema choices
--------------
- One row per grant. `funder_award_id` = the source permalink slug,
  e.g. `g-2025-79300` (stable, unique, source-authoritative).
- Funder = Alfred P. Sloan Foundation (F4320306151, US,
  DOI 10.13039/100000879, ROR https://ror.org/052csg198).
- `funding_type` = `grant` for every row.
- `funder_scheme` = the grant's Program (e.g. "Research", "Higher
  Education", "Technology", "Public Understanding of Science and
  Technology"). `sub_program` and `initiative` are carried as separate
  columns for the notebook to fold into the scheme/description.
- **Amount IS published per grant** ("$25,000" form) -> `amount` (int) +
  `currency` = 'USD'. §6.7 is *not* waived here — Sloan posts a real
  dollar figure on every grant card.
- Each card has a grantee organization and (usually) a named Investigator.
  We model `lead_investigator.name` = the Investigator when present (else
  the grantee org), and `affiliation.name` = the grantee organization.
  `affiliation.country` is derived from the city/region: 'US' when the
  region is a US state/territory code, else mapped from a small country
  table, else NULL (we never guess).

Provenance: `sloan_foundation` (verified count=0 on production 2026-05-29).

Output
------
s3://openalex-ingest/awards/sloan/sloan_grants.parquet

Usage
-----
    python sloan_to_s3.py                    # full run
    python sloan_to_s3.py --skip-upload      # local dev
    python sloan_to_s3.py --skip-download    # reuse cached JSON
    python sloan_to_s3.py --limit 3          # smoke test (first 3 pages)
    python sloan_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3 playwright playwright-stealth
    playwright install chromium
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

# Shared Playwright helper (Method 6). Same dir as this script.
sys.path.insert(0, str(Path(__file__).parent))
from _playwright_helper import PlaywrightSession  # noqa: E402

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

SITE_BASE = "https://sloan.org"
GRANTS_URL_TEMPLATE = f"{SITE_BASE}/grants-database?page={{page}}"

# Funder — Alfred P. Sloan Foundation.
# Verified F4320306151, country US, DOI 10.13039/100000879.
FUNDER_ID = 4320306151
FUNDER_DISPLAY_NAME = "Alfred P. Sloan Foundation"

PROVENANCE = "sloan_foundation"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/sloan/sloan_grants.parquet"

# Cloudflare on sloan.org clears on the FIRST navigation of a fresh context,
# then hard-sticks the challenge on every reuse of that context (confirmed
# 2026-05-29). So we recreate the browser context per page (and per retry) via
# `session.recreate_context()`. `domcontentloaded` + a wait for the grantee
# selector is the reliable signal that the challenge cleared and the grant list
# rendered; the challenge interstitial never produces that selector and keeps
# title 'Just a moment...'.
MIN_REQUEST_INTERVAL_S = 0.6
PAGE_WAIT_UNTIL = "domcontentloaded"
PAGE_TIMEOUT_MS = 60000

# Grant rows render as <li><header><.grantee>…; presence of this selector means
# the challenge cleared AND the list is populated.
GRANT_ROW_SELECTOR = "li > header > .grantee"
GRANT_ROW_TIMEOUT_MS = 15000

# Per-page retry budget against the Cloudflare challenge. A fresh context
# usually clears on attempt 1, but an occasional page needs a few tries
# (page 2 wanted 4 attempts in validation 2026-05-29).
CHALLENGE_MAX_ATTEMPTS = 8
CHALLENGE_RETRY_SLEEP_S = 2.0
CHALLENGE_TITLE_MARKER = "just a moment"

# Defensive upper bound on page count (10 grants/page; ~3,400 grants ->
# ~340 pages). The walk stops early when a page yields no grants or
# repeats the previous page.
MAX_PAGES = 600

# US states + territories + DC + military codes — used to set affiliation
# country to 'US'. Canadian provinces (ON, QC, BC, ...) are deliberately
# NOT here so "Toronto, ON" is not misread as US.
US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR", "VI", "GU", "AS", "MP", "AA", "AE", "AP",
}

# Small map of spelled-out country names that show up in the region slot
# for international grantees. Anything not matched stays NULL (no guessing).
COUNTRY_NAME_TO_CODE = {
    "united states": "US", "usa": "US",
    "united kingdom": "GB", "england": "GB", "scotland": "GB", "wales": "GB",
    "uk": "GB", "germany": "DE", "france": "FR", "canada": "CA",
    "australia": "AU", "netherlands": "NL", "switzerland": "CH",
    "sweden": "SE", "italy": "IT", "spain": "ES", "japan": "JP",
    "china": "CN", "israel": "IL", "india": "IN", "brazil": "BR",
    "norway": "NO", "denmark": "DK", "finland": "FI", "belgium": "BE",
    "austria": "AT", "ireland": "IE", "mexico": "MX", "chile": "CL",
    "south africa": "ZA", "new zealand": "NZ", "singapore": "SG",
}


# =============================================================================
# Page extraction
# =============================================================================

# One JS pass per page returns a clean list of grant dicts. We select only
# the top-level grant rows (an <li> whose direct child is a <header> with a
# .grantee cell) to avoid the nested Program/Investigator <li> items. The
# detail label-value pairs are collected into a dict keyed by lowercased
# label, so any label (Program / Sub-program / Initiative / Investigator)
# is captured without hardcoding the set.
_EXTRACT_JS = r"""
() => {
  const rows = Array.from(document.querySelectorAll('li')).filter(
    li => li.querySelector(':scope > header > .grantee')
  );
  const clean = (el) => {
    if (!el) return null;
    const c = el.cloneNode(true);
    c.querySelectorAll('.hide-desktop, .label').forEach(s => s.remove());
    const t = c.textContent.replace(/\s+/g, ' ').trim();
    return t || null;
  };
  return rows.map(li => {
    const header = li.querySelector(':scope > header');
    const details = li.querySelector(':scope > .details');
    const fields = {};
    let description = null, permalink = null;
    if (details) {
      description = clean(details.querySelector('.brief-description'));
      const pl = details.querySelector('footer a.permalink');
      permalink = pl ? pl.getAttribute('href') : null;
      details.querySelectorAll('.grid ul.col > li').forEach(item => {
        const label = item.querySelector('.label');
        if (!label) return;
        const key = label.textContent.replace(/\s+/g, ' ').trim().toLowerCase();
        fields[key] = clean(item);
      });
    }
    return {
      grantee:      clean(header ? header.querySelector('.grantee') : null),
      amount:       clean(header ? header.querySelector('.amount') : null),
      city:         clean(header ? header.querySelector('.city') : null),
      year:         clean(header ? header.querySelector('.year') : null),
      description:  description,
      permalink:    permalink,
      program:      fields['program'] || null,
      sub_program:  fields['sub-program'] || null,
      initiative:   fields['initiative'] || null,
      investigator: fields['investigator'] || null,
    };
  });
}
"""


def fetch_page_with_retry(
    session: PlaywrightSession, page_num: int, verbose: bool = False
) -> tuple[list[dict], str]:
    """Fetch one grants-database page, recreating the browser context per attempt
    to defeat Cloudflare challenge escalation.

    Returns (rows, title). An empty `rows` with a non-challenge title (e.g.
    'Grants Database') means the page genuinely has no grants — the natural
    end-of-database signal. An empty `rows` whose title still reads
    'Just a moment...' means we exhausted the retry budget while blocked (a hard
    failure the caller must surface, not mistake for end-of-database).
    """
    url = GRANTS_URL_TEMPLATE.format(page=page_num)
    last_title = ""
    for attempt in range(1, CHALLENGE_MAX_ATTEMPTS + 1):
        session.recreate_context()
        try:
            session.fetch_with_response(
                url, wait_until=PAGE_WAIT_UNTIL, timeout_ms=PAGE_TIMEOUT_MS
            )
        except Exception as e:
            last_title = "Just a moment..."
            if verbose:
                print(f"    attempt {attempt}: nav error {e!s:.80}")
            time.sleep(CHALLENGE_RETRY_SLEEP_S)
            continue

        # Wait for the grant list to render. If the challenge is still up, the
        # selector never appears and this times out — that's our retry signal.
        try:
            session.page.wait_for_selector(
                GRANT_ROW_SELECTOR, timeout=GRANT_ROW_TIMEOUT_MS
            )
            rows = session.page.evaluate(_EXTRACT_JS)
            last_title = session.page.title()
            if rows:
                if verbose:
                    print(f"    attempt {attempt}: rows={len(rows)} title={last_title!r}")
                return rows, last_title
        except Exception:
            last_title = session.page.title()

        if CHALLENGE_TITLE_MARKER not in (last_title or "").lower():
            # Challenge cleared but no grant rows present -> genuine empty page
            # (past the last page of the database).
            if verbose:
                print(f"    attempt {attempt}: 0 rows, title={last_title!r} (no challenge) -> end")
            return [], last_title

        if verbose:
            print(f"    attempt {attempt}: challenge (title={last_title!r}), retrying")
        time.sleep(CHALLENGE_RETRY_SLEEP_S)

    return [], last_title


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: load grants-database page 1")
    print("=" * 60)
    with PlaywrightSession(min_interval_s=MIN_REQUEST_INTERVAL_S) as session:
        rows, title = fetch_page_with_retry(session, 1, verbose=True)
        print(f"  page 1 grants parsed: {len(rows)} (title={title!r})")
        if rows:
            r = rows[0]
            print(f"  first grantee:   {r.get('grantee')}")
            print(f"        amount:    {r.get('amount')}")
            print(f"        city:      {r.get('city')}")
            print(f"        year:      {r.get('year')}")
            print(f"        program:   {r.get('program')} / sub={r.get('sub_program')} / init={r.get('initiative')}")
            print(f"        investig.: {r.get('investigator')}")
            print(f"        permalink: {r.get('permalink')}")
            print(f"        descr:     {(r.get('description') or '')[:70]}")


# =============================================================================
# Download
# =============================================================================

def download_grants(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Walk grants-database pages")
    print("=" * 60)
    all_rows: list[dict] = []
    seen_keys: set[str] = set()
    prev_page_keys: Optional[frozenset] = None
    with PlaywrightSession(min_interval_s=MIN_REQUEST_INTERVAL_S) as session:
        max_pages = limit if limit is not None else MAX_PAGES
        for page_num in range(1, max_pages + 1):
            rows, title = fetch_page_with_retry(session, page_num)
            if not rows:
                if CHALLENGE_TITLE_MARKER in (title or "").lower():
                    # Hard block: exhausted the retry budget while still on the
                    # Cloudflare challenge. Do NOT treat as end-of-database —
                    # that would silently truncate the corpus. Abort loudly so
                    # the partial JSON is never mistaken for a complete scrape.
                    raise RuntimeError(
                        f"page {page_num} stayed Cloudflare-blocked after "
                        f"{CHALLENGE_MAX_ATTEMPTS} attempts (title={title!r}); "
                        f"aborting walk with {len(all_rows)} grants collected"
                    )
                print(f"  [page {page_num}] 0 grants (title={title!r}) — end of database")
                break
            # Wrap-around guard: if this page is identical to the previous
            # one (some paginators clamp past the last page), stop.
            page_keys = frozenset(r.get("permalink") or json.dumps(r, sort_keys=True) for r in rows)
            if prev_page_keys is not None and page_keys == prev_page_keys:
                print(f"  [page {page_num}] identical to previous page — end of database")
                break
            prev_page_keys = page_keys
            new_here = 0
            for r in rows:
                key = r.get("permalink") or json.dumps(r, sort_keys=True)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_rows.append(r)
                new_here += 1
            print(f"  [page {page_num}] +{new_here} grants (total {len(all_rows)})")
            if limit is not None and page_num >= limit:
                break
    raw_path = output_dir / "sloan_grants_raw.json"
    raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_rows)} grants to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

_AMOUNT_RE = re.compile(r"-?\$?\s*([\d,]+)")
# The grant-detail slug is the full path segment after /grant-detail/. Sloan's
# slugs are richer than a plain g-YYYY-N: they carry trailing serials and
# program suffixes (g-2014-3-01, g-2017-5-12-econ), an occasional 'b' year
# prefix (g-b2014-17), and a few named slugs (g-books, g-nacme, g-mphd). Each
# full slug is unique and source-authoritative, so we capture the whole
# segment — a narrow regex silently collapsed ~800 distinct grants in testing.
_PERMALINK_SLUG_RE = re.compile(r"/grant-detail/([^/?#\s]+)")
_SLUG_YEAR_RE = re.compile(r"(?:19|20)\d{2}")

# Suffix/prefix tokens stripped before splitting a person name into
# given/family. Sloan investigator names are mostly "First Last" but some
# carry a degree or generational suffix.
_DEGREE_SUFFIXES = {
    "phd", "ph.d.", "ph.d", "md", "m.d.", "m.d", "sc.d.", "sc.d", "d.o.", "d.o",
    "dphil", "jr.", "jr", "sr.", "sr", "ii", "iii", "iv", "mph", "msc", "mba",
}
_PREFIX_TITLES = {"dr", "dr.", "prof", "prof.", "professor", "mr", "mr.", "ms", "ms.", "mrs", "mrs."}


def split_name(full: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """'Jordan Blashek' -> ('Jordan', 'Blashek'). Strips honorifics and
    trailing degree/generational suffixes. Single token -> (None, token).
    None/empty -> (None, None)."""
    if not full:
        return (None, None)
    tokens = full.split()
    while tokens and tokens[0].lower().rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    while tokens and tokens[-1].lower().rstrip(".,") in {s.rstrip(".,") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return (None, None)
    if len(tokens) == 1:
        return (None, tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def parse_amount(raw: Optional[str]) -> Optional[int]:
    """'$25,000' -> 25000 ; '$1,234,567' -> 1234567 ; None/garbage -> None."""
    if not raw:
        return None
    m = _AMOUNT_RE.search(raw)
    if not m:
        return None
    try:
        val = int(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return val if val > 0 else None


def split_city_region(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """'Arlington, VA' -> ('Arlington', 'VA'). 'London, United Kingdom' ->
    ('London', 'United Kingdom'). Single-token -> (token, None)."""
    if not raw:
        return (None, None)
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) == 1:
        return (parts[0] or None, None)
    city = ", ".join(parts[:-1]).strip() or None
    region = parts[-1].strip() or None
    return (city, region)


def region_to_country(region: Optional[str]) -> Optional[str]:
    """US state code -> 'US'; spelled-out country -> ISO2; else None."""
    if not region:
        return None
    r = region.strip()
    if r.upper() in US_STATE_CODES:
        return "US"
    return COUNTRY_NAME_TO_CODE.get(r.lower())


def slug_from_permalink(permalink: Optional[str]) -> Optional[str]:
    if not permalink:
        return None
    m = _PERMALINK_SLUG_RE.search(permalink)
    return m.group(1) if m else None


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    skipped_no_id = 0
    for r in records:
        permalink = r.get("permalink")
        funder_award_id = slug_from_permalink(permalink)
        if not funder_award_id:
            skipped_no_id += 1
            continue
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)

        # Year: prefer the explicit .year field; fall back to the year
        # embedded in the permalink slug (g-YYYY-ID).
        year = None
        yr_field = (r.get("year") or "").strip()
        if re.fullmatch(r"(?:19|20)\d{2}", yr_field):
            year = int(yr_field)
        else:
            # Fallback: first 4-digit year embedded in the slug (handles the
            # 'b' prefix form g-b2014-17 too). Named slugs (g-books) have none,
            # which is fine — the card .year field covers 100% in practice.
            ym = _SLUG_YEAR_RE.search(funder_award_id)
            if ym:
                year = int(ym.group(0))

        grantee = (r.get("grantee") or "").strip() or None
        investigator = (r.get("investigator") or "").strip() or None
        amount = parse_amount(r.get("amount"))
        city, region = split_city_region(r.get("city"))
        country = region_to_country(region)
        # lead investigator name: the named investigator when present,
        # otherwise the grantee organization stands in as the lead.
        lead_name = investigator or grantee
        # given/family only make sense for a person — split the investigator,
        # never the org. When there's no named investigator both stay NULL
        # (the grantee org is still captured via grantee_org / affiliation).
        given_name, family_name = split_name(investigator)

        rows.append({
            "funder_award_id": funder_award_id,
            "year":            year,
            "grantee_org":     grantee,
            "lead_name":       lead_name,
            "investigator":    investigator,
            "given_name":      given_name,
            "family_name":     family_name,
            "amount":          amount,
            "currency":        "USD" if amount is not None else None,
            "city":            city,
            "region":          region,
            "country":         country,
            "program":         r.get("program"),
            "sub_program":     r.get("sub_program"),
            "initiative":      r.get("initiative"),
            "description":     r.get("description"),
            "permalink_url":   (SITE_BASE + permalink) if permalink and permalink.startswith("/") else permalink,
        })

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        raise RuntimeError("no grants parsed — investigate scraper")
    if skipped_no_id:
        print(f"  [WARN] skipped {skipped_no_id} record(s) without a parseable permalink id")

    n = len(df)
    n_org   = df["grantee_org"].astype(bool).sum()
    n_amt   = df["amount"].notna().sum()
    n_year  = df["year"].notna().sum()
    n_ctry  = df["country"].astype(bool).sum()
    n_prog  = df["program"].astype(bool).sum()
    n_inv   = df["investigator"].astype(bool).sum()
    n_desc  = df["description"].astype(bool).sum()
    print(f"  rows: {n}")
    print(f"  coverage: grantee_org={n_org} ({n_org*100//n}%) "
          f"amount={n_amt} ({n_amt*100//n}%) year={n_year} ({n_year*100//n}%)")
    print(f"            country={n_ctry} ({n_ctry*100//n}%) program={n_prog} ({n_prog*100//n}%) "
          f"investigator={n_inv} ({n_inv*100//n}%) description={n_desc} ({n_desc*100//n}%)")
    if n_year:
        print(f"  year range: {int(df['year'].min())} – {int(df['year'].max())}  "
              f"({df['year'].nunique()} distinct years)")
    print(f"\n  By program:")
    print(df["program"].value_counts(dropna=False).head(12).to_string())
    print(f"\n  Distinct non-US / unmapped regions (review):")
    nonus = df.loc[df["country"].isna() & df["region"].notna(), "region"].value_counts().head(15)
    print(nonus.to_string() if not nonus.empty else "    (none)")
    amt = df["amount"].dropna()
    if not amt.empty:
        print(f"\n  Amount summary (USD): min={int(amt.min())} "
              f"median={int(amt.median())} max={int(amt.max())} "
              f"total={int(amt.sum()):,}")

    # Runbook §1.2.5 — astype("string") before parquet write
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "sloan_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
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
    prev_path = output_dir / "_prev_sloan_grants.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/sloan"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse sloan_grants_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only fetch first N grants-database pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Alfred P. Sloan Foundation Grants Database → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "sloan_grants_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_grants(args.output_dir, args.limit)

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
    print("Next: notebooks/awards/CreateSloanAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
