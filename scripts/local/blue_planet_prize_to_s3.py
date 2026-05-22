#!/usr/bin/env python3
"""
Blue Planet Prize → S3 Data Pipeline (PRIZE PATTERN, static-HTML scrape)
========================================================================

Downloads the Blue Planet Prize laureate listings from the Asahi Glass
Foundation's official site (`af-info.or.jp/en/blueplanet/`) — one yearly
listing page per year, 1992-present, two laureates per year. The Blue
Planet Prize is given annually for "the establishment of harmony between
human society and nature." Awarding body in OpenAlex: Asahi Glass
Foundation (F4320309996, JP, DOI 10.13039/100007684, no ROR).

Source authority
----------------
af-info.or.jp is the Asahi Glass Foundation's own site. Each laureate
appears on `https://www.af-info.or.jp/en/blueplanet/list-YYYY.html` (years
1992-2023) or `list_YYYY.html` (years 2024+, after a URL format change).
**Schema drift**: pre-2024 uses hyphen, 2024+ uses underscore — both
are documented and handled below.

Each yearly list page has exactly two laureate sections anchored at
`#text-01` and `#text-02`. Inside each section:
  - `<h2 class="c-ttl-lv02">` carries `"Title FirstName LastName (Country)"`
    for individual laureates, or `"Organization Name"` for institutional
    laureates (this prize honors both individuals and organizations).
  - Following text contains: birth/founding date, affiliation/HQ,
    contribution summary, and a "For more Details" PDF link.

The Foundation's "About the Prize" page documents the current prize
amount as **USD 500,000 per laureate** (verified 2026-05-21 at
af-info.or.jp/en/blueplanet/about.html — "Each recipient is presented
with a certificate of merit, a commemorative trophy, and 500,000 USD in
prize money"). Historical amounts were quoted in JPY 50,000,000 per
laureate; we ship the current documented USD ceiling uniformly since the
foundation no longer publishes year-banded amounts. Document the
assumption in the notebook header.

Output
------
s3://openalex-ingest/awards/blue_planet_prize/blue_planet_prize_laureates.parquet

Usage
-----
    python blue_planet_prize_to_s3.py                # full run (~68 laureates, ~30 sec)
    python blue_planet_prize_to_s3.py --skip-upload  # local dev
    python blue_planet_prize_to_s3.py --skip-download
    python blue_planet_prize_to_s3.py --year 2025    # single-year smoke test
    python blue_planet_prize_to_s3.py --allow-shrink

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

BASE_URL = "https://www.af-info.or.jp/en/blueplanet"
LIST_INDEX_URL = f"{BASE_URL}/list.html"
ABOUT_URL      = f"{BASE_URL}/about.html"

# Awarding body — Asahi Glass Foundation.
# Verified F4320309996, country JP, DOI 10.13039/100007684, no ROR.
FUNDER_ID = 4320309996
FUNDER_DISPLAY_NAME = "Asahi Glass Foundation"

PROVENANCE = "blue_planet_prize"
# Currency per official current rule (af-info.or.jp/en/blueplanet/about.html
# fetched 2026-05-21): "Each recipient is presented with a certificate of merit,
# a commemorative trophy, and 500,000 USD in prize money."
CURRENCY = "USD"
BLUE_PLANET_AMOUNT_USD = 500_000.0

# Year range: prize started 1992; current year is the latest published list.
EARLIEST_YEAR = 1992
LATEST_YEAR   = 2025  # adjust upward as new years publish

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/blue_planet_prize/blue_planet_prize_laureates.parquet"

USER_AGENT = "openalex-walden-blueplanet-ingest/1.0 (+https://openalex.org)"

# Polite throttle. af-info.or.jp is a static site, no documented rate limit;
# 0.4s between pages = ~150 req/min, well below any reasonable WAF threshold.
MIN_REQUEST_INTERVAL_S = 0.4


# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html, application/xhtml+xml, */*; q=0.01",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    # af-info.or.jp serves UTF-8 but does NOT declare a charset on the
    # Content-Type header, so requests falls back to ISO-8859-1. That
    # corrupts the Japanese wave dash (`～` U+FF5E) used in lifespan
    # notation ("1928～2005") into a 3-byte Latin-1 sequence, breaking
    # the lifespan regex below. Force UTF-8 to recover the proper char.
    if resp.encoding and resp.encoding.lower() in ("iso-8859-1", "latin-1"):
        resp.encoding = "utf-8"
    return resp


# =============================================================================
# Year discovery + URL-format handling (schema drift: hyphen vs underscore)
# =============================================================================

def fetch_year_index() -> list[int]:
    """Discover all years that have laureate pages by scanning the master
    list.html — the foundation publishes a year link for each published
    cohort. We don't fall back to a hardcoded year range so the script
    keeps working as new years are added."""
    resp = _http_get(LIST_INDEX_URL)
    resp.raise_for_status()
    html = resp.text
    yrs_dash  = set(re.findall(r'list-(\d{4})\.html',  html))
    yrs_under = set(re.findall(r'list_(\d{4})\.html',  html))
    all_yrs = sorted(int(y) for y in (yrs_dash | yrs_under))
    print(f"  discovered {len(all_yrs)} laureate years on /list.html: "
          f"{all_yrs[0]}-{all_yrs[-1]}")
    return all_yrs


def fetch_year_page(year: int) -> tuple[str, str]:
    """Fetch the year listing page. Tries hyphen-format first (pre-2024
    convention), falls back to underscore-format (post-2024). Returns
    (url, html). Raises on 404 from both formats."""
    for sep in ("-", "_"):
        url = f"{BASE_URL}/list{sep}{year}.html"
        resp = _http_get(url)
        if resp.status_code == 200:
            return url, resp.text
        elif resp.status_code != 404:
            resp.raise_for_status()
    raise RuntimeError(
        f"Year {year} not found at either list-{year}.html or list_{year}.html "
        "— foundation may have changed URL format or removed the page."
    )


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: index + one year page reachable + parseable")
    print("=" * 60)
    years = fetch_year_index()
    if not years or years[0] != EARLIEST_YEAR:
        print(f"[WARN] earliest year {years[0] if years else 'NONE'} != "
              f"expected {EARLIEST_YEAR}")
    url, html = fetch_year_page(years[0])
    laureates = parse_year_page(html, years[0])
    print(f"  fetched {url}")
    print(f"  parsed {len(laureates)} laureates: "
          f"{[l['name_clean'][:50] for l in laureates]}")
    if not laureates:
        print("[ERROR] parsed 0 laureates from earliest year — selector may have changed")
        sys.exit(3)


# =============================================================================
# Page parsing
# =============================================================================

# Each laureate section starts at `id="text-0N"` (typically anchors 01-02 for
# the two laureates per year; occasionally text-03 for three-recipient years).
# The c-ttl-lv02 heading carries the name in modern (~2004+) pages. Older
# pages (2003 + earlier joint-prize years) use a card-style layout where the
# name lives in `c-card__text` inside a `c-card__body` block; we fall back
# to that pattern. The fallback extracts ALL c-card__text names from the
# section, in declaration order.
_NAME_HEADING_RE = re.compile(
    r'class="c-ttl-lv02"[^>]*>\s*(.+?)\s*</',
    re.DOTALL,
)
_CARD_NAME_RE = re.compile(
    r'class="c-card__text"[^>]*>\s*([^<]{3,120}?)\s*</',
)
_CARD_COUNTRY_RE = re.compile(
    r'class="c-card__text--small"[^>]*>\s*([^<]{2,40}?)\s*</',
)

# Heuristic split: "Title FirstName LastName (Country)" vs "Organization Name (Founded in X)"
# Strip honorifics like Dr./Professor/Mr./Ms.
_TITLE_PREFIX_RE = re.compile(r'^(?:Dr\.|Prof\.|Professor|Mr\.|Ms\.|Mrs\.|Sir)\s+', re.I)
# Capture ALL parenthetical groups at the end of the heading (org names can have
# `(IIED) (Founded in the UK)` — two parens — and the founding paren must be
# classified separately from the acronym paren).
_PAREN_ALL_RE = re.compile(r'\(([^()]+)\)')
_FOUNDING_PAREN_RE = re.compile(r'^(?:Founded|Established)\s+(?:in|on)\b', re.I)

# Born / Founded markers — keep terminator optional (Blue Planet's HTML doesn't
# put periods after these clauses; affiliation runs immediately after the year).
# Year is captured; trailing `, in [Country]` is consumed so affiliation starts
# at the actual institution name.
_BORN_RE     = re.compile(
    r'Born\s+(?:in|on)\s+(?:[A-Z][a-z]+\s+\d{1,2},\s+)?(\d{4})'
    r'(?:\s*,?\s*in\s+(?:the\s+)?[A-Z][\w\s]+?(?=\s+[A-Z]))?',
    re.I,
)
# Older laureates (pre-~2000) often omit "Born in" and instead use a
# "lifespan" notation: "1928ï½2005" (UTF-8 mojibake for the Japanese
# wave dash U+FF5E) or "1925–2007" or "1931-2019". Detect this format.
_LIFESPAN_RE = re.compile(
    r'\b(\d{4})\s*[ï~–\-–—～]\s*(\d{4})\b'
)
_FOUNDED_RE  = re.compile(
    r'(?:Founded|Established)\s+(?:in|on)\s+(?:[A-Z][a-z]+\s+\d{1,2},\s+)?(\d{4})'
    r'(?:\s*,?\s*in\s+(?:the\s+)?[A-Z][\w\s]+?(?=\s+[A-Z]))?',
    re.I,
)
# Acronym detector — parens with 2-6 uppercase letters look like acronyms,
# NOT country codes. Used to keep "(IPBES)" / "(IIED)" out of the country slot.
_ACRONYM_PAREN_RE = re.compile(r'^[A-Z][A-Z0-9]{1,5}$')


def _strip_html(s: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip()


def parse_year_page(html: str, year: int) -> list[dict]:
    """Parse a year-listing page into a list of laureate dicts."""
    out = []
    # Section boundaries: text-01 ... text-02 ... end-of-content
    # Use re.split-style: find all `id="text-0N"` positions
    # Most years have exactly text-01 + text-02 (two laureates). A handful
    # of years (e.g. 2003, 2012, 2014) have a third laureate at text-03 — the
    # foundation occasionally honors three recipients when categories overlap.
    section_starts = [(m.group(1), m.start()) for m in re.finditer(
        r'id="?(text-0\d)"?', html)]
    if not section_starts:
        return []
    section_starts.append(("__END__", len(html)))
    for i in range(len(section_starts) - 1):
        anchor = section_starts[i][0]
        s, e = section_starts[i][1], section_starts[i + 1][1]
        sec_html = html[s:e]
        record = parse_section(sec_html, year, anchor)
        if record:
            out.append(record)

    # Fallback for joint-prize / pre-2004 card-style years (2003 etc.). Two
    # or three laureates share a single anchored section, and individual names
    # live in `<div class="c-card__text">` cards rather than `c-ttl-lv02`
    # headings. Pick up any extra names that weren't matched above.
    if len(out) < 2:
        captured = {r["name_clean"].lower() for r in out}
        for j, nm in enumerate(_CARD_NAME_RE.finditer(html)):
            raw_name = _strip_html(nm.group(1))
            if not raw_name:
                continue
            # Apply same honorific strip used for c-ttl-lv02 names so
            # de-duplication compares apples to apples ("Dr. Vo Quy" in the
            # card vs "Vo Quy" parsed from c-ttl-lv02 must match).
            name_clean_check = _TITLE_PREFIX_RE.sub("", raw_name).strip().lower()
            if name_clean_check in captured:
                continue
            # The country tag is rendered immediately after the name card
            country = None
            tail_html = html[nm.end():nm.end() + 800]
            cm = _CARD_COUNTRY_RE.search(tail_html)
            if cm:
                country = _strip_html(cm.group(1))
            name_clean = _TITLE_PREFIX_RE.sub("", raw_name).strip()
            slug_seed = re.sub(r'[^a-z0-9]+', '-', name_clean.lower()).strip('-')[:60]
            synth_anchor = f"card-{j+1:02d}"
            funder_award_id = f"blue-planet-{year}-{synth_anchor}-{slug_seed}"
            out.append({
                "funder_award_id":  funder_award_id,
                "year":             year,
                "anchor":           synth_anchor,
                "raw_heading":      raw_name,
                "name_clean":       name_clean,
                "country":          country,
                "recipient_kind":   "individual",
                "birth_year":       None,
                "death_year":       None,
                "founding_year":    None,
                "affiliation":      None,
                "contribution":     None,
                "landing_page_url": None,
            })
            captured.add(name_clean.lower())
    return out


def parse_section(sec_html: str, year: int, anchor: str) -> Optional[dict]:
    """Parse one #text-0N section. Returns None if the heading isn't found."""
    m = _NAME_HEADING_RE.search(sec_html)
    if not m:
        return None
    raw_heading = _strip_html(m.group(1))
    if not raw_heading:
        return None

    # Parse parentheticals from the heading. Two patterns we see:
    #   "Dr. Syukuro Manabe (USA)"                           ← individual + country
    #   "IIED (acronym) (Founded in the UK)"                 ← organization + founding paren
    #   "Prof. Robert Costanza (USA & Australia)"            ← multi-country
    # Strategy: find all parens; classify each. If a paren starts with
    # "Founded in" or "Established in", that's the founding/HQ paren —
    # remove it from the heading and remember it. Whatever non-founding
    # paren remains at the end is the country.
    country = None
    founding_paren = None
    name_no_paren = raw_heading
    # walk parens right-to-left so we strip from the tail inward
    parens = list(_PAREN_ALL_RE.finditer(raw_heading))
    # Determine if the heading looks like a person (honorific prefix) — used to
    # distinguish a tail "(USA)" / "(Sweden)" country code from an organization
    # acronym like "(IPBES)". Both are 3-6 uppercase letters; the honorific
    # tells us which interpretation is correct.
    heading_is_person = bool(_TITLE_PREFIX_RE.match(raw_heading))
    for p in reversed(parens):
        inner = p.group(1).strip()
        if _FOUNDING_PAREN_RE.match(inner):
            founding_paren = inner
            name_no_paren = (name_no_paren[:p.start()] + name_no_paren[p.end():]).strip()
        elif country is None and p.end() == len(raw_heading):
            # tail-anchored paren. Strip if:
            #   - heading_is_person → always country (e.g. "Dr. X (USA)")
            #   - NOT an acronym → country/qualifier (e.g. "Dr. X (USA & UK)", "Org (Indonesia)")
            # Don't strip if heading is org-style AND the paren is an acronym
            # (e.g. "Intergovernmental Science (IPBES)").
            if heading_is_person or not _ACRONYM_PAREN_RE.match(inner):
                country = inner
                name_no_paren = (name_no_paren[:p.start()] + name_no_paren[p.end():]).strip()
        # else: interior paren or acronym attached to org name — keep in heading.
    # Clean whitespace
    name_no_paren = re.sub(r'\s+', ' ', name_no_paren).strip()

    # Strip honorific prefix
    name_clean = _TITLE_PREFIX_RE.sub("", name_no_paren).strip()

    # Get visible text of the section (post-heading, pre "For more Details")
    after_heading = sec_html[m.end():]
    cutoff = re.search(r'For more Details', after_heading)
    if cutoff:
        after_heading = after_heading[:cutoff.start()]
    body_text = _strip_html(after_heading)

    # Classify individual vs organization based on Born/Founded/lifespan markers
    born_m     = _BORN_RE.search(body_text)
    found_m    = _FOUNDED_RE.search(body_text)
    lifespan_m = _LIFESPAN_RE.search(body_text)
    death_year = None
    if born_m:
        recipient_kind = "individual"
        birth_year = int(born_m.group(1))
        founding_year = None
        marker_end = born_m.end()
    elif lifespan_m:
        # "1928~2005" or "1931-2019" — older laureates, no explicit "Born in"
        recipient_kind = "individual"
        birth_year = int(lifespan_m.group(1))
        death_year = int(lifespan_m.group(2))
        founding_year = None
        marker_end = lifespan_m.end()
    elif found_m or founding_paren:
        recipient_kind = "organization"
        birth_year = None
        if found_m:
            founding_year = int(found_m.group(1))
            marker_end = found_m.end()
        else:
            # parse year from founding_paren
            yr = re.search(r'\b(1[89]\d\d|20[0-2]\d)\b', founding_paren or "")
            founding_year = int(yr.group(1)) if yr else None
            marker_end = 0
    else:
        recipient_kind = "unknown"
        birth_year = None
        founding_year = None
        marker_end = 0

    # Extract affiliation (the phrase immediately after the Born/Founded
    # marker, terminated by a sentence break OR a contribution-keyword).
    # Contribution keywords: Pioneering, Groundbreaking, In a/an, Over,
    # His/Her/Their + verb, capitalized verb-starting sentence.
    affil = None
    contribution = None
    if marker_end:
        post = body_text[marker_end:].strip(' .,;:')
        # Try affiliation: text up to a contribution-keyword
        contrib_keywords = (
            r'Pioneering|Groundbreaking|Outstanding|Pioneering work|For pioneering|'
            r'His\s+(?:research|work|contributions?)|Her\s+(?:research|work|contributions?)|'
            r'Their\s+(?:research|work|contributions?)|In a groundbreaking|'
            r'Over\s+(?:more\s+than\s+)?\d+\s+years|In \d{4}\b'
        )
        am = re.match(rf'(.{{5,300}}?)(?=\s+(?:{contrib_keywords}))', post)
        if am:
            affil = am.group(1).strip(' .,;:')
            contribution = post[am.end():].strip(' .,;:')
        else:
            # Heuristic fallback: split at the LONGEST clause that ends in
            # "Foundation/Institute/University/Laboratory/Center/Agency".
            am2 = re.match(r'(.{5,300}?\b(?:Foundation|Institute|University|Laboratory|Center|Centre|Agency|Society|Department|Service)\b[^.]*?)(?=\s+[A-Z])', post)
            if am2:
                affil = am2.group(1).strip(' .,;:')
                contribution = post[am2.end():].strip(' .,;:')
            else:
                # Last resort: first 150 chars is affil, rest is contribution
                affil = post[:150].strip(' .,;:') if len(post) > 30 else None
                contribution = post[150:].strip(' .,;:') if len(post) > 150 else None

    if not contribution:
        contribution = body_text[marker_end:].strip(' .,;:') or None

    # Slug for funder_award_id
    slug_seed = re.sub(r'[^a-z0-9]+', '-', name_clean.lower()).strip('-')[:60]
    funder_award_id = f"blue-planet-{year}-{anchor}-{slug_seed}"

    return {
        "funder_award_id":   funder_award_id,
        "year":              year,
        "anchor":            anchor,
        "raw_heading":       raw_heading,
        "name_clean":        name_clean,
        "country":           country,
        "recipient_kind":    recipient_kind,
        "birth_year":        birth_year,
        "death_year":        death_year,
        "founding_year":     founding_year,
        "affiliation":       affil,
        "contribution":      contribution,
        "landing_page_url":  None,  # set per-row below to year-list URL
    }


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}


def split_name(full_name: str) -> tuple[Optional[str], Optional[str]]:
    """Return (given_name, family_name) following runbook §2.4.1.
    Returns (None, name) for organizations (single-token names with no
    obvious split — caller should use recipient_kind to gate this)."""
    if not full_name:
        return None, None
    name = full_name.strip()
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()
    toks = name.split()
    while toks and toks[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


# =============================================================================
# Download loop
# =============================================================================

def download_all(year_filter: Optional[int], output_dir: Path) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download Blue Planet Prize laureate pages by year")
    print("=" * 60)
    years = fetch_year_index()
    if year_filter:
        years = [y for y in years if y == year_filter]
        if not years:
            print(f"[ERROR] year {year_filter} not in published list")
            sys.exit(4)
    rows: list[dict] = []
    for year in years:
        url, html = fetch_year_page(year)
        records = parse_year_page(html, year)
        for r in records:
            r["landing_page_url"] = url
            rows.append(r)
        print(f"  {year}: {len(records)} laureates  ({url})")
    print(f"\n  ✓ collected {len(rows)} laureate records across "
          f"{len(years)} years")
    return rows


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw records")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for r in all_rows:
        if r["funder_award_id"] in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {r['funder_award_id']} — "
                "Blue Planet Prize (year, anchor) should be unique per "
                "runbook prize pattern"
            )
        seen_award_ids.add(r["funder_award_id"])

        kind = r["recipient_kind"]
        if kind == "individual":
            given, family = split_name(r["name_clean"])
            affil_name = r.get("affiliation")
        elif kind == "organization":
            given, family = None, None
            # For orgs the heading itself is the affiliation
            affil_name = r["name_clean"]
        else:
            given, family = split_name(r["name_clean"])
            affil_name = r.get("affiliation")

        display_name = f"Blue Planet Prize {r['year']} — {r['name_clean']}"

        # Description: contribution text + country/founding context where present
        desc_parts = []
        if r.get("contribution"):
            desc_parts.append(r["contribution"])
        if r.get("country"):
            desc_parts.append(f"Origin: {r['country']}")
        description = ". ".join(desc_parts) if desc_parts else None

        final.append({
            "funder_award_id":   r["funder_award_id"],
            "year":              r["year"],
            "anchor":            r["anchor"],
            "name_clean":        r["name_clean"],
            "raw_heading":       r["raw_heading"],
            "given_name":        given,
            "family_name":       family,
            "country":           r.get("country"),
            "recipient_kind":    kind,
            "birth_year":        r.get("birth_year"),
            "founding_year":     r.get("founding_year"),
            "affiliation_name":  affil_name,
            "display_name":      display_name,
            "description":       description,
            "amount":            BLUE_PLANET_AMOUNT_USD,
            "currency":          CURRENCY,
            "start_date":        f"{r['year']}-01-01",
            "end_date":          f"{r['year']}-12-31",
            "landing_page_url":  r["landing_page_url"],
            "declined":          False,
        })

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  recipient kinds: {df['recipient_kind'].value_counts().to_dict()}")
    print(f"  countries seen: {df['country'].nunique()}")
    print(f"  affiliation coverage: "
          f"{df['affiliation_name'].notna().sum()}/{len(df)} "
          f"({df['affiliation_name'].notna().sum()*100/len(df):.0f}%)")
    print(f"  description coverage: "
          f"{df['description'].notna().sum()}/{len(df)} "
          f"({df['description'].notna().sum()*100/len(df):.0f}%)")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}  "
          f"({df['year'].nunique()} distinct years)")

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
    parquet_path = output_dir / "blue_planet_prize_laureates.parquet"
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
    prev_path = output_dir / "_prev_blue_planet_prize.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/blueplanet"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse blue_planet_prize_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--year", type=int, default=None,
                        help="Only fetch a single year (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Blue Planet Prize → S3 Pipeline (PRIZE PATTERN, static-HTML scrape)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "blue_planet_prize_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        all_rows = download_all(args.year, args.output_dir)
        raw_path = args.output_dir / "blue_planet_prize_raw.json"
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
    print(f"Next: notebooks/awards/CreateBluePlanetPrizeAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
