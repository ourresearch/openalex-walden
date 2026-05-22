#!/usr/bin/env python3
"""
Hewlett Foundation grants → S3 Data Pipeline (FacetWP method-3)
==================================================================

Downloads grants from the William and Flora Hewlett Foundation's
public grant database (`hewlett.org/grants/`) via direct POST to
their FacetWP refresh endpoint. Iterates 9 program facets × ~100
pages each. ~7,000 grants total (subject to FacetWP's 1000-row
per-query cap; see "Known limitations" below).

Source authority
----------------
hewlett.org is the awarding body's own site (WordPress + FacetWP),
no auth required. The grant search at `/grants/database/` is driven
by a same-origin POST to `https://hewlett.org/grants/` with a
JSON FacetWP `facetwp_refresh` payload. Replicated directly here
without browser automation — the captured XHR payload format was
extracted from the live page via Playwright once, then the format
is stable enough to replay via plain HTTP. Method #3 on the runbook
ladder (search/index APIs).

Schema per grant card (HTML parsed via regex on Hewlett's
`ActiveGrantItem-*` CSS classes):
  - ActiveGrantItem-title         → grantee org name
  - ActiveGrantItem-program       → Hewlett program name (funder_scheme)
  - ActiveGrantItem-projectTitle  → short project description
                                    ('for general operating support', etc.)
  - ActiveGrantItem-status-text   → Active / Closed
  - (?:Active|Closed)GrantItem-meta          → amount ($N,NNN.NN) + term + award date
  - ActiveGrantItem-overview      → full description paragraph
  - (?:Active|Closed)GrantItem-link (href)   → grantee website URL

Known limitations
-----------------
FacetWP caps each filtered query at 1,000 results. The full
unfiltered total is 19,926 grants (verified 2026-05-20), but
program-filtered queries cap as follows:

  Economy and Society:           394 rows
  Education:                    1000 rows (CAPPED — actual >1000)
  Effective Philanthropy:        952 rows
  Environment:                  1000 rows (CAPPED)
  Gender Equity and Governance: 1000 rows (CAPPED)
  Performing Arts:              1000 rows (CAPPED)
  Racial Justice:                 98 rows
  Special Projects:             1000 rows (CAPPED)
  U.S. Democracy:                915 rows
  --
  Sum of available:    ~7,359 grants

The 5 capped programs each lose ~50%+ of their grants to the
FacetWP cap. There is no year facet, no status facet, and no
alphabetic-prefix filter exposed on the Hewlett UI to slice
further. To get the remaining ~12,500 grants requires either:
  (a) Hewlett enabling year/decade facets,
  (b) Aggressively slicing via the `grant_search` text-search
      facet by alphabetic prefix, OR
  (c) WP REST `/wp/v2/grantee` (3,162 grantee orgs) + per-grantee
      detail-page scrape to recover the missing grants.

This script ships option (a)-floor: all accessible cards across
9 programs, documenting the 1000-cap limitation explicitly in the
tracker row and the notebook header so the boss + future contractors
know the per-program coverage. Follow-up work to recover the missing
~12,500 grants is a Step 0 entry in the tracker.

Output
------
s3://openalex-ingest/awards/hewlett/hewlett_grants.parquet

Usage
-----
    python hewlett_to_s3.py                # full run (~7K grants, ~5 min)
    python hewlett_to_s3.py --skip-upload  # local dev
    python hewlett_to_s3.py --skip-download
    python hewlett_to_s3.py --programs 31392,21943  # subset
    python hewlett_to_s3.py --max-pages 5  # smoke-test (5 pages per program)
    python hewlett_to_s3.py --allow-shrink

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

XHR_URL = "https://hewlett.org/grants/"
REFERER = "https://hewlett.org/grants/"

# Awarding body — William and Flora Hewlett Foundation.
# Verified F4320307873, country US.
FUNDER_ID = 4320307873
FUNDER_DISPLAY_NAME = "William and Flora Hewlett Foundation"

PROVENANCE = "hewlett_facetwp"
CURRENCY = "USD"  # Hewlett is US-based; all grants in USD

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/hewlett/hewlett_grants.parquet"

USER_AGENT = "openalex-walden-hewlett-ingest/1.0 (+https://openalex.org)"

# Polite throttle. Hewlett's FacetWP doesn't document rate limits; 0.6 sec
# between pages = ~100 req/min, comfortably below any reasonable WAF threshold.
MIN_REQUEST_INTERVAL_S = 0.6

# 9 Hewlett programs (extracted from the FacetWP grant_programs_dropdown
# facet on 2026-05-20). The empty-string 'Any' option is omitted — we
# iterate programs explicitly to defeat the 1000-row unfiltered cap.
PROGRAMS = [
    ("24372977", "Economy and Society"),
    ("31392",    "Education"),
    ("31528",    "Effective Philanthropy"),
    ("21943",    "Environment"),
    ("21300",    "Gender Equity and Governance"),
    ("31521",    "Performing Arts"),
    ("29057789", "Racial Justice"),
    ("31532",    "Special Projects"),
    ("70696",    "U.S. Democracy"),
]


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


def build_payload(program_id: Optional[str], paged: int) -> dict:
    return {
        "action": "facetwp_refresh",
        "data": {
            "facets": {
                "grant_search": "",
                "grant_programs_dropdown": [program_id] if program_id else [],
                "search_details": [],
                "pagination": [],
            },
            "frozen_facets": {},
            "http_params": {"get": {}, "uri": "grants", "url_vars": []},
            "template": "wp",
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
    resp = _http_post(build_payload(None, 1))
    resp.raise_for_status()
    j = resp.json()
    if "template" not in j or "settings" not in j:
        print(f"[ERROR] response missing 'template'/'settings' keys; got {list(j.keys())}")
        sys.exit(3)
    pager = j["settings"].get("pager", {})
    total_unfiltered = pager.get("total_rows_unfiltered")
    print(f"  unfiltered grant total: {total_unfiltered}")
    if total_unfiltered is None or total_unfiltered < 5000:
        print(f"[WARN] unfiltered total {total_unfiltered} below expected ~20K — schema may have changed")
    cards = len(re.findall(r'class="GrantsListing-item"', j["template"]))
    print(f"  grant cards on page 1: {cards}")
    if cards == 0:
        print("[ERROR] zero cards found in template — selector may have changed")
        sys.exit(3)


# =============================================================================
# Card parsing
# =============================================================================

# Each grant card is an <li class="GrantsListing-item"> ... </li> block.
# Inside, the relevant fields are wrapped by class="ActiveGrantItem-{name}".
_CARD_BLOCK_RE = re.compile(
    r'(<li[^>]*class="[^"]*GrantsListing-item[^"]*"[^>]*>.*?</li>)',
    re.DOTALL,
)

# Field-extraction helpers — strip HTML tags from the inner content of the
# wrapping element by class name.
def _field(card: str, class_name: str) -> Optional[str]:
    m = re.search(
        rf'class="(?:Active|Closed)GrantItem-{re.escape(class_name)}[^"]*"[^>]*>(.+?)</',
        card,
        re.DOTALL,
    )
    if not m:
        return None
    text = re.sub(r'<[^>]+>', ' ', m.group(1))
    text = unescape(re.sub(r'\s+', ' ', text)).strip()
    return text or None


# Hewlett's `(?:Active|Closed)GrantItem-meta` block holds multiple `<p class="(?:Active|Closed)GrantItem-text">`
# children — amount, term, awarded-date. Parse all three.
_TEXT_PIECE_RE = re.compile(
    r'class="(?:Active|Closed)GrantItem-text"[^>]*>(.+?)</p>',
    re.DOTALL,
)


def _parse_meta(card: str) -> dict[str, Optional[str]]:
    out = {"amount_raw": None, "term_raw": None, "date_awarded_raw": None}
    # Restrict regex to the meta block specifically so we don't catch other text
    meta_block = re.search(
        r'class="(?:Active|Closed)GrantItem-meta[^"]*"[^>]*>(.+?)</div>',
        card,
        re.DOTALL,
    )
    if not meta_block:
        return out
    pieces = _TEXT_PIECE_RE.findall(meta_block.group(1))
    for p in pieces:
        text = re.sub(r'<[^>]+>', ' ', p)
        text = unescape(re.sub(r'\s+', ' ', text)).strip()
        if not text:
            continue
        if text.startswith("$"):
            out["amount_raw"] = text
        elif "Term:" in text or text.lower().endswith("month") or text.lower().endswith("months"):
            out["term_raw"] = text.replace("Term:", "").strip()
        elif "Awarded:" in text or "Award:" in text:
            out["date_awarded_raw"] = re.sub(r'(Awarded|Award):\s*', '', text).strip()
    return out


# `<a href="..." class="(?:Active|Closed)GrantItem-link" ...>` — extract href
_LINK_HREF_RE = re.compile(
    r'<a[^>]*class="[^"]*(?:Active|Closed)GrantItem-link[^"]*"[^>]*href="([^"]+)"',
)
# Some markups put href before class — try that order too
_LINK_HREF_RE_ALT = re.compile(
    r'<a[^>]*href="([^"]+)"[^>]*class="[^"]*(?:Active|Closed)GrantItem-link[^"]*"',
)


def _parse_link(card: str) -> Optional[str]:
    for pat in (_CARD_LINK_HREF, _LINK_HREF_RE, _LINK_HREF_RE_ALT):
        if pat is None:
            continue
        m = pat.search(card)
        if m:
            return unescape(m.group(1))
    return None

# Allow the loop above to find both orderings
_CARD_LINK_HREF = re.compile(
    r'<a[^>]*(?:class="[^"]*(?:Active|Closed)GrantItem-link[^"]*"[^>]*href="([^"]+)"|href="([^"]+)"[^>]*class="[^"]*(?:Active|Closed)GrantItem-link[^"]*")',
)


def parse_card(card_html: str, program_name: str) -> dict:
    title       = _field(card_html, "title")
    projectTitle = _field(card_html, "projectTitle")
    status      = _field(card_html, "status-text")
    overview    = _field(card_html, "overview")
    meta        = _parse_meta(card_html)

    # Extract link href
    link = None
    for pat in (_CARD_LINK_HREF, _LINK_HREF_RE, _LINK_HREF_RE_ALT):
        if pat is None: continue
        m = pat.search(card_html)
        if m:
            # CARD_LINK_HREF has two alternation groups — first one populated is the URL
            link = unescape(next((g for g in m.groups() if g), ""))
            break

    return {
        "grantee":          title,
        "program":          program_name,
        "projectTitle":     projectTitle,
        "status":           status,
        "overview":         overview,
        "amount_raw":       meta["amount_raw"],
        "term_raw":         meta["term_raw"],
        "date_awarded_raw": meta["date_awarded_raw"],
        "grantee_website":  link,
    }


# =============================================================================
# Amount + date parsing
# =============================================================================

# $1,950,000.00 → 1950000.00
def parse_amount(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    s = raw.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


# "Awarded: March 16, 2026" → "2026-03-16"
_MONTH_TO_NUM = {m.lower(): i+1 for i, m in enumerate(
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"])}
_DATE_RE = re.compile(
    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})',
    re.IGNORECASE,
)


def parse_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    m = _DATE_RE.search(raw)
    if not m:
        return None
    month, day, year = m.group(1), m.group(2), m.group(3)
    return f"{year}-{_MONTH_TO_NUM[month.lower()]:02d}-{int(day):02d}"


# "Term: 36.0 Months" → 36 (months)
_TERM_RE = re.compile(r'(\d+(?:\.\d+)?)\s*[Mm]onths?')


def parse_term_months(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    m = _TERM_RE.search(raw)
    if not m:
        return None
    try:
        return int(float(m.group(1)))
    except ValueError:
        return None


# Compute end_date from start_date + term_months
def add_months(iso_start: Optional[str], months: Optional[int]) -> Optional[str]:
    if not iso_start or not months:
        return None
    try:
        y, mo, d = (int(x) for x in iso_start.split("-"))
        # add months
        new_m = mo + months
        new_y = y + (new_m - 1) // 12
        new_mo = ((new_m - 1) % 12) + 1
        # Clamp day to last day of new month if needed (e.g. Jan 31 + 1 mo)
        # Use day=min(d, 28) for safety on Feb edge case; awards aren't sensitive here.
        return f"{new_y:04d}-{new_mo:02d}-{min(d,28):02d}"
    except Exception:
        return None


# =============================================================================
# Download loop
# =============================================================================

def download_program(program_id: str, program_name: str, max_pages: Optional[int],
                     output_dir: Path) -> list[dict]:
    print(f"\n  --- {program_name} (id={program_id}) ---")
    rows: list[dict] = []
    page = 1
    total_rows = None
    while True:
        resp = _http_post(build_payload(program_id, page))
        if resp.status_code != 200:
            print(f"    [page {page}] HTTP {resp.status_code}; stopping program")
            break
        j = resp.json()
        if total_rows is None:
            pager = j.get("settings", {}).get("pager", {})
            total_rows = pager.get("total_rows", 0)
            total_pages = pager.get("total_pages", 0)
            per_page = pager.get("per_page", 10)
            print(f"    total_rows={total_rows} total_pages={total_pages} per_page={per_page}")
        cards = _CARD_BLOCK_RE.findall(j.get("template", ""))
        if not cards:
            print(f"    [page {page}] 0 cards — end of corpus for this program")
            break
        for c in cards:
            parsed = parse_card(c, program_name)
            parsed["facetwp_program_id"] = program_id
            parsed["facetwp_page"]       = page
            rows.append(parsed)
        if page % 10 == 0 or page == 1:
            print(f"    [page {page}] +{len(cards)} cards (running total {len(rows)})")
        page += 1
        if max_pages and page > max_pages:
            print(f"    [LIMIT] stopping at --max-pages={max_pages}")
            break
        if total_pages and page > total_pages:
            break
    print(f"  ✓ {program_name}: {len(rows)} cards collected")
    return rows


# =============================================================================
# Build DataFrame
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}


def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw cards")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for r in all_rows:
        # Generate funder_award_id from (grantee, program, date)
        # Use the date_awarded if present, else fallback to page+grantee+program for stability
        date_iso = parse_date(r.get("date_awarded_raw"))
        amount_f = parse_amount(r.get("amount_raw"))
        term_mo  = parse_term_months(r.get("term_raw"))
        end_iso  = add_months(date_iso, term_mo)

        grantee  = (r.get("grantee") or "").strip()
        program  = (r.get("program") or "").strip()
        if not grantee:
            continue

        # Slug key — case-insensitive, punctuation-stripped
        def slug(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:60]

        award_id_parts = [
            slug(grantee),
            slug(program),
            (date_iso or "no-date").replace("-", ""),
            slug(r.get("projectTitle") or "")[:30],
        ]
        funder_award_id = "hewlett-" + "-".join(p for p in award_id_parts if p)

        if funder_award_id in seen_award_ids:
            # Append a stable disambiguator if multiple awards collide
            i = 2
            base = funder_award_id
            while f"{base}-v{i}" in seen_award_ids:
                i += 1
            funder_award_id = f"{base}-v{i}"
            # We DON'T raise here because Hewlett's FacetWP genuinely shows
            # one grantee getting multiple parallel grants in the same
            # program/date; this is a real collision, not a data bug.
        seen_award_ids.add(funder_award_id)

        final.append({
            "funder_award_id":     funder_award_id,
            "grantee":             grantee,
            "program":             program,
            "facetwp_program_id":  r.get("facetwp_program_id"),
            "projectTitle":        r.get("projectTitle"),
            "status":              r.get("status"),
            "overview":            r.get("overview"),
            "amount":              amount_f,
            "currency":            CURRENCY if amount_f is not None else None,
            "term_months":         term_mo,
            "start_date":          date_iso,
            "end_date":            end_iso,
            "amount_raw":          r.get("amount_raw"),
            "term_raw":            r.get("term_raw"),
            "date_awarded_raw":    r.get("date_awarded_raw"),
            "grantee_website":     r.get("grantee_website"),
            "declined":            False,  # schema parity
        })

    df = pd.DataFrame.from_records(final)
    n_amt = df["amount"].notna().sum()
    n_dt  = df["start_date"].notna().sum()
    n_term = df["term_months"].notna().sum()
    print(f"  rows: {len(df)}  amount={n_amt} ({n_amt*100/len(df):.0f}%)  "
          f"dates={n_dt} ({n_dt*100/len(df):.0f}%)  "
          f"term={n_term} ({n_term*100/len(df):.0f}%)")
    print(f"\n  By program:")
    print(df.groupby("program").size().sort_values(ascending=False).to_string())
    print(f"\n  By status:")
    print(df.groupby("status").size().to_string())

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
    parquet_path = output_dir / "hewlett_grants.parquet"
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
    prev_path = output_dir / "_prev_hewlett_grants.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/hewlett"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse existing per-program JSON dumps in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--programs", type=str, default=None,
                        help="Comma-separated FacetWP program IDs to ingest (default: all 9)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Limit pages per program (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Hewlett Foundation → S3 Pipeline (FacetWP method-3)")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    selected = PROGRAMS
    if args.programs:
        wanted = {p.strip() for p in args.programs.split(",")}
        selected = [(pid, name) for pid, name in PROGRAMS if pid in wanted]
        print(f"\n  Filtered to {len(selected)} programs")

    if args.skip_download:
        # Reuse cached JSON
        raw_path = args.output_dir / "hewlett_grants_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        all_rows: list[dict] = []
        for pid, name in selected:
            rows = download_program(pid, name, args.max_pages, args.output_dir)
            all_rows.extend(rows)
        raw_path = args.output_dir / "hewlett_grants_raw.json"
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
    print(f"Next: notebooks/awards/CreateHewlettAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
