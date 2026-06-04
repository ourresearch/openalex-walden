#!/usr/bin/env python3
"""
Novo Nordisk Foundation GRANTS (funded projects) -> S3   (GRANT PATTERN, method-3 search API)
=============================================================================================

Downloads the Novo Nordisk Foundation's open **grant-recipient** database
(the GRANT-funding side) and writes a project-level parquet for the
OpenAlex awards pipeline.

This is the FUNDING side, distinct from the foundation's PRIZE side that is
already ingested (priority 119, provenance ``novo_nordisk_fonden_prizes``,
``CreateNovoNordiskFondenAwards.ipynb``). Different provenance, different
S3 path, different synthetic-ID namespace -> no double-counting. See the
notebook header for the de-dup reasoning.

Source authority
----------------
NNF runs WordPress (same site as the prize archive). The grant-recipient
search at ``/en/grant-recipient/`` is backed by an admin-ajax "content_api"
endpoint that returns the canonical funded-projects list as HTML fragments:

    /wp/wp-admin/admin-ajax.php?action=content_api&type=grant_recipients&meta_key=year

This is method-3 on the runbook ingest ladder (a search/index API behind the
public search) — preferred over scraping rendered pages. The endpoint reports
``data-total-posts`` (the authoritative corpus size) and accepts ``page`` +
``per_page``; we use it as the loop terminator (runbook Step 1: "empty page !=
end of corpus").

Each list item carries exactly these fields (no per-grant detail page exists):
  - owner-name   : the PI/recipient name OR an institution name
  - owner-title  : the PI's role (e.g. "Postdoctoral Researcher"); empty for
                   institution-level grants
  - location     : the host institution
  - description  : the project title
  - amount       : "DKK 16.151.127"  (Danish number format: '.' = thousands)
  - year         : the grant (award/decision) year

Amount handling (NO Step 6.7 waiver)
------------------------------------
NNF publishes the grant amount in **DKK** on every row, so we map
amount + currency='DKK'. amount is parsed from the Danish-formatted string
("DKK 16.151.127" -> 16151127.0). Rows with no/zero amount get NULL amount.

Fields the source does NOT publish (mapped NULL / honest):
  - start/end dates: only a single grant *year* is published. We ship
    start_year = year and leave start_date/end_date for the notebook to derive
    (YYYY-01-01); end_date / end_year stay NULL.
  - native grant ID: none. funder_award_id is a synthetic stable hash of
    (owner_name|location|description|amount|year). Collision detection raises.
  - per-grant landing page: none (the list is the only surface). landing_page
    is NULL.

funder_scheme enrichment
------------------------
The list rows don't carry the scheme inline, but the search page exposes a
``category`` filter (the grant programme / scheme). Categories do NOT partition
the corpus (sum of per-category counts < total), so the unfiltered pass is the
authoritative complete corpus and the category pass is an *enrichment* that
tags funder_scheme onto matching rows (keyed by the same identity hash). Rows
with no category match keep funder_scheme = NULL. Disable with --no-scheme.

PI / institution split
----------------------
A row is institution-level when owner-title is empty AND owner-name == location
(e.g. "John Innes Centre" / "" / "John Innes Centre"). Those get NULL
lead_investigator and affiliation.name = location. Otherwise owner-name is a
person: split with the canonical split_name helper (runbook 2.4.1) and set
affiliation.name = location.

Awarding funder in OpenAlex:
  Novo Nordisk Fonden (F4320325957, DK, DOI 10.13039/501100009708).

Output
------
  s3://openalex-ingest/awards/novo_nordisk/novo_nordisk_projects.parquet

Usage
-----
    python scripts/local/novo_nordisk_to_s3.py --limit 40 --skip-upload   # smoke
    python scripts/local/novo_nordisk_to_s3.py --skip-upload              # full local
    python scripts/local/novo_nordisk_to_s3.py                            # full + upload
    python scripts/local/novo_nordisk_to_s3.py --no-scheme                # skip enrichment
    python scripts/local/novo_nordisk_to_s3.py --allow-shrink             # override 1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import html as _html
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) -----------------
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

    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None,
                   newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


# =============================================================================
# Configuration
# =============================================================================

LIST_PAGE_URL = "https://novonordiskfonden.dk/en/grant-recipient/"
API_URL = "https://novonordiskfonden.dk/wp/wp-admin/admin-ajax.php"
API_QUERY = "action=content_api&type=grant_recipients&meta_key=year"

FUNDER_ID = 4320325957
FUNDER_DISPLAY_NAME = "Novo Nordisk Fonden"
PROVENANCE = "novo_nordisk_fonden_grants"
CURRENCY = "DKK"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/novo_nordisk/novo_nordisk_projects.parquet"

USER_AGENT = "openalex-walden-novo-nordisk-grants-ingest/1.0 (+https://openalex.org)"
REQUEST_DELAY_S = 0.3
# The content_api offset pagination has a broken CMS record around offset ~1826
# (in every ordering) that truncates whatever page-window *contains* it, though
# OFFSET-skipping *past* it works fine. A smaller page size shrinks the lost
# band: per_page=500 loses ~174 rows, per_page=100 loses ~74 (~99.2% coverage).
# 100 is the balance of coverage vs request count (~90 pages for ~9k grants).
PER_PAGE = 100
REQUEST_TIMEOUT = 90
MAX_RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5

DEFAULT_OUTPUT_DIR = Path("data/novo_nordisk")

# --- canonical PI name splitter (ported verbatim from wolf_to_s3.py, 2.4.1) --
_NAME_SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii",
                  "iv", "jr", "sr"}
_HONORIFIC_RE = re.compile(
    r"^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Mrs\.?|Ms\.?|Miss)\s+", re.I
)


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    cleaned = _HONORIFIC_RE.sub("", name.strip())
    cleaned = _HONORIFIC_RE.sub("", cleaned)
    tokens = cleaned.split()
    while tokens and tokens[-1].lower().strip(",.") in _NAME_SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# Organizational-recipient detector. Some grants are awarded to an organization
# (NGO, university, institute, foundation, museum) rather than a person; the
# owner-name is then the org and must NOT be split into given/family (runbook
# §6.4a — institution must not appear as a PI name). Observed org recipients all
# have an empty owner-title, so we only apply this when the title is empty to
# avoid demoting titled people. Tokens are org-specific enough that a person
# name won't match (Danish + English).
# Danish glues compound words (Rigshospitalet, Carlsbergfondet, Hjerteforeningen),
# so most tokens are matched as plain substrings, not word-bounded. The few
# tokens that could collide with real surnames are deliberately excluded:
#   - generic "kors" would hit Korsgren/Korsholm/Korsgaard (real people) — only
#     the standalone "røde kors" is kept;
#   - generic "center" would hit "Name, Steno Diabetes Center ..." person+
#     affiliation strings — NNF's own centres are caught via "foundation".
# institutional classification additionally requires an empty owner-title, which
# every observed org recipient has, so a titled person can't be demoted.
_ORG_TOKEN_RE = re.compile(
    r"(?i)("
    r"fond|foundation|stiftung|forening|forbund|selskab|komit|"
    r"universit|institut|hospital|klinik|museum|akademi|academy|"
    r"kommune|region|college|skole|gymnasium|akvarium|"
    r"unicef|røde kors|red cross|caritas|adra|dalberg|astra|madkultur|"
    r"experimentarium|kræftens|sclerose|indsamling|aid services|"
    r"government of|agency for|society|\btrust\b|videnskab|"
    r"danmark|denmark|grønland|greenland|\baps\b|\ba/s\b"
    r")"
)


def looks_like_org(name: Optional[str]) -> bool:
    return bool(name) and bool(_ORG_TOKEN_RE.search(name))


# =============================================================================
# HTTP
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def http_get(url: str) -> tuple[int, str]:
    """Throttled GET with retries. Returns (status_code, text)."""
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - elapsed)
        try:
            resp = _session.get(url, timeout=REQUEST_TIMEOUT)
            _last_request_t = time.monotonic()
            return resp.status_code, resp.text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            _last_request_t = time.monotonic()
            sleep = min(2 ** attempt, 20)
            log(f"  retry {attempt}/{MAX_RETRIES} for {url[:90]}: {exc} (sleep {sleep}s)")
            time.sleep(sleep)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {url}") from last_exc


# =============================================================================
# Parsing
# =============================================================================

_TOTAL_RE = re.compile(r'data-total-posts="(\d+)"')


def _clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = re.sub(r"\s+", " ", _html.unescape(text)).strip()
    return text or None


def _field(block: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, block, re.S)
    return _clean(m.group(1)) if m else None


def parse_amount(raw: Optional[str]) -> Optional[float]:
    """'DKK 16.151.127' -> 16151127.0 ; 'DKK 2.495.722,50' -> 2495722.5."""
    if not raw:
        return None
    s = raw.upper().replace("DKK", "").replace("\xa0", " ").strip()
    s = re.sub(r"[^\d.,]", "", s)
    if not s:
        return None
    # Danish format: '.' thousands, ',' decimal.
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "")
    try:
        val = float(s)
    except ValueError:
        return None
    return val if val > 0 else None


def parse_items(fragment: str) -> list[dict]:
    rows: list[dict] = []
    blocks = fragment.split('class="nnf-grant-list__item single-grant"')[1:]
    for bl in blocks:
        owner_name = _field(bl, r'owner-name">(.*?)</div>')
        owner_title = _field(bl, r'owner-title">(.*?)</div>')
        location = _field(bl, r'__location.*?info-text">(.*?)</div>')
        description = _field(bl, r'__description.*?info-text">(.*?)</div>')
        amount_raw = _field(bl, r'__amount.*?info-text">(.*?)</div>')
        year = _field(bl, r'__year.*?info-text">(.*?)</div>')
        if not (owner_name or description):
            continue
        rows.append({
            "owner_name_raw": owner_name,
            "owner_title_raw": owner_title,
            "location": location,
            "description": description,
            "amount_raw": amount_raw,
            "award_year": year,
        })
    return rows


def make_award_id(row: dict) -> str:
    key = "|".join([
        (row.get("owner_name_raw") or "").casefold(),
        (row.get("location") or "").casefold(),
        (row.get("description") or "").casefold(),
        row.get("amount_raw") or "",
        row.get("award_year") or "",
    ])
    return "nnf-grant-" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def enrich_row(row: dict) -> dict:
    owner_name = row.get("owner_name_raw")
    owner_title = row.get("owner_title_raw")
    location = row.get("location")

    title_empty = not (owner_title and owner_title.strip())
    name_eq_loc = (owner_name or "").casefold() == (location or "").casefold()
    institutional = bool(owner_name) and title_empty and (
        name_eq_loc or looks_like_org(owner_name)
    )
    if institutional:
        # The org IS the recipient/affiliation; the location field is unreliable
        # for these rows (sometimes carries a project description), so use the
        # org name as the affiliation.
        given, family = None, None
        affiliation = owner_name
    else:
        given, family = split_name(owner_name)
        affiliation = location

    amount = parse_amount(row.get("amount_raw"))
    yr = row.get("award_year")
    year_int = yr if (yr and re.fullmatch(r"\d{4}", yr)) else None

    return {
        "funder_award_id": make_award_id(row),
        "display_name": row.get("description"),
        "owner_name_raw": owner_name,
        "owner_title_raw": owner_title,
        "lead_given_name": given,
        "lead_family_name": family,
        "is_institutional": "true" if institutional else "false",
        "institution": affiliation,
        "amount": str(amount) if amount is not None else None,
        "amount_raw": row.get("amount_raw"),
        "currency": CURRENCY if amount is not None else None,
        "award_year": year_int,
        "funder_scheme": None,  # filled by enrichment pass
        "provenance": PROVENANCE,
        "funder_id": str(FUNDER_ID),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Fetch passes
# =============================================================================

def _fetch_page(query: str, page: int) -> tuple[Optional[int], list[dict]]:
    """One page. Returns (reported_total_or_None, items). Raises after too many non-200s."""
    consecutive_non200 = 0
    while True:
        url = f"{API_URL}?{query}&per_page={PER_PAGE}&page={page}"
        status, text = http_get(url)
        if status != 200:
            consecutive_non200 += 1
            log(f"  page {page}: HTTP {status} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"Too many non-200s on page {page}; aborting to avoid truncation.")
            continue
        m = _TOTAL_RE.search(text)
        total = int(m.group(1)) if m else None
        return total, parse_items(text)


def fetch_all(limit: Optional[int]) -> tuple[list[dict], int]:
    """Page the unfiltered corpus. Returns (rows, reported_total).

    Offset pagination here can return a *short* page mid-corpus. Some are
    transient flakes (retried below); one is structural — a broken CMS record
    near offset ~1826 truncates whatever page-window contains it (see PER_PAGE
    note). Short pages are NOT redistributed to later pages, so accepting one
    silently drops grants; we retry any non-last short page before accepting it.
    Trailing empty pages report data-total-posts="0", so we keep the *max*
    reported total, never clobbering the real count.
    """
    rows: list[dict] = []
    reported_total = 0
    consecutive_empty = 0
    page = 1
    while True:
        total, items = _fetch_page(API_QUERY, page)
        if total is not None and total > reported_total:
            reported_total = total

        # Expected size for this page (last page is legitimately short).
        expected = PER_PAGE
        if reported_total:
            last_page = (reported_total + PER_PAGE - 1) // PER_PAGE
            if page >= last_page:
                expected = reported_total - (last_page - 1) * PER_PAGE

        # Retry a short non-last page (flake) before accepting it.
        if items and len(items) < expected and not limit:
            recovered = items
            for attempt in range(1, 4):
                log(f"  page {page}: short ({len(recovered)}/{expected}); retry {attempt}/3")
                time.sleep(1.0)
                _t, retry_items = _fetch_page(API_QUERY, page)
                if len(retry_items) >= len(recovered):
                    recovered = retry_items
                if len(recovered) >= expected:
                    break
            items = recovered
            if len(items) < expected:
                log(f"  page {page}: still short after retries ({len(items)}/{expected}); accepting")

        if not items:
            consecutive_empty += 1
            log(f"  page {page}: 0 items ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY})")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            page += 1
            continue
        consecutive_empty = 0
        rows.extend(items)
        log(f"  page {page}: +{len(items)} items (total parsed {len(rows)} / reported {reported_total})")

        if limit and len(rows) >= limit:
            rows = rows[:limit]
            log(f"  --limit {limit} reached; stopping")
            break
        if reported_total and len(rows) >= reported_total:
            break
        page += 1
    return rows, reported_total


def discover_categories() -> list[tuple[str, str]]:
    """(value, scheme_name) pairs from the page's category <select>."""
    status, html = http_get(LIST_PAGE_URL)
    if status != 200:
        log(f"  could not load list page for categories (HTTP {status}); skipping enrichment")
        return []
    i = html.find('name="category"')
    if i < 0:
        return []
    seg = html[i:html.find("</select>", i)]
    out: list[tuple[str, str]] = []
    for val, txt in re.findall(r'<option[^>]*value="([^"]+)"[^>]*>(.*?)</option>', seg):
        name = _clean(txt)
        if val and name:
            out.append((val, name))
    return out


def enrich_schemes(by_id: dict[str, dict]) -> int:
    """For each category, fetch its grants and tag funder_scheme by identity hash."""
    cats = discover_categories()
    log(f"Scheme enrichment: {len(cats)} categories")
    tagged = 0
    scheme_map: dict[str, set[str]] = {}
    for ci, (val, name) in enumerate(cats, 1):
        page = 1
        consecutive_empty = 0
        consecutive_non200 = 0
        seen_here = 0
        cat_total = None
        while True:
            url = f"{API_URL}?{API_QUERY}&category={val}&per_page={PER_PAGE}&page={page}"
            status, text = http_get(url)
            if status != 200:
                consecutive_non200 += 1
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    log(f"  [{ci}/{len(cats)}] {name[:40]}: too many non-200; moving on")
                    break
                page += 1
                continue
            consecutive_non200 = 0
            m = _TOTAL_RE.search(text)
            if m:
                cat_total = int(m.group(1))
            items = parse_items(text)
            if not items:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    break
                page += 1
                continue
            consecutive_empty = 0
            for r in items:
                scheme_map.setdefault(make_award_id(r), set()).add(name)
            seen_here += len(items)
            if cat_total and seen_here >= cat_total:
                break
            if len(items) < PER_PAGE and not (cat_total and seen_here < cat_total):
                break
            page += 1
        if ci % 20 == 0:
            log(f"  [{ci}/{len(cats)}] categories scanned")
    for award_id, names in scheme_map.items():
        if award_id in by_id:
            by_id[award_id]["funder_scheme"] = " | ".join(sorted(names))
            tagged += 1
    log(f"  tagged funder_scheme on {tagged}/{len(by_id)} grants")
    return tagged


# =============================================================================
# Assemble / validate / write / upload
# =============================================================================

STRING_COLS = [
    "funder_award_id", "display_name", "owner_name_raw", "owner_title_raw",
    "lead_given_name", "lead_family_name", "is_institutional", "institution",
    "amount", "amount_raw", "currency", "award_year", "funder_scheme",
    "provenance", "funder_id", "downloaded_at",
]


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in STRING_COLS:
        if col not in df.columns:
            df[col] = None
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    df = df[STRING_COLS].astype("string")
    return df


def validate(df: pd.DataFrame, reported_total: int) -> None:
    n = len(df)
    log(f"Local validation: {n} unique rows (source reported {reported_total})")
    if n == 0:
        raise RuntimeError("No rows parsed — aborting.")
    if df["funder_award_id"].nunique() != n:
        raise RuntimeError("funder_award_id collision after dedup — aborting.")

    def pct(col: str) -> None:
        c = int(df[col].notna().sum())
        log(f"  {col:<18}{c:>7}/{n}  ({100.0 * c / n:5.1f}%)")

    for col in ["display_name", "owner_name_raw", "lead_family_name", "institution",
                "amount", "currency", "award_year", "funder_scheme"]:
        pct(col)
    inst = int((df["is_institutional"] == "true").sum())
    log(f"  institutional rows {inst}/{n} ({100.0 * inst / n:4.1f}%)")
    amt = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amt):
        log(f"  amount(DKK) min={amt.min():,.0f} max={amt.max():,.0f} "
            f"mean={amt.mean():,.0f} total={amt.sum():,.0f}")
    yrs = pd.to_numeric(df["award_year"], errors="coerce").dropna()
    if len(yrs):
        log(f"  year range {int(yrs.min())}-{int(yrs.max())}")
    log(f"  top PI family names: {df['lead_family_name'].value_counts().head(3).to_dict()}")


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 required for upload; use --skip-upload locally.") from exc
    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = local_path.with_suffix(".prev.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_rows = len(pd.read_parquet(prev_path))
        log(f"  previous S3 rows={prev_rows:,}, new rows={new_rows:,}")
        if new_rows < prev_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {prev_rows:,}. "
                f"Use --allow-shrink to override."
            )
    except client.exceptions.ClientError:
        log("  No existing S3 parquet; first ingest.")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Novo Nordisk Foundation grants -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke: stop after N grants (also skips enrichment)")
    parser.add_argument("--no-scheme", action="store_true", help="Skip the funder_scheme category-enrichment pass")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("Novo Nordisk Foundation GRANTS ingest starting")
    log(f"source={LIST_PAGE_URL}")
    log(f"funder_id={FUNDER_ID}  provenance={PROVENANCE}")

    raw_rows, reported_total = fetch_all(args.limit)
    log(f"Fetched {len(raw_rows)} raw rows (reported total {reported_total})")

    records = [enrich_row(r) for r in raw_rows]
    by_id = {r["funder_award_id"]: r for r in records}

    if args.no_scheme or args.limit:
        log("Scheme enrichment skipped (--no-scheme or --limit set).")
    else:
        enrich_schemes(by_id)

    df = build_dataframe(list(by_id.values()))
    validate(df, reported_total)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "novo_nordisk_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(out, args.allow_shrink)


if __name__ == "__main__":
    main()
