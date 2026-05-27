#!/usr/bin/env python3
"""
Carnegie Corporation of New York to S3 Data Pipeline
=====================================================

Downloads the foundation's grants database from carnegie.org's public
paginated grants listing at /grants/grants-database/?page=N. Each
listing card carries the full per-grant record inline (year, grantee
org, USD amount, program) — no per-grant detail fetch needed.

Source authority
----------------
carnegie.org is the awarding body's own site. The /grants/grants-database/
page paginates 1..372 (verified 2026-05-27 by walking the first-page
pagination markup and probing page 373 → returns the same cached last-
page content as 372). At 25 cards/page × 371 full pages + 5 cards on
page 372 the expected corpus is ~9,280 grants.

**NOTE on CLAUDE.md's burned-source list**: Carnegie Corporation is
listed as Cloudflare-403-blocked in the handoff doc, but all my probes
on 2026-05-27 returned HTTP 200 cleanly with the full listing markup.
Either Carnegie has relaxed the WAF rule or the original block was
specific to a different User-Agent. Documented in the PR body — if
Cloudflare re-blocks during the admin's Databricks run, fall back to
Playwright (Method 6).

Schema choices vs the project's standard funder schema
------------------------------------------------------
Carnegie funds organizations directly (universities, think tanks,
NGOs), not individual researchers — same pattern as Hewlett #86 and
Kauffman #139. `lead_investigator` ships as org-only (given/family/
orcid NULL, affiliation.name = grantee org, country = 'US' since
Carnegie's program scope is US + international but the data doesn't
distinguish per-grant country reliably; we conservatively ship US for
the foundation domicile only when the grantee is a known US org, else
NULL — but in practice the source doesn't expose grantee country, so
country ships NULL throughout to avoid inferring).

Each card surfaces 4 fields:
  - Year         -> start_year (integer 4-digit)
  - Grantee      -> affiliation.name
  - Amount       -> amount (USD, parsed from "$X,YYY" string)
  - Program      -> funder_scheme

Provenance: `carnegie_corporation` (verified count=0 on production).

Amount and currency
-------------------
USD per the foundation's domicile. Amounts on the listing pages are
formatted as plain "$X,YYY" strings (e.g. "$300,000"); we parse the
digit-comma sequence into a float. Expected 100% amount coverage.

Output
------
s3://openalex-ingest/awards/carnegie/carnegie_grants.parquet

Usage
-----
    python carnegie_to_s3.py                    # full run (~3 min, 372 pages)
    python carnegie_to_s3.py --skip-upload      # local dev
    python carnegie_to_s3.py --skip-download    # reuse cached JSON
    python carnegie_to_s3.py --limit 3          # smoke test (first N pages)
    python carnegie_to_s3.py --allow-shrink     # override §1.4

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

SITE_BASE = "https://www.carnegie.org"
LISTING_URL = f"{SITE_BASE}/grants/grants-database/"

# Awarding body — Carnegie Corporation of New York.
# Verified F4320306125, country US, DOI 10.13039/100000308.
FUNDER_ID = 4320306125
FUNDER_DISPLAY_NAME = "Carnegie Corporation of New York"

PROVENANCE = "carnegie_corporation"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/carnegie/carnegie_grants.parquet"

USER_AGENT = "openalex-walden-carnegie-ingest/1.0 (+https://openalex.org)"

# Polite throttle — Carnegie has ~372 pages. At 0.4s/page = ~150s total.
MIN_REQUEST_INTERVAL_S = 0.4

# Hard upper bound on pages walked; loop exits on duplicate-content detection.
MAX_PAGES = 500


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
# Card extraction
# =============================================================================

# Each grant card is anchored by `data-url="/grants/grants-database/grant/{id}/"`.
# Card body renders 4 fields in order: Year, Grantee Org, $Amount, Program.
# The non-greedy capture stops at the next `data-url=` or end-of-listing markers.
_CARD_RE = re.compile(
    r'(<[^>]+data-url="(?P<url>[^"]*grants-database/grant/[^"]+)"[^>]*>'
    r'(?P<body>.{0,5000}?))'
    r'(?=<[^>]+data-url="[^"]*grants-database/grant/|'
    r'</section|</main|</article|class="pager)',
    re.DOTALL,
)

_AMOUNT_RE = re.compile(r"\$([0-9][\d,]*)")
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def _parse_grant_id(url: str) -> str:
    m = re.search(r"/grant/([^/]+?)/?$", url)
    return m.group(1) if m else url


def parse_listing_page(html: str) -> list[dict]:
    """Return one dict per grant card on a listing page."""
    out: list[dict] = []
    for m in _CARD_RE.finditer(html):
        url = m.group("url")
        body = m.group("body")
        text_parts = [p.strip() for p in re.split(r"</?[a-z][^>]*>", body) if p.strip()]
        # Heuristic: filter out generic UI fragments (tooltip text etc.).
        # The 4 substantive fields appear in order: year, grantee, $amount, program.
        year: Optional[str] = None
        grantee: Optional[str] = None
        amount_str: Optional[str] = None
        program: Optional[str] = None
        for chunk in text_parts:
            clean = unescape(re.sub(r"\s+", " ", chunk)).strip()
            if not clean:
                continue
            if year is None and _YEAR_RE.fullmatch(clean):
                year = clean
                continue
            if amount_str is None and clean.startswith("$") and _AMOUNT_RE.fullmatch(clean):
                amount_str = clean
                continue
            # First non-year, non-amount field is the grantee org.
            if grantee is None and year is not None:
                grantee = clean
                continue
            # Last meaningful field is the program.
            if grantee is not None and amount_str is not None and program is None:
                program = clean
                continue
        out.append({
            "grant_id":   _parse_grant_id(url),
            "url":        SITE_BASE + url if url.startswith("/") else url,
            "year":       int(year) if year else None,
            "grantee":    grantee,
            "amount_str": amount_str,
            "program":    program,
        })
    return out


def _amount_str_to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = _AMOUNT_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: page 1 + page 200 parse + pagination ceiling")
    print("=" * 60)
    r = _http_get(f"{LISTING_URL}?page=1")
    r.raise_for_status()
    cards = parse_listing_page(r.text)
    print(f"  page 1: {len(cards)} cards parsed")
    if len(cards) < 10:
        print(f"[ERROR] page 1 returned only {len(cards)} cards; selector changed?")
        sys.exit(3)
    s = cards[0]
    print(f"  sample: id={s['grant_id']} year={s['year']} grantee={s['grantee']!r} amount={s['amount_str']!r} program={s['program']!r}")
    # Probe page 200 for stability
    r2 = _http_get(f"{LISTING_URL}?page=200")
    r2.raise_for_status()
    cards2 = parse_listing_page(r2.text)
    print(f"  page 200: {len(cards2)} cards parsed")
    # Pagination ceiling
    last_m = re.search(r'page=(\d+)[^"]*"[^>]*class="[^"]*pager(?:-last)?', r.text)
    last_from_page1 = re.findall(r'page=(\d+)', r.text)
    if last_from_page1:
        print(f"  pagination ceiling from page-1 pager links: {max(int(p) for p in last_from_page1)}")


# =============================================================================
# Download
# =============================================================================

def download_grants(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print(f"Step 1: Walk {LISTING_URL}?page=N until duplicate content")
    print("=" * 60)
    all_grants: list[dict] = []
    seen_ids: set[str] = set()
    last_page_signature: Optional[tuple] = None
    page_n = 1
    # Terminators (in priority order):
    # 1. MAX_PAGES ceiling.
    # 2. Duplicate-content (server caches last page for out-of-range N).
    # Empty pages and transient non-200s are NOT terminators (v2 §1: Carnegie
    # has Cloudflare/WAF history; a 503 or empty card grid is a flake, not EOF).
    # We log and continue, but cap consecutive failures so we don't spin forever.
    consecutive_empty = 0
    consecutive_non200 = 0
    MAX_CONSECUTIVE_EMPTY = 3
    MAX_CONSECUTIVE_NON200 = 5
    while page_n <= MAX_PAGES:
        if limit is not None and page_n > limit:
            print(f"  [LIMIT] stopping after {limit} pages")
            break
        r = _http_get(f"{LISTING_URL}?page={page_n}")
        if r.status_code != 200:
            consecutive_non200 += 1
            print(f"  page {page_n}: HTTP {r.status_code} "
                  f"(consecutive_non200={consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(
                    f"download_grants: {consecutive_non200} consecutive non-200 responses "
                    f"ending at page {page_n}; refusing to short-circuit corpus walk "
                    f"(Carnegie WAF is the usual culprit; retry later or escalate to "
                    f"agent-browser per v2 §1)."
                )
            page_n += 1
            continue
        consecutive_non200 = 0
        cards = parse_listing_page(r.text)
        if not cards:
            consecutive_empty += 1
            print(f"  page {page_n}: 0 cards "
                  f"(consecutive_empty={consecutive_empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                # 3 empty pages in a row is probably real EOF, not a flake.
                print(f"  page {page_n}: {MAX_CONSECUTIVE_EMPTY} consecutive empty pages → end of corpus")
                break
            page_n += 1
            continue
        consecutive_empty = 0
        # Detect end-of-pagination via duplicate-content (server caches the last page for any out-of-range N).
        sig = tuple(c["grant_id"] for c in cards)
        if sig == last_page_signature:
            print(f"  page {page_n}: same card IDs as previous page → end of corpus")
            break
        last_page_signature = sig
        new_in_chunk = 0
        for c in cards:
            if c["grant_id"] in seen_ids:
                continue
            seen_ids.add(c["grant_id"])
            all_grants.append(c)
            new_in_chunk += 1
        if page_n % 25 == 0 or page_n <= 3:
            print(f"  page {page_n}: +{len(cards)} cards ({new_in_chunk} new; total unique grants={len(all_grants)})")
        page_n += 1

    raw_path = output_dir / "carnegie_raw.json"
    raw_path.write_text(json.dumps(all_grants, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_grants)} grants ({page_n - 1} pages walked) to {raw_path}")
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
        gid = r.get("grant_id")
        if not gid:
            continue
        funder_award_id = f"carnegie-{gid}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)
        grantee = (r.get("grantee") or "").strip() or None
        amount = _amount_str_to_float(r.get("amount_str"))
        rows.append({
            "funder_award_id":  funder_award_id,
            "grant_id":         gid,
            "year":             r.get("year"),
            "grantee_org":      grantee,
            "amount":           amount,
            "currency":         "USD" if amount is not None else None,
            "program":          r.get("program"),
            "landing_page_url": r.get("url"),
        })

    df = pd.DataFrame.from_records(rows)
    n_g = df["grantee_org"].astype(bool).sum()
    n_y = df["year"].notna().sum()
    n_a = df["amount"].notna().sum()
    n_p = df["program"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: grantee_org={n_g} ({n_g*100/len(df):.0f}%) "
          f"year={n_y} ({n_y*100/len(df):.0f}%) "
          f"amount={n_a} ({n_a*100/len(df):.0f}%) "
          f"program={n_p} ({n_p*100/len(df):.0f}%)")
    if n_a:
        amts = df["amount"].dropna()
        print(f"  amount stats: min=${amts.min():,.0f} median=${amts.median():,.0f} max=${amts.max():,.0f}  total=${amts.sum():,.0f}")
    if n_y:
        yrs = df["year"].dropna().astype(int)
        print(f"  year range: {yrs.min()} – {yrs.max()}")
    print(f"\n  By program (top 12):")
    print(df.groupby("program", dropna=False).size().sort_values(ascending=False).head(12).to_string())
    # Runbook §1.2.5 — astype("string") before parquet to dodge pyarrow int-inference.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "carnegie_grants.parquet"
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
    prev_path = output_dir / "_prev_carnegie_grants.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/carnegie"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse carnegie_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only fetch first N listing pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Carnegie Corporation of New York → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "carnegie_raw.json"
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
    print(f"Next: notebooks/awards/CreateCarnegieAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
