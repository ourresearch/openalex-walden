#!/usr/bin/env python3
"""
MacArthur Fellows → S3 Pipeline (FELLOWSHIP PATTERN, Crownpeak Solr search)
=============================================================================

Downloads MacArthur Fellows ("Genius Grant" recipients) from the John D.
and Catherine T. MacArthur Foundation's public **Crownpeak Solr search
index** at `searchg2.crownpeak.net/live-macfound-redesign-rt/select`.
The MacArthur Fellowship is given annually since 1981 — no specific
field, no application accepted, recipients chosen by an anonymous
selection committee. Award is $800,000 paid quarterly over 5 years.

Source authority
----------------
`macfound.org` is the foundation's own site. The Fellows search page
at `/programs/awards/fellows/results` is a SPA powered by a Crownpeak
Solr backend (Crownpeak is the foundation's CMS vendor). Discovery:

  1. Loaded `/programs/awards/fellows/results` in a browser.
  2. The page links to `/js/fellows.js` + `/js/crownpeak.searchg2-1.0.3.js`.
  3. fellows.js contains `collection: 'live-macfound-redesign-rt'` and
     the standard searchg2 URL pattern
     `https://searchg2.crownpeak.net/%%COLLECTION%%/%%HANDLER%%`.
  4. Hitting `searchg2.crownpeak.net/live-macfound-redesign-rt/select`
     returns a Solr response — public endpoint, no auth.
  5. Filter on `custom_s_template:"fellow page"` gives 1,174 fellow
     records (1981-2025, 45 distinct years).

This is the first ingest on the project to use **Crownpeak Solr** as
the data source — a new CMS family distinct from Drupal/JSON:API
(Lemelson #134), WordPress/REST (Hertz/CIFAR), Nuxt+Craft/GraphQL
(Welch #133), Webflow (WFP #131), Next.js (Heineken — failed), and
FacetWP (Hewlett #121). Documented in the script header.

Per-fellow fields available in the Solr response:
  - custom_s_name             → full name
  - custom_s_class_year       → fellowship year (1981-2025)
  - custom_s_title            → field-of-work description (e.g. "Nanotechnologist")
  - custom_s_age              → age at award
  - custom_s_area_display     → primary area (Biochemistry, etc.)
  - custom_s_area2_display    → secondary area (often present)
  - custom_s_association      → home institution at time of award
  - custom_s_city / state     → award-time location
  - custom_s_birth_state_display → birth state (US fellows)
  - custom_s_country          → country
  - custom_s_short_bio        → bio paragraph (HTML)
  - custom_s_url              → /fellows/class-of-{year}/{slug}
  - custom_s_profile_photo    → image URL (audit only)

Amount: $800,000 per fellow per the foundation's About page
(verified 2026-05-22 verbatim: "Each fellowship comes with an award
of $800,000 to the recipient, paid out in equal quarterly installments
over five years."). Historical amounts were lower (the original 1981
stipend was tied to age) but the foundation publishes only the current
rule, so we ship $800K uniformly and document the assumption. §6.7
amount-coverage NOT waived.

Awarding body in OpenAlex: John D. and Catherine T. MacArthur Foundation
(F4320306142, US, DOI 10.13039/100000870).

Output
------
s3://openalex-ingest/awards/macarthur_fellows/macarthur_fellows.parquet

Usage
-----
    python macarthur_fellows_to_s3.py                # full run (1174 fellows, ~3 paginated Solr calls)
    python macarthur_fellows_to_s3.py --skip-upload
    python macarthur_fellows_to_s3.py --skip-download
    python macarthur_fellows_to_s3.py --allow-shrink

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
import urllib.parse
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

SOLR_BASE = "https://searchg2.crownpeak.net/live-macfound-redesign-rt/select"
FELLOW_FILTER = 'custom_s_template:"fellow page"'

FUNDER_ID = 4320306142
FUNDER_DISPLAY_NAME = "John D. and Catherine T. MacArthur Foundation"

PROVENANCE = "macarthur_fellows"
CURRENCY = "USD"
MACARTHUR_FELLOWSHIP_AMOUNT_USD = 800_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/macarthur_fellows/macarthur_fellows.parquet"

USER_AGENT = "openalex-walden-macarthur-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.5


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


def _solr_query(rows: int, start: int = 0) -> str:
    params = {
        "q": FELLOW_FILTER,
        "rows": str(rows),
        "start": str(start),
        "wt": "json",
        "sort": "custom_s_class_year asc, custom_s_sort_name asc",
    }
    return f"{SOLR_BASE}?{urllib.parse.urlencode(params)}"


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: Crownpeak Solr endpoint reachable")
    print("=" * 60)
    resp = _http_get(_solr_query(rows=1))
    resp.raise_for_status()
    j = resp.json()
    found = j.get("response", {}).get("numFound", 0)
    print(f"  numFound: {found}")
    if found < 1000:
        print(f"[ERROR] expected ~1,174 fellows, got {found} — selector or collection may have changed")
        sys.exit(3)
    docs = j["response"]["docs"]
    if docs:
        d0 = docs[0]
        print(f"  sample: {d0.get('custom_s_name')!r} ({d0.get('custom_s_class_year')})")


# =============================================================================
# Solr paginated fetch
# =============================================================================

def fetch_all_fellows() -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch all MacArthur Fellows via Crownpeak Solr")
    print("=" * 60)
    page_size = 500
    start = 0
    all_docs: list[dict] = []
    total = None
    while True:
        resp = _http_get(_solr_query(rows=page_size, start=start))
        resp.raise_for_status()
        j = resp.json()
        if total is None:
            total = j["response"]["numFound"]
            print(f"  total fellows: {total}")
        docs = j["response"]["docs"]
        all_docs.extend(docs)
        print(f"  start={start}: +{len(docs)} (running {len(all_docs)}/{total})")
        if len(docs) < page_size or len(all_docs) >= total:
            break
        start += page_size
    return all_docs


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r'^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame)\s+',
    re.I,
)


def split_name(full_name: str) -> tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    name = _HONORIFIC_PREFIXES_RE.sub("", full_name).strip()
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


def _strip_html(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip() or None


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_docs: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_docs)} raw Solr docs")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for d in all_docs:
        name = d.get("custom_s_name") or d.get("metadata_title") or ""
        year_str = d.get("custom_s_class_year")
        if not year_str:
            continue
        try:
            year = int(year_str)
        except (TypeError, ValueError):
            continue

        # Slug from custom_s_url e.g. /fellows/class-of-2007/paul-rothemund
        url_path = d.get("custom_s_url") or ""
        slug = url_path.rsplit("/", 1)[-1] if url_path else re.sub(
            r'[^a-z0-9]+', '-', name.lower()).strip('-')
        if not slug:
            continue

        funder_award_id = f"macarthur-fellow-{year}-{slug}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "MacArthur Fellow (year, slug) should be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        given, family = split_name(name)

        # Compose description from field-of-work title + short bio
        field_title = d.get("custom_s_title")
        bio = _strip_html(d.get("custom_s_short_bio"))
        # Cap bio length
        if bio and len(bio) > 800:
            bio = bio[:800].rsplit(" ", 1)[0] + "…"
        desc_parts: list[str] = []
        if field_title:
            desc_parts.append(field_title.strip())
        if bio:
            desc_parts.append(bio)
        description = ". ".join(desc_parts) if desc_parts else None

        # Affiliation = current association at time of award
        affil_name = d.get("custom_s_association")
        # Country: Solr field may be "United States" or 2-letter — normalize to display name
        country_display = d.get("custom_s_country") or None

        # Primary area (one of ~10 broad categories)
        area = d.get("custom_s_area_display") or d.get("custom_s_area")

        display_name = f"MacArthur Fellowship {year} — {name}"

        landing = f"https://www.macfound.org{url_path}" if url_path else None

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              year,
            "slug":              slug,
            "name":              name,
            "given_name":        given,
            "family_name":       family,
            "field_title":       field_title,
            "area":              area,
            "affiliation":       affil_name,
            "country":           country_display,
            "age_at_award":      d.get("custom_s_age"),
            "display_name":      display_name,
            "description":       description,
            "amount":            MACARTHUR_FELLOWSHIP_AMOUNT_USD,
            "currency":          CURRENCY,
            "start_date":        f"{year}-01-01",
            "end_date":          f"{year+4}-12-31",  # 5-year payout window
            "landing_page_url":  landing,
            "declined":          False,
        })

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  unique years: {df['year'].nunique()}")
    print(f"  field_title coverage: {df['field_title'].notna().sum()}/{len(df)} "
          f"({df['field_title'].notna().sum()*100/len(df):.0f}%)")
    print(f"  affiliation coverage: {df['affiliation'].notna().sum()}/{len(df)} "
          f"({df['affiliation'].notna().sum()*100/len(df):.0f}%)")
    print(f"  country coverage: {df['country'].notna().sum()}/{len(df)} "
          f"({df['country'].notna().sum()*100/len(df):.0f}%)")
    print(f"  description coverage: {df['description'].notna().sum()}/{len(df)}")

    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "macarthur_fellows.parquet"
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
    prev_path = output_dir / "_prev_macarthur.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/macarthur"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("MacArthur Fellows → S3 Pipeline (Crownpeak Solr search)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "macarthur_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_docs = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_docs)} cached docs")
    else:
        all_docs = fetch_all_fellows()
        raw_path = args.output_dir / "macarthur_raw.json"
        raw_path.write_text(json.dumps(all_docs, ensure_ascii=False, indent=2))
        print(f"\n  Cached {len(all_docs)} raw docs to {raw_path}")

    df = build_dataframe(all_docs)
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
    print(f"Next: notebooks/awards/CreateMacArthurFellowsAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
