#!/usr/bin/env python3
"""
Vilcek Foundation Prizes → S3 Pipeline (PRIZE PATTERN, WP REST with wrapped envelope)
=======================================================================================

Downloads laureates from all of the Vilcek Foundation's annual prize
programs, which honor the contributions of immigrants to American arts
and sciences. Source: `vilcek.org` — WordPress site with a custom
`/wp-json/wp/v2/prize_recipients` endpoint that returns a NON-STANDARD
**wrapped envelope**: `{success: True, data: {records: [...]}}` instead
of the usual WP REST direct-array response. First ingest on the project
to encounter this pattern; documented here so future contributors who
hit similar wrappers know to dereference `data.records`.

Awarding body in OpenAlex: Vilcek Foundation (F4320307087, US,
DOI 10.13039/100002374).

Programs covered (verified 2026-05-22 from /wp-json/wp/v2/prize_type):
  - Vilcek Prize ($100K, broad — 86 winners)
  - Vilcek Prize for Creative Promise ($50K, early-career — 135 winners)
  - Vilcek Prize for Excellence ($100K — 5 honorees)
  - Marica Vilcek Prize (in art history — 5 honorees)
  - Vilcek Prize for Creative Promise Honoree (40 honorees)

Counts in the taxonomy sum higher than the 184 prize_recipients because
each recipient can carry multiple prize_type tags (e.g., a Creative
Promise winner is also a Vilcek Prize Honoree). The script disambiguates
by taking the *first non-honoree* taxonomy term as the canonical scheme.

Per-record fields (from the WP REST wrapped envelope):
  - id, slug, title.rendered                  → recipient name
  - acf.prize_amount                          → string like "$100,000" or "$50,000"
  - acf.title_role                            → HTML blob with affiliation + bio
  - acf.location                              → "City, State"
  - prize_type[]                              → IDs into prize_type taxonomy
  - prize_year[]                              → IDs into prize_year taxonomy
  - prize_category[]                          → IDs into prize_category taxonomy

Amount: parsed from `acf.prize_amount` string (e.g., "$100,000" → 100000.0).
Most entries carry their amount; honorees may have NULL. §6.7 amount
coverage = best-effort; rows without a parseable amount ship NULL.

Output
------
s3://openalex-ingest/awards/vilcek/vilcek_prizes.parquet

Usage
-----
    python vilcek_prizes_to_s3.py                # full run (~184 recipients, 2 pages + 3 taxonomies)
    python vilcek_prizes_to_s3.py --skip-upload
    python vilcek_prizes_to_s3.py --skip-download
    python vilcek_prizes_to_s3.py --allow-shrink

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

WP_BASE = "https://vilcek.org/wp-json/wp/v2"
RECIPIENTS_URL = f"{WP_BASE}/prize_recipients?per_page=100"
TAXONOMY_URLS = {
    "prize_type":     f"{WP_BASE}/prize_type?per_page=100",
    "prize_year":     f"{WP_BASE}/prize_year?per_page=100",
    "prize_category": f"{WP_BASE}/prize_category?per_page=100",
}

# Awarding body — Vilcek Foundation.
# Verified F4320307087, country US, DOI 10.13039/100002374.
FUNDER_ID = 4320307087
FUNDER_DISPLAY_NAME = "Vilcek Foundation"

PROVENANCE = "vilcek_foundation"
CURRENCY = "USD"

# Per-scheme fallback when ACF prize_amount is missing (most pre-2024 records
# don't carry the dollar string in ACF). Amounts verified 2026-05-22 from each
# prize's own /prizes/{slug}/ overview page on vilcek.org.
SCHEME_AMOUNT_FALLBACK = {
    "Vilcek Prize":                            100_000.0,
    "Vilcek Prize for Excellence":             100_000.0,
    "Marica Vilcek Prize":                     100_000.0,
    "Vilcek Prize for Creative Promise":        50_000.0,
    # Honorees aren't directly priced — no monetary award per program
    # description. Ship NULL with §6.7 partial-waiver.
    "Vilcek Prize for Creative Promise Honoree": None,
}

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/vilcek/vilcek_prizes.parquet"

USER_AGENT = "openalex-walden-vilcek-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.4


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


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: WP REST prize_recipients reachable, wrapper detected")
    print("=" * 60)
    resp = _http_get(RECIPIENTS_URL + "&page=1")
    resp.raise_for_status()
    j = resp.json()
    # Vilcek wraps the response: {success: True, data: {records: [...]}}
    if not isinstance(j, dict) or not j.get("success"):
        print(f"[ERROR] expected wrapper {{success:true, data:{{records:[...]}}}}, got: {str(j)[:200]}")
        sys.exit(3)
    records = j.get("data", {}).get("records", [])
    print(f"  page 1 records: {len(records)}")
    if records:
        r = records[0]
        print(f"  sample: id={r['id']} slug={r.get('slug')!r}")


# =============================================================================
# Paginated fetch with wrapper handling
# =============================================================================

def fetch_all_recipients() -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch all Vilcek prize_recipients via WP REST")
    print("=" * 60)
    all_records: list[dict] = []
    page = 1
    while True:
        url = f"{RECIPIENTS_URL}&page={page}"
        resp = _http_get(url)
        # X-WP-Total / X-WP-TotalPages reported in headers
        total = resp.headers.get("x-wp-total")
        total_pages = resp.headers.get("x-wp-totalpages")
        if resp.status_code == 400 and total_pages and int(page) > int(total_pages):
            break
        resp.raise_for_status()
        j = resp.json()
        records = j.get("data", {}).get("records", []) if isinstance(j, dict) else []
        if not records:
            break
        all_records.extend(records)
        print(f"  page {page}: +{len(records)} (running {len(all_records)}/{total})")
        if total and len(all_records) >= int(total):
            break
        page += 1
        if page > 50:
            break
    return all_records


def fetch_taxonomy(url: str) -> dict[int, dict]:
    """Return {term_id: term_dict}."""
    out: dict[int, dict] = {}
    page = 1
    while True:
        resp = _http_get(f"{url}&page={page}")
        if resp.status_code >= 400:
            break
        terms = resp.json()
        if not isinstance(terms, list) or not terms:
            break
        for t in terms:
            out[t["id"]] = t
        if len(terms) < 100:
            break
        page += 1
    return out


# =============================================================================
# Amount parser
# =============================================================================

_AMOUNT_RE = re.compile(r'\$([\d,]+)')


def parse_amount(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    m = _AMOUNT_RE.search(raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


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

# Honoree-type slugs we deprioritize when selecting the canonical scheme —
# a recipient may carry both their primary prize_type and an "honoree"
# secondary tag; we want the primary.
_HONOREE_SUFFIX_RE = re.compile(r'honoree', re.I)


def _canonical_scheme(prize_type_ids: list[int], type_terms: dict[int, dict]) -> Optional[dict]:
    """Pick the canonical scheme term: the first non-honoree-tagged term in
    prize_type[]. Falls back to the first term if all are honorees."""
    if not prize_type_ids:
        return None
    non_honoree = [
        type_terms[tid] for tid in prize_type_ids
        if tid in type_terms and not _HONOREE_SUFFIX_RE.search(type_terms[tid].get("slug", ""))
    ]
    if non_honoree:
        return non_honoree[0]
    # Fallback — first known term
    for tid in prize_type_ids:
        if tid in type_terms:
            return type_terms[tid]
    return None


def build_dataframe(all_records: list[dict],
                    type_terms: dict[int, dict],
                    year_terms: dict[int, dict],
                    category_terms: dict[int, dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_records)} raw records")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    dropped_no_year = 0

    for r in all_records:
        title_obj = r.get("title", {})
        name = _strip_html(title_obj.get("rendered") if isinstance(title_obj, dict) else title_obj) or ""
        slug = r.get("slug") or re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')

        acf = r.get("acf") or {}
        # amount resolution moved below scheme
        title_role_html = acf.get("title_role")
        location = acf.get("location")
        bio_html = acf.get("biography") or ""

        # Resolve taxonomies
        scheme_term = _canonical_scheme(r.get("prize_type", []) or [], type_terms)
        scheme_name = scheme_term.get("name") if scheme_term else None
        scheme_slug = scheme_term.get("slug") if scheme_term else None

        # Amount: parse ACF prize_amount string first; fall back to scheme map
        # (most pre-2024 records have an empty ACF string).
        amount = parse_amount(acf.get("prize_amount"))
        if amount is None and scheme_name is not None:
            amount = SCHEME_AMOUNT_FALLBACK.get(scheme_name)

        year_ids = r.get("prize_year", []) or []
        year = None
        for yid in year_ids:
            term = year_terms.get(yid)
            if term:
                try:
                    y = int((term.get("name") or "").strip())
                    if 1990 <= y <= 2030:
                        year = y
                        break
                except (TypeError, ValueError):
                    continue
        if year is None:
            dropped_no_year += 1
            continue

        cat_ids = r.get("prize_category", []) or []
        category_name = None
        for cid in cat_ids:
            term = category_terms.get(cid)
            if term:
                category_name = term.get("name")
                break

        # Disambiguate by year + scheme + slug
        sch_slug = scheme_slug or "unknown"
        funder_award_id = f"vilcek-{sch_slug}-{year}-{slug}"
        if funder_award_id in seen_award_ids:
            # Append disambiguator
            i = 2
            base = funder_award_id
            while f"{base}-v{i}" in seen_award_ids:
                i += 1
            funder_award_id = f"{base}-v{i}"
        seen_award_ids.add(funder_award_id)

        given, family = split_name(name)

        # Compose description from title_role + biography
        role_text = _strip_html(title_role_html)
        bio_text = _strip_html(bio_html)
        desc_parts = []
        if role_text:
            desc_parts.append(role_text)
        if bio_text and bio_text != role_text:
            if len(bio_text) > 600:
                bio_text = bio_text[:600].rsplit(" ", 1)[0] + "…"
            desc_parts.append(bio_text)
        description = " | ".join(desc_parts) if desc_parts else None

        display_name = f"{scheme_name or 'Vilcek Prize'} {year} — {name}"

        # Parse location → country guess (most Vilcek recipients are US-based)
        country = None
        if location and "," in location:
            country = "US"  # All Vilcek prizes go to US-based contributors by program rule

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              year,
            "slug":              slug,
            "name":              name,
            "given_name":        given,
            "family_name":       family,
            "scheme":            scheme_name,
            "category":          category_name,
            "affiliation":       role_text,
            "location":          location,
            "country":           country,
            "display_name":      display_name,
            "description":       description,
            "amount":            amount,
            "currency":          CURRENCY if amount is not None else None,
            "start_date":        f"{year}-01-01",
            "end_date":          f"{year}-12-31",
            "landing_page_url":  r.get("link"),
            "declined":          False,
        })

    if dropped_no_year:
        print(f"  [WARN] dropped {dropped_no_year} records with no resolvable year")

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  schemes:")
    print(df.groupby("scheme").size().to_string())
    print(f"  amount coverage: {df['amount'].notna().sum()}/{len(df)} "
          f"({df['amount'].notna().sum()*100/len(df):.0f}%)")
    print(f"  affiliation coverage: {df['affiliation'].notna().sum()}/{len(df)} "
          f"({df['affiliation'].notna().sum()*100/len(df):.0f}%)")
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
    parquet_path = output_dir / "vilcek_prizes.parquet"
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
    prev_path = output_dir / "_prev_vilcek.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/vilcek"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Vilcek Foundation → S3 Pipeline (PRIZE PATTERN, WP REST wrapped envelope)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "vilcek_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        cached = json.loads(raw_path.read_text())
        all_records = cached["records"]
        type_terms = {int(k): v for k, v in cached["type_terms"].items()}
        year_terms = {int(k): v for k, v in cached["year_terms"].items()}
        cat_terms  = {int(k): v for k, v in cached["category_terms"].items()}
        print(f"\n[SKIP] reusing {raw_path} with {len(all_records)} cached records")
    else:
        print("\n" + "=" * 60)
        print("Step 0: Fetch 3 taxonomies (prize_type, prize_year, prize_category)")
        print("=" * 60)
        type_terms = fetch_taxonomy(TAXONOMY_URLS["prize_type"])
        year_terms = fetch_taxonomy(TAXONOMY_URLS["prize_year"])
        cat_terms  = fetch_taxonomy(TAXONOMY_URLS["prize_category"])
        print(f"  prize_type: {len(type_terms)} terms — "
              f"{sorted({t['name'] for t in type_terms.values()})}")
        print(f"  prize_year: {len(year_terms)} terms")
        print(f"  prize_category: {len(cat_terms)} terms")

        all_records = fetch_all_recipients()
        raw_path = args.output_dir / "vilcek_raw.json"
        raw_path.write_text(json.dumps({
            "records": all_records,
            "type_terms": type_terms,
            "year_terms": year_terms,
            "category_terms": cat_terms,
        }, ensure_ascii=False, indent=2, default=str))
        print(f"\n  Cached {len(all_records)} records + 3 taxonomies to {raw_path}")

    df = build_dataframe(all_records, type_terms, year_terms, cat_terms)
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
    print(f"Next: notebooks/awards/CreateVilcekAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
