#!/usr/bin/env python3
"""
Welch Foundation Awards → S3 Pipeline (PRIZE PATTERN, GraphQL via Nuxt SPA)
============================================================================

Downloads laureates from BOTH of the Welch Foundation's annual award
programs:
  - **Welch Award in Chemistry** ($500,000 USD/year since 1972, 61 laureates)
  - **Norman Hackerman Award in Chemical Research** ($100,000 USD/year, 27 laureates)

Source: `welch1.org` — a Nuxt.js + Craft CMS site that exposes its data
via a public GraphQL endpoint at `https://welch1.org/api`. Method #2 on
the runbook ladder (REST/GraphQL API), discovered by:

  1. Loading `/awards/welch-award-in-chemistry/past-recipients` in a browser.
  2. The page is a Nuxt SPA with state embedded as `window.__NUXT__ = ...`.
  3. The embedded payload showed `awardRecipient` entry references but
     no inline laureate list — the data comes from server fetches.
  4. The page's Apollo cache (in `__NUXT__.apollo.defaultClient`) revealed
     the underlying Craft GraphQL schema.
  5. Probing common paths found `/api` returns
     `{"errors":[{"message":"No GraphQL query was supplied"}]}` — a public
     GraphQL endpoint with NO auth.
  6. GraphQL introspection (`{ __schema { types { name } } }`) confirmed
     the `awardRecipients_awardRecipient_Entry` type with `awardYear`,
     `awardCategory`, `recipientAffiliation`, etc.

This is the first ingest on the project to use **GraphQL via SPA-state
discovery** as the method — distinct from REST APIs (CIFAR), CKAN/Socrata
(CONAHCYT), FacetWP (Hewlett/Hertz), and bulk-CSV (NEH). Documented in
the script header so future contributors can apply the same discovery
playbook to other Nuxt/Apollo/Craft sites.

Each recipient row carries:
  - awardYear      → year of the award
  - title          → recipient's full name (Craft `title` field)
  - slug, url      → /awards/{category-slug}/past-recipients/{slug}
  - recipientAffiliation       → home institution
  - recipientDescriptionBlurb  → short citation
  - recipientDescription       → full bio paragraph
  - awardCategory[0].title     → either "Welch Award In Chemistry" or
                                 "Norman Hackerman Award In Chemical Research"

Amount per category (verified 2026-05-22 from each award's overview page):
  - Welch Award in Chemistry:        USD 500,000
  - Norman Hackerman Award:          USD 100,000

Awarding body in OpenAlex: The Welch Foundation (F4320306196, US,
DOI 10.13039/100000928).

Output
------
s3://openalex-ingest/awards/welch/welch_awards.parquet

Usage
-----
    python welch_award_to_s3.py                # full run (~88 recipients, single GraphQL call)
    python welch_award_to_s3.py --skip-upload  # local dev
    python welch_award_to_s3.py --skip-download
    python welch_award_to_s3.py --allow-shrink

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

API_URL = "https://welch1.org/api"

# Awarding body — The Welch Foundation.
# Verified F4320306196, country US, DOI 10.13039/100000928.
FUNDER_ID = 4320306196
FUNDER_DISPLAY_NAME = "Welch Foundation"

PROVENANCE = "welch_foundation"

# Category → amount map. Both verified 2026-05-22 from each award's
# overview page on welch1.org.
CATEGORY_AMOUNTS = {
    "Welch Award In Chemistry":                       500_000.0,
    "Norman Hackerman Award In Chemical Research":    100_000.0,
}
CATEGORY_SCHEMES = {
    "Welch Award In Chemistry":                       "Welch Award in Chemistry",
    "Norman Hackerman Award In Chemical Research":    "Norman Hackerman Award in Chemical Research",
}
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/welch/welch_awards.parquet"

USER_AGENT = "openalex-walden-welch-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.5

# The GraphQL query. Fields selected match the introspection probe.
GRAPHQL_QUERY = """
{
  entries(section: "awardRecipients", limit: 200, orderBy: "awardYear DESC") {
    id
    title
    slug
    url
    ... on awardRecipients_awardRecipient_Entry {
      awardYear
      isAPastRecipient
      awardAdditionalHeader
      recipientAffiliation
      recipientDescriptionBlurb
      recipientDescription
      awardCategory {
        id
        title
        slug
        groupHandle
        ... on awards_Category {
          title
          slug
        }
      }
    }
  }
}
""".strip()


# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_post(url: str, json_body: dict, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.post(url, data=json.dumps(json_body), timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: GraphQL endpoint reachable + introspection works")
    print("=" * 60)
    # Lightweight introspection — confirms the schema has the expected type
    resp = _http_post(API_URL, {
        "query": '{ __type(name: "awardRecipients_awardRecipient_Entry") { name } }'
    })
    resp.raise_for_status()
    j = resp.json()
    type_data = j.get("data", {}).get("__type") or {}
    if type_data.get("name") != "awardRecipients_awardRecipient_Entry":
        print(f"[ERROR] expected awardRecipients_awardRecipient_Entry type, got: {j}")
        sys.exit(3)
    print("  schema confirms awardRecipients_awardRecipient_Entry type")


# =============================================================================
# GraphQL fetch
# =============================================================================

def fetch_recipients() -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch all Welch Foundation recipients via GraphQL")
    print("=" * 60)
    resp = _http_post(API_URL, {"query": GRAPHQL_QUERY})
    resp.raise_for_status()
    j = resp.json()
    if "errors" in j and j.get("errors"):
        print(f"[ERROR] GraphQL returned errors: {j['errors']}")
        sys.exit(4)
    entries = j.get("data", {}).get("entries", [])
    print(f"  fetched {len(entries)} recipient entries")
    # Coverage by category
    import collections
    by_cat = collections.Counter()
    for e in entries:
        cats = e.get("awardCategory") or []
        for c in cats:
            by_cat[c.get("title", "?")] += 1
    print(f"  by category:")
    for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {n}")
    return entries


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

def build_dataframe(all_entries: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_entries)} raw entries")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    dropped_unknown_category = 0

    for e in all_entries:
        cats = e.get("awardCategory") or []
        if not cats:
            dropped_unknown_category += 1
            continue
        cat_title = cats[0].get("title")
        cat_slug  = cats[0].get("slug")
        if cat_title not in CATEGORY_AMOUNTS:
            print(f"  [WARN] unknown category {cat_title!r} for {e.get('title')} — using amount=NULL")
        amount = CATEGORY_AMOUNTS.get(cat_title)
        scheme = CATEGORY_SCHEMES.get(cat_title) or cat_title

        year = e.get("awardYear")
        if year is None:
            continue
        year = int(year)

        # funder_award_id = welch-{category-slug}-{year}-{slug}
        slug = e.get("slug") or re.sub(r'[^a-z0-9]+', '-', (e.get('title') or '').lower()).strip('-')
        funder_award_id = f"welch-{cat_slug or 'unknown'}-{year}-{slug}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "(category, year, slug) should be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        full_name = e.get("title") or ""
        given, family = split_name(full_name)

        # Description: blurb (short citation) + full description (longer bio)
        blurb = _strip_html(e.get("recipientDescriptionBlurb"))
        full_desc = _strip_html(e.get("recipientDescription"))
        # Compose description; cap length for parquet
        if blurb and full_desc and blurb != full_desc:
            description = f"{blurb} {full_desc}"
        else:
            description = blurb or full_desc
        if description and len(description) > 1200:
            description = description[:1200].rsplit(" ", 1)[0] + "…"

        affiliation = _strip_html(e.get("recipientAffiliation"))
        display_name = f"{scheme} {year} — {full_name}"

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              year,
            "slug":              slug,
            "name":              full_name,
            "given_name":        given,
            "family_name":       family,
            "category_title":    cat_title,
            "category_slug":     cat_slug,
            "scheme":            scheme,
            "affiliation":       affiliation,
            "blurb":             blurb,
            "description":       description,
            "display_name":      display_name,
            "amount":            amount,
            "currency":          CURRENCY if amount is not None else None,
            "start_date":        f"{year}-01-01",
            "end_date":          f"{year}-12-31",
            "landing_page_url":  e.get("url"),
            "is_past_recipient": e.get("isAPastRecipient"),
            "declined":          False,
        })

    if dropped_unknown_category:
        print(f"  [WARN] dropped {dropped_unknown_category} entries with no awardCategory")

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  schemes:")
    print(df.groupby("scheme").size().to_string())
    print(f"  amount coverage: {df['amount'].notna().sum()}/{len(df)}")
    print(f"  affiliation coverage: {df['affiliation'].notna().sum()}/{len(df)} "
          f"({df['affiliation'].notna().sum()*100/len(df):.0f}%)")
    print(f"  description coverage: {df['description'].notna().sum()}/{len(df)} "
          f"({df['description'].notna().sum()*100/len(df):.0f}%)")

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
    parquet_path = output_dir / "welch_awards.parquet"
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
    prev_path = output_dir / "_prev_welch.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/welch"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse welch_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Welch Foundation → S3 Pipeline (PRIZE PATTERN, GraphQL via Nuxt SPA)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "welch_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_entries = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_entries)} cached entries")
    else:
        all_entries = fetch_recipients()
        raw_path = args.output_dir / "welch_raw.json"
        raw_path.write_text(json.dumps(all_entries, ensure_ascii=False, indent=2))
        print(f"\n  Cached {len(all_entries)} raw entries to {raw_path}")

    df = build_dataframe(all_entries)
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
    print(f"Next: notebooks/awards/CreateWelchAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
