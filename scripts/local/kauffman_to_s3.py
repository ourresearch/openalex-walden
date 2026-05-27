#!/usr/bin/env python3
"""
Ewing Marion Kauffman Foundation to S3 Data Pipeline
=====================================================

Downloads the foundation's grant directory from kauffman.org's WordPress
WP REST API. The site exposes a `grant` custom post type at
`/wp-json/wp/v2/grant` with per-grant `meta` fields carrying the
approved USD amount, approval date, grantee city, and grantee web URL.
Three Kauffman-specific taxonomies — `states`, `strategies`, and
`grant-types` — round out the row.

Source authority
----------------
kauffman.org is the awarding body's own site. The WP REST endpoint is
public, no auth, paginated via `?per_page=100&page=N`. As of the
2026-05-27 probe: 337 total grants across 4 pages (X-WP-Total: 337,
X-WP-TotalPages: 4 at per_page=100).

Schema choices vs the project's standard funder schema
------------------------------------------------------
Kauffman funds organizations (e.g. universities, nonprofits, school
districts) directly, not individual researchers. Each row in the
source carries:
  - title         -> grantee organization name
  - content       -> grant description/purpose
  - meta.kauffman_grant_approved_amount  -> USD amount (populated)
  - meta.kauffman_grant_approved_on      -> approval date YYYY-MM-DD
  - meta.kauffman_grant_city             -> grantee city
  - meta.kauffman_grant_url              -> grantee website
  - states / strategies / grant-types    -> taxonomy term IDs

We model this with `lead_investigator` as an org-only record:
given_name / family_name / orcid all NULL, `affiliation.name` = the
grantee org, `affiliation.country` = 'US' (Kauffman's state taxonomy
is exclusively 2-letter US state codes). Same approach as the Hewlett
Foundation (priority 86) which also funds orgs, not PIs.

Provenance: `kauffman_foundation` (verified count=0 on production).

Amount and currency
-------------------
USD per the foundation's own approved-amount meta field; values vary
per grant (smoke-sample: $65K to $500K). No §6.7 waiver — amounts ship
populated.

Output
------
s3://openalex-ingest/awards/kauffman/kauffman_grants.parquet

Usage
-----
    python kauffman_to_s3.py                    # full run
    python kauffman_to_s3.py --skip-upload      # local dev
    python kauffman_to_s3.py --skip-download    # reuse cached JSON
    python kauffman_to_s3.py --limit 50         # smoke test
    python kauffman_to_s3.py --allow-shrink     # override §1.4

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

SITE_BASE = "https://www.kauffman.org"
GRANT_ENDPOINT      = f"{SITE_BASE}/wp-json/wp/v2/grant"
STATES_ENDPOINT     = f"{SITE_BASE}/wp-json/wp/v2/states"
STRATEGIES_ENDPOINT = f"{SITE_BASE}/wp-json/wp/v2/strategies"
GRANT_TYPES_ENDPOINT = f"{SITE_BASE}/wp-json/wp/v2/grant-types"

# Awarding body — Ewing Marion Kauffman Foundation.
# Verified F4320306140, country US, DOI 10.13039/100000865.
FUNDER_ID = 4320306140
FUNDER_DISPLAY_NAME = "Ewing Marion Kauffman Foundation"

PROVENANCE = "kauffman_foundation"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/kauffman/kauffman_grants.parquet"

USER_AGENT = "openalex-walden-kauffman-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.4
WP_PAGE_SIZE = 100


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
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return unescape(re.sub(r"\s+", " ", s)).strip()


# =============================================================================
# Taxonomy lookups
# =============================================================================

def fetch_taxonomy_map(endpoint: str) -> dict[int, str]:
    r = _http_get(f"{endpoint}?per_page=100")
    r.raise_for_status()
    terms = r.json()
    return {t["id"]: t["name"] for t in terms}


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: WP REST /grant + taxonomies")
    print("=" * 60)
    r = _http_get(f"{GRANT_ENDPOINT}?per_page=1")
    r.raise_for_status()
    total = r.headers.get("X-WP-Total")
    print(f"  X-WP-Total: {total} grants advertised")
    sample = r.json()
    if not sample:
        print("[ERROR] empty response from /grant endpoint")
        sys.exit(3)
    s = sample[0]
    title = s.get("title", {}).get("rendered", "")
    amount = s.get("meta", {}).get("kauffman_grant_approved_amount")
    approved_on = s.get("meta", {}).get("kauffman_grant_approved_on")
    print(f"  sample: {title} | amount=${amount} | approved={approved_on}")
    # Confirm taxonomies are reachable
    for name, ep in (("states", STATES_ENDPOINT), ("strategies", STRATEGIES_ENDPOINT), ("grant-types", GRANT_TYPES_ENDPOINT)):
        try:
            tax = fetch_taxonomy_map(ep)
            print(f"  {name}: {len(tax)} terms")
        except Exception as e:
            print(f"  {name}: ERROR {e}")
            sys.exit(3)


# =============================================================================
# Download
# =============================================================================

def download_grants(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print(f"Step 1: Walk {GRANT_ENDPOINT} (per_page={WP_PAGE_SIZE})")
    print("=" * 60)

    states_map      = fetch_taxonomy_map(STATES_ENDPOINT)
    strategies_map  = fetch_taxonomy_map(STRATEGIES_ENDPOINT)
    grant_types_map = fetch_taxonomy_map(GRANT_TYPES_ENDPOINT)
    print(f"  taxonomies: states={len(states_map)} strategies={len(strategies_map)} grant-types={len(grant_types_map)}")

    all_grants: list[dict] = []
    page = 1
    advertised_total_pages: Optional[int] = None
    MAX_PAGES_FALLBACK = 200  # safety net if X-WP-TotalPages header is missing
    while True:
        r = _http_get(f"{GRANT_ENDPOINT}?per_page={WP_PAGE_SIZE}&page={page}")
        if r.status_code == 400:
            # WP REST returns 400 with code "rest_post_invalid_page_number" once we walk past the last page.
            break
        r.raise_for_status()
        if advertised_total_pages is None:
            try:
                advertised_total_pages = int(r.headers.get("X-WP-TotalPages") or 0) or None
            except (TypeError, ValueError):
                advertised_total_pages = None
        chunk = r.json()
        if not chunk:
            # Empty page mid-walk is logged but does NOT terminate (how-to-add-a-funder-v2.md §1).
            # X-WP-TotalPages (read above) or the 400 branch above are the authoritative terminators.
            print(f"  page {page}: empty response; continuing (advertised_total_pages={advertised_total_pages})")
        for g in chunk:
            meta = g.get("meta", {}) or {}
            states_ids      = g.get("states", []) or []
            strategies_ids  = g.get("strategies", []) or []
            grant_type_ids  = g.get("grant-types", []) or []
            all_grants.append({
                "wp_id":            g.get("id"),
                "slug":             g.get("slug"),
                "title_raw":        g.get("title", {}).get("rendered", ""),
                "content_raw":      g.get("content", {}).get("rendered", ""),
                "date":             g.get("date"),
                "modified":         g.get("modified"),
                "link":             g.get("link"),
                "amount_usd":       meta.get("kauffman_grant_approved_amount"),
                "approved_on":      meta.get("kauffman_grant_approved_on"),
                "grantee_city":     meta.get("kauffman_grant_city"),
                "grantee_url":      meta.get("kauffman_grant_url"),
                "states_names":     [states_map.get(i) for i in states_ids if i in states_map],
                "strategies_names": [strategies_map.get(i) for i in strategies_ids if i in strategies_map],
                "grant_type_names": [grant_types_map.get(i) for i in grant_type_ids if i in grant_types_map],
            })
            if limit is not None and len(all_grants) >= limit:
                break
        total = r.headers.get("X-WP-Total")
        total_pages = r.headers.get("X-WP-TotalPages")
        print(f"  page {page} (X-WP-TotalPages={total_pages}): +{len(chunk)} grants (total {len(all_grants)} of {total} advertised)")
        if limit is not None and len(all_grants) >= limit:
            print(f"  [LIMIT] stopping after {limit} grants")
            break
        if total_pages and page >= int(total_pages):
            break
        if not total_pages and page >= MAX_PAGES_FALLBACK:
            print(f"  no X-WP-TotalPages header; stopping at MAX_PAGES_FALLBACK={MAX_PAGES_FALLBACK}")
            break
        page += 1

    raw_path = output_dir / "kauffman_raw.json"
    raw_path.write_text(json.dumps(all_grants, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_grants)} grants to {raw_path}")
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
        wp_id = r.get("wp_id")
        if wp_id is None:
            continue
        funder_award_id = f"kauffman-{wp_id}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)

        # Grantee org title can carry HTML entities (e.g. &amp;)
        grantee = unescape(_strip_tags(r.get("title_raw") or "")) or None
        description = unescape(_strip_tags(r.get("content_raw") or "")) or None

        approved_on = r.get("approved_on") or None
        start_year = None
        if approved_on and len(approved_on) >= 4 and approved_on[:4].isdigit():
            start_year = int(approved_on[:4])

        amount = r.get("amount_usd")
        # Source stores numeric amounts as int but pyarrow needs consistent type. Coerce to float; treat 0/"" as missing.
        try:
            amount_f = float(amount) if amount not in (None, "", 0, "0") else None
        except (TypeError, ValueError):
            amount_f = None

        # Multi-valued taxonomies -> first value wins for the canonical scheme;
        # full lists ship in dedicated columns so the notebook can do anything fancier.
        strategies = r.get("strategies_names") or []
        grant_types = r.get("grant_type_names") or []
        states = r.get("states_names") or []
        scheme = strategies[0] if strategies else (grant_types[0] if grant_types else None)

        rows.append({
            "funder_award_id":   funder_award_id,
            "wp_id":             wp_id,
            "slug":              r.get("slug"),
            "grantee_org":       grantee,
            "description":       description,
            "amount":            amount_f,
            "currency":          "USD" if amount_f is not None else None,
            "approved_on":       approved_on,
            "start_year":        start_year,
            "grantee_city":      r.get("grantee_city") or None,
            "grantee_state":     states[0] if states else None,
            "grantee_url":       r.get("grantee_url") or None,
            "strategy":          scheme,
            "strategies_all":    ", ".join(s for s in strategies if s) or None,
            "grant_type":        grant_types[0] if grant_types else None,
            "grant_types_all":   ", ".join(t for t in grant_types if t) or None,
            "landing_page_url":  r.get("link"),
        })

    df = pd.DataFrame.from_records(rows)
    n_amount = df["amount"].notna().sum()
    n_inst = df["grantee_org"].astype(bool).sum()
    n_desc = df["description"].astype(bool).sum()
    n_strategy = df["strategy"].astype(bool).sum()
    n_year = df["start_year"].notna().sum()
    n_state = df["grantee_state"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: grantee_org={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"description={n_desc} ({n_desc*100/len(df):.0f}%) "
          f"amount={n_amount} ({n_amount*100/len(df):.0f}%) "
          f"start_year={n_year} ({n_year*100/len(df):.0f}%) "
          f"state={n_state} ({n_state*100/len(df):.0f}%) "
          f"strategy={n_strategy} ({n_strategy*100/len(df):.0f}%)")
    if n_amount:
        print(f"  amount stats: min=${df['amount'].min():,.0f} median=${df['amount'].median():,.0f} max=${df['amount'].max():,.0f}")
    if n_year:
        years = df["start_year"].dropna().astype(int)
        print(f"  year range: {years.min()} – {years.max()}")
    print(f"\n  By strategy:")
    print(df.groupby("strategy", dropna=False).size().sort_values(ascending=False).head(10).to_string())
    print(f"\n  By grant_type:")
    print(df.groupby("grant_type", dropna=False).size().sort_values(ascending=False).head(10).to_string())
    print(f"\n  Top 10 grantee states:")
    print(df.groupby("grantee_state", dropna=False).size().sort_values(ascending=False).head(10).to_string())
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
    parquet_path = output_dir / "kauffman_grants.parquet"
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
    prev_path = output_dir / "_prev_kauffman_grants.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/kauffman"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse kauffman_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only fetch first N grants (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Ewing Marion Kauffman Foundation → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "kauffman_raw.json"
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
    print(f"Next: notebooks/awards/CreateKauffmanAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
