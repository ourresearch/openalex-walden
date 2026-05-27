#!/usr/bin/env python3
"""
Cottrell Scholars (Research Corporation for Science Advancement) to S3
=======================================================================

Downloads the Cottrell Scholar Award awardee directory from RCSA's
public awardee dashboard at rescorp.org. The dashboard page embeds the
full scholar roster as a JSON array inside the page HTML (the page
renders the array client-side as a DataTable). We parse the embedded
array directly — no DataTable AJAX endpoint to hit.

Source authority
----------------
rescorp.org is the awarding body's own site. The /cottrell-scholars/awardee-dashboard/
page surfaces three JSON arrays — the first two are sorted by last name
with multi-year scholars merged into one row (year="2005, 2012"); the
third is sorted by award year and splits multi-year scholars into
distinct rows ("2005" and "2012" as separate records). We use the
third array because its per-award granularity matches our schema
(one funder_award_id per (year, scholar) pair).

Amount and currency
-------------------
Verified from rescorp.org/cottrell-scholars/cottrell-scholar-award/:
"Each three-year award provides $120,000". USD 120,000 per scholar,
3-year term. `amount` and `currency` ship populated; `end_year` =
start_year + 2.

Country
-------
Derived from the dashboard's `organization_state` field. Canadian
provinces are flagged as 'CA' (Cottrell eligibility is U.S. + Canadian
research universities and PUIs per the program page; RCSA uses 'QB'
for Quebec in the dashboard, which we normalize). 'Foreign' org_type
ships with country=NULL.

Output
------
s3://openalex-ingest/awards/cottrell_scholars/cottrell_scholars.parquet

Usage
-----
    python cottrell_scholars_to_s3.py                    # full run
    python cottrell_scholars_to_s3.py --skip-upload      # local dev
    python cottrell_scholars_to_s3.py --skip-download    # reuse cached JSON
    python cottrell_scholars_to_s3.py --limit 25         # smoke test
    python cottrell_scholars_to_s3.py --allow-shrink     # override §1.4

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
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim ---
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

DASHBOARD_URL = "https://rescorp.org/cottrell-scholars/awardee-dashboard/"

# Awarding body — Research Corporation for Science Advancement (RCSA).
# Verified F4320306487, country US, DOI 10.13039/100001309.
FUNDER_ID = 4320306487
FUNDER_DISPLAY_NAME = "Research Corporation for Science Advancement"

PROVENANCE = "cottrell_scholars"

# Official program parameters, verified from the cottrell-scholar-award/ page:
# "Each three-year award provides $120,000".
AMOUNT_USD       = 120000.0
DURATION_YEARS   = 3

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/cottrell_scholars/cottrell_scholars.parquet"

USER_AGENT = "openalex-walden-cottrell-ingest/1.0 (+https://openalex.org)"

# Polite throttle (single page fetch — minimal impact).
MIN_REQUEST_INTERVAL_S = 0.5

# Canadian province codes seen on the dashboard. RCSA uses 'QB' for
# Quebec (not the standard 'QC') — keep both in case they ever fix it.
CANADIAN_PROVINCES = {
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE",
    "QC", "QB", "SK", "YT",
}


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


# =============================================================================
# Extract canonical scholar JSON from the dashboard page
# =============================================================================

def extract_scholars(html: str) -> list[dict]:
    """Return the canonical scholar list — the per-(year,name) array, which
    splits multi-year scholars into distinct rows. RCSA emits three
    arrays on the dashboard: the first two are sorted by last name with
    multi-year scholars merged into one row (year="2005, 2012") and the
    third is sorted by award year with each award as its own row. We
    want the third for clean per-award granularity."""
    starts = [m.start() for m in re.finditer(r'\[\{"first_name"', html)]
    if not starts:
        raise RuntimeError("No embedded scholar JSON array found on the page.")
    # Pick the array with the most distinct (full_name, year) rows.
    # That array is the one where multi-year scholars are split into
    # separate records.
    best: list[dict] = []
    for st in starts:
        try:
            d, _ = json.JSONDecoder().raw_decode(html[st:])
        except Exception:
            continue
        if any("," in str(r.get("year", "")) for r in d):
            # Merged-year array; skip.
            continue
        if len(d) > len(best):
            best = d
    if not best:
        # Fallback: take the first array and explode the comma-year rows.
        d, _ = json.JSONDecoder().raw_decode(html[starts[0]:])
        exploded: list[dict] = []
        for r in d:
            years = [y.strip() for y in str(r.get("year", "")).split(",") if y.strip()]
            for y in years:
                r2 = dict(r)
                r2["year"] = y
                exploded.append(r2)
        return exploded
    return best


def country_from_state(state: Optional[str], org_type: Optional[str]) -> Optional[str]:
    """Cottrell eligibility is US + Canadian academic institutions."""
    if not state:
        return None
    s = state.upper().strip()
    if s in CANADIAN_PROVINCES:
        return "CA"
    if org_type == "Foreign":
        # Honor the dashboard's "Foreign" tag — don't claim US for non-US records.
        return None
    # Otherwise assume US (50 states + DC + territories).
    return "US"


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: dashboard page reachable + JSON parses")
    print("=" * 60)
    r = _http_get(DASHBOARD_URL)
    r.raise_for_status()
    rows = extract_scholars(r.text)
    print(f"  scholars in canonical array: {len(rows)}")
    if len(rows) < 400:
        print(f"  [WARN] only {len(rows)} scholars; expected 600+")
    yrs = sorted({int(d["year"]) for d in rows if d.get("year") and str(d["year"]).isdigit()})
    print(f"  year range: {yrs[0]} - {yrs[-1]} ({len(yrs)} distinct years)")
    if rows:
        s = rows[0]
        c = country_from_state(s.get("organization_state"), s.get("organization_type"))
        print(f"  sample: {s.get('full_name')} | {s.get('year')} | {s.get('organization_name')} ({c}) | {s.get('discipline')}")


# =============================================================================
# Download
# =============================================================================

def download_scholars(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Fetch dashboard page + extract scholar JSON")
    print("=" * 60)
    r = _http_get(DASHBOARD_URL)
    r.raise_for_status()
    rows = extract_scholars(r.text)
    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] only retaining first {limit} scholars")
    print(f"  scholars: {len(rows)}")
    raw_path = output_dir / "cottrell_raw.json"
    raw_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    print(f"  cached to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

def _slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        first = (r.get("first_name") or "").strip()
        last  = (r.get("last_name")  or "").strip()
        if not first and not last:
            continue
        name = f"{first} {last}".strip()
        year_raw = str(r.get("year") or "").strip()
        if not year_raw or not year_raw.isdigit():
            print(f"  [WARN] skipping row with non-numeric year: {r}")
            continue
        year = int(year_raw)
        slug = _slugify(f"{last}-{first}")
        funder_award_id = f"cottrell-{year}-{slug}"
        if funder_award_id in seen_ids:
            print(f"  [WARN] duplicate funder_award_id {funder_award_id!r}; skipping")
            continue
        seen_ids.add(funder_award_id)
        country = country_from_state(r.get("organization_state"), r.get("organization_type"))
        rows.append({
            "funder_award_id":   funder_award_id,
            "slug":              slug,
            "year":              year,
            "name":              name,
            "given_name":        first or None,
            "family_name":       last or None,
            "institution":       r.get("organization_name") or None,
            "institution_type":  r.get("organization_type") or None,
            "institution_state": r.get("organization_state") or None,
            "country":           country,
            "discipline":        r.get("discipline") or None,
            "amount":            AMOUNT_USD,
            "currency":          "USD",
            "duration_years":    DURATION_YEARS,
            "landing_page_url":  DASHBOARD_URL,
        })
    df = pd.DataFrame.from_records(rows)
    n_inst = df["institution"].astype(bool).sum()
    n_disc = df["discipline"].astype(bool).sum()
    n_country_us = (df["country"] == "US").sum()
    n_country_ca = (df["country"] == "CA").sum()
    n_country_null = df["country"].isna().sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"discipline={n_disc} ({n_disc*100/len(df):.0f}%)")
    print(f"  country distribution: US={n_country_us}, CA={n_country_ca}, NULL={n_country_null}")
    print(f"  year range: {df['year'].min()} – {df['year'].max()}")
    print(f"  cohorts: {df['year'].nunique()}")
    print(f"\n  by discipline:")
    print(df.groupby("discipline").size().to_string())
    print(f"\n  by institution_type:")
    print(df.groupby("institution_type").size().to_string())
    # Runbook §1.2.5 — astype("string") immediately before parquet write.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "cottrell_scholars.parquet"
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
    prev_path = output_dir / "_prev_cottrell_scholars.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/cottrell"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse cottrell_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only retain first N scholars (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Cottrell Scholars (RCSA) → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "cottrell_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_scholars(args.output_dir, args.limit)

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
    print(f"Next: notebooks/awards/CreateCottrellScholarsAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
