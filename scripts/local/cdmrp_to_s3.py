#!/usr/bin/env python3
"""
CDMRP (Congressionally Directed Medical Research Programs) to S3 Data Pipeline
=============================================================================

Downloads all CDMRP grant awards and uploads a parquet to S3 for Databricks
ingestion.

Source note:
    CDMRP's own awards database at https://cdmrp.health.mil/search.aspx was
    DECOMMISSIONED (notice dated 2025-09-24). All CDMRP award lookups now point
    to the Dimensions-for-DTIC instance (https://dtic.dimensions.ai), a public,
    anonymous Digital Science Dimensions instance hosting DoD grant awards.
    CDMRP is filterable there as a funder via its GRID id grid.496791.4.

Data Source: https://dtic.dimensions.ai/discover/grant/results.json?and_facet_funder=grid.496791.4
    - Clean anonymous JSON API (no cookie / CSRF / auth required).
    - ~23,713 CDMRP grants. Fixed page size 20; opaque `np=` cursor pagination.
    - GOTCHA: the np cursor does NOT preserve the funder facet -> re-append
      &and_facet_funder=grid.496791.4 to EVERY cursor URL or it leaks into
      other DoD funders.

Output: s3://openalex-ingest/awards/cdmrp/cdmrp_grants.parquet

Distinct from the DoD USAspending ingest: this is a dedicated, richer source
(real PI names, abstracts, program/mechanism) under its own provenance
'dimensions_cdmrp'; CreateAwards dedup (lower priority wins) lets the dedicated
rows override the fuzzier aggregator rows for the same grant.

Requirements:
    pip install pandas pyarrow requests
    AWS CLI configured with write access to s3://openalex-ingest/awards/cdmrp/

Usage:
    python cdmrp_to_s3.py
    python cdmrp_to_s3.py --skip-upload          # local validation only
    python cdmrp_to_s3.py --limit 200            # smoke test (first ~200)
    python cdmrp_to_s3.py --resume               # resume from checkpoint

Author: OpenAlex Team
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# --- UTF-8 compatibility shim (runbook §1.2) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://dtic.dimensions.ai"
GRID = "grid.496791.4"  # Congressionally Directed Medical Research Programs
FACET = f"and_facet_funder={GRID}"
RESULTS_URL = f"{BASE}/discover/grant/results.json?{FACET}"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cdmrp/cdmrp_grants.parquet"

MAX_CONSECUTIVE_EMPTY = 3      # never bail on first empty page (runbook §1)
MAX_CONSECUTIVE_NON200 = 5
REQUEST_DELAY = 0.3            # polite delay between cursor pages


def _loads(v):
    """project_numbers / funding_schemes arrive as JSON-encoded strings."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v or []


def extract(rec: dict) -> dict:
    """Map one Dimensions grant doc -> flat row."""
    award_id = proposal = None
    for pn in _loads(rec.get("project_numbers")):
        if isinstance(pn, dict):
            if pn.get("label") == "Award Number" and not award_id:
                award_id = pn.get("project_num")
            elif pn.get("label") == "Proposal Number" and not proposal:
                proposal = pn.get("project_num")
    program = mechanism = None
    for fs in _loads(rec.get("funding_schemes")):
        if isinstance(fs, dict):
            if fs.get("label") == "Funding Program":
                program = fs.get("funding_scheme")
            elif fs.get("label") == "Funding Mechanism":
                mechanism = fs.get("funding_scheme")

    pi_first = pi_last = pi_org = None
    researchers = rec.get("researcher_details") or []
    pi = next((r for r in researchers if r.get("role") == "PI"), None) or \
        (researchers[0] if researchers else None)
    if pi:
        pi_first = pi.get("first_name")
        pi_last = pi.get("last_name")
        affs = pi.get("affiliations") or []
        if affs and isinstance(affs[0], dict):
            pi_org = affs[0].get("name")

    org_name = org_city = org_country = org_grid = None
    orgs = rec.get("research_orgs_full") or []
    if orgs and isinstance(orgs[0], (list, tuple)) and orgs[0]:
        o = orgs[0]
        org_name = o[0] if len(o) > 0 else None
        org_city = o[1] if len(o) > 1 else None
        org_country = o[2] if len(o) > 2 else None
        org_grid = o[3] if len(o) > 3 else None

    gid = rec.get("id", "")
    return {
        "grant_id": gid,
        "funder_award_id": award_id or proposal or gid,
        "proposal_number": proposal,
        "title": rec.get("title"),
        "short_abstract": rec.get("short_abstract"),
        "pi_first_name": pi_first,
        "pi_last_name": pi_last,
        "pi_affiliation": pi_org or org_name,
        "institution": org_name,
        "institution_city": org_city,
        "institution_country": org_country,
        "ror_or_grid": org_grid,
        "amount": rec.get("funding_amount"),
        "currency": rec.get("funding_currency"),
        "program": program,
        "mechanism": mechanism,
        "start_date": rec.get("start_date"),
        "end_date": rec.get("end_date"),
        "program_officer": rec.get("program_officer"),
        "landing_page_url": f"{BASE}/details/grant/{gid}" if gid else None,
    }


def _paginate(session, query_facets, rows, seen, limit):
    """Paginate ONE Dimensions query via the np cursor, re-appending facets each page.

    The discover API caps a single query at 100 pages (2,000 records) -> page 101
    hard-400s. Callers must slice the corpus (by year) so each query stays <2,000.
    """
    url = f"{RESULTS_URL}&{query_facets}"
    empties = non200 = 0
    while True:
        try:
            r = session.get(url, headers={"Accept": "application/json"}, timeout=60)
        except requests.exceptions.RequestException as e:
            non200 += 1
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"too many request errors: {e}")
            time.sleep(2 ** min(non200, 6))
            continue
        if r.status_code == 204 or not (r.content or b"").strip():
            return  # empty slice (e.g. a year with no CDMRP grants) — not an error
        if r.status_code != 200:
            non200 += 1
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"too many non-200s (last {r.status_code}) for {query_facets}")
            time.sleep(2 ** min(non200, 6))
            continue
        non200 = 0
        d = r.json()
        docs = d.get("docs", [])
        new = [rec for rec in docs if rec.get("id") not in seen]
        if not new:
            empties += 1
            if empties >= MAX_CONSECUTIVE_EMPTY or not docs:
                return
        else:
            empties = 0
            for rec in new:
                seen.add(rec.get("id"))
                rows.append(extract(rec))
        if limit and len(rows) >= limit:
            return
        nxt = d.get("navigation", {}).get("results_json")
        if not nxt:
            return
        url = f"{BASE}{nxt}&{FACET}&{query_facets}"   # re-append BOTH facets to cursor
        time.sleep(REQUEST_DELAY)


def harvest(session: requests.Session, limit: int | None = None,
            years=range(1992, 2028)) -> list:
    """Harvest all CDMRP grants by slicing per facet_year (each year <2,000)."""
    rows, seen = [], set()
    for y in years:
        before = len(rows)
        _paginate(session, f"and_facet_year={y}", rows, seen, limit)
        got = len(rows) - before
        if got:
            print(f"  year {y}: +{got} grants ({len(rows)} total)")
        if limit and len(rows) >= limit:
            print(f"  reached --limit {limit}")
            return rows[:limit]
    return rows


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="Download CDMRP grants (Dimensions/DTIC) to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cdmrp_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="harvest only first N (smoke test)")
    ap.add_argument("--resume", action="store_true", help="reuse cached harvest json if present")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CDMRP Grants to S3 (Dimensions-for-DTIC, funder grid.496791.4)")
    print("=" * 60)

    cache = args.output_dir / "cdmrp_harvest.json"
    if args.resume and cache.exists():
        rows = json.loads(cache.read_text())
        print(f"Resumed {len(rows)} grants from {cache}")
    else:
        session = requests.Session()  # default UA works anonymously
        rows = harvest(session, limit=args.limit)
        cache.write_text(json.dumps(rows))
        print(f"Harvested {len(rows)} grants -> {cache}")

    if not rows:
        print("[ERROR] no grants harvested")
        sys.exit(1)

    df = pd.DataFrame(rows)
    # Stringify everything per runbook §1.2 (defensive typing downstream)
    df = df.astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")

    out = args.output_dir / "cdmrp_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e6:.1f} MB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual upload: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")
    print("Next: run notebooks/awards/CreateCDMRPAwards in Databricks")


if __name__ == "__main__":
    main()
