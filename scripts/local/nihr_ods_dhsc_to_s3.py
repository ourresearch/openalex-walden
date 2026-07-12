#!/usr/bin/env python3
"""
DHSC (UK Department of Health and Social Care) via NIHR Open Data -> S3
=======================================================================

Harvests the FULL NIHR funded portfolio from NIHR's Opendatasoft portal
(https://nihr.opendatasoft.com) and splits it into:

  1. SHIP set (this funder's parquet): DHSC-direct programme awards that are
     NOT already covered by the existing first-party NIHR ingest
     (provenance='nihr', priority 13, built from the same portal).
     DHSC-direct = Policy Research Programme rows + NIHR (ODA) global-health
     rows from `nihr-summary-view`, plus the DHSC Policy Research Unit (PRU)
     sub-project records from the dedicated `prp_dataset`.
  2. STAGING set (side parquet, NOT read by any notebook): every other row of
     the refreshed portfolio + the DHSC-direct rows that ARE already covered
     as NIHR awards. This is decision input for the coordinator on whether to
     refresh/extend the old (truncated) 9,999-row NIHR ingest.

Why the old NIHR ingest is truncated: nihr_to_s3.py paginates the ODS
Explore records API with offset+limit, which Opendatasoft hard-caps at
offset+limit <= 10,000. This script instead uses the ODS *export* endpoint
(/api/explore/v2.1/catalog/datasets/{id}/exports/json), which streams the
whole dataset with no cap (11,502 rows on 2026-07-12 vs 9,999 harvested by
the old script).

Overlap detection: funder_award_id shapes are compared against the live
OpenAlex awards API (funder.id:F4320319990 = NIHR). Awards with a non-null
display_name under that funder are the first-party priority-13 ingest rows
(Crossref/DataCite shell awards have display_name NULL); portal project_ids
found in that set are "already covered".

Data sources
------------
  https://nihr.opendatasoft.com/explore/dataset/nihr-summary-view/  (11,502)
  https://nihr.opendatasoft.com/explore/dataset/prp_dataset/        (509)
  https://api.openalex.org/awards?filter=funder.id:F4320319990      (overlap)

Output
------
  s3://openalex-ingest/awards/nihr_ods_dhsc/nihr_ods_dhsc_projects.parquet
  s3://openalex-ingest/awards/nihr_ods_dhsc/staging/nihr_full_portfolio_staging.parquet

Funder (SHIP set): Department of Health and Social Care, funder_id 4320319994
Currency: GBP (implicit; single-country funder; portal column is
`award_amount_from_dh` = "award amount from Department of Health").
The `prp_dataset` PRU records publish NO amounts (no amount field exists in
that dataset) -> those rows ship with amount NULL under a documented Step 6.7
waiver; the summary-view subset has ~99% amount coverage.

Usage
-----
    python nihr_ods_dhsc_to_s3.py                     # full run + upload
    python nihr_ods_dhsc_to_s3.py --limit 50          # smoke test
    python nihr_ods_dhsc_to_s3.py --skip-upload       # build parquet only
    python nihr_ods_dhsc_to_s3.py --allow-shrink      # override §1.4 guard

Requirements
------------
    pip install pandas pyarrow requests boto3
    AWS creds (repo .env is auto-loaded if AWS_ACCESS_KEY_ID unset)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
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
    def _open(file, mode="r", buffering=-1, encoding=None, errors=None,
              newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open
# --- end shim ---

ODS_BASE = "https://nihr.opendatasoft.com/api/explore/v2.1/catalog/datasets"
SUMMARY_DATASET = "nihr-summary-view"
PRP_DATASET = "prp_dataset"
NIHR_FUNDER_OPENALEX = "F4320319990"  # existing first-party NIHR ingest funder
OPENALEX_AWARDS = "https://api.openalex.org/awards"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nihr_ods_dhsc/nihr_ods_dhsc_projects.parquet"
S3_STAGING_KEY = "awards/nihr_ods_dhsc/staging/nihr_full_portfolio_staging.parquet"

USER_AGENT = "OpenAlex awards ingest (mailto:kyle@ourresearch.org)"

# Columns shipped (source fidelity — portal field names kept verbatim)
SHIP_COLUMNS = [
    "source_dataset", "project_id", "project_title", "funding_and_awards_link",
    "funder", "project_status", "programme", "programme_type",
    "programme_stream", "funding_stream", "award_amount_from_dh",
    "start_date", "end_date", "plain_english_abstract", "scientific_abstract",
    "organisation_type", "contracted_organisation", "postcode",
    "award_holder_name", "lead_given_name", "lead_family_name", "orcid",
    "involvement_type", "prp_portfolio", "prp_theme", "prp_keywords",
]


def load_repo_dotenv() -> None:
    """Populate AWS/OpenAlex creds from openalex-walden/.env when unset."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


# ---------------------------------------------------------------------------
# Name parsing (canonical split_name from wolf_to_s3.py, runbook §2.4.1,
# plus a documented leading-honorific strip for NIHR's "Professor X" style
# and UK post-nominal honours which NIHR appends after the family name).
# ---------------------------------------------------------------------------
LEADING_TITLES = {
    "professor", "prof", "prof.", "dr", "dr.", "mr", "mr.", "mrs", "mrs.",
    "ms", "ms.", "miss", "sir", "dame", "lord", "lady", "rev", "rev.",
}
UK_HONOURS = {"cbe", "obe", "mbe", "kbe", "dbe", "frs", "freng", "fmedsci", "frcp", "frcs"}


def split_name(name: str):
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').

    Strips trailing degree/suffix tokens (PhD, MD, Jr., Sr., II, III) before
    splitting. Last whitespace-separated token = family name; rest = given.
    Extended for NIHR: leading honorifics (Professor/Dr/...) and trailing UK
    honours (CBE/OBE/...) are stripped first.
    """
    if not name:
        return None, None
    tokens = name.split()
    while tokens and tokens[0].lower().strip(",.") in LEADING_TITLES:
        tokens.pop(0)
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in (suffixes | UK_HONOURS):
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# ---------------------------------------------------------------------------
# Downloads
# ---------------------------------------------------------------------------
def fetch_ods_export(dataset: str, out_path: Path, session: requests.Session) -> list:
    """Download a full ODS dataset via the export endpoint (no 10k cap)."""
    if out_path.exists():
        print(f"  [CACHE] {out_path.name} exists; reusing (delete to refetch)")
        return json.loads(out_path.read_text())
    url = f"{ODS_BASE}/{dataset}/exports/json"
    print(f"  GET {url}")
    t0 = time.time()
    r = session.get(url, timeout=300)
    r.raise_for_status()
    rows = r.json()
    out_path.write_text(json.dumps(rows, ensure_ascii=False))
    print(f"  [OK] {dataset}: {len(rows):,} rows in {time.time()-t0:.1f}s")
    return rows


def fetch_existing_nihr_award_ids(cache_path: Path, session: requests.Session) -> tuple:
    """All funder_award_ids currently on OpenAlex funder F4320319990.

    Returns (first_party_ids, all_ids). First-party = display_name non-null
    (the priority-13 'nihr' ingest shipped titles; Crossref/DataCite shells
    did not).
    """
    if cache_path.exists():
        print(f"  [CACHE] {cache_path.name} exists; reusing (delete to refetch)")
        recs = json.loads(cache_path.read_text())
    else:
        api_key = os.environ.get("OPENALEX_API_KEY", "")
        recs, cursor, page = [], "*", 0
        consecutive_err = 0
        while cursor:
            url = (f"{OPENALEX_AWARDS}?filter=funder.id:{NIHR_FUNDER_OPENALEX}"
                   f"&select=funder_award_id,display_name&per-page=200&cursor={cursor}")
            if api_key:
                url += f"&api_key={api_key}"
            resp = session.get(url, timeout=60)
            if resp.status_code != 200:
                consecutive_err += 1
                print(f"  page {page}: HTTP {resp.status_code} ({consecutive_err}/5); retrying")
                if consecutive_err >= 5:
                    raise RuntimeError("OpenAlex awards API failing repeatedly; aborting")
                time.sleep(3 * consecutive_err)
                continue
            consecutive_err = 0
            d = resp.json()
            recs.extend(d["results"])
            cursor = d["meta"].get("next_cursor")
            page += 1
            if page % 20 == 0:
                print(f"  [{page} pages] {len(recs):,} existing NIHR awards fetched")
            time.sleep(0.15)
        cache_path.write_text(json.dumps(recs, ensure_ascii=False))
        print(f"  [OK] {len(recs):,} existing NIHR award ids fetched from OpenAlex")
    first_party = {r["funder_award_id"] for r in recs if r.get("display_name")}
    all_ids = {r["funder_award_id"] for r in recs}
    print(f"  first-party (display_name non-null): {len(first_party):,} | total: {len(all_ids):,}")
    return first_party, all_ids


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def norm_date(v: Optional[str]) -> Optional[str]:
    """Return YYYY-MM-DD or None. Accepts YYYY-MM-DD or DD/MM/YYYY."""
    if not v or v in ("Not Available", "N/A"):
        return None
    v = str(v).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", v):
        return v[:10]
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    # Placeholder strings (case-insensitive): 'Not Available', 'Not available', 'N/A'
    return v if v and v.lower() not in ("not available", "n/a", "none", "tbc") else None


def summary_row_to_record(r: dict) -> dict:
    given, family = split_name(clean(r.get("award_holder_name")) or "")
    return {
        "source_dataset": SUMMARY_DATASET,
        "project_id": clean(r.get("project_id")),
        "project_title": clean(r.get("project_title")),
        "funding_and_awards_link": clean(r.get("funding_and_awards_link")),
        "funder": clean(r.get("funder")),
        "project_status": clean(r.get("project_status")),
        "programme": clean(r.get("programme")),
        "programme_type": clean(r.get("programme_type")),
        "programme_stream": clean(r.get("programme_stream")),
        "funding_stream": clean(r.get("funding_stream")),
        "award_amount_from_dh": (str(r["award_amount_from_dh"])
                                 if r.get("award_amount_from_dh") is not None else None),
        "start_date": norm_date(r.get("start_date")),
        "end_date": norm_date(r.get("end_date")),
        "plain_english_abstract": clean(r.get("plain_english_abstract")),
        "scientific_abstract": clean(r.get("scientific_abstract")),
        "organisation_type": clean(r.get("organisation_type")),
        "contracted_organisation": clean(r.get("contracted_organisation")),
        "postcode": clean(r.get("postcode")),
        "award_holder_name": clean(r.get("award_holder_name")),
        "lead_given_name": given,
        "lead_family_name": family,
        "orcid": clean(r.get("orcid")),
        "involvement_type": clean(r.get("involvement_type")),
        "prp_portfolio": None,
        "prp_theme": None,
        "prp_keywords": None,
    }


def prp_row_to_record(r: dict) -> dict:
    given, family = split_name(clean(r.get("principle_investigator")) or "")
    kw = r.get("keywords")
    if isinstance(kw, list):
        kw = "|".join(str(x) for x in kw)
    return {
        "source_dataset": PRP_DATASET,
        "project_id": clean(r.get("project_reference")),
        "project_title": clean(r.get("title")),
        "funding_and_awards_link": None,
        "funder": "NIHR (non-ODA)",
        "project_status": None,
        "programme": "Policy Research Programme",
        "programme_type": "Research",
        "programme_stream": None,
        "funding_stream": "Policy Research Programme",
        "award_amount_from_dh": None,  # prp_dataset publishes no amounts
        "start_date": norm_date(r.get("start_date")),
        "end_date": norm_date(r.get("end_date")),
        "plain_english_abstract": clean(r.get("summary")),
        "scientific_abstract": None,
        "organisation_type": None,
        "contracted_organisation": clean(r.get("lead_organisation")),
        "postcode": clean(r.get("postcode")),
        "award_holder_name": clean(r.get("principle_investigator")),
        "lead_given_name": given,
        "lead_family_name": family,
        "orcid": None,
        "involvement_type": "Chief Investigator",
        "prp_portfolio": clean(r.get("portfolio")),
        "prp_theme": clean(r.get("theme")),
        "prp_keywords": clean(kw),
    }


def is_dhsc_direct(r: dict) -> bool:
    """DHSC-named-funder programmes within the NIHR portal.

    - Policy Research Programme: commissioned directly by DHSC.
    - NIHR (ODA) rows: Global Health Research funded from DHSC's UK-aid
      (ODA) allocation.
    """
    return (r.get("programme") == "Policy Research Programme"
            or r.get("funder") == "NIHR (ODA)")


# ---------------------------------------------------------------------------
# §1.4 shrink check + upload
# ---------------------------------------------------------------------------
def check_no_shrink(new_count: int, key: str, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 required for §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{key}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / ("_prev_" + Path(key).name)
    try:
        client.download_file(S3_BUCKET, key, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    print(f"    previous: {prev_count}   new: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink ({prev_count} -> {new_count}).")
        return False
    print("    [OK] not smaller; safe to overwrite.")
    return True


def upload(parquet_path: Path, key: str, n_rows: int, allow_shrink: bool,
           output_dir: Path) -> bool:
    if not check_no_shrink(n_rows, key, allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{key}"
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="DHSC (via NIHR ODS portal) -> S3")
    parser.add_argument("--output-dir", default=str(Path(__file__).parent / "nihr_ods_dhsc_data"),
                        help="Directory for downloads + parquet output")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: truncate each source dataset to N rows")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink guard")
    args = parser.parse_args()

    load_repo_dotenv()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    print("=" * 60)
    print("Step 1: Download full NIHR portfolio (ODS export endpoints)")
    print("=" * 60)
    summary = fetch_ods_export(SUMMARY_DATASET, output_dir / "nihr_summary_view.json", session)
    prp = fetch_ods_export(PRP_DATASET, output_dir / "prp_dataset.json", session)
    if args.limit:
        summary = summary[: args.limit]
        prp = prp[: args.limit]
        print(f"  [LIMIT] truncated to {len(summary)} summary rows, {len(prp)} PRP rows")

    print("\n" + "=" * 60)
    print("Step 2: Fetch existing NIHR awards from OpenAlex (overlap set)")
    print("=" * 60)
    first_party_ids, all_ids = fetch_existing_nihr_award_ids(
        output_dir / "openalex_nihr_existing.json", session)

    print("\n" + "=" * 60)
    print("Step 3: Split portfolio into DHSC ship set vs NIHR staging set")
    print("=" * 60)
    sv_ids = {r.get("project_id") for r in summary}

    ship_records, staging_records = [], []
    dhsc_covered = dhsc_new = 0
    for r in summary:
        rec = summary_row_to_record(r)
        covered = rec["project_id"] in first_party_ids
        rec["already_in_openalex_nihr"] = str(covered)
        if is_dhsc_direct(r):
            rec["dhsc_direct"] = "True"
            if covered:
                dhsc_covered += 1
                staging_records.append(rec)     # overlap: coordinator decides
            else:
                dhsc_new += 1
                ship_records.append(rec)
        else:
            rec["dhsc_direct"] = "False"
            staging_records.append(rec)

    # PRU sub-projects from prp_dataset: keep refs not present in summary view,
    # dedupe on project_reference (some refs repeat; NIHR200701 repeats 42x
    # because sub-projects carry the parent contract ref — those collapse).
    seen = set()
    prp_added = 0
    for r in prp:
        ref = clean(r.get("project_reference"))
        if not ref or ref in sv_ids or ref in seen or ref in first_party_ids:
            continue
        seen.add(ref)
        rec = prp_row_to_record(r)
        rec["already_in_openalex_nihr"] = "False"
        rec["dhsc_direct"] = "True"
        ship_records.append(rec)
        prp_added += 1

    print(f"  portal rows: {len(summary):,} summary + {len(prp):,} prp_dataset")
    print(f"  DHSC-direct in summary view: {dhsc_covered + dhsc_new:,} "
          f"(covered by old NIHR ingest: {dhsc_covered:,} -> staging; new: {dhsc_new:,} -> ship)")
    print(f"  PRU sub-projects added from prp_dataset: {prp_added:,} -> ship")
    print(f"  SHIP:    {len(ship_records):,} rows")
    print(f"  STAGING: {len(staging_records):,} rows")

    # funder_award_id collision check — MUST raise, not warn (runbook)
    ship_ids = [r["project_id"] for r in ship_records]
    if len(ship_ids) != len(set(ship_ids)):
        dupes = sorted({i for i in ship_ids if ship_ids.count(i) > 1})
        raise RuntimeError(f"funder_award_id collision in ship set: {dupes[:10]}")

    print("\n" + "=" * 60)
    print("Step 4: Write parquet (all columns forced to string dtype)")
    print("=" * 60)
    cols = SHIP_COLUMNS + ["already_in_openalex_nihr", "dhsc_direct"]
    df_ship = pd.DataFrame(ship_records, columns=cols).astype("string")
    df_staging = pd.DataFrame(staging_records, columns=cols).astype("string")
    ship_path = output_dir / "nihr_ods_dhsc_projects.parquet"
    staging_path = output_dir / "nihr_full_portfolio_staging.parquet"
    df_ship.to_parquet(ship_path, index=False)
    df_staging.to_parquet(staging_path, index=False)
    amt_cov = df_ship["award_amount_from_dh"].notna().mean() * 100
    sv_ship = df_ship[df_ship["source_dataset"] == SUMMARY_DATASET]
    sv_cov = sv_ship["award_amount_from_dh"].notna().mean() * 100 if len(sv_ship) else 0.0
    print(f"  ship:    {len(df_ship):,} rows -> {ship_path.name} "
          f"(amount coverage {amt_cov:.1f}% overall; {sv_cov:.1f}% on summary-view subset)")
    print(f"  staging: {len(df_staging):,} rows -> {staging_path.name}")

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading.")
        return
    if args.limit:
        print("\n[SKIP] --limit set; refusing to upload a truncated corpus.")
        return

    print("\n" + "=" * 60)
    print("Step 5: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    ok = upload(ship_path, S3_KEY, len(df_ship), args.allow_shrink, output_dir)
    ok2 = upload(staging_path, S3_STAGING_KEY, len(df_staging), args.allow_shrink, output_dir)
    if not (ok and ok2):
        sys.exit(1)
    print(f"\nDONE {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
