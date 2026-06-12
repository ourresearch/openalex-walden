#!/usr/bin/env python3
"""
Shriners Hospitals for Children to S3 Data Pipeline
===================================================

Scrapes Shriners' currently-funded research grants from their public
ProposalCentral "Insights" portal and uploads a parquet to S3 for Databricks
ingestion.

Data source: proposalcentral.com Insights public award search (GMID=83). The
    portal is an ASP.NET MVC app with a server-side DataTables endpoint. Flow:
      1. GET  /Insights/tW9nmA12StM=/Public/Index  -> anti-forgery token + cookie
      2. POST that token + a blank search (GMID=83, SearchTextOption=false) back
         to /Public/Index  -> stores the search in session
      3. POST DataTables params to /Public/GetAwardSearchData
         (iDisplayLength=200) -> JSON with iTotalRecords + aaData rows
    Each row carries grantID (native award id), projectTitle, PI first/last,
    ORCID, awardAmount (USD), start/end dates, institution + city/state, and
    programName. ~93 currently-funded grants. Default UA fine; no Cloudflare.
    (Shriners is on the IAMHRF expanded list; track at organization level.)

Output: s3://openalex-ingest/awards/shriners/shriners_grants.parquet
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
PORTAL = "tW9nmA12StM="
BASE = f"https://proposalcentral.com/Insights/{PORTAL.replace('=', '%3D')}/Public"
REF = f"https://proposalcentral.com/Insights/{PORTAL}/Public/Index"
GMID = "83"  # Shriners grant maker id on ProposalCentral
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/shriners/shriners_grants.parquet"

TOKEN_RE = re.compile(
    r'__RequestVerificationToken"\s+type="hidden"\s+value="([^"]*)"')
ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


def fetch_awards(session):
    """Return the list of award dicts from GetAwardSearchData."""
    # 1. GET index -> token (+ session cookie via the jar)
    r = session.get(REF, timeout=40)
    r.raise_for_status()
    m = TOKEN_RE.search(r.text)
    if not m:
        print("[ERROR] anti-forgery token not found on index page")
        sys.exit(1)
    token = m.group(1)

    # 2. POST a blank search so the server stores it in session. The
    #    SearchTextOption radio MUST be a valid bool value (false/true) or
    #    model validation fails and the form is re-rendered with no search.
    blank = {
        "__RequestVerificationToken": token,
        "GMID": GMID,
        "AwardSearch.SearchTextOption": "false",
        "AwardSearch.SearchText": "",
        "AwardSearch.SearchName": "",
        "AwardSearch.ProjectTitle": "",
        "AwardSearch.ProgramID": "",
        "AwardSearch.Institution": "",
        "AwardSearch.City": "",
        "AwardSearch.StateAbbr": "",
        "AwardSearch.CountryName": "",
        "AwardSearch.AwardStatus": "",
        "AwardSearch.AwardStartFromDate": "",
        "AwardSearch.AwardStartToDate": "",
        "AwardSearch.AwardEndFromDate": "",
        "AwardSearch.AwardEndToDate": "",
    }
    session.post(f"{BASE}/Index", data=blank,
                 headers={"Referer": REF}, timeout=40)

    # 3. POST DataTables params -> JSON
    data = {
        "__RequestVerificationToken": token,
        "sEcho": "2", "iColumns": "8",
        "iDisplayStart": "0", "iDisplayLength": "200", "GMID": GMID,
    }
    rd = session.post(f"{BASE}/GetAwardSearchData", data=data, headers={
        "X-Requested-With": "XMLHttpRequest", "Referer": REF,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }, timeout=60)
    rd.raise_for_status()
    payload = rd.json()
    return payload.get("aaData", []), payload.get("iTotalRecords", 0)


def to_row(a):
    orcid = (a.get("orCiD") or "").strip()
    if not ORCID_RE.match(orcid):
        orcid = None
    amount = a.get("awardAmount")
    try:
        amount = str(int(round(float(amount)))) if amount not in (None, "") else None
    except (ValueError, TypeError):
        amount = None
    proposal_id = a.get("awardProposalID")
    inst = (a.get("primaryInstitution") or "").strip() or None
    return {
        "funder_award_id": (a.get("grantID") or "").strip() or None,
        "title": (a.get("projectTitle") or "").strip() or None,
        "description": None,
        "pi_given": (a.get("piFirstName") or "").strip() or None,
        "pi_family": (a.get("piLastName") or "").strip() or None,
        "orcid": orcid,
        "institution": inst,
        "city": (a.get("primaryCity") or "").strip() or None,
        "state": (a.get("primaryState") or "").strip() or None,
        "amount": amount,
        "start_date_raw": (a.get("awardStartDate") or "").strip() or None,
        "end_date_raw": (a.get("awardEndDate") or "").strip() or None,
        "program": (a.get("programName") or "").strip() or None,
        "landing_page_url": (
            f"https://proposalcentral.com/Insights/{PORTAL}/Public/"
            f"AwardDetails/{proposal_id}" if proposal_id else None),
    }


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
    ap = argparse.ArgumentParser(description="Shriners grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/shriners_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Shriners Hospitals for Children grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    awards, total = fetch_awards(s)
    print(f"GetAwardSearchData: iTotalRecords={total}, rows={len(awards)}")
    if len(awards) < 50:
        print(f"[ERROR] only {len(awards)} rows — search/session flow changed?")
        sys.exit(1)

    rows, seen = [], set()
    for a in awards:
        rec = to_row(a)
        aid = rec["funder_award_id"]
        if rec["title"] and aid and aid not in seen:
            seen.add(aid)
            rows.append(rec)
        time.sleep(0)  # single-shot API, no per-row fetch

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "orcid", "start_date_raw"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    out = args.output_dir / "shriners_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
