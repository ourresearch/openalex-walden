#!/usr/bin/env python3
"""
Government of Ontario: Ontario Research Funding Summary to S3
=============================================================

The Ontario Ministry of Colleges and Universities publishes every project it
funds through its peer-reviewed research programmes as two XLSX files on the
Ontario open data catalogue (CKAN, Open Government Licence - Ontario):

  * ontario-research-funding-summary-historical  (Oct 2004 - June 2018)
  * ontario-research-funding-summary-current     (July 2018 - present)

One row per project: Program, Round, Project Number, Project Title, Project
Description, FOR codes, Lead Research Institution, Approval Date, Fiscal Year,
Ontario Commitment (CAD), Total Project Costs, lead researcher Salutation /
First / Middle / Last Name, Expenditure Type, Keywords. Method 0 (bulk file)
on the runbook ladder. Resource URLs are resolved through the CKAN
package_show API so a re-published file is picked up automatically.

Research filter (Kyle / coordinator, oxjob #1451): keep the research funding
programmes only --
  Ontario Research Fund - Research Infrastructure, Ontario Research Fund -
  Research Excellence, Early Researcher Awards, PDF (Post-Doctoral Fellowship
  Program), ISOP (International Strategic Opportunities Program), PDA
  (Premier's Discovery Awards), SRA.
Excluded: YSTOP (Youth Science and Technology Outreach), TSTOP (Teacher
Science and Technology Outreach) and PCA (Premier's Catalyst Awards, prizes to
companies for commercialised products).

funder_award_id = the ministry's Project Number, which is the reference
grantees cite: ER17-13-081 (ERA), RE09-077 (ORF-RE), 5-digit CFI project
numbers for ORF-RI (the ORF-RI match is awarded against the CFI project).

Output: s3://openalex-ingest/awards/ontario_research_funding/ontario_research_funding_projects.parquet
"""

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; the canonical call is sys.stdout.reconfigure(encoding="utf-8"),
#  done below through the renamed module alias.) See runbook §1.2.
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

CKAN = "https://data.ontario.ca/api/3/action/package_show"
PACKAGES = ["ontario-research-funding-summary-historical", "ontario-research-funding-summary-current"]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ontario_research_funding/ontario_research_funding_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}

KEEP_PROGRAMS = {
    "Ontario Research Fund - Research Infrastructure",
    "Ontario Research Fund - Research Excellence",
    "Early Researcher Awards",
    "PDF",    # Post-Doctoral Fellowship Program
    "ISOP",   # International Strategic Opportunities Program
    "PDA",    # Premier's Discovery Awards
    "SRA",
}
EXCLUDE_PROGRAMS = {"YSTOP", "TSTOP", "PCA"}  # outreach programmes; company product prizes

COLS = {  # normalised column name -> source header (headers differ slightly between files)
    "program": "program", "round": "round", "project number": "project_number",
    "project title": "title", "project description": "description",
    "field of research (for) - level 1 division code": "for1_code",
    "for - level 1 division title": "for1_title",
    "for - level 2 group code": "for2_code", "for - level 2 group title": "for2_title",
    "for - level 3 class code": "for3_code", "for - level 3 class title": "for3_title",
    "for - level 4 sub-class code": "for4_code", "for - level 4 sub-class title": "for4_title",
    "lead research institution": "institution", "institution type": "institution_type",
    "city": "city", "approval date": "approval_date", "fiscal year": "fiscal_year",
    "ontario commitment": "ontario_commitment", "total project costs": "total_project_costs",
    "salutation": "salutation", "first name": "first_name", "middle name": "middle_name",
    "last name": "last_name", "expenditure type": "expenditure_type", "language": "language",
    "keywords": "keywords",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def resource_url(package: str) -> str:
    r = requests.get(CKAN, params={"id": package}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    res = [x for x in r.json()["result"]["resources"]
           if x["format"].upper() == "XLSX" and x["url"].lower().endswith("_en.xlsx")]
    if len(res) != 1:
        raise SystemExit(f"{package}: expected 1 English XLSX resource, got {[x['url'] for x in res]}")
    return res[0]["url"]


def load(package: str, cache_dir: Path) -> pd.DataFrame:
    url = resource_url(package)
    path = cache_dir / url.rsplit("/", 1)[-1]
    if not path.exists():
        log(f"GET {url}")
        r = requests.get(url, headers=HEADERS, timeout=300)
        r.raise_for_status()
        path.write_bytes(r.content)
        log(f"  {len(r.content):,} bytes")
    xl = pd.ExcelFile(path)
    sheets = [s for s in xl.sheet_names if s.strip().lower() != "legend"]
    if len(sheets) != 1:
        raise SystemExit(f"{path.name}: expected 1 data sheet, got {sheets}")
    df = pd.read_excel(path, sheet_name=sheets[0], dtype=str)
    norm = {c: re.sub(r"\s+", " ", c.replace("2Group", "2 Group")).strip().lower() for c in df.columns}
    missing = set(COLS) - set(norm.values())
    if missing:
        raise SystemExit(f"{path.name}: missing columns {missing}")
    df = df.rename(columns={c: COLS[n] for c, n in norm.items() if n in COLS})[list(COLS.values())]
    df["source_file"] = path.name
    df["source_sheet"] = sheets[0]
    log(f"{package}: {len(df)} rows ({sheets[0]})")
    return df


def clean(s):
    if s is None or (isinstance(s, float) and pd.isna(s)) or pd.isna(s):
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def main() -> None:
    p = argparse.ArgumentParser(description="Ontario Research Funding Summary -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N rows (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.concat([load(pk, args.output_dir) for pk in PACKAGES], ignore_index=True)
    for c in df.columns:
        df[c] = df[c].map(clean)

    unknown = set(df["program"]) - KEEP_PROGRAMS - EXCLUDE_PROGRAMS
    if unknown:
        raise SystemExit(f"unknown programme(s), decide research/non-research first: {unknown}")
    log("programme counts:\n" + df["program"].value_counts().to_string())
    df = df[df["program"].isin(KEEP_PROGRAMS)].copy()
    log(f"after research filter: {len(df)} rows")

    df["funder_award_id"] = df["project_number"]
    df["approval_date"] = pd.to_datetime(df["approval_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["amount"] = pd.to_numeric(df["ontario_commitment"], errors="coerce")
    df["currency"] = "CAD"
    df["given_name"] = df.apply(
        lambda r: " ".join(x for x in [r["first_name"], r["middle_name"]] if x) or None, axis=1)
    df["family_name"] = df["last_name"]

    if args.limit:
        df = df.head(args.limit)

    if df["funder_award_id"].isna().any():
        raise SystemExit("rows with no Project Number")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")

    for c in ["title", "description", "approval_date", "amount", "family_name", "institution", "keywords"]:
        log(f"  {c:14s} {df[c].notna().mean():6.1%}")
    log(f"  total Ontario commitment CAD {df['amount'].sum():,.0f}")
    log(f"  approval years {df['approval_date'].str[:4].min()}-{df['approval_date'].str[:4].max()}")

    df = df.astype("string")
    parquet_path = args.output_dir / "ontario_research_funding_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_ontario_research_funding_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
