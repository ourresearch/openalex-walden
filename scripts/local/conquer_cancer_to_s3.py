#!/usr/bin/env python3
"""
Conquer Cancer, the ASCO Foundation (Conquer Cancer Foundation) to S3 Data Pipeline
===================================================================================

Conquer Cancer's Grants & Awards programme publishes every recipient since 1984 in the
"View All Recipients" database (https://www.asco.org/career-development/grants-awards/
grant-award-recipients, linked from https://www.conquer.org/cancer-research/grants-awards).
That Angular page loads ONE static JSON file, which is the whole database:

    https://www.asco.org/assets/contents/grant-award-recipients.json

Each record: Year, First Name, Last Name, Degree(s), Institution, Title, Grant Amount ($),
Subject Area, Program, Project ID. Ladder step 0 (the funder's own bulk file): one GET,
no pagination. asco.org robots.txt is "User-agent: * / Allow: /".

funder_award_id = Project ID, the citable grant number (runbook 2.1.1). Recent grants use
"{year}{programme}-{10 digits}" (e.g. 2021YIA-6088880692), which is exactly what citing
works put in their Crossref funding metadata; older grants have short numeric ids
(e.g. 12822), also cited. 3 merit-award records have no Project ID; they get a synthetic
"CCF-{programme code}{year}-{family-name slug}" key.

Scope: every programme in the database is kept (research grants, career development
awards, fellowships, training programmes, and the meeting Merit / abstract / travel
awards and Clinical Trials Participation Awards). The notebook header flags the merit /
travel / participation programmes as the not-clearly-research pathway.

Output: s3://openalex-ingest/awards/conquer_cancer/conquer_cancer_projects.parquet
"""

import argparse
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; see runbook 1.2) ---
# (renamed-sys variant; the canonical call is sys.stdout.reconfigure(encoding="utf-8"))
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

SOURCE_URL = "https://www.asco.org/assets/contents/grant-award-recipients.json"
PAGE_URL = "https://www.asco.org/career-development/grants-awards/grant-award-recipients"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/conquer_cancer/conquer_cancer_projects.parquet"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)",
    "Accept": "application/json",
}
RETRIES = 4
MIN_EXPECTED = 8000  # the database held 8,838 records on 2026-10-01

# Programme -> (funding_type, research_pathway). research_pathway = False marks the
# meeting merit / abstract / travel awards and the institutional Clinical Trials
# Participation Award, which are kept but flagged (not clearly research funding).
CAREER = {"Young Investigator Award", "Career Development Award",
          "Global Oncology Young Investigator Award",
          "James B. Nachman Endowed ASCO Junior Faculty Award in Pediatric Oncology"}
RESEARCH = {"Advanced Clinical Research Award", "International Innovation Grant",
            "ASCO Registry Research Grant", "Career Pathway Grants in Symptom Management",
            "Improving Cancer Care Grant", "Research Professorship"}
FELLOWSHIP_RE = re.compile(r"fellowship(?! merit)", re.I)
TRAINING = {"International Development and Education Award", "Oncology Summer Internship",
            "Medical Student Rotation Award", "ASCO Clinical Oncology & Research Experience"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def classify(program: str) -> tuple[str, bool]:
    base = re.sub(r"\s*\(Peer [Rr]eviewed and .*\)$", "", program).strip()
    if base in CAREER:
        return "career_development", True
    if base in RESEARCH:
        return "research", True
    if FELLOWSHIP_RE.search(base) and "Merit" not in base:
        return "fellowship", True
    if base in TRAINING:
        return "training", True
    if "Travel" in base:
        return "travel", False
    # Merit / abstract / research-presentation awards and Clinical Trials Participation
    return "prize", False


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def program_code(program: str) -> str:
    return "".join(w[0] for w in re.findall(r"[A-Za-z]+", program) if w[0].isupper())


def parse_amount(s: str):
    s = (s or "").strip()
    if not s:
        return None
    v = re.sub(r"[^0-9.]", "", s)
    return float(v) if v else None


def fetch() -> list[dict]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(SOURCE_URL, headers=HEADERS, timeout=120)
            log(f"GET {SOURCE_URL} -> {r.status_code}, {len(r.content):,} bytes")
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"unexpected payload type {type(data)}")
            return data
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  attempt {attempt + 1} failed: {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"could not fetch {SOURCE_URL}: {last}")


def main() -> None:
    p = argparse.ArgumentParser(description="Conquer Cancer grant & award recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N records (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--input-json", type=Path, default=None, help="use a saved copy of the JSON instead of fetching")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    data = json.loads(args.input_json.read_text()) if args.input_json else fetch()
    log(f"{len(data)} records in the recipients database")
    if not args.limit and len(data) < MIN_EXPECTED:
        raise SystemExit(f"only {len(data)} records (expected >= {MIN_EXPECTED}); refusing to continue")
    if args.limit:
        data = data[: args.limit]

    rows = []
    for r in data:
        program = (r.get("Program") or "").strip()
        year = r.get("Year")
        first = (r.get("First Name") or "").strip() or None
        last = (r.get("Last Name") or "").strip() or None
        pid = (r.get("Project ID") or "").strip()
        ftype, research_pathway = classify(program)
        if pid:
            award_id, synthetic = pid, False
        else:
            award_id = f"CCF-{program_code(program)}{year}-{slug(last or r.get('Institution') or '')}"
            synthetic = True
        amount = parse_amount(r.get("Grant Amount ($)"))
        rows.append({
            "funder_award_id": award_id,
            "project_id": pid or None,
            "award_id_synthetic": synthetic,
            "year": str(year) if year is not None else None,
            "program": program or None,
            "funding_type": ftype,
            "research_pathway": research_pathway,
            "title": re.sub(r"\s+", " ", (r.get("Title") or "")).strip() or None,
            "lead_given_name": first,
            "lead_family_name": last,
            "degrees": (r.get("Degree(s)") or "").strip() or None,
            "institution": re.sub(r"\s+", " ", (r.get("Institution") or "")).strip() or None,
            "subject_area": (r.get("Subject Area") or "").strip() or None,
            "amount": amount,
            "amount_raw": (r.get("Grant Amount ($)") or "").strip() or None,
            "currency": "USD" if amount is not None else None,
            "landing_page_url": PAGE_URL,
            "source_url": SOURCE_URL,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    log(f"{len(df)} awards, {int(df['award_id_synthetic'].sum())} with a synthetic key")
    for c in ["title", "lead_family_name", "institution", "amount", "subject_area"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  years {df['year'].min()}-{df['year'].max()}; research pathway {int(df['research_pathway'].sum())}, "
        f"merit/travel/participation {int((~df['research_pathway']).sum())}")
    log(f"  total amount USD {df['amount'].sum():,.0f}")
    for prog, n in df["program"].value_counts().head(12).items():
        log(f"    {n:5d}  {prog}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "conquer_cancer_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_conquer_cancer_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
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
