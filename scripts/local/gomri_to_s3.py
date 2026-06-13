#!/usr/bin/env python3
"""
Gulf of Mexico Research Initiative (GoMRI) to S3 Data Pipeline
==============================================================

Scrapes the GoMRI Research Information System (research.gulfresearchinitiative.org)
and uploads a parquet to S3 for Databricks ingestion.

GoMRI was the 10-year (2010-2020), $500M Deepwater-Horizon research program. Its
project records are still live and enumerable by integer project id:
    https://research.gulfresearchinitiative.org/research-awards/projects/?pid=N
Each page (HTTP 200) has an <h1>Project Overview</h1> followed by the title
(<h2>), a "Funding Source:" line, a Principal Investigator block (name,
institution, department), Member Institutions, and a "Summary:" abstract.
Invalid pids return HTTP 500. ~293 projects (pids ~1-450). Per-project amounts
and dates are not published (only aggregate per RFP round) -> NULL (§6.7 waiver).
PIs are at US institutions (Gulf program). provenance `gomri`, priority 331.
F4320309630 (Path A, single clean row).

Output: s3://openalex-ingest/awards/gomri/gomri_projects.parquet

Usage:
    python gomri_to_s3.py
    python gomri_to_s3.py --limit 10
    python gomri_to_s3.py --max-pid 450
    python gomri_to_s3.py --skip-upload

Author: OpenAlex Team
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

from bs4 import BeautifulSoup

BASE = "https://research.gulfresearchinitiative.org/research-awards/projects/?pid="
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gomri/gomri_projects.parquet"
EXPECTED_MIN = 200
DEFAULT_MAX_PID = 450
STOP_AFTER_CONSECUTIVE_MISS = 60   # pids have gaps; stop only after a long dry run
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n:
        return None, None
    n = _TITLE_RE.sub("", n).strip()
    # GoMRI names are 'First (nick) Last' Western order
    n = re.sub(r"\([^)]*\)", "", n).strip()
    toks = n.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def parse_project(html, pid):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style"]):
        x.extract()
    h2s = soup.select("h2")
    title = None
    for h in h2s:
        t = clean(h.get_text(" ", strip=True))
        if t and t.lower() != "summary:":
            title = t
            break
    if not title:
        return None
    lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    pi_name = pi_inst = abstract = funding_source = members = None
    for i, l in enumerate(lines):
        ls = l.strip()
        if ls == "Principal Investigator" and pi_name is None:
            pi_name = lines[i + 1] if i + 1 < len(lines) else None
            pi_inst = lines[i + 2] if i + 2 < len(lines) else None
        elif ls == "Member Institutions" and members is None:
            members = lines[i + 1] if i + 1 < len(lines) else None
        elif ls.rstrip(":").lower() == "summary" and abstract is None:
            abstract = " ".join(lines[i + 1:i + 6])[:4000]
        elif ls.startswith("Funding Source:"):
            funding_source = ls[len("Funding Source:"):].strip()
    # Skip GoMRI's own internal admin/legacy/synthesis "projects" — these list the
    # funder as its own institution and a "Team" as PI, not a researcher grant.
    pin = (pi_name or "")
    if re.search(r"\b(Team|Admin|Legacy|Synthesis|Outreach|Office)\b", pin, re.I) or \
       (pi_inst and "Gulf of Mexico Research Initiative" in pi_inst):
        return None
    given, family = split_name(pi_name)
    return {
        "funder_award_id": f"gomri-{pid}",
        "title": title,
        "description": clean(abstract),
        "pi_given": given,
        "pi_family": family,
        "institution": clean(pi_inst),
        "member_institutions": clean(members),
        "funder_scheme": clean(funding_source),
        "landing_page_url": f"{BASE}{pid}",
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
    ap = argparse.ArgumentParser(description="GoMRI projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/gomri_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--max-pid", type=int, default=DEFAULT_MAX_PID)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Gulf of Mexico Research Initiative projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    rows = []
    miss = 0
    for pid in range(1, args.max_pid + 1):
        try:
            r = s.get(f"{BASE}{pid}", timeout=40)
        except Exception as e:
            print(f"  pid {pid}: error {e}; continuing")
            time.sleep(1)
            continue
        if r.status_code != 200 or len(r.text) < 500:
            miss += 1
            if miss >= STOP_AFTER_CONSECUTIVE_MISS and pid > 100:
                print(f"  {miss} consecutive misses at pid {pid} — assuming end of corpus")
                break
            continue
        rec = parse_project(r.text, pid)
        if not rec:
            miss += 1
            continue
        miss = 0
        rows.append(rec)
        if len(rows) % 25 == 0:
            print(f"  [{time.strftime('%H:%M:%S')}] pid {pid}: {len(rows)} projects")
        if args.limit and len(rows) >= args.limit:
            break
        time.sleep(0.25)

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "description"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "gomri_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
