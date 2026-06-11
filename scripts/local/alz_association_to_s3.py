#!/usr/bin/env python3
"""
Alzheimer's Association (US) to S3 Data Pipeline
================================================

Scrapes the Alzheimer's Association funded-studies database and uploads a parquet
to S3 for Databricks ingestion.

Data source: alz.org first-party JSON API behind the funded-studies search:
    https://www.alz.org/api/grants/getfundedstudies (Content-Type is text/plain
    but the body is JSON: {"FundedStudies": [...]}). The unfiltered call silently
    caps at 500 newest rows, so we slice by Cycle (year) 2008..current. Default
    UA works. ~2,400 funded studies; native award_id (high work-linkage), real
    PI + institution, year/cycle. Amounts are NOT published (section 6.7 waiver).

Output: s3://openalex-ingest/awards/alz_association/alz_association_studies.parquet

Usage:
    python alz_association_to_s3.py
    python alz_association_to_s3.py --skip-upload

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

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

API = "https://www.alz.org/api/grants/getfundedstudies"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alz_association/alz_association_studies.parquet"
CYCLES = list(range(2008, 2027))


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def parse_rows(payload):
    out = []
    for r in payload.get("FundedStudies", []):
        aid = clean(r.get("award_id")) or clean(r.get("FundedStudyID"))
        if not aid:
            continue
        desc = clean(r.get("description")) or clean(r.get("background"))
        out.append({
            "funder_award_id": aid,
            "title": clean(r.get("title")),
            "pi_given": clean(r.get("pi_first_name")),
            "pi_family": clean(r.get("pi_last_name")),
            "institution": clean(r.get("lead_institution")),
            "city": clean(r.get("city")),
            "country": clean(r.get("country")),
            "programme": clean(r.get("program")),
            "description": desc,
            "start_year": clean(r.get("cycle")),
        })
    return out


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
    ap = argparse.ArgumentParser(description="Alzheimer's Association funded studies to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/alz_association_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Alzheimer's Association (US) funded studies -> S3")
    print("=" * 60)

    s = requests.Session()
    rows, seen = [], set()
    for cyc in CYCLES:
        params = {"SearchTerm": "", "Cycle": str(cyc), "Investigator": "",
                  "State": "-1", "Country": "-1", "Program": "-1"}
        try:
            r = s.get(API, params=params, timeout=60)
            if r.status_code != 200:
                print(f"  cycle {cyc}: HTTP {r.status_code}; retry once")
                time.sleep(3)
                r = s.get(API, params=params, timeout=60)
            payload = json.loads(r.text)
        except Exception as e:
            print(f"  cycle {cyc}: error {e}; skipping")
            continue
        new = 0
        for rec in parse_rows(payload):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
            new += 1
        print(f"  cycle {cyc}: +{new} ({len(rows)} total)")
        time.sleep(0.4)

    if len(rows) < 1500:
        print(f"[ERROR] only {len(rows)} studies — expected ~2,400; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/len(df))}%)")
    print(f"  years {df['start_year'].min()}-{df['start_year'].max()}; dupes {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "alz_association_studies.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
