#!/usr/bin/env python3
"""
EMBO (European Molecular Biology Organization) to S3 Data Pipeline
==================================================================

Scrapes EMBO's public Young Investigator / Installation Grantee / Global
Investigator directory (the backend of the yip-search.embo.org SPA) and uploads
a parquet to S3 for Databricks ingestion.

Data source: https://yip-search.embo.org/api/yips/  (single anonymous JSON GET,
    878 awardees, selection years 2001-2026). No pagination, no auth.
    TLS note: embo-web03's cert chain isn't in some CA bundles -> verify=False.

Scope: covers EMBO's named individual-investigator awards (YIP/IG/GIN). EMBO
    Postdoctoral Fellowships are behind a members-only portal and are not
    publicly scrapable, so they are out of scope. Amounts are NOT published for
    these awards (NULL, fellowship/investigator waiver per runbook 6.7).

Output: s3://openalex-ingest/awards/embo/embo_awards.parquet

Usage:
    python embo_to_s3.py
    python embo_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

API_URL = "https://yip-search.embo.org/api/yips/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/embo/embo_awards.parquet"


def extract(rec: dict) -> dict:
    inst = rec.get("affiliations") or rec.get("affiliation_at_selection")
    if isinstance(inst, list):
        inst = inst[0] if inst else None
    return {
        "funder_award_id": str(rec.get("id")) if rec.get("id") is not None else rec.get("project_number"),
        "project_number": rec.get("project_number"),
        "title": rec.get("project_title"),
        "pi_given": rec.get("first_name"),
        "pi_family": rec.get("last_name"),
        "institution": inst,
        "country": rec.get("country") or rec.get("country_at_selection"),
        "programme_name": rec.get("programme_name"),
        "programme_type": rec.get("programme_type"),
        "subject": rec.get("subject_1"),
        "start_year": rec.get("since"),
        "end_year": rec.get("until"),
        "landing_page_url": rec.get("web_address"),
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
    ap = argparse.ArgumentParser(description="Scrape EMBO YIP/IG/GIN awardees to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/embo_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("EMBO YIP/IG/GIN awardees -> S3")
    print("=" * 60)

    # embo-web03 cert chain isn't in the conda CA bundle; verify=False is required.
    import urllib3
    urllib3.disable_warnings()
    r = requests.get(API_URL, timeout=60, verify=False)
    r.raise_for_status()
    records = r.json()
    print(f"Fetched {len(records)} awardees")

    rows = [extract(x) for x in records]
    if not rows:
        print("[ERROR] no records")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"DataFrame: {len(df)} rows, {len(df.columns)} columns")
    out = args.output_dir / "embo_awards.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
