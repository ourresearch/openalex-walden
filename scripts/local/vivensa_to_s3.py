#!/usr/bin/env python3
"""
Vivensa Foundation (Dunhill Medical Trust) to S3 Data Pipeline
==============================================================

Downloads the foundation's published 360Giving grants file and uploads a
parquet to S3 for Databricks ingestion.

Data source: 360Giving XLSX published by the Vivensa Foundation (the Dunhill
    Medical Trust, rebranded 2025/26; grant identifiers keep the
    360G-dunhillmedical-* prefix). The current file URL is discovered from the
    360Giving registry (https://registry.threesixtygiving.org/data.json,
    publisher prefix 360G-dunhillmedical, newest `modified` wins) with the
    2026-01-15 file as a pinned fallback. Default UA fine everywhere.

Scope: one clean workbook, ~232 grants 2009-2026. 100% id/title/description/
    amount(GBP)/award date/recipient org/programme; ~98% planned start/end
    dates. Org-level only — the file carries NO PI names (like the CEP and
    dane.gov.pl funders).

Output: s3://openalex-ingest/awards/vivensa/vivensa_grants.parquet

Usage:
    python vivensa_to_s3.py
    python vivensa_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import io
import json
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

REGISTRY = "https://registry.threesixtygiving.org/data.json"
PUBLISHER_PREFIX = "360G-dunhillmedical"
FALLBACK_XLSX = ("https://vivensafoundation.org.uk/wp-content/uploads/2026/01/"
                 "2026-01-15-VF-final-file.xlsx")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/vivensa/vivensa_grants.parquet"

COLMAP = {
    "Identifier": "funder_award_id",
    "Title": "title",
    "Description": "description",
    "Currency": "currency",
    "Amount Awarded": "amount",
    "Award Date": "award_date",
    "Planned Dates:Start Date": "start_date",
    "Planned Dates:End Date": "end_date",
    "Recipient Org:Name": "recipient",
    "Grant Programme:Title": "programme",
}


def discover_xlsx_url(session):
    """Newest 360Giving registry distribution for the Vivensa/Dunhill publisher."""
    try:
        r = session.get(REGISTRY, timeout=40)
        r.raise_for_status()
        entries = [e for e in r.json()
                   if e.get("publisher", {}).get("prefix") == PUBLISHER_PREFIX]
        entries.sort(key=lambda e: e.get("modified", ""), reverse=True)
        for e in entries:
            url = (e.get("distribution") or [{}])[0].get("downloadURL")
            if url:
                print(f"Registry file (modified {e.get('modified', '?')[:10]}): {url}")
                return url
    except Exception as e:
        print(f"[WARN] registry discovery failed ({e}); using pinned fallback")
    return FALLBACK_XLSX


def clean_date(v):
    if pd.isna(v):
        return None
    return str(v)[:10]


def clean_amount(v):
    if pd.isna(v):
        return None
    try:
        return str(int(float(v)))
    except (TypeError, ValueError):
        return None


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
    ap = argparse.ArgumentParser(description="Vivensa (Dunhill) 360Giving grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/vivensa_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Vivensa Foundation (Dunhill Medical Trust) grants -> S3")
    print("=" * 60)

    s = requests.Session()
    url = discover_xlsx_url(s)
    r = s.get(url, timeout=120)
    r.raise_for_status()
    raw = pd.read_excel(io.BytesIO(r.content), dtype=object, engine="openpyxl")
    print(f"Workbook rows: {len(raw)}")

    missing = [c for c in COLMAP if c not in raw.columns]
    if missing:
        print(f"[ERROR] expected 360Giving columns missing: {missing}")
        sys.exit(1)

    df = raw[list(COLMAP)].rename(columns=COLMAP)
    df["amount"] = df["amount"].map(clean_amount)
    for c in ("award_date", "start_date", "end_date"):
        df[c] = df[c].map(clean_date)
    df["start_year"] = df["start_date"].fillna(df["award_date"]).str[:4]
    df = df[df["funder_award_id"].notna()]

    dupes = df["funder_award_id"].duplicated().sum()
    if dupes:
        print(f"[WARN] {dupes} duplicate ids; keeping first")
        df = df.drop_duplicates("funder_award_id", keep="first")

    df = df.astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    print(f"  amounts: {df['amount'].notna().sum()}/{len(df)}; "
          f"years {df['start_year'].min()}-{df['start_year'].max()}")
    if len(df) < 200:
        print(f"[ERROR] only {len(df)} grants — expected ~232; refusing to ship a shrunk file")
        sys.exit(1)

    out = args.output_dir / "vivensa_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
