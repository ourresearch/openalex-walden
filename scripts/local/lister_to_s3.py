#!/usr/bin/env python3
"""
Lister Institute of Preventive Medicine to S3 Data Pipeline
===========================================================

Scrapes the Lister Institute's Research Prize Fellowship awardees and uploads a
parquet to S3 for Databricks ingestion.

Data source: lister-institute.org.uk fellows pages (Umbraco, fully
    server-rendered, default UA fine; NOT a 360Giving publisher). Five static
    pages: current-fellows, former-fellows, former-fellows-retired-from-science,
    2024-prize-winners, 2025-prize-winners (~209 `.person` cards total).

Fields per card: fellow name (PI), institution, fellowship years ("Fellow
    2002-2007"; absent on the prize-winner pages, where the page year is used),
    research interests (used as the award title/topic). Per-grant amounts are
    NOT published (the prize is policy-level ~GBP 300k) -> amounts NULL,
    funding_type=fellowship (section 6.7 waiver). No native award ids
    (synthesized name+year slug). Fellows appearing on multiple pages are
    deduped by normalized name.

Output: s3://openalex-ingest/awards/lister/lister_fellowships.parquet

Usage:
    python lister_to_s3.py
    python lister_to_s3.py --skip-upload

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
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://lister-institute.org.uk"
PAGES = [
    ("fellows/current-fellows", None),
    ("fellows/former-fellows", None),
    ("fellows/former-fellows-retired-from-science", None),
    ("fellows/2024-prize-winners", "2024"),
    ("fellows/2025-prize-winners", "2025"),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lister/lister_fellowships.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def parse_pi(raw):
    if not raw:
        return None, None
    first = _TITLE_RE.sub("", raw.strip()).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip() or None


def parse_page(html, page_year):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for card in soup.select(".person"):
        h3 = card.find("h3")
        name = norm_ws(h3.get_text(" ", strip=True)) if h3 else None
        if not name:
            continue
        inst_el = card.select_one(".institution")
        dates_el = card.select_one(".fellowship-dates")
        ri_el = card.select_one(".research-interests")
        institution = norm_ws(inst_el.get_text(" ", strip=True)) if inst_el else None
        interests = norm_ws(ri_el.get_text(" ", strip=True)) if ri_el else None
        start_year = end_year = None
        if dates_el:
            m = re.search(r"(\d{4})\s*[-–]\s*(\d{4})?", dates_el.get_text(" ", strip=True))
            if m:
                start_year = m.group(1)
                end_year = m.group(2)
        if not start_year and page_year:
            start_year = page_year
        out.append({
            "name": name,
            "institution": institution,
            "interests": interests,
            "start_year": start_year,
            "end_year": end_year,
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
    ap = argparse.ArgumentParser(description="Lister Institute fellowships to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/lister_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Lister Institute Research Prize Fellowships -> S3")
    print("=" * 60)

    s = requests.Session()
    records = {}
    for path, page_year in PAGES:
        r = s.get(f"{BASE}/{path}/", timeout=60)
        if r.status_code != 200:
            print(f"  [WARN] {path}: HTTP {r.status_code}; retrying once")
            time.sleep(4)
            r = s.get(f"{BASE}/{path}/", timeout=60)
            r.raise_for_status()
        rows = parse_page(r.text, page_year)
        new = 0
        for rec in rows:
            key = re.sub(r"[^a-z0-9]+", "-", (_TITLE_RE.sub("", rec["name"])).lower()).strip("-")
            if key in records:
                # keep the richer record (dates/institution), prefer existing
                old = records[key]
                for f in ("institution", "interests", "start_year", "end_year"):
                    if not old.get(f) and rec.get(f):
                        old[f] = rec[f]
                continue
            records[key] = rec
            new += 1
        print(f"  {path}: {len(rows)} cards, +{new} new ({len(records)} total)")
        time.sleep(0.8)

    if len(records) < 150:
        print(f"[ERROR] only {len(records)} fellows — expected ~200; refusing partial ship")
        sys.exit(1)

    rows = []
    for key, rec in records.items():
        given, family = parse_pi(rec["name"])
        yr = rec["start_year"]
        rows.append({
            "funder_award_id": f"lister-{key}" + (f"-{yr}" if yr else ""),
            "title": rec["interests"] or "Lister Institute Research Prize Fellowship",
            "pi_given": given,
            "pi_family": family,
            "institution": rec["institution"],
            "start_year": yr,
            "end_year": rec["end_year"],
        })

    df = pd.DataFrame(rows).astype("string")
    dupes = df["funder_award_id"].duplicated().sum()
    if dupes:
        print(f"[WARN] {dupes} duplicate ids; suffixing")
        df["funder_award_id"] = df["funder_award_id"] + df.groupby(
            "funder_award_id").cumcount().map(lambda i: "" if i == 0 else f"-{i+1}")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("pi_family", "institution", "title", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    print(f"  years {df['start_year'].min()}-{df['start_year'].max()}")
    out = args.output_dir / "lister_fellowships.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
