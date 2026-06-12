#!/usr/bin/env python3
"""
Autism Science Foundation (US) to S3 Data Pipeline
==================================================

Scrapes the Autism Science Foundation's funded grantees and uploads a parquet to
S3 for Databricks ingestion.

Data source: autismsciencefoundation.org per-year "Grantees" hub pages
    https://autismsciencefoundation.org/<YEAR>-grantees/ (2010-2025), linked from
    /asf-funded-research/. Default UA fine. Each page groups grantees under grant
    -type section headings (Predoctoral/Postdoctoral Fellowships, etc.); each
    grantee is an <h4> "Name | Institution", followed by a "Mentor: …" line and
    the project title. PI + institution + grant type + project title + year are
    available; amounts are NOT published (section 6.7 waiver). No native id
    (synthesized). Autism research = MH-relevant.

Output: s3://openalex-ingest/awards/autism_science_foundation/asf_grants.parquet

Usage:
    python autism_science_foundation_to_s3.py
    python autism_science_foundation_to_s3.py --skip-upload
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

BASE = "https://autismsciencefoundation.org"
YEARS = list(range(2010, 2026))
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/autism_science_foundation/asf_grants.parquet"

_DEGREE = re.compile(r",?\s*(Ph\.?D\.?|M\.?D\.?|MSc|MS|MPH|MA|BA|BSc|Sc\.?D\.?|DO)\b\.?", re.I)
_TITLE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss)\.?\s+", re.I)
SECTION_KW = ("fellowship", "award", "grant", "scholar", "accelerator", "treatment network")


def parse_pi(raw):
    if not raw:
        return None, None
    n = _DEGREE.sub("", raw).strip().rstrip(",")
    n = _TITLE.sub("", n).strip()
    parts = n.split()
    if len(parts) < 2:
        return (None, n or None)
    return " ".join(parts[:-1]), parts[-1]


def parse_year_page(html, year):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    headings = soup.find_all(["h2", "h3", "h4", "h5"])
    out, section = [], None
    for h in headings:
        txt = h.get_text(" ", strip=True)
        if not txt:
            continue
        if " | " in txt:  # grantee
            name, _, inst = txt.partition(" | ")
            title = mentor = None
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3", "h4", "h5"):
                    break
                if sib.name == "p":
                    pt = sib.get_text(" ", strip=True)
                    if not pt:
                        continue
                    if pt.lower().startswith("mentor"):
                        mentor = pt.split(":", 1)[-1].strip()
                    elif title is None and len(pt) > 12:
                        title = pt
                if title and mentor:
                    break
            out.append({"name": name.strip(), "institution": inst.strip(),
                        "title": title, "mentor": mentor, "grant_type": section,
                        "year": str(year)})
        elif any(k in txt.lower() for k in SECTION_KW) and len(txt) < 60:
            section = txt
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
    ap = argparse.ArgumentParser(description="Autism Science Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/asf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Autism Science Foundation grantees -> S3")
    print("=" * 60)

    s = requests.Session()
    rows, seen = [], set()
    for yr in YEARS:
        url = f"{BASE}/{yr}-grantees/"
        try:
            r = s.get(url, timeout=60)
        except Exception as e:
            print(f"  {yr}: error {e}")
            continue
        if r.status_code != 200:
            print(f"  {yr}: HTTP {r.status_code} (no page)")
            continue
        recs = parse_year_page(r.text, yr)
        new = 0
        for rec in recs:
            given, family = parse_pi(rec["name"])
            slug = re.sub(r"[^a-z0-9]+", "-", (rec["name"] or "").lower()).strip("-")
            aid = f"asf-{rec['year']}-{slug}"
            if aid in seen or not family:
                continue
            seen.add(aid)
            rows.append({
                "funder_award_id": aid,
                "title": rec["title"] or f"ASF {rec['grant_type'] or 'award'} ({rec['year']})",
                "pi_given": given, "pi_family": family,
                "institution": rec["institution"],
                "programme": rec["grant_type"],
                "mentor": rec["mentor"],
                "start_year": rec["year"],
                "landing_page_url": url,
            })
            new += 1
        print(f"  {yr}: +{new} ({len(rows)} total)")
        time.sleep(0.5)

    if len(rows) < 80:
        print(f"[ERROR] only {len(rows)} grantees — expected ~190; layout change?")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "programme", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/len(df))}%)")
    print(f"  years {df['start_year'].min()}-{df['start_year'].max()}")
    out = args.output_dir / "asf_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
