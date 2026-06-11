#!/usr/bin/env python3
"""
Blood Cancer UK (formerly Bloodwise) to S3 Data Pipeline
========================================================

Scrapes Blood Cancer UK's current research-projects portfolio and uploads a
parquet to S3 for Databricks ingestion.

Data source: bloodcancer.org.uk. Project URLs enumerated from sitemap.xml
    (~120 `/research/research-projects/{slug}/` server-rendered pages; default
    UA fine). NOT a 360Giving publisher (GrantNav shows recipient-only), and the
    site holds only the current portfolio (no historical Bloodwise/LLR archive).

Per-project fields: title (H1), lead researcher (PI) + institution (inline,
    right after the PI name), Related Conditions, Research Type, Region. The
    charity publishes **no amounts and no dates** for these projects, so
    amount/start_date/currency are NULL (section 6.7 waiver). No native ids
    (slug used).

Output: s3://openalex-ingest/awards/blood_cancer_uk/blood_cancer_uk_projects.parquet

Usage:
    python blood_cancer_uk_to_s3.py
    python blood_cancer_uk_to_s3.py --skip-upload --max-grants 10

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

SITEMAP = "https://bloodcancer.org.uk/sitemap.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/blood_cancer_uk/blood_cancer_uk_projects.parquet"
REQUEST_DELAY = 0.3

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
LABELS = ("Related Conditions", "Research Type", "Region", "Lead researcher")


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def get_project_urls(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    return sorted(set(re.findall(
        r"<loc>([^<]+/research/research-projects/[^<]+)</loc>", r.text)))


def parse_project(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    for s in soup(["script", "style", "nav", "header", "footer"]):
        s.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    pi = inst = conditions = rtype = region = None
    for i, l in enumerate(lines):
        if l == "Lead researcher":
            if i + 1 < len(lines):
                pi = lines[i + 1]
            if i + 2 < len(lines) and not any(lines[i + 2].startswith(x) for x in LABELS):
                inst = lines[i + 2]
        elif l == "Related Conditions" and i + 1 < len(lines):
            conditions = lines[i + 1]
        elif l == "Research Type" and i + 1 < len(lines):
            rtype = lines[i + 1]
        elif l == "Region" and i + 1 < len(lines):
            region = lines[i + 1]
    pi_given, pi_family = parse_pi(pi)
    return {
        "funder_award_id": f"bcuk-{slug}",
        "title": title,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": inst,
        "conditions": conditions,
        "research_type": rtype,
        "region": region,
        "landing_page_url": f"https://bloodcancer.org.uk/research/research-projects/{slug}/",
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
    ap = argparse.ArgumentParser(description="Blood Cancer UK projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/blood_cancer_uk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-grants", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Blood Cancer UK research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    urls = get_project_urls(s)
    if args.max_grants:
        urls = urls[:args.max_grants]
    print(f"Project URLs: {len(urls)}")

    rows, seen, failures = [], set(), 0
    for n, url in enumerate(urls, 1):
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        html = None
        for _ in (1, 2):
            try:
                r = s.get(url, timeout=60)
                if r.status_code == 200:
                    html = r.text
                    break
                time.sleep(3)
            except Exception:
                time.sleep(3)
        if html is None:
            failures += 1
            print(f"  [{n}] FAIL {slug}")
            if failures > 20:
                print("[ERROR] >20 failures; STOP to avoid partial ship")
                sys.exit(1)
            continue
        rec = parse_project(html, slug)
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        if n % 40 == 0:
            print(f"  [{n}/{len(urls)}] {len(rows)} projects")
        time.sleep(REQUEST_DELAY)

    if len(rows) < len(urls) * 0.9:
        print(f"[ERROR] only {len(rows)}/{len(urls)} (<90%); refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "blood_cancer_uk_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
