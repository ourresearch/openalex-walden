#!/usr/bin/env python3
"""
Research Manitoba to S3 Data Pipeline
=====================================

Scrapes Research Manitoba's public grants-management search (GMS) and uploads a
parquet to S3 for Databricks ingestion.

Data source: https://gms.researchmanitoba.ca/funding-search/?save_button=Search
    (their live f4django grants DB — NOT the WordPress site, whose wp/v2/project
    endpoint is empty). The `save_button=Search` param is REQUIRED (without it the
    search returns 0 rows). Paginated `&page=N`, 30 awards/page. Default UA fine
    (no WAF on the gms subdomain).

Scope: the search-result rows carry Year, Applicant (Last, First), Institution,
    Program, Project Title, Amount (CAD) — a complete PI + amount + institution
    ingest. The per-grant abstract + duration live on detail pages
    (/funding-search/{id}/); skipped here (1,169 pages) and left to a later
    enrichment pass. The list exposes no native grant id, so funder_award_id is
    a synthetic hash of (year|applicant|title|amount).

Output: s3://openalex-ingest/awards/research_manitoba/research_manitoba_grants.parquet

Usage:
    python research_manitoba_to_s3.py
    python research_manitoba_to_s3.py --skip-upload --max-pages 3

Author: OpenAlex Team
"""

import argparse
import hashlib
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

BASE = "https://gms.researchmanitoba.ca/funding-search/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/research_manitoba/research_manitoba_grants.parquet"
REQUEST_DELAY = 0.8


def parse_amount(raw):
    if not raw:
        return None
    m = re.search(r"[\d,]+", raw.replace("$", ""))
    return str(int(m.group(0).replace(",", ""))) if m else None


def split_applicant(raw):
    """'Strachan, Shaelyn' -> (given, family). Orgs (no comma) -> (None, whole)."""
    raw = (raw or "").strip()
    if not raw:
        return None, None
    if "," in raw:
        family, given = raw.split(",", 1)
        return given.strip() or None, family.strip() or None
    return None, raw


def parse_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tr in soup.find_all("tr", class_=["odd", "even"]):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 7:
            continue
        # col0 blank, then Year, Applicant, Institution, Program, Title, Amount
        _, year, applicant, institution, program, title, amount = tds[:7]
        if not (title or applicant):
            continue
        given, family = split_applicant(applicant)
        yr = re.sub(r"\D", "", year)[:4] or None
        sig = f"{yr}|{applicant}|{title}|{amount}"
        aid = "rm-" + hashlib.sha1(sig.encode("utf-8")).hexdigest()[:16]
        out.append({
            "funder_award_id": aid,
            "title": title or None,
            "pi_given": given,
            "pi_family": family,
            "institution": institution or None,
            "amount": parse_amount(amount),
            "programme": program or None,
            "start_year": yr,
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
    ap = argparse.ArgumentParser(description="Research Manitoba grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/research_manitoba_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-pages", type=int, default=80)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Research Manitoba grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})  # default UA fine

    rows, seen = [], set()
    empty_streak = 0
    for page in range(1, args.max_pages + 1):
        params = {"save_button": "Search", "page": page}
        try:
            r = s.get(BASE, params=params, timeout=60)
            if r.status_code != 200:
                print(f"  page {page}: HTTP {r.status_code}; retry once")
                time.sleep(4)
                r = s.get(BASE, params=params, timeout=60)
            page_rows = parse_rows(r.text)
        except Exception as e:
            print(f"  page {page}: error {e}; retry once")
            time.sleep(4)
            try:
                page_rows = parse_rows(s.get(BASE, params=params, timeout=60).text)
            except Exception as e2:
                print(f"  page {page}: failed twice ({e2}); STOP to avoid partial")
                sys.exit(1)
        new = 0
        for rec in page_rows:
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
            new += 1
        if not page_rows:
            empty_streak += 1
            if empty_streak >= 2:
                print(f"  page {page}: empty x2 -> end of results")
                break
        else:
            empty_streak = 0
        if page % 5 == 0 or not page_rows:
            print(f"  page {page}: +{new} ({len(rows)} total)")
        time.sleep(REQUEST_DELAY)

    if len(rows) < 800:
        print(f"[ERROR] only {len(rows)} grants — expected ~1,169; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    print(f"  years {df['start_year'].min()}-{df['start_year'].max()}")
    out = args.output_dir / "research_manitoba_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
