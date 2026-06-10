#!/usr/bin/env python3
"""
Stroke Association (UK) to S3 Data Pipeline
===========================================

Scrapes the Stroke Association's public funded-research portal (Drupal 10) and
uploads a parquet to S3 for Databricks ingestion.

Data source:
    Listing: https://www.stroke.org.uk/research/research-projects?page=0..N
    Detail:  https://www.stroke.org.uk/about-us/research/projects/<slug>
    ~173 funded projects, 2016-2026. Fully server-rendered HTML.

WAF note:
    The site is behind CloudFront, which 403s the default python-requests UA
    site-wide. A browser User-Agent header (no cookies/JS) returns 200 for
    everything. That single header is the only requirement.

Output: s3://openalex-ingest/awards/stroke_association/stroke_association_projects.parquet

Requirements: pip install pandas pyarrow requests beautifulsoup4
    AWS CLI configured with write access to s3://openalex-ingest/awards/stroke_association/

Usage:
    python stroke_association_to_s3.py
    python stroke_association_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
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

BASE = "https://www.stroke.org.uk"
LIST_URL = BASE + "/research/research-projects"
# CloudFront 403s the default requests UA; a browser UA fully unblocks it.
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/stroke_association/stroke_association_projects.parquet"

MAX_CONSECUTIVE_EMPTY = 3
REQUEST_DELAY = 0.7


import re

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def text_of(soup, cls):
    el = soup.select_one(f".views-field-{cls} .field-content")
    return el.get_text(" ", strip=True) if el else None


def parse_pi(pi_raw):
    """First PI -> (given, family). Strip titles; split on last whitespace token."""
    if not pi_raw:
        return None, None
    first = re.split(r";| and |&", pi_raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    # strip trailing post-nominals / degrees
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def clean_amount(raw):
    """'£269,697.00' -> 269697.00 (float as string), else None."""
    if not raw:
        return None
    m = re.search(r"[\d,]+(?:\.\d+)?", raw.replace(" ", ""))
    if not m:
        return None
    try:
        return str(float(m.group(0).replace(",", "")))
    except ValueError:
        return None


def collect_slugs(session) -> list:
    """Page through the listing view to gather all detail URLs."""
    slugs, page, empties = [], 0, 0
    while True:
        r = session.get(LIST_URL, params={"page": page}, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select(".views-row")
        links = [a["href"] for a in soup.select(".views-field-view-node a.readmore[href]")]
        if not links:
            empties += 1
            if empties >= MAX_CONSECUTIVE_EMPTY or not rows:
                break
        else:
            empties = 0
            for h in links:
                slugs.append(h if h.startswith("http") else BASE + h)
        # honor the "Go to last page" terminator if present
        last = soup.select_one('a[title="Go to last page"]')
        if last and "page=" in last.get("href", ""):
            import re
            m = re.search(r"page=(\d+)", last["href"])
            if m and page >= int(m.group(1)):
                break
        page += 1
        if page > 60:  # hard backstop
            break
        time.sleep(REQUEST_DELAY)
    # dedupe, preserve order
    return list(dict.fromkeys(slugs))


def parse_detail(session, url) -> dict:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    d = BeautifulSoup(r.text, "html.parser")
    h1 = d.select_one("h1.stroke-page-title") or d.select_one("h1")
    body = d.select_one(".field--name-body")
    pi_raw = text_of(d, "field-principal-investigator")
    pi_given, pi_family = parse_pi(pi_raw)
    return {
        "funder_award_id": text_of(d, "field-research-id"),
        "slug": url.rstrip("/").rsplit("/", 1)[-1],
        "title": h1.get_text(" ", strip=True) if h1 else None,
        "scientific_title": text_of(d, "field-scientific-title"),
        "pi_raw": pi_raw,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "amount": clean_amount(text_of(d, "field-grant-value")),
        "institution": text_of(d, "field-institution"),
        "region": text_of(d, "field-region"),
        "amount_gbp_raw": text_of(d, "field-grant-value"),
        "research_area": text_of(d, "field-research-area"),
        "year_awarded": text_of(d, "field-years-dropdown"),
        "start_date_raw": text_of(d, "field-start-date"),
        "end_date_raw": text_of(d, "field-end-date"),
        "status": text_of(d, "field-status"),
        "description": body.get_text("\n", strip=True) if body else None,
        "landing_page_url": url,
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
    ap = argparse.ArgumentParser(description="Scrape Stroke Association funded research to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/stroke_association_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Stroke Association (UK) funded research -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})

    slugs = collect_slugs(s)
    print(f"Collected {len(slugs)} project URLs")
    if args.limit:
        slugs = slugs[:args.limit]

    rows = []
    for i, url in enumerate(slugs, 1):
        try:
            rows.append(parse_detail(s, url))
        except Exception as e:
            print(f"  [WARN] {url}: {e}")
        if i % 25 == 0:
            print(f"  {i}/{len(slugs)} details fetched")
        time.sleep(REQUEST_DELAY)

    if not rows:
        print("[ERROR] no projects scraped")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    out = args.output_dir / "stroke_association_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
