#!/usr/bin/env python3
"""
Tenovus Cancer Care (Wales) to S3 Data Pipeline
===============================================

Scrapes Tenovus Cancer Care's research portfolio and uploads a parquet to S3 for
Databricks ingestion.

Data source: tenovuscancercare.org.uk. Project URLs come from the XML sitemap at
    `/sitemap` (NOT /sitemap.xml — that 404s; the path is in robots.txt). Filter
    `<loc>` entries containing `/research-listings/` and excluding
    `/research-listings/get-involved/...` (surveys, not awards) -> 31 research
    project detail pages. The browsable listing is JS/AJAX-paginated and a dead
    end for enumeration; the sitemap is the authority (Umbraco AJAX endpoint
    returns the same 31). Default UA fine.

Per-detail fields (paired Bootstrap `.col-6` label/value): Funding Amount (GBP,
    hex-entity-encoded), Supervisor (PI), Location (institution), Cancer Type,
    Funding Type (scheme), Research Type, Status, and a date range. Title =
    <title>. No native ids (slug used).

Output: s3://openalex-ingest/awards/tenovus/tenovus_grants.parquet

Usage:
    python tenovus_to_s3.py
    python tenovus_to_s3.py --skip-upload

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

SITEMAP = "https://www.tenovuscancercare.org.uk/sitemap"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/tenovus/tenovus_grants.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&|,", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def parse_amount(raw):
    if not raw:
        return None
    m = re.search(r"[\d,]+", raw)
    return str(int(m.group(0).replace(",", ""))) if m else None


def mon_year(tok):
    m = re.match(r"([A-Za-z]{3})[a-z]*\s+(\d{4})", tok.strip())
    if m and m.group(1).title() in MONTHS:
        return f"{m.group(2)}-{MONTHS[m.group(1).title()]:02d}-01", m.group(2)
    y = re.search(r"(\d{4})", tok)
    return (f"{y.group(1)}-01-01", y.group(1)) if y else (None, None)


def get_slugs(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
    return [u for u in locs if "/research-listings/" in u
            and "/get-involved" not in u
            and not u.rstrip("/").endswith("/research-listings")]


def parse_detail(html, url):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title")
    title = title.get_text(strip=True) if title else None
    if title:
        title = re.sub(r"\s*[|–-]\s*Tenovus.*$", "", title).strip()
    fields = {}
    cols = soup.select(".col-6")
    for i in range(0, len(cols) - 1, 2):
        lab = cols[i].get_text(" ", strip=True)
        val = cols[i + 1].get_text(" ", strip=True)
        if lab and len(lab) < 30:
            fields[lab.rstrip(":")] = val
    start_date = start_year = end_year = None
    dm = re.search(r"([A-Z][a-z]{2,8}\s+\d{4})\s*[-–]\s*([A-Z][a-z]{2,8}\s+\d{4})",
                   soup.get_text(" ", strip=True))
    if dm:
        start_date, start_year = mon_year(dm.group(1))
        _, end_year = mon_year(dm.group(2))
    pi_given, pi_family = parse_pi(fields.get("Supervisor"))
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    return {
        "funder_award_id": f"tenovus-{slug}"[:90],
        "title": title,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": fields.get("Location"),
        "amount": parse_amount(fields.get("Funding Amount")),
        "programme": fields.get("Funding Type"),
        "research_type": fields.get("Research Type"),
        "cancer_type": fields.get("Cancer Type"),
        "start_date": start_date,
        "start_year": start_year,
        "end_year": end_year,
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
    ap = argparse.ArgumentParser(description="Tenovus Cancer Care grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/tenovus_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Tenovus Cancer Care research grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    slugs = get_slugs(s)
    print(f"Research project URLs: {len(slugs)}")
    if len(slugs) < 20:
        print(f"[ERROR] only {len(slugs)} slugs — sitemap layout change?")
        sys.exit(1)

    rows = []
    for n, url in enumerate(slugs, 1):
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
            print(f"  [{n}] FAIL {url}")
            continue
        rows.append(parse_detail(html, url))
        time.sleep(0.4)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "tenovus_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
