#!/usr/bin/env python3
"""
James S. McDonnell Foundation (JSMF) to S3 Data Pipeline
========================================================

Scrapes JSMF's public grants listing (jsmf.org/grants, a WordPress + FacetWP
faceted table) and uploads a parquet to S3 for Databricks ingestion.

Data source: POST https://www.jsmf.org/grants/  (FacetWP AJAX; browser UA
    REQUIRED — Cloudflare 403s the default python-requests UA). Sending the
    grant_status facet empty returns ALL 1,161 grants (1987-2025), 20/page x 59.

Scope: listing fields only (title, recipient org, amount USD, year). The PI
    ("Project Lead") lives on per-grant detail pages behind robots Crawl-delay 10
    (1,161 pages = hours), so PI is left to a later enrichment pass; this is an
    institution-level ingest (like the CEP/dane.gov.pl funders).

Output: s3://openalex-ingest/awards/jsmf/jsmf_grants.parquet

Usage:
    python jsmf_to_s3.py
    python jsmf_to_s3.py --skip-upload --max-pages 2

Author: OpenAlex Team
"""

import argparse
import json
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

URL = "https://www.jsmf.org/grants/"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/jsmf/jsmf_grants.parquet"
REQUEST_DELAY = 1.5


def body_for(page):
    return {
        "action": "facetwp_refresh",
        "data[facets][search_grants]": "", "data[facets][focus_area]": "",
        "data[facets][grant_status]": "",  # empty = ALL grants
        "data[template]": "wp", "data[http_params][uri]": "grants",
        "data[extras][sort]": "default", "data[extras][counts]": "true",
        "data[extras][pager]": "true", "data[soft_refresh]": "0",
        "data[first_load]": "1", "data[is_bfcache]": "0", "data[paged]": str(page),
    }


def parse_amount(raw):
    if not raw:
        return None
    m = re.search(r"[\d,]+", raw)
    return str(int(m.group(0).replace(",", ""))) if m else None


def parse_rows(template_html):
    soup = BeautifulSoup(template_html, "html.parser")
    out = []
    for tr in soup.find_all("tr", attrs={"itemtype": re.compile("Grant")}):
        a = tr.find(attrs={"itemprop": "url"})
        url = a.get("href") if a else None
        slug = url.rstrip("/").rsplit("/", 1)[-1] if url else None
        title = a.get_text(" ", strip=True) if a else None
        date_el = tr.find(attrs={"itemprop": "dateAwarded"})
        rec_el = tr.find(attrs={"itemprop": "recipient"})
        amt_el = tr.find(attrs={"itemprop": "amount"})
        if not slug and not title:
            continue
        out.append({
            "funder_award_id": slug,
            "title": title,
            "recipient": rec_el.get_text(" ", strip=True) if rec_el else None,
            "amount": parse_amount(amt_el.get_text(strip=True)) if amt_el else None,
            "start_date_raw": date_el.get("datetime") if date_el else None,
            "start_year": (date_el.get_text(strip=True) if date_el else None),
            "landing_page_url": url,
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
    ap = argparse.ArgumentParser(description="Scrape JSMF grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/jsmf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-pages", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("James S. McDonnell Foundation grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Referer": URL,
                      "X-Requested-With": "XMLHttpRequest",
                      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})

    # page 1 to learn total_pages
    r = s.post(URL, data=body_for(1), timeout=40)
    r.raise_for_status()
    idx = r.text.find('{"facets')
    obj, _ = json.JSONDecoder().raw_decode(r.text[idx:])
    total_pages = obj.get("settings", {}).get("pager", {}).get("total_pages", 1)
    if args.max_pages:
        total_pages = min(total_pages, args.max_pages)
    print(f"total_pages: {total_pages}")

    rows, seen = parse_rows(obj["template"]), set()
    seen = {x["funder_award_id"] for x in rows}
    for page in range(2, total_pages + 1):
        time.sleep(REQUEST_DELAY)
        rp = s.post(URL, data=body_for(page), timeout=40)
        if rp.status_code != 200:
            print(f"  page {page}: HTTP {rp.status_code}; retrying once")
            time.sleep(5)
            rp = s.post(URL, data=body_for(page), timeout=40)
        i = rp.text.find('{"facets')
        if i < 0:
            print(f"  page {page}: no facets payload; skipping")
            continue
        o, _ = json.JSONDecoder().raw_decode(rp.text[i:])
        for rec in parse_rows(o.get("template", "")):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        if page % 10 == 0:
            print(f"  page {page}: {len(rows)} grants")

    if not rows:
        print("[ERROR] no grants scraped")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    out = args.output_dir / "jsmf_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
