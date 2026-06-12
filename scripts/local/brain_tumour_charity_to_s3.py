#!/usr/bin/env python3
"""
The Brain Tumour Charity to S3 Data Pipeline
============================================

Scrapes The Brain Tumour Charity's (UK) funded research projects and uploads a
parquet to S3 for Databricks ingestion.

Data source: WordPress custom post type
    https://www.thebraintumourcharity.org/wp-json/wp/v2/tbtc-research?per_page=100
    (X-WP-Total 49) for enumeration (id/title/link); the useful fields live in
    the rendered detail page under "Fast facts" labels: Title, Lead
    researcher(s), Where (institution), When (date range), Cost (GBP), Research
    type. Default UA fine. Rich source — 100% PI / amount / dates / institution.

Output: s3://openalex-ingest/awards/brain_tumour_charity/brain_tumour_charity_grants.parquet

Usage:
    python brain_tumour_charity_to_s3.py
    python brain_tumour_charity_to_s3.py --skip-upload

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

REST = "https://www.thebraintumourcharity.org/wp-json/wp/v2/tbtc-research?per_page=100&page=1"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/brain_tumour_charity/brain_tumour_charity_grants.parquet"

# label (colon stripped, lowercased) -> field
LABELS = {
    "title": "detail_title",
    "lead researcher": "pi_raw", "lead researchers": "pi_raw",
    "where": "institution", "when": "when", "cost": "cost_raw",
    "research type": "research_type",
}
_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";|,| and |&|/", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def parse_cost(raw):
    if not raw:
        return None
    s = raw.replace(",", "")
    m = re.search(r"£\s*(\d+(?:\.\d+)?)\s*(million|m\b|k\b)?", s, re.I)
    if not m:
        return None
    val = float(m.group(1))
    suf = (m.group(2) or "").lower()
    if suf.startswith("m"):
        val *= 1_000_000
    elif suf == "k":
        val *= 1_000
    return str(int(val))


def mon_year(tok):
    m = re.match(r"([A-Za-z]{3})[a-z]*\s+(\d{4})", tok.strip())
    if m and m.group(1).title() in MONTHS:
        return f"{m.group(2)}-{MONTHS[m.group(1).title()]:02d}-01", m.group(2)
    y = re.search(r"(\d{4})", tok)
    return (f"{y.group(1)}-01-01", y.group(1)) if y else (None, None)


def parse_detail(html, rest_title):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    f = {}
    for i, l in enumerate(lines):
        key = l.split(":", 1)[0].rstrip(":").strip().lower()
        norm = LABELS.get(key)
        if not norm:
            continue
        val = l.split(":", 1)[1].strip() if ":" in l else ""
        if not val and i + 1 < len(lines):
            nxt = lines[i + 1]
            if nxt.split(":", 1)[0].rstrip(":").strip().lower() not in LABELS:
                val = nxt
        val = re.sub(r"^[:\-–—•·]\s*", "", val).strip()
        if val and norm not in f:
            f[norm] = val

    start_date = start_year = end_year = None
    when = f.get("when", "")
    dm = re.search(r"([A-Za-z]{3,9}\s+\d{4})\s*[-–]\s*([A-Za-z]{3,9}\s+\d{4})", when)
    if dm:
        start_date, start_year = mon_year(dm.group(1))
        _, end_year = mon_year(dm.group(2))
    elif when:
        start_date, start_year = mon_year(when)
    given, family = parse_pi(f.get("pi_raw"))
    # guard against a stray 1-2 char "Title" label (the Tessa Jowell BRAIN
    # MATRIX row parsed a lone "A"); fall back to the WP-REST title when the
    # detail-page Title label is implausibly short.
    dt = (f.get("detail_title") or "").strip()
    return {
        "title": dt if len(dt) >= 4 else rest_title,
        "pi_given": given, "pi_family": family,
        "institution": f.get("institution"),
        "amount": parse_cost(f.get("cost_raw")),
        "research_type": f.get("research_type"),
        "start_date": start_date, "start_year": start_year, "end_year": end_year,
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
    ap = argparse.ArgumentParser(description="Brain Tumour Charity grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/brain_tumour_charity_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("The Brain Tumour Charity research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    posts = s.get(REST, timeout=60).json()
    print(f"REST records: {len(posts)}")
    if len(posts) < 40:
        print(f"[ERROR] only {len(posts)} — expected ~49")
        sys.exit(1)

    rows = []
    for n, p in enumerate(posts, 1):
        link = p.get("link", "")
        slug = link.rstrip("/").rsplit("/", 1)[-1]
        rest_title = BeautifulSoup(p.get("title", {}).get("rendered", ""), "html.parser").get_text(" ", strip=True)
        det = {"title": rest_title}
        try:
            rd = s.get(link, timeout=60)
            if rd.status_code == 200:
                det = parse_detail(rd.text, rest_title)
        except Exception:
            pass
        rows.append({
            "funder_award_id": f"tbtc-{p.get('id') or slug}",
            "title": det.get("title"),
            "pi_given": det.get("pi_given"), "pi_family": det.get("pi_family"),
            "institution": det.get("institution"),
            "amount": det.get("amount"),
            "programme": det.get("research_type"),
            "start_date": det.get("start_date"), "start_year": det.get("start_year"),
            "end_year": det.get("end_year"),
            "landing_page_url": link or None,
        })
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "brain_tumour_charity_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
