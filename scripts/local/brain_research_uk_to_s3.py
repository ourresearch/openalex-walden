#!/usr/bin/env python3
"""
Brain Research UK to S3 Data Pipeline
=====================================

Scrapes Brain Research UK's funded research projects and uploads a parquet to S3
for Databricks ingestion.

Data source: brainresearchuk.org.uk. Project detail URLs come from the XML
    sitemap (`/sitemap.xml` -> ~71 `/research-project/<slug>` pages). Default UA
    fine. Each detail page has a `Researcher` label (PI on the next line), the
    host institution as a "University of …" line, a funding type
    (Fellowship/Project grant -> funder_scheme), and an `Awarded in <Month
    Year>` date (sometimes a completion date too). Amounts are NOT published
    (section 6.7 waiver). Slug = native id. Neuro/brain research (MH-adjacent).

Output: s3://openalex-ingest/awards/brain_research_uk/brain_research_uk_grants.parquet

Usage:
    python brain_research_uk_to_s3.py
    python brain_research_uk_to_s3.py --skip-upload

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

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

from bs4 import BeautifulSoup

SITEMAP = "https://www.brainresearchuk.org.uk/sitemap.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/brain_research_uk/brain_research_uk_grants.parquet"

INST_RE = re.compile(r"(University|College|Institute|Imperial|King'?s|Hospital|"
                     r"School of|UCL|Queen Mary|Newcastle|Cardiff|Dundee|Edinburgh)", re.I)
FUND_TYPES = ("fellowship", "project grant", "phd studentship", "programme grant",
              "pump priming", "small grant", "studentship", "research grant")
_TITLE_RE = re.compile(r"^(Professor|Prof|Doctor|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], 1)}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&|,", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def mon_year(s):
    m = re.search(r"([A-Za-z]+)\s+(\d{4})", s or "")
    if m and m.group(1).title() in MONTHS:
        return f"{m.group(2)}-{MONTHS[m.group(1).title()]:02d}-01", m.group(2)
    y = re.search(r"(\d{4})", s or "")
    return (f"{y.group(1)}-01-01", y.group(1)) if y else (None, None)


def get_urls(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    return sorted(set(re.findall(r"<loc>([^<]+/research-project/[^<]+)</loc>", r.text)))


def parse_detail(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    pi = institution = ftype = None
    start_date = start_year = end_year = None
    researcher_idx = None
    for i, l in enumerate(lines):
        low = l.lower()
        if low == "researcher" and i + 1 < len(lines):
            pi = pi or lines[i + 1]
            researcher_idx = researcher_idx if researcher_idx is not None else i
        elif low in FUND_TYPES and ftype is None:
            ftype = l
        elif low.startswith("awarded in"):
            tail = l[len("awarded in"):].strip() or (lines[i + 1] if i + 1 < len(lines) else "")
            start_date, start_year = mon_year(tail)
        elif low.startswith(("completed", "completion")):
            tail = l.split(" ", 1)[1] if " " in l else (lines[i + 1] if i + 1 < len(lines) else "")
            _, end_year = mon_year(tail)
    # institution = first University/College line in the few lines AFTER the PI
    if researcher_idx is not None:
        for j in range(researcher_idx + 1, min(researcher_idx + 6, len(lines))):
            cand = lines[j].strip()
            if INST_RE.search(cand) and len(cand) < 60 and cand.lower() != "institute":
                institution = cand
                break
    given, family = parse_pi(pi)
    return {
        "funder_award_id": f"bruk-{slug}",
        "title": title,
        "pi_given": given, "pi_family": family,
        "institution": institution,
        "programme": ftype,
        "start_date": start_date, "start_year": start_year, "end_year": end_year,
        "landing_page_url": None,
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
    ap = argparse.ArgumentParser(description="Brain Research UK grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/brain_research_uk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-grants", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Brain Research UK research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    urls = get_urls(s)
    if args.max_grants:
        urls = urls[:args.max_grants]
    print(f"Project URLs: {len(urls)}")
    if len(urls) < 40 and not args.max_grants:
        print(f"[ERROR] only {len(urls)} — sitemap change?")
        sys.exit(1)

    rows, seen = [], set()
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
            print(f"  FAIL {slug}")
            continue
        rec = parse_detail(html, slug)
        rec["landing_page_url"] = url
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.4)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "programme", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "brain_research_uk_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
