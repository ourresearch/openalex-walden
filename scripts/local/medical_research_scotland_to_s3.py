#!/usr/bin/env python3
"""
Medical Research Scotland to S3 Data Pipeline
=============================================

Scrapes Medical Research Scotland's awarded PhD studentships / fellowships and
uploads a parquet to S3 for Databricks ingestion.

Data source: medicalresearchscotland.org.uk. Award detail URLs come from the
    Yoast sitemap https://medicalresearchscotland.org.uk/awards-sitemap.xml
    (~503 /awards/<slug> pages). Default UA fine. Detail pages carry the award
    body with labels `Student:` (the awardee), `Supervisors:`, `Year Award
    Started:`, plus the host institution as a "University of …" line; the page
    also has nav clutter (`close`, menu items) that must be excluded. Amounts
    are essentially not published (section 6.7 waiver). Slug = native id.

Output: s3://openalex-ingest/awards/medical_research_scotland/mrs_awards.parquet

Usage:
    python medical_research_scotland_to_s3.py
    python medical_research_scotland_to_s3.py --skip-upload --max-grants 10

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

SITEMAP = "https://medicalresearchscotland.org.uk/awards-sitemap.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/medical_research_scotland/mrs_awards.parquet"

INST_RE = re.compile(r"(University|College|Institute|Royal|NHS|Hospital|School of|"
                     r"Abertay|Heriot-Watt|Strathclyde|Napier|Robert Gordon)", re.I)
_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
NAV = {"awards", "phd studentships", "close", "award rates", "research funding strategy",
       "scientific publications", "undergraduate vacation scholarships", "past award",
       "current award", "/", "menu", "home", "about", "contact"}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&|,", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def val_after(lines, i, label):
    """value on the same line after `label`, else next non-empty line."""
    l = lines[i]
    v = l.split(label, 1)[1] if label in l else ""
    v = v.strip().strip(":").strip()
    if not v and i + 1 < len(lines):
        nxt = lines[i + 1].strip()
        if nxt.lower() not in NAV:
            v = nxt
    return v or None


def parse_detail(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [re.sub(r"\s+", " ", l).strip()
             for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    student = supervisor = year = institution = None
    anchor = None
    for i, l in enumerate(lines):
        low = l.lower()
        if low.startswith("student:"):
            student = val_after(lines, i, "Student:")
            if student and ":" in student:  # format "Student: Name : University"
                student = student.split(":")[0].strip()
            anchor = anchor if anchor is not None else i
        elif low.startswith("supervisor"):
            supervisor = val_after(lines, i, ":") or val_after(lines, i, "Supervisors")
            anchor = anchor if anchor is not None else i
        elif low.startswith("year award started"):
            m = re.search(r"(19|20)\d{2}", l) or (i + 1 < len(lines) and re.search(r"(19|20)\d{2}", lines[i + 1]))
            if m:
                year = m.group(0)
    # institution: nearest University-line above the anchor (award body), skipping nav
    if anchor is not None:
        for j in range(anchor, max(-1, anchor - 8), -1):
            cand = lines[j].strip()
            if cand.lower() in NAV or len(cand) > 70:
                continue
            if INST_RE.search(cand) and not cand.lower().startswith(("student", "supervisor", "year")):
                institution = cand
                break
    if institution is None:
        for l in lines:
            if INST_RE.search(l) and l.lower() not in NAV and len(l) < 70:
                institution = l.strip()
                break

    pi = student or supervisor
    given, family = parse_pi(pi)
    return {
        "funder_award_id": f"mrs-{slug}",
        "title": title,
        "pi_given": given, "pi_family": family,
        "institution": institution,
        "supervisor": supervisor if student else None,
        "start_year": year,
        "landing_page_url": None,
    }


def get_urls(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    locs = re.findall(r"<loc>([^<]+/awards/[^<]+)</loc>", r.text)
    return sorted(set(u for u in locs if not u.rstrip("/").endswith("/awards")))


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
    ap = argparse.ArgumentParser(description="Medical Research Scotland awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/mrs_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-grants", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Medical Research Scotland awards -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    urls = get_urls(s)
    if args.max_grants:
        urls = urls[:args.max_grants]
    print(f"Award URLs: {len(urls)}")

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
            if failures > 20:
                print("[ERROR] >20 failures; STOP")
                sys.exit(1)
            continue
        rec = parse_detail(html, slug)
        rec["landing_page_url"] = url
        if rec["funder_award_id"] not in seen and rec["title"]:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        if n % 100 == 0:
            print(f"  [{n}/{len(urls)}] {len(rows)} awards")
        time.sleep(0.3)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "mrs_awards.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
