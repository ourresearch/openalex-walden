#!/usr/bin/env python3
"""
Breast Cancer Now to S3 Data Pipeline
=====================================

Scrapes Breast Cancer Now's individual research projects and uploads a parquet
to S3 for Databricks ingestion.

Data source: breastcancernow.org. The "individual research projects" listing is
    a server-rendered, paginated page:
    /our-research/research-centres-and-projects/individual-research-projects?page=<N>
    (12 detail links per page; ?page=0 == ?page=1 == default, all "page 1"; the
    final page carries 1 link; out-of-range pages clamp to the last page). The
    crawler accumulates links and stops after two consecutive pages add nothing
    new (no page count hardcoded). Total = ~73 unique detail pages.

    Each detail page (200, UA "Mozilla/5.0") exposes labelled blocks:
        Researcher        -> "Professor Sophia Karagiannis"
        Institutions      -> "Breast Cancer Now Research Unit, King's College London"
        Project cost      -> "£178,848 (1 year of a 5-year project)"
    Some entries (cohort studies, e.g. the Generations Study) have no project
    cost. No structured calendar start/end dates are published; the parenthetical
    after the cost is captured as a free-text duration. Slug = id.
    (Breast cancer funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/breast_cancer_now/breast_cancer_now_grants.parquet
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

BASE = "https://breastcancernow.org"
LIST_PATH = "/our-research/research-centres-and-projects/individual-research-projects"
HEADERS = {"User-Agent": "Mozilla/5.0"}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/breast_cancer_now/breast_cancer_now_grants.parquet"

# detail links live directly under the listing path (one extra slug segment)
DETAIL_RE = re.compile(
    r'href="(/our-research/research-centres-and-projects/'
    r'individual-research-projects/[^"#?]+)"'
)
_TITLE_RE = re.compile(
    r"^(Professor|Prof|Doctor|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I
)
# split a multi-researcher value and keep only the first named person
_FIRST_PI_RE = re.compile(r"\s+and\s+|,|&|;|/", re.I)
_AMOUNT_RE = re.compile(r"£\s*([\d,]+)")
_YEAR_RANGE_RE = re.compile(r"(20\d{2})\s*(?:-|–|—|to)\s*(20\d{2})")

RESEARCHER_LABELS = {"researcher", "researchers", "lead researcher",
                     "principal investigator", "research lead"}
INSTITUTION_LABELS = {"institution", "institutions", "host institution"}
COST_LABELS = {"project cost", "cost", "total cost", "award", "funding",
               "grant", "amount awarded"}


def parse_pi(raw):
    """First named researcher -> (given, family), honorific stripped."""
    if not raw:
        return None, None
    first = _FIRST_PI_RE.split(raw, 1)[0].strip().lstrip(":").strip()
    first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def value_after_label(lines, labels):
    """Value rendered on the line after a standalone label line."""
    for i, l in enumerate(lines):
        norm = l.strip().rstrip(":").lower()
        if norm in labels and i + 1 < len(lines):
            v = lines[i + 1].strip()
            if v:
                return v
    return None


def get_links(session):
    """Crawl every pagination page; stop after 2 consecutive no-new pages."""
    links, seen = [], set()
    consec_no_new = 0
    max_pages = 40  # safety cap; real listing is ~8 pages
    for page in range(max_pages + 1):
        url = f"{BASE}{LIST_PATH}?page={page}"
        try:
            r = session.get(url, timeout=40)
        except Exception:
            break
        if r.status_code != 200:
            break
        page_links = []
        for m in DETAIL_RE.findall(r.text):
            if m not in seen:
                page_links.append(m)
        new = [m for m in page_links if m not in seen]
        if new:
            for m in new:
                seen.add(m)
                links.append(BASE + m)
            consec_no_new = 0
        else:
            consec_no_new += 1
        if consec_no_new >= 2 and page >= 1:
            break
        time.sleep(0.5)
    return links


def parse_detail(html, url):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None

    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n")
             if l.strip()]

    researcher = value_after_label(lines, RESEARCHER_LABELS)
    institution = value_after_label(lines, INSTITUTION_LABELS)
    cost_line = value_after_label(lines, COST_LABELS)

    # amount: prefer the labelled cost line, else first £ figure on the page
    amount = None
    duration = None
    if cost_line:
        m = _AMOUNT_RE.search(cost_line)
        if m:
            amount = m.group(1).replace(",", "")
        paren = re.search(r"\(([^)]*)\)", cost_line)
        if paren:
            duration = paren.group(1).strip()
    if amount is None:
        for l in lines:
            m = _AMOUNT_RE.search(l)
            if m:
                amount = m.group(1).replace(",", "")
                break

    # start/end: only if an explicit calendar-year range is published (rare)
    start_date = end_date = None
    blob = " ".join(filter(None, [cost_line, researcher, title]))
    ym = _YEAR_RANGE_RE.search(blob)
    if ym:
        start_date, end_date = ym.group(1), ym.group(2)

    given, family = parse_pi(researcher)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    if not title:
        title = slug.replace("-", " ").strip().capitalize() or None
    return {
        "funder_award_id": f"bcn-{slug}",
        "title": title,
        "pi_given": given,
        "pi_family": family,
        "institution": institution,
        "amount": amount,
        "start_date": start_date,
        "end_date": end_date,
        "duration": duration,
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
    ap = argparse.ArgumentParser(description="Breast Cancer Now grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/bcn_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Breast Cancer Now individual research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(HEADERS)
    links = get_links(s)
    print(f"Listing: {len(links)} unique detail links")
    if len(links) < 50:
        print(f"[ERROR] only {len(links)} links — listing/pagination change?")
        sys.exit(1)

    rows, seen = [], set()
    for url in links:
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        rec = parse_detail(r.text, url)
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount"):
        pct = round(100 * df[c].notna().sum() / max(len(df), 1))
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({pct}%)")

    # shrink-guard: this listing should yield ~73 projects
    if len(df) < 60:
        print(f"[ERROR] only {len(df)} rows (expected ~73) — aborting before write.")
        sys.exit(1)

    out = args.output_dir / "breast_cancer_now_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
