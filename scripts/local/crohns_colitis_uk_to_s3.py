#!/usr/bin/env python3
"""
Crohn's & Colitis UK to S3 Data Pipeline
========================================

Scrapes Crohn's & Colitis UK's funded research projects and uploads a parquet to
S3 for Databricks ingestion.

Data source: crohnsandcolitis.org.uk. The "projects we have funded" page is a Vue
    listing backed by an Umbraco JSON API:
    /umbraco/api/listingapi/getlisting?parent=20985&page=<N> (27 projects, 3
    pages). Each result's `url` is a detail page carrying labelled fields. Default
    UA fine. PI + institution + GBP amount + duration are available. Slug = id.
    (IBD funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/crohns_colitis_uk/crohns_colitis_uk_grants.parquet
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

BASE = "https://www.crohnsandcolitis.org.uk"
LIST_API = BASE + "/umbraco/api/listingapi/getlisting?parent=20985&page={}&tags=&category=&sort="
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/crohns_colitis_uk/crohns_colitis_uk_grants.parquet"

INST_RE = re.compile(r"(University|College|Institute|Imperial|King'?s|Hospital|"
                     r"School of|UCL|Queen Mary|Trust|Centre)", re.I)
_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
PI_LABELS = ("who is leading this research", "who is leading the research",
             "research lead", "lead researcher")


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r",| at | from ", raw)[0].strip().lstrip(":").strip()
    first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def get_listing(session):
    items = []
    for pg in range(1, 6):
        r = session.get(LIST_API.format(pg), timeout=40)
        if r.status_code != 200:
            break
        d = r.json()
        results = d.get("results") or d.get("items") or []
        for it in results:
            url = it.get("url")
            if url:
                items.append({"url": BASE + url if url.startswith("/") else url,
                              "title": it.get("heading")})
        if pg >= (d.get("numPages") or 1):
            break
        time.sleep(0.3)
    return items


def parse_detail(html, fallback_title, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = (h1.get_text(" ", strip=True) if h1 else None) or fallback_title
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    pi = institution = amount = duration = None
    for i, l in enumerate(lines):
        low = l.lower().rstrip(":")
        if any(low == lab or low.startswith(lab) for lab in PI_LABELS):
            v = l.split(":", 1)[1].strip() if ":" in l else ""
            pi = pi or (v or (lines[i + 1] if i + 1 < len(lines) else None))
        if institution is None and INST_RE.search(l) and len(l) < 70 \
                and not low.startswith(("who is", "our funding", "duration")):
            # "Dr X, University of Y" or a standalone institution line
            m = re.search(r",\s*(.+(University|College|Institute|Hospital|Trust|Centre).*)$", l)
            institution = (m.group(1) if m else l).strip()
        if amount is None:
            am = re.search(r"£\s*([\d,]+(?:\.\d+)?)", l)
            if am:
                amount = str(int(float(am.group(1).replace(",", ""))))
        if low.startswith("duration"):
            duration = (l.split(":", 1)[1].strip() if ":" in l else
                        (lines[i + 1] if i + 1 < len(lines) else None))
    given, family = parse_pi(pi)
    return {
        "funder_award_id": f"ccuk-{slug}",
        "title": title,
        "pi_given": given, "pi_family": family,
        "institution": institution,
        "amount": amount,
        "duration": duration,
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
    ap = argparse.ArgumentParser(description="Crohn's & Colitis UK grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ccuk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Crohn's & Colitis UK funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    items = get_listing(s)
    print(f"Listing: {len(items)} projects")
    if len(items) < 15:
        print(f"[ERROR] only {len(items)} — API change?")
        sys.exit(1)

    rows, seen = [], set()
    for it in items:
        slug = it["url"].rstrip("/").rsplit("/", 1)[-1]
        try:
            r = s.get(it["url"], timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        rec = parse_detail(r.text, it["title"], slug)
        rec["landing_page_url"] = it["url"]
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.3)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "crohns_colitis_uk_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
