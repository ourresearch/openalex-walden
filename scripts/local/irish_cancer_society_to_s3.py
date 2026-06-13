#!/usr/bin/env python3
"""
Irish Cancer Society to S3 Data Pipeline
========================================

Scrapes the Irish Cancer Society's currently funded research projects and
uploads a parquet to S3 for Databricks ingestion.

Data source: cancer.ie/about-our-research/our-current-research. Server-rendered
    "Current Projects" accordion (plain requests, UA Mozilla/5.0): one
    div.team-item per entry with h3.team-item-title = "PI[, institution]",
    div.team-item-project = "scheme [year]", div.team-item-teaser-text = prose
    description (used as the title; an embedded quoted 'titled "..."' string is
    preferred when present). 13 highlighted projects; the page's "100+
    researchers" claim is not enumerated anywhere. No amounts on the page
    (per-funder NULL waiver, runbook section 6.7). IAMHRF funder; F4320320839.

Output: s3://openalex-ingest/awards/irish_cancer_society/irish_cancer_society_grants.parquet
"""

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.cancer.ie"
LIST_URL = BASE + "/about-our-research/our-current-research"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/irish_cancer_society/irish_cancer_society_grants.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
QUOTED_TITLE_RE = re.compile(r"\btitled?\b[,:]?\s*[\"'‘“](.+?)[\"'’”]", re.S)


def norm(s):
    return re.sub(r"\s+", " ", s or "").strip()


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


def slugify(s):
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def parse_item(item):
    h3 = item.select_one(".team-item-title")
    proj = item.select_one(".team-item-project")
    teaser = item.select_one(".team-item-teaser-text")
    if h3 is None:
        return None
    raw_pi = norm(h3.get_text(" ", strip=True))
    # drop parentheticals ("(Advanced Nurse Practitioner ...)"), keep first PI
    name_line = norm(re.sub(r"\([^)]*\)", "", raw_pi))
    name_part, _, inst_part = (p.strip() for p in name_line.partition(","))
    lead = re.split(r"\s+and\s+|\s*&\s*", name_part)[0].strip()
    given, family = parse_pi(lead)

    scheme_line = norm(proj.get_text(" ", strip=True)) if proj else ""
    scheme_clean = norm(re.sub(r"\([^)]*\)", "", scheme_line))
    ym = YEAR_RE.search(scheme_clean)
    year = ym.group(1) if ym else None
    scheme = norm(YEAR_RE.sub("", scheme_clean)) or None

    desc = norm(teaser.get_text(" ", strip=True)) if teaser else ""
    qm = QUOTED_TITLE_RE.search(desc)
    title = norm(qm.group(1)) if qm else (desc or None)
    if not title:  # last resort: name the award after PI + scheme
        title = norm(f"{scheme or 'Research project'} — {lead}")

    a = item.find("a", href=True)
    link = a["href"] if a else None
    slug = slugify(f"{given or ''} {family or ''}") or \
        hashlib.md5(raw_pi.encode("utf-8")).hexdigest()[:10]
    return {
        "funder_award_id": f"ics-{slug}",
        "title": title,
        "pi_given": given, "pi_family": family,
        "institution": inst_part or None,
        "scheme": scheme,
        "year_awarded": year,
        "landing_page_url": (BASE + link) if (link or "").startswith("/") else (link or LIST_URL),
        "_raw": raw_pi + "|" + scheme_line,
    }


def scrape(session):
    r = session.get(LIST_URL, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.select("div.researchers-item-team div.team-item") or \
        soup.select("div.team-item")
    rows, seen = [], set()
    for item in items:
        rec = parse_item(item)
        if not rec:
            continue
        if rec["funder_award_id"] in seen:  # same-name PI twice -> hash suffix
            rec["funder_award_id"] += "-" + hashlib.md5(
                rec.pop("_raw").encode("utf-8")).hexdigest()[:6]
        else:
            rec.pop("_raw")
        if rec["funder_award_id"] in seen:
            continue
        seen.add(rec["funder_award_id"])
        rows.append(rec)
    return rows


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
    ap = argparse.ArgumentParser(description="Irish Cancer Society grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ics_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Irish Cancer Society funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = scrape(s)
    print(f"Parsed: {len(rows)} projects")
    if len(rows) < 10:
        print(f"[ERROR] only {len(rows)} — page change?")
        sys.exit(1)

    cols = ["funder_award_id", "title", "pi_given", "pi_family", "institution",
            "scheme", "year_awarded", "landing_page_url"]
    df = pd.DataFrame(rows)[cols].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "scheme", "year_awarded"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "irish_cancer_society_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
