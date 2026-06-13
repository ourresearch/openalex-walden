#!/usr/bin/env python3
"""
Versus Arthritis to S3 Data Pipeline
====================================

Scrapes Versus Arthritis's live research projects and uploads a parquet to S3
for Databricks ingestion.

Data source: arthritis-uk.org (the current Versus Arthritis domain; the old
    versusarthritis.org 403s plain requests behind Imperva). The
    live-research-projects listing is an A-Z-by-condition server-side filter
    (?alphabeticIndex=<LETTER>); the unfiltered page paints only ~7 cards, so
    all letters are walked (carrying letters A B C F J M O P R S V = 61 unique
    projects as of 2026-06-12). Detail pages carry labelled fields, label and
    value on separate text lines with a "- "/"- " (hyphen/en-dash) value
    prefix: Reference (native id), Lead applicant, Organisation, Type of grant,
    Status of grant, Amount of the original award (GBP), Start date.
    Mozilla/5.0 UA, plain requests. Live portfolio only (no completed/archive
    listing exists). (IAMHRF-list funder, F4320327444.)

Output: s3://openalex-ingest/awards/versus_arthritis/versus_arthritis_grants.parquet
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
from string import ascii_uppercase

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.arthritis-uk.org"
LIST_URL = BASE + "/our-research/research-we-fund/live-research-projects/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/versus_arthritis/versus_arthritis_grants.parquet"
UA = {"User-Agent": "Mozilla/5.0"}

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
_SEP_RE = re.compile(r"^[\s\-–—:]+")  # hyphen / en-dash / em-dash / colon
LABELS = {  # detail-page label (lowercased) -> field key
    "lead applicant": "pi_raw",
    "organisation": "institution",
    "type of grant": "scheme",
    "status of grant": "status",
    "amount of the original award": "amount_raw",
    "start date": "start_date_raw",
    "reference": "reference",
}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r",| and | at | from ", raw)[0].strip().lstrip(":").strip()
    first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def get_listing(session):
    urls = set()
    for letter in ascii_uppercase:  # walk all letters; empty ones just repaint defaults
        try:
            r = session.get(LIST_URL, params={"alphabeticIndex": letter}, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "live-research-projects/" not in h:
                continue
            tail = h.split("live-research-projects/")[1].strip("/")
            if tail and "/" not in tail and "?" not in tail:
                urls.add(h if h.startswith("http") else BASE + h)
        time.sleep(0.3)
    return sorted(urls)


def parse_detail(html, url, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    fields = {}
    for i, line in enumerate(lines):
        low = line.lower()
        for label, key in LABELS.items():
            if key in fields or not low.startswith(label):
                continue
            rest = line[len(label):]
            if rest and rest[0] not in " \t-–—:":  # e.g. "References" prose
                continue
            val = _SEP_RE.sub("", rest).strip()  # inline "Label - value" variant
            if not val and i + 1 < len(lines):  # label/value on separate lines
                nxt = lines[i + 1]
                if not any(nxt.lower().startswith(l2) for l2 in LABELS):
                    val = _SEP_RE.sub("", nxt).strip()
            if key == "reference" and (not val or " " in val
                                       or not any(c.isdigit() for c in val)):
                continue  # guard against prose "reference ..." lines
            if val:
                fields[key] = val
            break

    amount = None
    if fields.get("amount_raw"):
        m = re.search(r"£\s*([\d,]+(?:\.\d+)?)", fields["amount_raw"])
        if m:
            amount = str(int(float(m.group(1).replace(",", ""))))
    given, family = parse_pi(fields.get("pi_raw"))
    return {
        "funder_award_id": fields.get("reference") or f"va-{slug}",
        "title": title,
        "pi_given": given, "pi_family": family,
        "institution": fields.get("institution"),
        "amount": amount,
        "start_date_raw": fields.get("start_date_raw"),
        "status": fields.get("status"),
        "scheme": fields.get("scheme"),
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
    ap = argparse.ArgumentParser(description="Versus Arthritis grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/va_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Versus Arthritis live research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(UA)
    urls = get_listing(s)
    print(f"Listing: {len(urls)} projects")
    if len(urls) < 50:
        print(f"[ERROR] only {len(urls)} — listing change? (expect ~61)")
        sys.exit(1)

    rows, seen = [], set()
    for url in urls:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        rec = parse_detail(r.text, url, slug)
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.5)

    if len(rows) < 50:
        print(f"[ERROR] only {len(rows)} parsed rows — detail-page change? (expect ~61)")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    native = int(df["funder_award_id"].str.fullmatch(r"\d+").sum())
    print(f"  native Reference id: {native}/{len(df)} ({round(100*native/max(len(df),1))}%)")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw",
              "status", "scheme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "versus_arthritis_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
