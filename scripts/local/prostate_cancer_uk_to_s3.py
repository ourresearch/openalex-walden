#!/usr/bin/env python3
"""
Prostate Cancer UK to S3 Data Pipeline
======================================

Scrapes Prostate Cancer UK's funded research projects and uploads a parquet to
S3 for Databricks ingestion.

Data source: prostatecanceruk.org. The visible research index is a JS island
    (server-side-empty), so enumeration comes from sitemap.xml ->
    /research/research-we-fund/<grant-id> detail pages (~204; the URL slug IS
    the native grant reference, e.g. TLD-CAF25-011). Chrome UA used (index
    tolerates default but crawl verified with Chrome). Detail pages carry
    labeled fields with VARIANT spellings — Reference / Researcher(s) /
    Institution(s) / 'Intitution' (sic) / Institute / Award / Grant award /
    Duration — parsed tolerantly. Amounts GBP where present (~72%).

Output: s3://openalex-ingest/awards/prostate_cancer_uk/prostate_cancer_uk_grants.parquet

Usage:
    python prostate_cancer_uk_to_s3.py
    python prostate_cancer_uk_to_s3.py --skip-upload --max-grants 10

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

SITEMAP = "https://prostatecanceruk.org/sitemap.xml"
CHROME_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/prostate_cancer_uk/prostate_cancer_uk_grants.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
LABELS = {
    "reference": "reference",
    "researcher": "pi_raw", "researchers": "pi_raw",
    "researchers and institutions": "pi_raw",
    "institution": "institution", "institutions": "institution",
    "intitution": "institution", "institute": "institution",
    "award": "amount_raw", "grant award": "amount_raw",
    "duration": "duration",
}


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
    m = re.search(r"£\s*([\d,]+(?:\.\d+)?)\s*(million|m\b|k\b)?", raw, re.I)
    if not m:
        return None
    val = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    if suf.startswith("m"):
        val *= 1_000_000
    elif suf == "k":
        val *= 1_000
    return str(int(val))


def get_urls(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    locs = re.findall(r"<loc>([^<]+/research/research-we-fund/[^<]+)</loc>", r.text)
    return sorted(set(u for u in locs if not u.rstrip("/").endswith("research-we-fund")))


def parse_detail(html, url):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    for x in soup(["script", "style"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    fields = {}
    for i, l in enumerate(lines):
        # label may or may not have a trailing colon (e.g. "Reference:" vs "Researcher")
        key = l.split(":", 1)[0].rstrip(":").strip().lower()
        norm = LABELS.get(key)
        if not norm:
            continue
        # value: text after the colon on this line, else the next line
        val = l.split(":", 1)[1].strip() if ":" in l else ""
        if not val and i + 1 < len(lines):
            nxt = lines[i + 1]
            # don't grab the next line if it's itself a label
            if nxt.split(":", 1)[0].rstrip(":").strip().lower() not in LABELS:
                val = nxt
        val = re.sub(r"^[-–—•·]\s*", "", val).strip()  # strip leading dash/bullet
        if val and norm not in fields:
            fields[norm] = val
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    ref = fields.get("reference") or slug.upper()
    given, family = parse_pi(fields.get("pi_raw"))
    dur = fields.get("duration") or ""
    sy = ey = None
    ym = re.findall(r"(20\d{2})", dur)
    if ym:
        sy = ym[0]
        ey = ym[-1] if len(ym) > 1 else None
    return {
        "funder_award_id": ref,
        "title": title,
        "pi_given": given,
        "pi_family": family,
        "institution": fields.get("institution"),
        "amount": parse_amount(fields.get("amount_raw")),
        "start_year": sy,
        "end_year": ey,
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
    ap = argparse.ArgumentParser(description="Prostate Cancer UK grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/prostate_cancer_uk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-grants", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Prostate Cancer UK research grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": CHROME_UA})
    urls = get_urls(s)
    if args.max_grants:
        urls = urls[:args.max_grants]
    print(f"Detail URLs: {len(urls)}")
    if len(urls) < 150 and not args.max_grants:
        print(f"[ERROR] only {len(urls)} urls — sitemap layout change?")
        sys.exit(1)

    rows, seen, failures = [], set(), 0
    for n, url in enumerate(urls, 1):
        html = None
        for _ in (1, 2, 3):
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
            print(f"  [{n}] FAIL {url.rsplit('/',1)[-1]}")
            if failures > 20:
                print("[ERROR] >20 failures; STOP to avoid partial ship")
                sys.exit(1)
            continue
        rec = parse_detail(html, url)
        if rec["funder_award_id"] in seen:
            rec["funder_award_id"] += f"-{n}"
        seen.add(rec["funder_award_id"])
        rows.append(rec)
        if n % 50 == 0:
            print(f"  [{n}/{len(urls)}] {len(rows)} grants")
        time.sleep(0.35)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "prostate_cancer_uk_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
