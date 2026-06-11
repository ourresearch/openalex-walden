#!/usr/bin/env python3
"""
California Institute for Regenerative Medicine (CIRM) to S3 Data Pipeline
========================================================================

Scrapes CIRM's public awards database and uploads a parquet to S3 for
Databricks ingestion.

Data source: cirm.ca.gov. Grant URLs are enumerated from the WordPress sitemap
    https://www.cirm.ca.gov/wp-sitemap-posts-grant-1.xml (1,575 awards under
    /our-progress/awards/{slug}/), then each server-rendered detail page is
    parsed. No JSON/CSV export exists (grant post type is not in WP REST). No UA
    gating, no WAF — default UA returns full pages.

Per-grant fields (Elementor markup, parsed as label:value line pairs): native
    Grant Number, title (H1), lead Investigator name + Institution, Award Value
    (USD), Award Type (scheme). Explicit start/end dates are not published
    (status only), so dates are left NULL. ~2006-present; state agency, rich.

Output: s3://openalex-ingest/awards/cirm/cirm_grants.parquet

Usage:
    python cirm_to_s3.py
    python cirm_to_s3.py --skip-upload --max-grants 20

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

SITEMAP = "https://www.cirm.ca.gov/wp-sitemap-posts-grant-1.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cirm/cirm_grants.parquet"
REQUEST_DELAY = 0.35

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def parse_pi(pi_raw):
    if not pi_raw:
        return None, None
    first = re.split(r";| and |&|,", pi_raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|MBA|DVM|DPhil|ScD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?",
                   "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def parse_amount(raw):
    if not raw:
        return None
    m = re.search(r"[\d,]+", raw.replace("$", ""))
    return str(int(m.group(0).replace(",", ""))) if m else None


def page_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script", "style", "nav", "header", "footer"]):
        s.extract()
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    lines = [l for l in re.sub(r"\n\s*\n+", "\n",
             soup.get_text("\n", strip=True)).split("\n") if l.strip()]
    return title, lines


def val_after(lines, label, within=3):
    """First non-empty line after a line equal to `label`."""
    for i, l in enumerate(lines):
        if l.strip().rstrip(":") == label.rstrip(":"):
            for j in range(i + 1, min(i + 1 + within, len(lines))):
                if lines[j].strip():
                    return lines[j].strip()
    return None


def parse_grant(html, slug):
    title, lines = page_lines(html)
    grant_no = val_after(lines, "Grant Number:")
    award_value = val_after(lines, "Award Value:")
    status = val_after(lines, "Status:")
    # investigator block: first Name: after Investigator(s):, with its Institution:
    pi = inst = None
    for i, l in enumerate(lines):
        if l.strip().startswith("Investigator"):
            blk = lines[i:i + 12]
            pi = val_after(blk, "Name:")
            inst = val_after(blk, "Institution:")
            break
    if inst is None:
        inst = val_after(lines, "Institution:")
    # Award Type = the line directly above "Grant Number:" (e.g. "Basic Biology I")
    scheme = None
    for i, l in enumerate(lines):
        if l.strip() == "Grant Number:" and i > 0:
            cand = lines[i - 1].strip()
            if cand and not cand.endswith(":") and len(cand) < 60:
                scheme = cand
            break
    pi_given, pi_family = parse_pi(pi)
    return {
        "funder_award_id": grant_no or f"cirm-{slug}",
        "title": title,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": inst,
        "amount": parse_amount(award_value),
        "programme": scheme,
        "status": status,
    }


def get_grant_urls(session):
    r = session.get(SITEMAP, timeout=60)
    r.raise_for_status()
    return re.findall(r"<loc>([^<]+/our-progress/awards/[^<]+)</loc>", r.text)


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
    ap = argparse.ArgumentParser(description="CIRM awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cirm_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-grants", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CIRM awards -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    urls = get_grant_urls(s)
    if args.max_grants:
        urls = urls[:args.max_grants]
    print(f"Grant URLs: {len(urls)}")

    rows, seen, failures = [], set(), 0
    for n, url in enumerate(urls, 1):
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        html = None
        for attempt in (1, 2):
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
            print(f"  [{n}] FAIL {slug}")
            if failures > 50:
                print("[ERROR] >50 fetch failures; STOP to avoid partial ship")
                sys.exit(1)
            continue
        rec = parse_grant(html, slug)
        if rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        if n % 100 == 0:
            print(f"  [{n}/{len(urls)}] {len(rows)} grants, {failures} failures")
        time.sleep(REQUEST_DELAY)

    expected = len(urls)
    if len(rows) < expected * 0.9:
        print(f"[ERROR] only {len(rows)}/{expected} grants (<90%); refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("pi_family", "institution", "amount", "programme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "cirm_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
