#!/usr/bin/env python3
"""
Financial Markets Foundation for Children (Australia) to S3 Data Pipeline
========================================================================

Scrapes the FMFC funded-projects list (foundationforchildren.com.au) and uploads
a parquet to S3 for Databricks ingestion.

Data source: server-rendered HTML at
    https://www.foundationforchildren.com.au/where-your-money-goes/
Each project is a `div.wrap` containing a table row (project title via
`a.trigger`, grant recipient/institution, grant amount, term) plus a hidden
`div.window#item_<id>` whose `div.top` carries Application No and Chief
Investigator in `<strong>` tags. ~168 projects; amounts in AUD; CI (PI)
populated on ~67% (older projects omit it); no per-project year published.
provenance `fmfc`, priority 329. F4320323274 (Path A; dup F4320310185 merge-flag).

Output: s3://openalex-ingest/awards/fmfc/fmfc_projects.parquet

Usage:
    python fmfc_to_s3.py
    python fmfc_to_s3.py --limit 10
    python fmfc_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

from bs4 import BeautifulSoup

URL = "https://www.foundationforchildren.com.au/where-your-money-goes/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fmfc/fmfc_projects.parquet"
EXPECTED_MIN = 120
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Associate Professor|A/Prof|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    return s or None


def split_name(raw):
    """'Dr K Lycett' / 'Professor Anne-Louise Ponsonby - VIC' -> (given, family)."""
    n = clean(raw)
    if not n:
        return None, None
    n = re.sub(r"\s*[-–]\s*[A-Z]{2,3}$", "", n)  # drop trailing state code " - VIC"
    n = _TITLE_RE.sub("", n).strip()
    n = n.split(",")[0].strip()
    toks = n.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def parse_amount(raw):
    m = re.search(r"\$?\s*([\d,]+)", raw or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


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
    ap = argparse.ArgumentParser(description="FMFC projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fmfc_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Financial Markets Foundation for Children -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    soup = BeautifulSoup(s.get(URL, timeout=60).text, "html.parser")
    wraps = soup.select("div.wrap")
    print(f"project wraps: {len(wraps)}")

    rows, seen = [], set()
    for w in wraps:
        a = w.select_one("a.trigger")
        if not a:
            continue
        title = clean(a.get_text(" ", strip=True))
        if not title:
            continue
        tds = w.select("tr.trow td")
        institution = clean(tds[1].get_text(" ", strip=True)) if len(tds) > 1 else None
        amount = parse_amount(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else None
        term = clean(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else None
        win = w.select_one("div.window div.top")
        appno = ci = None
        if win:
            strongs = win.select("strong")
            appno = clean(strongs[0].get_text(" ", strip=True)) if len(strongs) > 0 else None
            ci = clean(strongs[1].get_text(" ", strip=True)) if len(strongs) > 1 else None
        given, family = split_name(ci)
        aid = f"fmfc-{slugify(appno) if appno else slugify(title)}"
        if aid in seen:
            aid = f"{aid}-{len(rows)}"
        seen.add(aid)
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "institution": institution,
            "amount": amount,
            "term": term,
            "pi_given": given,
            "pi_family": family,
            "landing_page_url": URL,
        })
        if args.limit and len(rows) >= args.limit:
            break

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    for c in df.columns:
        if c != "amount":
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "institution", "amount", "pi_family"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    if df.amount.notna().any():
        print(f"  amount AUD: min={df.amount.min():.0f} median={df.amount.median():.0f} max={df.amount.max():.0f}")
    out = args.output_dir / "fmfc_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
