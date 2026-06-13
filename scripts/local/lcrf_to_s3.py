#!/usr/bin/env python3
"""
Lung Cancer Research Foundation (LCRF) to S3 Data Pipeline
==========================================================

Harvests LCRF funded grants via the WordPress REST API and uploads a parquet to
S3 for Databricks ingestion.

Data source: WordPress REST custom post type `grants`:
    https://www.lungcancerresearchfoundation.org/wp-json/wp/v2/grants?per_page=100&page=N
    (X-WP-Total ~441). Each record's `title.rendered` encodes
    "YEAR – PROGRAM – INSTITUTION – PI" (en-dash delimited); year also parsed
    from the leading token. Amounts/abstracts are not in the REST payload
    (abstract is on the detail page; amount unpublished) -> NULL (§6.7 waiver).
provenance `lcrf`, priority 334. F4320309412 (Path A).

Output: s3://openalex-ingest/awards/lcrf/lcrf_grants.parquet

Usage:
    python lcrf_to_s3.py [--limit N] [--skip-upload]

Author: OpenAlex Team
"""

import argparse
import html
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

API = "https://www.lungcancerresearchfoundation.org/wp-json/wp/v2/grants"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lcrf/lcrf_grants.parquet"
EXPECTED_MIN = 350
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

INST_KEY = re.compile(r"Universit|College|Institute|Hospital|School|Center|Centre|"
                      r"Cancer|Clinic|Foundation|Health|Medical|Laborator|NYU|MIT|UCLA|UCSF", re.I)
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor)\.?\s+", re.I)
DEGREE = re.compile(r",?\s*(MD|PhD|DPhil|MPH|MSc|MS|DO|PharmD|DVM|ScD|MBBS|FRCP|RN)\b\.?", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n:
        return None, None
    n = _TITLE_RE.sub("", n)
    n = DEGREE.sub("", n).strip(" .,")
    n = n.split(",")[0].strip()
    toks = n.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def parse_title(raw):
    """'YEAR – PROGRAM – INSTITUTION – PI' -> dict."""
    t = html.unescape(raw or "")
    parts = [p.strip() for p in re.split(r"\s[–—-]\s", t) if p.strip()]
    if len(parts) < 2:
        return {"year": None, "program": None, "institution": None, "pi": None, "title": clean(t)}
    year = None
    m = re.match(r"(20\d\d)", parts[0])
    if m:
        year = int(m.group(1))
        parts = parts[1:]
    # last part = PI unless it looks like an institution
    pi = institution = None
    if parts and INST_KEY.search(parts[-1]) and not DEGREE.search(parts[-1]):
        institution = parts[-1]
        program = " - ".join(parts[:-1])
    elif len(parts) >= 2:
        pi = parts[-1]
        institution = parts[-2]
        program = " - ".join(parts[:-2])
    else:
        program = parts[0]
    return {"year": year, "program": clean(program), "institution": clean(institution),
            "pi": clean(pi), "title": clean(t)}


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
    ap = argparse.ArgumentParser(description="LCRF grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/lcrf_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Lung Cancer Research Foundation grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    rows, seen = [], set()
    page, pages, non200 = 1, None, 0
    while True:
        r = s.get(API, params={"per_page": 100, "page": page}, timeout=60)
        if r.status_code != 200:
            if r.status_code == 400 and rows:
                break
            non200 += 1
            if non200 >= 5:
                raise RuntimeError(f"too many non-200s at page {page}")
            time.sleep(3)
            continue
        if pages is None:
            pages = int(r.headers.get("X-WP-TotalPages", "0") or 0)
        batch = r.json()
        if not batch:
            break
        for rec in batch:
            p = parse_title((rec.get("title") or {}).get("rendered"))
            slug = rec.get("slug") or str(rec.get("id"))
            if slug in seen:
                continue
            seen.add(slug)
            given, family = split_name(p["pi"])
            rows.append({
                "funder_award_id": f"lcrf-{slug}"[:120],
                "title": p["title"],
                "pi_given": given,
                "pi_family": family,
                "institution": p["institution"],
                "funder_scheme": p["program"],
                "start_year": p["year"],
                "landing_page_url": rec.get("link"),
            })
        print(f"  page {page}/{pages}: {len(rows)} grants")
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break
        if pages and page >= pages:
            break
        page += 1
        time.sleep(0.2)

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} grants — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).drop_duplicates(subset="funder_award_id").reset_index(drop=True)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year":
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "funder_scheme", "start_year"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "lcrf_grants.parquet"
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
