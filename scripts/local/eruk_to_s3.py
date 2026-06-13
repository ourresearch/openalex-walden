#!/usr/bin/env python3
"""
Epilepsy Research UK / Epilepsy Research Institute UK to S3 Data Pipeline
========================================================================

Harvests the Epilepsy Research UK funded-project portfolio via the WordPress
REST API and uploads a parquet to S3 for Databricks ingestion.

Data source: WordPress REST custom post type `research_portfolio`:
    https://epilepsyresearch.org.uk/wp-json/wp/v2/research_portfolio?per_page=100&page=N
    (X-WP-Total ~178). REST gives title.rendered + link + the
    epilepsy-research-categories taxonomy. PI / institution / amount / abstract
    are only on the Elementor detail page with no clean labelled fields, so this
    ingest carries title + category; PI/institution/amount NULL (§6.7 waiver).
    The title is the award-linkage signal. provenance `eruk`, priority 335.
    F4320320012 (Path A).

Output: s3://openalex-ingest/awards/eruk/eruk_projects.parquet

Usage:
    python eruk_to_s3.py [--limit N] [--skip-upload]

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

API = "https://epilepsyresearch.org.uk/wp-json/wp/v2/research_portfolio"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/eruk/eruk_projects.parquet"
EXPECTED_MIN = 140
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", html.unescape(str(v))).strip()
    return s or None


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
    ap = argparse.ArgumentParser(description="Epilepsy Research UK portfolio to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/eruk_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Epilepsy Research UK research portfolio -> S3")
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
            slug = rec.get("slug") or str(rec.get("id"))
            if slug in seen:
                continue
            seen.add(slug)
            cats = rec.get("epilepsy-research-categories") or []
            rows.append({
                "funder_award_id": f"eruk-{slug}"[:120],
                "title": clean((rec.get("title") or {}).get("rendered")),
                "category_ids": ",".join(str(c) for c in cats) if cats else None,
                "landing_page_url": rec.get("link"),
            })
        print(f"  page {page}/{pages}: {len(rows)} projects")
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break
        if pages and page >= pages:
            break
        page += 1
        time.sleep(0.2)

    rows = [r for r in rows if r["title"]]
    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "category_ids"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "eruk_projects.parquet"
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
