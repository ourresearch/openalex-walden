#!/usr/bin/env python3
"""
Foundation for Baltic and East European Studies (Östersjöstiftelsen) to S3
==========================================================================

Harvests the Östersjöstiftelsen funded-project database (ostersjostiftelsen.se)
via its WordPress REST API and uploads a parquet to S3 for Databricks ingestion.

Data source: WordPress REST custom post type `project`:
    https://www.ostersjostiftelsen.se/wp-json/wp/v2/project?per_page=100&page=N
    (X-WP-Total ~426; English titles + abstracts on the .se host)
Each record gives id, slug, link, title.rendered, content.rendered (abstract).

LIMITATION: the per-project Principal Investigator, research field, project type,
and grant year are NOT exposed in wp-json (acf is empty, no taxonomies) and on
the site they render only inside a global filter widget / JS archive — not
per-project. So this ingest carries title + abstract only; PI, year, institution
and amount are NULL. The title is the award-linkage signal. provenance
`ostersjostiftelsen`, priority 327. F4320310975 (Path A).

Output: s3://openalex-ingest/awards/ostersjostiftelsen/ostersjostiftelsen_projects.parquet

Usage:
    python ostersjostiftelsen_to_s3.py
    python ostersjostiftelsen_to_s3.py --limit 10
    python ostersjostiftelsen_to_s3.py --skip-upload

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

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

from bs4 import BeautifulSoup

API = "https://www.ostersjostiftelsen.se/wp-json/wp/v2/project"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ostersjostiftelsen/ostersjostiftelsen_projects.parquet"
PER_PAGE = 100
EXPECTED_MIN = 350
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MAX_NON200 = 5


def clean_html(s):
    if not s:
        return None
    txt = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt or None


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
    ap = argparse.ArgumentParser(description="Östersjöstiftelsen projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ostersjostiftelsen_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Östersjöstiftelsen projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    rows, seen = [], set()
    page, total_pages, non200 = 1, None, 0
    while True:
        try:
            r = s.get(API, params={"per_page": PER_PAGE, "page": page}, timeout=60)
        except Exception as e:
            non200 += 1
            print(f"  page {page}: error {e} ({non200}/{MAX_NON200})")
            if non200 >= MAX_NON200:
                raise
            time.sleep(3)
            continue
        if r.status_code != 200:
            # WP returns 400 past the last page — that's the terminator only if
            # we've already collected pages; otherwise treat as a flake.
            if r.status_code == 400 and rows:
                print(f"  page {page}: HTTP 400 (past last page) — stopping")
                break
            non200 += 1
            print(f"  page {page}: HTTP {r.status_code} ({non200}/{MAX_NON200})")
            if non200 >= MAX_NON200:
                raise RuntimeError(f"too many non-200s at page {page}")
            time.sleep(3)
            continue
        non200 = 0
        if total_pages is None:
            total_pages = int(r.headers.get("X-WP-TotalPages", "0") or 0)
            print(f"  X-WP-Total={r.headers.get('X-WP-Total')} pages={total_pages}")
        batch = r.json()
        if not batch:
            break
        for rec in batch:
            slug = rec.get("slug") or str(rec.get("id"))
            if slug in seen:
                continue
            seen.add(slug)
            rows.append({
                "funder_award_id": f"osg-{slug}"[:120],
                "title": clean_html((rec.get("title") or {}).get("rendered")),
                "description": clean_html((rec.get("content") or {}).get("rendered")),
                "landing_page_url": rec.get("link"),
            })
        print(f"  page {page}: +{len(batch)} ({len(rows)} total)")
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break
        if total_pages and page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    rows = [r for r in rows if r["title"]]
    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "description"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "ostersjostiftelsen_projects.parquet"
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
