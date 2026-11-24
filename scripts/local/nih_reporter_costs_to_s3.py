#!/usr/bin/env python3
"""
NIH RePORTER costs for ExPORTER's FY1985-1999 years, to S3
===========================================================

NIH ExPORTER's project files for fiscal years 1985-1999 carry no cost (0 of 647,231 project-year rows on 21 Nov 2026), so an NIH
project that ran in those years would show only its FY2000-onward money. NIH RePORTER, NIH's own database behind ExPORTER, does
publish the cost of those applications. This script asks RePORTER for the cost of every FY1985-1999 ExPORTER application that has
none, by application id, and writes one parquet that CreateNIHAwards (Step 1b) reads. Those fiscal years are closed, so the file is
pulled once; re-running it re-uses the cache and asks only for ids it has not asked yet (oxjob #2326, option C).

Input:  nih_projects_combined.parquet, the output of nih_exporter_to_s3.py
Output: s3://openalex-ingest/awards/nih/nih_reporter_costs.parquet
        columns application_id (string), fiscal_year (int), project_num, award_amount (double, USD), fetched_at (UTC date)

RePORTER API (https://api.reporter.nih.gov): one request at a time, at least 1.1 s apart (NIH asks for at most one a second, and for
large jobs at weekends or 9 PM-5 AM Eastern); 500 application ids per request; about 1,300 requests, 30-50 minutes.

Usage:
    python nih_reporter_costs_to_s3.py --projects-parquet ./nih_data/nih_projects_combined.parquet --output-dir ./nih_data
    python nih_reporter_costs_to_s3.py ... --skip-upload          # build the parquet only
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

FIRST_FY, LAST_FY = 1985, 1999          # the fiscal years ExPORTER publishes without a cost
BATCH = 500                             # application ids per request (RePORTER returns at most 500 records a request)
MIN_INTERVAL_S = 1.1
API_URL = "https://api.reporter.nih.gov/v2/projects/search"
USER_AGENT = "openalex-walden nih_reporter_costs_to_s3"
FIELDS = ["ApplId", "FiscalYear", "ProjectNum", "SubprojectId", "AwardAmount"]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nih/nih_reporter_costs.parquet"


def needed_application_ids(projects: pd.DataFrame) -> list[str]:
    """Every FY1985-1999 project-year row (not a sub-project) whose ExPORTER cost is blank, as sorted application ids."""
    fy = pd.to_numeric(projects["fy"], errors="coerce")
    sub = projects["subproject_id"].fillna("").astype(str).str.strip()
    cost = projects["total_cost"].fillna("").astype(str).str.strip()
    keep = fy.between(FIRST_FY, LAST_FY) & (sub == "") & (cost == "")
    ids = projects.loc[keep, "application_id"].dropna().astype(str).str.strip()
    return sorted(set(ids[ids != ""]), key=int)


def read_cache(cache: Path) -> tuple[set[str], list[dict]]:
    asked, results = set(), []
    if cache.exists():
        lines = cache.read_text().splitlines()
        for i, line in enumerate(lines):
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                if i == len(lines) - 1:      # a run killed mid-write: that batch is asked again
                    print("  [CACHE] last line is incomplete; its batch will be asked again")
                    break
                raise
            asked.update(str(x) for x in d["asked"])
            results.extend(d["results"])
    return asked, results


def fetch(ids: list[str], cache: Path, session=None, sleep=time.sleep, clock=time.time) -> None:
    """Ask RePORTER for the ids not yet in the cache, BATCH per request, one request at a time, MIN_INTERVAL_S apart."""
    asked, _ = read_cache(cache)
    todo = [i for i in ids if i not in asked]
    if cache.exists() and cache.read_text() and not cache.read_text().endswith("\n"):   # drop an incomplete last line before appending
        cache.write_text(cache.read_text().rsplit("\n", 1)[0] + "\n" if "\n" in cache.read_text() else "")
    short = 0
    session = session or requests.Session()
    last = 0.0
    with cache.open("a") as f:
        for b in range(0, len(todo), BATCH):
            chunk = todo[b:b + BATCH]
            body = {"criteria": {"appl_ids": [int(x) for x in chunk], "exclude_subprojects": True},
                    "include_fields": FIELDS, "offset": 0, "limit": BATCH}
            for attempt in range(6):
                wait = MIN_INTERVAL_S - (clock() - last)
                if wait > 0:
                    sleep(wait)
                last = clock()
                try:
                    r = session.post(API_URL, json=body, headers={"User-Agent": USER_AGENT}, timeout=120)
                    r.raise_for_status()
                    d = r.json()
                    break
                except requests.RequestException as e:
                    print(f"  [RETRY] batch {b // BATCH}: {e}")
                    if attempt < 5:
                        sleep(min(300, 10 * 2 ** attempt))
            else:
                sys.exit(f"[ERROR] batch {b // BATCH}: RePORTER failed 6 times; re-run to resume from the cache")
            if d["meta"]["total"] > BATCH:
                sys.exit(f"[ERROR] batch {b // BATCH}: {d['meta']['total']} records for {len(chunk)} ids; the id filter was ignored")
            f.write(json.dumps({"asked": chunk, "results": d["results"]}) + "\n")
            f.flush()
            if len(d["results"]) < len(chunk):      # ids RePORTER has no record for stay without a cost (their projects stay blank)
                short += len(chunk) - len(d["results"])
                if len(d["results"]) < 0.9 * len(chunk):
                    print(f"  [SHORT] batch {b // BATCH}: {len(d['results'])} records for {len(chunk)} ids")
            if (b // BATCH) % 50 == 0:
                print(f"  [FETCH] {b + len(chunk):,} of {len(todo):,} ids")
    print(f"  [FETCH] done; {short:,} of {len(todo):,} ids asked this run have no RePORTER record")


def build_rows(ids: list[str], results: list[dict], fetched_at: str) -> pd.DataFrame:
    """One row per asked application id that RePORTER returned with a cost; never a sub-project; a repeated id must agree."""
    want, rows = set(ids), {}
    for x in results:
        k = str(x.get("appl_id"))
        if k not in want or (x.get("subproject_id") or "").strip():
            continue
        row = {"application_id": k, "fiscal_year": x.get("fiscal_year"), "project_num": x.get("project_num"),
               "award_amount": x.get("award_amount"), "fetched_at": fetched_at}
        if k in rows and (rows[k]["award_amount"], rows[k]["fiscal_year"]) != (row["award_amount"], row["fiscal_year"]):
            raise ValueError(f"RePORTER returned two different costs for application {k}")
        rows[k] = row
    df = pd.DataFrame(list(rows.values()), columns=["application_id", "fiscal_year", "project_num", "award_amount", "fetched_at"])
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
    df["award_amount"] = pd.to_numeric(df["award_amount"], errors="coerce").astype("float64")
    return df[df["award_amount"].notna()].sort_values("application_id", key=lambda s: s.astype(int)).reset_index(drop=True)


def upload_to_s3(local_path: Path) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri], capture_output=True, text=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"  [ERROR] Upload failed: {getattr(e, 'stderr', e)}")
        return False


def main():
    ap = argparse.ArgumentParser(description="NIH RePORTER costs for ExPORTER's FY1985-1999 years, to S3")
    ap.add_argument("--projects-parquet", type=Path, default=Path("./nih_data/nih_projects_combined.parquet"))
    ap.add_argument("--output-dir", type=Path, default=Path("./nih_data"))
    ap.add_argument("--cache", type=Path, help="JSONL of RePORTER responses (default: <output-dir>/nih_reporter_costs_cache.jsonl)")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache = args.cache or args.output_dir / "nih_reporter_costs_cache.jsonl"

    projects = pd.read_parquet(args.projects_parquet, columns=["application_id", "fy", "subproject_id", "total_cost"])
    ids = needed_application_ids(projects)
    print(f"FY{FIRST_FY}-{LAST_FY} project years with no ExPORTER cost: {len(ids):,}")
    fetch(ids, cache)
    _, results = read_cache(cache)
    df = build_rows(ids, results, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    print(f"RePORTER cost found for {len(df):,} of {len(ids):,} ({len(df) / max(len(ids), 1):.2%}); "
          f"USD {df['award_amount'].sum():,.0f}; zero or less: {(df['award_amount'] <= 0).sum():,}")
    out = args.output_dir / "nih_reporter_costs.parquet"
    df.to_parquet(out, index=False)
    print(f"  [SAVE] {out}")
    if not args.skip_upload and not upload_to_s3(out):
        sys.exit(1)
    print("Next: run CreateNIHAwards (Step 1b reads the file), then the next Nightly Grants run sums the filled years.")


if __name__ == "__main__":
    main()
