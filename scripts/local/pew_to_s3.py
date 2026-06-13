#!/usr/bin/env python3
"""
Pew Charitable Trusts — Biomedical Scholars to S3 Data Pipeline
===============================================================

Harvests the Pew Biomedical Scholars directory (pew.org) and uploads a parquet
to S3 for Databricks ingestion.

Data source: first-party JSON list API plus server-rendered detail pages.
    LIST:   https://www.pew.org/api/Scholar/GetScholarsByYear?root=<GUID>&year=YYYY
            -> [{full_Name, fellowship_Job_Title, fellowship_Institution, itemUrl}]
            iterate year 1985..current (~19-22 scholars/year, ~800 total).
    DETAIL: https://www.pew.org{itemUrl}
            -> Research field, Award year, Research abstract, Department,
               Institution (cleaner than the list).
Pew Scholars are named career fellowships (not project grants), so there is no
per-grant project title and no published per-scholar amount (fixed stipend) —
amount is NULL (§6.7 waiver); display_name is "<name> — Pew Biomedical Scholar
(<year>)"; the research abstract is the description. provenance `pew_biomed`,
priority 328. F4320306148 (Path A). Covers the Biomedical Scholars program only
(Latin American Fellows / Innovation Fund are separate — follow-up).

Output: s3://openalex-ingest/awards/pew_biomed/pew_biomed_scholars.parquet

Usage:
    python pew_to_s3.py
    python pew_to_s3.py --limit 12       # smoke test
    python pew_to_s3.py --skip-upload
    python pew_to_s3.py --no-detail      # list-only (skip detail-page abstracts)

Author: OpenAlex Team
"""

import argparse
import json
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

ROOT = "ef94fe3e-66a0-420c-82e5-0cc09b34806a"
LIST_API = "https://www.pew.org/api/Scholar/GetScholarsByYear"
SITE = "https://www.pew.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/pew_biomed/pew_biomed_scholars.parquet"
YEARS = list(range(1985, 2027))
EXPECTED_MIN = 600
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
DEGREE_TAIL = re.compile(r",?\s*(Ph\.?D\.?|M\.?D\.?|D\.?Phil\.?|Pharm\.?D\.?|Sc\.?D\.?|"
                         r"M\.?S\.?|D\.?V\.?M\.?|M\.?P\.?H\.?)\.*", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip().strip(",").strip()
    return s or None


def split_name(full):
    """'Ana Paula Arruda, Ph.D.' -> ('Ana Paula','Arruda'); strip degree tail."""
    full = clean(full)
    if not full:
        return None, None
    name = DEGREE_TAIL.sub("", full).strip().strip(",").strip()
    toks = name.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def fetch_detail(session, url):
    try:
        r = session.get(url, timeout=40)
        if r.status_code != 200:
            return {}
    except Exception:
        return {}
    soup = BeautifulSoup(r.text, "html.parser")
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    out = {}
    for i, l in enumerate(lines):
        low = l.lower()
        if low == "research field" and i + 1 < len(lines):
            out["field"] = lines[i + 1]
        elif low == "award year" and i + 1 < len(lines):
            out["year"] = lines[i + 1]
        elif low == "institution" and i + 1 < len(lines):
            out["institution"] = lines[i + 1]
        elif low == "research" and i + 1 < len(lines):
            out["abstract"] = " ".join(lines[i + 1:i + 12])[:4000]
    return out


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
    ap = argparse.ArgumentParser(description="Pew Biomedical Scholars to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/pew_biomed_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-detail", action="store_true")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = args.output_dir / "pew_details.jsonl"

    print("=" * 60)
    print("Pew Biomedical Scholars -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA

    # Step A: list all scholars by year
    scholars = []
    for yr in YEARS:
        try:
            r = s.get(LIST_API, params={"root": ROOT, "year": yr}, timeout=40)
            if r.status_code != 200 or not r.content:
                continue
            arr = r.json()
        except Exception as e:
            print(f"  year {yr}: error {e}")
            continue
        for rec in arr:
            url = rec.get("itemUrl") or ""
            scholars.append({
                "year": yr,
                "full_name": rec.get("full_Name"),
                "job_title": rec.get("fellowship_Job_Title"),
                "institution": rec.get("fellowship_Institution"),
                "item_url": (SITE + url) if url.startswith("/") else url,
            })
        if arr:
            print(f"  year {yr}: +{len(arr)} ({len(scholars)} total)")
        time.sleep(0.15)
    print(f"List complete: {len(scholars)} scholars")
    if args.limit:
        scholars = scholars[: args.limit]

    # Step B: detail pages (checkpointed) for abstract / field
    details = {}
    if checkpoint.exists():
        for line in open(checkpoint, encoding="utf-8"):
            d = json.loads(line)
            details[d["item_url"]] = d["detail"]
    if not args.no_detail:
        todo = [sc for sc in scholars if sc["item_url"] and sc["item_url"] not in details]
        print(f"Fetching {len(todo)} detail pages...")
        with open(checkpoint, "a", encoding="utf-8") as ck:
            for n, sc in enumerate(todo, 1):
                d = fetch_detail(s, sc["item_url"])
                details[sc["item_url"]] = d
                ck.write(json.dumps({"item_url": sc["item_url"], "detail": d}, ensure_ascii=False) + "\n")
                if n % 100 == 0:
                    ck.flush()
                    print(f"  {n}/{len(todo)} details")
                time.sleep(0.2)

    rows, seen = [], set()
    for sc in scholars:
        given, family = split_name(sc["full_name"])
        if not family:
            continue
        d = details.get(sc["item_url"], {})
        year = d.get("year") or sc["year"]
        institution = clean(d.get("institution")) or clean(sc["institution"])
        slug = sc["item_url"].rstrip("/").rsplit("/", 1)[-1] if sc["item_url"] else f"{family}-{year}"
        aid = f"pew-{slug}"
        if aid in seen:
            continue
        seen.add(aid)
        nm = clean(DEGREE_TAIL.sub("", sc["full_name"] or "").strip())
        rows.append({
            "funder_award_id": aid,
            "title": f"{nm} — Pew Biomedical Scholar ({year})" if nm else None,
            "description": clean(d.get("abstract")),
            "pi_given": given,
            "pi_family": family,
            "institution": institution,
            "funder_scheme": "Pew Biomedical Scholars Program",
            "research_field": clean(d.get("field")),
            "start_year": int(year) if str(year).isdigit() else None,
            "landing_page_url": sc["item_url"],
        })

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} scholars — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year":
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "description", "start_year"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}  years: {df.start_year.min()}-{df.start_year.max()}")
    out = args.output_dir / "pew_biomed_scholars.parquet"
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
