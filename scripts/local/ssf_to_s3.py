#!/usr/bin/env python3
"""
Swedish Foundation for Strategic Research (SSF) to S3 Data Pipeline
==================================================================

Scrapes SSF's (Stiftelsen for Strategisk Forskning) granted projects and uploads
a parquet to S3 for Databricks ingestion.

Data source: strategiska.se (server-rendered; NOT wp-json, NOT SweCRIS — SSF is
    absent from SweCRIS's 13-funder set). Two-hop crawl:
      1. Index pages list funding calls:
         /forskning/genomford-forskning/ (~235 completed) and
         /forskning/pagaende-forskning/ (~44 ongoing).
      2. Each call slug has a `/projekt/` sub-page with a grant table:
         DNr | Projekttitel | Projektledare | Forskningsomrade |
         Start- och slutdatum (YYMMDD - YYMMDD) | Beviljat belopp (SEK).
    Default UA fine; Swedish text. Programme = the call slug.

Output: s3://openalex-ingest/awards/ssf/ssf_grants.parquet

Usage:
    python ssf_to_s3.py
    python ssf_to_s3.py --skip-upload --max-calls 5

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

BASE = "https://strategiska.se"
INDEX_PAGES = [
    "/forskning/genomford-forskning/",
    "/forskning/pagaende-forskning/",
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ssf/ssf_grants.parquet"
REQUEST_DELAY = 0.4


def yymmdd_to_date(tok):
    tok = tok.strip()
    m = re.match(r"(\d{2})(\d{2})(\d{2})$", tok)
    if not m:
        return None, None
    yy, mm, dd = int(m.group(1)), m.group(2), m.group(3)
    year = 2000 + yy if yy <= 40 else 1900 + yy
    return f"{year}-{mm}-{dd}", str(year)


def parse_pi(raw):
    """SSF lists 'Lastname, Firstname'."""
    raw = (raw or "").strip()
    if not raw:
        return None, None
    if "," in raw:
        family, given = raw.split(",", 1)
        return given.strip() or None, family.strip() or None
    parts = raw.split()
    if len(parts) < 2:
        return None, raw or None
    return " ".join(parts[:-1]), parts[-1]


def parse_amount(raw):
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    return digits or None


def slug_to_programme(slug):
    s = re.sub(r"-(\d{4})$", r" \1", slug).replace("-", " ")
    return s.strip().title()


def get_call_slugs(session):
    slugs = {}
    for idx in INDEX_PAGES:
        r = session.get(BASE + idx, timeout=60)
        r.raise_for_status()
        for u in re.findall(
                r'href="(https://strategiska\.se/forskning/(?:genomford|pagaende)-forskning/([a-z0-9-]+)/)"',
                r.text):
            full, slug = u
            if slug and slug not in slugs:
                slugs[slug] = full.rstrip("/")
    return slugs


def parse_projekt(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    programme = slug_to_programme(slug)
    out = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        head = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any("dnr" in h for h in head):
            continue
        idx = {h: i for i, h in enumerate(head)}

        def col(cells, *keys):
            for k in keys:
                for h, i in idx.items():
                    if k in h and i < len(cells):
                        return cells[i].get_text(" ", strip=True)
            return None
        for tr in rows[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            dnr = col(cells, "dnr")
            title = col(cells, "projekttitel", "titel")
            if not (dnr or title):
                continue
            pi = col(cells, "projektledare")
            area = col(cells, "forskningsomr")
            dates = col(cells, "datum") or ""
            amount = col(cells, "belopp")
            start_date = start_year = end_date = None
            dm = re.search(r"(\d{6})\s*[-–]\s*(\d{6})", dates)
            if dm:
                start_date, start_year = yymmdd_to_date(dm.group(1))
                end_date, _ = yymmdd_to_date(dm.group(2))
            given, family = parse_pi(pi)
            out.append({
                "funder_award_id": dnr or f"ssf-{slug}-{len(out)}",
                "title": title,
                "pi_given": given,
                "pi_family": family,
                "research_area": area,
                "amount": parse_amount(amount),
                "programme": programme,
                "start_date": start_date,
                "end_date": end_date,
                "start_year": start_year,
            })
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
    ap = argparse.ArgumentParser(description="SSF granted projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ssf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-calls", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("SSF (Swedish Foundation for Strategic Research) -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    calls = get_call_slugs(s)
    items = list(calls.items())
    if args.max_calls:
        items = items[:args.max_calls]
    print(f"Funding calls: {len(items)}")

    rows, seen, no_table, failures = [], set(), 0, 0
    for n, (slug, call_url) in enumerate(items, 1):
        url = call_url + "/projekt/"
        html = None
        for _ in (1, 2):
            try:
                r = s.get(url, timeout=60)
                if r.status_code == 200:
                    html = r.text
                    break
                if r.status_code == 404:
                    html = ""  # some calls have no /projekt/ table
                    break
                time.sleep(3)
            except Exception:
                time.sleep(3)
        if html is None:
            failures += 1
            if failures > 30:
                print("[ERROR] >30 fetch failures; STOP")
                sys.exit(1)
            continue
        recs = parse_projekt(html, slug) if html else []
        if not recs:
            no_table += 1
        for rec in recs:
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        if n % 40 == 0:
            print(f"  [{n}/{len(items)}] {len(rows)} grants ({no_table} calls w/o table)")
        time.sleep(REQUEST_DELAY)

    if len(rows) < 200:
        print(f"[ERROR] only {len(rows)} grants — expected low-thousands; check parse")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns "
          f"({no_table} calls without a project table)")
    for c in ("title", "pi_family", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/len(df))}%)")
    out = args.output_dir / "ssf_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
