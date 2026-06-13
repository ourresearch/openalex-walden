#!/usr/bin/env python3
"""
Ragnar Söderberg Foundation (Sweden) to S3 Data Pipeline
========================================================

Scrapes the Ragnar Söderberg Foundation grantee profiles
(ragnar.soderbergs.se) and uploads a parquet to S3 for Databricks ingestion.

Data source: server-rendered grantee profile pages, enumerated from the three
research-area listings (/medicin/, /ekonomi/, /rattsvetenskap/) which link to
/forskare/<slug>/ profiles. Each profile carries Swedish-labelled fields:
    <date>  |  <field area>  |  <name>  |  <scheme "Ragnar Söderberg … anslag YYYY">
    Anslagsförvaltare  -> host institution
    Anslagssumma       -> grant amount ("1 770 000 kr" = SEK)
    Projektsammanfattning -> project summary (first line = project title)
~97 grantees. Rich coverage: name + institution + SEK amount + year + abstract.
Swedish-language titles/abstracts (ingested as-is). provenance
`ragnar_soderberg`, priority 326. F4320309816 (Path A).

Output: s3://openalex-ingest/awards/ragnar_soderberg/ragnar_soderberg_grants.parquet

Usage:
    python ragnar_soderberg_to_s3.py
    python ragnar_soderberg_to_s3.py --limit 8
    python ragnar_soderberg_to_s3.py --skip-upload

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

BASE = "https://ragnar.soderbergs.se"
AREAS = {"Medicin": "/medicin/", "Ekonomi": "/ekonomi/", "Rättsvetenskap": "/rattsvetenskap/"}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ragnar_soderberg/ragnar_soderberg_grants.parquet"
EXPECTED_MIN = 70
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(name):
    if not name:
        return None, None
    toks = name.split()
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def parse_amount_sek(s):
    """'1 770 000 kr' -> 1770000.0"""
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s.split("kr")[0])
    return float(digits) if digits else None


def get_profile_links(session):
    links = set()
    for area, path in AREAS.items():
        r = session.get(BASE + path, timeout=40)
        if r.status_code != 200:
            print(f"  area {area}: HTTP {r.status_code}")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "/forskare/" in h:
                links.add(h if h.startswith("http") else BASE + h)
    return sorted(links)


def parse_profile(html, url, session=None):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    name = clean(h1.get_text(" ", strip=True)) if h1 else None
    for x in soup(["script", "style", "nav", "header", "footer", "iframe"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    field = scheme = institution = amount_raw = year = date_year = None
    summary_idx = None
    # Swedish date line e.g. "3 april, 2023" — robust source of the grant year.
    SV_DATE = re.compile(
        r"\b\d{1,2}\s+(?:januari|februari|mars|april|maj|juni|juli|augusti|"
        r"september|oktober|november|december)\,?\s+((?:19|20)\d{2})\b", re.I)
    for i, l in enumerate(lines):
        if l in AREAS and field is None:
            field = l
        dm = SV_DATE.search(l)
        if dm and date_year is None:
            date_year = int(dm.group(1))
        if re.search(r"anslag", l, re.I) and re.search(r"\b(19|20)\d{2}\b", l) and scheme is None:
            scheme = l
            ym = re.search(r"\b(19|20)\d{2}\b", l)
            year = int(ym.group(0)) if ym else None
        if l.lower().startswith("anslagsförvaltare") and i + 1 < len(lines):
            institution = lines[i + 1]
        if l.lower().startswith("anslagssumma") and i + 1 < len(lines):
            amount_raw = lines[i + 1]
        if l.lower().startswith("projektsammanfattning"):
            summary_idx = i
    if not name and lines:
        # fall back: name is the line just before the scheme
        name = clean(soup.title.get_text().split("|")[0]) if soup.title else None

    title = description = None
    if summary_idx is not None and summary_idx + 1 < len(lines):
        title = lines[summary_idx + 1]
        description = " ".join(lines[summary_idx + 1:])[:4000]

    given, family = split_name(name)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    return {
        "funder_award_id": f"ragnar-{slug}",
        "title": clean(title) or clean(scheme),
        "description": clean(description),
        "amount": parse_amount_sek(amount_raw),
        "start_year": year or date_year,
        "pi_given": given,
        "pi_family": family,
        "institution": clean(institution),
        "funder_scheme": clean(scheme),
        "field": field,
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
    ap = argparse.ArgumentParser(description="Ragnar Söderberg Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ragnar_soderberg_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Ragnar Söderberg Foundation grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    links = get_profile_links(s)
    print(f"profile links: {len(links)}")
    if args.limit:
        links = links[: args.limit]

    rows, seen = [], set()
    for n, url in enumerate(links, 1):
        try:
            r = s.get(url, timeout=40)
            if r.status_code != 200:
                print(f"  {url}: HTTP {r.status_code}")
                continue
            rec = parse_profile(r.text, url)
        except Exception as e:
            print(f"  {url}: error {e}")
            continue
        if rec["funder_award_id"] in seen or not rec["pi_family"]:
            continue
        seen.add(rec["funder_award_id"])
        rows.append(rec)
        if n % 25 == 0:
            print(f"  {n}/{len(links)} ({len(rows)} kept)")
        time.sleep(0.3)

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} grantees — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year"):
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year", "funder_scheme"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "ragnar_soderberg_grants.parquet"
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
