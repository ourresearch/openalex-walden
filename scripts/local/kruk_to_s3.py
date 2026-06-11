#!/usr/bin/env python3
"""
Kidney Research UK (KRUK) to S3 Data Pipeline
=============================================

Scrapes Kidney Research UK's published award announcements and uploads a
parquet to S3 for Databricks ingestion.

Data source: https://www.kidneyresearchuk.org/research/latest-awards/
    (single server-rendered page of accordion sections, one per grant round,
    e.g. "Main Hybrid grant round awards: February 2026"). **Firefox UA is
    MANDATORY** — the default python-requests UA and Chrome UA are both 403'd;
    Firefox passes. ~8 rounds / ~53 awards as of 2026-06 (the page grows as
    rounds are announced; KRUK publishes no historical archive).

Per-award lines inside a section: PI ("Dr. Peter Wing"), institution
    (", University of Oxford"), amount+duration ("£120k over 12 months",
    abbreviated k/M), "Title: ...". No native ids (synthesized). start_year
    from the round heading; scheme = heading minus the date.

Output: s3://openalex-ingest/awards/kruk/kruk_awards.parquet

Usage:
    python kruk_to_s3.py
    python kruk_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

URL = "https://www.kidneyresearchuk.org/research/latest-awards/"
FIREFOX_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) "
              "Gecko/20100101 Firefox/126.0")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kruk/kruk_awards.parquet"

PI_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+[A-Z]")
AMT_RE = re.compile(r"^£\s*([\d.,]+)\s*([kKmM])?\b(?:\s+over\s+(\d+)\s+months)?")
HEAD_RE = re.compile(r"^(.+?):\s*([A-Z][a-z]+)\s+(\d{4})$")
STOP_PREFIXES = ("Total awarded", "Overall success rate", "Details of the awards",
                 "Expand", "We are delighted", "We received", "Following discussion",
                 "This recommendation")

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def parse_amount(m):
    if not m:
        return None
    val = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    if suf == "k":
        val *= 1_000
    elif suf == "m":
        val *= 1_000_000
    return str(int(val))


MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}


def parse_awards(html):
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    lines = [l.strip() for l in txt.split("\n") if l.strip()]

    rows, section, sect_year, sect_date = [], None, None, None
    cur = None

    def flush():
        nonlocal cur
        if cur and (cur.get("title") or cur.get("amount")):
            rows.append(cur)
        cur = None

    for i, l in enumerate(lines):
        # section heading = the line right before "Expand"
        if i + 1 < len(lines) and lines[i + 1] == "Expand":
            hm = HEAD_RE.match(l)
            if hm:
                flush()
                section = hm.group(1).strip()
                month, year = hm.group(2), hm.group(3)
                sect_year = year
                sect_date = (f"{year}-{MONTHS[month]:02d}-01"
                             if month in MONTHS else None)
                continue
        if section is None:
            continue
        if PI_RE.match(l) and not l.startswith("Title:"):
            flush()
            cur = {"pi_raw": l.rstrip(","), "scheme": section,
                   "start_year": sect_year, "start_date": sect_date}
            continue
        if cur is None:
            continue
        am = AMT_RE.match(l)
        if am:
            cur["amount"] = parse_amount(am)
            cur["duration_months"] = am.group(3)
            continue
        if l.startswith("Title:"):
            cur["title"] = l[len("Title:"):].strip()
            continue
        if any(l.startswith(p) for p in STOP_PREFIXES):
            continue
        # first non-amount/non-title line after the PI = institution
        # (may lead with a comma, or be a bare line because the PI line ended in ",")
        if "institution" not in cur and "title" not in cur:
            cur["institution"] = l.lstrip(", ").strip()
            continue
        if "title" in cur and len(l) > 2 and not HEAD_RE.match(l):
            cur["title"] += " " + l
            continue
    flush()
    return rows


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
    ap = argparse.ArgumentParser(description="Kidney Research UK awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/kruk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Kidney Research UK latest awards -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": FIREFOX_UA})
    r = s.get(URL, timeout=60)
    r.raise_for_status()
    raw_rows = parse_awards(r.text)
    print(f"Awards parsed: {len(raw_rows)}")
    if len(raw_rows) < 30:
        print(f"[ERROR] only {len(raw_rows)} awards — expected ~50; layout change?")
        sys.exit(1)

    rows, seen = [], set()
    for rec in raw_rows:
        given, family = parse_pi(rec.get("pi_raw"))
        title = re.sub(r"\s+", " ", rec.get("title") or "").strip() or None
        slug_src = f"{family or rec.get('pi_raw')}-{rec.get('start_year')}-{(title or '')[:40]}"
        aid = "kruk-" + re.sub(r"[^a-z0-9]+", "-", slug_src.lower()).strip("-")[:70]
        if aid in seen:
            aid = aid + "-2"
        seen.add(aid)
        rows.append({
            "funder_award_id": aid,
            "title": title or f"Kidney Research UK award ({rec.get('scheme')})",
            "pi_given": given,
            "pi_family": family,
            "institution": rec.get("institution"),
            "amount": rec.get("amount"),
            "programme": rec.get("scheme"),
            "start_date": rec.get("start_date"),
            "start_year": rec.get("start_year"),
            "duration_months": rec.get("duration_months"),
        })

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("pi_family", "institution", "amount", "title", "programme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "kruk_awards.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
