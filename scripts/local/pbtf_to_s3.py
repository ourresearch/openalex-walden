#!/usr/bin/env python3
"""
Pediatric Brain Tumor Foundation (PBTF) to S3 Data Pipeline
===========================================================

Scrapes the PBTF funded-projects database (curethekids.org) and uploads a
parquet to S3 for Databricks ingestion.

Data source: server-rendered WordPress listing at
    https://curethekids.org/research/funded-projects/?pg=N
Each project is a `div.card11` with: title (h3.card11-title), abstract
(p.card11-lead), detail link (a.card11-link -> /project/<slug>/), and a
label/value list (ul.card11-list li -> <strong>Award</strong><span>$X over Y
years (awarded YYYY)</span>, <strong>Principal Investigator</strong><span>Name,
degrees, Institution</span>, optional Funding Partners). ~110 projects across
~11 pages. USD amounts published (parsed from the Award string); awarded-year
parsed from the "(awarded YYYY)" suffix when present. provenance `pbtf`,
priority 324. F4320306797 (Path A). Cloudflare blocks datacenter IPs — runs from
a residential IP with a browser UA.

Output: s3://openalex-ingest/awards/pbtf/pbtf_projects.parquet

Usage:
    python pbtf_to_s3.py
    python pbtf_to_s3.py --limit 10     # smoke test
    python pbtf_to_s3.py --skip-upload

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

BASE = "https://curethekids.org/research/funded-projects/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/pbtf/pbtf_projects.parquet"
MAX_PAGES = 25
EXPECTED_MIN = 80
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

INST_KEY = re.compile(
    r"Universit|College|Institute|Hospital|School|Center|Centre|Children|"
    r"Clinic|Laborator|Research|Health|Cancer", re.I)
_TITLE_RE = re.compile(r"^(Dr\.?|Prof\.?|Professor)\s+", re.I)
DEGREE_RE = re.compile(r"^(MD|PhD|DPhil|MBBS|MSc|MPH|DO|DVM|MBChB|FRCP|ScD|"
                       r"PharmD|DPhil|MS|BM|BCh|MRCP)\.?$", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    return s or None


def split_name(name):
    """Western 'First M. Last' -> (given, family); strips leading Dr./Prof."""
    if not name:
        return None, None
    n = _TITLE_RE.sub("", name).strip()
    tokens = n.split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_pi(raw):
    """'David H. Gutmann, MD, PhD, Washington University...' -> (given, family, inst)."""
    raw = clean(raw)
    if not raw:
        return None, None, None
    parts = [p.strip() for p in raw.split(",")]
    name = parts[0]
    # institution = last part with an institution keyword, else last part
    institution = None
    for p in reversed(parts[1:]):
        if INST_KEY.search(p):
            institution = p
            break
    if institution is None and len(parts) > 1:
        institution = parts[-1] if not DEGREE_RE.match(parts[-1]) else None
    given, family = split_name(name)
    return given, family, clean(institution)


def parse_award(raw):
    """'$750,000 over 2 years (awarded 2024)' -> (amount_float, year_int)."""
    raw = clean(raw) or ""
    amt = None
    m = re.search(r"\$\s*([\d,]+)", raw)
    if m:
        try:
            amt = float(m.group(1).replace(",", ""))
        except ValueError:
            amt = None
    ym = re.search(r"awarded\s+(\d{4})", raw, re.I)
    year = int(ym.group(1)) if ym else None
    return amt, year


def parse_card(card):
    t = card.select_one(".card11-title")
    title = clean(t.get_text(" ", strip=True)) if t else None
    if not title:
        return None
    lead = card.select_one(".card11-lead")
    desc = clean(lead.get_text(" ", strip=True)) if lead else None
    a = card.select_one("a.card11-link")
    url = a.get("href") if a else None
    slug = url.rstrip("/").rsplit("/", 1)[-1] if url else re.sub(r"[^a-z0-9]+", "-", title.lower())[:80]

    award_raw = pi_raw = partners_raw = None
    for li in card.select(".card11-list li"):
        st = li.find("strong")
        sp = li.find("span")
        if not (st and sp):
            continue
        label = (clean(st.get_text(" ", strip=True)) or "").lower().rstrip(":")
        value = sp.get_text(" ", strip=True)
        if not label:
            continue
        # Fuzzy label match — the source has plural/typo/role variants
        # (Investigator(s), Principle Investigator, Project Lead(s), Co-Investigator).
        if label.startswith("award") and award_raw is None:
            award_raw = value
        elif ("investigat" in label or "project lead" in label) and pi_raw is None:
            pi_raw = value
        elif "funding partner" in label and partners_raw is None:
            partners_raw = value
    amount, year = parse_award(award_raw)
    # Multiple PIs may be listed ("A; B" or "A and B") — take the first as lead.
    if pi_raw:
        pi_raw = re.split(r";| and (?=[A-Z])", pi_raw)[0].strip()
    given, family, institution = parse_pi(pi_raw)
    return {
        "funder_award_id": f"pbtf-{slug}",
        "title": title,
        "description": desc,
        "amount": amount,
        "start_year": year,
        "pi_given": given,
        "pi_family": family,
        "institution": institution,
        "funding_partners": clean(partners_raw),
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
    ap = argparse.ArgumentParser(description="Pediatric Brain Tumor Foundation projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/pbtf_data"))
    ap.add_argument("--limit", type=int, default=None, help="smoke test: cap projects")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Pediatric Brain Tumor Foundation projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    rows, seen = [], set()
    consecutive_empty = 0
    for pg in range(1, MAX_PAGES + 1):
        url = BASE if pg == 1 else f"{BASE}?pg={pg}"
        try:
            r = s.get(url, timeout=60)
        except Exception as e:
            print(f"  pg {pg}: error {e}; continuing")
            continue
        cards = BeautifulSoup(r.text, "html.parser").select("div.card11")
        if not cards:
            consecutive_empty += 1
            print(f"  pg {pg}: 0 cards ({consecutive_empty}/2)")
            if consecutive_empty >= 2:
                break
            continue
        consecutive_empty = 0
        new = 0
        for c in cards:
            rec = parse_card(c)
            if rec and rec["funder_award_id"] not in seen:
                seen.add(rec["funder_award_id"])
                rows.append(rec)
                new += 1
        print(f"  pg {pg}: +{new} ({len(rows)} total)")
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break
        time.sleep(0.4)

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year"):
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "pbtf_projects.parquet"
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
