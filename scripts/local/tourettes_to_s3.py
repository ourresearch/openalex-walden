#!/usr/bin/env python3
"""
Tourettes Action (UK) to S3 Data Pipeline
=========================================

Scrapes Tourettes Action funded-research pages and uploads a parquet to S3.

Data source: legacy numbered-URL pages on tourettes-action.org.uk:
    /167-2017-funded-projects.html, /168-2018-..., /169-2019-...
Each project is a text block: project title, then a PI line
("Name, role, Institution, Country"), then "Award: £N", then
"Study Duration: YYYY - YYYY". Parsed by anchoring on the Award:/Study Duration:
labels. ~9 awards with full fields incl GBP amount. provenance `tourettes`,
priority 337. F4320312907 (Path A).

Output: s3://openalex-ingest/awards/tourettes/tourettes_grants.parquet

Usage: python tourettes_to_s3.py [--limit N] [--skip-upload]
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

PAGES = {
    2017: "https://www.tourettes-action.org.uk/167-2017-funded-projects.html",
    2018: "https://www.tourettes-action.org.uk/168-2018-funded-projects.html",
    2019: "https://www.tourettes-action.org.uk/169-2019-funded-projects.html",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/tourettes/tourettes_grants.parquet"
EXPECTED_MIN = 6
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

INST_KEY = re.compile(r"Universit|College|Institute|Hospital|School|Centre|Center|"
                      r"Karolinska|Imperial|UCL|King'?s|Trust|NHS", re.I)
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n:
        return None, None
    n = _TITLE_RE.sub("", n).strip()
    toks = n.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


NON_PERSON = re.compile(r"Universit|College|Institute|Centre|Center|Hospital|School|"
                        r"Director|Knowledge|Studies|Department|Trust|Foundation|NHS|Society",
                        re.I)


def parse_pi_line(line):
    """'Dr Tara Murphy, Consultant ..., Great Ormond Street Hospital, UK' -> (g,f,inst).

    The line before 'Award:' is usually the PI line, but on some entries it is an
    institution/role line (no person). Validate: the first comma-field must look
    like a person (<=4 tokens, no institution/role keyword); otherwise NULL the PI
    and treat the keyword-bearing field as the institution.
    """
    parts = [p.strip() for p in line.split(",")]
    name = parts[0]
    institution = None
    for p in parts:
        if INST_KEY.search(p):
            institution = p
            break
    if NON_PERSON.search(name) or len(name.split()) > 4:
        return None, None, clean(institution)
    g, f = split_name(name)
    return g, f, clean(institution)


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def parse_page(html, year):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    text = soup.get_text("\n", strip=True)
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    rows = []
    for i, l in enumerate(lines):
        m = re.match(r"Award:\s*£?\s*([\d,]+)", l)
        if not m:
            continue
        amount = float(m.group(1).replace(",", "")) if m.group(1) else None
        # PI line is the line before Award:, title is the line before that
        pi_line = lines[i - 1] if i >= 1 else ""
        title = lines[i - 2] if i >= 2 else None
        # year from following "Study Duration:" line if present
        yr = year
        if i + 1 < len(lines):
            dm = re.search(r"(20\d\d)", lines[i + 1])
            if dm:
                yr = int(dm.group(1))
        g, f, inst = parse_pi_line(pi_line)
        if not title or NON_PERSON.search(title) or re.match(r"Award:|Study Duration:", title):
            continue
        rows.append({
            "funder_award_id": f"tourettes-{slugify(title)}",
            "title": clean(title),
            "pi_given": g, "pi_family": f, "institution": inst,
            "amount": amount, "start_year": yr,
            "landing_page_url": PAGES[year],
        })
    return rows


def upload_to_s3(local_path, bucket, key):
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
    ap = argparse.ArgumentParser(description="Tourettes Action grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/tourettes_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Tourettes Action funded research -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.headers["Accept-Language"] = "en-GB,en;q=0.9"
    rows, seen = [], set()
    for year, url in PAGES.items():
        r = s.get(url, timeout=40)
        if r.status_code != 200:
            print(f"  {year}: HTTP {r.status_code}")
            continue
        for rec in parse_page(r.text, year):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        print(f"  {year}: {len(rows)} total")

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} awards — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)
    if args.limit:
        rows = rows[: args.limit]

    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year"):
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "tourettes_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out}")
    for _, r in df.iterrows():
        print("   ", repr(r.pi_given), repr(r.pi_family), "|", repr(str(r.institution)[:30]), "| £", r.amount, "|", r.start_year)

    if args.limit:
        print("\nSmoke run complete.")
        return
    if not args.skip_upload and not upload_to_s3(out, S3_BUCKET, S3_KEY):
        sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
