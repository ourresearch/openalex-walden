#!/usr/bin/env python3
"""
Sparks (GOSH Charity & Sparks National Call) to S3 Data Pipeline
================================================================

Scrapes the Sparks subset of Great Ormond Street Hospital Charity's "previous
national calls" page and uploads a parquet to S3 for Databricks ingestion.

Data source: gosh.org "previous national calls" page (one HTML page, ~9 tables
    under h2 headings). The SPARKS subset = the six tables whose h2 heading
    contains "GOSH Charity and Sparks National Call" (2017-18 .. 2022-23, 66
    rows). The three newer "GOSH Charity"-only calls (2023-24/24-25/25-26) are a
    different funder and are EXCLUDED. Columns are mapped by HEADER NAME, never
    by position: tables 2019-20..2022-23 use `Lead Investigator | Institution |
    Grant Title | Total`, while 2018-19 and 2017-18 use `Lead investigator |
    Host institution | Total | Project title`. In those two oldest tables the
    header LABELS for the last two columns are swapped relative to where the data
    actually sits (the "Total"-labelled cell holds the title and vice versa), so
    after mapping by header we content-correct the title/amount pair by detecting
    which cell is money. No native grant id; synthesize from call year + PI +
    title. GBP amounts. Children's medical-research charity on the IAMHRF list.
    Cold-accessible with UA "Mozilla/5.0".

Output: s3://openalex-ingest/awards/sparks/sparks_grants.parquet
"""

import argparse
import hashlib
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

UA = "Mozilla/5.0"
PAGE = ("https://www.gosh.org/apply-for-funding/research/research-funding-schemes/"
        "gosh-charity-national-funding-call/previous-national-calls/")
SPARKS_HEADING = "gosh charity and sparks national call"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sparks/sparks_grants.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
# a money cell: a £ sign followed (allowing spaces) by a digit
_MONEY_RE = re.compile(r"£\s*\d")
# capture the numeric run after a £ (handles "£249, 945" with a stray space and
# "£247,202.38" with pence)
_AMOUNT_RE = re.compile(r"£\s*([\d.,\s]+)")
# academic-year span in a heading, e.g. "...National Call 2022-23" -> "2022-23"
_CALL_RE = re.compile(r"(20\d{2}-\d{2})")


def parse_pi(raw):
    """'Professor Waseem Qasim' -> ('Waseem', 'Qasim'). Strip honorific."""
    if not raw:
        return None, None
    name = _TITLE_RE.sub("", raw.strip()).strip()
    name = re.sub(r"\s+", " ", name)
    if not re.search(r"[A-Za-z]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name or None
    return " ".join(parts[:-1]), parts[-1]


def is_money(s):
    return bool(_MONEY_RE.search(s or ""))


def parse_amount(raw):
    """'£247,202.38' -> '247202'; '£249, 945' -> '249945'; keep integer part."""
    if not raw:
        return None
    m = _AMOUNT_RE.search(raw)
    if not m:
        return None
    s = m.group(1).replace(",", "").replace(" ", "").strip()
    s = s.split(".")[0]  # drop pence
    return s if s.isdigit() else None


def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip()) or None


def header_index_map(header_cells):
    """Map logical field -> column index by HEADER NAME (case-insensitive)."""
    idx = {}
    for i, h in enumerate(header_cells):
        h = (h or "").strip().lower()
        if h == "lead investigator":
            idx["pi"] = i
        elif h in ("institution", "host institution"):
            idx["institution"] = i
        elif h in ("grant title", "project title"):
            idx["title"] = i
        elif h == "total":
            idx["amount"] = i
    return idx


def parse_table(table, call, year):
    trs = table.find_all("tr")
    if not trs:
        return []
    header = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])]
    idx = header_index_map(header)
    if not all(k in idx for k in ("pi", "institution", "title", "amount")):
        print(f"  [WARN] {call}: unmapped header {header} -> skipping table")
        return []

    rows = []
    for tr in trs[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if len(cells) < len(header):
            continue
        pi_raw = clean(cells[idx["pi"]])
        institution = clean(cells[idx["institution"]])
        title = clean(cells[idx["title"]])
        amount_raw = cells[idx["amount"]]
        if not pi_raw:
            continue
        # 2017-18 / 2018-19: the "Total"-labelled cell actually holds the title
        # and the "Project title" cell holds the money -> swap by content.
        if not is_money(amount_raw) and is_money(title):
            title, amount_raw = amount_raw, title
        amount = parse_amount(amount_raw)
        given, family = parse_pi(pi_raw)
        key = f"{pi_raw}|{title or ''}"
        aid = "sparks-%s-%s" % (year, hashlib.md5(key.encode("utf-8")).hexdigest()[:10])
        rows.append({
            "funder_award_id": aid,
            "title": clean(title),
            "pi_given": given, "pi_family": family,
            "institution": institution,
            "amount": amount,
            "year_awarded": year,
            "call": call,
            "landing_page_url": PAGE,
        })
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
    ap = argparse.ArgumentParser(description="Sparks (GOSH & Sparks) grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/sparks_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Sparks (GOSH Charity & Sparks National Call) grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    page = s.get(PAGE, timeout=40).text
    soup = BeautifulSoup(page, "html.parser")

    rows, seen, per_call = [], set(), []
    for h2 in soup.find_all("h2"):
        heading = h2.get_text(" ", strip=True)
        if SPARKS_HEADING not in heading.lower():
            continue
        m = _CALL_RE.search(heading)
        if not m:
            print(f"  [WARN] no call-year in heading: {heading!r}")
            continue
        call = m.group(1)            # e.g. "2022-23"
        year = call.split("-")[0]    # e.g. "2022"
        table = h2.find_next("table")
        if table is None:
            print(f"  [WARN] no table after heading {heading!r}")
            continue
        recs = parse_table(table, call, year)
        kept = 0
        for r in recs:
            if r["funder_award_id"] in seen:
                continue
            seen.add(r["funder_award_id"])
            rows.append(r)
            kept += 1
        per_call.append((call, kept))
        print(f"  {call}: {kept} grants")

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    print("Rows per call:")
    for call, n in per_call:
        print(f"  {call}: {n}")
    for c in ("title", "pi_family", "institution", "amount"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    if len(df) < 55:
        print(f"[ERROR] only {len(df)} rows — expected 66; page/parser changed?")
        sys.exit(1)

    out = args.output_dir / "sparks_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
