#!/usr/bin/env python3
"""
Gerber Foundation to S3 Data Pipeline
=====================================

Scrapes the Gerber Foundation's recent national research-grant award lists
(annual PDFs) and uploads a parquet to S3 for Databricks ingestion.

Data source: gerberfoundation.org/research-grants/ links one PDF per recent
    year (2023-2025). Each PDF is a flat list of awards; `pdftotext -layout`
    yields 3-line records:
        Institution (PI Name, degrees)                         $amount
        City, ST
        Project Title
    (City/Title order occasionally swaps.) The PI is the LAST parenthetical
    group before the dollar amount — some institutions carry their own parens
    (e.g. "... Medical Center (Cincinnati) (Zachary Taylor, Ph.D.)"). No native
    grant id; synthesize from year + institution + PI + title. USD amounts.
    Maternal/infant/child-health funder on the IAMHRF expanded list.

Output: s3://openalex-ingest/awards/gerber/gerber_grants.parquet
"""

import argparse
import hashlib
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

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
PAGE = "https://www.gerberfoundation.org/research-grants/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gerber/gerber_grants.parquet"

# header: <institution> (<pi, degrees>) [ $<amount> ]   -- greedy so the LAST
# paren group is the PI, even if the institution carries its own parens
# (e.g. "... Medical Center (Cincinnati) (Zachary Taylor, Ph.D.)"). The amount
# is optional here because a wrapped record can push it onto the city line.
HEADER_RE = re.compile(r"^(.*)\(([^()]+)\)\s*(?:\$\s*([\d,]+))?\s*$")
AMOUNT_RE = re.compile(r"\$\s*([\d,]+)")
CITY_RE = re.compile(r"^(.+),\s*([A-Z]{2})\.?$")
DEGREE_RE = re.compile(
    r",?\s*\b(MD|PhD|Ph\.?D|MPH|MS|MSc|DO|Au\.?D|FAAP|RN|RD|ScD|DrPH|"
    r"MBBS|DVM|PharmD|MBA|BSN|MSN|FACS)\b\.?", re.I)
YEAR_RE = re.compile(r"\b(20\d{2})\b")


def parse_pi(raw):
    """'Naomi Abe, MD' -> ('Naomi', 'Abe'). Strip degrees."""
    name = DEGREE_RE.sub("", raw).strip().rstrip(",").strip()
    name = re.sub(r"\s+", " ", name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name
    return " ".join(parts[:-1]), parts[-1]


def year_from_url(url):
    # prefer the 4-digit year that appears in the filename
    m = YEAR_RE.search(url.rsplit("/", 1)[-1])
    return m.group(1) if m else None


def _is_pi(grp):
    """A parenthetical that looks like an investigator (comma or a degree)."""
    return ("," in grp) or bool(DEGREE_RE.search(grp))


def parse_pdf(path, year):
    txt = subprocess.run(["pdftotext", "-layout", str(path), "-"],
                         capture_output=True, text=True).stdout
    # records are separated by blank lines -> parse block by block, so a
    # line-wrapped record (institution+PI on one line, "City, ST  $amount" on
    # the next, multi-line title) is handled the same as a clean one.
    blocks, cur = [], []
    for l in txt.split("\n"):
        if l.strip():
            cur.append(l.strip())
        elif cur:
            blocks.append(cur)
            cur = []
    if cur:
        blocks.append(cur)

    rows = []
    for blk in blocks:
        # locate the header line (institution + PI parenthetical)
        hdr_idx = institution = pi_raw = amount = None
        for k, l in enumerate(blk):
            m = HEADER_RE.match(l)
            if m and _is_pi(m.group(2)):
                hdr_idx = k
                institution = re.sub(r"\s+", " ", m.group(1)).strip()
                pi_raw = m.group(2).strip()
                amount = m.group(3)
                break
        if hdr_idx is None or not institution or "total" in institution.lower():
            continue
        # amount may live on a wrapped continuation line
        if amount is None:
            for l in blk:
                am = AMOUNT_RE.search(l)
                if am:
                    amount = am.group(1)
                    break
        if not amount:
            continue
        amount = amount.replace(",", "")
        # remaining lines -> city (one) + title (the rest), after stripping any
        # trailing $amount fragment from a wrapped city line
        city, title_parts = None, []
        for k, l in enumerate(blk):
            if k == hdr_idx:
                continue
            l2 = AMOUNT_RE.sub("", l).strip()
            if not l2:
                continue
            if city is None and len(l2) < 45 and CITY_RE.match(l2):
                city = l2
            else:
                title_parts.append(l2)
        title = " ".join(title_parts) if title_parts else None
        given, family = parse_pi(pi_raw)
        key = f"{year}|{institution}|{pi_raw}|{title or ''}".lower()
        aid = "gerber-%s-%s" % (year, hashlib.md5(key.encode()).hexdigest()[:10])
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "pi_given": given, "pi_family": family,
            "institution": institution or None,
            "city": city,
            "amount": amount,
            "year_awarded": year,
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
    ap = argparse.ArgumentParser(description="Gerber Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/gerber_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Gerber Foundation research grants -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    page = s.get(PAGE, timeout=40).text
    pdfs = sorted(set(re.findall(
        r'href="(https://www\.gerberfoundation\.org/wp-content/uploads/[^"]*[Gg]rant[^"]*\.pdf)"',
        page)))
    print(f"Found {len(pdfs)} grant PDFs")
    if len(pdfs) < 2:
        print("[ERROR] expected >=2 annual PDFs — page changed?")
        sys.exit(1)

    rows, seen = [], set()
    for url in pdfs:
        year = year_from_url(url)
        local = args.output_dir / url.rsplit("/", 1)[-1]
        local.write_bytes(s.get(url, timeout=60).content)
        recs = parse_pdf(local, year)
        print(f"  {year}: {len(recs)} records ({url.rsplit('/',1)[-1]})")
        for r in recs:
            if r["title"] and r["funder_award_id"] not in seen:
                seen.add(r["funder_award_id"])
                rows.append(r)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "city"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    if len(df) < 40:
        print(f"[WARN] only {len(df)} records — expected ~57; check parser")
    out = args.output_dir / "gerber_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
