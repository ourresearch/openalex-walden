#!/usr/bin/env python3
"""
TSC Alliance (Tuberous Sclerosis Alliance) awards to S3 Data Pipeline
=====================================================================

Scrapes the TSC Alliance funded-grant history and uploads a parquet to S3 for
Databricks ingestion. (OpenAlex funder F4320306781; awards wave 5 / IAMHRF.)

Data source: tscalliance.org. The whole site sits behind Cloudflare
    ("Attention Required"), which 403s plain `requests` AND Playwright
    **Chromium** headless, but serves a clean 200 to Playwright **Firefox**
    headless (do NOT use headless=False; do NOT use chromium). Flow:
      1. firefox.launch(headless=True) -> goto
         /researchers/grants-funding/ -> 200, settle ~3s.
      2. The funded-grant lists are NOT linked sub-pages (those 404) -- they are
         PowerfulFAQ accordion answer panels in the SAME DOM. Read them via
         document.querySelectorAll('.pafa-accordion-q') -> nextElementSibling
         .innerText. The two award panels are "Currently funded grant awards"
         (FY2025-FY2024) and "Past grant awards" (FY2023 -> FY2002-2005). The
         panels are readable without expanding (innerText returns full text).
      3. Each panel's innerText is a flat run of lines structured as:
           FY <year>                      (fiscal-year header)
           <Scheme Name>                  (scheme header, e.g. Research Grant Award)
           <PI Name, Degrees>             (one award: PI line, has a degree token)
           [Sponsor: <name>]              (optional, fellowships only -- skipped)
           <Institution>
           <Project Title>
           ... (repeats) ...
         A PI line is identified by a degree token {PhD|MD|DPhil|MDCM} (every
         named PI carries one of these; MA/MS/BSc never appear alone, and
         dropping them avoids ", MA"/", MS" US-state-code institution
         collisions). Consecutive PI lines = co-PIs of ONE award (e.g. the
         FY2002-2005 Rothberg award lists 5 PIs). Scheme/FY headers carry no
         degree token; institutions carry institution keywords -> both excluded.
    NO per-record amounts or award-ids on the page (policy caps only) -- the
    only amounts/DOIs live in the 13 Crossref grant deposits (prefix 10.51761),
    out of scope here. funder_award_id is synthesized tsc-<fy>-<md5(pi|title)>.

Output: s3://openalex-ingest/awards/tsc_alliance/tsc_alliance_grants.parquet
    (S3 target WIRED; pass --skip-upload to dry-run without uploading.)
"""

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

URL = "https://www.tscalliance.org/researchers/grants-funding/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/tsc_alliance/tsc_alliance_grants.parquet"
SHRINK_GUARD = 45  # expect ~100-120 named awards FY2002-FY2025; abort if fewer

# Award panels to harvest (lower-cased substring match on the accordion header).
TARGET_PANELS = ("currently funded grant awards", "past grant awards")

# A fiscal-year header: "FY 2025", "FY2024", "FY 2002 - 2005" (take the first year).
FY_RE = re.compile(r"^FY\s*(20\d\d)\b")
# PI-line detector: a degree token NOT shared with US state abbreviations.
# (MA/MS/BSc/etc. are dropped deliberately -- every PI also carries PhD/MD/
#  DPhil/MDCM, while institutions routinely end ", MA"/", MS".)
PI_DEGREE_RE = re.compile(r"\b(PhD|MD|DPhil|MDCM)\b")
# Institution keywords -- a line with one of these is never a PI (belt-and-suspenders).
INST_KW_RE = re.compile(
    r"University|Universit|Hospital|College|Cent(?:er|re)|Institute|School|"
    r"Clinic|Foundation|LLC|Medicine|Medical|UCLA|UMC|Cancer|Laborator|Society",
    re.I,
)
# Honorifics / degree suffixes stripped when splitting a PI name.
_HONORIFIC_RE = re.compile(
    r"^((?:Associate\s+Professor|Assistant\s+Professor|Professor|Prof|Dr|Mr|"
    r"Mrs|Ms|Miss|Sir|Dame)\.?\s+)+",
    re.I,
)
_DEGREE_STRIP_RE = re.compile(
    r",?\s*\b(PhD|MD|DPhil|MDCM|MBBS|MBChB|MRCPsych|BSc|BS|MSc|MS|MA|MPH|ScD|"
    r"DO|DVM|PharmD|FMedSci|MRCP|FRCP|MBA|II|MMed)\b\.?",
    re.I,
)


def is_pi_line(line: str) -> bool:
    """True if `line` is a principal-investigator line (anchors one award)."""
    if not line or line.lower().startswith("sponsor"):
        return False
    return bool(PI_DEGREE_RE.search(line)) and not INST_KW_RE.search(line)


def parse_pi(raw):
    """First PI of a (possibly multi-PI) line -> (given, family).

    Strip honorifics + trailing degrees; family = last token. Multiple PIs are
    separated by ',', '&', ';', '/', or ' and ' -> keep only the first.
    """
    if not raw:
        return None, None
    first = re.split(r"\s+and\s+|&|;|,|/", raw, maxsplit=1)[0].strip()
    first = _HONORIFIC_RE.sub("", first).strip()
    first = _DEGREE_STRIP_RE.sub("", first).strip().strip(",").strip()
    if not re.search(r"[A-Za-z]{2}", first):  # nothing name-like left
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return None, first or None
    return " ".join(parts[:-1]), parts[-1]


def clean(text):
    """Collapse whitespace; return None for empties."""
    if not text:
        return None
    t = re.sub(r"\s+", " ", text).strip()
    return t or None


def make_award_id(fy, pi_raw, title):
    """Synthesize a stable id: tsc-<fy>-<md5(pi|title)[:10]>."""
    key = f"{(pi_raw or '').strip().lower()}|{(title or '').strip().lower()}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()[:10]
    return f"tsc-{fy}-{digest}"


def fetch_panels():
    """Load the grants page in Firefox headless; return combined panel text."""
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)  # Cloudflare -> Firefox, NOT chromium
        page = browser.new_page()
        resp = page.goto(URL, timeout=60000, wait_until="domcontentloaded")
        status = resp.status if resp else None
        page.wait_for_timeout(3000)
        print(f"  GET {URL} -> {status} | {page.title()!r}")
        if status != 200:
            print(f"[ERROR] non-200 status {status} (Cloudflare? wrong engine?)")
            browser.close()
            sys.exit(1)
        panels = page.evaluate(
            """() => {
                const out = {};
                document.querySelectorAll('.pafa-accordion-q').forEach(q => {
                    const a = q.nextElementSibling;
                    out[q.innerText.trim()] = a ? a.innerText : '';
                });
                return out;
            }"""
        )
        browser.close()

    chunks = []
    for header, body in panels.items():
        if any(t in header.lower() for t in TARGET_PANELS) and body:
            print(f"  panel {header!r}: {len(body)} chars")
            chunks.append(body)
    if not chunks:
        print("[ERROR] no award panels found (selector/markup change?)")
        sys.exit(1)
    return "\n".join(chunks)


def parse_awards(text):
    """Walk the combined panel text into award records (PI-anchored)."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    rows, seen = [], set()
    fy = scheme = None
    n = len(lines)
    i = 0
    while i < n:
        line = lines[i]

        m = FY_RE.match(line)
        if m:
            fy = int(m.group(1))
            scheme = None  # each fiscal year re-introduces its own scheme headers
            i += 1
            continue

        if is_pi_line(line):
            # Collect consecutive PI lines = co-PIs of the SAME award.
            pis = [line]
            j = i + 1
            while j < n and is_pi_line(lines[j]):
                pis.append(lines[j])
                j += 1
            # Optional "Sponsor:" line (fellowships) sits before the institution.
            if j < n and lines[j].lower().startswith("sponsor"):
                j += 1
            institution = lines[j] if j < n else None
            title = lines[j + 1] if j + 1 < n else None
            i = j + 2

            pi_raw = clean(pis[0])
            title = clean(title)
            if not title or fy is None:  # need a title (for the id) and a year
                continue
            pi_given, pi_family = parse_pi(pi_raw)
            aid = make_award_id(fy, pi_raw, title)
            if aid in seen:
                continue
            seen.add(aid)
            rows.append({
                "funder_award_id": aid,
                "title": title,
                "pi_raw": pi_raw,
                "pi_given": pi_given,
                "pi_family": pi_family,
                "investigators": "; ".join(clean(x) for x in pis),
                "institution": clean(institution),
                "scheme": clean(scheme),
                "fiscal_year_raw": f"FY{fy}",
                "year_awarded": fy,
                "landing_page_url": URL,
            })
            continue

        # Any other top-level line is a scheme/section header (or stray prose).
        scheme = line
        i += 1

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
    ap = argparse.ArgumentParser(description="TSC Alliance awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/tsc_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("TSC Alliance awards -> S3")
    print("=" * 60)

    print("Loading grants page (Firefox headless; Cloudflare-gated)...")
    text = fetch_panels()
    rows = parse_awards(text)
    print(f"\nParsed {len(rows)} award records")
    if len(rows) < SHRINK_GUARD:
        print(f"[ERROR] only {len(rows)} records (< {SHRINK_GUARD}) — "
              f"accordion/markup change? aborting.")
        sys.exit(1)

    df = pd.DataFrame(rows)
    str_cols = [c for c in df.columns if c != "year_awarded"]
    df[str_cols] = df[str_cols].astype("string")
    df["year_awarded"] = df["year_awarded"].astype("Int64")

    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_raw", "pi_given", "pi_family", "institution",
              "scheme", "year_awarded"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    # Rows per fiscal-year era (descending).
    print("\nRows per fiscal year:")
    counts = df["fiscal_year_raw"].value_counts()
    for fyr in sorted(counts.index, key=lambda s: int(re.search(r"\d+", s).group()),
                      reverse=True):
        print(f"  {fyr}: {counts[fyr]}")

    out = args.output_dir / "tsc_alliance_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    print("\nSamples:")
    for _, r in df.head(3).iterrows():
        print(f"  - [{r['fiscal_year_raw']}] {r['funder_award_id']} | "
              f"{r['pi_given']} {r['pi_family']} | {r['institution']} | "
              f"{r['scheme']}\n      {r['title']}")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    else:
        print(f"\n[skip-upload] would upload to s3://{S3_BUCKET}/{S3_KEY}")
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
