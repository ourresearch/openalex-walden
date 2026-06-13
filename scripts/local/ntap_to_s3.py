#!/usr/bin/env python3
"""
Neurofibromatosis Therapeutic Acceleration Program (NTAP) to S3 Pipeline
========================================================================

Scrapes NTAP's index of funded NF1 initiatives (n-tap.org, Johns Hopkins) and
uploads a parquet to S3 for Databricks ingestion.

Data source: server-rendered HTML index at
    https://www.n-tap.org/for-researchers/index-of-funded-nf1-initiatives-research
Each project is a `div.details` with `div.title` (project title), a `<p>`
description, and a sequence of `div.researcher_name` blocks carrying — in order —
study type, PI(s) (name + degrees, ";"-separated), institution, and location.
~91 projects. Amounts and years are not published on the index -> NULL (§6.7
waiver). Country derived from the location line. provenance `ntap`, priority 330.
F4320333709 (Path A; dup F4320315263 merge-flag).

Output: s3://openalex-ingest/awards/ntap/ntap_projects.parquet

Usage:
    python ntap_to_s3.py
    python ntap_to_s3.py --limit 10
    python ntap_to_s3.py --skip-upload

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

URL = "https://www.n-tap.org/for-researchers/index-of-funded-nf1-initiatives-research"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ntap/ntap_projects.parquet"
EXPECTED_MIN = 60
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

INST_KEY = re.compile(r"Universit|College|Institute|Hospital|School|Center|Centre|"
                      r"Clinic|Health Network|Research|Foundation|Children|Laborator", re.I)
DEGREE = re.compile(r"\b(Ph\.?D|M\.?D|D\.?Phil|M\.?B\.?B\.?S|D\.?O|M\.?Sc|Pharm\.?D|"
                    r"D\.?V\.?M|Sc\.?D|M\.?P\.?H|B\.?S)\.?\b", re.I)
LOC = re.compile(r"^.+,\s*(USA|United States|Canada|UK|United Kingdom|Germany|France|"
                 r"Australia|Israel|Italy|Spain|Netherlands|Sweden|Switzerland|Japan|"
                 r"China|Belgium|Austria|Denmark|Finland|Norway|Ireland|[A-Z][a-z]+)\s*$")
STUDY_WORDS = re.compile(r"study|grant|program|tool|resource|initiative|fellowship|"
                         r"clinical|preclinical|consortium|network|award", re.I)
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n:
        return None, None
    n = _TITLE_RE.sub("", n).strip()
    n = n.split(",")[0].strip()          # drop trailing degrees
    n = DEGREE.sub("", n).strip(" .,")
    toks = n.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def classify(divs):
    """Assign the researcher_name divs to (study_type, pi, institution, location).

    The blocks are consistently four divs in fixed order
    [study_type, PI(s), institution, location], so assign positionally. Fall back
    to content heuristics only if the count differs (PI = has a degree token;
    location = 'City, Country'; institution = has an institution keyword).
    """
    vals = [clean(d.get_text(" ", strip=True)) for d in divs]
    vals = [v for v in vals if v]
    if len(vals) == 4:
        return vals[0], vals[1], vals[2], vals[3]
    study = pi = inst = loc = None
    for t in vals:
        if pi is None and DEGREE.search(t):
            pi = t
        elif loc is None and LOC.match(t) and not INST_KEY.search(t):
            loc = t
        elif inst is None and INST_KEY.search(t):
            inst = t
        elif study is None:
            study = t
    return study, pi, inst, loc


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
    ap = argparse.ArgumentParser(description="NTAP projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ntap_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("NTAP funded NF1 initiatives -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    soup = BeautifulSoup(s.get(URL, timeout=60).text, "html.parser")
    details = soup.select("div.details")
    print(f"project blocks: {len(details)}")

    rows, seen = [], set()
    for d in details:
        tnode = d.select_one(".title")
        title = clean(tnode.get_text(" ", strip=True)) if tnode else None
        if not title:
            continue
        desc = None
        p = d.find("p")
        if p:
            desc = clean(p.get_text(" ", strip=True))
        study, pi_raw, inst, loc = classify(d.select(".researcher_name"))
        first_pi = re.split(r";| and (?=[A-Z])", pi_raw)[0] if pi_raw else None
        given, family = split_name(first_pi)
        country = None
        if loc:
            m = LOC.match(loc)
            if m:
                country = {"USA": "United States", "UK": "United Kingdom"}.get(m.group(1), m.group(1))
        aid = f"ntap-{slugify(title)}"
        if aid in seen:
            aid = f"{aid}-{len(rows)}"
        seen.add(aid)
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "description": desc,
            "pi_given": given,
            "pi_family": family,
            "institution": inst,
            "country": country,
            "funder_scheme": study,
            "landing_page_url": URL,
        })
        if args.limit and len(rows) >= args.limit:
            break

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "country", "description"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    out = args.output_dir / "ntap_projects.parquet"
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
