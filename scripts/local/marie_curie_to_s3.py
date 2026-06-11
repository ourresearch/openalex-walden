#!/usr/bin/env python3
"""
Marie Curie (UK end-of-life care charity) to S3 Data Pipeline
=============================================================

Scrapes Marie Curie's research-funded projects and uploads a parquet to S3 for
Databricks ingestion.

Data source: mariecurie.org.uk/research-and-policy/research/projects (server-
    rendered listing, ~16 current project cards; default UA fine). Each detail
    page embeds the structured fields in a Next.js JSON/markdown island:
    `## Researchers / ### <name> / #### <role> / <institution>` plus a Grant
    Scheme name and `Duration: N months`. This is the UK end-of-life-care
    charity's own research programme — NOT the EU MSCA programme (that is in the
    EC/CORDIS ingest).

Fields: title, lead investigator (the researcher whose role is "Lead
    Investigator") + institution, grant scheme, duration. The charity publishes
    **no grant amounts and no explicit start dates** (duration only) -> amount/
    start_date NULL (section 6.7 waiver). No native ids (slug used).

Output: s3://openalex-ingest/awards/marie_curie/marie_curie_projects.parquet

Usage:
    python marie_curie_to_s3.py
    python marie_curie_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import html as ihtml
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

BASE = "https://www.mariecurie.org.uk"
LIST_PAGE = f"{BASE}/research-and-policy/research/projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/marie_curie/marie_curie_projects.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def parse_pi(raw):
    if not raw:
        return None, None
    first = _TITLE_RE.sub("", raw.strip()).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def get_slugs(session):
    r = session.get(LIST_PAGE, timeout=60)
    r.raise_for_status()
    return sorted(set(re.findall(
        r'href="(?:https://www\.mariecurie\.org\.uk)?/research-and-policy/research/projects/([a-z0-9-]+)"',
        r.text)))


def clean_inst(s):
    # cut the institution string at the first JSON/markup delimiter
    s = re.split(r'["\]<}]', s, 1)[0]
    return re.sub(r"\s+", " ", s).strip() or None


def parse_detail(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None

    u = ihtml.unescape(html.replace("\\n", "\n"))
    # researcher triples: ### name / #### role / institution
    pi = inst = None
    for m in re.finditer(r"###\s*([^\n#]+?)\s*\n+####\s*([^\n#]+?)\s*\n+([^\n#]+)", u):
        name, role, institution = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        if "lead" in role.lower():
            pi, inst = name, clean_inst(institution)
            break
        if pi is None:  # fallback to first researcher
            pi, inst = name, clean_inst(institution)
    dm = re.search(r"Duration:\s*(\d+)\s*months", u)
    duration = dm.group(1) if dm else None
    sm = re.search(r"\*\*\s*([A-Z][^*<\n]{3,60}?Grant[^*<\n]{0,30})\s*<br", u) \
        or re.search(r"([A-Z][A-Za-z ]{3,50}Grant Scheme)", u)
    scheme = re.sub(r"\s+", " ", sm.group(1)).strip() if sm else None

    pi_given, pi_family = parse_pi(pi)
    return {
        "funder_award_id": f"mariecurie-{slug}",
        "title": title,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": inst,
        "programme": scheme,
        "duration_months": duration,
        "landing_page_url": f"{BASE}/research-and-policy/research/projects/{slug}",
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
    ap = argparse.ArgumentParser(description="Marie Curie research projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/marie_curie_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Marie Curie (UK) research projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "text/html"})
    slugs = get_slugs(s)
    print(f"Project slugs: {len(slugs)}")
    if len(slugs) < 8:
        print(f"[ERROR] only {len(slugs)} slugs — listing layout change?")
        sys.exit(1)

    rows = []
    for n, slug in enumerate(slugs, 1):
        url = f"{BASE}/research-and-policy/research/projects/{slug}"
        html = None
        for _ in (1, 2):
            try:
                r = s.get(url, timeout=60)
                if r.status_code == 200:
                    html = r.text
                    break
                time.sleep(3)
            except Exception:
                time.sleep(3)
        if html is None:
            print(f"  [{n}] FAIL {slug}")
            continue
        rows.append(parse_detail(html, slug))
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "programme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "marie_curie_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
