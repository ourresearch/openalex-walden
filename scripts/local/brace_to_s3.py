#!/usr/bin/env python3
"""
BRACE (Alzheimer's research charity, UK) to S3 Data Pipeline
============================================================

Scrapes BRACE funded-research projects (alzheimers-brace.org) and uploads a
parquet to S3.

Data source: static HTML theme-listing pages (plain GET + browser UA):
    /causes-and-understanding/  /treatments-and-prevention/  /dementia-diagnosis/
Each project is an <h2> = researcher name, followed by a <p> = project
title/description; the theme (page) becomes funder_scheme. ~44 projects. No
amount/institution/year published -> NULL (§6.7 waiver). provenance `brace`,
priority 337. F4320312938 (Path A).

Output: s3://openalex-ingest/awards/brace/brace_projects.parquet
Usage: python brace_to_s3.py [--limit N] [--skip-upload]
Author: OpenAlex Team
"""
import argparse, re, subprocess, sys
from pathlib import Path
import pandas as pd, requests
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass
from bs4 import BeautifulSoup

PAGES = {
    "Causes and understanding": "https://www.alzheimers-brace.org/causes-and-understanding/",
    "Treatments and prevention": "https://www.alzheimers-brace.org/treatments-and-prevention/",
    "Dementia diagnosis": "https://www.alzheimers-brace.org/dementia-diagnosis/",
}
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/brace/brace_projects.parquet"
EXPECTED_MIN = 30
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)
SECTION = re.compile(r"^(Treatments|Causes|Dementia|Subscribe|BRACE|SWDBB|Our |Research|Donate|Latest)", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n: return None, None
    n = _TITLE_RE.sub("", n).strip()
    toks = n.split()
    if not toks: return None, None
    if len(toks) == 1: return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/brace_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("BRACE funded research -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA; s.headers["Accept-Language"] = "en-GB,en;q=0.9"
    rows, seen = [], set()
    for theme, url in PAGES.items():
        r = s.get(url, timeout=40)
        if r.status_code != 200:
            print(f"  {theme}: HTTP {r.status_code}"); continue
        soup = BeautifulSoup(r.text, "html.parser")
        for h in soup.find_all(["h2", "h3"]):
            name = clean(h.get_text(" ", strip=True))
            if not name or SECTION.match(name) or len(name.split()) > 6:
                continue
            p = h.find_next("p")
            title = clean(p.get_text(" ", strip=True)) if p else None
            if not title or len(title) < 15:
                continue
            g, f = split_name(name)
            if not f:
                continue
            aid = f"brace-{slug(name)}-{slug(title[:30])}"
            if aid in seen: continue
            seen.add(aid)
            rows.append({"funder_award_id": aid, "title": title, "pi_given": g,
                         "pi_family": f, "funder_scheme": theme,
                         "landing_page_url": url})
        print(f"  {theme}: {len(rows)} total")
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    if a.limit: rows = rows[:a.limit]
    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "funder_scheme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "brace_projects.parquet"; df.to_parquet(out, index=False)
    print("Wrote", out)
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
