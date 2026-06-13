#!/usr/bin/env python3
"""
Global Lyme Alliance (US) to S3 Data Pipeline
=============================================

Scrapes Global Lyme Alliance current grantees (globallymealliance.org/grantees)
and uploads a parquet to S3.

Data source: the /grantees page lists current grantees, each as a heading
"Name, Ph.D." followed by the project title and an abstract paragraph. Prior-year
grantees sit behind a HubSpot JS filter (querystring ignored) and institution/
amount are not published — so this is current grantees only (PI + title +
abstract). ~6 records. provenance `gla`, priority 344. F4320315262 (Path A).

Output: s3://openalex-ingest/awards/gla/gla_grants.parquet
Usage: python gla_to_s3.py [--limit N] [--skip-upload]
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

URL = "https://www.globallymealliance.org/grantees"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/gla/gla_grants.parquet"
EXPECTED_MIN = 4
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
NAME_DEG = re.compile(r"^(.+?),\s*(Ph\.?D|M\.?D|D\.?Phil|DVM|Sc\.?D)", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = clean(raw) or ""
    n = n.split(",")[0].strip()
    toks = n.split()
    if len(toks) < 2: return (None, toks[0]) if toks else (None, None)
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
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/gla_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("Global Lyme Alliance grantees -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    soup = BeautifulSoup(s.get(URL, timeout=40).text, "html.parser")
    rows, seen = [], set()
    for h in soup.find_all(["h2", "h3", "h4", "strong"]):
        t = clean(h.get_text(" ", strip=True))
        if not t or not NAME_DEG.match(t):
            continue
        g, f = split_name(t)
        if not f:
            continue
        # title + abstract = following sibling text blocks
        segs = []
        for nx in h.find_all_next(string=True, limit=12):
            x = clean(nx)
            if x and not NAME_DEG.match(x) and len(x) > 10:
                segs.append(x)
            if len(segs) >= 2:
                break
        title = segs[0] if segs else None
        desc = segs[1] if len(segs) > 1 else None
        if not title:
            continue
        aid = f"gla-{slug(f)}-{slug(title[:30])}"
        if aid in seen: continue
        seen.add(aid)
        rows.append({"funder_award_id": aid, "title": title, "pi_given": g, "pi_family": f,
                     "description": desc, "landing_page_url": URL})
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    if a.limit: rows = rows[:a.limit]
    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "description"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "gla_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.title)[:50]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
