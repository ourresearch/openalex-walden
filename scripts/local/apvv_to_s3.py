#!/usr/bin/env python3
"""
APVV (Slovak Research and Development Agency) to S3 Data Pipeline
================================================================

Scrapes the APVV funded-projects database and uploads a parquet to S3 for
Databricks ingestion. APVV is OpenAlex funder F4320323251 (Agentúra na Podporu
Výskumu a Vývoja, 26,398 works); existing award coverage is Crossref-derived
only, so this dedicated ingest adds canonical project metadata incl. EUR amounts.

Data source: https://www.apvv.sk/databaza-financovanych-projektov.html — a
    server-rendered funded-projects DB. The XLS export (?xls=1) 500s server-side,
    but the HTML listing paginates cleanly:
      1. GET the page -> a per-session `_token` (CSRF) + cookies.
      2. POST the page (action="", method=post) with `_token`, all empty search
         filters, NO `project_calls[]` filter, AND `select_index=N` to page.
         ~15 rows/page, final page select_index≈362 (~5,400 projects).
         (GET ?select_index=N alone does NOT work — needs the session POST.)
         NOTE: selecting the 34 `project_calls[]` options in the dropdown returns
         only the ~1,168 recent "VV" general-call projects; OMITTING the call
         filter returns the FULL corpus across every program type — APVT (the
         predecessor APVT agency), APVV general calls, SK-* bilateral (SK-FR,
         SK-AT, ...), VV-MVP, VVCE, etc.
      3. Each data row = 6 <td>: project number (e.g. APVV-15-0254, APVT-10-035702,
         SK-FR-0019-11), title, recipient (institution [- faculty]), approved
         support (EUR "200 000.00"), call (VV 2017), science field. No PI in the
         listing (org-level; §6.7) — PIs live only in per-project final-card PDFs
         (5k+, out of scope).

Output: s3://openalex-ingest/awards/apvv/apvv_grants.parquet
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

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
URL = "https://www.apvv.sk/databaza-financovanych-projektov.html"
LANDING = URL
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/apvv/apvv_grants.parquet"
ID_RE = re.compile(r"^[A-Za-z]{2,}[-A-Za-z0-9]*\d{2}")  # APVT-/APVV-/SK-*/VV-MVP-/VVCE- (all program types)
_NULLISH = {"", "nan", "none", "-", "—", "n/a"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", str(v))).strip()
    s = s.strip(" -")
    return None if s.lower() in _NULLISH else s


def parse_amount(raw):
    s = clean(raw)
    if not s:
        return None
    num = re.sub(r"[^\d,.]", "", s.replace("\xa0", "").replace(" ", ""))
    # Slovak: space thousands (already removed), '.' or ',' decimal
    if "," in num and "." not in num:
        num = num.replace(",", ".")
    else:
        num = num.replace(",", "")
    try:
        v = float(num)
        return str(int(round(v))) if v > 0 else None
    except ValueError:
        return None


def parse_rows(html):
    out = []
    for row in re.findall(r"<tr[^>]*>.*?</tr>", html, re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
        if len(cells) < 6:
            continue
        vals = [clean(c) for c in cells]
        if not vals[0] or not ID_RE.match(vals[0]):
            continue
        out.append(vals)
    return out


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
    ap = argparse.ArgumentParser(description="APVV funded projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/apvv_data"))
    ap.add_argument("--max-pages", type=int, default=500)
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("APVV (Slovak R&D Agency) funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    r = s.get(URL, timeout=60)
    tok = re.search(r'name="_token" value="([^"]+)"', r.text).group(1)
    print("  token ok; querying ALL program types (project_calls[] omitted)")
    # Selecting the 34 VV calls returns only ~1,168; omitting project_calls[]
    # returns the full ~5,431 across APVT/APVV/SK-*/VV-MVP/VVCE program types.
    base = [("_token", tok), ("project_number", ""), ("project_name", ""),
            ("project_recipient", ""), ("science", ""), ("rating", ""),
            ("keywords", ""), ("description", "")]

    recs, seen, dry = [], set(), 0
    for n in range(args.max_pages):
        try:
            resp = s.post(URL, data=base + [("select_index", str(n))], timeout=120)
            rows = parse_rows(resp.text)
        except Exception as e:
            print(f"  [page {n}] ERROR {e}; retry once")
            time.sleep(2)
            rows = parse_rows(s.post(URL, data=base + [("select_index", str(n))], timeout=120).text)
        new = 0
        for v in rows:
            aid = v[0]
            if aid in seen:
                continue
            seen.add(aid)
            new += 1
            recs.append({
                "funder_award_id": aid,
                "title": v[1],
                "institution": v[2],
                "amount": parse_amount(v[3]),
                "currency": "EUR",
                "scheme": v[4],            # call (VV 2017, etc.)
                "science_field": v[5],
                "landing_page_url": LANDING,
            })
        if n % 25 == 0:
            print(f"  page {n:>3}: +{new} ({len(recs)} total)")
        dry = dry + 1 if new == 0 else 0
        if dry >= 2:                       # two consecutive empty/all-dup pages = end
            print(f"  stop at page {n} (no new rows)")
            break
        time.sleep(args.delay)

    df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "institution", "amount", "scheme", "science_field"):
        nn = df[c].notna().sum()
        print(f"  {c:14}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    out = args.output_dir / "apvv_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if len(df) < 3000:
            print(f"[ERROR] only {len(df)} rows — expected ~5,400; pagination broke?")
            sys.exit(1)
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
