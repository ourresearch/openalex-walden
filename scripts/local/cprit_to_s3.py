#!/usr/bin/env python3
"""
CPRIT (Cancer Prevention and Research Institute of Texas) -> S3 Data Pipeline
============================================================================

Scrapes the CPRIT funded-grants database and uploads a parquet to S3 for
Databricks ingestion. CPRIT is OpenAlex funder F4320308129 (ROR 0003xa228, DOI
10.13039/100004917, ~12,959 works); existing award coverage is Crossref-derived,
so this dedicated ingest adds canonical project metadata (native ids, PIs,
institutions, USD amounts, award dates) for ~2,285 grants totalling ~$4.14B.

Source: https://cprit.texas.gov/grants-funded/  (formerly www.cprit.state.tx.us,
now 301 -> texas.gov, behind Cloudflare).
Mechanism:
  - A cold requests/curl GET hits a Cloudflare "Just a moment" managed challenge.
  - HEADLESS Playwright chromium clears that challenge automatically on load (no
    interactive solve, no headed browser) and lands on the real grants page.
  - The grants table is a client-side DataTables instance (`#grants`) with NO ajax
    source (serverSide:false) — all rows live in the DataTable's JS data store.
    We pull every row via the DataTable API (`dt.rows().every`), bypassing the
    10-row client pagination. No per-detail-page fetches needed for these columns.

Field mapping (8 table cols): Grant ID -> funder_award_id (+ landing url from the
row link); Title -> title; Program -> scheme; PI/PD "Family, Given" -> person
lead; Organization -> institution; Cancer Types -> description; Award Date
YYYY/MM/DD -> start_date_raw (no end date published); Award Amount "$N"/"$N*"
-> amount (USD int; trailing '*' = pending contract negotiation, kept).
Landmines handled: 17 "Pending" recruitment awards (PI + landing nulled);
CPRIT-Scholar rows link to a /cprit-scholars/ page (kept as the real landing).

Output: s3://openalex-ingest/awards/cprit/cprit_grants.parquet
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

URL = "https://cprit.texas.gov/grants-funded/"
ORIGIN = "https://cprit.texas.gov"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cprit/cprit_grants.parquet"
COLS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family", "institution",
        "amount", "currency", "scheme", "start_date_raw", "end_date_raw",
        "description", "landing_page_url"]


def fetch_rows():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, viewport={"width": 1366, "height": 900},
                                  locale="en-US")
        page = ctx.new_page()
        page.goto(URL, wait_until="networkidle", timeout=90000)
        time.sleep(5)  # let Cloudflare managed-challenge JS + DataTables init settle
        if "just a moment" in page.title().lower():
            for _ in range(8):
                time.sleep(5)
                if "just a moment" not in page.title().lower():
                    break
            else:
                raise RuntimeError("Cloudflare challenge did not clear headless")

        meta = page.evaluate("""() => {
            if (!(window.jQuery && jQuery.fn.dataTable
                  && jQuery.fn.dataTable.isDataTable('#grants'))) return null;
            let dt = jQuery('#grants').DataTable();
            return {total: dt.rows().count(), serverSide: dt.page.info().serverSide};
        }""")
        if not meta:
            raise RuntimeError("#grants DataTable not found")
        print(f"  DataTable rows={meta['total']} serverSide={meta['serverSide']}")

        rows = page.evaluate("""() => {
            let dt = jQuery('#grants').DataTable();
            let out = [];
            dt.rows().every(function () {
                let tds = Array.from(this.node().querySelectorAll('td'));
                let a = tds[0].querySelector('a');
                out.push({
                    grant_id: tds[0].innerText.trim(),
                    href:     a ? a.getAttribute('href') : null,
                    title:    tds[1].innerText.trim(),
                    program:  tds[2].innerText.trim(),
                    pi:       tds[3].innerText.trim(),
                    org:      tds[4].innerText.trim(),
                    cancer:   tds[5].innerText.trim(),
                    date:     tds[6].innerText.trim(),
                    amount:   tds[7].innerText.trim()
                });
            });
            return out;
        }""")
        browser.close()
        return rows


def nn(s):
    if s is None:
        return None
    s = s.strip()
    return s or None


def parse_amount(raw):
    digits = re.sub(r"[^\d]", "", (raw or "").strip())   # '$899,999*' -> '899999'
    return digits if digits else None


def parse_date(raw):
    raw = (raw or "").strip()
    m = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"^(\d{4})$", raw)
    return f"{m.group(1)}-01-01" if m else None


def parse_pi(raw):
    raw = (raw or "").strip()
    if not raw or raw.lower() == "pending":
        return None, None, None
    if "," in raw:
        family, given = [x.strip() for x in raw.split(",", 1)]
        family = family or None
        given = given or None
        full = f"{given} {family}" if (given and family) else (family or given)
        return nn(full), given, family
    return raw, None, raw


def build_records(rows):
    recs = []
    for r in rows:
        href = nn(r.get("href"))
        full, given, family = parse_pi(r.get("pi"))
        recs.append({
            "funder_award_id": nn(r.get("grant_id")),
            "title": nn(r.get("title")),
            "pi_full": full, "pi_given": given, "pi_family": family,
            "institution": nn(r.get("org")),
            "amount": parse_amount(r.get("amount")),
            "currency": "USD",
            "scheme": nn(r.get("program")),
            "start_date_raw": parse_date(r.get("date")),
            "end_date_raw": None,
            "description": nn(r.get("cancer")),
            "landing_page_url": (ORIGIN + href) if href else None,
        })
    return recs


def upload_to_s3(local_path):
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
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
    ap = argparse.ArgumentParser(description="CPRIT funded grants to S3")
    ap.add_argument("--output-dir", default="/tmp/cprit_data")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("CPRIT (Cancer Prevention and Research Institute of Texas) -> S3")
    print("=" * 60)

    rows = fetch_rows()
    with open(os.path.join(args.output_dir, "cprit_raw.json"), "w") as f:
        json.dump(rows, f)
    df = pd.DataFrame(build_records(rows), columns=COLS).astype("object")
    df = df.where(df.notna(), None)
    for c in COLS:
        df[c] = df[c].astype("string")

    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw", "scheme", "description"):
        nnz = int(df[c].notna().sum())
        print(f"  {c:16}: {nnz}/{len(df)} ({round(100*nnz/max(len(df),1))}%)")

    if len(df) < 1800:
        print(f"[ERROR] only {len(df)} rows — expected ~2,285; source/Cloudflare changed?")
        sys.exit(1)

    out = os.path.join(args.output_dir, "cprit_grants.parquet")
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({os.path.getsize(out)/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
