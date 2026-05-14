#!/usr/bin/env python3
"""
Robert Wood Johnson Foundation to S3 (Playwright scrape)
========================================================

RWJF publishes its complete grants explorer at
https://www.rwjf.org/en/grants/awarded-grants.html with URL pagination
via `?s=N` (1-indexed). Page 1 reports the corpus stats:
  31,717 grants totaling $14.4B across 1972-2026, 15 grants/page,
  ~2,115 pages.

The page is JS-rendered (Adobe Experience Manager front-end + JS data
load); plain HTTP returns chrome only. We drive it with Playwright +
headless Chromium.

After DOMContentLoaded the grants list streams in over ~5-8 seconds.
Each grant is rendered as a series of "Label:\nValue\n." entries:
  Grant Title: ...
  Location: ...
  Year Awarded: YYYY
  Amount Awarded: N,NNN
  Program Area: ...        (sometimes plural: "Program Areas:")

Output: s3://openalex-ingest/awards/rwj/rwj_grants.parquet
Currency hardcoded USD.

Why Playwright over agent-browser CLI:
  agent-browser uses subprocess.run on Windows which doesn't reliably
  terminate child Chrome processes when the page never fires `load`
  (RWJF keeps network busy indefinitely). Playwright Python is native
  and has clean timeout semantics.
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rwj/rwj_grants.parquet"
LISTING_URL = "https://www.rwjf.org/en/grants/awarded-grants.html?s={page}"

DEFAULT_CHROME = r"C:\Users\kyled\.agent-browser\browsers\chrome-148.0.7778.97\chrome.exe"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# Each grant on the rendered page is formatted as a series of "Label:\nValue\n."
# entries. We split on "Grant Title:" and parse the labelled fields.
def parse_grants(page_text: str, page_num: int) -> list[dict]:
    chunks = re.split(r"\n(?=Grant Title:)", page_text)
    out: list[dict] = []
    for chunk in chunks:
        if "Grant Title:" not in chunk:
            continue
        chunk = chunk.split("\nResults\n")[0]
        chunk = chunk.split("\nFooter\n")[0]

        def field(label: str) -> str | None:
            # Primary: "Label:\n{value}\n.\n" (followed by another field)
            m = re.search(rf"{re.escape(label)}\s*:\s*\n?\s*(.+?)\s*\n\s*\.\s*\n", chunk, re.S)
            if m:
                return m.group(1).strip()
            # Fallback for last field of a chunk: stop at next "Label:\n", end of
            # chunk, OR a lone "." line (which terminates the value).
            m = re.search(
                rf"{re.escape(label)}\s*:\s*\n?\s*(.+?)(?=\n\s*\.\s*(?:\n|$)|\n[A-Z][^:]*:\s*\n|\Z)",
                chunk, re.S,
            )
            if not m:
                return None
            v = m.group(1).strip()
            # Trim trailing standalone period that some chunks include
            v = re.sub(r"\s*\n\s*\.\s*$", "", v).strip()
            return v

        title = field("Grant Title")
        location = field("Location")
        year = field("Year Awarded")
        amount = field("Amount Awarded")
        prog = field("Program Area") or field("Program Areas")
        if not title:
            continue
        amount_clean = (amount or "").replace(",", "").replace(".", "")
        out.append({
            "page": page_num,
            "grant_title": title,
            "location": location,
            "year_awarded": int(year) if year and year.isdigit() else None,
            "amount_usd": float(amount.replace(",", "")) if amount and amount_clean.isdigit() else None,
            "program_area": prog,
            "downloaded_at": datetime.utcnow().isoformat(),
        })
    return out


def scrape_page(page, page_num: int) -> tuple[list[dict], tuple[int, int] | None]:
    url = LISTING_URL.format(page=page_num)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PWTimeout:
        pass
    page.wait_for_timeout(8000)
    text = page.inner_text("body")
    grants = parse_grants(text, page_num)
    if len(grants) < 5:
        # First render may have only a few grants — wait longer
        page.wait_for_timeout(5000)
        text = page.inner_text("body")
        grants = parse_grants(text, page_num)
    page_of = None
    m = re.search(r"page\s+(\d+)\s+of\s+(\d+)", text, re.I)
    if m:
        page_of = (int(m.group(1)), int(m.group(2)))
    return grants, page_of


def main() -> None:
    p = argparse.ArgumentParser(description="RWJF -> parquet -> S3")
    p.add_argument("--resume-from-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=2200,
                   help="Safety upper bound; updated dynamically from page 1's 'page X of Y'")
    p.add_argument("--checkpoint-every", type=int, default=50)
    p.add_argument("--limit-pages", type=int, default=None,
                   help="For smoke-test: only fetch first N pages")
    p.add_argument("--chrome-path", default=DEFAULT_CHROME,
                   help="Path to chrome.exe (default: agent-browser Chrome). Empty string = Playwright bundled Chromium.")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("RWJF -> S3 (Playwright) starting")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "rwj_grants.parquet"

    rows: list[dict] = []
    total_pages = args.max_pages
    if args.limit_pages:
        total_pages = args.resume_from_page + args.limit_pages - 1

    with sync_playwright() as pw:
        launch_kwargs = {"headless": True}
        if args.chrome_path:
            launch_kwargs["executable_path"] = args.chrome_path
        browser = pw.chromium.launch(**launch_kwargs)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)",
        )
        page = ctx.new_page()
        try:
            page_num = args.resume_from_page
            # Termination is `page_num <= total_pages`. total_pages is set from
            # page 1's parsed "X of Y" header (defaults to args.max_pages if the
            # parse fails). Empty pages in the middle are logged but not treated
            # as end-of-corpus — observed in oxjobs #161: a prior run bailed at
            # page 321 with 4,800 rows after Playwright returned empty content on
            # one page; the site actually had 2,116 pages × ~15 grants each.
            while page_num <= total_pages:
                grants, page_of = scrape_page(page, page_num)
                if page_of and page_num == args.resume_from_page:
                    total_pages = min(total_pages, page_of[1])
                    log(f"RWJF reports page {page_of[0]} of {page_of[1]} (capping at {total_pages})")
                if not grants:
                    log(f"[page {page_num}/{total_pages}] zero grants — continuing")
                else:
                    rows.extend(grants)
                if page_num == args.resume_from_page or page_num % 5 == 0:
                    log(f"[page {page_num}/{total_pages}] +{len(grants)} grants (running: {len(rows):,})")
                if rows and len(rows) % (args.checkpoint_every * 15) == 0:
                    pd.DataFrame(rows).to_parquet(parquet_path, index=False)
                    log(f"  checkpoint: {len(rows):,} rows written")
                page_num += 1
        finally:
            ctx.close()
            browser.close()

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: grant_title={df.grant_title.notna().sum():,}, "
            f"amount_usd={df.amount_usd.notna().sum():,}, "
            f"year_awarded={df.year_awarded.notna().sum():,}, "
            f"program_area={df.program_area.notna().sum():,}")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
