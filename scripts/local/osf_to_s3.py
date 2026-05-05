#!/usr/bin/env python3
"""
Open Society Foundations to S3 (Playwright scrape)
==================================================

OSF publishes its awarded grants list at
https://www.opensocietyfoundations.org/grants/past with URL pagination
via `?page=N`. The page is JS-rendered (plain HTTP returns chrome only)
so we drive it with Playwright + headless Chromium.

Each page exposes 10 grant cards via `<div class="a-grantsDatabase__header">`
with text content shaped:  "{Grantee} {Year} ${Amount}".

Output: s3://openalex-ingest/awards/osf/osf_grants.parquet
Currency hardcoded USD.

Pipeline strategy:
  - Walk page=1, 2, 3, ... extracting card text.
  - Stop when a page returns the same first card as the previous page,
    or yields zero cards (typical end-of-listing signal).
  - Checkpoint every 25 pages.

Why Playwright over agent-browser CLI:
  agent-browser uses subprocess.run on Windows which doesn't reliably
  terminate child Chrome processes when the page never fires `load`,
  leading to orphan processes and hangs. Playwright Python is native
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
S3_KEY = "awards/osf/osf_grants.parquet"
LISTING_URL = "https://www.opensocietyfoundations.org/grants/past?page={page}"

# Default to the agent-browser Chrome install if present, else use Playwright's
# bundled Chromium. Override with --chrome-path.
DEFAULT_CHROME = r"C:\Users\kyled\.agent-browser\browsers\chrome-148.0.7778.97\chrome.exe"

# Card text shape: "{Grantee} {Year} ${Amount}".
CARD_RE = re.compile(
    r"^(?P<grantee>.+?)\s+(?P<year>\d{4})(?:\s+\$(?P<amount>[\d,]+))?\s*$"
)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_card(text: str) -> dict | None:
    m = CARD_RE.match(text.strip())
    if not m:
        return None
    g = m.groupdict()
    amt = g.get("amount")
    return {
        "grantee_name": (g["grantee"] or "").strip(),
        "year": int(g["year"]),
        "amount_usd": float(amt.replace(",", "")) if amt else None,
        "card_text": text.strip(),
    }


def scrape_page(page, page_num: int) -> list[dict]:
    url = LISTING_URL.format(page=page_num)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PWTimeout:
        # Some sites keep network busy indefinitely; the DOM may still be
        # there. Continue and let the locator find what's rendered.
        pass
    # Cards stream in over a few seconds after DOMContentLoaded
    page.wait_for_timeout(5000)
    cards = page.locator("div.a-grantsDatabase__header").all()
    parsed: list[dict] = []
    for c in cards:
        try:
            t = c.inner_text(timeout=2000).strip()
        except PWTimeout:
            continue
        # Cards collapse internal whitespace for parsing
        t = re.sub(r"\s+", " ", t)
        p = parse_card(t)
        if p:
            parsed.append(p)
    return parsed


def main() -> None:
    p = argparse.ArgumentParser(description="OSF -> parquet -> S3")
    p.add_argument("--resume-from-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=2000)
    p.add_argument("--checkpoint-every", type=int, default=25)
    p.add_argument("--limit-pages", type=int, default=None,
                   help="For smoke-test: only fetch first N pages")
    p.add_argument("--chrome-path", default=DEFAULT_CHROME,
                   help="Path to chrome.exe (default: agent-browser Chrome). Pass empty string to use Playwright's bundled Chromium.")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("OSF -> S3 (Playwright) starting")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "osf_grants.parquet"

    end_page = args.max_pages
    if args.limit_pages:
        end_page = args.resume_from_page + args.limit_pages - 1

    rows: list[dict] = []
    last_first_card: str | None = None

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
            for page_num in range(args.resume_from_page, end_page + 1):
                cards = scrape_page(page, page_num)
                if not cards:
                    log(f"[page {page_num}] zero cards — stopping (end of listing)")
                    break
                first_card = cards[0]["card_text"]
                if first_card == last_first_card:
                    log(f"[page {page_num}] same first card as previous — end of listing")
                    break
                last_first_card = first_card
                for c in cards:
                    c["page"] = page_num
                    c["downloaded_at"] = datetime.utcnow().isoformat()
                rows.extend(cards)
                if page_num == args.resume_from_page or page_num % 5 == 0:
                    log(f"[page {page_num}] +{len(cards)} cards (running: {len(rows):,})")
                if rows and len(rows) % (args.checkpoint_every * 10) == 0:
                    pd.DataFrame(rows).to_parquet(parquet_path, index=False)
                    log(f"  checkpoint: {len(rows):,} rows written")
        finally:
            ctx.close()
            browser.close()

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: grantee_name={df.grantee_name.notna().sum():,}, "
            f"year={df.year.notna().sum():,}, amount_usd={df.amount_usd.notna().sum():,}")
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
