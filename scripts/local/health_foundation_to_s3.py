#!/usr/bin/env python3
"""
The Health Foundation awards to S3 Data Pipeline
================================================

Scrapes The Health Foundation's "Projects (A-Z)" database and uploads a parquet
to S3 for Databricks ingestion. (OpenAlex funder F4320320265; awards wave 6 /
IAMHRF. Org-level thin build per runbook section 6.7 -- NO PI, NO amounts.)

Data source: health.org.uk. The whole site sits behind **Akamai**, which 403s
    plain `requests` AND Playwright **Chromium** headless ("403 Access Denied"),
    but serves a clean 200 to Playwright **Firefox** headless (do NOT use
    headless=False; do NOT use chromium). Flow:
      1. firefox.launch(headless=True) -> goto
         /work-we-support/projects?page=N -> 200.
      2. The A-Z DB self-reports "628 matches": pagination is ?page=0..62,
         10 cards/page, page 62 returns 8 (62*10 + 8 = 628), page 63 returns 0.
         Crawl all 63 pages in ONE Firefox context, throttling ~0.4s/page.
      3. Each card is an `article.c-list-item` carrying:
           h2/h3 a            -> project title + detail URL
           .c-list-item__meta -> "Project | 15 October 2025" (type | date)
           p                  -> 1-2 line summary
         There is NO PI and NO award amount anywhere on the listing (or detail)
         pages -- this is a title+date+programme+URL build only.

    `scheme` note: the per-project Programme NAME (e.g. "Economies for Healthier
    Lives") lives only on each detail page, where it is mixed in with the global
    programmes-nav links -- not reliably extractable without 628 extra fetches,
    which is out of scope for this listing-only thin build. So `scheme` is set to
    the Health Foundation funding-AREA section taken from the project URL
    ("Funding and partnerships" / "Funding and fellowships"), a deterministic,
    100%-filled grouping verified live (the two real first-path-segments). Live
    probe 2026-06-15: page 0 -> 200, "628 match", 10 cards; page 62 -> 8 cards;
    page 63 -> 0. funder_award_id = thf-<url-slug>.

    Bulk-export check (Kyle's STEP 0, done first): The Health Foundation is NOT a
    360Giving publisher (0 hits in registry.threesixtygiving.org data.json, 857
    publishers), has no matching data.gov.uk/CKAN dataset (171 "health
    foundation grants" hits are all unrelated NHS-trust spend reports), and
    health.org.uk offers no CSV/Excel download (robots.txt itself is Akamai-
    403'd). No clean bulk file exists -> scraping the A-Z DB is required.

Output: s3://openalex-ingest/awards/health_foundation/health_foundation_grants.parquet
    (S3 target WIRED; pass --skip-upload to dry-run without uploading.)
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.health.org.uk"
LIST_URL = BASE + "/work-we-support/projects?page={}"
MAX_PAGES = 200  # safety cap only; the crawl stops when a page returns 0 cards
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/health_foundation/health_foundation_grants.parquet"
SHRINK_GUARD = 500  # expect 628; abort if the crawl shrinks below this

YEAR_RE = re.compile(r"(?:19|20)\d\d")
COUNT_RE = re.compile(r"([\d,]+)\s+match", re.I)  # DB self-reports "628 matches"

# Extract every card on the current listing page (title, href, meta, summary).
CARD_JS = r"""() => Array.from(document.querySelectorAll('article.c-list-item')).map(a => {
    const t = a.querySelector('h2 a, h3 a');
    const meta = a.querySelector('.c-list-item__meta');
    const p = a.querySelector('.c-list-item__content > p, p');
    return {
        title: t ? t.textContent.trim() : null,
        href: t ? t.href : null,
        meta: meta ? meta.innerText.replace(/\s+/g, ' ').trim() : null,
        summary: p ? p.textContent.trim() : null,
    };
})"""


def clean(text):
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
    return t or None


def slug_of(href):
    """Last non-empty path segment of the detail URL."""
    path = urlsplit(href).path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else None


def section_of(href):
    """Health Foundation funding-AREA section -> scheme (e.g. 'Funding and partnerships')."""
    segs = [s for s in urlsplit(href).path.split("/") if s]
    if not segs:
        return None
    return segs[0].replace("-", " ").capitalize()


def year_of(meta):
    if not meta:
        return None
    m = YEAR_RE.search(meta)
    return int(m.group(0)) if m else None


def crawl():
    """Crawl listing pages until one returns 0 cards (Firefox-headless; Akamai-
    gated). Page 0 also yields the DB's self-reported "N matches" total so the
    caller can assert exact coverage instead of trusting a hard-coded page count
    (future projects beyond the last page are now picked up automatically).
    Returns (cards, advertised_total_or_None)."""
    cards, advertised = [], None
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)  # Akamai -> Firefox, NOT chromium
        page = browser.new_page()
        for n in range(MAX_PAGES):
            url = LIST_URL.format(n)
            resp = page.goto(url, timeout=60000, wait_until="domcontentloaded")
            status = resp.status if resp else None
            if n == 0:
                print(f"  GET {url} -> {status} | {page.title()!r}")
                if status != 200:
                    print(f"[ERROR] non-200 status {status} (Akamai? wrong engine?)")
                    browser.close()
                    sys.exit(1)
            try:
                page.wait_for_selector("article.c-list-item", timeout=15000)
            except Exception:
                pass  # may be a legitimately empty trailing page -> 0 cards below
            if n == 0:
                # parse the self-reported total AFTER hydration: the "628 matches"
                # header (like the page <title>) is JS-injected post-DOMContentLoaded,
                # so reading it before wait_for_selector returns None.
                m = COUNT_RE.search(page.inner_text("body"))
                advertised = int(m.group(1).replace(",", "")) if m else None
                print(f"  DB self-reports: {advertised} matches")
            page_cards = page.evaluate(CARD_JS)
            if not page_cards:
                print(f"  page {n:>2}: 0 cards — end of listing")
                break
            cards.extend(page_cards)
            if n % 10 == 0:
                print(f"  page {n:>2}: {len(page_cards)} cards (running total {len(cards)})")
            time.sleep(0.4)  # throttle ~0.4s/page
        else:
            print(f"[WARN] hit MAX_PAGES={MAX_PAGES} without an empty page")
        browser.close()
    return cards, advertised


def build_rows(cards):
    rows, seen = [], set()
    for c in cards:
        href, title = c.get("href"), clean(c.get("title"))
        if not href or not title:
            continue
        slug = slug_of(href)
        if not slug:
            continue
        aid = f"thf-{slug}"
        if aid in seen:  # dedupe by detail-URL slug (titles repeat across facets)
            continue
        seen.add(aid)
        summary = clean(c.get("summary"))
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "scheme": section_of(href),
            "year_awarded": year_of(c.get("meta")),
            "description": (summary[:300] if summary else None),
            "landing_page_url": href,
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
    ap = argparse.ArgumentParser(description="The Health Foundation projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/thf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("The Health Foundation projects -> S3")
    print("=" * 60)

    print("Crawling A-Z projects DB (Firefox headless; Akamai-gated)...")
    cards, advertised = crawl()
    rows = build_rows(cards)
    print(f"\nParsed {len(rows)} project records (from {len(cards)} cards)")
    # coverage gate: never silently under-crawl. If the DB advertised a total,
    # require the crawl to have reached it (catches truncated pagination); always
    # enforce the absolute shrink floor.
    if advertised and len(cards) < advertised:
        print(f"[ERROR] crawled {len(cards)} cards but DB reports {advertised} "
              f"matches — incomplete pagination, aborting.")
        sys.exit(1)
    if len(rows) < SHRINK_GUARD:
        print(f"[ERROR] only {len(rows)} records (< {SHRINK_GUARD}) — "
              f"pagination/markup change? aborting.")
        sys.exit(1)

    df = pd.DataFrame(rows)
    str_cols = [c for c in df.columns if c != "year_awarded"]
    df[str_cols] = df[str_cols].astype("string")
    df["year_awarded"] = df["year_awarded"].astype("Int64")

    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "scheme", "year_awarded", "description", "landing_page_url"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    print("\nScheme (funding-area) distribution:")
    for k, v in df["scheme"].value_counts(dropna=False).items():
        print(f"  {k}: {v}")
    print("\nYear range:",
          f"{df['year_awarded'].min()}-{df['year_awarded'].max()}")

    out = args.output_dir / "health_foundation_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    print("\nSamples:")
    for _, r in df.head(2).iterrows():
        print(f"  - {r['funder_award_id']} | {r['year_awarded']} | {r['scheme']}\n"
              f"      {r['title']}\n"
              f"      {r['description']}\n"
              f"      {r['landing_page_url']}")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    else:
        print(f"\n[skip-upload] would upload to s3://{S3_BUCKET}/{S3_KEY}")
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
