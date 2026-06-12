#!/usr/bin/env python3
"""
Brain Aneurysm Foundation to S3 Data Pipeline
=============================================

Scrapes the Brain Aneurysm Foundation's research-grant award recipients and
uploads a parquet to S3 for Databricks ingestion.

Data source: bafound.org/research/research-grant-awards/. The site sits behind
    WP Engine's edge firewall, so plain `requests` gets a 403; DEFAULT-UA
    headless chromium (playwright) returns HTTP 200 instantly with no challenge
    (a custom UA is unnecessary — the default works). Flow:
      1. page.goto /research/research-grant-awards/ -> 200; wait for the first
         `.recipient-card` to render, then read page.content().
      2. same browser context: GET /page-sitemap.xml -> 200 XML, which lists
         the canonical recipient detail URLs under
         /research/research-grant-awards/<slug>/ (used to build stable
         funder_award_ids and landing-page URLs).
    The page is four year-accordion sections ("20XX Award Recipients", 2025-2022;
    pre-2022 does not exist). Only one tab is visually open at a time, but ALL
    four sections' markup is in the DOM (the closed ones are display:none) — so
    we parse page.content(), not the visible inner_text. Each recipient is one
    `li > div.recipient-card`:
        h3.recipient-name            -> name + trailing degrees
        p.recipient-institution      -> institution
        h6.recipient-project-label   -> "Project"
        p.recipient-project          -> title
        h5.recipient-award-amount    -> "Recipient of $XX,XXX" (USD)
        p.recipient-award-titles     -> one or more "<...> Chair of Research"
                                        sponsor lines (<br>-separated) -> joined
        a.arrow-link                 -> "Read Bio" href (its slug usually maps
                                        to the sitemap detail URL)
    A few h3s bundle two co-recipients in one card (joined by "&" / " and ");
    these stay one row — the full credit string goes in pi_raw and the FIRST
    person is parsed into pi_given/pi_family. The year comes from the enclosing
    accordion heading. USD amounts. Cerebral-aneurysm research funder on the
    IAMHRF expanded list.

    funder_award_id: the bio-link slug is matched against the sitemap detail
    URLs; on a hit the id is "baf-<slug>" (deduped with year/title when one
    investigator holds several awards), landing_page_url is the canonical detail
    URL. On a miss (e.g. a card whose "Read Bio" link is mis-pointed at another
    recipient's page) we fall back to "baf-<year>-<md5(name|title)[:10]>" and the
    awards page as the landing URL.

Output: s3://openalex-ingest/awards/brain_aneurysm/brain_aneurysm_grants.parquet
"""

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.bafound.org"
AWARDS_URL = f"{BASE}/research/research-grant-awards/"
SITEMAP_URL = f"{BASE}/page-sitemap.xml"
DETAIL_PREFIX = f"{BASE}/research/research-grant-awards/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/brain_aneurysm/brain_aneurysm_grants.parquet"

# trailing academic degrees on a name ("Julian Clarke, MD, MA")
DEGREE_RE = re.compile(
    r",?\s*\b(MD|PhD|Ph\.?D|MBBS|MBChB|MBBChir|MA|MS|MSc|MSE|MPH|DO|DVM|DDS|"
    r"PharmD|ScD|DrPH|RN|RD|MBA|BSN|MSN|FAANS|FAANP|FACS|FAAP|FRCP|FRCS|"
    r"FAHA|FANA)\b\.?", re.I)
# generational suffix so family-name isn't "III"/"Jr"
SUFFIX_RE = re.compile(r"\s+\b(?:Jr|Sr|II|III|IV)\b\.?$", re.I)
# split a credit string on co-recipient separators ("&", " and ", "., and")
PERSON_SPLIT_RE = re.compile(r"\s*&\s*|\s+and\s+", re.I)
AMOUNT_RE = re.compile(r"\$\s*([\d,]+)")
YEAR_RE = re.compile(r"(20\d{2})")


def parse_pi(raw):
    """First co-recipient -> (given, family). Strip degrees + suffixes.

    Commas inside a name only separate degrees, so split people on "&"/"and"
    (not commas) to isolate the first person, then drop trailing degrees.
    """
    if not raw:
        return None, None
    first = PERSON_SPLIT_RE.split(raw)[0].strip()
    first = DEGREE_RE.sub("", first).strip().rstrip(",.").strip()
    first = SUFFIX_RE.sub("", first).strip()
    first = re.sub(r"\s+", " ", first)
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return None, first or None
    return " ".join(parts[:-1]), parts[-1]


def _text(node):
    return node.get_text(" ", strip=True) if node else None


def sponsor_text(node):
    """recipient-award-titles -> Chair-of-Research line(s), <br>-joined."""
    if not node:
        return None
    for br in node.find_all("br"):
        br.replace_with("\n")
    lines = [re.sub(r"\s+", " ", x).strip()
             for x in node.get_text("\n").split("\n") if x.strip()]
    return " | ".join(lines) or None


def year_for_grid(grid):
    """Year from the nearest preceding '(20XX) Award Recipients' heading."""
    node = grid
    while node is not None:
        prev = node.find_previous(class_="accordion-heading")
        if prev:
            m = YEAR_RE.search(prev.get_text())
            if m:
                return m.group(1)
        node = node.parent
    return None


def slug_from_href(href):
    return href.rstrip("/").split("/")[-1] if href else None


def fetch_sitemap_details(ctx):
    """{slug: canonical detail URL} for recipient pages under the awards path."""
    r = ctx.request.get(SITEMAP_URL, timeout=40000)
    print(f"  sitemap status={r.status}")
    if r.status != 200:
        print("  [WARN] sitemap unavailable — all ids will be synthesized")
        return {}
    out = {}
    for loc in re.findall(r"<loc>([^<]+)</loc>", r.text()):
        if loc.startswith(DETAIL_PREFIX):
            slug = loc.rstrip("/").split("/")[-1]
            if slug and slug != "research-grant-awards":
                out[slug] = loc
    print(f"  sitemap recipient detail URLs: {len(out)}")
    return out


def synth_id(year, name, title):
    key = f"{name or ''}|{title or ''}".lower()
    return "baf-%s-%s" % (year or "x", hashlib.md5(key.encode()).hexdigest()[:10])


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


def scrape(ctx, page, sitemap):
    """Parse all four year sections into award rows."""
    page.goto(AWARDS_URL, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_selector("div.recipient-card", timeout=30000)
    except Exception:
        print("[WARN] recipient cards never rendered; parsing whatever loaded")
    soup = BeautifulSoup(page.content(), "html.parser")

    grids = soup.select("ul.recipient-grid")
    print(f"  recipient grids: {len(grids)}")

    rows, seen_ids = [], set()
    for grid in grids:
        year = year_for_grid(grid)
        cards = grid.select("div.recipient-card")
        print(f"  {year}: {len(cards)} recipients")
        for card in cards:
            name = _text(card.select_one("h3.recipient-name"))
            title = _text(card.select_one("p.recipient-project"))
            institution = _text(card.select_one("p.recipient-institution"))
            sponsor = sponsor_text(card.select_one("p.recipient-award-titles"))
            amt_raw = _text(card.select_one("h5.recipient-award-amount")) or ""
            m = AMOUNT_RE.search(amt_raw)
            amount = m.group(1).replace(",", "") if m else None
            bio_href = (card.select_one("a.arrow-link") or {}).get("href") \
                if card.select_one("a.arrow-link") else None
            slug = slug_from_href(bio_href)

            # slug-based id + canonical landing URL when the bio slug is a real
            # sitemap detail page; otherwise synthesize.
            if slug and slug in sitemap:
                aid = f"baf-{slug}"
                if aid in seen_ids:                       # same PI, many awards
                    aid = f"baf-{slug}-{year}"
                if aid in seen_ids:                       # >1 award same year
                    aid = f"baf-{slug}-{year}-" \
                          f"{hashlib.md5((title or '').encode()).hexdigest()[:6]}"
                landing = sitemap[slug]
            else:
                aid = synth_id(year, name, title)
                landing = AWARDS_URL

            if aid in seen_ids:
                continue
            seen_ids.add(aid)

            pi_given, pi_family = parse_pi(name)
            rows.append({
                "funder_award_id": aid,
                "title": title,
                "pi_given": pi_given,
                "pi_family": pi_family,
                "pi_raw": name,
                "institution": institution,
                "amount": amount,
                "year_awarded": year,
                "sponsor": sponsor,
                "landing_page_url": landing,
            })
    return rows


def main():
    ap = argparse.ArgumentParser(description="Brain Aneurysm Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/baf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Brain Aneurysm Foundation research grants -> S3")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()  # DEFAULT UA -> 200 (no WPE challenge)
        page = ctx.new_page()
        print("Fetching sitemap...")
        sitemap = fetch_sitemap_details(ctx)
        print("Loading awards page...")
        rows = scrape(ctx, page, sitemap)
        browser.close()

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    by_year = df["year_awarded"].value_counts().sort_index(ascending=False)
    print("Rows per year:")
    for yr, n in by_year.items():
        print(f"  {yr}: {n}")
    for c in ("title", "pi_family", "institution", "amount", "year_awarded",
              "sponsor"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    if len(df) < 50:
        print(f"[ERROR] only {len(df)} rows — expected ~62; "
              f"DOM/selectors changed?")
        sys.exit(1)

    out = args.output_dir / "brain_aneurysm_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
