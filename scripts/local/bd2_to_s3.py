#!/usr/bin/env python3
"""
BD² (Breakthrough Discoveries for Thriving with Bipolar Disorder) to S3
=======================================================================

Scrapes BD²'s funded grants and uploads a parquet to S3 for Databricks
ingestion. (BD² is on the IAMHRF list; priority 40.)

Data source: bipolardiscoveries.org. Plain `requests` is blocked by the
    WP-Engine WAF (challenge page), but DEFAULT-UA headless chromium
    (playwright) gets HTTP 200 instantly — a custom UA is not required here.

    Grants live on four program pages, each rendered as a list of collapsed
    accordions (`li.module__grant_info--item`). The full grant content —
    title, year, and the entire Team block — is present in the *static* DOM
    even while collapsed (CSS hides it; it is not lazy-loaded), so parsing is
    expansion-independent. We still best-effort JS-click every "Expand to Read
    More" toggle first (matches the site's intended reading flow and future-
    proofs against a switch to lazy-loading), then parse `page.content()`.

    Per grant the markup is cleanly classed:
      - title       -> `.module__grant_info--itemTitle h3`
      - year        -> `.module__grant_info--itemYear` (a bare 20\\d\\d; only
                       the Discovery page carries it — null elsewhere)
      - team cards  -> `.module__grant_info--TeamCard`, each a 3-line block:
                       "Name, DEGREES" / "Institution" / "View full bio".
        On the Discovery and Collaboration pages the cards are visually split
        by "Lead PI:" / "Co-PIs:" labels; on the two single-grant platform
        pages there are no labels. In every case the *first* card is the lead
        PI and the remaining cards are co-PIs, so we map positionally:
        card[0] -> lead, card[1:] -> co-PIs.

    No per-grant amounts are published anywhere on the site (only cohort
    totals in press releases) — none are invented.

    Pages ingested (the integrated-network page is intentionally excluded —
    those are clinical-network sites, not grants):
      /our-work/discovery-research/    -> 12 grants (2025x4, 2024x4, 2023x4)
      /our-work/collaboration-projects/->  4 grants (2025)
      /our-work/brain-omics-platform/  ->  1 grant
      /our-work/genetics-platform/     ->  1 grant

Output: s3://openalex-ingest/awards/bd2/bd2_grants.parquet
"""

import argparse
import hashlib
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://bipolardiscoveries.org"
# program slug (used in funder_award_id + URL) -> scheme (program display name)
PROGRAMS = [
    ("discovery-research", "Discovery Research"),
    ("collaboration-projects", "Collaboration Projects"),
    ("brain-omics-platform", "Brain Omics Platform"),
    ("genetics-platform", "Genetics Platform"),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bd2/bd2_grants.parquet"
SHRINK_GUARD = 14  # expect ~18

YEAR_RE = re.compile(r"^20\d\d$")
BIO_RE = re.compile(r"^view\s+(full\s+)?bio$", re.I)


def strip_degrees(name):
    """'Michael Fox, MD, PhD' -> 'Michael Fox'. PI names are 'First [Mid] Last'
    with all post-nominal degrees after the first comma, so cutting there
    drops every degree token in one shot."""
    if not name:
        return None
    return (name.split(",", 1)[0]).strip() or None


def split_name(name):
    """Degree-stripped name -> (given, family). family = last token."""
    clean = strip_degrees(name)
    if not clean:
        return None, None
    parts = clean.split()
    if len(parts) < 2:
        return None, (parts[0] if parts else None)
    return " ".join(parts[:-1]), parts[-1]


def card_name_inst(card):
    """A TeamCard -> (name_with_degrees, institution).

    Card text is three lines: name / institution / 'View full bio'. Take the
    TeamName element for the name (fallback: first line) and the first
    non-'view bio' line after it for the institution.
    """
    name_el = card.select_one(".module__grant_info--TeamName")
    lines = [l.strip() for l in card.get_text("\n", strip=True).split("\n") if l.strip()]
    name = name_el.get_text(strip=True) if name_el else (lines[0] if lines else None)
    inst = None
    start = 1 if (lines and name and lines[0] == name) else 0
    for l in lines[start:]:
        if l == name or BIO_RE.match(l):
            continue
        inst = l
        break
    return name, inst


def parse_grant(li, slug, scheme):
    """One `li.module__grant_info--item` -> a row dict (or None if no title)."""
    h3 = li.select_one(".module__grant_info--itemTitle h3") or li.find("h3")
    title = re.sub(r"\s+", " ", h3.get_text(" ", strip=True)).strip() if h3 else None
    if not title:
        return None

    yr_el = li.select_one(".module__grant_info--itemYear")
    yr = yr_el.get_text(strip=True) if yr_el else ""
    year_awarded = yr if YEAR_RE.match(yr) else None

    cards = li.select(".module__grant_info--TeamCard")
    lead_name = lead_inst = None
    co_pis = []
    if cards:
        lead_name, lead_inst = card_name_inst(cards[0])
        for c in cards[1:]:
            n, inst = card_name_inst(c)
            n = strip_degrees(n)
            if not n:
                continue
            co_pis.append(f"{n} ({inst})" if inst else n)
    pi_given, pi_family = split_name(lead_name)
    co_pis_raw = "; ".join(co_pis) or None

    aid = f"bd2-{slug}-{hashlib.md5(title.encode('utf-8')).hexdigest()[:10]}"
    return {
        "funder_award_id": aid,
        "title": title,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": lead_inst,
        "co_pis_raw": co_pis_raw,
        "scheme": scheme,
        "year_awarded": year_awarded,
        "landing_page_url": f"{BASE}/our-work/{slug}/",
    }


def expand_all(page):
    """Best-effort: JS-dispatch a click on every accordion toggle so the page
    renders fully expanded. Parsing does not depend on this (content is in the
    static DOM regardless), so any failure is non-fatal."""
    try:
        n = page.evaluate(
            "() => { const e = document.querySelectorAll("
            "'.module__grant_info--collapseExpand'); e.forEach(x => x.click()); "
            "return e.length; }"
        )
        time.sleep(1.5)
        return n
    except Exception as e:
        print(f"    expand warning (non-fatal): {type(e).__name__}: {e}")
        return 0


def scrape(page):
    rows, per_program = [], {}
    for slug, scheme in PROGRAMS:
        url = f"{BASE}/our-work/{slug}/"
        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(2)
        status = resp.status if resp else None
        n_expanded = expand_all(page)
        soup = BeautifulSoup(page.content(), "html.parser")
        items = soup.select("li.module__grant_info--item")
        n = 0
        for li in items:
            row = parse_grant(li, slug, scheme)
            if row:
                rows.append(row)
                n += 1
        per_program[scheme] = n
        print(f"  {scheme:<24} status={status} expanded={n_expanded} "
              f"items={len(items)} grants={n}")
        time.sleep(1)
    return rows, per_program


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
    ap = argparse.ArgumentParser(description="BD² grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/bd2_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("BD² — Breakthrough Discoveries for Thriving with Bipolar Disorder -> S3")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()  # DEFAULT UA: clears the WP-Engine WAF
        page = ctx.new_page()
        rows, per_program = scrape(page)
        browser.close()

    df = pd.DataFrame(rows).astype("string")
    print(f"\nGrants per program:")
    for scheme, n in per_program.items():
        print(f"  {scheme:<24} {n}")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")

    if len(df) < SHRINK_GUARD:
        print(f"[ERROR] only {len(df)} grants (< {SHRINK_GUARD}) — "
              f"site markup change? Aborting before write.")
        sys.exit(1)

    for c in ("title", "pi_given", "pi_family", "institution", "co_pis_raw",
              "scheme", "year_awarded"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    out = args.output_dir / "bd2_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
