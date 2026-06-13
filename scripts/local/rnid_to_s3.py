#!/usr/bin/env python3
"""
RNID / Action on Hearing Loss research projects to S3 Data Pipeline
===================================================================

Scrapes RNID's (formerly Action on Hearing Loss / Deafness Research UK)
funded biomedical-research projects and uploads a parquet to S3 for
Databricks ingestion. (RNID is on the IAMHRF list; OpenAlex awards wave 5.)

Data source: rnid.org.uk. Plain `requests` gets a hard 403 (WAF) on every
    content page, but headless chromium (playwright) with the DEFAULT UA
    renders hubs + detail pages at 200, and the in-browser request API
    (ctx.request.get) returns 200 for detail pages directly. Flow:
      1. Three theme-hub listing pages under
         /what-we-do/biomedical-research/research-we-fund/:
           restoring-hearing/ · silencing-tinnitus/ · preventing-hearing-loss/
         Each hub renders one project block per funded project. A block is:
           <h3>  Project title
           <dl class="govuk-summary-list">
             Researcher / Researchers / Student  ->  named investigator
             Where                               ->  institution
           </dl>
           <a> "Find out more about <PI>'s project"  ->  detail-page URL
      2. Per unique project, GET the detail page in the same context
         (~0.5s throttle). The detail-page <h1> is the authoritative title;
         "Project start date: <Month Year>" (when present) gives the year.

    Institution comes from the labelled "Where" row (high coverage), NOT prose.
    No amounts anywhere on hub or detail pages (§6.7 NULL waiver) -> no amount.
    Year is partial (only on detail pages that carry a start date).

Funder-id note (3-way dup, all share Wikidata Q4677412): F4320320250
    "Action on Hearing Loss" (ROR 05w6qh410, matches tracker name) vs
    F4320330762 "Royal National Institute for Deaf People" (max-works=181,
    no ROR) vs F4320320010 "Deafness Research UK". Resolve at admin step;
    this scraper only emits the native portfolio (no funder id baked in).

Output: s3://openalex-ingest/awards/rnid/rnid_grants.parquet
"""

import argparse
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

BASE = "https://rnid.org.uk"
HUB_BASE = f"{BASE}/what-we-do/biomedical-research/research-we-fund/"
THEMES = ["restoring-hearing", "silencing-tinnitus", "preventing-hearing-loss"]
THEME_LABEL = {
    "restoring-hearing": "Restoring hearing",
    "silencing-tinnitus": "Silencing tinnitus",
    "preventing-hearing-loss": "Preventing hearing loss",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rnid/rnid_grants.parquet"

SHRINK_GUARD = 55  # expect ~71+; abort below this

# Detail-page URL = hub_base + theme + project-slug (two path segments past theme)
DETAIL_RE = re.compile(
    r"/what-we-do/biomedical-research/research-we-fund/[^/]+/[^/]+/?$")
PERSON_KEYS = ("researcher", "researchers", "student", "students")

_TITLE_RE = re.compile(
    r"^((?:Associate\s+Professor|Assistant\s+Professor|Professor|Prof|Dr|Mr|"
    r"Mrs|Ms|Miss|Sir|Dame|A/Prof)\.?\s+)+", re.I)
_MONTHS = (r"January|February|March|April|May|June|July|August|September|"
           r"October|November|December")
_START_RE = re.compile(
    rf"(?:project\s+)?start\s+date[:\s]+(?:{_MONTHS})?\s*(20\d\d)", re.I)
_BEGAN_RE = re.compile(rf"(?:began|started|commenced)\s+in\s+(?:{_MONTHS}\s+)?(20\d\d)", re.I)


def parse_pi(raw):
    """First named person -> (given, family). Strip honorifics; family=last token."""
    if not raw:
        return None, None
    first = re.split(r";|,| and | & |&", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?",
                   "", first).strip().rstrip(",")
    if not re.search(r"[A-Za-z]{2}", first):
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def slug_from_url(url):
    """Last non-empty path segment -> award slug."""
    return url.rstrip("/").split("/")[-1] or None


def parse_hub(html, theme):
    """One project per dl.govuk-summary-list block on a theme hub.

    Each block carries a person row (Researcher/Researchers/Student) and a
    'Where' (institution) row; the project's <h3> title sits immediately
    above it, and a 'Find out more' detail link immediately below.
    """
    soup = BeautifulSoup(html, "html.parser")
    projects = []
    for dl in soup.select("dl.govuk-summary-list"):
        pi_raw = institution = None
        for row in dl.select("div.govuk-summary-list__row"):
            dt = row.select_one("dt")
            dd = row.select_one("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True).lower()
            val = dd.get_text(" ", strip=True) or None
            if pi_raw is None and key in PERSON_KEYS:
                pi_raw = val
            elif institution is None and key == "where":
                institution = val
        # detail link: nearest following detail-pattern anchor, bounded by next dl
        next_dl = dl.find_next("dl", class_="govuk-summary-list")
        detail_url = None
        a = dl.find_next("a", href=DETAIL_RE)
        while a is not None:
            if next_dl is not None and _comes_after(a, next_dl):
                break  # this link belongs to the next project
            href = a.get("href", "")
            if href.startswith("/"):
                href = BASE + href
            detail_url = href
            break
        if not detail_url:
            continue
        # hub-side title fallback (authoritative title comes from detail H1)
        h3 = dl.find_previous("h3")
        hub_title = h3.get_text(" ", strip=True) if h3 else None
        projects.append({
            "slug": slug_from_url(detail_url),
            "detail_url": detail_url,
            "pi_raw": pi_raw,
            "institution": institution,
            "hub_title": hub_title,
            "theme": THEME_LABEL.get(theme, theme),
        })
    return projects


def _comes_after(node_a, node_b):
    """True if node_a appears at/after node_b in document order."""
    for el in node_b.next_elements:
        if el is node_a:
            return True
    return False


def fetch_detail(ctx, page, url):
    """GET a detail page in the warmed browser context; one nav-retry fallback."""
    for attempt in (1, 2):
        try:
            r = ctx.request.get(url, timeout=40000)
            if r.status == 200:
                return r.text()
            print(f"    status={r.status}" + ("" if attempt == 2 else ", retrying"))
        except Exception as e:
            print(f"    fetch error: {e}")
        if attempt == 1:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1500)
                return page.content()
            except Exception:
                pass
    return None


def parse_detail(html):
    """Authoritative title (<h1>) + year_awarded (start date, when present)."""
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None
    title = re.sub(r"\s+", " ", title).strip() if title else None
    txt = soup.get_text("\n", strip=True)
    year = None
    m = _START_RE.search(txt) or _BEGAN_RE.search(txt)
    if m:
        year = m.group(1)
    return title, year


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
    ap = argparse.ArgumentParser(description="RNID research projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/rnid_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("RNID / Action on Hearing Loss research projects -> S3")
    print("=" * 60)

    rows, seen, failed = [], set(), []
    per_theme = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()  # default UA renders 200; custom UA risks 403
        page = ctx.new_page()

        # 1) Load each theme hub, parse project blocks
        hub_projects = []
        for theme in THEMES:
            url = HUB_BASE + theme + "/"
            print(f"\nLoading hub: {theme}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2500)
                html = page.content()
            except Exception as e:
                print(f"  [ERROR] hub load failed: {e}")
                continue
            projs = parse_hub(html, theme)
            per_theme[theme] = len(projs)
            print(f"  parsed {len(projs)} project blocks")
            hub_projects.extend(projs)

        # de-dupe by slug (same PI can hold >1 project; same slug = same project)
        uniq = []
        for pr in hub_projects:
            aid = f"rnid-{pr['slug']}" if pr["slug"] else None
            if not aid or aid in seen:
                continue
            seen.add(aid)
            pr["award_id"] = aid
            uniq.append(pr)
        print(f"\nUnique projects across hubs: {len(uniq)}")

        if len(uniq) < SHRINK_GUARD:
            print(f"[ERROR] only {len(uniq)} projects (< shrink-guard "
                  f"{SHRINK_GUARD}) — hub layout/selectors may have changed.")
            browser.close()
            sys.exit(1)

        # 2) Fetch detail pages for authoritative title + year
        for n, pr in enumerate(uniq, 1):
            print(f"  [{n}/{len(uniq)}] {pr['slug'][:60]}")
            html = fetch_detail(ctx, page, pr["detail_url"])
            title = year = None
            if html is None:
                failed.append(pr["slug"])
            else:
                title, year = parse_detail(html)
            if not title:
                title = pr["hub_title"]  # fallback to hub h3
            pi_given, pi_family = parse_pi(pr["pi_raw"])
            rows.append({
                "funder_award_id": pr["award_id"],
                "title": title,
                "pi_raw": pr["pi_raw"],
                "pi_given": pi_given,
                "pi_family": pi_family,
                "institution": pr["institution"],
                "theme": pr["theme"],
                "year_awarded": year,
                "landing_page_url": pr["detail_url"],
            })
            time.sleep(0.5)
        browser.close()

    if failed:
        print(f"\n[WARNING] {len(failed)} detail pages failed: {', '.join(failed)}")

    df = pd.DataFrame(rows).astype("string")
    print("\n" + "=" * 60)
    print(f"DataFrame: {len(df)} rows, {len(df.columns)} columns")
    print("\nRows per theme (hub blocks parsed, pre-dedupe):")
    for theme in THEMES:
        print(f"  {THEME_LABEL.get(theme, theme)}: {per_theme.get(theme, 0)}")
    print(f"\nRows per theme (final, deduped):")
    print(df["theme"].value_counts().to_string())
    print("\nFill stats:")
    for c in ("title", "pi_raw", "pi_given", "pi_family", "institution",
              "theme", "year_awarded", "landing_page_url"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    out = args.output_dir / "rnid_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    print("\n3 sample rows:")
    for _, r in df.head(3).iterrows():
        print(f"  - id={r['funder_award_id']}")
        print(f"    title={r['title']}")
        print(f"    pi={r['pi_given']} {r['pi_family']}  | inst={r['institution']}"
              f"  | theme={r['theme']}  | year={r['year_awarded']}")
        print(f"    url={r['landing_page_url']}")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    else:
        print(f"\n[skip-upload] S3 target (NOT uploaded): "
              f"s3://{S3_BUCKET}/{S3_KEY}")
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
