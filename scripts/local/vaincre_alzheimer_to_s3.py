#!/usr/bin/env python3
"""
Fondation Vaincre Alzheimer to S3 Data Pipeline
===============================================

Scrapes Fondation Vaincre Alzheimer's funded research projects (French Alzheimer
charity) and uploads a parquet to S3 for Databricks ingestion.

Data source: vaincrealzheimer.org. The index page
    /la-recherche/projets-finances/ links one page per year
    /la-recherche/projets-finances/projets-<YYYY>/ for 2006-2026 (~125-134
    projects). Each year page lays out projects as `div.icon_box_inner` blocks
    that are structurally consistent across every era:
        - <h3>          = PI name with French honorifics (Dr/Dr./Pr/Prof/Prof Dr)
        - first <p>     = institution (INSERM, CNRS, Universite ..., Laboratoire)
        - second <p>    = "<title> - <grant type>" (e.g. "- Subvention Standard");
                          a clean <strong> title is also present on most blocks
        - <h4>"Montant N EUR" only on older pages (2006-2015); recent pages
          (2016+) omit amounts. UA "Mozilla/5.0" required.
    (Alzheimer funder; on the IAMHRF list. OpenAlex awards batch #8.)

Output: s3://openalex-ingest/awards/vaincre_alzheimer/vaincre_alzheimer_grants.parquet
"""

import argparse
import hashlib
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.vaincrealzheimer.org"
INDEX = BASE + "/la-recherche/projets-finances/"
YEAR_RE = re.compile(r"/la-recherche/projets-finances/projets-(\d{4})/")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/vaincre_alzheimer/vaincre_alzheimer_grants.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# French honorifics, stripped repeatedly so combos like "Prof Dr" fall off.
_HON_RE = re.compile(
    r"^(?:Professeur|Docteur|Professor|Prof|Pr|Dr|Mme|M)\.?\s+", re.I)
# Grant-type tails appended after the title, e.g. "... - Subvention Standard".
_GRANT_RE = re.compile(
    r"\s*[–—-]\s*((?:Subvention|Allocation|Bourse|Prix|Standard|"
    r"Doctorale?|Post[- ]?doctorale?|Jeune Chercheur|Innovation|Coup de Pouce|"
    r"Émergence|Emergence)[^–—]*)$", re.I)
_NON_TITLE = ("en savoir", "montant", "dates", "subvention", "allocation",
              "bourse")
# "Montant <digits, span-split> EUR" -> digits.
_AMT_RE = re.compile(r"Montant([^€]{1,40})€")


def parse_pi(raw):
    """Strip French honorifics, then split into (given, family)."""
    if not raw:
        return None, None
    name = raw.strip()
    prev = None
    while name != prev:  # peel "Prof Dr ", "Dr. ", etc.
        prev = name
        name = _HON_RE.sub("", name).strip()
    if not re.search(r"[A-Za-zÀ-ſ]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return (None, name or None)
    return " ".join(parts[:-1]), parts[-1]


def clean_title(text):
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    text = _GRANT_RE.sub("", text).strip().rstrip("–—-").strip()
    return text or None


def get_year_pages(session):
    """Crawl the index, return sorted [(year, url)]."""
    r = session.get(INDEX, timeout=40)
    r.raise_for_status()
    years = {}
    for y in YEAR_RE.findall(r.text):
        years[y] = f"{BASE}/la-recherche/projets-finances/projets-{y}/"
    return sorted(years.items())


def parse_box(box, year, url):
    h3 = box.find("h3")
    pi_raw = h3.get_text(" ", strip=True) if h3 else ""
    low_pi = pi_raw.lower()
    if not pi_raw or "par année" in low_pi or "projets de recherche financ" in low_pi:
        return None  # navigation / index block, not a project

    ps = box.find_all("p")
    institution = ps[0].get_text(" ", strip=True) if ps else None

    # Title: prefer a clean <strong>; else the second <p> minus the grant tail.
    title = None
    for s in box.find_all("strong"):
        t = s.get_text(" ", strip=True)
        if t and len(t) >= 8 and not t.lower().startswith(_NON_TITLE):
            title = t
            break
    if not title and len(ps) >= 2:
        title = ps[1].get_text(" ", strip=True)
    title = clean_title(title)

    amount = None
    m = _AMT_RE.search(box.get_text(" ", strip=True))
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if 3 <= len(digits) <= 7:
            amount = digits

    given, family = parse_pi(pi_raw)
    award_id = hashlib.md5(f"{pi_raw}|{title}".encode("utf-8")).hexdigest()[:10]
    return {
        "funder_award_id": f"fva-{year}-{award_id}",
        "title": title,
        "pi_given": given,
        "pi_family": family,
        "institution": institution,
        "amount": amount,
        "currency": "EUR" if amount else None,
        "year_awarded": str(year),
        "landing_page_url": url,
    }


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
    ap = argparse.ArgumentParser(description="Fondation Vaincre Alzheimer grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fva_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Fondation Vaincre Alzheimer funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(HEADERS)
    pages = get_year_pages(s)
    print(f"Index: {len(pages)} year pages ({pages[0][0]}-{pages[-1][0]})")
    if len(pages) < 10:
        print(f"[ERROR] only {len(pages)} year pages — index change?")
        sys.exit(1)

    rows, seen, per_year = [], set(), {}
    for year, url in pages:
        try:
            r = s.get(url, timeout=40)
        except Exception as e:
            print(f"  {year}: fetch error {e}")
            per_year[year] = 0
            continue
        if r.status_code != 200:
            print(f"  {year}: HTTP {r.status_code}")
            per_year[year] = 0
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        n = 0
        for box in soup.select("div.icon_box_inner"):
            rec = parse_box(box, year, url)
            if rec and rec["title"] and rec["funder_award_id"] not in seen:
                seen.add(rec["funder_award_id"])
                rows.append(rec)
                n += 1
        per_year[year] = n
        print(f"  {year}: {n} projects")
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_given", "pi_family", "institution", "amount",
              "year_awarded", "landing_page_url"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    if len(df) < 90:
        print(f"\n[ERROR] shrink-guard: only {len(df)} rows (<90) — structure changed?")
        sys.exit(1)

    out = args.output_dir / "vaincre_alzheimer_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
