#!/usr/bin/env python3
"""
FNR Luxembourg to S3 Data Pipeline
==================================

Scrapes Fonds National de la Recherche Luxembourg (FNR, F4320321038) funded
projects and uploads a parquet to S3 for Databricks ingestion.

Data source: fnr.lu. Custom WP endpoint /wp-json/fnr/projects returns ~149
    "project finder" news entries ({actus: [...]}, each with permalink/title/
    year). Result-ish entries (title contains result/laureat/funded/...) carry
    INLINE HTML TABLES on the detail page, one table per funded project:
    row0 = header (td cells: Acronym | Title/Project title | Host institution/
    Institution/Organisation | PI/Applicant | ... | FNR funding/FNR Funding),
    row1 = data, rows 2+ = 2-cell Keywords/Abstract pairs. Headers vary per
    call; map by name. Only recent (~2025-2026) pages have inline tables;
    some newer posts have a "View funded projects" SharePoint button instead
    (skipped + counted); older posts have no tables at all. No native award
    ids -> synthesize from page slug + md5(pi|title).

Output: s3://openalex-ingest/awards/fnr_luxembourg/fnr_luxembourg_grants.parquet
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

BASE = "https://www.fnr.lu"
PROJECTS_API = BASE + "/wp-json/fnr/projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fnr_luxembourg/fnr_luxembourg_grants.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MIN_ROWS = 70  # shrink guard (expect ~95+)

RESULTISH_RE = re.compile(r"result|laur[eé]at|funded|outcome|retained|selected", re.I)
_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r",| at | from ", raw)[0].strip().lstrip(":").strip()
    first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def classify_header(text):
    """Map a header-cell text to a canonical field name (or None)."""
    h = re.sub(r"\s+", " ", (text or "")).strip().lower().rstrip(":")
    if not h:
        return None
    if h == "acronym":
        return "acronym"
    if h in ("title", "project title", "project"):
        return "title"
    if h == "pi" or "applicant" in h or "investigator" in h or h == "project leader":
        return "pi"
    if "institution" in h or "organisation" in h or "organization" in h:
        return "institution"
    if ("funding" in h or "committed" in h) and "period" not in h and "duration" not in h:
        return "amount"
    return None


def parse_amount(text):
    """'€ 1,135,000' / 'EUR 536,000.00' / '9 568 €' -> digits-only string."""
    if not text:
        return None
    m = re.search(r"\d[\d\s.,\u00a0\u202f]*", text)
    if not m:
        return None
    num = re.sub(r"[\s\u00a0\u202f]", "", m.group(0)).rstrip(".,")
    num = re.sub(r"[.,]\d{1,2}$", "", num)  # drop decimal cents
    num = re.sub(r"[.,]", "", num)
    return num or None


def parse_page_tables(html):
    """Yield dicts of canonical fields from every project table on a page."""
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_cells = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        colmap = {}
        for idx, txt in enumerate(header_cells):
            field = classify_header(txt)
            if field and field not in colmap:
                colmap[field] = idx
        # require a title column plus at least one of pi/amount -> project table
        if "title" not in colmap or not ({"pi", "amount"} & set(colmap)):
            continue
        n = len(header_cells)
        for tr in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if len(cells) != n:        # 2-cell Keywords/Abstract rows etc.
                continue
            if cells == header_cells:  # repeated header (defensive)
                continue
            rec = {}
            for field, idx in colmap.items():
                v = cells[idx].strip()
                rec[field] = v if v and v not in ("–", "-", "—", "n/a", "N/A") else None
            if rec.get("title") or rec.get("acronym"):
                yield rec


def has_sharepoint_button(html):
    """Newer posts link the project list out to SharePoint instead of tables."""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        text = a.get_text(" ", strip=True).lower()
        if "sharepoint" in href.lower() and "iso%20certification" in href.lower():
            continue  # site-wide ISO-certificate footer link, on every page
        if "funded project" in text or ("sharepoint" in href.lower() and href):
            return True
    return False


def get_listing(session):
    r = session.get(PROJECTS_API, timeout=40)
    r.raise_for_status()
    return r.json().get("actus") or []


def main():
    ap = argparse.ArgumentParser(description="FNR Luxembourg grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fnr_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("FNR Luxembourg funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(HEADERS)
    actus = get_listing(s)
    print(f"Endpoint: {len(actus)} entries")
    resultish = [e for e in actus if e.get("title") and e.get("permalink")
                 and RESULTISH_RE.search(e["title"])]
    print(f"Result-ish by title: {len(resultish)}")
    if len(resultish) < 50:
        print(f"[ERROR] only {len(resultish)} result-ish entries — endpoint change?")
        sys.exit(1)

    rows, seen = [], set()
    sharepoint_pages, nodata_pages, fail_pages = [], [], []
    for e in resultish:
        url = e["permalink"]
        page_title = re.sub(r"\s+", " ", e["title"]).strip()
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        ym = YEAR_RE.search(page_title) or YEAR_RE.search(slug)
        year = ym.group(0) if ym else None
        try:
            r = s.get(url, timeout=40)
        except Exception as exc:
            print(f"  [WARN] fetch failed {url}: {exc}")
            fail_pages.append(url)
            continue
        if r.status_code != 200:
            print(f"  [WARN] HTTP {r.status_code} {url}")
            fail_pages.append(url)
            continue

        page_rows = 0
        for rec in parse_page_tables(r.text):
            acronym, ptitle = rec.get("acronym"), rec.get("title")
            title = f"{acronym}: {ptitle}" if acronym and ptitle else (ptitle or acronym)
            pi_raw = rec.get("pi")
            award_id = "fnr-%s-%s" % (
                slug, hashlib.md5(f"{pi_raw or ''}|{title}".encode("utf-8")).hexdigest()[:10])
            if award_id in seen:
                continue
            seen.add(award_id)
            given, family = parse_pi(pi_raw)
            rows.append({
                "funder_award_id": award_id,
                "title": title,
                "pi_given": given, "pi_family": family,
                "institution": rec.get("institution"),
                "amount": parse_amount(rec.get("amount")),
                "call": page_title,
                "year": year,
                "landing_page_url": url,
            })
            page_rows += 1
        if page_rows:
            print(f"  {page_rows:3d} rows | {page_title}")
        elif has_sharepoint_button(r.text):
            sharepoint_pages.append(page_title)
        else:
            nodata_pages.append(page_title)
        time.sleep(0.4)

    print(f"\nPages with tables: "
          f"{len(set(r['landing_page_url'] for r in rows))} | "
          f"SharePoint-button (skipped): {len(sharepoint_pages)} | "
          f"no project table: {len(nodata_pages)} | fetch failures: {len(fail_pages)}")
    for t in sharepoint_pages:
        print(f"  [sharepoint] {t}")

    if len(rows) < MIN_ROWS:
        print(f"[ERROR] only {len(rows)} rows (< {MIN_ROWS}) — site change?")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "fnr_luxembourg_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


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


if __name__ == "__main__":
    main()
