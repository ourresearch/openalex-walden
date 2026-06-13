#!/usr/bin/env python3
"""
Alcohol Change UK to S3 Data Pipeline
=====================================

Scrapes Alcohol Change UK's (formerly Alcohol Research UK) funded research
projects and uploads a parquet to S3 for Databricks ingestion.

Data source: alcoholchange.org.uk/research/our-funded-projects. Single
    server-rendered page (plain requests, UA Mozilla/5.0) with 5 scheme
    sections (div.section-header > h2): Commissioned research, Research and
    Development Grants, Our co-funded projects, Small Grants, Research
    Studentships. The first four schemes are 1-col table rows of
    "<a>title</a> <strong>institution</strong> - Month Year"; Studentships is
    a 4-col table (PI/Grantholder | Institution | Year published | PhD title).
    No PI columns kept and no amounts anywhere on the page (per-funder NULL
    waiver, runbook section 6.7). IAMHRF funder; legacy Alcohol Research UK
    F4320319998 + current Alcohol Change UK F4320327606.

Output: s3://openalex-ingest/awards/alcohol_change_uk/alcohol_change_uk_grants.parquet
"""

import argparse
import copy
import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://alcoholchange.org.uk"
LIST_URL = BASE + "/research/our-funded-projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alcohol_change_uk/alcohol_change_uk_grants.parquet"

MONTH_RE = re.compile(r"(January|February|March|April|May|June|July|August|"
                      r"September|October|November|December)\s+((?:19|20)\d{2})")
STUDENTSHIPS = "Research Studentships"


def norm(s):
    return re.sub(r"\s+", " ", s or "").strip()


def award_id(title):
    return "acuk-" + hashlib.md5(title.encode("utf-8")).hexdigest()[:10]


def parse_entry_td(td):
    """One-col entry: '<a>title</a> institution - Month Year' (markup is messy:
    the strong/dash/date nesting varies, and a couple of links cut mid-word)."""
    links = td.find_all("a")
    if not links:
        return None
    link = links[0].get("href") or ""
    # the CMS sometimes splits one title across consecutive same-href <a> tags
    # ("…(ELAStiC" + ")", "…Librar" + "y") -> join their raw text
    title = norm("".join(a.get_text() for a in links
                         if (a.get("href") or "") == link))
    rest_td = copy.copy(td)
    for x in rest_td.find_all("a"):
        if (x.get("href") or "") == link:
            x.extract()
    rest = norm(rest_td.get_text(" ", strip=True))
    # re-attach a word fragment / closing paren the link boundary chopped off
    m = re.match(r"^([a-z]{1,3}|\))(\s+|$)", rest)
    if m:
        title += m.group(1)
        rest = rest[m.end():].strip()
    year = None
    dm = MONTH_RE.search(rest)
    if dm:
        year = dm.group(2)
        rest = rest[:dm.start()] + rest[dm.end():]
    institution = rest.strip(" -–—|,.")
    if re.search(r"co[- ]?funded with", institution, re.I):
        # "co-funded with the MRC - The University of Bristol" -> keep last chunk
        parts = re.split(r"\s[-–]\s", institution)
        institution = parts[-1].strip(" -–—|,.") if len(parts) > 1 else ""
        if re.search(r"co[- ]?funded with", institution, re.I):
            institution = ""
    if not title:
        return None
    return {
        "title": title,
        "institution": institution or None,
        "year_awarded": year,
        "landing_page_url": (BASE + link) if link.startswith("/") else (link or None),
    }


def parse_studentship_tr(tr):
    """4-col row: PI/Grantholder | Institution | Year published | PhD title.
    PI column intentionally dropped (no-PI build)."""
    cells = [norm(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
    if not any(cells) or cells[0].lower().startswith("pi/grantholder"):
        return None
    while len(cells) < 4:
        cells.append("")
    _pi, institution, year, title = cells[:4]
    if not title:
        return None
    return {
        "title": title,
        "institution": institution or None,
        "year_awarded": year if re.fullmatch(r"(?:19|20)\d{2}", year) else None,
        "landing_page_url": LIST_URL,
    }


def scrape(session):
    r = session.get(LIST_URL, timeout=40)
    r.raise_for_status()
    main = BeautifulSoup(r.text, "html.parser").find("main")
    if main is None:
        return []
    rows, seen, scheme = [], set(), None
    for el in main.find_all(["div", "tr"]):
        if el.name == "div" and "section-header" in (el.get("class") or []):
            h2 = el.find("h2")
            scheme = norm(h2.get_text(" ", strip=True)) if h2 else None
            continue
        if el.name != "tr" or not scheme:
            continue
        if scheme == STUDENTSHIPS:
            recs = [parse_studentship_tr(el)]
        else:  # stray <p> duplicates below the tables are ignored on purpose
            recs = [parse_entry_td(td) for td in el.find_all("td")
                    if td.get_text(strip=True)]
        for rec in recs:
            if not rec:
                continue
            rec["scheme"] = scheme
            rec["funder_award_id"] = award_id(rec["title"])
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
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
    ap = argparse.ArgumentParser(description="Alcohol Change UK grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/acuk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Alcohol Change UK funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = scrape(s)
    print(f"Parsed: {len(rows)} projects")
    if len(rows) < 20:
        print(f"[ERROR] only {len(rows)} — page change?")
        sys.exit(1)

    cols = ["funder_award_id", "title", "institution", "scheme",
            "year_awarded", "landing_page_url"]
    df = pd.DataFrame(rows)[cols].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "institution", "scheme", "year_awarded"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    print("  by scheme:", df["scheme"].value_counts().to_dict())
    out = args.output_dir / "alcohol_change_uk_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
