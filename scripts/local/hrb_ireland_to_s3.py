#!/usr/bin/env python3
"""
Health Research Board (Ireland) — HRB -> S3 Data Pipeline
=========================================================

Crawls the HRB "Grants approved" register and uploads a parquet to S3 for
Databricks ingestion. HRB is OpenAlex funder F4320312041 (ROR 003hb2249, DOI
10.13039/100010414); existing award coverage is Crossref-derived only, so this
dedicated ingest adds canonical project metadata with PI, amount and abstract.

Data source: ONE static HTML register page (~3.7 MB) listing 1,675 grants:
    https://www.hrb.ie/funding-category/research-funding/investments-impacts/grants-approved/
  Each <article class="grant-approved"> card has title, Year, Scheme, Host
  Institution, Lead PI Name, and a detail-page URL. Detail pages (.card-title /
  .card-value pairs) add Grant Value (EUR), Principal Investigator, Award Year,
  Scheme Type, research-area classification, plus an abstract in .entry-content.
  100% title/PI/institution/amount, 93% abstract, all years derived from the
  Award Year (the source carries only a relative "Duration", no absolute dates).

Notes:
- NO native grant reference id on the pages -> funder_award_id is synthesized from
  the detail-URL slug (prefix "hrb:"). The slug is the grant's stable permalink.
- Year-only -> start_date_raw = YYYY-01-01 (the notebook keeps start_year only and
  leaves start_date NULL); end_date_raw null.

Output: s3://openalex-ingest/awards/hrb_ireland/hrb_ireland_grants.parquet
"""

import argparse
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

LISTING = ("https://www.hrb.ie/funding-category/research-funding/"
           "investments-impacts/grants-approved/")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "en-IE,en;q=0.9"}
WORKERS = 4          # small concurrent pool (server ~1.2s/req)
THROTTLE = 0.5       # per-worker polite sleep
CURRENCY = "EUR"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hrb_ireland/hrb_ireland_grants.parquet"

COLS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
        "institution", "amount", "currency", "scheme", "start_date_raw",
        "end_date_raw", "description", "landing_page_url"]

TITLES = {"dr", "prof", "professor", "mr", "mrs", "ms", "miss", "mx", "sir",
          "dame", "rev", "assoc", "associate", "a/prof"}


def split_name(full):
    if not full:
        return (None, None, None)
    full = re.sub(r"\s+", " ", full).strip()
    toks = full.split(" ")
    while toks and toks[0].lower().strip(".") in TITLES:
        toks = toks[1:]
    if not toks:
        return (full, None, None)
    clean = " ".join(toks)
    if len(toks) == 1:
        return (clean, None, toks[0])
    return (clean, " ".join(toks[:-1]), toks[-1])


def clean_amount(s):
    if not s:
        return None
    m = re.search(r"€\s*([\d][\d,\.]*)", s) or re.search(r"([\d][\d,]{2,})", s)
    if not m:
        return None
    num = m.group(1).replace(",", "").split(".")[0]
    return num if num.isdigit() and int(num) > 0 else None


def text_or_none(el):
    if el is None:
        return None
    t = el.get_text(" ", strip=True)
    return t if t else None


def fetch_listing():
    s = requests.Session(); s.headers.update(HEADERS)
    r = s.get(LISTING, timeout=60); r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    rows = []
    for art in soup.find_all("article", class_="grant-approved"):
        a = art.find("a", class_="whole-article-link") or art.find("a", href=True)
        url = a["href"].rstrip("/") if (a and a.get("href")) else None
        meta = {}
        for mi in art.select(".meta-item"):
            lab = text_or_none(mi.find(class_="meta-label"))
            val = text_or_none(mi.find(class_="meta-value"))
            if lab:
                meta[lab] = val
        rows.append({
            "url": url,
            "l_title": text_or_none(art.select_one("h2.entry-title")
                                    or art.select_one(".entry-title")),
            "l_year": meta.get("Year"),
            "l_scheme": meta.get("Scheme"),
            "l_institution": meta.get("Host Institution"),
            "l_pi": meta.get("Lead PI Name"),
        })
    return rows


def parse_detail(html):
    soup = BeautifulSoup(html, "lxml")
    d = {}
    for ct in soup.select(".card-title"):
        parent = ct.parent
        cv = (parent.select_one(".card-value") if parent else None) \
            or ct.find_next(class_="card-value")
        lab = ct.get_text(" ", strip=True)
        if lab and cv is not None:
            d[lab] = cv.get_text(" ", strip=True)
    ec = soup.select_one(".entry-content")
    abstract = ec.get_text(" ", strip=True) if ec else None
    if abstract:
        abstract = re.sub(r"\s+", " ", abstract).strip()
        if len(abstract) < 30:
            abstract = None
    return d, text_or_none(soup.select_one("h1")), abstract


def fetch_detail(url):
    s = fetch_detail._sess
    try:
        r = s.get(url, timeout=45)
        time.sleep(THROTTLE)
        if r.status_code != 200:
            return url, None, None, None, r.status_code
        d, title, abstract = parse_detail(r.text)
        return url, d, title, abstract, 200
    except Exception as e:
        return url, None, None, None, f"ERR:{type(e).__name__}"


def _init_worker():
    s = requests.Session(); s.headers.update(HEADERS)
    fetch_detail._sess = s


def upload_to_s3(local_path: Path) -> bool:
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
    ap = argparse.ArgumentParser(description="HRB Ireland grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/hrb_ireland_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Health Research Board (Ireland) -> S3")
    print("=" * 60)

    t0 = time.time()
    listing = fetch_listing()
    by_url = {r["url"]: r for r in listing if r["url"]}
    urls = list(by_url.keys())
    print(f"listing cards: {len(listing)}  unique detail URLs: {len(urls)}")

    details = {}
    statuses = []
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS, initializer=_init_worker) as ex:
        futs = {ex.submit(fetch_detail, u): u for u in urls}
        for fut in as_completed(futs):
            url, d, title, abstract, st = fut.result()
            statuses.append(st)
            details[url] = (d, title, abstract, st)
            done += 1
            if done % 300 == 0:
                ok = sum(1 for s in statuses if s == 200)
                print(f"  {done}/{len(urls)} fetched ({ok} ok) [{time.time()-t0:.0f}s]")
    ok = sum(1 for s in statuses if s == 200)
    print(f"detail crawl: {ok}/{len(urls)} HTTP200 [{time.time()-t0:.0f}s]")

    rows = []
    for url in urls:
        L = by_url[url]
        d, dtitle, abstract, _ = details.get(url, ({}, None, None, None))
        d = d or {}
        pi_full_raw = d.get("Principal Investigator") or L["l_pi"]
        year = d.get("Award Year") or L["l_year"]
        pf, pg, pfam = split_name(pi_full_raw)
        sd = f"{str(year).strip()}-01-01" if year and re.match(r"^\d{4}$", str(year).strip()) else None
        rows.append({
            "funder_award_id": "hrb:" + url.rsplit("/", 1)[-1],
            "title": dtitle or L["l_title"],
            "pi_full": pf, "pi_given": pg, "pi_family": pfam,
            "institution": d.get("Host Institution") or L["l_institution"],
            "amount": clean_amount(d.get("Grant Value")),
            "currency": CURRENCY,
            "scheme": d.get("Scheme") or L["l_scheme"],
            "start_date_raw": sd,
            "end_date_raw": None,
            "description": abstract,
            "landing_page_url": url + "/",
        })

    df = pd.DataFrame(rows, columns=COLS)
    for c in COLS:
        df[c] = df[c].apply(lambda v: None if (v is None or (isinstance(v, float) and pd.isna(v)) or v == "") else str(v))
        df[c] = df[c].astype("string")

    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw",
              "scheme", "description"):
        nn = int(df[c].notna().sum())
        print(f"  {c:16}: {nn}/{len(df)} ({round(100*nn/max(len(df),1))}%)")

    if len(df) < 1400:
        print(f"[ERROR] only {len(df)} rows — expected ~1,675; listing changed?")
        sys.exit(1)

    out = args.output_dir / "hrb_ireland_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
