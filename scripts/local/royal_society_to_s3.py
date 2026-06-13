#!/usr/bin/env python3
"""
Royal Society (UK) research grants & fellowships to S3 Data Pipeline
===================================================================

Scrapes Royal Society fellowship/grant cohort announcements and uploads a
parquet to S3. (The Royal Society *Medals/prizes* are a separate ingest — this
covers research fellowships/grants only: URF, Dorothy Hodgkin, Newton
International, Career Development, Faraday Discovery, etc.)

Data source: the cohort-announcement news posts, enumerated from the content
sitemap:
    https://royalsociety.org/sitemaps/sitemap_content.xml   (fetch gzip)
Filter /news/ locs by fellowship/grant keywords, excluding medal/prize/
new-fellows. Each recipient on a cohort page is a <p> with the name in <strong>
followed by the project title and institution (separated by <br>). Older posts
(2009-2015) are looser (name + institution, no italic title) — handled by a
fallback. Pages that yield no recipients (e.g. a stray "newton-apple-tree" news
post) drop out naturally. provenance `royal_society_grants`, priority 339.
F4320320006 (Path A). Distinct from the already-ingested Royal Society Medals.

Output: s3://openalex-ingest/awards/royal_society_grants/royal_society_grants.parquet
Usage: python royal_society_to_s3.py [--limit N] [--skip-upload]
Author: OpenAlex Team
"""
import argparse, re, subprocess, sys, time
from pathlib import Path
import pandas as pd, requests
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass
from bs4 import BeautifulSoup

SITEMAP = "https://royalsociety.org/sitemaps/sitemap_content.xml"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/royal_society_grants/royal_society_grants.parquet"
EXPECTED_MIN = 300
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
KW = re.compile(r"fellowship|fellow|urf|career-development|dorothy-hodgkin|newton|faraday|research-grant", re.I)
SKIP = re.compile(r"medal|prize|new-fellows|banknote|apple-tree|award-winner|obituar|elect", re.I)
NAME_RE = re.compile(r"^(Dr|Professor|Prof|Mr|Mrs|Ms|Miss|Dame|Sir|Mx)\b\.?\s+[A-Z]")
INST_KEY = re.compile(r"Universit|College|Institute|Imperial|UCL|King'?s|Hospital|"
                      r"School|Centre|Center|Laborator|Trust|Museum|Observatory", re.I)
_TITLE_RE = re.compile(r"^(Dr|Professor|Prof|Mr|Mrs|Ms|Miss|Dame|Sir|Mx)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    n = re.sub(r"\s+(FRS|FMedSci|OBE|CBE|MBE|FREng)\b", "", n).strip()
    toks = n.split()
    if not toks: return None, None
    if len(toks) == 1: return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:80]


def scheme_from_url(url):
    m = re.search(r"/news/\d{4}(?:/\d{2})?/([^/]+)/?$", url)
    s = (m.group(1) if m else "").replace("-", " ")
    s = re.sub(r"\b20\d\d\b", "", s).strip().title()
    return s or None


def year_from_url(url):
    m = re.search(r"/news/(\d{4})", url)
    return int(m.group(1)) if m else None


def parse_page(html, url):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    rows = []
    year = year_from_url(url)
    scheme = scheme_from_url(url)
    for st in soup.find_all("strong"):
        name = clean(st.get_text(" ", strip=True))
        if not name or not NAME_RE.match(name) or len(name.split()) > 6:
            continue
        # collect following text segments within the same <p>
        p = st.find_parent("p") or st.parent
        segs = []
        for nx in st.next_siblings:
            t = nx.get_text(" ", strip=True) if hasattr(nx, "get_text") else str(nx).strip()
            t = clean(t)
            if t:
                segs.append(t)
        title = institution = None
        for sgmt in segs:
            sgmt = re.sub(r"^[-–—,\s]+", "", sgmt).strip()
            if institution is None and INST_KEY.search(sgmt):
                institution = sgmt
            elif title is None and not INST_KEY.search(sgmt) and len(sgmt) > 8:
                title = sgmt
        g, f = split_name(name)
        if not f:
            continue
        rows.append({"funder_award_id": f"rs-{slug(name)}-{year or ''}-{slug(scheme or '')[:20]}",
                     "title": title, "pi_given": g, "pi_family": f,
                     "institution": institution, "funder_scheme": scheme,
                     "start_year": year, "landing_page_url": url})
    return rows


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/royal_society_data"))
    ap.add_argument("--limit", type=int, default=None, help="cap cohort PAGES (smoke)")
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("Royal Society fellowships/grants -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    sm = s.get(SITEMAP, timeout=60, headers={"Accept-Encoding": "gzip"}).text
    locs = re.findall(r"<loc>([^<]+)</loc>", sm)
    cohorts = [l for l in locs if "/news/" in l and KW.search(l) and not SKIP.search(l)]
    print(f"cohort candidate pages: {len(cohorts)}")
    if a.limit:
        cohorts = cohorts[: a.limit]
    rows, seen = [], set()
    for n, url in enumerate(cohorts, 1):
        try:
            r = s.get(url, timeout=40)
        except Exception as e:
            print(f"  {url}: {e}"); continue
        if r.status_code != 200:
            continue
        new = 0
        for rec in parse_page(r.text, url):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec); new += 1
        if n % 10 == 0 or new:
            print(f"  [{n}/{len(cohorts)}] +{new} ({len(rows)}) {url.split('/news/')[-1][:40]}")
        time.sleep(0.2)

    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year": df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "funder_scheme", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    print("  dupes:", df.funder_award_id.duplicated().sum())
    out = a.output_dir / "royal_society_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    if a.limit:
        print(df.groupby(["pi_given","pi_family"]).size().sort_values(ascending=False).head(4).to_string())
        print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
