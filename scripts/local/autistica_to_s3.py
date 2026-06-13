#!/usr/bin/env python3
"""
Autistica (UK) to S3 Data Pipeline
==================================

Scrapes Autistica funded research projects (autistica.org.uk) and uploads a
parquet to S3.

Data source: two listing pages (current + previous research projects) linking to
per-project detail pages, each with a "Project details" block: Project lead
(PI — may be a person or, for internally-led work, an organisation), Organisation
(recipient org), Location, Starts (start date), Duration. ~46 projects. Non-person
"Project lead" values are NULLed. Amounts not published -> NULL (§6.7 waiver).
provenance `autistica`, priority 343. F4320312945 (Path A; dup F4320324910).

Output: s3://openalex-ingest/awards/autistica/autistica_projects.parquet
Usage: python autistica_to_s3.py [--limit N] [--skip-upload]
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

LISTS = ["https://www.autistica.org.uk/our-research/research-projects",
         "https://www.autistica.org.uk/our-research/previous-research-projects"]
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/autistica/autistica_projects.parquet"
EXPECTED_MIN = 30
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)
NON_PERSON = re.compile(r"Universit|College|Institute|NHS|Trust|Autistica|Foundation|"
                        r"Centre|Center|Network|Council|Charity|Hospital|Limited|Ltd|Group", re.I)
MONTHS = "January February March April May June July August September October November December"


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    n = re.split(r",|;| and ", n)[0].strip()
    toks = n.split()
    if len(toks) < 2 or len(toks) > 4: return None, None
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def label_val(lines, label):
    for i, l in enumerate(lines):
        if l.strip().lower() == label.lower() and i + 1 < len(lines):
            return clean(lines[i + 1])
    return None


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/autistica_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("Autistica research projects -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    detail = set()
    for u in LISTS:
        soup = BeautifulSoup(s.get(u, timeout=40).text, "html.parser")
        for a2 in soup.find_all("a", href=True):
            if re.search(r"/research-projects/[a-z]", a2["href"]):
                href = a2["href"]
                detail.add(href if href.startswith("http") else "https://www.autistica.org.uk" + href)
    detail = sorted(detail)
    print(f"detail pages: {len(detail)}")
    if a.limit:
        detail = detail[: a.limit]
    rows, seen = [], set()
    for n, url in enumerate(detail, 1):
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        title = None
        if soup.title:
            title = clean(re.sub(r"\s*[|\-–]\s*Autistica.*$", "", soup.title.get_text(" ", strip=True), flags=re.I))
        if not title or title.lower() == "autistica":
            # fallback: an h1 that isn't the brand
            for h in soup.find_all("h1"):
                t = clean(h.get_text(" ", strip=True))
                if t and t.lower() != "autistica":
                    title = t
                    break
        for x in soup(["nav", "header", "footer", "script", "style"]):
            x.extract()
        lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
        lead = label_val(lines, "Project lead")
        org = label_val(lines, "Organisation")
        starts = label_val(lines, "Starts")
        year = None
        if starts:
            ym = re.search(r"(20\d\d)", starts)
            year = int(ym.group(1)) if ym else None
        g, f = (None, None)
        if lead and not NON_PERSON.search(lead):
            g, f = split_name(lead)
        if not title:
            continue
        aid = f"autistica-{slug(url.rstrip('/').rsplit('/',1)[-1])}"
        if aid in seen: continue
        seen.add(aid)
        rows.append({"funder_award_id": aid, "title": title, "pi_given": g, "pi_family": f,
                     "institution": org, "start_year": year, "landing_page_url": url})
        if n % 15 == 0:
            print(f"  [{n}/{len(detail)}] {len(rows)}")
        time.sleep(0.2)
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year": df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "autistica_projects.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(4).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:30]), "|", repr(str(r2.title)[:36]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
