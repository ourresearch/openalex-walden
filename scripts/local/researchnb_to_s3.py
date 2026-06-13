#!/usr/bin/env python3
"""
New Brunswick Health Research Foundation / ResearchNB (Canada) to S3 Pipeline
============================================================================

Scrapes ResearchNB award-announcement pages (researchnb.ca) and uploads a
parquet to S3.

Data source: index https://researchnb.ca/research-funding/award-recipients/
links to per-program award-announcement-* detail pages. Each awardee is a text
block: "Name - Institution, City - $Amount" then the project title on the next
line. Program/year derived from the announcement title/slug. Recipient PI +
institution + amount (CAD) + title. provenance `researchnb`, priority 340.
F4320314075 (Path A).

Output: s3://openalex-ingest/awards/researchnb/researchnb_grants.parquet
Usage: python researchnb_to_s3.py [--limit N] [--skip-upload]
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

INDEX = "https://researchnb.ca/research-funding/award-recipients/"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/researchnb/researchnb_grants.parquet"
EXPECTED_MIN = 25
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
# Name - Institution(, City) - $Amount
ROW = re.compile(r"^(?P<name>[^-–]{4,60})\s[-–]\s(?P<inst>.+?)\s[-–]\s\$?(?P<amt>[\d,]+)\s*$")
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    toks = n.split()
    if not toks: return None, None
    if len(toks) == 1: return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/researchnb_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("ResearchNB award recipients -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    idx = BeautifulSoup(s.get(INDEX, timeout=40).text, "html.parser")
    pages = sorted(set(a2.get("href") for a2 in idx.find_all("a", href=True)
                       if a2.get("href") and re.search(r"award-announcement|awardee|recipients|-fund", a2["href"], re.I)
                       and "researchnb.ca" in a2["href"] and "/research-funding/" not in a2["href"]))
    print(f"announcement pages: {len(pages)}")
    rows, seen = [], set()
    for url in pages:
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find(["h1"])
        prog = clean(h1.get_text(" ", strip=True)) if h1 else None
        ym = re.search(r"(20\d\d)", url + " " + (prog or ""))
        year = int(ym.group(1)) if ym else None
        for x in soup(["nav", "header", "footer", "script", "style"]):
            x.extract()
        lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
        for i, l in enumerate(lines):
            m = ROW.match(l)
            if not m:
                continue
            amount = None
            try: amount = float(m.group("amt").replace(",", ""))
            except ValueError: pass
            title = lines[i + 1] if i + 1 < len(lines) else None
            if title and (ROW.match(title) or len(title) < 8):
                title = None
            g, f = split_name(m.group("name"))
            if not f:
                continue
            aid = f"researchnb-{slug(m.group('name'))}-{slug((title or prog or '')[:30])}"
            if aid in seen: continue
            seen.add(aid)
            rows.append({"funder_award_id": aid, "title": clean(title) or clean(prog),
                         "pi_given": g, "pi_family": f, "institution": clean(m.group("inst")),
                         "amount": amount, "start_year": year, "funder_scheme": prog,
                         "landing_page_url": url})
        print(f"  {url.split('/')[-2][:45]}: {len(rows)} total")
        time.sleep(0.2)
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    if a.limit: rows = rows[:a.limit]
    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year"): df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "amount"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "researchnb_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(4).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:28]), "| $", r2.amount)
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
