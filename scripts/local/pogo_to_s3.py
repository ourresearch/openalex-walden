#!/usr/bin/env python3
"""
Pediatric Oncology Group of Ontario (POGO) to S3 Data Pipeline
==============================================================

Scrapes POGO research-funding pages (pogo.ca) and uploads a parquet to S3.

Data source: static WordPress pages (no grants post type):
    /research-data/pogo-research-unit/active-grants/
    /research-data/pogo-research-unit/past-grants/
    /research-data/pogo-research-unit/research-funding/project-funding-grant-recipients/
    /research-data/pogo-research-unit/research-funding/current-seed-grant-recipients/
Grants are <strong>-labelled blocks: "Project Title:", "Principal
Investigator(s):", "Co-Investigators:", "Project Summary:", "Funding:" — parsed
by splitting on the labels; year from a preceding heading. ~20-40 grants.
provenance `pogo`, priority 341. F4320319937 (Path A).

Output: s3://openalex-ingest/awards/pogo/pogo_grants.parquet
Usage: python pogo_to_s3.py [--limit N] [--skip-upload]
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

PAGES = [
    "https://www.pogo.ca/research-data/pogo-research-unit/past-grants/",
    "https://www.pogo.ca/research-data/pogo-research-unit/active-grants/",
    "https://www.pogo.ca/research-data/pogo-research-unit/research-funding/project-funding-grant-recipients/",
    "https://www.pogo.ca/research-data/pogo-research-unit/research-funding/current-seed-grant-recipients/",
]
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/pogo/pogo_grants.parquet"
EXPECTED_MIN = 12
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip().strip(":").strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    n = re.split(r"\band\b|,|;|&", n)[0].strip()
    n = re.sub(r",?\s*(PhD|MD|FRCPC|MSc|MEd|RN|Ph\.D|M\.D).*$", "", n, flags=re.I).strip()
    toks = n.split()
    if len(toks) < 2 or len(toks) > 5: return None, None
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def parse_page(html, url):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    text = soup.get_text("\n", strip=True)
    # Split into blocks by "Project Title:" where present
    rows = []
    if "Project Title" in text:
        blocks = re.split(r"(?=Project Title\s*:)", text)
        for b in blocks:
            title = re.search(r"Project Title\s*:\s*(.+)", b)
            pi = re.search(r"Principal Investigator(?:\(s\))?\s*:\s*(.+)", b)
            fund = re.search(r"Funding\s*:\s*\$?\s*([\d,.]+)", b)
            summ = re.search(r"Project Summary\s*:\s*(.+)", b)
            yr = re.search(r"\b(20[012]\d)\b", b)
            if not title or not pi:
                continue
            g, f = split_name(pi.group(1))
            if not f:
                continue
            amount = None
            if fund:
                try: amount = float(fund.group(1).replace(",", ""))
                except ValueError: pass
            rows.append({"title": clean(title.group(1)), "pi_given": g, "pi_family": f,
                         "amount": amount, "description": clean(summ.group(1))[:2000] if summ else None,
                         "start_year": int(yr.group(1)) if yr else None})
    else:
        # active-grants: "Principal Investigator" anchored, title is preceding heading
        lines = [l for l in text.split("\n") if l.strip()]
        for i, l in enumerate(lines):
            if re.match(r"Principal Investigator", l, re.I):
                pi_val = re.sub(r"Principal Investigator(?:\(s\))?\s*:?\s*", "", l, flags=re.I) or (lines[i + 1] if i + 1 < len(lines) else "")
                g, f = split_name(pi_val)
                title = lines[i - 1] if i >= 1 else None
                if not f or not title:
                    continue
                rows.append({"title": clean(title), "pi_given": g, "pi_family": f,
                             "amount": None, "description": None, "start_year": None})
    for r in rows:
        r["funder_award_id"] = f"pogo-{slug(r['title'][:40])}-{slug(r['pi_family'])}"
        r["landing_page_url"] = url
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
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/pogo_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("POGO research grants -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    rows, seen = [], set()
    for url in PAGES:
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            print(f"  {url.split('/')[-2]}: HTTP {r.status_code}"); continue
        for rec in parse_page(r.text, url):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        print(f"  {url.split('/')[-2]}: {len(rows)} total")
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
    for c in ("title", "pi_family", "amount", "description"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "pogo_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(4).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "| $", r2.amount, "|", repr(str(r2.title)[:40]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
