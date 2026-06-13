#!/usr/bin/env python3
"""
IFAU (Institute for Evaluation of Labour Market and Education Policy, Sweden)
to S3 Data Pipeline.

IFAU both runs research and awards competitive external grants. The awarded
grants are listed at:
    https://www.ifau.se/en/Research/Research-grants/awarded-research-grants/
Each grant is three lines: project title, PI name, then ", Institution. NNN kr"
(SEK). ~26 grants. provenance `ifau`, priority 338. F4320327653 (Path A).

Output: s3://openalex-ingest/awards/ifau/ifau_grants.parquet
Usage: python ifau_to_s3.py [--limit N] [--skip-upload]
Author: OpenAlex Team
"""
import argparse, re, subprocess, sys
from pathlib import Path
import pandas as pd, requests
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass
from bs4 import BeautifulSoup

URLS = ["https://www.ifau.se/en/Research/Research-grants/awarded-research-grants/",
        "https://www.ifau.se/sv/Forskning/Forskningsbidrag/beviljade-forskningsbidrag/"]
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/ifau/ifau_grants.parquet"
EXPECTED_MIN = 15
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
AMT = re.compile(r",\s*(.+?)\.?\s*([\d  \xa0]{5,})\s*kr", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip().strip(",.")
    return s or None


def split_name(raw):
    n = clean(raw)
    if not n: return None, None
    toks = n.split()
    if len(toks) < 2 or len(toks) > 4: return None, None
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
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ifau_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("IFAU awarded research grants -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    r = s.get(URLS[0], timeout=40)
    soup = BeautifulSoup(r.text, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    rows, seen = [], set()
    cur_year = None
    for i, l in enumerate(lines):
        ym = re.search(r"\b(20[12]\d)\b", l)
        if ym and ("minute" in l.lower() or "distribution" in l.lower() or "beslut" in l.lower()):
            cur_year = int(ym.group(1))
        m = AMT.search(l)
        if not m:
            continue
        # this line = ", Institution. NNN kr"; PI = prev line; title = line before that
        institution = clean(m.group(1))
        amt = m.group(2)
        amount = None
        if amt:
            try: amount = float(re.sub(r"[  \xa0]", "", amt))
            except ValueError: amount = None
        pi_line = lines[i - 1] if i >= 1 else ""
        title = lines[i - 2] if i >= 2 else None
        g, f = split_name(pi_line)
        if not title or len(title) < 12 or not f:
            continue
        aid = f"ifau-{slug(title[:40])}-{slug(f)}"
        if aid in seen: continue
        seen.add(aid)
        rows.append({"funder_award_id": aid, "title": clean(title), "pi_given": g,
                     "pi_family": f, "institution": institution, "amount": amount,
                     "start_year": cur_year, "landing_page_url": URLS[0]})
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
    if df.amount.notna().any(): print("  SEK median", int(df.amount.median()))
    out = a.output_dir / "ifau_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(4).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:30]), "|", r2.amount)
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
