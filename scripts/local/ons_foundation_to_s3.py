#!/usr/bin/env python3
"""
Oncology Nursing Society Foundation (ONS Foundation) to S3 Data Pipeline
=======================================================================

Scrapes ONS Foundation research-grant recipients (onfgivesback.org) and uploads
a parquet to S3.

Data source: the research-grant page lists recent recipients as a name line
("Name, Credentials") followed by a "YYYY RExx ... Research Grant Recipient"
line (grant scheme + year). The press releases are prose (mostly scholarships),
so the research-grant page is the cleanest structured source. Project title and
institution are not published per recipient, so display_name is the grant scheme
and PI is the recipient; institution/amount NULL (§6.7 waiver). ~5-12 recipients.
provenance `ons_foundation`, priority 347. F4320308531 (Path A).

Output: s3://openalex-ingest/awards/ons_foundation/ons_foundation_grants.parquet
Usage: python ons_foundation_to_s3.py [--limit N] [--skip-upload]
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

URL = "https://www.onfgivesback.org/funding-for-nurses/research/research-grant"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/ons_foundation/ons_foundation_grants.parquet"
EXPECTED_MIN = 4
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
RECIP = re.compile(r"(20\d\d)\s+(.*?Research Grant)\s+Recipient", re.I)
NAME_LINE = re.compile(r"^(Dr\.?\s+)?[A-Z][A-Za-z'’\-]+\s+.*\b(PhD|RN|MSN|BSN|DNP|MD|FAAN|MS|MPH)\b", re.I)
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    n = n.split(",")[0].strip()
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
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ons_foundation_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("ONS Foundation research-grant recipients -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    soup = BeautifulSoup(s.get(URL, timeout=40).text, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    rows, seen = [], set()
    for i, l in enumerate(lines):
        m = RECIP.search(l)
        if not m:
            continue
        # name is the preceding line
        name_line = lines[i - 1] if i >= 1 else ""
        if not NAME_LINE.match(name_line):
            continue
        g, f = split_name(name_line)
        if not f:
            continue
        year = int(m.group(1))
        scheme = clean(m.group(2))
        aid = f"ons-{year}-{slug(f)}-{slug(scheme)[:20]}"
        if aid in seen: continue
        seen.add(aid)
        rows.append({"funder_award_id": aid,
                     "title": f"{scheme} ({year})",
                     "pi_given": g, "pi_family": f,
                     "funder_scheme": scheme, "start_year": year,
                     "landing_page_url": URL})
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    if a.limit: rows = rows[:a.limit]
    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year": df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "funder_scheme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "ons_foundation_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.title)[:45]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
