#!/usr/bin/env python3
"""
British Association for Counselling and Psychotherapy (BACP) to S3 Pipeline
==========================================================================

Scrapes BACP funded research grants (bacp.co.uk) and uploads a parquet to S3.

Data source: per-year grant pages (bacp-research-grants-<year>) linked from the
research awards hub. NOTE: the hub mixes funded grants with prizes (New Researcher
Award etc.) — only the bacp-research-grants-<year> pages are funded grants. Each
grant: "Project title:" + title, "Principal Investigator: Dr Name", "Lead
institution: X". Tiny funder (scheme launched 2023). Amounts not per-grant ->
NULL (§6.7 waiver). provenance `bacp`, priority 346. F4320312936 (Path A).

Output: s3://openalex-ingest/awards/bacp/bacp_grants.parquet
Usage: python bacp_to_s3.py [--limit N] [--skip-upload]
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

HUB = "https://www.bacp.co.uk/about-us/advancing-the-profession/research/awards/"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/bacp/bacp_grants.parquet"
EXPECTED_MIN = 2
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


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


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/bacp_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("BACP research grants -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    hub = BeautifulSoup(s.get(HUB, timeout=40).text, "html.parser")
    pages = sorted(set((h if h.startswith("http") else "https://www.bacp.co.uk" + h)
                       for h in (a2["href"] for a2 in hub.find_all("a", href=True))
                       if re.search(r"research-grants-20\d\d", h)))
    print(f"grant pages: {len(pages)}")
    rows, seen = [], set()
    for url in pages:
        ym = re.search(r"(20\d\d)", url)
        year = int(ym.group(1)) if ym else None
        soup = BeautifulSoup(s.get(url, timeout=40).text, "html.parser")
        for x in soup(["nav", "header", "footer", "script", "style"]):
            x.extract()
        lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
        for i, l in enumerate(lines):
            m = re.match(r"Principal Investigator\s*:\s*(.+)", l, re.I)
            if not m:
                continue
            g, f = split_name(m.group(1))
            title = institution = None
            for j in range(max(0, i - 4), min(i + 5, len(lines))):
                if re.match(r"Project title\s*:", lines[j], re.I):
                    title = clean(re.sub(r"Project title\s*:\s*", "", lines[j], flags=re.I)) or (clean(lines[j + 1]) if j + 1 < len(lines) else None)
                if re.match(r"Lead institution\s*:", lines[j], re.I):
                    institution = clean(re.sub(r"Lead institution\s*:\s*", "", lines[j], flags=re.I))
            if not f or not title:
                continue
            aid = f"bacp-{year}-{slug(f)}"
            if aid in seen: continue
            seen.add(aid)
            rows.append({"funder_award_id": aid, "title": title, "pi_given": g, "pi_family": f,
                         "institution": institution, "start_year": year, "landing_page_url": url})
        print(f"  {year}: {len(rows)} total")
        time.sleep(0.3)
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year": df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = a.output_dir / "bacp_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:30]), "|", repr(str(r2.title)[:38]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
