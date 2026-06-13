#!/usr/bin/env python3
"""
Batten Disease Support and Research Association / BDGRI to S3 Data Pipeline
==========================================================================

Scrapes the Batten Disease Global Research Initiative funded-project pages
(battenresearch.org) and uploads a parquet to S3. BDSRA Foundation is a founding
funder of BDGRI; the BDGRI grant rounds are the structured source.

Data source: per-year recipient pages linked from
battenresearch.org/funded-projects/. Each project: title, then "CHIEF
INVESTIGATORS:" name(s), "AFFILIATION:" institution, "AMOUNT: USD $N". ~tens of
projects (2025, 2026 rounds). provenance `bdsra`, priority 345. F4320308508
(Path A). Consortium-funded (BDGRI = BDSRA US/Canada/Australia + BDFA UK).

Output: s3://openalex-ingest/awards/bdsra/bdsra_grants.parquet
Usage: python bdsra_to_s3.py [--limit N] [--skip-upload]
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

HUB = "https://battenresearch.org/funded-projects/"
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/bdsra/bdsra_grants.parquet"
EXPECTED_MIN = 5
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_TITLE_RE = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    n = re.sub(r",?\s*(PhD|MD|Ph\.D|M\.D|DVM|MBBS).*$", "", n, flags=re.I).strip()
    toks = n.split()
    if len(toks) < 2 or len(toks) > 4: return None, None
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def parse_page(html, url, year):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["nav", "header", "footer", "script", "style"]):
        x.extract()
    lines = [l for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    rows = []
    for i, l in enumerate(lines):
        if not re.match(r"CHIEF INVESTIGATORS?:", l, re.I):
            continue
        title = lines[i - 1] if i >= 1 else None
        pi = lines[i + 1] if i + 1 < len(lines) else None
        institution = amount = None
        for j in range(i + 1, min(i + 12, len(lines))):
            if re.match(r"AFFILIATION:", lines[j], re.I):
                institution = clean(lines[j + 1]) if j + 1 < len(lines) else None
                if j + 2 < len(lines) and not re.match(r"AMOUNT:", lines[j + 2], re.I) and "USA" not in (institution or "") + "x":
                    institution = clean((institution or "") + " " + lines[j + 2])
            if re.match(r"AMOUNT:", lines[j], re.I):
                am = re.search(r"([\d,]+)", lines[j] + (" " + lines[j + 1] if j + 1 < len(lines) else ""))
                if am:
                    try: amount = float(am.group(1).replace(",", ""))
                    except ValueError: pass
                break
        g, f = split_name(pi)
        if not title or not f or re.match(r"CHIEF|AFFIL|AMOUNT", title, re.I):
            continue
        rows.append({"funder_award_id": f"bdsra-{year}-{slug(f)}-{slug(title[:30])}",
                     "title": clean(title), "pi_given": g, "pi_family": f,
                     "institution": institution, "amount": amount, "start_year": year,
                     "landing_page_url": url})
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
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/bdsra_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("BDGRI / BDSRA grant recipients -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    hub = BeautifulSoup(s.get(HUB, timeout=40).text, "html.parser")
    pages = sorted(set(a2["href"] for a2 in hub.find_all("a", href=True)
                       if re.search(r"grant-recipient|funded", a2["href"], re.I) and "battenresearch.org" in a2["href"]
                       and "funded-projects" not in a2["href"]))
    print(f"recipient pages: {len(pages)}")
    rows, seen = [], set()
    for url in pages:
        ym = re.search(r"(20\d\d)", url)
        year = int(ym.group(1)) if ym else None
        try:
            r = s.get(url, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        for rec in parse_page(r.text, url, year):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        print(f"  {year}: {len(rows)} total")
        time.sleep(0.3)
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
    out = a.output_dir / "bdsra_grants.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(5).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:28]), "| $", r2.amount)
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
