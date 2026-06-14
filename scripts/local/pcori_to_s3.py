#!/usr/bin/env python3
"""
Patient-Centered Outcomes Research Institute (PCORI) to S3 Data Pipeline
=======================================================================

Builds a parquet from PCORI's bulk project export (preferred over scraping the
JS faceted-search pages, which carry far less per project).

Data source: the "Download Results" CSV from PCORI's Explore-Our-Portfolio:
    https://www.pcori.org/explore-our-portfolio  ->  "Download Results"
    -> /sites/default/files/views_data_export/.../project-export.csv
~2,683 projects × 16 columns: Title, Project Status, Funding Type, State,
Intervention Strategy, Health Conditions, Project End Date, Organization,
Project Budget ($), Award Date, Principal Investigator, Other PI, Project Lead,
URL, Year Completed. Organization/Budget 100%, PI 47% (PCORI lists N/A for
evidence-synthesis products). USD amounts. provenance `pcori`, priority 345.
F4320308927 (Path A).

NOTE: PCORI's WAF (obolus challenge) ConnectionResets plain-HTTP/headless
requests, so the CSV is obtained via the browser (Chrome extension) "Download
Results" export — it lands in Downloads. This script reads that local CSV.

Output: s3://openalex-ingest/awards/pcori/pcori_projects.parquet
Usage:
    python pcori_to_s3.py --input ~/Downloads/project-export.csv
    python pcori_to_s3.py --input <csv> --skip-upload
Author: OpenAlex Team
"""
import argparse, re, subprocess, sys
from pathlib import Path
import pandas as pd
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

S3_BUCKET, S3_KEY = "openalex-ingest", "awards/pcori/pcori_projects.parquet"
EXPECTED_MIN = 2000
DEGREE = re.compile(r",?\s*\b(M\.?D|Ph\.?D|Dr\.?P\.?H|M\.?P\.?H|M\.?S|M\.?P\.?P|R\.?N|"
                    r"M\.?B\.?A|Sc\.?D|Pharm\.?D|D\.?O|M\.?S\.?W|J\.?D|M\.?H\.?S|FACP)\b\.?", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], 1)}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    if s.lower() in ("", "n/a", "na", "none", "nan"):
        return None
    return s


def split_name(raw):
    n = clean(raw)
    if not n:
        return None, None
    n = n.split(",")[0].strip()  # name is before the first degree comma
    n = DEGREE.sub("", n).strip()
    toks = n.split()
    if len(toks) < 2 or len(toks) > 5:
        return None, None
    return " ".join(toks[:-1]), toks[-1]


def parse_amount(raw):
    s = clean(raw)
    if not s:
        return None
    m = re.search(r"([\d,]+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_year(raw):
    s = clean(raw) or ""
    m = re.search(r"(20\d\d|19\d\d)", s)
    return int(m.group(1)) if m else None


def start_date(raw):
    s = clean(raw) or ""
    m = re.search(r"([A-Za-z]+)\s+(20\d\d)", s)
    if m and m.group(1) in MONTHS:
        return f"{m.group(2)}-{MONTHS[m.group(1)]:02d}-01"
    return None


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser(description="PCORI project export -> S3")
    ap.add_argument("--input", type=Path, required=True, help="path to project-export.csv (browser download)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/pcori_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("PCORI project export -> S3")
    src = pd.read_csv(a.input, dtype=str, encoding="utf-8-sig")
    print(f"CSV rows: {len(src)}; cols: {list(src.columns)}")

    rows, seen = [], set()
    for _, r in src.iterrows():
        title = clean(r.get("Title"))
        if not title:
            continue
        url = clean(r.get("URL"))
        landing = ("https://" + url) if url and not url.startswith("http") else url
        aid = f"pcori-{slug(url.split('/')[-1]) if url else slug(title)}"
        if aid in seen:
            aid = f"{aid}-{len(rows)}"
        seen.add(aid)
        g, f = split_name(r.get("Principal Investigator"))
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "pi_given": g, "pi_family": f,
            "institution": clean(r.get("Organization")),
            "amount": parse_amount(r.get("Project Budget")),
            "start_date": start_date(r.get("Award Date")),
            "start_year": parse_year(r.get("Award Date")),
            "end_year": parse_year(r.get("Year Completed")) or parse_year(r.get("Project End Date")),
            "funder_scheme": clean(r.get("Funding Type")),
            "status": clean(r.get("Project Status")),
            "health_conditions": clean(r.get("Health Conditions")),
            "landing_page_url": landing,
        })
    if a.limit:
        rows = rows[: a.limit]
    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    for c in ("start_year", "end_year"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year", "end_year"):
            df[c] = df[c].astype("string")
    if not a.limit and len(df) < EXPECTED_MIN:
        print(f"[ERROR] only {len(df)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    print(f"\nDataFrame: {len(df)} rows")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        nn = df[c].notna().sum(); print(f"  {c}: {nn}/{len(df)} ({round(100*nn/max(len(df),1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}; amount USD median {df.amount.median():.0f}")
    out = a.output_dir / "pcori_projects.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    if a.limit:
        print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
