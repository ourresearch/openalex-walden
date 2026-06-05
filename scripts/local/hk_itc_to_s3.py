#!/usr/bin/env python3
"""
HK ITC (Innovation and Technology Commission, Hong Kong) to S3 Data Pipeline
=============================================================================

Downloads the Innovation and Technology Fund's annual approved-projects CSVs
from itf.gov.hk and writes a parquet for Databricks.

Data Source:
  http://www.itf.gov.hk/datagovhk/annuallist_{YYYY}_e.csv
  6 annual CSVs available 2019-2024 (~1,800 unique R&D grants total).
  2019 file is cp1252-encoded; 2020+ are UTF-8.

Output: s3://openalex-ingest/awards/hk_itc/hk_itc_projects.parquet

Schema notes:
  - funder_award_id = `Reference No.` (e.g. ITS/047/18FX, ARD/301) — the real
    cited grant ID used by ITC-funded papers.
  - amount = Funds Approved ($M) × 1,000,000 in HKD; revised amount preferred
    when present.
  - Lead Applicant is the recipient institution (companies, universities,
    applied-science orgs). No PI names in source.
  - Programme = ITSP / MHKJFS / TCFS / PRP / MRP / NIFS / UICP — the funding
    scheme acronym.

Usage:
    python hk_itc_to_s3.py --skip-upload
"""
import argparse, json, re, sys, urllib.request
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/hk_itc/hk_itc_projects.parquet"
BASE_URL  = "http://www.itf.gov.hk/datagovhk/annuallist_{year}_e.csv"
YEARS     = list(range(2019, 2025))   # 2019-2024 available; probe extends naturally if newer years appear

def fetch_year(year, output_dir):
    url = BASE_URL.format(year=year)
    out = output_dir / f"itc_{year}.csv"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "OpenAlex-HK-ITC/1.0 (contact@openalex.org)"})
        raw = urllib.request.urlopen(req, timeout=30).read()
        if len(raw) < 2000:  # 404 response is a tiny HTML page
            print(f"  {year}: skip (response {len(raw)}b — likely 404)")
            return None
        out.write_bytes(raw)
        return out
    except Exception as e:
        print(f"  {year}: err {e}")
        return None

def parse_year(path, year):
    # 2019 is cp1252; 2020+ are utf-8
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise SystemExit(f"could not decode {path}")
    # Use the canonical first 8 columns (CSVs have a long footnote in column 9)
    canonical = ["No.", "Programme*", "Reference No.", "Project Title", "Lead Applicant",
                 "Funds Approved ($M)", "Revised Funds Approved ($M)", "Remarks"]
    cols = [c for c in canonical if c in df.columns]
    df = df[cols].copy()
    # Filter out rows where No. isn't a numeric — they're footnote rows
    df = df[df["No."].astype("string").str.match(r"^\d+\.?$", na=False)]
    df["_year"] = year
    return df

def parse_amount(s):
    if s is None or pd.isna(s): return None
    s = str(s).strip().replace(",", "").replace("$", "")
    if s in ("", "-", "N/A", "NA"): return None
    try:
        v = float(s) * 1_000_000     # source is in $M HKD
        return v if v > 0 else None
    except ValueError:
        return None

def transform(dfs):
    df = pd.concat(dfs, ignore_index=True).replace("", pd.NA)
    # Prefer revised amount, fall back to original
    df["amount_revised"] = df.get("Revised Funds Approved ($M)", pd.Series([None]*len(df))).map(parse_amount)
    df["amount_orig"] = df["Funds Approved ($M)"].map(parse_amount)
    df["amount"] = df["amount_revised"].combine_first(df["amount_orig"])
    df["funder_award_id"] = df["Reference No."].astype("string").str.strip()
    df["title"] = df["Project Title"].astype("string").str.strip()
    df["funder_scheme"] = df["Programme*"].astype("string").str.strip()
    df["institution_name"] = df["Lead Applicant"].astype("string").str.strip()
    df["year"] = pd.to_numeric(df["_year"], errors="coerce").astype("Int64")
    df["remarks"] = df.get("Remarks", pd.Series([None]*len(df))).astype("string").str.strip()
    # Drop the empty funder_award_id rows (any footnote stragglers)
    df = df[df["funder_award_id"].notna() & (df["funder_award_id"] != "")]
    # Slug-collision guard — Reference No. should be unique per (year, project)
    # but the same Reference No. can appear in multiple years if it spans them
    # (the ITC publishes one-row-per-grant-year, not aggregated). Aggregate by Reference No.:
    agg = df.groupby("funder_award_id", dropna=True).agg(
        title=("title", "first"),
        funder_scheme=("funder_scheme", "first"),
        institution_name=("institution_name", "first"),
        amount=("amount", "max"),                       # max across years = the revised total
        year=("year", "min"),                            # earliest year = grant award year
        remarks=("remarks", lambda s: " | ".join(sorted(set(x for x in s if pd.notna(x))))[:500]),
        n_year_rows=("funder_award_id", "size"),
    ).reset_index()
    # Future-year cap
    yr_now = datetime.now(timezone.utc).year
    agg.loc[agg["year"] > yr_now + 1, "year"] = pd.NA
    # Slug-collision guard
    dup = agg["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate funder_award_id values")
    agg["provenance"] = "hk_itc_itf"
    agg["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in agg.columns:
        if agg[c].dtype == object: agg[c] = agg[c].astype("string")
    return agg

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}.")
        print(f"  §1.4 shrink-check OK")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="HK ITC (itf.gov.hk) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/hk_itc"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("HK ITC (Innovation and Technology Fund) -> S3")
    print("=" * 60)
    dfs = []
    print(f"[1/2] Downloading annual ITF CSVs")
    for year in YEARS:
        p = fetch_year(year, a.output_dir)
        if p is None: continue
        df = parse_year(p, year)
        print(f"  {year}: {len(df):,} grants")
        dfs.append(df)
    if not dfs:
        raise SystemExit("no year files downloaded")
    print(f"[2/2] Aggregating by Reference No. across years")
    df = transform(dfs)
    out = a.output_dir / "hk_itc_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} unique grants, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    amt_cov = 100 * (df["amount"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id  {len(df):,} (100%)")
    print(f"  title            {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  HKD amount       {(df['amount']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  institution      {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  year             {df['year'].notna().sum():,} ({nn('year'):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  Amount median: HKD {float(df['amount'].median()):,.0f}, max: HKD {float(df['amount'].max()):,.0f}")
    if df["year"].notna().any():
        print(f"  Year range: {int(df['year'].min())}-{int(df['year'].max())}")
    print(f"\nTop programmes: {df['funder_scheme'].value_counts().head(5).to_dict()}")
    print(f"Top institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws:
            subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else:
            print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateHKITCAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
