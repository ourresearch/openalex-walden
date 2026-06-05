#!/usr/bin/env python3
"""
INCa (Institut National Du Cancer, France) to S3 Data Pipeline
================================================================

Downloads the official INCa funded-projects CSV from data.gouv.fr and writes
a parquet for Databricks.

Data Source:
  CKAN organization: https://www.data.gouv.fr/api/1/organizations/institut-national-du-cancer/
  Dataset: projets-de-recherche-soutenus-par-linstitut-2008-2022
  Direct CSV: https://static.data.gouv.fr/resources/projets-de-recherche-soutenus-par-linstitut-2008-2022/.../data-gouv-2008-2022.csv
  ~2.2K projects 2008-2022. Re-extract from the dataset endpoint at refresh
  time so URL rotation doesn't break this.

Output: s3://openalex-ingest/awards/inca/inca_projects.parquet

Schema notes:
  - Reference is a numeric project id; funder_award_id is constructed as
    "{Funder}-{Reference}" (e.g. INCa-513, INCa-DGOS-6368) to match the
    cited grant format in papers (verified against OpenAIRE INCa records).
  - Amount is in EUR, formatted "1?570?367 €" where ? is cp1252 nbsp. Strip
    all whitespace + "€" and parse as float. 100% coverage on the source.
  - EN + FR abstracts are both published. Prefer EN for description; fall
    back to FR (saves ~21% English-NULL rows).
  - Co-funded grants carry a compound Funder field (e.g. INCa-DGOS, INCa-Fondation
    ARC-LNCC). funder_award_id keeps the compound prefix so identical numeric
    reference under different funder combinations stays distinct.

Usage:
    python inca_to_s3.py --skip-upload
"""
import argparse, csv, io, json, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import urllib.request
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/inca/inca_projects.parquet"
ORG_SLUG = "institut-national-du-cancer"
DATASET_SLUG = "projets-de-recherche-soutenus-par-linstitut-2008-2022"
DATA_GOUV_API = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_SLUG}/"

# Amount in source: "1?570?367 €" (cp1252 nbsp = chr 0xA0, displays as ? when forced ASCII)
# Strip all whitespace + the euro symbol; treat empty as None.
_AMOUNT_CLEAN = re.compile(r"[\s\?€]+")

def fetch_dataset_metadata():
    """Hit the data.gouv.fr dataset endpoint and return the CSV resource URL.
    Re-fetch every run so we pick up the latest snapshot rather than a pinned URL."""
    req = urllib.request.Request(DATA_GOUV_API, headers={"User-Agent": "OpenAlex-INCa/1.0 (contact@openalex.org)"})
    d = json.loads(urllib.request.urlopen(req, timeout=30).read())
    for r in d.get("resources", []):
        if r.get("format") == "csv":
            return r["url"], r.get("title", "")
    raise SystemExit("no CSV resource found on the INCa dataset")

def parse_amount(s):
    if not s: return None
    s = _AMOUNT_CLEAN.sub("", str(s)).replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None  # §6.7
    except ValueError:
        return None

def download(output_dir: Path):
    url, title = fetch_dataset_metadata()
    print(f"[1/2] Downloading INCa CSV: {title}")
    print(f"      url={url}")
    csv_path = output_dir / "inca_raw.csv"
    urllib.request.urlretrieve(url, csv_path)
    print(f"      saved {csv_path.name}: {csv_path.stat().st_size/1e6:.1f} MB")
    return csv_path

def transform(csv_path: Path) -> pd.DataFrame:
    print(f"[2/2] Parsing CSV (cp1252) -> normalized DataFrame")
    df = pd.read_csv(csv_path, sep=";", encoding="cp1252", dtype=str, keep_default_na=False)
    df = df.replace("", pd.NA)
    # Construct funder_award_id from Funder + Reference (e.g. INCa-513, INCa-DGOS-6368)
    df["funder_award_id"] = df["Funder"].fillna("INCa").str.strip() + "-" + df["Reference"].astype(str).str.strip()
    # Drop rows with no Reference (would produce 'Funder-nan')
    df = df[df["Reference"].notna() & (df["Reference"].str.strip() != "")]
    # Amount
    df["amount_eur"] = df["Amount"].map(parse_amount).astype("Float64")
    # PI names (cp1252 already decoded by pd.read_csv with encoding=cp1252)
    df["pi_given_name"] = df["Investigator.Firstname"].astype("string").str.strip()
    df["pi_family_name"] = df["Investigator.Lastname"].astype("string").str.strip()
    # Description prefers EN, falls back to FR
    en = df["Summary.En"].astype("string").str.strip()
    fr = df["Summary.Fr"].astype("string").str.strip()
    df["description"] = en.fillna(fr)
    # Other fields
    df["title"] = df["Title"].astype("string").str.strip()
    df["call_year"] = pd.to_numeric(df["Call.Year"], errors="coerce").astype("Int64")
    df["call_description"] = df["Call.Description"].astype("string").str.strip()
    df["call_reference"] = df["Call.Reference"].astype("string").str.strip()
    df["acronym"] = df["Acronym"].astype("string").str.strip()
    df["institution_name"] = df["Investigator.Research_Organization.Name"].astype("string").str.strip()
    df["institution_city"] = df["Investigator.Research_Organization.City"].astype("string").str.strip()
    df["funder_label"] = df["Funder"].astype("string").str.strip()
    # Future-year cap
    yr_now = datetime.now(timezone.utc).year
    df.loc[df["call_year"] > yr_now + 1, "call_year"] = pd.NA
    # Dedup on funder_award_id (raise — same {Funder}-{Reference} twice is a data bug)
    dup = df["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate funder_award_id values — investigate")
    # Project columns + provenance
    out = df[[
        "funder_award_id", "title", "description", "amount_eur",
        "pi_given_name", "pi_family_name", "institution_name", "institution_city",
        "call_year", "call_description", "call_reference", "acronym", "funder_label",
    ]].copy()
    out["provenance"] = "inca_data_gouv"
    out["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # All object → pandas string
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].astype("string")
    return out

def check_no_shrink(df, allow_shrink):
    """§1.4: refuse to upload a parquet smaller than the one currently in S3 unless --allow-shrink."""
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(
                f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}. "
                f"Pass --allow-shrink only if this shrink is real."
            )
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="INCa (data.gouv.fr CSV) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/inca"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print(f"INCa (Institut National Du Cancer) -> S3")
    print("=" * 60)
    csv_path = download(a.output_dir)
    df = transform(csv_path)
    out = a.output_dir / "inca_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().mean()
    amt_cov = 100 * (df["amount_eur"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id   {len(df):,} (100%)")
    print(f"  title             {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  description       {df['description'].notna().sum():,} ({nn('description'):.1f}%)")
    print(f"  EUR amount        {(df['amount_eur']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  PI family_name    {df['pi_family_name'].notna().sum():,} ({nn('pi_family_name'):.1f}%)")
    print(f"  institution       {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  call_year         {df['call_year'].notna().sum():,} ({nn('call_year'):.1f}%)")
    print(f"  acronym           {df['acronym'].notna().sum():,} ({nn('acronym'):.1f}%)  -- often missing; expected")
    print(f"\nYear range: {int(df['call_year'].min())}-{int(df['call_year'].max())}")
    print(f"Top funders: {df['funder_label'].value_counts().head(5).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws:
            subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else:
            print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateINCaAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
