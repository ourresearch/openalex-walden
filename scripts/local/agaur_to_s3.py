#!/usr/bin/env python3
"""
AGAUR (Agència de Gestió d'Ajuts Universitaris i de Recerca, Catalonia) to S3
==============================================================================

Pulls AGAUR-issued grants from the Catalonia RAISC public registry exposed via
the analisi.transparenciacatalunya.cat Socrata API.

Data Source:
  RAISC: https://analisi.transparenciacatalunya.cat/resource/s9xt-n979.json
  Filter: entitat_oo_aa_o_departament_1 like '%AGAUR%' AND beneficiary != anonymized
  Aggregation: rows are monthly payments; aggregate by `clau` prefix (strip
  trailing "[YYYYMMDD]") to get one row per grant.

Output: s3://openalex-ingest/awards/agaur/agaur_projects.parquet

Schema notes:
  - AGAUR beneficiaries are INSTITUTIONS (universities, foundations, companies);
    no PI names in source. lead_investigator NULL across all rows; affiliation =
    the institution.
  - Anonymized rows (`ra_social_del_beneficiari` = "Persona física" or "Benef. no
    publicable") are EXCLUDED — these are individual student bursaries with GDPR-
    redacted recipients; ~660K rows of those exist but are unusable as
    grant records.
  - Amount aggregated from monthly payments per clau prefix.

Output columns:
  funder_award_id  = clau prefix (e.g. "DOC-23-019-22285")
  title            = objecte_de_la_convocat_ria (programme description)
  amount           = sum of monthly payments in EUR
  institution_name = ra_social_del_beneficiari (the awarded institution)
  start_year       = year of earliest payment
  funder_scheme    = the convocatòria title

Usage:
    python agaur_to_s3.py --skip-upload
"""
import argparse, json, re, sys, urllib.request, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/agaur/agaur_projects.parquet"
SOCRATA_RESOURCE = "https://analisi.transparenciacatalunya.cat/resource/s9xt-n979.json"
# AGAUR filter — entitat_oo_aa_o_departament_1 contains "AGAUR"
# Exclude anonymized beneficiaries (private persons under GDPR redaction)
SOQL_WHERE = (
    "entitat_oo_aa_o_departament_1 like '%AGAUR%' "
    "AND ra_social_del_beneficiari not like '%no publicable%' "
    "AND ra_social_del_beneficiari not like 'Persona%'"
)
PAGE_SIZE = 5000

_CLAU_DATE_RE = re.compile(r"\s*\[\d{4,8}\].*$")

def fetch_all_pages():
    """Fetch all rows matching the AGAUR filter via Socrata pagination."""
    all_rows = []
    offset = 0
    while True:
        params = {"$where": SOQL_WHERE, "$limit": PAGE_SIZE, "$offset": offset, "$order": "clau"}
        url = SOCRATA_RESOURCE + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "OpenAlex-AGAUR/1.0 (contact@openalex.org)"})
        rows = json.loads(urllib.request.urlopen(req, timeout=60).read())
        if not rows: break
        all_rows.extend(rows)
        print(f"  page offset={offset}, fetched {len(rows)}, cumulative {len(all_rows)}")
        if len(rows) < PAGE_SIZE: break
        offset += PAGE_SIZE
    return all_rows

def transform(raw_rows: list) -> pd.DataFrame:
    """Aggregate monthly-payment rows by clau prefix → one row per unique AGAUR grant."""
    df = pd.DataFrame(raw_rows)
    df["clau_prefix"] = df["clau"].astype("string").str.replace(_CLAU_DATE_RE, "", regex=True).str.strip()
    df["amount_row"] = pd.to_numeric(df["import_subvenci_pr_stec_ajut"], errors="coerce")
    df["year_row"] = pd.to_datetime(df["data_concessi"], errors="coerce").dt.year.astype("Int64")
    # Aggregate
    agg = df.groupby("clau_prefix", dropna=True).agg(
        title=("objecte_de_la_convocat_ria", "first"),
        title_es=("t_tol_convocat_ria_castell", "first"),
        institution_name=("ra_social_del_beneficiari", "first"),
        institution_cif=("cif_beneficiari", "first"),
        funder_scheme=("t_tol_convocat_ria_catal", "first"),
        bdns_code=("codi_bdns", "first"),
        raisc_code=("codi_raisc", "first"),
        amount=("amount_row", "sum"),
        start_year=("year_row", "min"),
        end_year=("year_row", "max"),
        beneficiary_type=("tipus_de_beneficiaris", "first"),
        n_payments=("clau", "size"),
    ).reset_index()
    agg = agg.rename(columns={"clau_prefix": "funder_award_id"})
    # 0/neg → NULL (§6.7)
    agg["amount"] = agg["amount"].where(agg["amount"] > 0, other=pd.NA)
    # Future-year cap
    yr_now = datetime.now(timezone.utc).year
    agg.loc[agg["start_year"] > yr_now + 1, "start_year"] = pd.NA
    agg.loc[agg["end_year"] > yr_now + 1, "end_year"] = pd.NA
    # Slug-collision guard
    dup = agg["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate funder_award_id values")
    agg["provenance"] = "agaur_raisc_socrata"
    agg["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # All object → pandas string
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
    ap = argparse.ArgumentParser(description="AGAUR (RAISC Socrata) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/agaur"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("AGAUR (Catalonia RAISC Socrata) -> S3")
    print("=" * 60)
    print(f"[1/2] Fetching AGAUR named-beneficiary rows from Socrata")
    raw = fetch_all_pages()
    print(f"      total rows: {len(raw):,}")
    print(f"[2/2] Aggregating by clau prefix → one row per grant")
    df = transform(raw)
    out = a.output_dir / "agaur_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} unique grants, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    amt_cov = 100 * (df["amount"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id  {len(df):,} (100%)")
    print(f"  title            {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  EUR amount       {(df['amount']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  institution_name {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  start_year       {df['start_year'].notna().sum():,} ({nn('start_year'):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  Amount median: EUR{float(df['amount'].median()):,.0f}, max: EUR{float(df['amount'].max()):,.0f}")
    if df["start_year"].notna().any():
        print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
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
    print(f"\nNext: notebooks/awards/CreateAGAURAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
