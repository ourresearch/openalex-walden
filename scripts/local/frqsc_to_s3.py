#!/usr/bin/env python3
"""
FRQSC (Fonds de recherche du Québec - Société et culture) to S3 Data Pipeline
==================================================================================

Downloads the official FRQSC funded-projects CSVs from donneesquebec.ca CKAN and
writes a parquet for Databricks.

Data Source:
  CKAN: https://www.donneesquebec.ca/recherche/api/3/action/package_show?id=liste-du-financement-accorde-par-le-fonds-de-recherche-du-quebec-societe-et-culture
  Each fiscal year is its own CSV resource (2019-2020 through 2023-2024+). The
  same Dossier (project ID) typically appears in multiple years if funding
  continues across years; we aggregate by Dossier, summing Montant_total across
  fiscal years, keeping the earliest Debut_Financement as start_year.

Output: s3://openalex-ingest/awards/frqsc/frqsc_projects.parquet

Schema notes:
  - funder_award_id = Dossier (the FRQSC internal project ID, e.g. 196577 — this
    is the integer cited in OpenAlex grants links to FRQSC and matches the
    'Numéro de dossier' shown on FRQSC award letters).
  - Amount: source format "600 000" (space-separated thousands) in CAD. Strip
    whitespace, parse to float, sum across fiscal years when same Dossier.
  - PI name format: "Lastname, Firstname" — split on first comma.
  - institution: `etablissement` (e.g. "Université Concordia"). Quebec
    province; 99%+ Canadian.
  - funder_scheme: `Programme` (e.g. "Regroupements stratégiques",
    "Grands défis de société").
  - description: keywords (`Mots_cles`) — joined with semicolons. No prose
    abstract in source.

Usage:
    python frqsc_to_s3.py --skip-upload
"""
import argparse, csv, json, re, sys
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import urllib.request
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/frqsc/frqsc_projects.parquet"
DATASET_SLUG = "liste-du-financement-accorde-par-le-fonds-de-recherche-du-quebec-societe-et-culture"
CKAN_API     = f"https://www.donneesquebec.ca/recherche/api/3/action/package_show?id={DATASET_SLUG}"
FUND_LABEL   = "FRQSC"   # the value in the Fonds column we expect

_WS = re.compile(r"\s+")

def fetch_resources():
    """Return [(year_label, url)] for each CSV resource with non-zero size."""
    req = urllib.request.Request(CKAN_API, headers={"User-Agent": "OpenAlex-FRQSC/1.0 (contact@openalex.org)"})
    d = json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
    out = []
    for r in d.get("resources", []):
        if r.get("format") == "CSV" and (r.get("size") or 0) > 0:
            yr = re.search(r"(\d{4}-\d{4})", r.get("name", "")) or re.search(r"(\d{4}-\d{4})", r.get("url", ""))
            out.append((yr.group(1) if yr else r.get("name", "?"), r["url"]))
    out.sort()
    return out

def parse_amount(s):
    if s is None or pd.isna(s): return None
    s = _WS.sub("", str(s)).replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None

def split_pi(name):
    """`Lastname, Firstname` → (Firstname, Lastname). S.O. (no PI) → (None, None)."""
    if name is None or pd.isna(name) or (str(name).strip() in ("S.O.", "S/O", "N.D.", "")):
        return (None, None)
    parts = name.split(",", 1)
    if len(parts) == 2:
        return (parts[1].strip() or None, parts[0].strip() or None)
    return (None, name.strip() or None)

def parse_start_year(s):
    """`2017-2018` → 2017."""
    if s is None or pd.isna(s): return None
    m = re.match(r"(\d{4})", str(s))
    return int(m.group(1)) if m else None

def download_all(output_dir: Path):
    resources = fetch_resources()
    print(f"[1/2] Downloading {len(resources)} FRQSC CSVs from CKAN")
    dfs = []
    for year, url in resources:
        print(f"      {year}: {url}")
        p = output_dir / f"frqsc_{year}.csv"
        urllib.request.urlretrieve(url, p)
        df = pd.read_csv(p, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        df["_year_file"] = year
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.replace("", pd.NA)
    print(f"      total source rows (one row per fiscal year per Dossier): {len(df):,}")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[2/2] Aggregating by Dossier → one row per grant")
    # Filter to FRQSC-funded rows only (the Fonds column should always be FRQSC for this dataset,
    # but cofunded rows sometimes list co-funders; we keep all rows that have Fonds==FRQSC or empty).
    df = df[df["Fonds"].fillna(FUND_LABEL).str.strip().str.upper() == FUND_LABEL].copy()
    df["amount_research"]    = df["Montant_recherche"].map(parse_amount)
    df["amount_indirect"]    = df["Frais_indirect"].map(parse_amount)
    df["amount_total_row"]   = df["Montant_total"].map(parse_amount)
    df["start_year_row"]     = df["Debut_Financement"].map(parse_start_year)
    pi = df["Titulaire"].map(split_pi)
    df["pi_given_name"]      = pi.map(lambda t: t[0])
    df["pi_family_name"]     = pi.map(lambda t: t[1])
    # Aggregate by Dossier (project id) — sum amounts across fiscal years
    # Take earliest start_year, latest fiscal year, first non-null PI/institution/programme
    agg = df.groupby("Dossier", dropna=True).agg(
        title=("Programme", "first"),                    # programme name as fallback title (source has no project title)
        funder_scheme=("Programme", "first"),
        programme_volet=("Programme_-_volet", "first"),
        amount=("amount_total_row", "sum"),
        amount_indirect=("amount_indirect", "sum"),
        amount_research=("amount_research", "sum"),
        institution_name=("etablissement", "first"),
        institution_country=("Pays_etablissement", "first"),
        institution_province=("Province_etablissement", "first"),
        category=("Categorie_de_financement", "first"),
        recipient_type=("Type_de_Recipiendaire", "first"),
        research_domain=("Domaines_de_recherche", "first"),
        keywords=("Mots_cles", "first"),
        pi_given_name=("pi_given_name", "first"),
        pi_family_name=("pi_family_name", "first"),
        start_year=("start_year_row", "min"),
        last_fiscal_year=("_year_file", "max"),
        cofinancement=("Cofinancement", "first"),
        n_rows=("Dossier", "size"),
    ).reset_index()
    agg["funder_award_id"] = agg["Dossier"].astype(str).str.strip()
    # 0/neg → NULL (§6.7)
    for c in ("amount", "amount_research", "amount_indirect"):
        agg[c] = agg[c].where(agg[c] > 0, other=pd.NA)
    # Description = keywords joined (no abstract in source)
    agg["description"] = agg["keywords"]
    # Slug-collision guard
    dup = agg["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate Dossier values — investigate")
    # future-year cap
    yr_now = datetime.now(timezone.utc).year
    agg.loc[agg["start_year"] > yr_now + 1, "start_year"] = pd.NA
    out = agg[[
        "funder_award_id", "title", "description",
        "amount", "amount_research", "amount_indirect",
        "pi_given_name", "pi_family_name",
        "institution_name", "institution_country", "institution_province",
        "funder_scheme", "programme_volet", "category", "recipient_type",
        "research_domain", "keywords",
        "start_year", "last_fiscal_year", "cofinancement", "n_rows",
    ]].copy()
    out["provenance"]    = "frqsc_data_quebec"
    out["ingested_at"]   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].astype("string")
    return out

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(
                f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}. Pass --allow-shrink only if real.")
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="FRQSC (donneesquebec.ca CKAN) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/frqsc"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("FRQSC (Fonds de recherche du Québec - Société et culture) -> S3")
    print("=" * 60)
    src = download_all(a.output_dir)
    df = transform(src)
    out = a.output_dir / "frqsc_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} unique grants, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    amt_cov = 100 * (df["amount"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id (Dossier)  {len(df):,} (100%)")
    print(f"  title (Programme)          {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  description (keywords)     {df['description'].notna().sum():,} ({nn('description'):.1f}%)")
    print(f"  CAD amount                 {(df['amount']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  PI family_name             {df['pi_family_name'].notna().sum():,} ({nn('pi_family_name'):.1f}%)")
    print(f"  institution                {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  start_year                 {df['start_year'].notna().sum():,} ({nn('start_year'):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  amount median: CAD {float(df['amount'].median()):,.0f}, mean: CAD {float(df['amount'].mean()):,.0f}")
    print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    print(f"  Top programmes: {df['funder_scheme'].value_counts().head(5).to_dict()}")
    print(f"  Top institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws:
            subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else:
            print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateFRQSCAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
