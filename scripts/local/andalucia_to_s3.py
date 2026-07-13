#!/usr/bin/env python3
"""
Junta de Andalucia research subsidies to S3
===========================================

Pulls the "Subvenciones otorgadas por la Junta de Andalucia" bulk open-data
file (ALL regional subsidies since ~2016, ~4.5M rows) from the Junta's open
data portal API and scopes it to research / R&D+i awards.

Data source (documented on the Junta CKAN portal,
https://www.juntadeandalucia.es/datosabiertos/portal/dataset/subvenciones-otorgadas-por-la-junta-de-andalucia):
  https://datos.juntadeandalucia.es/api/v0/subventions/all?format=csv
  (302 -> pre-generated pipe-delimited dataset-subvenciones.csv, ~1.5 GB)

INCLUSION RULE (research scoping) - a row is kept iff:
  (a) program == '54A'   (budget programme "INVESTIGACION CIENTIFICA E
      INNOVACION" - PAIDI research projects/groups, predoctoral & young-
      researcher contracts, R&D infrastructure, Severo Ochoa/Maria de Maeztu
      co-funding), OR
  (b) the announcement/finality text matches RESEARCH_RE (investigacion,
      I+D+i, cientific-, PAIDI, pre/postdoctoral, Talentia, ...) - this pulls
      in biomedical/health research under programme 41K, business R&D under
      72A/72B, and nominative research subsidies (CSIC Donana, royal
      academies) under other programmes - AND is not matched by EXCLUDE_RE
      (educational innovation 54C, employment training, commercial
      innovation).
  Rule derived by reviewing every distinct (program, announcement) pair
  matching either branch across the full corpus.

Amounts: EUR, whole units (amount column, decimal point).

Award identity: no per-grant id in the source. funder_award_id =
  "{id_system_internal}:{beneficiary}" (grant-resolution batch id + verbatim
  beneficiary string), aggregated (sum amount, min grant_date). id_seq is NOT
  used - it is a row sequence that can be renumbered when the portal
  regenerates the file.

Beneficiaries: institutions (universities, CSIC institutes, companies) and
  physical persons (predoctoral/postdoctoral fellows; physical_person = 'X',
  masked NIF). Person names come as "GIVEN SURNAME1 SURNAME2" with NO comma,
  so a reliable given/family split is impossible - lead_investigator stays
  NULL for person rows (raw name preserved in the parquet); institution
  beneficiaries ship as the affiliation.

Output: s3://openalex-ingest/awards/andalucia/andalucia_projects.parquet

Usage:
    py -3 andalucia_to_s3.py --output-dir C:/tmp/andalucia --skip-upload
    py -3 andalucia_to_s3.py --limit 500000 --skip-upload   # smoke test
"""
import argparse
import builtins
import csv
import re
import shutil
import sys
import time
import unicodedata
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- UTF-8 shim
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    _orig_open = builtins.open
    def _utf8_open(file, mode="r", buffering=-1, encoding=None, *a, **kw):
        if "b" not in str(mode) and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, *a, **kw)
    builtins.open = _utf8_open
    _orig_write_text = Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_write_text(self, data, encoding or "utf-8", errors, newline)
    Path.write_text = _wt
    _orig_read_text = Path.read_text
    def _rt(self, encoding=None, errors=None):
        return _orig_read_text(self, encoding or "utf-8", errors)
    Path.read_text = _rt

import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/andalucia/andalucia_projects.parquet"
ALL_CSV_URL = "https://datos.juntadeandalucia.es/api/v0/subventions/all?format=csv"
COUNT_URL = "https://datos.juntadeandalucia.es/api/v0/subventions/count"
UA = {"User-Agent": "OpenAlex-Andalucia/1.0 (mailto:support@openalex.org)"}

RESEARCH_PROGRAM_CD = "54A"   # INVESTIGACION CIENTIFICA E INNOVACION
RESEARCH_RE = re.compile(
    r"(investigaci"                                  # investigacion / investig.
    r"|i\+d"                                         # I+D / I+D+i / I+D+I
    r"|cientific"                                    # cientifica/cientifico
    r"|paidi"                                        # Andalusian research plan
    r"|predoctoral|postdoctoral|posdoctoral|doctoral"
    r"|talentia"                                     # talent programme
    r"|severo ochoa|s\.ochoa|maeztu)"                # excellence co-funding
)
EXCLUDE_RE = re.compile(
    r"(innovacion educativa|innovacion y evaluacion educativa"
    r"|formacion para el empleo|innovacion comercial)"
)


def deacc(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()


def is_research(row: dict) -> bool:
    if (row.get("program") or "").strip() == RESEARCH_PROGRAM_CD:
        return True
    blob = deacc((row.get("announcement") or "") + " " + (row.get("finality") or "")
                 + " " + (row.get("regulatory_base") or ""))
    if EXCLUDE_RE.search(blob):
        return False
    return bool(RESEARCH_RE.search(blob))


# ------------------------------------------------------------------ download
def download(cache: Path) -> Path:
    local = cache / "dataset-subvenciones.csv"
    if local.exists() and local.stat().st_size > 1e8:
        print(f"  cache hit: {local} ({local.stat().st_size/1e9:.2f} GB)")
        return local
    cache.mkdir(parents=True, exist_ok=True)
    part = local.with_suffix(".part")
    print(f"  downloading {ALL_CSV_URL} (~1.5 GB, streaming)")
    req = urllib.request.Request(ALL_CSV_URL, headers=UA)
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=600) as resp, open(part, "wb") as out:
        done = 0
        while True:
            chunk = resp.read(1 << 22)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            if done % (1 << 28) < (1 << 22):
                print(f"    {done/1e9:.2f} GB ({time.time()-t0:.0f}s)")
    part.rename(local)
    print(f"  downloaded {local.stat().st_size/1e9:.2f} GB in {time.time()-t0:.0f}s")
    return local


# ----------------------------------------------------------------- transform
KEEP = ["amount", "announcement", "award_year", "beneficiary",
        "budget_application", "european_funds", "finality", "grant_date",
        "id_organism", "id_seq", "id_system_internal", "name_program",
        "nif_cif_public", "organism", "physical_person", "program",
        "regulatory_base", "type"]


def parse_filter(path: Path, limit: int = 0) -> pd.DataFrame:
    csv.field_size_limit(10 ** 7)
    rows, n = [], 0
    t0 = time.time()
    with open(path, encoding="utf-8", newline="", errors="replace") as f:
        rdr = csv.DictReader(f, delimiter="|")
        for row in rdr:
            n += 1
            if n % 1_000_000 == 0:
                print(f"    scanned {n/1e6:.0f}M rows, kept {len(rows):,} "
                      f"({time.time()-t0:.0f}s)")
            if limit and n >= limit:
                print(f"    --limit {limit} reached")
                break
            if is_research(row):
                rows.append({k: (row.get(k) or "").strip() or None for k in KEEP})
    print(f"    total scanned {n:,}, kept {len(rows):,} research rows")
    return pd.DataFrame(rows)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    df["grant_dt"] = pd.to_datetime(df["grant_date"], format="%Y-%m-%d", errors="coerce")
    df["award_key"] = (df["id_system_internal"].fillna("NOBATCH") + ":" +
                       df["beneficiary"].fillna("NOBENEF"))
    df["is_person"] = df["physical_person"].fillna("") == "X"

    agg = df.groupby("award_key", dropna=False).agg(
        id_system_internal=("id_system_internal", "first"),
        announcement=("announcement", "first"),
        regulatory_base=("regulatory_base", "first"),
        finality=("finality", "first"),
        program=("program", "first"),
        name_program=("name_program", "first"),
        organism=("organism", "first"),
        id_organism=("id_organism", "first"),
        beneficiary=("beneficiary", "first"),
        is_person=("is_person", "first"),
        nif_cif_public=("nif_cif_public", "first"),
        european_funds=("european_funds", "first"),
        grant_type=("type", "first"),
        amount=("amount_num", "sum"),
        first_date=("grant_dt", "min"),
        award_year=("award_year", "min"),
        n_rows=("award_key", "size"),
    ).reset_index().rename(columns={"award_key": "funder_award_id"})

    agg["amount"] = agg["amount"].where(agg["amount"] > 0, other=pd.NA)
    agg["institution_name"] = agg["beneficiary"].where(~agg["is_person"])
    agg["person_name_raw"] = agg["beneficiary"].where(agg["is_person"])
    agg["grant_date"] = agg["first_date"].dt.strftime("%Y-%m-%d")
    agg = agg.drop(columns=["first_date"])
    agg["provenance"] = "andalucia"
    agg["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if agg["funder_award_id"].duplicated().any():
        raise RuntimeError("funder_award_id collision after aggregation")

    for c in agg.columns:
        if c != "amount":
            agg[c] = agg[c].astype("string")
    return agg


def check_no_shrink(df: pd.DataFrame, allow_shrink: bool):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < "
                             f"existing {len(prev):,}. Use --allow-shrink to override.")
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")


def main():
    ap = argparse.ArgumentParser(description="Junta de Andalucia research subsidies -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("andalucia_out"))
    ap.add_argument("--cache-dir", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after scanning N source rows (smoke test)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    cache = a.cache_dir or (a.output_dir / "cache")
    a.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 64)
    print("Junta de Andalucia research subsidies -> S3")
    print("=" * 64)
    print("[1/3] Downloading full subsidies CSV (checkpointed)")
    path = download(cache)
    print("[2/3] Scanning + research filter")
    rows = parse_filter(path, a.limit)
    if rows.empty:
        raise RuntimeError("0 research rows - inclusion rule or source broke")
    print("[3/3] Aggregating to one row per (resolution batch, beneficiary)")
    df = transform(rows)

    out = a.output_dir / "andalucia_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out}: {len(df):,} awards, {out.stat().st_size/1e6:.1f} MB")

    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    print("\nCoverage:")
    print(f"  funder_award_id    100%")
    print(f"  title(announcement) {nn('announcement'):.1f}%")
    print(f"  amount (EUR)       {nn('amount'):.1f}%")
    print(f"  institution_name   {nn('institution_name'):.1f}%  (org beneficiaries)")
    print(f"  person beneficiary {nn('person_name_raw'):.1f}%")
    print(f"  grant_date         {nn('grant_date'):.1f}%")
    amt = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  EUR total {amt.sum():,.0f}, median {amt.median():,.0f}, max {amt.max():,.0f}")
    print(f"  program split: {df['program'].value_counts().head(8).to_dict()}")
    print(f"  year range: {df['award_year'].min()}-{df['award_year'].max()}")

    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess
        aws = shutil.which("aws")
        if not aws:
            raise RuntimeError("aws CLI not found; rerun with --skip-upload and upload manually")
        subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=True)
        print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
    print("\nNext: notebooks/awards/CreateAndaluciaAwards.ipynb")


if __name__ == "__main__":
    main()
