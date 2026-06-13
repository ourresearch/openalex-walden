#!/usr/bin/env python3
"""
Ministère des Affaires sociales et de la Santé (France) to S3 Data Pipeline
===========================================================================

Downloads the French health ministry's applied-health-research grant dataset
(SIRANO: PHRC / PHRIP / PRME / PREPS / PRT calls) from data.gouv.fr and uploads
a parquet to S3 for Databricks ingestion.

Data source: data.gouv.fr dataset 646cbff5541af15c423811cf, ~5 dated CSV
snapshots (semicolon-delimited, cp1252/latin-1). Columns: appel_a_projets
(scheme), annee_de_selection (year), region, nom_etablissement (institution),
type_etablissement, acronyme, titre (title), discipline_principale, nom_porteur
+ prenom_porteur (PI family/given — pre-split!), financement_total (EUR amount),
numero_registre_essais (trial registry id), numero_tranche. Snapshots are merged
and de-duplicated on (acronyme, titre, nom_porteur, annee). These are the
ministry's OWN DGOS calls — not ANR/INSERM. French. provenance `min_sante_fr`,
priority 333. F4320322734 (Path A).

Output: s3://openalex-ingest/awards/min_sante_fr/min_sante_fr_projects.parquet

Usage:
    python fr_sante_to_s3.py
    python fr_sante_to_s3.py --limit 20
    python fr_sante_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import io
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

DATASET = "https://www.data.gouv.fr/api/1/datasets/646cbff5541af15c423811cf/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/min_sante_fr/min_sante_fr_projects.parquet"
EXPECTED_MIN = 800
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    if s.lower() in ("", "nan", "none", "nc", "n/a"):
        return None
    return s


def read_csv(raw):
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=";", dtype=str)
            if df.shape[1] >= 10:
                return df
        except Exception:
            continue
    return None


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="French health ministry grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/min_sante_fr_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Ministère des Affaires sociales et de la Santé (FR) -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    d = s.get(DATASET, timeout=60).json()
    csvs = [r for r in d["resources"] if r.get("format") == "csv"]
    print(f"CSV snapshots: {len(csvs)}")

    frames = []
    for r in csvs:
        raw = s.get(r["url"], timeout=120).content
        df = read_csv(raw)
        if df is not None:
            frames.append(df)
            print(f"  {r.get('title')}: {len(df)} rows")
    if not frames:
        print("[ERROR] no CSV parsed")
        sys.exit(1)
    alldf = pd.concat(frames, ignore_index=True)

    rows = []
    for _, r in alldf.iterrows():
        titre = clean(r.get("titre"))
        if not titre:
            continue
        acr = clean(r.get("acronyme"))
        year = clean(r.get("annee_de_selection"))
        fam = clean(r.get("nom_porteur"))
        given = clean(r.get("prenom_porteur"))
        amt = clean(r.get("financement_total"))
        amount = None
        if amt:
            m = re.search(r"[\d  .,]+", amt)
            if m:
                num = m.group(0).replace(" ", "").replace("\xa0", "").replace(".", "").replace(",", ".")
                try:
                    amount = float(num)
                except ValueError:
                    amount = None
        key = re.sub(r"[^a-z0-9]+", "-", f"{acr or titre[:40]}-{fam or ''}-{year or ''}".lower()).strip("-")[:120]
        rows.append({
            "funder_award_id": f"minsante-{key}",
            "title": titre,
            "acronym": acr,
            "pi_given": given,
            "pi_family": fam,
            "institution": clean(r.get("nom_etablissement")),
            "region": clean(r.get("region")),
            "amount": amount,
            "start_year": year,
            "funder_scheme": clean(r.get("appel_a_projets")),
            "discipline": clean(r.get("discipline_principale")),
            "trial_registry": clean(r.get("numero_registre_essais")),
        })

    df = pd.DataFrame(rows).drop_duplicates(subset="funder_award_id").reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c not in ("amount", "start_year"):
            df[c] = df[c].astype("string")

    if not args.limit and len(df) < EXPECTED_MIN:
        print(f"[ERROR] only {len(df)} projects — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year", "funder_scheme"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    if df.amount.notna().any():
        print(f"  amount EUR: median={df.amount.median():.0f} max={df.amount.max():.0f}")
    if df.start_year.notna().any():
        print(f"  years: {int(df.start_year.min())}-{int(df.start_year.max())}")
    print("  schemes:", dict(df.funder_scheme.value_counts().head(6)))
    out = args.output_dir / "min_sante_fr_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
