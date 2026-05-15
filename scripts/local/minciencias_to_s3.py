#!/usr/bin/env python3
"""
MinCiencias (Colombia) to S3 (GRANT PATTERN)
=============================================

Pulls Colombian research and innovation projects from datos.gov.co
(the Colombian national open-data portal), Socrata dataset 6hgx-q9pi:
"Proyectos de Investigación e Innovación evaluados y aprobados desde
el año 2009".

Source: https://www.datos.gov.co/resource/6hgx-q9pi.json (Socrata API)
Output: s3://openalex-ingest/awards/minciencias/minciencias_projects.parquet

Awarding bodies in OpenAlex (year-bounded, applied in the notebook):
- 2019-onwards: Ministerio de Ciencia, Tecnología e Innovación (F3277441329)
- pre-2019:    COLCIENCIAS / Departamento Administrativo de Ciencia,
               Tecnología e Innovación (F4320309955) — the predecessor

About 3,155 projects, year range 2007-2021 (this dataset isn't updated
to current; the most recent dataset refresh is per `rowsUpdatedAt`).

Data shape notes (informs the notebook transform):
- 90% have a non-zero amount (`monto_total_ap` / `monto_financiado_ap`)
- 99.5% have `entidad_ejecuta` (executing institution)
- Currency is **implicit COP** (Colombian Peso) — hardcoded in the
  notebook header per the runbook's implicit-currency rule.
- **No PI fields** in the source — `lead_investigator.given_name` /
  `family_name` / `orcid` are NULL by source-authority. We populate
  `lead_investigator.affiliation.name` from `entidad_ejecuta` and
  `affiliation.country = 'CO'`.

Source-authority rule respected: the funder is MinCiencias itself, the
source is the Colombian government's open-data portal hosting MinCiencias
data — single provenance.
"""

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

DATASET_ID = "6hgx-q9pi"
RESOURCE_URL = f"https://www.datos.gov.co/resource/{DATASET_ID}.json"
LANDING_PAGE_BASE = f"https://www.datos.gov.co/d/{DATASET_ID}"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/minciencias/minciencias_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests"
}

# Socrata single-request cap is 50,000; the dataset has ~3,155 rows.
SOCRATA_LIMIT = 50_000

# String-typed columns to coerce post-DataFrame so pandas/pyarrow don't
# infer numeric on null-heavy text columns (Rockefeller / IDRC bug,
# walden 5f694b7 / 0f8b891).
STRING_COLS = [
    "proyecto_id", "convocatoria_id", "desc_convocatoria",
    "codigo_proyecto", "titulo_proyecto",
    "fecha_registro", "fecha_aprobacion",
    "entidad_ejecuta",
    "tipo_financiacion", "desc_financiacion",
    "cod_prog_cti", "nme_prog_cti",
    "cod_area_tematica", "area_tematica",
    "cod_area_ciencia", "nme_area_ciencia",
    "estado_proyecto",
    "cod_dane_ciudad_entidad", "nme_ciudad_entidad",
    "cod_dane_depto_entidad", "nme_depto_entidad",
    "cod_dane_ciudad_pry", "nme_ciudad_pry",
    "cod_dane_depto_pry", "nme_depto_pry",
    "tpo_proyecto", "nme_tipo_proyecto",
    "slug", "source_url", "downloaded_at",
]

# Numeric (DOUBLE) columns — monetary amounts in COP, no minor unit.
DOUBLE_COLS = ["monto_financiado_ap", "monto_contrapartida_ap", "monto_total_ap"]

# Integer year
INT_COLS = ["ano_convocatoria"]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def slugify(proyecto_id: str | None) -> str | None:
    """Slug is just 'minciencias-{proyecto_id}'. proyecto_id is the dataset's
    primary key in MinCiencias's SIGP system, so unique by definition. If
    we still hit a collision, the slug-collision raise in main() catches it.
    """
    if not proyecto_id:
        return None
    pid = re.sub(r"[^a-z0-9]+", "-", str(proyecto_id).lower()).strip("-")
    return f"minciencias-{pid}" if pid else None


def main() -> None:
    p = argparse.ArgumentParser(description="MinCiencias (Colombia) -> parquet -> S3")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true",
                   help="Write parquet locally only; skip S3 upload")
    p.add_argument("--limit", type=int, default=None,
                   help="Truncate to first N rows (smoke testing)")
    args = p.parse_args()

    log("=" * 60)
    log("MinCiencias (Colombia) -> S3 starting")
    log(f"Source: {RESOURCE_URL}")

    socrata_limit = min(args.limit, SOCRATA_LIMIT) if args.limit else SOCRATA_LIMIT
    log(f"Fetching with $limit={socrata_limit:,}...")
    r = requests.get(
        RESOURCE_URL,
        params={"$limit": socrata_limit, "$order": "proyecto_id"},
        headers=HEADERS, timeout=60,
    )
    r.raise_for_status()
    rows = r.json()
    log(f"Fetched {len(rows):,} rows from Socrata")

    if args.limit:
        rows = rows[: args.limit]
        log(f"Limited to first {len(rows)} rows for smoke test")

    # Augment with synthetic columns
    download_ts = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["slug"] = slugify(row.get("proyecto_id"))
        row["source_url"] = LANDING_PAGE_BASE
        row["downloaded_at"] = download_ts

    df = pd.DataFrame(rows)
    log(f"DataFrame shape (pre-dedup): {df.shape}")

    # Deduplicate (proyecto_id, convocatoria_id) stub-vs-real pairs. The
    # source CRM emits two rows for some projects: a "registered" stub with
    # zero amounts and a "funded approved" row with the real amount. Keep
    # the row with the highest monto_total_ap per (proyecto_id, convocatoria_id);
    # ties resolved by row order (Socrata $order=proyecto_id is stable).
    if "proyecto_id" in df and "convocatoria_id" in df and "monto_total_ap" in df:
        pre = len(df)
        # Coerce the amount to numeric for the comparison without mutating yet
        amount_for_sort = pd.to_numeric(df["monto_total_ap"], errors="coerce").fillna(-1)
        df = (
            df.assign(_amt_rank=amount_for_sort)
              .sort_values("_amt_rank", ascending=False, kind="stable")
              .drop_duplicates(subset=["proyecto_id", "convocatoria_id"], keep="first")
              .drop(columns="_amt_rank")
              .reset_index(drop=True)
        )
        log(f"Dedup by (proyecto_id, convocatoria_id) keep-max-amount: {pre} -> {len(df)} ({pre - len(df)} stub rows dropped)")

    # Defensive typing
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")
    for col in DOUBLE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if not df.empty:
        nrows = len(df)
        log(
            f"Coverage: title={df.titulo_proyecto.notna().sum()}, "
            f"year={df.ano_convocatoria.notna().sum()}, "
            f"institution={df.entidad_ejecuta.notna().sum() if 'entidad_ejecuta' in df else 0}, "
            f"approval_date={df.fecha_aprobacion.notna().sum()}, "
            f"slug={df.slug.notna().sum()}"
        )
        # Amount coverage (>0 — Socrata stores zeros for unknown)
        if "monto_total_ap" in df:
            nz = (df.monto_total_ap > 0).sum()
            log(f"  monto_total_ap > 0: {nz}/{nrows} ({nz/nrows*100:.0f}%)")
        if "monto_financiado_ap" in df:
            nz = (df.monto_financiado_ap > 0).sum()
            log(f"  monto_financiado_ap > 0: {nz}/{nrows} ({nz/nrows*100:.0f}%)")
        if "ano_convocatoria" in df:
            log(f"  Year range: {df.ano_convocatoria.min()} - {df.ano_convocatoria.max()}")
            pre = (df.ano_convocatoria < 2019).sum()
            post = (df.ano_convocatoria >= 2019).sum()
            log(f"  pre-2019 (Colciencias era): {pre}; 2019+ (MinCiencias era): {post}")

    # Slug-collision detection — MUST raise per the runbook (post-PR-80 rule)
    if df.slug.notna().any():
        dup_mask = df.slug.duplicated(keep=False) & df.slug.notna()
        if dup_mask.any():
            log("FATAL: duplicate slugs detected:")
            log(str(df.loc[dup_mask, ["proyecto_id", "titulo_proyecto", "slug"]]))
            raise RuntimeError(
                f"{int(dup_mask.sum())} rows have duplicate funder_award_id slugs — "
                "fix the slug rule (add tiebreaker) before shipping."
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "minciencias_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path} ({parquet_path.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
