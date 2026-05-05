#!/usr/bin/env python3
"""
Argentina MINCYT to S3 Data Pipeline (multi-funder ingest)
==========================================================

Argentina's Ministerio de Ciencia, Tecnología e Innovación (MINCYT)
publishes a "Proyectos de ciencia, tecnología e innovación" CKAN
dataset on datos.gob.ar covering 2008–2019. Each project row carries
a `proyecto_fuente` field identifying which Argentine agency funded
it. A single ingest therefore covers three OpenAlex funders:

  - ANPCYT (Agencia Nacional de Promoción Científica y Tecnológica,
    a.k.a. "Agencia I+D+i")  -> F4320334832  (~13,065 projects)
  - CONICET (Consejo Nacional de Investigaciones Científicas y Técnicas)
                              -> F4320321594  (~5,528 projects)
  - INTA  (Instituto Nacional de Tecnología Agropecuaria)
                              -> F4320326565  (~673 projects)

Other minor `proyecto_fuente` values exist; the notebook maps known
ones and drops the rest.

Discovery: dataset is `mincyt_79e643e7-c75f-4f2b-9e8a-66ca8050a262`
on the datos.gob.ar CKAN. Resource URLs are read dynamically from the
package, so new annual files (2020+) auto-pickup if MINCYT publishes
them.

Output: s3://openalex-ingest/awards/argentina_mincyt/argentina_mincyt_projects.parquet
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

CKAN_PACKAGE_URL = (
    "https://datos.gob.ar/api/3/action/package_show"
    "?id=mincyt_79e643e7-c75f-4f2b-9e8a-66ca8050a262"
)
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/argentina_mincyt/argentina_mincyt_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (+https://openalex.org)",
}
REQUEST_DELAY = 0.3
RETRIES = 3
TIMEOUT = 60


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_with_retries(url: str) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed: {url} ({last_err})")


def discover_resources() -> tuple[list[dict], dict[str, dict]]:
    """Return (yearly_projects_resources, ref_tables_by_name).

    Yearly project resources are JSON files named "proyectos_YYYY".
    Ref tables (currency, project type, status, function, discipline) are
    JSON files starting with "ref_".
    """
    log(f"Fetching CKAN package metadata: {CKAN_PACKAGE_URL}")
    pkg = fetch_with_retries(CKAN_PACKAGE_URL).json()["result"]

    yearly = []
    refs: dict[str, dict] = {}
    for res in pkg.get("resources", []):
        name = res.get("name", "")
        fmt = (res.get("format") or "").upper()
        if fmt != "JSON":
            continue
        if name.startswith("proyectos_"):
            yearly.append(res)
        elif name.startswith("ref_"):
            refs[name] = res

    yearly.sort(key=lambda r: r.get("name", ""))
    log(f"  found {len(yearly)} yearly project resources, {len(refs)} reference tables")
    return yearly, refs


def fetch_json_resource(res: dict) -> list[dict]:
    """Fetch a JSON resource and return its `data` list (CKAN wrapper)."""
    r = fetch_with_retries(res["url"])
    payload = r.json()
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload if isinstance(payload, list) else []


def fetch_all_projects(yearly: list[dict], limit_files: int | None = None) -> list[dict]:
    out: list[dict] = []
    for i, res in enumerate(yearly[: limit_files or len(yearly)], 1):
        rows = fetch_json_resource(res)
        log(f"  [{i}/{len(yearly) if not limit_files else min(limit_files, len(yearly))}] "
            f"{res.get('name')}: {len(rows):,} rows (running total {len(out) + len(rows):,})")
        # Annotate each row with the source year derived from the resource name
        try:
            year_str = res["name"].split("_", 1)[1]
            year_int = int(year_str)
        except Exception:
            year_int = None
        for row in rows:
            row["_source_year"] = year_int
            row["_source_resource_id"] = res.get("id")
        out.extend(rows)
        time.sleep(REQUEST_DELAY)
    return out


def normalise_record(row: dict, ref_tables: dict[str, list[dict]]) -> dict:
    """Flatten one MINCYT project row, preserving source field names.

    Keep raw fields and add small derived helpers (parsed dates, dollar
    amount where currency is ARS or USD only). Currency lookup uses
    ref_MONEDA if currency_id is present; otherwise default to ARS.
    """
    return {
        # Source identifiers
        "proyecto_id": row.get("proyecto_id"),
        "codigo_identificacion": row.get("codigo_identificacion"),
        # Source attribution
        "proyecto_fuente": row.get("proyecto_fuente"),
        # Title / description / metadata
        "titulo": row.get("titulo"),
        "resumen": row.get("resumen"),
        "palabras_clave": row.get("palabras_clave"),
        # Source dates (string from MINCYT)
        "fecha_inicio": row.get("fecha_inicio"),
        "fecha_finalizacion": row.get("fecha_finalizacion"),
        # Source amount fields — keep all three; notebook chooses one
        "monto_total_solicitado": row.get("monto_total_solicitado"),
        "monto_financiado_adjudicado": row.get("monto_financiado_adjudicado"),
        "monto_total_adjudicado": row.get("monto_total_adjudicado"),
        # PI / participants (gendered counts)
        "sexo_director": row.get("sexo_director"),
        "cantidad_miembros_F": row.get("cantidad_miembros_F"),
        "cantidad_miembros_M": row.get("cantidad_miembros_M"),
        # Status code (looks up in ref_ESTADO_PROYECTO)
        "estado_id": row.get("estado_id"),
        # Currency reference (will be NULL for some; default ARS in notebook)
        "moneda_id": row.get("moneda_id"),
        # Project type (ref_TIPO_PROYECTO)
        "tipo_proyecto_id": row.get("tipo_proyecto_id"),
        # Function (ref_FUNCION)
        "funcion_id": row.get("funcion_id"),
        "_source_year": row.get("_source_year"),
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Argentina MINCYT -> parquet -> S3")
    p.add_argument("--limit-files", type=int, default=None,
                   help="For smoke-test: only process first N yearly files")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Argentina MINCYT -> S3 pipeline starting")

    # Suppress urllib3 SSL warnings for datos.gob.ar
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    yearly, refs = discover_resources()

    log(f"Phase 1: download {len(yearly)} yearly project files")
    rows = fetch_all_projects(yearly, limit_files=args.limit_files)
    log(f"Total raw projects: {len(rows):,}")

    log("Phase 2: normalise records")
    # Pre-fetch ref tables (small) — pass empty dict for now since the
    # notebook joins these via separate Spark tables if needed.
    ref_tables: dict[str, list[dict]] = {}
    out = [normalise_record(r, ref_tables) for r in rows]

    df = pd.DataFrame(out)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage:")
    log(f"  titulo: {df.titulo.notna().sum():,}")
    log(f"  monto_total_adjudicado: {df.monto_total_adjudicado.notna().sum():,}")
    log(f"  fecha_inicio: {df.fecha_inicio.notna().sum():,}")
    log(f"proyecto_fuente distribution:")
    for f, c in df.proyecto_fuente.value_counts().head(10).items():
        log(f"  {c:>6}: {f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "argentina_mincyt_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

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
