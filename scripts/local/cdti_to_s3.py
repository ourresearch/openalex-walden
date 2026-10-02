#!/usr/bin/env python3
"""
CDTI (Spain) to S3 Data Pipeline
================================

CDTI -- Centro para el Desarrollo Tecnologico y la Innovacion E.P.E. (until 2023
"...Tecnologico Industrial"; OpenAlex F4320321043) is Spain's public business
R&D+i agency. It funds companies (and some technology centres, research
associations and universities) mainly through partially reimbursable loans
("Ayudas Parcialmente Reembolsables": Proyectos CDTI de I+D, I+D en Cooperacion,
CIEN, Lineas de Innovacion, ...) and grants ("Subvencion": Neotec, Innterconecta,
Misiones, Eurostars, Cervera, PTA, ...).

Source: CDTI's own open-data API "Proyectos Aprobados" -- the dataset
"Ayudas Parcialmente Reembolsables y Subvenciones CDTI" on datos.gob.es
(publisher EA0041267), served from the download page
https://www.cdti.es/datos-abiertos-creditos-subvenciones-y-lineas as

    https://sede.cdti.gob.es/AreaPrivada/Servicios/DatosAbiertos/api/datos/proyectosidi/JSON/{year}/ALL

one JSON array per approval year (JSON / XML / XLSX offered; the API answers
HTTP 400 "El ano solicitado no es valido" for years before 2014). One record =
one beneficiary's approved aid: RazonSocial, NIFRazonSocial (company tax id),
TipoEntidad, PYME, TituloProyecto, FechaAprobacionResolucion, CCAA, Provincia,
Localidad, CodigoPostal, TipoAyuda (loan vs grant), InstrumentoFinanciero
(programme), AreaSectorial, CNAE, OrigenFondos, Presupuesto (project budget)
and AportacionCDTI (CDTI's contribution, EUR). Cooperative projects (CIEN,
Innterconecta, I+D en Cooperacion) appear once per partner, each partner being
a separate CDTI file with its own aid amount.

Spain's national subsidies database (BDNS, infosubvenciones.es) also carries
CDTI concessions, but only for the last ~4 years, without project titles, so
the CDTI file is the better source.

Scope: everything in the file is kept (company R&D and innovation funding is in
scope per the 2026-10-01 batch rule). Innovation-line loans (LIC/LICa/LIEE)
and FEMPA innovative-investment loans are innovation rather than R&D; they
are kept and flagged in the notebook header.

funder_award_id (runbook §2.1.1): papers cite CDTI file numbers
("IDI-20180224", "MIG-20221048", "CER-20191002"), but the open-data file does
not carry them. We ship a stable synthetic key
``CDTI-{NIF}-{YYYYMMDD approval}-{md5(title)[:10]}`` (suffixed with the postcode
when one company has two files for the same project on the same day). These
keys do not collapse onto citation stubs.

Output: s3://openalex-ingest/awards/cdti/cdti_projects.parquet
"""

import argparse
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

API = "https://sede.cdti.gob.es/AreaPrivada/Servicios/DatosAbiertos/api/datos/proyectosidi/JSON/{year}/ALL"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cdti/cdti_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
FIRST_YEAR_TRIED = 2000
REQUEST_DELAY = 2.0
RETRIES = 4

FIELDS = ["RazonSocial", "NIFRazonSocial", "TipoEntidad", "PYME", "TituloProyecto",
          "FechaAprobacionResolucion", "CCAA", "Provincia", "Localidad", "CodigoPostal",
          "TipoAyuda", "InstrumentoFinanciero", "AreaSectorial", "CNAE", "OrigenFondos",
          "Presupuesto", "AportacionCDTI", "UltimaFechaActualizacion"]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_year(year: int, cache_dir: Path | None) -> list[dict] | None:
    """Return the year's records, or None when the API says the year is not offered (HTTP 400)."""
    cache = cache_dir / f"cdti_{year}.json" if cache_dir else None
    if cache and cache.exists():
        return json.loads(cache.read_text())
    url = API.format(year=year)
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=300)
            log(f"  GET {url} -> HTTP {r.status_code}, {len(r.content):,} bytes")
            if r.status_code == 400 and "no es v" in r.text:
                return None
            if r.status_code != 200:
                last = f"HTTP {r.status_code}"
                time.sleep(5 * (attempt + 1))
                continue
            r.encoding = "utf-8"
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"unexpected payload type {type(data)}")
            if cache:
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps(data, ensure_ascii=False))
            time.sleep(REQUEST_DELAY)
            return data
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url} failed after {RETRIES} tries: {last}")


def eur(s: str | None) -> float | None:
    """'411.394,78 EUR-sign' -> 411394.78"""
    if not s:
        return None
    t = re.sub(r"[^\d,.-]", "", s).replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def iso(d: str | None) -> str | None:
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$", d or "")
    return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}" if m else None


def clean(s) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", type=Path, default=Path("./cdti_out"))
    ap.add_argument("--cache-dir", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=None, help="keep only the first N records (smoke test; fetches one year)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    this_year = datetime.now().year
    years = [this_year] if args.limit else list(range(FIRST_YEAR_TRIED, this_year + 1))
    records = []
    offered = []
    for y in years:
        data = fetch_year(y, args.cache_dir)
        if data is None:
            continue
        offered.append(y)
        for x in data:
            missing = [f for f in FIELDS if f not in x]
            if missing:
                raise SystemExit(f"{y}: record without fields {missing}: {x}")
            x = {f: clean(x.get(f)) for f in FIELDS}
            x["api_year"] = y
            records.append(x)
        log(f"{y}: {len(data)} records (running total {len(records)})")
    if not offered:
        raise SystemExit("API offered no years")
    log(f"years offered by the API: {offered[0]}-{offered[-1]} ({len(offered)} years)")
    if not args.limit:
        gaps = sorted(set(range(offered[0], offered[-1] + 1)) - set(offered))
        if gaps:
            raise SystemExit(f"API skipped years inside the range: {gaps}")
    if args.limit:
        records = records[: args.limit]

    df = pd.DataFrame(records)
    df["approval_date"] = df["FechaAprobacionResolucion"].map(iso)
    df["amount_cdti_eur"] = df["AportacionCDTI"].map(eur)
    df["budget_eur"] = df["Presupuesto"].map(eur)
    df["nif"] = df["NIFRazonSocial"].str.replace("-", "", regex=False).str.upper()
    norm_title = df["TituloProyecto"].fillna("").str.upper().str.replace(r"\s+", " ", regex=True).str.strip()
    df["title_hash"] = norm_title.map(lambda t: hashlib.md5(t.encode("utf-8")).hexdigest()[:10])
    base = "CDTI-" + df["nif"].fillna("NONIF") + "-" + df["approval_date"].fillna("nodate").str.replace("-", "", regex=False) + "-" + df["title_hash"]
    dup = base.duplicated(keep=False)
    df["funder_award_id"] = base.where(~dup, base + "-" + df["CodigoPostal"].fillna("x"))
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id after postcode suffix: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    log(f"{dup.sum()} rows needed the postcode suffix")

    log(f"{len(df)} awards; TipoAyuda: {df['TipoAyuda'].value_counts().to_dict()}")
    for t, g in df.groupby("TipoAyuda"):
        log(f"  {t}: {len(g)} rows, EUR {g['amount_cdti_eur'].sum():,.0f}")
    for c in ["TituloProyecto", "approval_date", "amount_cdti_eur", "RazonSocial", "InstrumentoFinanciero"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "cdti_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_cdti_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
