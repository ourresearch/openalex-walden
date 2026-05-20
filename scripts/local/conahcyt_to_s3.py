#!/usr/bin/env python3
"""
CONAHCYT/SECIHTI SNII to S3 Data Pipeline (CKAN-discovery + bulk CSV)
======================================================================

Downloads CONAHCYT's Sistema Nacional de Investigadores (SNII)
researcher appointment records (program code S191) from Mexico's
national open-data portal. 13 annual CSVs covering 2012-2024.

Funder name history: CONACYT (since 1970) → CONAHCYT (renamed 2023
under AMLO) → SECIHTI (renamed 2024 under Sheinbaum). All three names
refer to the same entity. OpenAlex has only the original CONACYT-era
name on file (F4320321739, "Consejo Nacional de Ciencia y Tecnología"),
so this script uses that funder_id throughout regardless of which
publishing-era host the CSV came from. The current host
`repodatos.atdt.gob.mx/api_update/secihti/...` reflects the latest
SECIHTI rebrand.

Source authority
----------------
Mexico's national open-data portal (datos.gob.mx) — CKAN open-data
API, method #1 on the runbook ladder. NOT an aggregator: CONAHCYT
publishes here directly through Mexico's Agencia de Transformación
Digital y Telecomunicaciones (ATDT).

  CKAN discovery: https://www.datos.gob.mx/api/3/action/package_show?id=programas_presupuestarios_conahcyt
  Bulk CSV host:  https://repodatos.atdt.gob.mx/api_update/secihti/programa_presupuestario_s191/base_snii-s191_{year}.csv

Schema (16 cols per the 2024 CSV header)
-----------------------------------------
  anio                              year
  cvu                               CVU researcher ID — UNIQUE per person across years
  nombre                            given names
  primer_apellido                   paternal surname (the family name for Spanish convention)
  segundo_apellido                  maternal surname (kept as a separate field, not used for family name)
  grado_estudios                    degree (Doctorado, etc.)
  nivel_distincion                  SNII level — drives funder_scheme
  inst_adscrip                      home institution
  entidad_federativa_inst_adscrip   Mexican state of institution
  area_conocimiento                 knowledge area
  fecha_inicio_vig                  appointment start date
  fecha_termino_vig                 appointment end date (mixed date formats — see _parse_date)
  tipo_apoyo                        type of support
  monto_anual                       ANNUAL stipend amount (MXN)
  monto_anual_adicional             additional annual amount (MXN, often blank)
  otros_apoyos                      other supports text field

Dedup rule
----------
Annual SNII CSVs publish ONE row per researcher per year of validity.
A single SNII appointment is a 3-year cycle, so the same researcher
appears in multiple annual files for the same appointment. Dedup key
= (cvu, nivel_distincion, fecha_inicio_vig). Keep the latest annual
record (max(anio)) per key — this preserves the most recent reported
monto_anual.

This follows the Kyle b121826 MinCiencias precedent: dedup by the
column we ship as amount. We collapse the annual disbursement view
into one row per appointment cycle.

Output
------
s3://openalex-ingest/awards/conahcyt/conahcyt_snii.parquet

Usage
-----
    python conahcyt_to_s3.py                  # full run (~10MB × 13 CSVs)
    python conahcyt_to_s3.py --skip-upload    # local dev
    python conahcyt_to_s3.py --skip-download  # reuse cached CSVs
    python conahcyt_to_s3.py --years 2024,2023  # subset for smoke
    python conahcyt_to_s3.py --allow-shrink   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

CKAN_PACKAGE_URL = "https://www.datos.gob.mx/api/3/action/package_show?id=programas_presupuestarios_conahcyt"

# Awarding body — CONAHCYT (Consejo Nacional de Humanidades, Ciencias y Tecnologías).
# Verified F4320321739, country MX. OpenAlex only has the CONACYT-era display name.
FUNDER_ID = 4320321739
FUNDER_DISPLAY_NAME = "Consejo Nacional de Ciencia y Tecnología"
# Era rename history captured in tracker notes; one funder_id covers all eras.

PROVENANCE = "conahcyt_snii_ckan"
CURRENCY = "MXN"

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/conahcyt/conahcyt_snii.parquet"

USER_AGENT = "openalex-walden-conahcyt-ingest/1.0 (+https://openalex.org)"

# Polite — repodatos.atdt.gob.mx serves cleanly but be respectful (Mexican
# government infra). 13 CSVs × 10MB ≈ 130MB total.
MIN_REQUEST_INTERVAL_S = 0.5


# =============================================================================
# Smoke + CKAN discovery
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: CKAN reachable + 2024 CSV accessible")
    print("=" * 60)
    r = requests.get(CKAN_PACKAGE_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"):
        print(f"[ERROR] CKAN package_show failed: {j.get('error')}")
        sys.exit(3)
    n_resources = len(j["result"]["resources"])
    print(f"  CKAN package has {n_resources} resources")
    snii_count = sum(1 for x in j["result"]["resources"]
                     if "S191" in x.get("name","") and x.get("format") == "CSV")
    print(f"  S191 SNII CSVs: {snii_count}")
    if snii_count < 5:
        print(f"[ERROR] expected 5+ S191 CSVs, got {snii_count}")
        sys.exit(3)


def discover_snii_csv_urls() -> list[tuple[str, str]]:
    """Return list of (year, csv_url) for all S191 CSVs."""
    r = requests.get(CKAN_PACKAGE_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    j = r.json()
    out: list[tuple[str, str]] = []
    for res in j["result"]["resources"]:
        if "S191" not in res.get("name", "") or res.get("format") != "CSV":
            continue
        url = res["url"]
        m = re.search(r"_(\d{4})\.csv$", url)
        year = m.group(1) if m else "unknown"
        out.append((year, url))
    return sorted(out)


def download_csv(year: str, url: str, output_dir: Path) -> Path:
    out = output_dir / f"base_snii-s191_{year}.csv"
    print(f"  [{year}] downloading {url}")
    headers = {"User-Agent": USER_AGENT}
    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)
    sz_mb = out.stat().st_size / (1024 * 1024)
    print(f"  [{year}] wrote {sz_mb:.1f} MB to {out}")
    return out


# =============================================================================
# Parse + dedup
# =============================================================================

# Dates in this CSV come in TWO formats simultaneously — `2022-01-01` ISO,
# AND `31/12/2025` Spanish (d/m/y). Confirmed in 2024 CSV sample.
_DATE_RES = [
    (re.compile(r"^(\d{4})-(\d{2})-(\d{2})$"), lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
    (re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$"), lambda m: f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"),
]


def parse_date(raw: Optional[str]) -> Optional[str]:
    if not raw or pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "no aplica", "n/a"):
        return None
    for pat, fmt in _DATE_RES:
        m = pat.match(s)
        if m:
            iso = fmt(m)
            # Validate that month/day are sane
            try:
                datetime.strptime(iso, "%Y-%m-%d")
                return iso
            except ValueError:
                continue
    return None


# SNII level → human-readable funder_scheme string.
# `nivel_distincion` field values: 1, 2, 3, C (Candidato), E (Emérito).
_LEVEL_LABEL = {
    "1": "SNII Investigador Nivel 1",
    "2": "SNII Investigador Nivel 2",
    "3": "SNII Investigador Nivel 3",
    "C": "SNII Candidato a Investigador",
    "E": "SNII Investigador Emérito",
}


def label_level(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    key = str(raw).strip().upper()
    return _LEVEL_LABEL.get(key, f"SNII Nivel {key}")


def build_dataframe(csv_paths: list[Path]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Read + concat all S191 SNII CSVs")
    print("=" * 60)
    frames = []
    for p in csv_paths:
        df = pd.read_csv(p, low_memory=False, dtype=str, keep_default_na=False)
        print(f"  {p.name}: {len(df)} rows × {len(df.columns)} cols")
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    print(f"\n  combined raw rows: {len(raw):,}")

    # Normalize columns
    rename_map = {
        "anio": "anio",
        "cvu": "cvu",
        "nombre": "nombre",
        "primer_apellido": "primer_apellido",
        "segundo_apellido": "segundo_apellido",
        "grado_estudios": "grado_estudios",
        "nivel_distincion": "nivel_distincion",
        "inst_adscrip": "inst_adscrip",
        "entidad_federativa_inst_adscrip": "entidad_federativa",
        "area_conocimiento": "area_conocimiento",
        "fecha_inicio_vig": "fecha_inicio_vig",
        "fecha_termino_vig": "fecha_termino_vig",
        "tipo_apoyo": "tipo_apoyo",
        "monto_anual": "monto_anual",
        "monto_anual_adicional": "monto_anual_adicional",
        "otros_apoyos": "otros_apoyos",
    }
    missing = [c for c in rename_map if c not in raw.columns]
    if missing:
        print(f"[ERROR] expected columns missing: {missing}\n"
              f"  actual columns: {list(raw.columns)}")
        sys.exit(4)
    raw = raw.rename(columns=rename_map)

    # Parse dates
    raw["start_date"] = raw["fecha_inicio_vig"].apply(parse_date)
    raw["end_date"]   = raw["fecha_termino_vig"].apply(parse_date)

    # Per-appointment dedup key (cvu × level × start_date)
    raw["dedup_key"] = (
        raw["cvu"].fillna("") + "|"
        + raw["nivel_distincion"].fillna("") + "|"
        + raw["start_date"].fillna("")
    )

    # Dedup: keep the most recent annual record per (cvu, level, start_date)
    raw["anio_int"] = pd.to_numeric(raw["anio"], errors="coerce").fillna(0).astype(int)
    raw = raw.sort_values("anio_int", ascending=False).drop_duplicates(
        subset=["dedup_key"], keep="first"
    )
    print(f"  after dedup on (cvu, nivel_distincion, fecha_inicio_vig): {len(raw):,}")

    # Build flat awards rows
    rows = []
    seen_award_ids: set[str] = set()
    for r in raw.to_dict(orient="records"):
        cvu = (r.get("cvu") or "").strip()
        start = r.get("start_date") or ""
        level = (r.get("nivel_distincion") or "").strip()
        if not cvu:
            continue
        funder_award_id = f"conahcyt-snii-{cvu}-{level}-{start}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"Duplicate funder_award_id {funder_award_id!r} after dedup — "
                f"investigate the raw payload before re-running."
            )
        seen_award_ids.add(funder_award_id)

        # Spanish naming convention: family name = primer_apellido (paternal).
        # segundo_apellido (maternal) preserved as a separate metadata field
        # but not used for the OpenAlex family_name.
        given  = (r.get("nombre") or "").strip()
        family = (r.get("primer_apellido") or "").strip()
        full_name = " ".join(x for x in (given, family,
                                          (r.get("segundo_apellido") or "").strip()) if x)

        # Amount parsing — strip commas, handle blank
        def num(s):
            try:
                v = float(str(s).replace(",", "").strip()) if s and str(s).strip() else None
                return v
            except ValueError:
                return None
        m_anual = num(r.get("monto_anual"))
        m_adic  = num(r.get("monto_anual_adicional"))
        amount  = (m_anual or 0.0) + (m_adic or 0.0)
        if amount <= 0:
            amount = None

        rows.append({
            "funder_award_id":     funder_award_id,
            "cvu":                 cvu,
            "last_reported_year":  str(r.get("anio_int") or ""),
            "researcher_full_name": full_name,
            "given_name":          given,
            "family_name":         family,
            "maternal_surname":    r.get("segundo_apellido") or None,
            "snii_level":          level,
            "snii_level_label":    label_level(level),
            "grado_estudios":      r.get("grado_estudios") or None,
            "institution":         r.get("inst_adscrip") or None,
            "entidad_federativa":  r.get("entidad_federativa") or None,
            "area_conocimiento":   r.get("area_conocimiento") or None,
            "start_date":          r.get("start_date"),
            "end_date":            r.get("end_date"),
            "amount_mxn":          amount,
            "currency":            CURRENCY if amount is not None else None,
            "tipo_apoyo":          r.get("tipo_apoyo") or None,
            "otros_apoyos":        r.get("otros_apoyos") or None,
            "landing_page_url":    None,  # CONAHCYT doesn't expose per-researcher SNII pages publicly
            "declined":            False,  # schema parity; SNII data doesn't surface declines
        })

    df = pd.DataFrame.from_records(rows)
    print(f"\n  flat output: {len(df):,} rows × {len(df.columns)} cols")
    n_amt = df["amount_mxn"].notna().sum()
    n_inst = df["institution"].astype(bool).sum()
    n_dates = df["start_date"].astype(bool).sum()
    print(f"  coverage: amount={n_amt:,} ({n_amt*100/len(df):.0f}%)  "
          f"institution={n_inst:,} ({n_inst*100/len(df):.0f}%)  "
          f"dates={n_dates:,} ({n_dates*100/len(df):.0f}%)")
    print(f"\n  By SNII level (funder_scheme):")
    print(df.groupby("snii_level_label").size().sort_values(ascending=False).to_string())
    print(f"\n  By area_conocimiento (top 8):")
    print(df.groupby("area_conocimiento").size().sort_values(ascending=False).head(8).to_string())

    # Runbook §1.2.5 — string before parquet to avoid pyarrow int-inference.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "conahcyt_snii.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  [OK] wrote {len(df):,} rows ({sz_mb:.1f} MB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_conahcyt_snii.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"    [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(
            f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate first."
        )
        return False
    print(f"    [OK] new corpus not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/conahcyt"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse existing per-year CSVs in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--years", type=str, default=None,
                        help="Comma-separated years to ingest (default: all 2012-2024)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("CONAHCYT / SECIHTI SNII (S191) → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    urls_by_year = discover_snii_csv_urls()
    print(f"\n  Discovered {len(urls_by_year)} S191 CSVs via CKAN")
    if args.years:
        wanted = {y.strip() for y in args.years.split(",")}
        urls_by_year = [(y, u) for y, u in urls_by_year if y in wanted]
        print(f"  Filtered to: {[y for y,_ in urls_by_year]}")

    csv_paths = []
    for year, url in urls_by_year:
        path = args.output_dir / f"base_snii-s191_{year}.csv"
        if args.skip_download:
            if not path.exists():
                print(f"[ERROR] --skip-download given but {path} missing")
                sys.exit(6)
            print(f"  [{year}] [SKIP] reusing {path}")
        else:
            path = download_csv(year, url, args.output_dir)
        csv_paths.append(path)

    df = build_dataframe(csv_paths)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateCONAHCYTAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
