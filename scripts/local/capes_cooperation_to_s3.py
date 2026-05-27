#!/usr/bin/env python3
"""
CAPES International Cooperation Projects -> S3 Pipeline
=======================================================

Downloads CAPES "Projetos de cooperacao internacional/pesquisa - SCBA"
rows from CAPES' official Dados Abertos CKAN portal. The package metadata
states that these are cost-support values granted by CAPES' Diretoria de
Relacoes Internacionais (DRI), with beneficiary, institution, program,
dates, and BRL amount fields.

Discovery (method-1 CKAN open-data API + bulk CSV resources):
  - package_search locates the CAPES package
  - CSV resources named BR-CAPES-PROJETOS-COOPERACAO-* are downloaded
  - duplicate source rows across resources are collapsed by a stable
    normalized key before writing parquet

Awarding body in OpenAlex:
  Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior
  (CAPES, F4320321091, BR, DOI 10.13039/501100002322).

Output
------
  s3://openalex-ingest/awards/capes_cooperation/capes_cooperation_projects.parquet

Usage
-----
    python capes_cooperation_to_s3.py --skip-upload
    python capes_cooperation_to_s3.py --limit 10 --skip-upload
    python capes_cooperation_to_s3.py --skip-download --skip-upload
    python capes_cooperation_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import hashlib
import io
import json
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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


# =============================================================================
# Configuration
# =============================================================================

CKAN_PACKAGE_SEARCH = "https://dadosabertos.capes.gov.br/api/3/action/package_search"
PACKAGE_QUERY = "2023-a-2025-projetos-de-cooperacao-internacional-pesquisa-scba"
PACKAGE_URL = "https://dadosabertos.capes.gov.br/dataset/2023-a-2025-projetos-de-cooperacao-internacional-pesquisa-scba"
METADATA_URL = "https://metadados.capes.gov.br/index.php/catalog/326"

FUNDER_ID = 4320321091
FUNDER_DISPLAY_NAME = "Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior"
PROVENANCE = "capes_cooperacao_internacional"
CURRENCY = "BRL"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/capes_cooperation/capes_cooperation_projects.parquet"

USER_AGENT = "openalex-walden-capes-cooperation-ingest/1.0 (+https://openalex.org)"
DEFAULT_OUTPUT_DIR = Path("/tmp/capes_cooperation")

SOURCE_COLUMNS = [
    "NM_BENEFICIARIO",
    "NR_DOCUMENTO",
    "AN_CONCESSAO",
    "DT_INICIO_CONCESSAO",
    "DT_TERMINO_CONCESSAO",
    "DT_DESISTENCIA",
    "SG_PROGRAMA_FOMENTO",
    "NM_IES_CAPES",
    "SG_IES_CAPES",
    "SG_UF_IES",
    "NM_MUNICIPIO_IES_ORIGEM",
    "CD_MUNICIPIO_PPG",
    "DS_SITUACAO_JURIDICA_IES_ORIGEM",
    "NM_PPG",
    "CD_PPG",
    "NM_GRANDE_AREA",
    "NM_AREA_AVALIACAO",
    "NM_AREA_CONHECIMENTO",
    "TP_NATUREZA_DESPESA",
    "VL_CONCEDIDO",
    "VL_EMPENHADO",
    "VL_PAGO",
]

PROCESS_RE = re.compile(r"(\d{5}\.\d+/\d{4}-\d{2})")
PROCESS_SUFFIX_RE = re.compile(r"\s+-\s+\d{5}\.\d+/\d{4}-\d{2}\s*$")
MONEY_RE = re.compile(r"-?\$?\s*([0-9,]+(?:\.[0-9]+)?)")
SAS_DATE_RE = re.compile(r"^(\d{2})([A-Z]{3})(\d{4})$")
MONTHS = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def clean_string(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = re.sub(r"\s+", " ", str(value).strip())
    return text or None


def clean_beneficiary_name(value: object) -> Optional[str]:
    text = clean_string(value)
    if not text:
        return None
    return PROCESS_SUFFIX_RE.sub("", text).strip() or None


def extract_source_award_number(value: object) -> Optional[str]:
    text = clean_string(value)
    if not text:
        return None
    m = PROCESS_RE.search(text)
    return m.group(1) if m else None


def parse_money(value: object) -> Optional[float]:
    text = clean_string(value)
    if not text:
        return None
    m = MONEY_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_year(value: object) -> Optional[int]:
    text = clean_string(value)
    if not text:
        return None
    m = re.search(r"(19\d{2}|20\d{2})", text)
    return int(m.group(1)) if m else None


def year_start(value: object) -> Optional[str]:
    year = parse_year(value)
    return f"{year}-01-01" if year else None


def year_end(value: object) -> Optional[str]:
    year = parse_year(value)
    return f"{year}-12-31" if year else None


def parse_sas_date(value: object) -> Optional[str]:
    text = clean_string(value)
    if not text:
        return None
    m = SAS_DATE_RE.match(text.upper())
    if not m:
        return None
    dd, mon, yyyy = m.groups()
    mm = MONTHS.get(mon)
    return f"{yyyy}-{mm}-{dd}" if mm else None


def split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    parts = full_name.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def title_case_if_upper(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    letters = [c for c in text if c.isalpha()]
    if letters and all(c.isupper() for c in letters):
        return text.title()
    return text


def source_row_key(row: pd.Series) -> str:
    beneficiary = (clean_beneficiary_name(row.get("NM_BENEFICIARIO")) or "").casefold()
    process_number = extract_source_award_number(row.get("NM_BENEFICIARIO")) or ""
    values = [beneficiary, process_number]
    for col in [
        "NR_DOCUMENTO",
        "AN_CONCESSAO",
        "DT_INICIO_CONCESSAO",
        "DT_TERMINO_CONCESSAO",
        "SG_PROGRAMA_FOMENTO",
        "NM_IES_CAPES",
        "CD_PPG",
        "VL_CONCEDIDO",
        "VL_EMPENHADO",
        "VL_PAGO",
    ]:
        values.append((clean_string(row.get(col)) or "").casefold())
    return "|".join(values)


def row_hash(row_key: str) -> str:
    return hashlib.sha1(row_key.encode("utf-8")).hexdigest()


def make_display_name(row: pd.Series) -> str:
    program = clean_string(row.get("SG_PROGRAMA_FOMENTO")) or "International cooperation"
    beneficiary = clean_beneficiary_name(row.get("NM_BENEFICIARIO")) or "beneficiary"
    institution = clean_string(row.get("SG_IES_CAPES")) or clean_string(row.get("NM_IES_CAPES"))
    year = clean_string(row.get("AN_CONCESSAO"))
    parts = [f"CAPES {program} researcher support for {title_case_if_upper(beneficiary)}"]
    if institution:
        parts.append(f"at {institution}")
    if year:
        parts.append(f"({year})")
    return " ".join(parts)


def make_description(row: pd.Series) -> Optional[str]:
    bits = []
    program = clean_string(row.get("SG_PROGRAMA_FOMENTO"))
    ppg = clean_string(row.get("NM_PPG"))
    institution = clean_string(row.get("NM_IES_CAPES"))
    area = clean_string(row.get("NM_AREA_CONHECIMENTO")) or clean_string(row.get("NM_AREA_AVALIACAO"))
    expense_type = clean_string(row.get("TP_NATUREZA_DESPESA"))
    amount = parse_money(row.get("VL_CONCEDIDO"))
    if program:
        bits.append(f"CAPES DRI program: {program}.")
    if expense_type:
        bits.append(f"Expense type: {expense_type}.")
    if ppg:
        bits.append(f"Graduate program: {ppg}.")
    if area:
        bits.append(f"Knowledge area: {area}.")
    if institution:
        bits.append(f"Institution: {institution}.")
    if amount is not None:
        bits.append(f"Granted amount: BRL {amount:,.2f}.")
    return " ".join(bits) or None


def http_get(url: str, params: Optional[dict] = None, timeout: int = 45, attempts: int = 3) -> requests.Response:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/csv,*/*"}
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            log(f"  GET {resp.url} -> {resp.status_code} ({len(resp.content):,} bytes)")
            resp.raise_for_status()
            return resp
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_error = e
            if attempt == attempts:
                break
            delay = 2 * attempt
            log(f"  GET retry {attempt}/{attempts} after {type(e).__name__}: {e}; sleeping {delay}s")
            time.sleep(delay)
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}") from last_error


def discover_csv_resources() -> list[dict]:
    params = {"q": PACKAGE_QUERY, "rows": "1"}
    payload = http_get(CKAN_PACKAGE_SEARCH, params=params).json()
    results = payload.get("result", {}).get("results", [])
    if not results:
        raise RuntimeError(f"CAPES package not found via CKAN package_search q={PACKAGE_QUERY!r}")
    package = results[0]
    resources = []
    for res in package.get("resources", []):
        name = res.get("name") or ""
        fmt = (res.get("format") or "").upper()
        url = res.get("url") or ""
        if fmt == "CSV" and name.startswith("BR-CAPES-PROJETOS-COOPERACAO-") and url:
            resources.append({"name": name, "url": url})
    if not resources:
        raise RuntimeError("No CAPES cooperation CSV resources found in package")
    log(f"  discovered {len(resources)} CSV resources")
    for res in resources:
        log(f"    {res['name']}: {res['url']}")
    return resources


def download_resource(resource: dict, raw_dir: Path, skip_download: bool) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", resource["name"]).strip("_") + ".csv"
    path = raw_dir / filename
    if skip_download:
        if not path.exists():
            raise FileNotFoundError(f"--skip-download requested but cached CSV is missing: {path}")
        log(f"  cache hit {path}")
        return path
    resp = http_get(resource["url"], timeout=60)
    path.write_bytes(resp.content)
    log(f"  wrote raw CSV {path} ({path.stat().st_size:,} bytes)")
    return path


def read_resource_csv(path: Path, resource: dict) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", encoding="latin1", dtype="string")
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{path} missing expected columns: {missing}")
    df = df[SOURCE_COLUMNS].copy()
    df["source_resource_name"] = resource["name"]
    df["source_resource_url"] = resource["url"]
    return df


def prepare_rows(raw_df: pd.DataFrame) -> list[dict]:
    raw_df = raw_df.copy()
    raw_df["source_row_key"] = raw_df.apply(source_row_key, axis=1)
    raw_df["source_row_hash"] = raw_df["source_row_key"].map(row_hash)
    raw_df["_non_null_score"] = raw_df[SOURCE_COLUMNS].notna().sum(axis=1)

    before = len(raw_df)
    raw_df = (
        raw_df.sort_values(["source_row_key", "_non_null_score", "source_resource_name"], ascending=[True, False, True])
        .drop_duplicates("source_row_key", keep="first")
        .sort_values(["AN_CONCESSAO", "SG_PROGRAMA_FOMENTO", "NM_BENEFICIARIO", "source_row_hash"], na_position="last")
        .reset_index(drop=True)
    )
    log(f"  deduped normalized source rows: {before:,} -> {len(raw_df):,} (dropped {before - len(raw_df):,})")

    rows = []
    skipped_nonpositive_amount = 0
    for _, r in raw_df.iterrows():
        beneficiary_name = clean_beneficiary_name(r.get("NM_BENEFICIARIO"))
        beneficiary_name = title_case_if_upper(beneficiary_name)
        given_name, family_name = split_name(beneficiary_name)
        source_hash = clean_string(r.get("source_row_hash"))
        amount = parse_money(r.get("VL_CONCEDIDO"))
        if amount is not None and amount <= 0:
            skipped_nonpositive_amount += 1
            continue
        committed = parse_money(r.get("VL_EMPENHADO"))
        paid = parse_money(r.get("VL_PAGO"))
        start_year = parse_year(r.get("DT_INICIO_CONCESSAO")) or parse_year(r.get("AN_CONCESSAO"))
        end_year = parse_year(r.get("DT_TERMINO_CONCESSAO"))

        rows.append(
            {
                "funder_award_id": f"capes-cooperation-{source_hash[:24]}",
                "source_row_hash": source_hash,
                "display_name": make_display_name(r),
                "description": make_description(r),
                "beneficiary_name": beneficiary_name,
                "beneficiary_given_name": given_name,
                "beneficiary_family_name": family_name,
                "source_document_masked": clean_string(r.get("NR_DOCUMENTO")),
                "source_award_number": extract_source_award_number(r.get("NM_BENEFICIARIO")),
                "source_year": parse_year(r.get("AN_CONCESSAO")),
                "source_start_year": start_year,
                "source_end_year": end_year,
                "start_date": f"{start_year}-01-01" if start_year else None,
                "end_date": f"{end_year}-12-31" if end_year else None,
                "withdrawal_date": parse_sas_date(r.get("DT_DESISTENCIA")),
                "funder_scheme": clean_string(r.get("SG_PROGRAMA_FOMENTO")),
                "institution_name": clean_string(r.get("NM_IES_CAPES")),
                "institution_acronym": clean_string(r.get("SG_IES_CAPES")),
                "institution_state": clean_string(r.get("SG_UF_IES")),
                "institution_city": clean_string(r.get("NM_MUNICIPIO_IES_ORIGEM")),
                "municipality_code": clean_string(r.get("CD_MUNICIPIO_PPG")),
                "institution_legal_status": clean_string(r.get("DS_SITUACAO_JURIDICA_IES_ORIGEM")),
                "graduate_program_name": clean_string(r.get("NM_PPG")),
                "graduate_program_code": clean_string(r.get("CD_PPG")),
                "major_area": clean_string(r.get("NM_GRANDE_AREA")),
                "evaluation_area": clean_string(r.get("NM_AREA_AVALIACAO")),
                "knowledge_area": clean_string(r.get("NM_AREA_CONHECIMENTO")),
                "expense_type": clean_string(r.get("TP_NATUREZA_DESPESA")),
                "amount": amount,
                "amount_committed": committed,
                "amount_paid": paid,
                "currency": CURRENCY if amount is not None else None,
                "landing_page_url": PACKAGE_URL,
                "metadata_url": METADATA_URL,
                "source_resource_name": clean_string(r.get("source_resource_name")),
                "source_resource_url": clean_string(r.get("source_resource_url")),
            }
        )
    if skipped_nonpositive_amount:
        log(f"  filtered {skipped_nonpositive_amount:,} rows with non-positive VL_CONCEDIDO")
    return rows


def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No CAPES cooperation rows parsed")
    n = len(rows)
    for field in [
        "funder_award_id",
        "display_name",
        "beneficiary_name",
        "funder_scheme",
        "institution_name",
        "graduate_program_name",
        "source_year",
        "amount",
        "currency",
    ]:
        count = sum(1 for r in rows if r.get(field) not in (None, "", []))
        log(f"  {field:<24} coverage {count:,}/{n:,} ({count * 100 / n:.1f}%)")

    ids = [r["funder_award_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise RuntimeError(f"Duplicate funder_award_id values: {len(ids) - len(set(ids))}")
    log(f"  funder_award_id uniqueness: {len(ids):,}/{n:,} distinct")

    years = [int(r["source_year"]) for r in rows if r.get("source_year") is not None]
    if years:
        log(f"  year range: {min(years)}-{max(years)}")

    amounts = [float(r["amount"]) for r in rows if r.get("amount") is not None]
    if amounts:
        amounts_sorted = sorted(amounts)
        log(
            "  amount stats BRL: "
            f"n={len(amounts):,} min={amounts_sorted[0]:,.2f} "
            f"median={amounts_sorted[len(amounts_sorted)//2]:,.2f} "
            f"max={amounts_sorted[-1]:,.2f} total={sum(amounts):,.2f}"
        )

    scheme_counts: dict[str, int] = {}
    for r in rows:
        scheme = r.get("funder_scheme") or "(missing)"
        scheme_counts[scheme] = scheme_counts.get(scheme, 0) + 1
    top = sorted(scheme_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    log("  top schemes: " + "; ".join(f"{k}={v:,}" for k, v in top))


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.astype("string")
    return df


def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3

        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError as e:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local validation") from e
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch CAPES cooperation projects -> parquet -> S3")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output_dir / "raw"
    output_path = args.output_dir / "capes_cooperation_projects.parquet"
    manifest_path = args.output_dir / "capes_cooperation_manifest.json"

    log("=== CAPES cooperation ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")

    if args.skip_download and manifest_path.exists():
        resources = json.loads(manifest_path.read_text())
        log(f"  manifest cache hit {manifest_path} ({len(resources)} resources)")
    else:
        resources = discover_csv_resources()
        manifest_path.write_text(json.dumps(resources, indent=2, ensure_ascii=False) + "\n")
        log(f"  wrote manifest {manifest_path}")

    frames = []
    for resource in resources:
        path = download_resource(resource, raw_dir, args.skip_download)
        frames.append(read_resource_csv(path, resource))

    raw_df = pd.concat(frames, ignore_index=True)
    log(f"  loaded {len(raw_df):,} source rows across {len(frames)} resources")
    rows = prepare_rows(raw_df)

    if args.limit is not None:
        rows = rows[: args.limit]
        log(f"--limit {args.limit}: keeping {len(rows):,} rows after dedupe")

    validate_rows(rows)

    df = build_dataframe(rows)
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path} ({output_path.stat().st_size / 1e3:.1f} KB)")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== CAPES cooperation ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed")

    upload_to_s3(output_path)
    log("=== CAPES cooperation ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
