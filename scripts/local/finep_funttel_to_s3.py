#!/usr/bin/env python3
"""
FINEP FUNTTEL projects -> S3 Data Pipeline
==========================================

Downloads the official Finep/FUNTTEL contracted-project workbook and writes a
parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://premio.finep.gov.br/a-finep-externo/fontes-de-recurso/outras-fontes/o-que-e-funttel?layout=dashboard&view=dashboard
    https://download.finep.gov.br/Contratacao_Funttel.xlsx

Finep's FUNTTEL dashboard states that the spreadsheets are updated weekly and
contain project-level contract and disbursement information for projects funded
with FUNTTEL resources. This ingest uses the "Contratacao_Funttel.xlsx"
contract workbook as the award authority because it has one row per contracted
project with contract number, title, recipient, dates, status, and amount.

Output
------
s3://openalex-ingest/awards/finep_funttel/finep_funttel_projects.parquet

Usage
-----
    python finep_funttel_to_s3.py --skip-upload
    python finep_funttel_to_s3.py --limit 10 --skip-upload
    python finep_funttel_to_s3.py --skip-download --skip-upload
    python finep_funttel_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3

The XLSX reader below intentionally uses only the Python standard library so
local validation does not depend on openpyxl being installed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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

    def _open_utf8(
        file,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


# =============================================================================
# Configuration
# =============================================================================

FUNTTEL_DASHBOARD_URL = (
    "https://premio.finep.gov.br/a-finep-externo/fontes-de-recurso/outras-fontes/"
    "o-que-e-funttel?layout=dashboard&view=dashboard"
)
CONTRACT_WORKBOOK_URL = "https://download.finep.gov.br/Contratacao_Funttel.xlsx"
DISBURSEMENT_WORKBOOK_URL = "https://download.finep.gov.br/liberacao_FUNTTEL.xlsx"

FUNDER_ID = 4320322904
FUNDER_DISPLAY_NAME = "Financiadora de Estudos e Projetos"
PROVENANCE = "finep_funttel"
CURRENCY = "BRL"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/finep_funttel/finep_funttel_projects.parquet"

USER_AGENT = "openalex-walden-finep-funttel-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 90, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s: {url}")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s: {url}")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def amount_to_string(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    try:
        amount = float(text.replace(",", ""))
    except ValueError:
        return None
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def excel_serial_to_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        return text[:10]
    try:
        serial = float(text)
    except ValueError:
        return None
    if serial <= 0:
        return None
    # Excel's 1900 date system includes the leap-year bug; 1899-12-30 is
    # the conventional base used by pandas/openpyxl for interoperable dates.
    return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime("%Y-%m-%d")


def col_to_index(cell_ref: str) -> int:
    letters = re.match(r"([A-Z]+)", cell_ref)
    if not letters:
        raise ValueError(f"Bad XLSX cell reference: {cell_ref}")
    idx = 0
    for char in letters.group(1):
        idx = idx * 26 + (ord(char) - ord("A") + 1)
    return idx - 1


def parse_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for si in root.findall("m:si", ns):
        parts = [node.text or "" for node in si.findall(".//m:t", ns)]
        strings.append("".join(parts))
    return strings


def sheet_paths(zf: zipfile.ZipFile) -> dict[str, str]:
    ns = {
        "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_by_id = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels.findall("rel:Relationship", ns)
    }
    out: dict[str, str] = {}
    for sheet in workbook.findall(".//m:sheet", ns):
        rid = sheet.attrib[f"{{{ns['r']}}}id"]
        target = rel_by_id[rid]
        out[sheet.attrib["name"]] = "xl/" + target.lstrip("/")
    return out


def parse_xlsx_sheet(path: Path, sheet_name: str) -> list[list[Optional[str]]]:
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as zf:
        shared_strings = parse_shared_strings(zf)
        paths = sheet_paths(zf)
        if sheet_name not in paths:
            raise RuntimeError(f"Workbook {path} is missing sheet {sheet_name!r}; found {sorted(paths)}")
        root = ET.fromstring(zf.read(paths[sheet_name]))

    rows: list[list[Optional[str]]] = []
    for row in root.findall(".//m:sheetData/m:row", ns):
        values: dict[int, Optional[str]] = {}
        for cell in row.findall("m:c", ns):
            ref = cell.attrib.get("r", "")
            idx = col_to_index(ref)
            cell_type = cell.attrib.get("t")
            value: Optional[str] = None
            if cell_type == "inlineStr":
                parts = [node.text or "" for node in cell.findall(".//m:t", ns)]
                value = "".join(parts)
            else:
                v = cell.find("m:v", ns)
                if v is not None and v.text is not None:
                    if cell_type == "s":
                        value = shared_strings[int(v.text)]
                    else:
                        value = v.text
            values[idx] = clean_text(value)
        if values:
            width = max(values) + 1
            row_values = [values.get(i) for i in range(width)]
            if any(v is not None for v in row_values):
                rows.append(row_values)
    return rows


def rows_to_dataframe(rows: list[list[Optional[str]]]) -> pd.DataFrame:
    header_idx: Optional[int] = None
    for idx, row in enumerate(rows):
        values = {clean_text(v) for v in row}
        if {"Instrumento", "Contrato", "Título", "Valor Finep"}.issubset(values):
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError("Could not locate FUNTTEL contract header row")

    header = [clean_text(v) for v in rows[header_idx]]
    records: list[dict[str, Any]] = []
    for row in rows[header_idx + 1:]:
        record = {}
        for i, name in enumerate(header):
            if not name:
                continue
            record[name] = clean_text(row[i] if i < len(row) else None)
        if not any(record.values()):
            continue
        if record.get("Instrumento") == "Instrumento":
            continue
        if not record.get("Contrato"):
            continue
        records.append(record)
    return pd.DataFrame(records)


def discover_workbook_urls() -> dict[str, str]:
    print(f"  Fetching FUNTTEL dashboard: {FUNTTEL_DASHBOARD_URL}")
    resp = polite_get(FUNTTEL_DASHBOARD_URL)
    html = resp.text
    urls = re.findall(r"https?://[^\"'\s<>]+(?:<br>\s*)?xlsx", html, flags=re.I)
    normalized = {re.sub(r"\.?<br>\s*", ".", url, flags=re.I) for url in urls}
    normalized = {url.replace("..xlsx", ".xlsx") for url in normalized}
    if CONTRACT_WORKBOOK_URL not in normalized:
        print("  [WARN] contract workbook URL not discoverable in page HTML; using documented URL")
    if DISBURSEMENT_WORKBOOK_URL not in normalized:
        print("  [WARN] disbursement workbook URL not discoverable in page HTML; using documented URL")
    return {
        "contract": CONTRACT_WORKBOOK_URL,
        "disbursement": DISBURSEMENT_WORKBOOK_URL,
    }


def download_sources(output_dir: Path) -> tuple[Path, dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download official Finep FUNTTEL workbooks")
    print("=" * 60)
    urls = discover_workbook_urls()
    manifest: dict[str, Any] = {
        "dashboard_url": FUNTTEL_DASHBOARD_URL,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "workbooks": {},
    }
    contract_path = output_dir / "Contratacao_Funttel.xlsx"
    for key, url in urls.items():
        resp = polite_get(url, timeout=120)
        workbook_path = output_dir / Path(urljoin(url, url).split("/")[-1])
        workbook_path.write_bytes(resp.content)
        manifest["workbooks"][key] = {
            "url": url,
            "path": workbook_path.name,
            "bytes": len(resp.content),
        }
        print(f"  [OK] {key}: {len(resp.content):,} bytes -> {workbook_path}")
        if key == "contract":
            contract_path = workbook_path
    (output_dir / "finep_funttel_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return contract_path, manifest


def load_cached_sources(output_dir: Path) -> tuple[Path, dict[str, Any]]:
    contract_path = output_dir / "Contratacao_Funttel.xlsx"
    manifest_path = output_dir / "finep_funttel_manifest.json"
    if not contract_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {contract_path}")
    manifest: dict[str, Any] = {
        "dashboard_url": FUNTTEL_DASHBOARD_URL,
        "downloaded_at": None,
        "workbooks": {"contract": {"url": CONTRACT_WORKBOOK_URL, "path": contract_path.name}},
    }
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(f"  [OK] loaded cached contract workbook: {contract_path}")
    return contract_path, manifest


def source_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)


def build_description(row: dict[str, Any]) -> Optional[str]:
    parts = []
    for label, key in [
        ("Demand", "Demanda"),
        ("Product", "Produto"),
        ("Line of action", "Linha de Ação"),
        ("Funding action", "Ação de Fomento"),
        ("Status", "Status"),
        ("Recipient", "Proponente"),
        ("Executor", "Executor"),
        ("Location", "Município"),
    ]:
        value = clean_text(row.get(key))
        if value:
            if key == "Município" and clean_text(row.get("UF")):
                value = f"{value}, {clean_text(row.get('UF'))}"
            parts.append(f"{label}: {value}")
    return "; ".join(parts) if parts else None


def normalize(contract_path: Path, manifest: dict[str, Any], limit: Optional[int], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Parse, normalize, and validate")
    print("=" * 60)
    rows = parse_xlsx_sheet(contract_path, "Projetos_Operação_Direta")
    source_df = rows_to_dataframe(rows)
    print(f"  Parsed contract sheet: {len(source_df):,} project rows, {len(source_df.columns)} columns")

    if limit is not None:
        source_df = source_df.head(limit).copy()
        print(f"  [LIMIT] keeping first {len(source_df):,} rows")

    required_columns = [
        "Instrumento",
        "Demanda",
        "Produto",
        "Ref",
        "Contrato",
        "Data Assinatura",
        "Prazo Execução",
        "Título",
        "Proponente",
        "CNPJ Proponente",
        "Município",
        "UF",
        "Valor Finep",
        "Status",
        "Valor Pago",
    ]
    missing_columns = [col for col in required_columns if col not in source_df.columns]
    if missing_columns:
        raise RuntimeError(f"Missing expected source columns: {missing_columns}")

    downloaded_at = manifest.get("downloaded_at") or datetime.now(timezone.utc).isoformat()
    workbook_url = (
        manifest.get("workbooks", {})
        .get("contract", {})
        .get("url", CONTRACT_WORKBOOK_URL)
    )
    records: list[dict[str, Any]] = []
    for _, source_row in source_df.iterrows():
        row = {col: source_row.get(col) for col in source_df.columns}
        signed_date = excel_serial_to_date(row.get("Data Assinatura"))
        end_date = excel_serial_to_date(row.get("Prazo Execução"))
        amount = amount_to_string(row.get("Valor Finep"))
        amount_paid = amount_to_string(row.get("Valor Pago"))
        contract = clean_text(row.get("Contrato"))
        records.append({
            "funder_award_id": contract,
            "display_name": clean_text(row.get("Título")),
            "description": build_description(row),
            "instrument": clean_text(row.get("Instrumento")),
            "demand": clean_text(row.get("Demanda")),
            "product": clean_text(row.get("Produto")),
            "line_of_action": clean_text(row.get("Linha de Ação")),
            "funding_action": clean_text(row.get("Ação de Fomento")),
            "ref": clean_text(row.get("Ref")),
            "contract_number": contract,
            "signed_date": signed_date,
            "end_date": end_date,
            "recipient_name": clean_text(row.get("Proponente")),
            "recipient_cnpj": clean_text(row.get("CNPJ Proponente")),
            "recipient_size": clean_text(row.get("Porte Proponente")),
            "executor_name": clean_text(row.get("Executor")),
            "municipality": clean_text(row.get("Município")),
            "state": clean_text(row.get("UF")),
            "amount": amount,
            "currency": CURRENCY if amount is not None else None,
            "status": clean_text(row.get("Status")),
            "counterpart_financial": amount_to_string(row.get("Contrapartida Financeira")),
            "counterpart_non_financial": amount_to_string(row.get("Contrapartida Não-Financeira")),
            "amount_paid": amount_paid,
            "intervening_parties": clean_text(row.get("Intervenientes")),
            "intervenor_financial_contribution": amount_to_string(row.get("Aporte Financeiro Interv.")),
            "intervenor_non_financial_contribution": amount_to_string(row.get("Aporte Não-Financeiro Interv.")),
            "funding_type": "research",
            "funder_scheme": "FUNTTEL",
            "source_year": signed_date[:4] if signed_date else None,
            "landing_page_url": FUNTTEL_DASHBOARD_URL,
            "source_workbook_url": workbook_url,
            "source_row_json": source_row_json(row),
            "downloaded_at": downloaded_at,
        })

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No FINEP FUNTTEL rows were normalized")

    for col in ["funder_award_id", "display_name", "recipient_name", "amount", "currency", "signed_date", "end_date"]:
        missing = int(out[col].isna().sum())
        if missing:
            raise RuntimeError(f"Required column {col} has {missing:,} missing values")

    dupes = int(out["funder_award_id"].duplicated().sum())
    if dupes:
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes:,}")

    if full_run and len(out) < 20:
        raise RuntimeError(f"Expected at least 20 FUNTTEL contract rows in a full run; got {len(out):,}")

    amount_numeric = pd.to_numeric(out["amount"], errors="coerce")
    paid_numeric = pd.to_numeric(out["amount_paid"], errors="coerce")
    year_numeric = pd.to_numeric(out["source_year"], errors="coerce")
    print(f"  Rows: {len(out):,}")
    print(f"  Unique funder_award_id: {out['funder_award_id'].nunique():,}")
    print(f"  Year range: {int(year_numeric.min())} - {int(year_numeric.max())}")
    print(f"  Amount coverage: {amount_numeric.notna().mean() * 100:.1f}%")
    print(f"  Total BRL amount: {amount_numeric.sum():,.2f}")
    print(f"  Total BRL paid: {paid_numeric.sum():,.2f}")
    print(f"  Recipient coverage: {out['recipient_name'].notna().mean() * 100:.1f}%")
    print(f"  Status distribution: {out['status'].value_counts().to_dict()}")
    print(f"  Demand distribution: {out['demand'].value_counts().to_dict()}")

    return out.astype("string")


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "finep_funttel_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df):,} rows ({parquet_path.stat().st_size / 1024:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink-check; "
            "rerun with --skip-upload for local validation."
        ) from exc

    client = boto3.client("s3")
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check needed.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_finep_funttel_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        print(f"\n[ERROR] Refusing to shrink FINEP FUNTTEL corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
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
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download FINEP FUNTTEL projects and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/finep_funttel"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached XLSX files from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("FINEP FUNTTEL projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {FUNTTEL_DASHBOARD_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw XLSX files")
        contract_path, manifest = load_cached_sources(args.output_dir)
    else:
        contract_path, manifest = download_sources(args.output_dir)

    df = normalize(contract_path, manifest, args.limit, full_run=args.limit is None)
    parquet_path = write_outputs(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
