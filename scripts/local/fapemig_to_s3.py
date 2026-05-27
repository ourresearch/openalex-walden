#!/usr/bin/env python3
"""
FAPEMIG Projetos Contratados -> S3 pipeline
===========================================

Downloads the official open-data workbook of contracted research and innovation
projects from Fundação de Amparo à Pesquisa do Estado de Minas Gerais (FAPEMIG)
and writes a parquet staging file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party. FAPEMIG's "Dados Abertos FAPEMIG" page publishes
the "PROJETOS CONTRATADOS" dataset and describes the fields as open data:

    https://fapemig.br/difusao-do-conhecimento/fapemig-em-numeros/dados-abertos-fapemig

The page links the current workbook:

    https://api.site.fapemig.br/wp-content/uploads/2026-4_projetos-contratados-4.xlsx

Validation on 2026-05-27:
  - 23,586 unique process numbers after repairing 11 wrapped worksheet rows
  - Year range: 2007-2025
  - 99.9-100% coverage on title, coordinator, amount, dates, and institutions
  - Amounts are published as valorcontratado and are in Brazilian reais (BRL)

The workbook has a small number of malformed rows where long titles are wrapped
into the following physical worksheet row, plus one row containing embedded TSV
records. The parser handles those deterministic source quirks and raises on
duplicate native IDs.

Output
------
s3://openalex-ingest/awards/fapemig/fapemig_projetos_contratados.parquet

Usage
-----
    python fapemig_to_s3.py --skip-upload
    python fapemig_to_s3.py --limit 10 --skip-upload
    python fapemig_to_s3.py --skip-download --skip-upload
    python fapemig_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import html
import json
import re
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---


DATA_PAGE_URL = "https://fapemig.br/difusao-do-conhecimento/fapemig-em-numeros/dados-abertos-fapemig"
DEFAULT_XLSX_URL = "https://api.site.fapemig.br/wp-content/uploads/2026-4_projetos-contratados-4.xlsx"

FUNDER_ID = 4320322980
FUNDER_DISPLAY_NAME = "Fundação de Amparo à Pesquisa do Estado de Minas Gerais"
PROVENANCE = "fapemig_projetos_contratados"
CURRENCY = "BRL"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fapemig/fapemig_projetos_contratados.parquet"

USER_AGENT = "openalex-walden-fapemig-ingest/1.0 (+https://openalex.org)"
EXPECTED_MIN_FULL_ROWS = 23000

XLSX_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
YEAR_RE = re.compile(r"^(?:19|20)\d{2}(?:\.0)?$")
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
PROCESS_RE = re.compile(r"[^a-z0-9]+")
TITLE_PREFIX_RE = re.compile(r"^(?:DR|DRA|PROF|PROFA|PROFESSOR|PROFESSORA)\.?\s+", re.IGNORECASE)


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = html.unescape(str(value)).replace("_x000D_", "\n")
    text = TAG_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text).strip()
    if not text or text.upper() == "NULL":
        return None
    return text


def discover_xlsx_url() -> str:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    log(f"Discovering workbook link from {DATA_PAGE_URL}")
    resp = session.get(DATA_PAGE_URL, timeout=60)
    resp.raise_for_status()
    links = re.findall(r'href=["\']([^"\']+projetos-contratados[^"\']+\.xlsx)["\']', resp.text, re.I)
    if not links:
        log("  [WARN] Could not discover workbook link; falling back to configured URL.")
        return DEFAULT_XLSX_URL
    url = urljoin(DATA_PAGE_URL, html.unescape(links[0]))
    log(f"  [OK] discovered workbook: {url}")
    return url


def download_workbook(output_dir: Path) -> tuple[Path, str]:
    print("\n" + "=" * 60)
    print("Step 1: Download FAPEMIG open-data workbook")
    print("=" * 60)
    xlsx_url = discover_xlsx_url()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Referer": DATA_PAGE_URL})
    resp = session.get(xlsx_url, timeout=120)
    log(f"  GET {xlsx_url} -> HTTP {resp.status_code}; {len(resp.content):,} bytes")
    resp.raise_for_status()
    if len(resp.content) < 100000:
        raise RuntimeError(f"Downloaded workbook is unexpectedly small: {len(resp.content):,} bytes")
    output_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = output_dir / "fapemig_projetos_contratados.xlsx"
    xlsx_path.write_bytes(resp.content)
    log(f"  [OK] wrote workbook to {xlsx_path}")
    return xlsx_path, xlsx_url


def load_cached_workbook(output_dir: Path) -> tuple[Path, str]:
    xlsx_path = output_dir / "fapemig_projetos_contratados.xlsx"
    if not xlsx_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {xlsx_path}")
    log(f"  [OK] using cached workbook {xlsx_path}")
    return xlsx_path, DEFAULT_XLSX_URL


def column_index(cell_ref: str) -> int:
    match = re.match(r"([A-Z]+)", cell_ref)
    if not match:
        raise RuntimeError(f"Cannot parse XLSX cell reference: {cell_ref}")
    idx = 0
    for ch in match.group(1):
        idx = idx * 26 + ord(ch) - ord("A") + 1
    return idx - 1


def read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out = []
    for si in root.findall(f"{{{XLSX_NS}}}si"):
        out.append("".join(t.text or "" for t in si.iter(f"{{{XLSX_NS}}}t")))
    return out


def physical_rows_from_xlsx(xlsx_path: Path) -> list[list[str]]:
    with zipfile.ZipFile(xlsx_path) as zf:
        shared = read_shared_strings(zf)
        sheet_name = "xl/worksheets/sheet1.xml"
        if sheet_name not in zf.namelist():
            raise RuntimeError("Expected xl/worksheets/sheet1.xml in FAPEMIG workbook.")
        rows: list[list[str]] = []
        for _, elem in ET.iterparse(BytesIO(zf.read(sheet_name)), events=("end",)):
            if not elem.tag.endswith("row"):
                continue
            values: list[str] = []
            for cell in elem:
                if not cell.tag.endswith("c"):
                    continue
                ref = cell.attrib.get("r", "")
                idx = column_index(ref)
                while len(values) <= idx:
                    values.append("")
                value_node = cell.find(f"{{{XLSX_NS}}}v")
                cell_type = cell.attrib.get("t")
                if value_node is None:
                    value = ""
                elif cell_type == "s":
                    value = shared[int(value_node.text or 0)]
                else:
                    value = value_node.text or ""
                values[idx] = value
            rows.append(values)
            elem.clear()
    if not rows:
        raise RuntimeError("No rows parsed from FAPEMIG workbook.")
    return rows


def is_year(value: str) -> bool:
    return bool(YEAR_RE.match((value or "").strip()))


def merge_title(existing: str, continuation: str) -> str:
    existing = existing.strip()
    continuation = continuation.strip()
    if not existing:
        return continuation
    if not continuation:
        return existing
    if continuation.startswith(existing):
        return continuation
    return f"{existing} {continuation}".strip()


def parse_workbook(xlsx_path: Path) -> tuple[list[dict[str, str]], dict[str, int]]:
    print("\n" + "=" * 60)
    print("Step 2: Parse workbook rows")
    print("=" * 60)

    physical = physical_rows_from_xlsx(xlsx_path)
    headers = [clean_text(v) or "" for v in physical[0]][:19]
    expected = [
        "ano", "processo", "coordenador", "modalidade", "chamada_num",
        "chamada_desc", "grupochamada", "linha", "titulo", "area",
        "camaraavaliacao", "valorcontratado", "instituicaoexecutora_sigla",
        "instituicaoexecutora_nome", "instituicaogestora_sigla",
        "instituicaogestora_nome", "data_inicio", "data_termino",
        "datadisponibilizacao",
    ]
    if headers != expected:
        raise RuntimeError(f"Unexpected workbook headers: {headers!r}")

    logical_lines: list[str] = []
    for values in physical[1:]:
        line = "\t".join(values).replace("_x000D_", "").replace("\r", "")
        for part in line.split("\n"):
            if part.strip():
                logical_lines.append(part)

    records: list[dict[str, str]] = []
    current: Optional[list[str]] = None
    repaired_continuations = 0
    extra_fields = 0
    orphan_continuations = 0

    def finish(fields: list[str]) -> None:
        nonlocal extra_fields
        if len(fields) < len(expected):
            raise RuntimeError(f"Could not repair short XLSX row: {fields!r}")
        if len(fields) > len(expected):
            extra_fields += len(fields) - len(expected)
        records.append(dict(zip(expected, fields[:len(expected)])))

    for line in logical_lines:
        fields = [clean_text(part) or "" for part in line.split("\t")]
        while fields and fields[-1] == "":
            fields.pop()
        if not fields:
            continue
        if len(fields) >= 2 and is_year(fields[0]) and fields[1]:
            if current is not None:
                finish(current)
            current = fields
        else:
            if current is None:
                orphan_continuations += 1
                continue
            repaired_continuations += 1
            if len(current) <= 9:
                while len(current) < 9:
                    current.append("")
                if fields and fields[0]:
                    current[8] = merge_title(current[8], fields[0])
                current.extend(fields[1:])
            else:
                current.extend(fields)

        if current is not None and len(current) >= len(expected):
            finish(current)
            current = None

    if current is not None:
        finish(current)

    stats = {
        "physical_rows": max(len(physical) - 1, 0),
        "logical_lines": len(logical_lines),
        "repaired_continuations": repaired_continuations,
        "extra_fields_truncated": extra_fields,
        "orphan_continuations": orphan_continuations,
    }
    log(
        f"  Parsed {len(records):,} logical records from "
        f"{stats['physical_rows']:,} physical worksheet rows"
    )
    log(
        "  Repair stats: "
        f"{repaired_continuations} continuations, "
        f"{extra_fields} extra fields truncated, "
        f"{orphan_continuations} orphan continuations"
    )
    return records, stats


def excel_date(value: Optional[str]) -> Optional[str]:
    value = clean_text(value)
    if not value:
        return None
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", value):
        d, m, y = value.split("/")
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    try:
        serial = float(value)
    except ValueError:
        return None
    if serial < 1:
        return None
    # Excel's 1900 date system includes a leap-year bug; 1899-12-30 matches
    # the convention used by pandas/openpyxl for these serials.
    return (datetime(1899, 12, 30) + timedelta(days=serial)).date().isoformat()


def amount_string(value: Optional[str]) -> Optional[str]:
    value = clean_text(value)
    if not value:
        return None
    normalized = value.replace(".", "").replace(",", ".") if "," in value else value
    try:
        amount = Decimal(normalized)
    except InvalidOperation:
        return None
    return f"{amount:.2f}"


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    name = clean_text(name)
    if not name:
        return None, None
    stripped = TITLE_PREFIX_RE.sub("", name).strip()
    parts = stripped.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def stable_award_id(processo: Optional[str]) -> str:
    processo = clean_text(processo)
    if not processo:
        raise RuntimeError("Missing processo; cannot build stable funder_award_id.")
    slug = PROCESS_RE.sub("-", processo.lower()).strip("-")
    if not slug:
        raise RuntimeError(f"Processo did not yield an ID slug: {processo!r}")
    return f"fapemig-{slug}"


def classify_funding_type(modalidade: Optional[str], grupo: Optional[str], linha: Optional[str]) -> str:
    text = " ".join(v for v in [modalidade, grupo, linha] if v).upper()
    if "BOLSA" in text or "PÓS-DOUTORADO" in text or "POS-DOUTORADO" in text:
        return "fellowship"
    return "grant"


def normalize_records(records: list[dict[str, str]], xlsx_url: str,
                      parser_stats: dict[str, int], *, limit: Optional[int]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 3: Normalize source records")
    print("=" * 60)

    if limit:
        records = records[:limit]
        log(f"  [LIMIT] keeping first {len(records):,} records")

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for rec in records:
        processo = clean_text(rec.get("processo"))
        if not processo or not is_year(clean_text(rec.get("ano")) or ""):
            continue
        amount = amount_string(rec.get("valorcontratado"))
        start_date = excel_date(rec.get("data_inicio"))
        end_date = excel_date(rec.get("data_termino"))
        published_date = excel_date(rec.get("datadisponibilizacao"))
        coordinator = clean_text(rec.get("coordenador"))
        given, family = split_name(coordinator)
        modalidade = clean_text(rec.get("modalidade"))
        grupo = clean_text(rec.get("grupochamada"))
        linha = clean_text(rec.get("linha"))
        chamada_desc = clean_text(rec.get("chamada_desc"))
        area = clean_text(rec.get("area"))
        camara = clean_text(rec.get("camaraavaliacao"))
        institution = clean_text(rec.get("instituicaoexecutora_nome"))
        source_title = clean_text(rec.get("titulo"))
        display_name = source_title or f"{modalidade or 'FAPEMIG project'} ({processo})"

        description_parts = []
        if chamada_desc:
            description_parts.append(f"Chamada: {chamada_desc}")
        if grupo:
            description_parts.append(f"Grupo: {grupo}")
        if linha:
            description_parts.append(f"Linha: {linha}")
        if area:
            description_parts.append(f"Área: {area}")
        if institution:
            description_parts.append(f"Instituição executora: {institution}")

        rows.append({
            "funder_award_id": stable_award_id(processo),
            "processo": processo,
            "display_name": display_name,
            "source_title": source_title,
            "description": "; ".join(description_parts) if description_parts else None,
            "coordinator": coordinator,
            "coordinator_given_name": given,
            "coordinator_family_name": family,
            "source_year": str(int(float(clean_text(rec.get("ano")) or "0"))),
            "start_date": start_date,
            "end_date": end_date,
            "published_date": published_date,
            "amount": amount,
            "currency": CURRENCY if amount else None,
            "modalidade": modalidade,
            "chamada_num": clean_text(rec.get("chamada_num")),
            "chamada_desc": chamada_desc,
            "grupochamada": grupo,
            "linha": linha,
            "area": area,
            "camaraavaliacao": camara,
            "instituicaoexecutora_sigla": clean_text(rec.get("instituicaoexecutora_sigla")),
            "instituicaoexecutora_nome": institution,
            "instituicaogestora_sigla": clean_text(rec.get("instituicaogestora_sigla")),
            "instituicaogestora_nome": clean_text(rec.get("instituicaogestora_nome")),
            "funding_type": classify_funding_type(modalidade, grupo, linha),
            "funder_scheme": modalidade,
            "landing_page_url": DATA_PAGE_URL,
            "source_file_url": xlsx_url,
            "parser_physical_rows": str(parser_stats.get("physical_rows", "")),
            "parser_logical_lines": str(parser_stats.get("logical_lines", "")),
            "parser_repaired_continuations": str(parser_stats.get("repaired_continuations", "")),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=limit is None)
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("Normalized dataframe is empty.")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(20).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print(f"  Distinct funder_award_id values: {df['funder_award_id'].nunique():,}")

    for col in [
        "processo", "display_name", "coordinator", "source_year", "start_date",
        "end_date", "published_date", "amount", "currency", "modalidade",
        "chamada_desc", "grupochamada", "linha", "area", "source_title",
        "instituicaoexecutora_nome", "instituicaogestora_nome",
    ]:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100
        print(f"  {col:28s}: {covered:,}/{total:,} ({pct:.1f}%)")

    for col, threshold in {
        "processo": 1.00,
        "display_name": 1.00,
        "coordinator": 0.99,
        "source_year": 0.99,
        "amount": 0.99,
        "start_date": 0.99,
        "instituicaoexecutora_nome": 0.99,
    }.items():
        coverage = df[col].notna().mean()
        if coverage < threshold and (full_run or col == "processo"):
            raise RuntimeError(f"Unexpectedly low {col} coverage: {coverage * 100:.1f}%")
        if coverage < threshold:
            print(f"  [WARN] limited-run {col} coverage is {coverage * 100:.1f}%; full run enforces {threshold * 100:.1f}%.")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(f"Full FAPEMIG run returned only {total:,} rows; expected at least {EXPECTED_MIN_FULL_ROWS:,}.")

    amounts = pd.to_numeric(df["amount"], errors="coerce")
    years = pd.to_numeric(df["source_year"], errors="coerce")
    print(f"  Year range: {int(years.min())}-{int(years.max())}")
    print(f"  Total BRL contracted amount: {amounts.sum():,.2f}")
    print(f"  Funding-type distribution: {df['funding_type'].value_counts(dropna=False).to_dict()}")
    print(f"  Top modalidades: {df['modalidade'].value_counts(dropna=False).head(10).to_dict()}")


def write_outputs(records: list[dict[str, str]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "fapemig_projetos_contratados_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote parsed source records to {raw_path}")

    parquet_path = output_dir / "fapemig_projetos_contratados.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
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

    prev_path = output_dir / "_prev_fapemig_projetos_contratados.parquet"
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
        print(f"\n[ERROR] Refusing to shrink FAPEMIG corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 5: Upload to S3")
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
        description="Download FAPEMIG open-data projects and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/fapemig"))
    parser.add_argument("--limit", type=int, default=None, help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached XLSX from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("FAPEMIG Projetos Contratados -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Source:     {DATA_PAGE_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.skip_download:
        print("\nStep 1: Reuse cached workbook")
        xlsx_path, xlsx_url = load_cached_workbook(args.output_dir)
    else:
        xlsx_path, xlsx_url = download_workbook(args.output_dir)

    records, parser_stats = parse_workbook(xlsx_path)
    df = normalize_records(records, xlsx_url, parser_stats, limit=args.limit)
    parquet_path = write_outputs(records, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
