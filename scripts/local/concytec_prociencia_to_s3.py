#!/usr/bin/env python3
"""
CONCYTEC/PROCIENCIA (Peru) to S3 (GRANT PATTERN)
=================================================

Pulls public subsidy/project records from PROCIENCIA's official
Observatorio de Subvenciones:

    https://observatorio.prociencia.gob.pe/subvenciones

The observatory is an official CONCYTEC/PROCIENCIA public portal announced
by gob.pe in March/April 2026 as open, real-time access to Peruvian
state-funded science, technology, and innovation projects from 2011 onward.
It currently exposes 4,694 subsidy/project records on the list page, with
detail pages at `/DetalleSubvencion/{id}`.

Output:
    s3://openalex-ingest/awards/concytec_prociencia/concytec_prociencia_projects.parquet

OpenAlex funder:
    F4320326614 - Consejo Nacional de Ciencia, Tecnologia e Innovacion Tecnologica
    ROR: https://ror.org/05c7j7r25
    DOI: 10.13039/501100010747

Source / mapping notes:
    - The durable source key is the observatory detail-page numeric id.
      The displayed "Numero de Subvencion" / project code is preserved as
      `project_code`, but it is not used alone as the OpenAlex
      `funder_award_id` because source codes can include promotions and
      variants that are not guaranteed globally unique.
    - Amounts are published as Peruvian soles, e.g. "S/ 1,939,475.00".
      The notebook maps parsed `amount` to currency `PEN`.
    - Lead investigator is the displayed "Lider de Proyecto". The source
      generally presents names as "FAMILY ,GIVEN". The notebook performs a
      best-effort split while preserving the full source name in raw data.
    - All parquet columns are strings by design (`df.astype("string")`) to
      avoid pandas/pyarrow type inference problems with null-heavy text
      fields. Databricks casts amount/date/year fields explicitly.

Run examples:
    python scripts/local/concytec_prociencia_to_s3.py --skip-upload --limit-ids 25
    python scripts/local/concytec_prociencia_to_s3.py --skip-upload --workers 8

The full crawl scans numeric detail ids until it has found the advertised
total from the list page, or until `--max-id` is reached. It writes
checkpoint files so interrupted runs can resume without refetching already
processed ids.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

BASE_URL = "https://observatorio.prociencia.gob.pe"
LIST_URL = f"{BASE_URL}/subvenciones"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/DetalleSubvencion/{{source_detail_id}}"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/concytec_prociencia/concytec_prociencia_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Detail ids are sparse. The first valid block is under 1,000, but the list
# page also exposes valid ids above 8,000; keep the default ceiling generous
# and stop by the advertised total instead of an assumed contiguous range.
DEFAULT_MAX_ID = 100_000
DEFAULT_WORKERS = 8
REQUEST_TIMEOUT = (10, 45)
MAX_RETRIES = 3
_THREAD_LOCAL = threading.local()

SECTION_LABELS = {
    "general": {
        "Numero de Subvención": "project_code",
        "Líder de Proyecto": "lead_investigator_name",
        "Organización / Afiliación": "lead_investigator_affiliation",
    },
    "descripcion": {
        "Resumen": "summary",
        "Objetivo General": "objective",
        "Palabras Clave": "keywords",
    },
    "intervencion": {
        "Intervención": "intervention_type",
        "Convenio": "agreement",
        "Concurso": "call",
    },
    "grado": {
        "Grado Académico": "degree_level",
        "Nombre del Grado": "degree_name",
    },
    "ejecutora": {
        "Nombre de la Organización": "executing_organization",
        "Dependencia / Unidad": "executing_unit",
        "Región": "executing_org_region",
        "Tipo de organización": "executing_org_type",
        "País": "executing_org_country",
    },
    "adicional": {
        "Área / Subárea OCDE": "oecd_area_subarea",
        "País ejecución": "execution_country",
        "Región ejecución": "execution_region",
        "Periodo de Ejecución": "execution_period",
        "Disciplina OCDE": "oecd_discipline",
    },
    "financiamiento": {
        "Monto total": "amount_text",
    },
}

COUNT_SECTIONS = {
    "equipo": "team_members_count",
    "publicaciones": "publications_count",
    "propiedad": "intellectual_property_count",
    "tesis": "theses_count",
    "presentaciones": "oral_presentations_count",
    "pasantias": "internships_count",
    "similares": "similar_projects_count",
}


@dataclass
class CrawlState:
    rows: dict[int, dict]
    invalid_ids: set[int]


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    if not value or value == "No se encontraron resultados.":
        return None
    return value


def remove_accents_for_key(value: str) -> str:
    """Small, explicit normalizer for comparing Spanish UI labels."""
    replacements = str.maketrans(
        "áéíóúÁÉÍÓÚñÑüÜ",
        "aeiouAEIOUnNuU",
    )
    return value.translate(replacements)


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"\D+", "", value)
    return int(digits) if digits else None


def parse_amount(amount_text: str | None) -> str | None:
    if not amount_text:
        return None
    cleaned = amount_text.replace("S/", "").replace(",", "").strip()
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    return match.group(0)


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_execution_dates(execution_period: str | None) -> tuple[str | None, str | None]:
    if not execution_period:
        return None, None
    dates = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", execution_period)
    start_date = parse_date(dates[0]) if len(dates) >= 1 else None
    end_date = parse_date(dates[1]) if len(dates) >= 2 else None
    return start_date, end_date


def extract_year(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"(20\d{2}|19\d{2})", value)
        if match:
            return match.group(1)
    return None


def card_value(card) -> tuple[str | None, str | None, str | None]:
    label_el = card.select_one(".det-card-label")
    value_el = card.select_one(".det-card-value")
    label = clean_text(label_el.get_text(" ", strip=True) if label_el else None)
    value = clean_text(value_el.get_text(" ", strip=True) if value_el else None)
    href = None
    if value_el:
        link = value_el.find("a", href=True)
        if link:
            href = link["href"]
    return label, value, href


def extract_section_cards(soup: BeautifulSoup, section_id: str) -> list:
    header = soup.find(id=section_id)
    if not header:
        return []
    row = header.find_next_sibling("div", class_="det-cards-row")
    if not row:
        return []
    return row.select(".det-info-card")


def extract_section_total(soup: BeautifulSoup, section_id: str) -> str | None:
    cards = extract_section_cards(soup, section_id)
    if not cards:
        return None
    text = clean_text(cards[0].get_text(" ", strip=True))
    if not text:
        return None
    if "No se encontraron resultados" in text:
        return "0"
    match = re.search(r"Total:\s*([\d,.]+)", text)
    if match:
        return str(parse_int(match.group(1)) or 0)
    return None


def fetch_url(url: str) -> requests.Response:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        _THREAD_LOCAL.session = session

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            return response
        except Exception as exc:  # noqa: BLE001 - retry boundary logs all request exceptions
            last_error = exc
            if attempt < MAX_RETRIES:
                sleep_s = 1.5 * attempt
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {last_error}")


def discover_list_metadata() -> tuple[int | None, int | None, list[int]]:
    log(f"Fetching list page: {LIST_URL}")
    response = fetch_url(LIST_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    total_match = re.search(r"Total:\s*([\d,.]+)", text)
    page_total = parse_int(total_match.group(1)) if total_match else None

    ids = []
    for link in soup.find_all("a", href=True):
        match = re.search(r"/DetalleSubvencion/(\d+)", link["href"])
        if match:
            ids.append(int(match.group(1)))
    max_seen = max(ids) if ids else None
    log(
        "List smoke test: "
        f"advertised_total={page_total}, first_page_detail_ids={len(ids)}, "
        f"max_first_page_id={max_seen}"
    )
    return page_total, max_seen, sorted(set(ids))


def parse_detail(source_detail_id: int, html: str, downloaded_at: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    if f"No se encontró la subvención con ID {source_detail_id}" in page_text:
        return None

    title = clean_text(soup.select_one(".det-title").get_text(" ", strip=True) if soup.select_one(".det-title") else None)
    if not title:
        return None

    row: dict[str, str | None] = {
        "source_detail_id": str(source_detail_id),
        "source_url": DETAIL_URL_TEMPLATE.format(source_detail_id=source_detail_id),
        "landing_page_url": DETAIL_URL_TEMPLATE.format(source_detail_id=source_detail_id),
        "display_name": title,
        "downloaded_at": downloaded_at,
    }

    subtitle = clean_text(soup.select_one(".det-subtitle").get_text(" ", strip=True) if soup.select_one(".det-subtitle") else None)
    if subtitle:
        row["project_code_from_header"] = clean_text(re.sub(r"^C[ÓO]DIGO DE PROYECTO\s*#?", "", subtitle, flags=re.I))

    for section_id, labels in SECTION_LABELS.items():
        for card in extract_section_cards(soup, section_id):
            label, value, href = card_value(card)
            if not label:
                continue
            for source_label, dest_col in labels.items():
                if remove_accents_for_key(label).lower() == remove_accents_for_key(source_label).lower():
                    row[dest_col] = value
                    if href:
                        row[f"{dest_col}_href"] = href if href.startswith("http") else f"{BASE_URL}{href}"
                    break

    for section_id, dest_col in COUNT_SECTIONS.items():
        row[dest_col] = extract_section_total(soup, section_id)

    # Prefer the explicit card value over the header copy, but keep both.
    if not row.get("project_code"):
        row["project_code"] = row.get("project_code_from_header")

    amount_text = row.get("amount_text")
    row["amount"] = parse_amount(amount_text)
    row["currency"] = "PEN" if row.get("amount") else None

    start_date, end_date = parse_execution_dates(row.get("execution_period"))
    row["start_date"] = start_date
    row["end_date"] = end_date
    row["start_year"] = extract_year(start_date, row.get("project_code"), row.get("call"))
    row["end_year"] = extract_year(end_date)

    row["slug"] = f"prociencia-{source_detail_id}"
    return row


def is_not_found_page(source_detail_id: int, html: str) -> bool:
    # The raw HTML entity-encodes the accented "o" in "encontró" on some
    # pages, so match the stable prefix plus the id instead of the fully
    # rendered Spanish sentence.
    return "No se encontr" in html and f"ID {source_detail_id}" in html


def fetch_detail(source_detail_id: int, downloaded_at: str) -> tuple[int, dict | None, bool, str | None]:
    url = DETAIL_URL_TEMPLATE.format(source_detail_id=source_detail_id)
    try:
        response = fetch_url(url)
        if response.status_code != 200:
            return source_detail_id, None, False, f"http_{response.status_code}"
        if is_not_found_page(source_detail_id, response.text):
            return source_detail_id, None, True, None
        parsed = parse_detail(source_detail_id, response.text, downloaded_at)
        if parsed is None:
            return source_detail_id, None, False, "parse_empty"
        return source_detail_id, parsed, False, None
    except Exception as exc:  # noqa: BLE001 - caller records id-level crawl failures
        return source_detail_id, None, False, str(exc)


def load_checkpoint(rows_path: Path, invalid_path: Path) -> CrawlState:
    rows: dict[int, dict] = {}
    invalid_ids: set[int] = set()
    if rows_path.exists():
        for line in rows_path.read_text().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            rows[int(row["source_detail_id"])] = row
    if invalid_path.exists():
        for line in invalid_path.read_text().splitlines():
            if line.strip().isdigit():
                invalid_ids.add(int(line.strip()))
    return CrawlState(rows=rows, invalid_ids=invalid_ids)


def append_jsonl(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def append_invalid(path: Path, source_detail_id: int) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{source_detail_id}\n")


def id_batches(ids: Iterable[int], batch_size: int) -> Iterable[list[int]]:
    batch: list[int] = []
    for source_detail_id in ids:
        batch.append(source_detail_id)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def crawl_details(
    ids_to_scan: list[int],
    rows_path: Path,
    invalid_path: Path,
    state: CrawlState,
    expected_total: int | None,
    workers: int,
    downloaded_at: str,
    stop_after_total: bool,
) -> dict[int, dict]:
    attempted = len(state.rows) + len(state.invalid_ids)
    error_count = 0
    start_time = time.monotonic()

    for batch in id_batches(ids_to_scan, batch_size=max(workers * 10, 50)):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(fetch_detail, source_detail_id, downloaded_at): source_detail_id
                for source_detail_id in batch
            }
            for future in as_completed(futures):
                source_detail_id, row, is_invalid, error = future.result()
                attempted += 1
                if row:
                    if source_detail_id not in state.rows:
                        state.rows[source_detail_id] = row
                        append_jsonl(rows_path, row)
                elif is_invalid:
                    if source_detail_id not in state.invalid_ids:
                        state.invalid_ids.add(source_detail_id)
                        append_invalid(invalid_path, source_detail_id)
                else:
                    if error:
                        error_count += 1
                        if error_count <= 10 or error_count % 100 == 0:
                            log(f"Detail id {source_detail_id} had fetch/parse issue: {error}")

        elapsed = max(time.monotonic() - start_time, 1.0)
        rate = max((attempted - len(state.rows) - len(state.invalid_ids)) / elapsed, 0.0)
        expected_msg = f"/{expected_total:,}" if expected_total else ""
        log(
            f"Scanned through id {batch[-1]:,}; valid={len(state.rows):,}{expected_msg}, "
            f"invalid={len(state.invalid_ids):,}, batch_done={len(batch):,}, elapsed={elapsed/60:.1f}m"
        )

        if (
            stop_after_total
            and expected_total
            and len(state.rows) >= expected_total
        ):
            log(f"Reached advertised total ({expected_total:,}); stopping crawl.")
            break

    return state.rows


def build_dataframe(rows: dict[int, dict]) -> pd.DataFrame:
    df = pd.DataFrame([rows[k] for k in sorted(rows)])
    if df.empty:
        raise RuntimeError("No valid PROCIENCIA detail records were fetched.")

    dup_mask = df["slug"].duplicated(keep=False) & df["slug"].notna()
    if dup_mask.any():
        log("FATAL: duplicate slugs detected:")
        log(str(df.loc[dup_mask, ["source_detail_id", "project_code", "display_name", "slug"]]))
        raise RuntimeError(
            f"{int(dup_mask.sum())} rows have duplicate funder_award_id slugs; "
            "fix the slug rule before shipping."
        )

    # All source columns are strings. Cast explicitly in the notebook.
    return df.astype("string")


def shrink_guard(output_path: Path, new_count: int, allow_shrink: bool) -> None:
    if not output_path.exists():
        return
    old_count = len(pd.read_parquet(output_path))
    if old_count and new_count < old_count * 0.9 and not allow_shrink:
        raise RuntimeError(
            f"Shrink guard: new output has {new_count:,} rows, existing local parquet "
            f"has {old_count:,}. Re-run with --allow-shrink if this drop is intentional."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="CONCYTEC/PROCIENCIA observatory -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("/tmp/concytec_prociencia_checkpoint"))
    parser.add_argument("--skip-upload", action="store_true", help="Write parquet locally only; skip S3 upload")
    parser.add_argument("--max-id", type=int, default=DEFAULT_MAX_ID, help="Highest detail id to probe if total is not reached")
    parser.add_argument("--limit-ids", type=int, default=None, help="Smoke-test mode: only scan this many numeric ids")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("CONCYTEC/PROCIENCIA (Peru) -> S3 starting")
    log(f"List source: {LIST_URL}")
    log(f"Output: s3://{S3_BUCKET}/{S3_KEY}")

    expected_total, max_first_page_id, first_page_ids = discover_list_metadata()
    if not expected_total or expected_total < 100:
        raise RuntimeError(f"Unexpected advertised total from list page: {expected_total}")
    if not first_page_ids:
        raise RuntimeError("List page smoke test found no detail ids.")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    rows_path = args.checkpoint_dir / "concytec_prociencia_rows.jsonl"
    invalid_path = args.checkpoint_dir / "concytec_prociencia_invalid_ids.txt"

    if args.reset_checkpoint:
        rows_path.unlink(missing_ok=True)
        invalid_path.unlink(missing_ok=True)
        log("Reset checkpoint files.")

    state = load_checkpoint(rows_path, invalid_path)
    log(f"Loaded checkpoint: valid={len(state.rows):,}, invalid={len(state.invalid_ids):,}")

    # The first page currently exposes high detail ids, but the official total
    # is the real stop condition. Scan from 1 upward until the total is reached.
    max_id = args.max_id
    if max_first_page_id:
        max_id = max(max_id, max_first_page_id + 250)
    all_ids = list(range(1, max_id + 1))
    if args.limit_ids:
        all_ids = all_ids[: args.limit_ids]
        log(f"Smoke mode: limiting scan to first {len(all_ids):,} numeric detail ids")

    already_seen = set(state.rows) | state.invalid_ids
    ids_to_scan = [source_detail_id for source_detail_id in all_ids if source_detail_id not in already_seen]
    log(f"Need to scan {len(ids_to_scan):,} ids (max_id={max_id:,}, workers={args.workers})")

    downloaded_at = datetime.now(timezone.utc).isoformat()
    crawl_details(
        ids_to_scan=ids_to_scan,
        rows_path=rows_path,
        invalid_path=invalid_path,
        state=state,
        expected_total=expected_total,
        workers=max(1, args.workers),
        downloaded_at=downloaded_at,
        stop_after_total=args.limit_ids is None,
    )

    if args.limit_ids is None and expected_total and len(state.rows) < expected_total:
        raise RuntimeError(
            f"Crawl ended with {len(state.rows):,} valid rows, below the advertised "
            f"source total {expected_total:,}. Do not upload partial data; resume the "
            "checkpoint or increase --max-id."
        )

    df = build_dataframe(state.rows)
    output_path = args.output_dir / "concytec_prociencia_projects.parquet"
    shrink_guard(output_path, len(df), args.allow_shrink)
    df.to_parquet(output_path, index=False)

    amount_nonnull = df["amount"].notna().sum() if "amount" in df else 0
    date_nonnull = df["start_date"].notna().sum() if "start_date" in df else 0
    org_nonnull = df["executing_organization"].notna().sum() if "executing_organization" in df else 0
    log(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")
    log(
        "Coverage: "
        f"rows={len(df):,}, amount={amount_nonnull:,}, start_date={date_nonnull:,}, "
        f"executing_org={org_nonnull:,}, min_year={df['start_year'].min()}, max_year={df['start_year'].max()}"
    )

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    boto3.client("s3").upload_file(str(output_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(130)
