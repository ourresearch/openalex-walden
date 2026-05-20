#!/usr/bin/env python3
"""
ANII Uruguay to S3 Data Pipeline (official project portal)
==========================================================

Downloads ANII-funded research project records from the official ANII
projects portal:

    https://anii.org.uy/proyectos/

The public page loads its table through ANII's own AJAX endpoint:

    https://anii.org.uy/app/ajax/frontend/getMoreProjects.php

The table exposes the project URL, beneficiary, broad support area, phase /
status, and call year. Per-project detail pages expose the fields needed for
the OpenAlex awards schema: native code, title, instrument, beneficiary,
department, subsidy amount/currency, start date, duration, call year, phase,
status, and public summary.

Scope
-----
This pipeline includes only rows whose broad area is INVESTIGACION. The ANII
portal also contains FORMACION, INNOVACION, and EMPRENDIMIENTOS rows, but those
include scholarships, training mobility, company/startup support, and other
non-research-award records. They are intentionally excluded here.

Source authority
----------------
anii.org.uy is the awarding body's own site. No PRISMA aggregate indicators,
Wikipedia, or third-party mirrors are used.

Output
------
s3://openalex-ingest/awards/anii/anii_projects.parquet

Usage
-----
    python scripts/local/anii_to_s3.py --skip-upload
    python scripts/local/anii_to_s3.py --skip-upload --limit 25
    python scripts/local/anii_to_s3.py --skip-download --skip-upload

Requirements
------------
    pip install pandas pyarrow requests

AWS CLI must be configured for uploads unless --skip-upload is used.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


PROJECTS_PAGE = "https://anii.org.uy/proyectos/"
PROJECTS_AJAX = "https://anii.org.uy/app/ajax/frontend/getMoreProjects.php"
SITE_BASE = "https://anii.org.uy"

FUNDER_ID = 4320310753
FUNDER_DISPLAY_NAME = "Agencia Nacional de Investigacion e Innovacion"
PROVENANCE = "anii_projects_portal"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/anii/anii_projects.parquet"

USER_AGENT = "openalex-walden-anii-ingest/1.0 (+https://openalex.org)"
REQUEST_DELAY_S = 0.15
MAX_RETRIES = 3

TABLE_FETCH_COUNT = 50_000


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_ascii(value: Optional[str]) -> str:
    if not value:
        return ""
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).upper()


def strip_comments(html: str) -> str:
    return re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)


def clean_html(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    raw = strip_comments(raw)
    raw = re.sub(r"<br\s*/?>", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = unescape(raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw or None


def slug_from_url(url: str) -> Optional[str]:
    match = re.search(r"/proyectos/([^/]+)/", url)
    return match.group(1) if match else None


def parse_amount(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Parse ANII subsidy strings like 'UYU 4000000' into amount/currency."""
    if not raw:
        return None, None
    text = clean_html(raw) or ""
    upper = text.upper()
    currency_match = re.search(r"\b([A-Z]{3})\b", upper)
    currency = currency_match.group(1) if currency_match else None
    number_match = re.search(r"(\d[\d\.,]*)", text)
    if not number_match:
        return None, currency
    number_text = number_match.group(1)
    # ANII examples observed in the full 2026-05-20 run were whole units with
    # no separators, but handle both Spanish thousands dots and decimal commas
    # so future formatted values do not silently turn 1.000.000 into NULL.
    if "," in number_text and "." in number_text:
        if number_text.rfind(",") > number_text.rfind("."):
            number_text = number_text.replace(".", "").replace(",", ".")
        else:
            number_text = number_text.replace(",", "")
    elif "," in number_text:
        parts = number_text.split(",")
        if len(parts[-1]) == 3 and all(len(part) == 3 for part in parts[1:]):
            number_text = "".join(parts)
        else:
            number_text = number_text.replace(",", ".")
    elif "." in number_text:
        parts = number_text.split(".")
        if len(parts[-1]) == 3 and all(len(part) == 3 for part in parts[1:]):
            number_text = "".join(parts)
    try:
        amount = float(number_text)
    except ValueError:
        return None, currency
    return amount, currency


def parse_start_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = clean_html(raw) or ""
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if not match:
        return None
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def parse_duration_months(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    text = clean_html(raw) or ""
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else None


def split_beneficiary(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """ANII usually publishes 'Person : Institution' in the beneficiary field."""
    text = clean_html(raw)
    if not text:
        return None, None
    parts = [p.strip() for p in re.split(r"\s+:\s+", text, maxsplit=1)]
    if len(parts) == 2:
        person, institution = parts
        return person or None, institution or None
    return None, text


def split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    tokens = full_name.strip().split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


class AniiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
        })
        self.last_request_at = 0.0

    def _sleep_if_needed(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - elapsed)

    def get(self, url: str, **kwargs) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            self._sleep_if_needed()
            try:
                resp = self.session.get(url, timeout=60, **kwargs)
                self.last_request_at = time.monotonic()
                resp.raise_for_status()
                return resp
            except Exception as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                log(f"  retry GET {url}: {exc}")
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def post(self, url: str, data: dict[str, str]) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            self._sleep_if_needed()
            try:
                resp = self.session.post(url, data=data, timeout=90)
                self.last_request_at = time.monotonic()
                resp.raise_for_status()
                return resp
            except Exception as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                log(f"  retry POST {url}: {exc}")
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")


def parse_table_rows(table_html: str) -> list[dict[str, Optional[str]]]:
    rows: list[dict[str, Optional[str]]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL | re.IGNORECASE):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.DOTALL | re.IGNORECASE)
        if len(cells) < 5:
            continue
        hrefs = re.findall(r'href="([^"]+)"', cells[0])
        detail_url = hrefs[0] if hrefs else None
        parsed = {
            "detail_url": detail_url,
            "source_project_code": slug_from_url(detail_url or ""),
            "table_title": clean_html(cells[0]),
            "table_beneficiary": clean_html(cells[1]),
            "table_area": clean_html(cells[2]),
            "table_phase_status": clean_html(cells[3]),
            "table_call_year": clean_html(cells[4]),
        }
        rows.append(parsed)
    return rows


def fetch_project_index(client: AniiClient) -> list[dict[str, Optional[str]]]:
    log("=" * 60)
    log("Step 1a: Download ANII project index")
    payload = {
        "offset": "0",
        "count": str(TABLE_FETCH_COUNT),
        "order": "anioconvocatoria",
        "criteria": "DESC",
        "qry": "",
        "departamento": "",
        "estado": "",
        "fase": "",
        "area": "",
        "instrumento": "",
        "sector": "",
    }
    resp = client.post(PROJECTS_AJAX, data=payload)
    data = resp.json()
    total = int(data.get("total") or 0)
    num_rows = int(data.get("numRows") or 0)
    rows = parse_table_rows(data.get("html") or "")
    log(f"  endpoint total={total:,} numRows={num_rows:,} parsed={len(rows):,}")
    if total and len(rows) != total:
        raise RuntimeError(
            f"Expected to parse {total:,} table rows from ANII endpoint, got {len(rows):,}"
        )
    research_rows = [
        row for row in rows
        if normalize_ascii(row.get("table_area")) == "INVESTIGACION"
    ]
    log(f"  research rows (INVESTIGACION): {len(research_rows):,}")
    if not research_rows:
        raise RuntimeError("No INVESTIGACION rows found; ANII table shape may have changed.")
    return research_rows


def parse_detail_page(html: str) -> dict[str, Optional[str]]:
    html = strip_comments(html)
    title_match = re.search(r"<h3>(.*?)<span class=\"ornament\"", html, flags=re.DOTALL)
    title = clean_html(title_match.group(1)) if title_match else None

    detail: dict[str, Optional[str]] = {"title": title}
    # Visible label/value fields inside the two content_details lists.
    for match in re.finditer(
        r'<span class="text">\s*<span>(.*?)</span>\s*(.*?)</span>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        label = normalize_ascii(clean_html(match.group(1)))
        value = clean_html(match.group(2))
        if label:
            detail[label] = value

    summary_match = re.search(
        r'RESUMEN PUBLICABLE</span></div>\s*<p>(.*?)</p>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    detail["summary"] = clean_html(summary_match.group(1)) if summary_match else None
    return detail


def enrich_details(
    client: AniiClient,
    rows: list[dict[str, Optional[str]]],
    output_dir: Path,
    limit: Optional[int],
) -> Path:
    log("=" * 60)
    log("Step 1b: Fetch ANII project detail pages")
    if limit:
        log(f"  limit set: fetching first {limit:,} of {len(rows):,} research rows")
        rows = rows[:limit]

    records: list[dict[str, Optional[str]]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    for i, row in enumerate(rows, start=1):
        url = row.get("detail_url")
        if not url:
            raise RuntimeError(f"Missing detail URL for row: {row}")
        resp = client.get(url)
        detail = parse_detail_page(resp.text)

        subsidy_raw = detail.get("SUBSIDIO")
        amount, currency = parse_amount(subsidy_raw)
        beneficiary_raw = detail.get("BENEFICIARIO") or row.get("table_beneficiary")
        lead_full_name, institution = split_beneficiary(beneficiary_raw)
        given, family = split_name(lead_full_name)

        project_code = detail.get("CODIGO") or row.get("source_project_code")
        if not project_code:
            raise RuntimeError(f"Missing ANII project code for {url}")

        record = {
            **row,
            "funder_award_id": f"anii-{project_code.lower()}",
            "project_code": project_code,
            "title": detail.get("title") or row.get("table_title"),
            "description": detail.get("summary"),
            "instrument": detail.get("INSTRUMENTO"),
            "beneficiary": beneficiary_raw,
            "beneficiary_person": lead_full_name,
            "beneficiary_institution": institution,
            "beneficiary_department": detail.get("DEPARTAMENTO"),
            "subsidy_raw": subsidy_raw,
            "amount": amount,
            "currency": currency,
            "start_date": parse_start_date(detail.get("FECHA DE INICIO")),
            "duration_raw": detail.get("DURACION"),
            "duration_months": parse_duration_months(detail.get("DURACION")),
            "call_year": detail.get("ANO CONVOCATORIA") or row.get("table_call_year"),
            "phase": detail.get("FASE"),
            "status": detail.get("ESTADO"),
            "lead_given_name": given,
            "lead_family_name": family,
            "source_url": url,
            "downloaded_at": fetched_at,
        }
        records.append(record)

        if i % 100 == 0 or i == len(rows):
            amount_count = sum(1 for r in records if r.get("amount") is not None)
            desc_count = sum(1 for r in records if r.get("description"))
            start_count = sum(1 for r in records if r.get("start_date"))
            log(
                f"  [{i:,}/{len(rows):,}] amount={amount_count:,} "
                f"start_date={start_count:,} description={desc_count:,}"
            )

    raw_path = output_dir / "anii_projects_raw.json"
    raw_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"  cached raw detail records: {raw_path}")
    return raw_path


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    log("=" * 60)
    log("Step 2: Build flat DataFrame")
    records = json.loads(raw_path.read_text(encoding="utf-8"))
    df = pd.DataFrame.from_records(records)
    log(f"  DataFrame rows={len(df):,} cols={len(df.columns):,}")
    if df.empty:
        raise RuntimeError("ANII dataframe is empty.")

    dup = df["funder_award_id"].duplicated(keep=False)
    if dup.any():
        sample = df.loc[dup, ["funder_award_id", "title", "source_url"]].head(20)
        raise RuntimeError(f"Duplicate funder_award_id values found:\n{sample}")

    if (df["title"].isna() | (df["title"].astype(str).str.len() == 0)).any():
        raise RuntimeError("At least one ANII research row lacks title.")

    amount_count = df["amount"].notna().sum()
    currency_count = df["currency"].notna().sum()
    log(
        f"  coverage title={df['title'].notna().sum():,}/{len(df):,}; "
        f"amount={amount_count:,}/{len(df):,}; currency={currency_count:,}/{len(df):,}; "
        f"start_date={df['start_date'].notna().sum():,}/{len(df):,}; "
        f"description={df['description'].notna().sum():,}/{len(df):,}"
    )
    if amount_count:
        amounts = pd.to_numeric(df["amount"], errors="coerce")
        log(
            f"  amount range: min={amounts.min():,.0f} "
            f"max={amounts.max():,.0f} avg={amounts.mean():,.0f}"
        )
    log("  currency counts:")
    log(str(df["currency"].fillna("NULL").value_counts().head(10)))

    # Runbook requirement: cast all raw/source columns to string before parquet.
    # Databricks casts amount/date/duration fields back to the target types.
    df = df.astype("string")
    return df


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    log("=" * 60)
    log("Step 3: Write parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "anii_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log(f"  wrote {len(df):,} rows to {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
    return parquet_path


def upload_to_s3(parquet_path: Path) -> None:
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log("=" * 60)
    log(f"Step 4: Upload to {s3_uri}")
    subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    log("  upload complete")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download ANII Uruguay research projects to parquet/S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/anii"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse anii_projects_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally only")
    parser.add_argument("--limit", type=int, default=None,
                        help="Fetch only first N research rows for smoke testing")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    log("=" * 60)
    log("ANII Uruguay awards pipeline starting")
    log(f"  funder_id: F{FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  source:    {PROJECTS_PAGE}")
    log(f"  s3:        s3://{S3_BUCKET}/{S3_KEY}")
    log(f"  output:    {args.output_dir}")

    raw_path = args.output_dir / "anii_projects_raw.json"
    if args.skip_download:
        if not raw_path.exists():
            log(f"[ERROR] --skip-download set but {raw_path} does not exist")
            sys.exit(2)
        log(f"  reusing cached raw file: {raw_path}")
    else:
        client = AniiClient()
        index_rows = fetch_project_index(client)
        raw_path = enrich_details(client, index_rows, args.output_dir, args.limit)

    df = build_dataframe(raw_path)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        log("--skip-upload set; manual upload command:")
        log(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        upload_to_s3(parquet_path)

    log("=" * 60)
    log("ANII pipeline complete.")
    log("Next: run notebooks/awards/CreateANIIAwards.ipynb in Databricks.")


if __name__ == "__main__":
    main()
