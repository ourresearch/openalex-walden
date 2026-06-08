#!/usr/bin/env python3
"""
MBIE Who Got Funded -> S3 Pipeline
==================================

Downloads science and innovation contract records from the New Zealand Ministry
of Business, Innovation and Employment (MBIE) "Who got funded" workbook.

Source authority
----------------
MBIE publishes the workbook on its own website:

    https://www.mbie.govt.nz/science-and-technology/science-and-innovation/
    funding-information-and-opportunities/investment-funds/who-got-funded/

The page describes the spreadsheet as the list of grants for scientific
research and associated activities funded by MBIE since 2015. It is refreshed
monthly, includes active/matured/terminated contracts, and explicitly excludes
grants made by the Royal Society Te Aparangi, Health Research Council, and
Callaghan Innovation, which have separate sources.

The workbook caveats say a source-unique row key can be created from
Appropriation + Contract ID + Organisation. In practice, repeated Contract IDs
are the same project split across multiple appropriations (identical title,
organisation, round, status, start and end dates). This exporter therefore
aggregates to one OpenAlex award per Contract ID so the citable MBIE grant ID is
preserved in funder_award_id, while keeping the appropriation split in JSON
audit columns.

OpenAlex funder
---------------
F4320321983 - Ministry of Business, Innovation and Employment (NZ)
ROR: https://ror.org/02jtq1b51
DOI: 10.13039/501100003524

Output
------
s3://openalex-ingest/awards/mbie/mbie_projects.parquet

Usage
-----
    python scripts/local/mbie_to_s3.py --limit 10 --skip-upload
    python scripts/local/mbie_to_s3.py --skip-upload
    python scripts/local/mbie_to_s3.py
    python scripts/local/mbie_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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


SOURCE_PAGE_URL = (
    "https://www.mbie.govt.nz/science-and-technology/science-and-innovation/"
    "funding-information-and-opportunities/investment-funds/who-got-funded/"
)
WORKBOOK_URL = (
    "https://www.mbie.govt.nz/assets/Data-Files/Science-technology/"
    "science-innovation/who-got-funded.xlsx"
)
DATA_SHEET = "MBIE SSIP Contract List"

FUNDER_ID = 4320321983
FUNDER_DISPLAY_NAME = "Ministry of Business, Innovation and Employment"
PROVENANCE = "mbie_who_got_funded"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mbie/mbie_projects.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp")
WORKBOOK_FILENAME = "mbie_who_got_funded.xlsx"
PARQUET_FILENAME = "mbie_projects.parquet"
EXPECTED_MIN_FULL_ROWS = 2500
REQUEST_TIMEOUT = (10, 120)

HEADERS = {
    "User-Agent": "openalex-walden-mbie/1.0 (+https://openalex.org)",
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
}

EXPECTED_COLUMNS = [
    "Appropriation",
    "Contract ID",
    "Project Title",
    "Investment round Title",
    "Organisation",
    "Start Date",
    "End Date",
    "Status",
    "All years GST excl.",
    "Public Statement",
]
FISCAL_YEAR_RE = re.compile(r"^20\d{2}/\d{2}$")


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = html.unescape(str(value))
    text = re.sub(r"[\u00a0 \t\r\n]+", " ", text).strip()
    return text or None


def parse_amount(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.replace(",", "").replace("$", "").strip()
        if not value:
            return None
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def parse_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def year_of(date_str: str | None) -> str | None:
    if not date_str:
        return None
    m = re.match(r"(\d{4})", date_str)
    if not m:
        return None
    year = int(m.group(1))
    return str(year) if 1800 <= year <= 2100 else None


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def download_workbook(output_dir: Path, skip_download: bool) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    workbook_path = output_dir / WORKBOOK_FILENAME
    if skip_download:
        if not workbook_path.exists():
            raise FileNotFoundError(
                f"--skip-download set but {workbook_path} does not exist. "
                "Run once without --skip-download first."
            )
        log(f"Using existing workbook {workbook_path}")
        return workbook_path

    log(f"Downloading official MBIE workbook: {WORKBOOK_URL}")
    response = requests.get(WORKBOOK_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    workbook_path.write_bytes(response.content)
    log(f"Wrote {workbook_path} ({workbook_path.stat().st_size:,} bytes)")
    return workbook_path


def load_source_rows(workbook_path: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook_path, sheet_name=DATA_SHEET)
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise RuntimeError(f"Workbook is missing expected columns: {missing}")
    df = df.dropna(how="all").copy()
    for col in [
        "Appropriation",
        "Contract ID",
        "Project Title",
        "Investment round Title",
        "Organisation",
        "Status",
        "Public Statement",
    ]:
        df[col] = df[col].map(clean_text)
    df["start_date"] = df["Start Date"].map(parse_date)
    df["end_date"] = df["End Date"].map(parse_date)
    df["amount_component"] = df["All years GST excl."].map(parse_amount)
    df["source_composite_id"] = df.apply(
        lambda row: "|".join(
            [
                clean_text(row.get("Appropriation")) or "",
                clean_text(row.get("Contract ID")) or "",
                clean_text(row.get("Organisation")) or "",
            ]
        ),
        axis=1,
    )
    return df


def unique_non_null(values: pd.Series) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def first_non_null(values: pd.Series) -> str | None:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return None


def assert_contract_groups_consistent(df: pd.DataFrame) -> None:
    check_cols = [
        "Project Title",
        "Investment round Title",
        "Organisation",
        "Start Date",
        "End Date",
        "Status",
    ]
    bad: list[str] = []
    for contract_id, group in df.groupby("Contract ID", dropna=False):
        for col in check_cols:
            if group[col].map(clean_text).nunique(dropna=True) > 1:
                bad.append(f"{contract_id}: {col}")
    if bad:
        sample = "; ".join(bad[:10])
        raise RuntimeError(
            "Repeated Contract IDs are not consistent project splits; "
            f"cannot aggregate safely. Examples: {sample}"
        )


def build_awards_dataframe(source_df: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    if source_df["Contract ID"].isna().any():
        raise RuntimeError("Contract ID is required for every MBIE source row.")
    if source_df["source_composite_id"].duplicated().any():
        dupes = source_df[source_df["source_composite_id"].duplicated(keep=False)]
        raise RuntimeError(f"Source composite key is not unique; first duplicate rows:\n{dupes.head()}")
    assert_contract_groups_consistent(source_df)

    fiscal_cols = [col for col in source_df.columns if FISCAL_YEAR_RE.match(str(col))]
    now = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for contract_id, group in source_df.groupby("Contract ID", sort=True, dropna=False):
        if limit is not None and len(records) >= limit:
            break
        title = first_non_null(group["Project Title"])
        organisation = first_non_null(group["Organisation"])
        if not contract_id or not title or not organisation:
            raise RuntimeError(f"Missing required title/organisation for contract {contract_id!r}")

        positive_components = [
            {"appropriation": clean_text(row["Appropriation"]), "amount": float(row["amount_component"])}
            for _, row in group.iterrows()
            if row.get("amount_component") is not None and not pd.isna(row.get("amount_component"))
        ]
        total_amount = sum(item["amount"] for item in positive_components)
        amount: str | None = f"{total_amount:.2f}" if total_amount > 0 else None

        annual_amounts: dict[str, float] = {}
        for col in fiscal_cols:
            vals = group[col].map(parse_amount)
            total = sum(v for v in vals if v is not None)
            if total > 0:
                annual_amounts[str(col)] = round(total, 2)

        source_composites = sorted(group["source_composite_id"].dropna().astype(str).tolist())
        appropriations = unique_non_null(group["Appropriation"])
        public_statements = unique_non_null(group["Public Statement"])
        description = public_statements[0] if public_statements else None

        start_date = parse_date(first_non_null(group["Start Date"]))
        end_date = parse_date(first_non_null(group["End Date"]))
        record = {
            "funder_award_id": clean_text(contract_id),
            "contract_id": clean_text(contract_id),
            "display_name": title,
            "description": description,
            "funder_scheme": first_non_null(group["Investment round Title"]),
            "appropriations_json": json_dumps(appropriations),
            "organisation": organisation,
            "status": first_non_null(group["Status"]),
            "amount": amount,
            "currency": "NZD" if amount is not None else None,
            "amount_components_json": json_dumps(positive_components),
            "annual_amounts_json": json_dumps(annual_amounts),
            "start_date": start_date,
            "end_date": end_date,
            "start_year": year_of(start_date),
            "end_year": year_of(end_date),
            "source_row_count": str(len(group)),
            "source_composite_ids_json": json_dumps(source_composites),
            "source_page_url": SOURCE_PAGE_URL,
            "source_workbook_url": WORKBOOK_URL,
            "provenance": PROVENANCE,
            "funder_id": str(FUNDER_ID),
            "funder_display_name": FUNDER_DISPLAY_NAME,
            "downloaded_at": now,
        }
        records.append(record)

    return pd.DataFrame(records)


def validate(df: pd.DataFrame, source_rows: int) -> None:
    n = len(df)
    log(f"Local validation: {n:,} unique MBIE contract awards from {source_rows:,} workbook rows")
    if n < EXPECTED_MIN_FULL_ROWS:
        log(f"  WARNING: row count {n:,} is below expected full-run floor {EXPECTED_MIN_FULL_ROWS:,}")

    def pct(col: str) -> None:
        count = int(df[col].notna().sum())
        pct_value = 100.0 * count / n if n else 0.0
        log(f"  {col:<24}{count:>7}/{n:<7} ({pct_value:5.1f}%)")

    for col in [
        "funder_award_id",
        "display_name",
        "description",
        "organisation",
        "amount",
        "currency",
        "start_date",
        "end_date",
        "funder_scheme",
        "status",
    ]:
        pct(col)

    distinct_ids = df["funder_award_id"].nunique(dropna=False)
    log(f"  unique funder_award_id {distinct_ids:,}/{n:,}")
    if distinct_ids != n:
        raise RuntimeError("funder_award_id is not unique after Contract ID aggregation.")

    years = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(years):
        log(f"  start_year range {int(years.min())}-{int(years.max())}")
    end_years = pd.to_numeric(df["end_year"], errors="coerce").dropna()
    if len(end_years):
        log(f"  end_year range {int(end_years.min())}-{int(end_years.max())}")
    amounts = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amounts):
        log(
            f"  amount range NZD {amounts.min():,.0f}-{amounts.max():,.0f}; "
            f"total NZD {amounts.sum():,.0f}"
        )

    log("  status distribution:")
    for status, count in df["status"].fillna("(missing)").value_counts().head(12).items():
        log(f"    {status}: {count}")
    log("  top funder_scheme values:")
    for scheme, count in df["funder_scheme"].fillna("(missing)").value_counts().head(12).items():
        log(f"    {scheme}: {count}")


def check_no_shrink(local_path: Path, allow_shrink: bool, client: Any) -> None:
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook section 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        previous_path = local_path.with_suffix(".previous.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
        previous_rows = len(pd.read_parquet(previous_path))
        log(f"  previous S3 rows={previous_rows:,}, new rows={new_rows:,}")
        if new_rows < previous_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {previous_rows:,}. "
                "Use --allow-shrink to override after manual review."
            )
    except client.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  No existing S3 parquet found; treating this as first ingest.")
            return
        raise


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc

    client = boto3.client("s3")
    check_no_shrink(local_path, allow_shrink, client)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="MBIE Who Got Funded workbook -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke test: emit first N unique contracts")
    parser.add_argument("--skip-download", action="store_true", help="Use cached workbook in --output-dir")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("MBIE Who Got Funded ingest starting")
    log(f"source_page={SOURCE_PAGE_URL}")
    log(f"workbook={WORKBOOK_URL}")
    log(f"funder_id=F{FUNDER_ID}")
    log(f"provenance={PROVENANCE}")

    workbook_path = download_workbook(args.output_dir, args.skip_download)
    source_df = load_source_rows(workbook_path)
    log(f"Loaded {len(source_df):,} workbook rows from sheet '{DATA_SHEET}'")
    df = build_awards_dataframe(source_df, args.limit)
    validate(df, len(source_df))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / PARQUET_FILENAME
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    log(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(output_path, args.allow_shrink)


if __name__ == "__main__":
    main()
