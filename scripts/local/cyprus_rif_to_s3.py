#!/usr/bin/env python3
"""
Cyprus Research and Innovation Foundation (RIF) funded projects -> S3.

Source authority
----------------
The source is RIF's own "Funded Projects" page and directly linked XLSX:

    https://www.research.org.cy/en/rifs-ri-programmes/funded-projects/
    https://www.research.org.cy/wp-content/uploads/Χρηματοδοτούμενα-Έργα-2016-2025.xlsx

The workbook contains project-level RIF proposal numbers, calls, programmes,
titles, coordinators, host/partner organizations, budgets, signature dates,
project start dates, durations, and English/Greek summaries. It is the current
RIF first-party funded-projects portfolio for 2016-2025.

OpenAlex funder
---------------
F4320330084 - Research and Innovation Foundation
DOI 10.13039/501100018877. Crossref identifies this DOI as Cyprus RIF
("Research and Innovation Foundation Cyprus"). OpenAlex currently has country
code GR on this duplicate/current entity, but the DOI is the current RIF
funder ID and the source is RIF's own Cyprus website.

Output
------
s3://openalex-ingest/awards/cyprus_rif/cyprus_rif_funded_projects.parquet

Usage
-----
    python scripts/local/cyprus_rif_to_s3.py --limit 10 --skip-upload
    python scripts/local/cyprus_rif_to_s3.py --skip-upload
    python scripts/local/cyprus_rif_to_s3.py
    python scripts/local/cyprus_rif_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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


FUNDER_ID = 4320330084
FUNDER_DISPLAY_NAME = "Research and Innovation Foundation"
PROVENANCE = "cyprus_rif_funded_projects"

FUNDED_PROJECTS_PAGE = "https://www.research.org.cy/en/rifs-ri-programmes/funded-projects/"
XLSX_URL = (
    "https://www.research.org.cy/wp-content/uploads/"
    "%CE%A7%CF%81%CE%B7%CE%BC%CE%B1%CF%84%CE%BF%CE%B4%CE%BF%CF%84"
    "%CE%BF%CF%8D%CE%BC%CE%B5%CE%BD%CE%B1-%CE%88%CF%81%CE%B3%CE%B1-2016-2025.xlsx"
)
SHEET_NAME = "Funded Projects"
HEADER_ROW_ZERO_BASED = 16

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cyprus_rif/cyprus_rif_funded_projects.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp/cyprus_rif")
DEFAULT_XLSX_NAME = "cyprus_rif_funded_projects_2016_2025.xlsx"
OUTPUT_PARQUET_NAME = "cyprus_rif_funded_projects.parquet"
EXPECTED_MIN_FULL_ROWS = 1200
REQUEST_TIMEOUT = (10, 120)
MAX_RETRIES = 5

HEADERS = {
    "User-Agent": "openalex-walden-cyprus-rif/1.0 (+https://openalex.org)",
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
}

SPACE_RE = re.compile(r"\s+")
SUFFIX_RE = re.compile(
    r"(?:,\s*)?(?:Ph\\.?D\\.?|M\\.?D\\.?|DPhil|Jr\\.?|Sr\\.?|II|III|IV)$",
    flags=re.I,
)
PREFIX_TITLES = {"dr", "dr.", "prof", "prof.", "professor", "mr", "mrs", "ms", "miss"}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).replace("\u00a0", " ").strip()
    if not text or text.lower() in {"nan", "none", "n/a", "na", "-"}:
        return None
    return SPACE_RE.sub(" ", text)


def json_string(value: Any) -> Optional[str]:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def split_name(name: Any) -> tuple[Optional[str], Optional[str]]:
    """Canonical given/family split for given-name ... family-name sources."""
    text = clean_text(name)
    if not text:
        return None, None
    text = SUFFIX_RE.sub("", text)
    parts = [p for p in text.replace(",", " ").split() if p]
    while parts and parts[0].lower().rstrip(".") in {p.rstrip(".") for p in PREFIX_TITLES}:
        parts.pop(0)
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def amount_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        amount = float(value)
    except (TypeError, ValueError):
        text = clean_text(value)
        if not text:
            return None
        try:
            amount = float(text.replace(",", ""))
        except ValueError:
            return None
    if amount <= 0:
        return None
    return amount


def date_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def year_from_date(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value[:4])
    except (TypeError, ValueError):
        return None


def end_year_from_start_duration(start_date: Optional[str], duration_months: Any) -> Optional[int]:
    if not start_date:
        return None
    try:
        months = int(float(duration_months))
    except (TypeError, ValueError):
        return None
    if months <= 0:
        return None
    start = pd.to_datetime(start_date, errors="coerce")
    if pd.isna(start):
        return None
    end = start + pd.DateOffset(months=months) - pd.Timedelta(days=1)
    return int(end.year)


def download_xlsx(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = output_dir / DEFAULT_XLSX_NAME
    if xlsx_path.exists():
        log(f"[CACHE] using {xlsx_path} ({xlsx_path.stat().st_size:,} bytes)")
        return xlsx_path

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(f"[DOWNLOAD] {XLSX_URL}")
            with requests.get(XLSX_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True) as resp:
                resp.raise_for_status()
                with xlsx_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 128):
                        if chunk:
                            f.write(chunk)
            log(f"[OK] downloaded {xlsx_path} ({xlsx_path.stat().st_size:,} bytes)")
            return xlsx_path
        except requests.RequestException as exc:
            last_exc = exc
            sleep = min(2 ** attempt, 30)
            log(f"  retry {attempt}/{MAX_RETRIES} after {exc} (sleep {sleep}s)")
            time.sleep(sleep)
    raise RuntimeError(f"Failed to download RIF workbook: {last_exc}") from last_exc


def read_source_rows(xlsx_path: Path) -> pd.DataFrame:
    log(f"[READ] {xlsx_path}")
    df = pd.read_excel(
        xlsx_path,
        sheet_name=SHEET_NAME,
        header=HEADER_ROW_ZERO_BASED,
        engine="openpyxl",
    )
    df = df.dropna(how="all")
    expected = {
        "RIF Proposal Number",
        "Call",
        "Programme",
        "Title",
        "Coordinator",
        "Organisation Name",
        "Role",
        "Project Requested Funding",
        "Signature Date",
        "Project Start Date",
        "Duration",
        "Publishable Summary (ΕΝ)",
        "Publishable Summary (GR)",
    }
    missing = expected - set(df.columns)
    if missing:
        raise RuntimeError(f"Workbook schema changed; missing columns: {sorted(missing)}")
    log(f"[OK] read {len(df):,} organization-role rows")
    return df


def row_value(row: pd.Series, column: str) -> Optional[str]:
    return clean_text(row.get(column))


def partner_payload(rows: pd.DataFrame) -> list[dict[str, Optional[str]]]:
    partners: list[dict[str, Optional[str]]] = []
    for _, row in rows.iterrows():
        role = row_value(row, "Role")
        if (role or "").lower() == "host":
            continue
        partners.append(
            {
                "role": role,
                "coordinator": row_value(row, "Coordinator"),
                "organization": row_value(row, "Organisation Name"),
                "organization_project_budget": clean_text(row.get("Organisation Project Budget")),
                "organization_project_requested_funding": clean_text(row.get("Organisation Project Requested Funding")),
            }
        )
    return partners


def all_roles_payload(rows: pd.DataFrame) -> list[dict[str, Optional[str]]]:
    payload: list[dict[str, Optional[str]]] = []
    for _, row in rows.iterrows():
        payload.append(
            {
                "role": row_value(row, "Role"),
                "coordinator": row_value(row, "Coordinator"),
                "organization": row_value(row, "Organisation Name"),
                "organization_project_budget": clean_text(row.get("Organisation Project Budget")),
                "organization_project_requested_funding": clean_text(row.get("Organisation Project Requested Funding")),
            }
        )
    return payload


def normalize_awards(source_df: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    source_df = source_df.copy()
    source_df["role_norm"] = source_df["Role"].map(lambda x: (clean_text(x) or "").lower())
    host_df = source_df[source_df["role_norm"] == "host"].copy()
    if host_df.empty:
        raise RuntimeError("No Host rows found in RIF workbook")
    duplicated_hosts = host_df["RIF Proposal Number"].duplicated(keep=False)
    if duplicated_hosts.any():
        dupes = host_df.loc[duplicated_hosts, "RIF Proposal Number"].astype(str).head(20).tolist()
        raise RuntimeError(f"Expected one Host row per proposal; duplicate hosts include {dupes}")

    grouped = {pid: rows.copy() for pid, rows in source_df.groupby("RIF Proposal Number", dropna=False)}
    records: list[dict[str, Any]] = []
    for _, row in host_df.iterrows():
        proposal = row_value(row, "RIF Proposal Number")
        if not proposal:
            continue
        rows = grouped.get(row.get("RIF Proposal Number"), pd.DataFrame())
        start_date = date_string(row.get("Project Start Date"))
        signature_date = date_string(row.get("Signature Date"))
        duration_months = amount_float(row.get("Duration"))
        given_name, family_name = split_name(row.get("Coordinator"))
        amount = amount_float(row.get("Project Requested Funding"))
        records.append(
            {
                "funder_award_id": proposal,
                "rif_proposal_number": proposal,
                "call": row_value(row, "Call"),
                "programme": row_value(row, "Programme"),
                "display_name": row_value(row, "Title"),
                "description": row_value(row, "Publishable Summary (ΕΝ)"),
                "description_gr": row_value(row, "Publishable Summary (GR)"),
                "smart_specialisation_sector": row_value(row, "Smart Specialisation Strategy Sector*"),
                "lead_name": row_value(row, "Coordinator"),
                "lead_given_name": given_name,
                "lead_family_name": family_name,
                "executing_institution": row_value(row, "Organisation Name"),
                "role": row_value(row, "Role"),
                "project_budget": amount_float(row.get("Project Budget")),
                "amount": amount,
                "currency": "EUR" if amount is not None else None,
                "organisation_project_budget": amount_float(row.get("Organisation Project Budget")),
                "organisation_project_requested_funding": amount_float(row.get("Organisation Project Requested Funding")),
                "signature_date": signature_date,
                "start_date": start_date,
                "end_date": None,
                "start_year": year_from_date(start_date),
                "end_year": end_year_from_start_duration(start_date, duration_months),
                "duration_months": int(duration_months) if duration_months is not None else None,
                "partner_organizations": json_string(partner_payload(rows)),
                "all_organization_roles": json_string(all_roles_payload(rows)),
                "source_row_count": len(rows),
                "landing_page_url": FUNDED_PROJECTS_PAGE,
                "source_file_url": XLSX_URL,
                "source_sheet": SHEET_NAME,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "provenance": PROVENANCE,
                "funder_id": str(FUNDER_ID),
                "funder_display_name": FUNDER_DISPLAY_NAME,
            }
        )

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    df = df.sort_values(["rif_proposal_number"], kind="stable").reset_index(drop=True)
    if limit is not None:
        df = df.head(limit).copy()
    return df


def validate(df: pd.DataFrame, source_rows: int) -> None:
    n = len(df)
    print("\n" + "=" * 72)
    print("Local validation")
    print("=" * 72)
    print(f"  Source organization-role rows: {source_rows:,}")
    print(f"  Award rows: {n:,}")
    if n == 0:
        raise RuntimeError("No award rows after normalization")

    def coverage(col: str) -> None:
        c = int(df[col].notna().sum())
        pct = 100.0 * c / n if n else 0.0
        print(f"  {col:<36} {c:>7,}/{n:<7,} ({pct:5.1f}%)")

    for col in [
        "funder_award_id",
        "display_name",
        "description",
        "lead_name",
        "lead_family_name",
        "executing_institution",
        "amount",
        "currency",
        "signature_date",
        "start_date",
        "start_year",
        "end_year",
        "programme",
        "landing_page_url",
    ]:
        coverage(col)

    unique_ids = int(df["funder_award_id"].nunique())
    print(f"  unique funder_award_id              {unique_ids:>7,}/{n:<7,}")
    if unique_ids != n:
        raise RuntimeError("funder_award_id values are not unique")

    if n < EXPECTED_MIN_FULL_ROWS:
        print(f"  [WARN] row count {n:,} below expected full-run minimum {EXPECTED_MIN_FULL_ROWS:,}")

    years = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(years):
        print(f"  start_year range: {int(years.min())}-{int(years.max())}")

    amounts = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amounts):
        print(
            "  amount range EUR: "
            f"{amounts.min():,.2f}-{amounts.max():,.2f}; "
            f"total EUR {amounts.sum():,.2f}"
        )

    print("\n  Top programmes:")
    print(df["programme"].value_counts(dropna=False).head(20).to_string())

    print("\n  Organization-role row counts per award:")
    print(df["source_row_count"].value_counts().sort_index().head(20).to_string())


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_PARQUET_NAME
    # Runbook §1.2.5: force string dtype immediately before parquet write.
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    print(f"\n[OK] wrote {len(df):,} rows ({output_path.stat().st_size / 1024 / 1024:.2f} MB) to {output_path}")
    return output_path


def check_no_shrink(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; use --skip-upload locally") from exc

    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    print(f"\nRunbook §1.4 shrink check: s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except client.exceptions.ClientError:
        print("  No existing S3 parquet found; first upload is allowed.")
        return

    previous_path = local_path.with_suffix(".previous.parquet")
    client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
    previous_rows = len(pd.read_parquet(previous_path))
    print(f"  Previous rows: {previous_rows:,}; new rows: {new_rows:,}")
    if new_rows < previous_rows and not allow_shrink:
        raise RuntimeError(
            f"Refusing to shrink {S3_KEY}: new {new_rows:,} < previous {previous_rows:,}. "
            "Use --allow-shrink only after confirming the source legitimately shrank."
        )


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    check_no_shrink(local_path, allow_shrink)
    import boto3

    print(f"\nUploading {local_path} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_path), S3_BUCKET, S3_KEY)
    print("[OK] upload complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cyprus RIF funded projects -> parquet -> S3")
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: keep only the first N award rows")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--skip-download", action="store_true", help="Use cached XLSX in --output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow row-count shrink vs existing S3 parquet")
    args = parser.parse_args()

    print("=" * 72)
    print("Cyprus RIF funded projects -> S3 pipeline")
    print("=" * 72)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Source:     {FUNDED_PROJECTS_PAGE}")
    print(f"  Output dir: {args.output_dir}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    xlsx_path = args.output_dir / DEFAULT_XLSX_NAME
    if args.skip_download:
        if not xlsx_path.exists():
            print(f"[ERROR] --skip-download set but cached workbook does not exist: {xlsx_path}")
            sys.exit(2)
        print(f"[CACHE] --skip-download using {xlsx_path}")
    else:
        xlsx_path = download_xlsx(args.output_dir)

    source_df = read_source_rows(xlsx_path)
    awards_df = normalize_awards(source_df, args.limit)
    validate(awards_df, source_rows=len(source_df))
    output_path = write_parquet(awards_df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {output_path}")
        return

    upload_to_s3(output_path, args.allow_shrink)


if __name__ == "__main__":
    main()
