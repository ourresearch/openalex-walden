#!/usr/bin/env python3
"""
Czech Ministry of Education, Youth and Sports (MEYS/MSMT) awards -> S3 parquet.

Source authority
----------------
Official Czech IS VaVaI open-data exports:

    https://www.isvavai.cz/opendata
    https://www.isvavai.cz/dokumenty/opendata/CEP-projekty.csv
    https://www.isvavai.cz/dokumenty/opendata/CEP-ucastnici.csv

IS VaVaI is the Czech national information system for research,
experimental development, and innovation. CEP-projekty.csv contains all
public Czech R&D projects, keyed by `kod_projektu`. This ingest filters the
official provider code `MSM`, the Ministry of Education, Youth and Sports
(Ministerstvo školství, mládeže a tělovýchovy), and joins CEP-ucastnici.csv
to attach the source-reported lead recipient organization and ROR where
available.

The feed publishes project-level identifiers, titles, objectives, years,
day-level start/end dates where available, and CZK support amounts where
available. It does not publish named principal investigators, so the notebook
maps rows as organization-level grant awards with the lead recipient stored as
lead_investigator.affiliation.

Output
------
    s3://openalex-ingest/awards/meys/meys_projects.parquet

Usage
-----
    python scripts/local/meys_to_s3.py --skip-upload
    python scripts/local/meys_to_s3.py --limit 10 --skip-upload
    python scripts/local/meys_to_s3.py --skip-download --skip-upload
"""

from __future__ import annotations

import argparse
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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
# --- end shim -------------------------------------------------------------


FUNDER_ID = 4320321005
FUNDER_DISPLAY_NAME = "Ministerstvo Školství, Mládeže a Tělovýchovy"
PROVIDER_CODE = "MSM"
PROVENANCE = "isvavai_msm"

PROJECTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-projekty.csv"
PARTICIPANTS_URL = "https://www.isvavai.cz/dokumenty/opendata/CEP-ucastnici.csv"

PROJECTS_FILENAME = "CEP-projekty.csv"
PARTICIPANTS_FILENAME = "CEP-ucastnici.csv"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/meys/meys_projects.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-meys-ingest/1.0; +https://openalex.org)"

PROJECT_USECOLS = [
    "odkaz_na_isvavai.cz",
    "kod_projektu",
    "RAid",
    "poskytovatel",
    "kategorie_vav",
    "kod_programu",
    "nazev_projektu_originalni",
    "nazev_projektu_anglicky",
    "rok_zahajeni",
    "rok_ukonceni",
    "datum_zahajeni",
    "datum_ukonceni",
    "hlavni_obor",
    "hlavni_vedni_obor_oecd",
    "cile_reseni_originalni",
    "cile_reseni_anglicky",
    "klicova_slova_anglicky",
    "hlavni_prijemce",
    "nazev_organizace",
    "zeme_sidla",
    "celkove_naklady_na_dobu_reseni",
    "statni_podpora_na_dobu_reseni",
    "podpora_z_verejnych_zahranicnich_zdroju",
    "cislo_smlouvy",
    "posledni_stav_projektu",
    "kod_hodnoceni",
]

PARTICIPANT_USECOLS = [
    "kod_projektu",
    "poskytovatel",
    "ucastnik",
    "nazev_ucastnika",
    "zeme_sidla",
    "celkove_naklady_ucastnika_na_dobu_reseni",
    "statni_podpora_pro_ucastnika_na_dobu_reseni",
    "podpora_z_verejnych_zahranicnich_zdroju",
    "ROR",
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_blank(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def positive_number_count(series: pd.Series) -> int:
    nums = pd.to_numeric(series, errors="coerce")
    return int((nums > 0).sum())


def remote_size(url: str) -> int | None:
    try:
        response = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=30, allow_redirects=True)
        response.raise_for_status()
    except requests.RequestException:
        return None
    value = response.headers.get("content-length")
    return int(value) if value and value.isdigit() else None


def download_with_resume(url: str, path: Path, description: str) -> None:
    expected = remote_size(url)
    existing = path.stat().st_size if path.exists() else 0
    if expected and existing == expected:
        log(f"{description}: using cached complete file {path} ({existing:,} bytes)")
        return
    if expected and existing > expected:
        log(f"{description}: local file larger than remote; restarting download")
        path.unlink()
        existing = 0

    headers = {"User-Agent": USER_AGENT}
    mode = "wb"
    if existing:
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"
        log(f"{description}: resuming at byte {existing:,}")
    else:
        log(f"{description}: downloading {url}")

    with requests.get(url, headers=headers, stream=True, timeout=300) as response:
        if existing and response.status_code == 200:
            log(f"{description}: server ignored range request; restarting from byte 0")
            mode = "wb"
            existing = 0
        elif response.status_code not in (200, 206):
            response.raise_for_status()

        path.parent.mkdir(parents=True, exist_ok=True)
        downloaded = existing
        last_report = time.time()
        with path.open(mode) as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                fh.write(chunk)
                downloaded += len(chunk)
                if time.time() - last_report >= 5:
                    if expected:
                        log(f"{description}: {downloaded / 1024 / 1024:,.1f} MB / {expected / 1024 / 1024:,.1f} MB")
                    else:
                        log(f"{description}: {downloaded / 1024 / 1024:,.1f} MB")
                    last_report = time.time()

    final_size = path.stat().st_size
    if expected and final_size != expected:
        raise RuntimeError(f"{description}: incomplete download {final_size:,} != expected {expected:,} bytes")
    log(f"{description}: downloaded {final_size / 1024 / 1024:,.1f} MB")


def ensure_sources(output_dir: Path, skip_download: bool) -> tuple[Path, Path]:
    projects_path = output_dir / PROJECTS_FILENAME
    participants_path = output_dir / PARTICIPANTS_FILENAME
    if skip_download:
        missing = [str(p) for p in [projects_path, participants_path] if not p.exists()]
        if missing:
            raise FileNotFoundError("--skip-download requested but source files are missing: " + ", ".join(missing))
        log(f"Using cached source files in {output_dir}")
        return projects_path, participants_path

    download_with_resume(PROJECTS_URL, projects_path, "CEP projects")
    download_with_resume(PARTICIPANTS_URL, participants_path, "CEP participants")
    return projects_path, participants_path


def read_source_csv(path: Path, usecols: list[str], description: str) -> pd.DataFrame:
    log(f"Reading {description}: {path}")
    df = pd.read_csv(path, dtype=str, usecols=lambda col: col in usecols)
    missing = sorted(set(usecols) - set(df.columns))
    if missing:
        raise RuntimeError(f"{description} missing expected columns: {missing}")
    df = df.map(clean_blank) if hasattr(df, "map") else df.applymap(clean_blank)
    log(f"{description}: {len(df):,} rows, {len(df.columns)} columns")
    return df


def process_meys(projects: pd.DataFrame, participants: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    projects = projects[projects["poskytovatel"] == PROVIDER_CODE].copy()
    participants = participants[participants["poskytovatel"] == PROVIDER_CODE].copy()
    log(f"MEYS projects: {len(projects):,}")
    log(f"MEYS participants: {len(participants):,}")

    lead_participants = participants[
        [
            "kod_projektu",
            "ucastnik",
            "nazev_ucastnika",
            "zeme_sidla",
            "ROR",
            "celkove_naklady_ucastnika_na_dobu_reseni",
            "statni_podpora_pro_ucastnika_na_dobu_reseni",
            "podpora_z_verejnych_zahranicnich_zdroju",
        ]
    ].drop_duplicates()

    merged = projects.merge(
        lead_participants,
        left_on=["kod_projektu", "hlavni_prijemce"],
        right_on=["kod_projektu", "ucastnik"],
        how="left",
        suffixes=("", "_participant"),
    )
    if limit:
        merged = merged.head(limit).copy()
        log(f"Limit active: keeping first {len(merged):,} MEYS projects")

    rename_map = {
        "odkaz_na_isvavai.cz": "source_url",
        "kod_projektu": "project_code",
        "RAid": "research_activity_id",
        "poskytovatel": "provider_code",
        "kategorie_vav": "research_category_code",
        "kod_programu": "program_code",
        "nazev_projektu_originalni": "title_original",
        "nazev_projektu_anglicky": "title_en",
        "rok_zahajeni": "start_year",
        "rok_ukonceni": "end_year",
        "datum_zahajeni": "start_date",
        "datum_ukonceni": "end_date",
        "hlavni_obor": "primary_field_code",
        "hlavni_vedni_obor_oecd": "primary_oecd_field_code",
        "cile_reseni_originalni": "objectives_original",
        "cile_reseni_anglicky": "objectives_en",
        "klicova_slova_anglicky": "keywords_en",
        "hlavni_prijemce": "lead_participant_code",
        "nazev_organizace": "lead_org_name_project",
        "zeme_sidla": "lead_country_project",
        "celkove_naklady_na_dobu_reseni": "total_cost_czk",
        "statni_podpora_na_dobu_reseni": "state_support_czk",
        "podpora_z_verejnych_zahranicnich_zdroju": "foreign_public_support_czk",
        "cislo_smlouvy": "contract_number",
        "posledni_stav_projektu": "project_status_code",
        "kod_hodnoceni": "evaluation_code",
        "ucastnik": "lead_participant_code_matched",
        "nazev_ucastnika": "lead_org_name_participant",
        "zeme_sidla_participant": "lead_country_participant",
        "ROR": "lead_ror",
        "celkove_naklady_ucastnika_na_dobu_reseni": "lead_total_cost_czk",
        "statni_podpora_pro_ucastnika_na_dobu_reseni": "lead_state_support_czk",
        "podpora_z_verejnych_zahranicnich_zdroju_participant": "lead_foreign_public_support_czk",
    }
    merged = merged.rename(columns=rename_map)
    merged["funder_award_id"] = merged["project_code"]
    merged["provenance"] = PROVENANCE

    ordered_cols = [
        "source_url",
        "funder_award_id",
        "project_code",
        "research_activity_id",
        "provider_code",
        "research_category_code",
        "program_code",
        "title_original",
        "title_en",
        "start_year",
        "end_year",
        "start_date",
        "end_date",
        "primary_field_code",
        "primary_oecd_field_code",
        "objectives_original",
        "objectives_en",
        "keywords_en",
        "lead_participant_code",
        "lead_participant_code_matched",
        "lead_org_name_project",
        "lead_org_name_participant",
        "lead_country_project",
        "lead_country_participant",
        "lead_ror",
        "total_cost_czk",
        "state_support_czk",
        "foreign_public_support_czk",
        "lead_total_cost_czk",
        "lead_state_support_czk",
        "lead_foreign_public_support_czk",
        "contract_number",
        "project_status_code",
        "evaluation_code",
        "provenance",
    ]
    for col in ordered_cols:
        if col not in merged.columns:
            merged[col] = None
    return merged[ordered_cols]


def validate_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise RuntimeError("No MEYS rows after provider-code filter")
    n = len(df)
    duplicate_ids = df["funder_award_id"][df["funder_award_id"].duplicated()].dropna().unique()
    if len(duplicate_ids):
        raise RuntimeError(f"Duplicate funder_award_id values: {duplicate_ids[:10]}")

    coverage_fields = [
        "funder_award_id",
        "title_en",
        "title_original",
        "start_year",
        "end_year",
        "start_date",
        "end_date",
        "lead_org_name_project",
        "lead_country_project",
        "lead_ror",
        "objectives_en",
        "objectives_original",
        "state_support_czk",
    ]
    for field in coverage_fields:
        count = int(df[field].notna().sum())
        log(f"{field:<24} coverage {count:>6}/{n:<6} ({count * 100 / n:5.1f}%)")

    amount_count = positive_number_count(df["state_support_czk"])
    log(f"positive_state_support_czk coverage {amount_count:>6}/{n:<6} ({amount_count * 100 / n:5.1f}%)")

    start_years = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if not start_years.empty:
        log(f"Start year range: {int(start_years.min())}-{int(start_years.max())}")

    amount_values = pd.to_numeric(df["state_support_czk"], errors="coerce")
    amount_values = amount_values[amount_values > 0]
    if not amount_values.empty:
        log(
            "State support range: "
            f"CZK {amount_values.min():,.0f}-{amount_values.max():,.0f}; "
            f"total CZK {amount_values.sum():,.0f}"
        )

    log("Top program codes:")
    for program, count in df["program_code"].value_counts(dropna=False).head(15).items():
        log(f"  {program}: {count}")


def build_dataframe(output_dir: Path, skip_download: bool, limit: int | None) -> pd.DataFrame:
    projects_path, participants_path = ensure_sources(output_dir, skip_download)
    projects = read_source_csv(projects_path, PROJECT_USECOLS, "CEP projects")
    participants = read_source_csv(participants_path, PARTICIPANT_USECOLS, "CEP participants")
    df = process_meys(projects, participants, limit)
    validate_dataframe(df)
    return df


def check_no_shrink(new_count: int, allow_shrink: bool) -> None:
    if allow_shrink:
        log("--allow-shrink set; skipping S3 shrink guard")
        return
    try:
        import boto3

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        old_df = pd.read_parquet(BytesIO(obj["Body"].read()))
        old_count = len(old_df)
    except Exception as exc:
        log(f"No prior S3 parquet readable for shrink check ({type(exc).__name__}); continuing")
        return

    if new_count < old_count:
        raise RuntimeError(
            f"Shrink guard blocked upload: new row count {new_count:,} < existing {old_count:,}. "
            "Re-run with --allow-shrink only after confirming the source truly shrank."
        )
    log(f"S3 shrink check passed: new {new_count:,} >= existing {old_count:,}")


def upload_to_s3(local_path: Path, skip_upload: bool, allow_shrink: bool) -> None:
    if skip_upload:
        log("--skip-upload set; not uploading to S3")
        return

    check_no_shrink(len(pd.read_parquet(local_path)), allow_shrink)

    import boto3

    s3 = boto3.client("s3")
    log(f"Uploading {local_path} to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Czech MEYS IS VaVaI projects to parquet/S3")
    parser.add_argument("--limit", type=int, default=None, help="Limit to first N MEYS project rows for smoke tests")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/meys_awards"), help="Directory for CSV/parquet outputs")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached CEP CSV files in output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow uploading fewer rows than prior S3 parquet")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "meys_projects.parquet"

    log(f"Starting MEYS ingest (funder_id={FUNDER_ID}, provider_code={PROVIDER_CODE}, provenance={PROVENANCE})")
    df = build_dataframe(args.output_dir, args.skip_download, args.limit)

    # Required by runbook §1.2.5 immediately before parquet write.
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df):,} rows to {parquet_path}")

    upload_to_s3(parquet_path, args.skip_upload, args.allow_shrink)


if __name__ == "__main__":
    main()
