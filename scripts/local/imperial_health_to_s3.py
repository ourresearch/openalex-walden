#!/usr/bin/env python3
"""
Imperial Health Charity Grants -> S3 Pipeline (360Giving)
=========================================================

Downloads Imperial Health Charity's published 360Giving grant workbooks from
its own website. The charity funds patient care, staff development,
innovation, community programmes, hardship support, and research fellowships
connected to Imperial College Healthcare NHS Trust.

Source authority
----------------
The 360Giving Data Registry (https://registry.threesixtygiving.org/data.json)
lists publisher "Imperial Health Charity" and resolves to eight direct
workbook downloads from imperialcharity.org.uk covering 2018-2019 through
2025-2026.

The newer files split organization and individual grants into `orgs` and
`inds` sheets. Individual-recipient rows are anonymized by the source as
"Individual Recipient"; this script preserves those rows but does not invent
person names.

Awarding body in OpenAlex:
  Imperial Health Charity (F4320314580, GB, no ROR,
  DOI 10.13039/100013842).

Output
------
  s3://openalex-ingest/awards/imperial_health/imperial_health_grants.parquet

Usage
-----
    python imperial_health_to_s3.py                                  # full run
    python imperial_health_to_s3.py --skip-upload                    # local dev
    python imperial_health_to_s3.py --limit 50                       # smoke
    python imperial_health_to_s3.py --skip-download --skip-upload    # reuse cache
    python imperial_health_to_s3.py --allow-shrink                   # override section 1.4

Requirements
------------
    pip install pandas pyarrow openpyxl requests boto3
"""

import argparse
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pandas as pd

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


SOURCES = [
    ("2018-2019", "a003W0000010eiHQAQ", "https://www.imperialcharity.org.uk/download_file/view/1519/348", "imperial_health_2018_2019.xlsx"),
    ("2019-2020", "a003W000004stw1QAA", "https://www.imperialcharity.org.uk/download_file/view/1901/348", "imperial_health_2019_2020.xlsx"),
    ("2020-2021", "a003W000004stw2QAA", "https://www.imperialcharity.org.uk/download_file/view/1902/348", "imperial_health_2020_2021.xlsx"),
    ("2021-2022", "a003W000004lSWQQA2", "https://www.imperialcharity.org.uk/download_file/view/2105/348", "imperial_health_2021_2022.xlsx"),
    ("2022-2023", "a00P400000ab3qKIAQ", "https://www.imperialcharity.org.uk/download_file/view/3483/348", "imperial_health_2022_2023.xlsx"),
    ("2023-2024", "a00P400000abFjJIAU", "https://www.imperialcharity.org.uk/download_file/view/3484/348", "imperial_health_2023_2024.xlsx"),
    ("2024-2025", "a00P400000abFrNIAU", "https://www.imperialcharity.org.uk/download_file/view/3482/348", "imperial_health_2024_2025.xlsx"),
    ("2025-2026", "a00P400000abEk4IAE", "https://www.imperialcharity.org.uk/download_file/view/3481/348", "imperial_health_2025_2026.xlsx"),
]

FUNDER_ID = 4320314580
FUNDER_DISPLAY_NAME = "Imperial Health Charity"
PROVENANCE = "imperial_health_360giving"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/imperial_health/imperial_health_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-imperial-health-ingest/1.0; +https://openalex.org)"
DEFAULT_CACHE_DIR = Path(".cache/imperial_health")

COUNTRY_ISO = {"uk": "GB", "united kingdom": "GB", "gb": "GB"}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s.strip().lower() in ("", "nan", "none", "<na>"):
        return None
    s = s.replace("_x000D_", "\n")
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    s = re.sub(r"\n{2,}", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip() or None


def get_col(rec: dict, *names):
    for name in names:
        if name in rec:
            return rec.get(name)
    return None


def download_file(url: str, cache_path: Path, skip_download: bool) -> bytes:
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download set but cache not found: {cache_path}")
        log(f"  reusing cached workbook {cache_path}")
        return cache_path.read_bytes()
    log(f"  downloading {url}")
    try:
        import requests
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
        resp.raise_for_status()
        content = resp.content
    except ImportError:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            content = resp.read()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(content)
    log(f"  wrote cache {cache_path} ({len(content) / 1e3:.0f} KB)")
    return content


def parse_amount(v) -> Optional[float]:
    s = clean_text(v)
    if not s:
        return None
    try:
        amount = float(s.replace(",", ""))
    except ValueError:
        return None
    return amount if amount > 0 else None


def iso_date(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def year_of(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        return int(iso[:4])
    except ValueError:
        return None


def country_iso(v) -> Optional[str]:
    country = clean_text(v)
    if not country:
        return None
    return COUNTRY_ISO.get(country.lower())


def recipient_type(rec: dict) -> str:
    if clean_text(get_col(rec, "Recipient Ind:Identifier", "Recipient Ind:Name")):
        return "individual"
    org_name = clean_text(rec.get("Recipient Org:Name"))
    if org_name and org_name.lower() != "individual recipient":
        return "organization"
    if org_name:
        return "individual_anonymous"
    return "unknown"


def build_row(rec: dict, fiscal_year: str, registry_identifier: str, source_url: str, source_sheet: str) -> Optional[dict]:
    source_identifier = clean_text(rec.get("Identifier"))
    if not source_identifier:
        return None
    amount = parse_amount(rec.get("Amount Awarded"))
    currency = clean_text(rec.get("Currency"))
    award_date = iso_date(rec.get("Award Date"))
    planned_start_date = iso_date(get_col(rec, "Planned Dates:Start Date"))
    planned_end_date = iso_date(get_col(rec, "Planned Dates:End Date"))
    rtype = recipient_type(rec)
    org_name = clean_text(rec.get("Recipient Org:Name"))
    ind_name = clean_text(rec.get("Recipient Ind:Name"))
    country = clean_text(rec.get("Recipient Org:Country"))

    return {
        "source_identifier": source_identifier,
        "funder_award_id": source_identifier,  # disambiguated after all rows are parsed
        "title": clean_text(rec.get("Title")),
        "description": clean_text(rec.get("Description")),
        "amount": amount,
        "amount_raw": clean_text(rec.get("Amount Awarded")),
        "currency": currency.upper() if amount is not None and currency else None,
        "award_date": award_date,
        "planned_start_date": planned_start_date,
        "planned_end_date": planned_end_date,
        "start_date": planned_start_date or award_date,
        "end_date": planned_end_date,
        "start_year": year_of(planned_start_date or award_date),
        "end_year": year_of(planned_end_date),
        "recipient_type": rtype,
        "recipient_org": org_name,
        "recipient_org_identifier": clean_text(rec.get("Recipient Org:Identifier")),
        "recipient_ind_name": ind_name,
        "recipient_ind_identifier": clean_text(rec.get("Recipient Ind:Identifier")),
        "recipient_country": country,
        "recipient_country_iso": country_iso(country),
        "beneficiary_location_name": clean_text(get_col(rec, "Beneficiary Location:Name", "Beneficiary Location:0:Name")),
        "beneficiary_country_code": clean_text(get_col(rec, "Beneficiary Location:Country Code")),
        "grant_programme": clean_text(get_col(rec, "Grant Programme:Title", "Grant Programme: Title")),
        "grant_programme_url": clean_text(get_col(rec, "Grant Programme:URL", "Grant Programme: URL")),
        "funding_org": clean_text(rec.get("Funding Org:Name")) or FUNDER_DISPLAY_NAME,
        "funding_org_identifier": clean_text(rec.get("Funding Org:Identifier")),
        "source_url": clean_text(rec.get("URL")),
        "data_source": clean_text(rec.get("Data Source")),
        "source_fiscal_year": fiscal_year,
        "source_sheet": source_sheet,
        "registry_identifier": registry_identifier,
        "source_workbook_url": source_url,
    }


def disambiguate_award_ids(rows: list) -> int:
    by_source = defaultdict(list)
    for row in rows:
        by_source[row["source_identifier"]].append(row)
    changed = 0
    for source_identifier, grouped in by_source.items():
        if len(grouped) == 1:
            continue
        for row in grouped:
            suffix = re.sub(r"[^A-Za-z0-9]+", "-", row["source_fiscal_year"]).strip("-")
            row["funder_award_id"] = f"{source_identifier}-{suffix}"
            changed += 1
    return changed


def validate_rows(rows: list, disambiguated: int) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    log(f"  conflicting duplicate source identifiers disambiguated: {disambiguated}")
    for field in (
        "funder_award_id", "title", "description", "amount", "currency",
        "start_date", "start_year", "recipient_type", "recipient_org",
        "recipient_ind_identifier", "recipient_country_iso", "grant_programme",
    ):
        non_null = sum(1 for row in rows if row.get(field) not in (None, "", []))
        log(f"  {field:<26} coverage {non_null}/{n} ({non_null * 100 / n:.1f}%)")

    ids = [row["funder_award_id"] for row in rows if row.get("funder_award_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(f"funder_award_id collisions: {dups}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")

    years = [row["start_year"] for row in rows if row.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}-{max(years)}")
    amounts = [row["amount"] for row in rows if row.get("amount") is not None]
    if amounts:
        amounts_sorted = sorted(amounts)
        log(
            f"  amount stats: n={len(amounts)} ({len(amounts) * 100 / n:.1f}%) "
            f"min={min(amounts):,.0f} median={amounts_sorted[len(amounts_sorted) // 2]:,.0f} "
            f"max={max(amounts):,.0f} total={sum(amounts):,.0f}"
        )
    programs = {}
    types = {}
    for row in rows:
        programs[row.get("grant_programme") or "(none)"] = programs.get(row.get("grant_programme") or "(none)", 0) + 1
        types[row.get("recipient_type") or "(none)"] = types.get(row.get("recipient_type") or "(none)", 0) + 1
    log("  recipient types: " + ", ".join(f"{k}={v}" for k, v in sorted(types.items())))
    log("  top programmes: " + ", ".join(f"{k}={v}" for k, v in sorted(programs.items(), key=lambda item: (-item[1], item[0]))[:10]))


def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping section 1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        previous_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        previous_df = pd.read_parquet(io.BytesIO(previous_bytes))
        previous_count = len(previous_df)
        log(f"  section 1.4 shrink-check: previous S3 parquet had {previous_count:,} rows")
        if new_count < previous_count:
            log(f"  section 1.4 FAIL: new ({new_count:,}) < previous ({previous_count:,}). Aborting.")
            return False
        log(f"  section 1.4 OK: new {new_count:,} >= previous {previous_count:,}")
        return True
    except Exception as exc:
        log(f"  section 1.4 shrink-check skipped: {type(exc).__name__}: {str(exc)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Imperial Health Charity 360Giving grants -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "imperial_health_grants.parquet"

    log("=== Imperial Health Charity grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache_dir={args.cache_dir}")

    rows = []
    import io
    for fiscal_year, registry_identifier, url, cache_name in SOURCES:
        content = download_file(url, args.cache_dir / cache_name, args.skip_download)
        workbook = pd.ExcelFile(io.BytesIO(content))
        for sheet in workbook.sheet_names:
            raw_df = pd.read_excel(workbook, sheet_name=sheet, dtype=str)
            log(f"  loaded {len(raw_df):,} rows x {len(raw_df.columns)} cols from {fiscal_year}/{sheet}")
            for rec in raw_df.to_dict("records"):
                row = build_row(rec, fiscal_year, registry_identifier, url, sheet)
                if row is not None:
                    rows.append(row)

    disambiguated = disambiguate_award_ids(rows)
    if args.limit is not None:
        rows = rows[:args.limit]
        log(f"--limit {args.limit}: validating/writing {len(rows)} rows")

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows, disambiguated if args.limit is None else 0)

    log("Building DataFrame...")
    df = pd.DataFrame(rows)
    df = df.astype("string")
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size / 1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Imperial Health Charity ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("section 1.4 shrink-check failed.")
    upload_to_s3(output_path)
    log("=== Imperial Health Charity ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
