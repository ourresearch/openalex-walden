#!/usr/bin/env python3
"""
Maudsley Charity Grants -> S3 Pipeline (ORG-LEVEL GRANT PATTERN, 360Giving)
============================================================================

Downloads Maudsley Charity's published grant record from its own website in
the 360Giving open-data format. Maudsley Charity is a UK mental-health
charity supporting services, community programmes, and research connected to
South London and Maudsley NHS Foundation Trust, King's College London, and
related partners.

Source authority
----------------
The 360Giving Data Registry (https://registry.threesixtygiving.org/data.json)
lists the publisher "Maudsley Charity" and resolves to this direct workbook
download from maudsleycharity.org:

    https://maudsleycharity.org/wp-content/uploads/2026/03/MC-360-Giving-26-11-2025v2.xlsx

Sheet `grants` carries one row per published grant in the 360Giving schema.
The columns used here include:

    Identifier                   source grant identifier (numeric in this file)
    Title                        grant title
    Description                  free-text grant purpose
    Currency                     GBP
    Amount Awarded               awarded amount (0/blank means undisclosed)
    Award Date                   real ISO date
    Planned Dates:Start Date     planned grant start date
    Planned Dates:End Date       planned grant end date
    Recipient Org:Name           grantee organization
    Recipient Org:Country        source-authoritative recipient country
    Grant Programme:Title        programme/scheme label

The file contains a small number of exact duplicate rows by `Identifier`.
Those repeats are skipped only when the parsed row is identical; conflicting
duplicate identifiers raise because they would silently merge awards
downstream.

Awarding body in OpenAlex:
  Maudsley Charity (F4320313345, GB, ROR 028vg3q27,
  DOI 10.13039/100012176).

Output
------
  s3://openalex-ingest/awards/maudsley/maudsley_grants.parquet

Usage
-----
    python maudsley_to_s3.py                                  # full run
    python maudsley_to_s3.py --skip-upload                    # local dev
    python maudsley_to_s3.py --limit 50                       # smoke
    python maudsley_to_s3.py --skip-download --skip-upload    # reuse cache
    python maudsley_to_s3.py --allow-shrink                   # override section 1.4

Requirements
------------
    pip install pandas pyarrow openpyxl requests boto3
"""

import argparse
import re
import time
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


DATA_URL = "https://maudsleycharity.org/wp-content/uploads/2026/03/MC-360-Giving-26-11-2025v2.xlsx"
SHEET = "grants"

FUNDER_ID = 4320313345
FUNDER_DISPLAY_NAME = "Maudsley Charity"
PROVENANCE = "maudsley_360giving"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/maudsley/maudsley_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-maudsley-ingest/1.0; +https://openalex.org)"
DEFAULT_CACHE = Path(".cache/maudsley_360giving.xlsx")

COUNTRY_ISO = {
    "uk": "GB",
    "united kingdom": "GB",
    "gb": "GB",
    "great britain": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def download_workbook(skip_download: bool, cache_path: Path) -> bytes:
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download set but cache not found: {cache_path}")
        log(f"  reusing cached workbook {cache_path}")
        return cache_path.read_bytes()

    log(f"  downloading {DATA_URL}")
    try:
        import requests

        resp = requests.get(DATA_URL, headers={"User-Agent": USER_AGENT}, timeout=120)
        resp.raise_for_status()
        content = resp.content
    except ImportError:
        import urllib.request

        req = urllib.request.Request(DATA_URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            content = resp.read()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(content)
    log(f"  wrote cache {cache_path} ({len(content) / 1e3:.0f} KB)")
    return content


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


def parse_amount(v) -> Optional[float]:
    """Return positive 360Giving Amount Awarded as float; blank/0 is NULL."""
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
    try:
        ts = pd.to_datetime(v, errors="coerce")
    except Exception:
        return None
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
    c = clean_text(v)
    if not c:
        return None
    return COUNTRY_ISO.get(c.lower())


def prefixed_award_id(identifier: str) -> str:
    source_id = re.sub(r"\s+", "-", identifier.strip())
    if source_id.lower().startswith("360g-"):
        return source_id
    return f"360G-maudsley-{source_id}"


def build_row(rec: dict) -> Optional[dict]:
    source_identifier = clean_text(rec.get("Identifier"))
    if not source_identifier:
        return None

    amount = parse_amount(rec.get("Amount Awarded"))
    currency = clean_text(rec.get("Currency"))
    award_date = iso_date(rec.get("Award Date"))
    planned_start_date = iso_date(rec.get("Planned Dates:Start Date"))
    planned_end_date = iso_date(rec.get("Planned Dates:End Date"))
    start_date = planned_start_date or award_date

    return {
        "source_identifier": source_identifier,
        "funder_award_id": prefixed_award_id(source_identifier),
        "title": clean_text(rec.get("Title")),
        "description": clean_text(rec.get("Description")),
        "amount": amount,
        "amount_raw": clean_text(rec.get("Amount Awarded")),
        "currency": currency.upper() if amount is not None and currency else None,
        "award_date": award_date,
        "planned_start_date": planned_start_date,
        "planned_end_date": planned_end_date,
        "start_date": start_date,
        "end_date": planned_end_date,
        "start_year": year_of(start_date),
        "end_year": year_of(planned_end_date),
        "duration_months": clean_text(rec.get("Planned Dates:Duration (months)")),
        "grant_programme": clean_text(rec.get("Grant Programme:Title")),
        "recipient_org": clean_text(rec.get("Recipient Org:Name")),
        "recipient_org_identifier": clean_text(rec.get("Recipient Org:Identifier")),
        "recipient_city": clean_text(rec.get("Recipient Org:City")),
        "recipient_country": clean_text(rec.get("Recipient Org:Country")),
        "recipient_country_iso": country_iso(rec.get("Recipient Org:Country")),
        "funding_org_identifier": clean_text(rec.get("Funding Org:Identifier")),
        "funding_org": clean_text(rec.get("Funding Org:Name")) or FUNDER_DISPLAY_NAME,
        "source_url": clean_text(rec.get("URL")),
        "source_workbook_url": DATA_URL,
    }


def validate_rows(rows: list, duplicate_count: int) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")

    n = len(rows)
    log(f"  exact duplicate source rows skipped: {duplicate_count}")
    for field in (
        "title", "funder_award_id", "description", "amount", "currency",
        "start_date", "end_date", "start_year", "end_year", "recipient_org",
        "recipient_country_iso", "grant_programme",
    ):
        non_null = sum(1 for r in rows if r.get(field) not in (None, "", []))
        log(f"  {field:<22} coverage {non_null}/{n} ({non_null * 100 / n:.1f}%)")

    ids = [r["funder_award_id"] for r in rows if r.get("funder_award_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(f"funder_award_id collisions: {dups}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")

    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}-{max(years)}")
    end_years = [r["end_year"] for r in rows if r.get("end_year")]
    if end_years:
        log(f"  end_year range: {min(end_years)}-{max(end_years)}")

    amounts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amounts:
        amounts_sorted = sorted(amounts)
        log(
            f"  amount stats: n={len(amounts)} ({len(amounts) * 100 / n:.1f}%) "
            f"min={min(amounts):,.0f} median={amounts_sorted[len(amounts_sorted) // 2]:,.0f} "
            f"max={max(amounts):,.0f} total={sum(amounts):,.0f}"
        )

    programmes = {}
    for row in rows:
        programme = row.get("grant_programme") or "(none)"
        programmes[programme] = programmes.get(programme, 0) + 1
    top = sorted(programmes.items(), key=lambda item: (-item[1], item[0]))[:10]
    log("  programmes: " + ", ".join(f"{name}={count}" for name, count in top))


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
    parser = argparse.ArgumentParser(description="Fetch Maudsley Charity 360Giving grants -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "maudsley_grants.parquet"

    log("=== Maudsley Charity grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    content = download_workbook(args.skip_download, args.cache)
    import io

    raw_df = pd.read_excel(io.BytesIO(content), sheet_name=SHEET, dtype=str)
    log(f"  loaded {len(raw_df):,} rows x {len(raw_df.columns)} cols from sheet '{SHEET}'")

    records = raw_df.to_dict("records")
    if args.limit is not None:
        records = records[:args.limit]
        log(f"--limit {args.limit}: processing {len(records)} source rows")

    rows = []
    seen = {}
    duplicate_count = 0
    for rec in records:
        row = build_row(rec)
        if row is None:
            continue
        award_id = row["funder_award_id"]
        if award_id in seen:
            if row != seen[award_id]:
                raise RuntimeError(f"Conflicting duplicate funder_award_id: {award_id}")
            duplicate_count += 1
            continue
        seen[award_id] = row
        rows.append(row)

    log(f"Built {len(rows):,} unique grant rows")
    validate_rows(rows, duplicate_count)

    log("Building DataFrame...")
    df = pd.DataFrame(rows)
    df = df.astype("string")
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size / 1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Maudsley ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("section 1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Maudsley ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
