#!/usr/bin/env python3
"""
Daiwa Anglo-Japanese Foundation Grants -> S3 Pipeline (360Giving)
================================================================

Downloads the Daiwa Anglo-Japanese Foundation's published 360Giving grant
workbooks from its own website. The foundation funds UK-Japan collaboration,
including research, education, arts, culture, and institutional exchange.

Source authority
----------------
The 360Giving Data Registry (https://registry.threesixtygiving.org/data.json)
lists publisher "Daiwa Anglo-Japanese Foundation" and resolves to eight direct
workbook downloads on dajf.org.uk covering the March/September funding rounds
from 2021 through 2024.

Each workbook has a single 360Giving sheet. The columns used here include:

    Identifier                stable 360Giving grant id
    Title                     grant title
    Description               free-text grant purpose
    Currency                  GBP
    Amount Awarded            awarded amount
    Award Date                real award date
    Recipient Org:Name        grantee organization
    Recipient Org:Country     explicit recipient country (UK/Japan)
    Grant Programme: Title    Daiwa Foundation Award / Small Grant
    Funding Org:Name          Daiwa Anglo-Japanese Foundation

Awarding body in OpenAlex:
  Daiwa Anglo-Japanese Foundation (F4320320179, GB, ROR 00ev6sa36,
  DOI 10.13039/501100000594).

Output
------
  s3://openalex-ingest/awards/daiwa/daiwa_grants.parquet

Usage
-----
    python daiwa_to_s3.py                                  # full run
    python daiwa_to_s3.py --skip-upload                    # local dev
    python daiwa_to_s3.py --limit 50                       # smoke
    python daiwa_to_s3.py --skip-download --skip-upload    # reuse cache
    python daiwa_to_s3.py --allow-shrink                   # override section 1.4

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


SOURCES = [
    {
        "round": "March 2021",
        "registry_identifier": "a003W000001bHVUQA2",
        "url": "https://dajf.org.uk/wp-content/uploads/March-2021-Round_Daiwa-Foundation-Small-Grants-and-Awards.xlsx",
        "cache_name": "daiwa_2021_march.xlsx",
    },
    {
        "round": "September 2021",
        "registry_identifier": "a003W000004t8BVQAY",
        "url": "https://dajf.org.uk/wp-content/uploads/September_2021.xlsx",
        "cache_name": "daiwa_2021_september.xlsx",
    },
    {
        "round": "March 2022",
        "registry_identifier": "a003W000006G9DRQA0",
        "url": "https://dajf.org.uk/wp-content/uploads/Daiwa-Anglo-Japanese-Foundation-March-2022-Grants.xlsx",
        "cache_name": "daiwa_2022_march.xlsx",
    },
    {
        "round": "September 2022",
        "registry_identifier": "a003W000007EITiQAO",
        "url": "https://dajf.org.uk/wp-content/uploads/September-2022-Round-Daiwa-Foundation-Small-Grants-and-Awards.xlsx",
        "cache_name": "daiwa_2022_september.xlsx",
    },
    {
        "round": "March 2023",
        "registry_identifier": "a00P4000001wxaIIAQ",
        "url": "https://dajf.org.uk/wp-content/uploads/March-2023-Round-Daiwa-Foundation-Small-Grants-and-Awards.xlsx",
        "cache_name": "daiwa_2023_march.xlsx",
    },
    {
        "round": "September 2023",
        "registry_identifier": "a00P400000BKnKuIAL",
        "url": "https://dajf.org.uk/wp-content/uploads/September-2023-Round-Daiwa-Foundation-Awards-and-Small-Grants.xlsx",
        "cache_name": "daiwa_2023_september.xlsx",
    },
    {
        "round": "March 2024",
        "registry_identifier": "a00P400000lMOrvIAG",
        "url": "https://dajf.org.uk/wp-content/uploads/Daiwa-Anglo-Japanese-Foundation-March-2024-Grants.xlsx",
        "cache_name": "daiwa_2024_march.xlsx",
    },
    {
        "round": "September 2024",
        "registry_identifier": "a00P400000QMQUpIAP",
        "url": "https://dajf.org.uk/wp-content/uploads/September-2024-Round-Daiwa-Foundation-Awards-and-Small-Grants.xlsx",
        "cache_name": "daiwa_2024_september.xlsx",
    },
]

FUNDER_ID = 4320320179
FUNDER_DISPLAY_NAME = "Daiwa Anglo-Japanese Foundation"
PROVENANCE = "daiwa_360giving"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/daiwa/daiwa_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-daiwa-ingest/1.0; +https://openalex.org)"
DEFAULT_CACHE_DIR = Path(".cache/daiwa")

COUNTRY_ISO = {
    "uk": "GB",
    "united kingdom": "GB",
    "gb": "GB",
    "japan": "JP",
    "jp": "JP",
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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
    country = clean_text(v)
    if not country:
        return None
    return COUNTRY_ISO.get(country.lower())


def get_col(rec: dict, *names):
    for name in names:
        if name in rec:
            return rec.get(name)
    return None


def build_row(rec: dict, source_round: str, registry_identifier: str, source_url: str) -> Optional[dict]:
    ident = clean_text(rec.get("Identifier"))
    if not ident:
        return None

    amount = parse_amount(rec.get("Amount Awarded"))
    currency = clean_text(rec.get("Currency"))
    award_date = iso_date(rec.get("Award Date"))
    country = clean_text(rec.get("Recipient Org:Country"))

    return {
        "funder_award_id": ident,
        "title": clean_text(rec.get("Title")),
        "description": clean_text(rec.get("Description")),
        "amount": amount,
        "amount_raw": clean_text(rec.get("Amount Awarded")),
        "currency": currency.upper() if amount is not None and currency else None,
        "award_date": award_date,
        "start_year": year_of(award_date),
        "recipient_org": clean_text(rec.get("Recipient Org:Name")),
        "recipient_org_identifier": clean_text(rec.get("Recipient Org:Identifier")),
        "recipient_charity_number": clean_text(rec.get("Recipient Org:Charity Number")),
        "recipient_company_number": clean_text(rec.get("Recipient Org:Company Number")),
        "recipient_postal_code": clean_text(rec.get("Recipient Org:Postal Code")),
        "recipient_country": country,
        "recipient_country_iso": country_iso(country),
        "grant_programme": clean_text(get_col(rec, "Grant Programme: Title", "Grant Programme:Title")),
        "funding_org": clean_text(rec.get("Funding Org:Name")) or FUNDER_DISPLAY_NAME,
        "funding_org_identifier": clean_text(rec.get("Funding Org:Identifier")),
        "last_modified": iso_date(get_col(rec, "Last modified", "Last Modified")),
        "data_source": clean_text(rec.get("Data Source")),
        "source_round": source_round,
        "registry_identifier": registry_identifier,
        "source_workbook_url": source_url,
    }


def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)

    for field in (
        "funder_award_id", "title", "description", "amount", "currency",
        "award_date", "start_year", "recipient_org", "recipient_org_identifier",
        "recipient_country_iso", "grant_programme", "funding_org", "source_round",
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
        log(f"  award year range: {min(years)}-{max(years)}")

    amounts = [row["amount"] for row in rows if row.get("amount") is not None]
    if amounts:
        amounts_sorted = sorted(amounts)
        log(
            f"  amount stats: n={len(amounts)} ({len(amounts) * 100 / n:.1f}%) "
            f"min={min(amounts):,.0f} median={amounts_sorted[len(amounts_sorted) // 2]:,.0f} "
            f"max={max(amounts):,.0f} total={sum(amounts):,.0f}"
        )

    countries = {}
    programmes = {}
    rounds = {}
    for row in rows:
        countries[row.get("recipient_country_iso") or "(none)"] = countries.get(row.get("recipient_country_iso") or "(none)", 0) + 1
        programmes[row.get("grant_programme") or "(none)"] = programmes.get(row.get("grant_programme") or "(none)", 0) + 1
        rounds[row.get("source_round") or "(none)"] = rounds.get(row.get("source_round") or "(none)", 0) + 1
    log("  countries: " + ", ".join(f"{k}={v}" for k, v in sorted(countries.items())))
    log("  programmes: " + ", ".join(f"{k}={v}" for k, v in sorted(programmes.items(), key=lambda item: (-item[1], item[0]))))
    log("  rounds: " + ", ".join(f"{k}={v}" for k, v in sorted(rounds.items())))


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
    parser = argparse.ArgumentParser(description="Fetch Daiwa 360Giving grants -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "daiwa_grants.parquet"

    log("=== Daiwa Anglo-Japanese Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache_dir={args.cache_dir}")

    rows = []
    import io

    for source in SOURCES:
        cache_path = args.cache_dir / source["cache_name"]
        content = download_file(source["url"], cache_path, args.skip_download)
        raw_df = pd.read_excel(io.BytesIO(content), sheet_name=0, dtype=str)
        log(f"  loaded {len(raw_df):,} rows x {len(raw_df.columns)} cols from {source['round']}")
        for rec in raw_df.to_dict("records"):
            row = build_row(rec, source["round"], source["registry_identifier"], source["url"])
            if row is not None:
                rows.append(row)

    if args.limit is not None:
        rows = rows[:args.limit]
        log(f"--limit {args.limit}: validating/writing {len(rows)} rows")

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = pd.DataFrame(rows)
    df = df.astype("string")
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size / 1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Daiwa ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("section 1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Daiwa ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
