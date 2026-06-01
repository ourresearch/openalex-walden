#!/usr/bin/env python3
"""
Wolfson Foundation Grants → S3 Pipeline (ORG-LEVEL GRANT PATTERN, method-1 open-data download)
==============================================================================================

Downloads the Wolfson Foundation's published grant record from its own
website, in the **360Giving** open-data standard. The Wolfson Foundation
is a major UK grant-making charity (founded 1955) funding research and
capital projects across Science & Medicine, Health & Disability,
Education, and Arts & Humanities.

Discovery (method-1, direct open-data file): the foundation is a 360Giving
publisher (registry prefix `360G-wolfson`). The 360Giving Data Registry
(https://registry.threesixtygiving.org/data.json) resolves to a single
direct download of the full grant corpus as an Excel workbook published
under CC BY 4.0:

    https://www.wolfson.org.uk/wp-content/uploads/2026/02/2014-2025-grants-awarded.xlsx

Sheet `grants` carries one row per grant in the 360Giving schema. The
columns used here:

    Identifier               360G-wolfson-NNNNN   stable, unique grant id
    Title                    grant title
    Description              free-text purpose
    Currency                 GBP (uniform)
    Amount Awarded           granted amount in GBP (0 where undisclosed)
    Award Date               real ISO date (YYYY-MM-DD)
    Recipient Org: Name      grantee organization
    Recipient Org: City      grantee city
    Recipient Org: Country   grantee country (source-authoritative; "UK")
    Grant Programme: Title   programme (Science and Medicine, Education, ...)

This is an ORG-LEVEL grant funder: each grant is led by the grantee
organization (no named principal investigator). lead_investigator
therefore carries given/family NULL and affiliation.name = grantee org,
with affiliation.country mapped from the source's Recipient Org: Country
(source-authoritative, not guessed). The grant `Identifier` is the stable,
unique, source-authoritative award id.

Awarding body in OpenAlex:
  Wolfson Foundation (F4320320670, GB, ROR 0333xzh65,
  DOI 10.13039/501100001320).

Amount: 360Giving `Amount Awarded` in GBP. Populated where published
(> 0); NULL where the foundation discloses 0 (≈55% of grants carry a
positive amount — Sloan-style partial coverage; §6.7 NOT waived, never
imputed; any 0/blank treated as NULL). Currency GBP set only where amount
present.

Dates: real `Award Date` is emitted as a true ISO date (no year-to-`-01-01`
false precision); start_year is derived from it. end_date/end_year are
NULL — the source publishes only a *planned* duration (on ~32% of grants),
not an actual grant end date, so no false-precision end is asserted.

Output
------
  s3://openalex-ingest/awards/wolfson/wolfson_grants.parquet

Usage
-----
    python wolfson_to_s3.py                                  # full run
    python wolfson_to_s3.py --skip-upload                    # local dev
    python wolfson_to_s3.py --limit 50                       # smoke
    python wolfson_to_s3.py --skip-download --skip-upload    # reuse cache
    python wolfson_to_s3.py --allow-shrink                   # override §1.4

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

# =============================================================================
# Configuration
# =============================================================================

DATA_URL = "https://www.wolfson.org.uk/wp-content/uploads/2026/02/2014-2025-grants-awarded.xlsx"
SHEET = "grants"

FUNDER_ID = 4320320670
FUNDER_DISPLAY_NAME = "Wolfson Foundation"

PROVENANCE = "wolfson_foundation"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wolfson/wolfson_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-wolfson-ingest/1.0; +https://openalex.org)"

DEFAULT_CACHE = Path(".cache/wolfson_grants.xlsx")

# Recipient Org: Country -> ISO 3166-1 alpha-2. Source-authoritative values only;
# anything unmapped stays NULL rather than being guessed.
COUNTRY_ISO = {
    "uk": "GB", "united kingdom": "GB", "gb": "GB", "great britain": "GB",
    "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB",
    "ireland": "IE", "usa": "US", "united states": "US",
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# Download
# =============================================================================

def download_workbook(use_cache: bool, cache_path: Path) -> bytes:
    if use_cache and cache_path.exists():
        log(f"  reusing cached workbook {cache_path}")
        return cache_path.read_bytes()
    import requests
    log(f"  downloading {DATA_URL}")
    resp = requests.get(DATA_URL, headers={"User-Agent": USER_AGENT}, timeout=120)
    resp.raise_for_status()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(resp.content)
    log(f"  wrote cache {cache_path} ({len(resp.content)/1e3:.0f} KB)")
    return resp.content


# =============================================================================
# Field parsing
# =============================================================================

def clean_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s.strip().lower() in ("", "nan", "none"):
        return None
    # Excel escapes carriage returns as the literal token _x000D_
    s = s.replace("_x000D_", "\n")
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    s = re.sub(r"\n{2,}", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip() or None


def parse_amount(v) -> Optional[float]:
    """360Giving Amount Awarded -> positive float (GBP) or None. Any 0/blank -> None (§6.7)."""
    if v is None:
        return None
    try:
        amt = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return amt if amt > 0 else None


def iso_date(v) -> Optional[str]:
    """Keep a real ISO date YYYY-MM-DD (no false precision)."""
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
    return COUNTRY_ISO.get(c.lower())  # unmapped -> None (never guessed)


def parse_int(v) -> Optional[int]:
    if v is None:
        return None
    m = re.search(r"\d+", str(v))
    return int(m.group(0)) if m else None


def build_row(rec: dict) -> Optional[dict]:
    ident = clean_text(rec.get("Identifier"))
    if not ident:
        return None
    amount = parse_amount(rec.get("Amount Awarded"))
    award_date = iso_date(rec.get("Award Date"))
    return {
        "identifier":          ident,
        "funder_award_id":     ident,
        "title":               clean_text(rec.get("Title")),
        "description":         clean_text(rec.get("Description")),
        "amount":              amount,
        "amount_raw":          clean_text(rec.get("Amount Awarded")),
        "currency":            "GBP" if amount is not None else None,
        "award_date":          award_date,
        "start_year":          year_of(award_date),
        "duration_months":     parse_int(rec.get("Planned Dates:Duration (months)")),
        "grant_programme":     clean_text(rec.get("Grant Programme: Title")),
        "recipient_org":       clean_text(rec.get("Recipient Org: Name")),
        "recipient_city":      clean_text(rec.get("Recipient Org: City")),
        "recipient_country":   clean_text(rec.get("Recipient Org: Country")),
        "recipient_country_iso": country_iso(rec.get("Recipient Org: Country")),
        "funding_org":         clean_text(rec.get("Funding Org: Name")) or FUNDER_DISPLAY_NAME,
        "grant_programme_url": clean_text(rec.get("Grant Programme: URL")),
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "funder_award_id", "amount", "award_date", "start_year",
              "recipient_org", "recipient_country_iso", "grant_programme", "description"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<22} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    ids = [r["funder_award_id"] for r in rows if r.get("funder_award_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(f"funder_award_id collisions: {dups}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")

    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  award year range: {min(years)}–{max(years)}")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        srt = sorted(amts)
        log(f"  amount stats (GBP): n={len(amts)} ({len(amts)*100/n:.1f}%) "
            f"min={min(amts):,.0f} median={srt[len(srt)//2]:,.0f} "
            f"max={max(amts):,.0f} total={sum(amts):,.0f}")

    progs = {}
    for r in rows:
        p = r.get("grant_programme") or "(none)"
        progs[p] = progs.get(p, 0) + 1
    log("  programmes: " + ", ".join(f"{k}={v}" for k, v in sorted(progs.items(), key=lambda x: -x[1])[:8]))


def build_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # §1.2.5: force string dtype to stop pandas int-inferring null-heavy cols.
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Wolfson Foundation grants (360Giving) → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "wolfson_grants.parquet"

    log("=== Wolfson Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    content = download_workbook(args.skip_download, args.cache)
    import io
    df_raw = pd.read_excel(io.BytesIO(content), sheet_name=SHEET, dtype=str)
    log(f"  loaded {len(df_raw):,} rows × {len(df_raw.columns)} cols from sheet '{SHEET}'")

    records = df_raw.to_dict("records")
    if args.limit is not None:
        records = records[:args.limit]
        log(f"--limit {args.limit}: processing {len(records)} rows")

    rows = []
    for rec in records:
        row = build_row(rec)
        if row is not None:
            rows.append(row)

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Wolfson ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Wolfson ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
