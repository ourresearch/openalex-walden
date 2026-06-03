#!/usr/bin/env python3
"""
Lloyd's Register Foundation Grants → S3 Pipeline (ORG-LEVEL GRANT PATTERN, method-1 360Giving open data)
=======================================================================================================

Downloads Lloyd's Register Foundation's published grant record from its own
website, in the **360Giving** open-data standard. LRF is a UK charity (the
grant-giving arm of Lloyd's Register) funding engineering, safety and
applied-science research worldwide.

Discovery (method-1, direct open-data file): LRF is a 360Giving publisher
(registry prefix `360G-LloydsRegisterFdn`). The 360Giving Data Registry
(https://registry.threesixtygiving.org/data.json) resolves to a single
direct Excel download published under the 360Giving licence:

    https://www.lrfoundation.org.uk/sites/default/files/.../lrf-360giving-master-sheet-...xlsx

This module shares the reusable 360Giving column-resolver below (handles the
per-publisher header variants — e.g. `Recipient Org:Name` here vs
`Recipient Org: Name` elsewhere — and the absence of a programme column).

Columns used (canonical 360Giving fields):

    Identifier                 360G-LloydsRegisterFdn-NNNN  stable unique grant id
    Title / Description        grant title / purpose
    Currency / Amount Awarded  GBP / granted amount
    Award Date                 decision date
    Planned Dates:Start Date   real project start date (YYYY-MM-DD)
    Planned Dates:End Date     real project end date
    Recipient Org:Name         grantee organization
    Recipient Org:Country      grantee country (free-text; LRF funds worldwide)

This is an ORG-LEVEL grant funder: each grant is made to a recipient
organization (no named PI). lead_investigator carries given/family NULL and
affiliation.name = the recipient org, with affiliation.country mapped from the
source's Recipient Org:Country (source-authoritative; never guessed — values
that are not a recognizable country, e.g. a bare city name, map to NULL). The
grant `Identifier` is the stable, unique, source-authoritative award id.

Awarding body in OpenAlex:
  Lloyd's Register Foundation (F4320310833, GB, ROR 057q4mw47,
  DOI 10.13039/100008885).

Amount: 360Giving `Amount Awarded` in GBP, populated where published (> 0);
NULL where the foundation discloses 0 (LRF publishes a positive amount on
100% of rows). §6.7 NOT waived, never imputed; any 0/blank treated as NULL.

Dates: real `Planned Dates:Start/End Date` are emitted as true ISO dates (no
false precision), falling back to `Award Date` for the start where the planned
start is absent; start_year/end_year derived from them.

Output
------
  s3://openalex-ingest/awards/lloyds_register/lloyds_register_grants.parquet

Usage
-----
    python lloyds_register_to_s3.py                                  # full run
    python lloyds_register_to_s3.py --skip-upload                    # local dev
    python lloyds_register_to_s3.py --limit 50                       # smoke
    python lloyds_register_to_s3.py --skip-download --skip-upload    # reuse cache
    python lloyds_register_to_s3.py --allow-shrink                   # override §1.4

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

DATA_URL = ("https://www.lrfoundation.org.uk/sites/default/files/2026-04/"
            "lrf-360giving-master-sheet-as-of-15.04.2026-2.xlsx")
SHEET = 0  # first sheet ("Sheet1")

FUNDER_ID = 4320310833
FUNDER_DISPLAY_NAME = "Lloyd's Register Foundation"

PROVENANCE = "lloyds_register_foundation"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lloyds_register/lloyds_register_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-lrf-ingest/1.0; +https://openalex.org)"

DEFAULT_CACHE = Path(".cache/lloyds_register_grants.xlsx")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# Reusable 360Giving helpers (column-variant tolerant)
# =============================================================================

def _norm_col(c: str) -> str:
    """Normalize a column header for matching: lowercase, strip, drop spaces around ':'."""
    return re.sub(r"\s*:\s*", ":", str(c).strip().lower())


def find_col(columns, *candidates) -> Optional[str]:
    """Return the actual column name matching any normalized candidate, else None.

    Handles the 360Giving per-publisher header variants
    (`Recipient Org:Name` vs `Recipient Org: Name`, etc.).
    """
    norm = {_norm_col(c): c for c in columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm:
            return norm[key]
    return None


# Recipient Org:Country free-text -> ISO 3166-1 alpha-2. Source-authoritative
# values only; anything not a recognizable country (e.g. a bare city) -> NULL.
COUNTRY_ISO = {
    "united kingdom": "GB", "great britain": "GB",
    "united kingdom of great britain and northern ireland": "GB",
    "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB",
    "united states": "US", "united states of america": "US", "usa": "US",
    "australia": "AU", "greece": "GR", "nigeria": "NG", "kenya": "KE",
    "sweden": "SE", "singapore": "SG", "netherlands": "NL", "canada": "CA",
    "turkey": "TR", "south africa": "ZA", "new zealand": "NZ", "china": "CN",
    "malta": "MT", "finland": "FI", "switzerland": "CH", "italy": "IT",
    "germany": "DE", "malaysia": "MY", "india": "IN", "belgium": "BE",
    "indonesia": "ID", "denmark": "DK", "philippines": "PH", "norway": "NO",
    "pakistan": "PK", "russian federation": "RU", "russia": "RU",
    "united arab emirates": "AE", "portugal": "PT", "ghana": "GH",
    "seychelles": "SC", "timor-leste": "TL", "botswana": "BW", "france": "FR",
    "argentina": "AR", "nepal": "NP", "japan": "JP", "tanzania": "TZ",
    "ireland": "IE", "spain": "ES", "brazil": "BR", "egypt": "EG",
    "viet nam": "VN", "vietnam": "VN", "bangladesh": "BD", "chile": "CL",
}


def country_iso(v) -> Optional[str]:
    """Map a free-text country value to ISO-2. Returns None when not a clear country (never guessed)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    # drop trailing parentheticals "(the)" / "(formerly ...)" and surrounding punctuation
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.replace(",", " ")
    s = re.sub(r"\bthe\b", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip().lower()
    if s in COUNTRY_ISO:
        return COUNTRY_ISO[s]
    # Korea: only South is published by LRF ("Korea (Republic of ...)")
    if "korea" in s and ("republic" in s or "south" in s):
        return "KR"
    return None  # unrecognized (e.g. a bare city name) -> never guessed


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


def parse_int(v) -> Optional[int]:
    if v is None:
        return None
    m = re.search(r"\d+", str(v))
    return int(m.group(0)) if m else None


# =============================================================================
# Build rows
# =============================================================================

def build_rows(df: pd.DataFrame) -> list:
    cols = list(df.columns)
    c_id = find_col(cols, "Identifier")
    c_title = find_col(cols, "Title")
    c_desc = find_col(cols, "Description")
    c_amount = find_col(cols, "Amount Awarded")
    c_award = find_col(cols, "Award Date")
    c_pstart = find_col(cols, "Planned Dates:Start Date")
    c_pend = find_col(cols, "Planned Dates:End Date")
    c_dur = find_col(cols, "Planned Dates:Duration (months)")
    c_rorg = find_col(cols, "Recipient Org:Name")
    c_rcity = find_col(cols, "Recipient Org:City")
    c_rcountry = find_col(cols, "Recipient Org:Country")
    c_prog = find_col(cols, "Grant Programme:Title")  # absent for LRF
    c_fund = find_col(cols, "Funding Org:Name")

    if not c_id:
        raise RuntimeError("no Identifier column found")
    log(f"  resolved cols: id={c_id!r} title={c_title!r} amount={c_amount!r} "
        f"pstart={c_pstart!r} pend={c_pend!r} country={c_rcountry!r} programme={c_prog!r}")

    rows = []
    for rec in df.to_dict("records"):
        ident = clean_text(rec.get(c_id))
        if not ident:
            continue
        amount = parse_amount(rec.get(c_amount)) if c_amount else None
        start_date = iso_date(rec.get(c_pstart)) if c_pstart else None
        if not start_date and c_award:
            start_date = iso_date(rec.get(c_award))
        end_date = iso_date(rec.get(c_pend)) if c_pend else None
        rows.append({
            "identifier":          ident,
            "funder_award_id":     ident,
            "title":               clean_text(rec.get(c_title)) if c_title else None,
            "description":         clean_text(rec.get(c_desc)) if c_desc else None,
            "amount":              amount,
            "amount_raw":          clean_text(rec.get(c_amount)) if c_amount else None,
            "currency":            "GBP" if amount is not None else None,
            "award_date":          iso_date(rec.get(c_award)) if c_award else None,
            "start_date":          start_date,
            "end_date":            end_date,
            "start_year":          year_of(start_date),
            "end_year":            year_of(end_date),
            "duration_months":     parse_int(rec.get(c_dur)) if c_dur else None,
            "grant_programme":     clean_text(rec.get(c_prog)) if c_prog else None,
            "recipient_org":       clean_text(rec.get(c_rorg)) if c_rorg else None,
            "recipient_city":      clean_text(rec.get(c_rcity)) if c_rcity else None,
            "recipient_country":   clean_text(rec.get(c_rcountry)) if c_rcountry else None,
            "recipient_country_iso": country_iso(rec.get(c_rcountry)) if c_rcountry else None,
            "funding_org":         (clean_text(rec.get(c_fund)) if c_fund else None) or FUNDER_DISPLAY_NAME,
        })
    return rows


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "funder_award_id", "amount", "start_date", "end_date",
              "start_year", "recipient_org", "recipient_country_iso", "description"):
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
        log(f"  start_year range: {min(years)}–{max(years)}")
    eyears = [r["end_year"] for r in rows if r.get("end_year")]
    if eyears:
        log(f"  end_year range: {min(eyears)}–{max(eyears)}")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        srt = sorted(amts)
        log(f"  amount stats (GBP): n={len(amts)} ({len(amts)*100/n:.1f}%) "
            f"min={min(amts):,.0f} median={srt[len(srt)//2]:,.0f} "
            f"max={max(amts):,.0f} total={sum(amts):,.0f}")

    countries = {}
    for r in rows:
        c = r.get("recipient_country_iso") or "(none)"
        countries[c] = countries.get(c, 0) + 1
    top = sorted(countries.items(), key=lambda x: -x[1])[:10]
    log("  recipient countries (ISO): " + ", ".join(f"{k}={v}" for k, v in top))


def build_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.astype("string")  # §1.2.5
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
    parser = argparse.ArgumentParser(description="Fetch Lloyd's Register Foundation grants (360Giving) → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "lloyds_register_grants.parquet"

    log("=== Lloyd's Register Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")

    content = download_workbook(args.skip_download, args.cache)
    import io
    df_raw = pd.read_excel(io.BytesIO(content), sheet_name=SHEET, dtype=str)
    log(f"  loaded {len(df_raw):,} rows × {len(df_raw.columns)} cols")

    rows = build_rows(df_raw)
    if args.limit is not None:
        rows = rows[:args.limit]
        log(f"--limit {args.limit}: keeping {len(rows)} rows")

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Lloyd's Register ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Lloyd's Register ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
